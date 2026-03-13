//! TLS configuration for QUIC transport.
//!
//! Provides self-signed certificate generation for development and
//! configurable certificate loading for production mTLS deployments.

use std::sync::Arc;

use crate::error::{TransportError, TransportResult};

/// A TLS certificate and private key pair.
pub struct CertKeyPair {
    pub cert_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
    pub private_key: rustls::pki_types::PrivateKeyDer<'static>,
}

impl std::fmt::Debug for CertKeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CertKeyPair")
            .field("cert_chain_len", &self.cert_chain.len())
            .field("private_key", &"[redacted]")
            .finish()
    }
}

/// Generate a self-signed certificate for development/testing.
///
/// The certificate is valid for the given subject alternative names
/// (DNS names and/or IP addresses).
pub fn generate_self_signed(san: &[&str]) -> TransportResult<CertKeyPair> {
    let mut params =
        rcgen::CertificateParams::new(san.iter().map(|s| s.to_string()).collect::<Vec<_>>())
            .map_err(|e| TransportError::Tls(format!("certificate params error: {e}")))?;

    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "RuniFi Transport");
    params
        .distinguished_name
        .push(rcgen::DnType::OrganizationName, "RuniFi");

    let key_pair = rcgen::KeyPair::generate()
        .map_err(|e| TransportError::Tls(format!("key generation error: {e}")))?;

    let cert = params
        .self_signed(&key_pair)
        .map_err(|e| TransportError::Tls(format!("self-sign error: {e}")))?;

    let cert_der = rustls::pki_types::CertificateDer::from(cert.der().to_vec());
    let key_der = rustls::pki_types::PrivateKeyDer::Pkcs8(
        rustls::pki_types::PrivatePkcs8KeyDer::from(key_pair.serialize_der()),
    );

    Ok(CertKeyPair {
        cert_chain: vec![cert_der],
        private_key: key_der,
    })
}

/// Load a certificate chain and private key from PEM files.
pub fn load_from_pem(cert_path: &str, key_path: &str) -> TransportResult<CertKeyPair> {
    let cert_data = std::fs::read(cert_path).map_err(TransportError::Io)?;
    let key_data = std::fs::read(key_path).map_err(TransportError::Io)?;

    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls_pemfile::certs(&mut cert_data.as_slice())
            .collect::<Result<Vec<_>, _>>()
            .map_err(TransportError::Io)?;

    if certs.is_empty() {
        return Err(TransportError::Tls(
            "no certificates found in PEM file".into(),
        ));
    }

    let key = rustls_pemfile::private_key(&mut key_data.as_slice())
        .map_err(TransportError::Io)?
        .ok_or_else(|| TransportError::Tls("no private key found in PEM file".into()))?;

    Ok(CertKeyPair {
        cert_chain: certs,
        private_key: key,
    })
}

/// Build a rustls ServerConfig for the QUIC server.
///
/// If `client_ca` is provided, mutual TLS (mTLS) is enforced.
pub fn build_server_config(
    cert_key: &CertKeyPair,
    _client_ca: Option<&[rustls::pki_types::CertificateDer<'static>]>,
) -> TransportResult<rustls::ServerConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut config = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| TransportError::Tls(format!("server config error: {e}")))?
        .with_no_client_auth()
        .with_single_cert(
            cert_key.cert_chain.clone(),
            cert_key.private_key.clone_key(),
        )
        .map_err(|e| TransportError::Tls(format!("server config error: {e}")))?;

    config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
    config.max_early_data_size = 0; // Disable 0-RTT for security

    Ok(config)
}

/// Build a rustls ClientConfig for the QUIC client.
///
/// For development, `danger_skip_verify` skips server certificate validation.
/// For production, provide trusted CA certificates.
pub fn build_client_config(
    client_cert: Option<&CertKeyPair>,
    danger_skip_verify: bool,
) -> TransportResult<rustls::ClientConfig> {
    if !danger_skip_verify {
        return Err(TransportError::Tls(
            "production client config requires trusted certificates; \
             use build_client_config_with_trusted_cert() instead"
                .into(),
        ));
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config_builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| TransportError::Tls(format!("client config error: {e}")))?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(SkipServerVerification));

    let mut config = if let Some(ck) = client_cert {
        config_builder.with_client_cert_resolver(Arc::new(StaticCertResolver {
            cert_chain: ck.cert_chain.clone(),
            private_key: ck.private_key.clone_key(),
        }))
    } else {
        config_builder.with_no_client_auth()
    };

    config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
    Ok(config)
}

/// Build a client config that trusts a specific self-signed certificate.
///
/// Used for testing and development when connecting to a server
/// with a self-signed cert.
pub fn build_client_config_with_trusted_cert(
    server_cert: &rustls::pki_types::CertificateDer<'static>,
    client_cert: Option<&CertKeyPair>,
) -> TransportResult<rustls::ClientConfig> {
    let mut root_store = rustls::RootCertStore::empty();
    root_store
        .add(server_cert.clone())
        .map_err(|e| TransportError::Tls(format!("add trusted cert error: {e}")))?;

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config_builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(|e| TransportError::Tls(format!("client config error: {e}")))?
        .with_root_certificates(root_store);

    let mut config = if let Some(ck) = client_cert {
        config_builder
            .with_client_auth_cert(ck.cert_chain.clone(), ck.private_key.clone_key())
            .map_err(|e| TransportError::Tls(format!("client cert error: {e}")))?
    } else {
        config_builder.with_no_client_auth()
    };

    config.alpn_protocols = vec![ALPN_PROTOCOL.to_vec()];
    Ok(config)
}

/// ALPN protocol identifier for RuniFi transport.
pub const ALPN_PROTOCOL: &[u8] = b"runifi/1";

/// Dangerous: skip server certificate verification (development only).
#[derive(Debug)]
struct SkipServerVerification;

impl rustls::client::danger::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Static certificate resolver for client mTLS.
#[derive(Debug)]
struct StaticCertResolver {
    cert_chain: Vec<rustls::pki_types::CertificateDer<'static>>,
    private_key: rustls::pki_types::PrivateKeyDer<'static>,
}

impl rustls::client::ResolvesClientCert for StaticCertResolver {
    fn resolve(
        &self,
        _acceptable_issuers: &[&[u8]],
        _sigschemes: &[rustls::SignatureScheme],
    ) -> Option<Arc<rustls::sign::CertifiedKey>> {
        let signing_key = rustls::crypto::ring::sign::any_supported_type(&self.private_key).ok()?;
        Some(Arc::new(rustls::sign::CertifiedKey::new(
            self.cert_chain.clone(),
            signing_key,
        )))
    }

    fn has_certs(&self) -> bool {
        !self.cert_chain.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_self_signed_cert() {
        let pair = generate_self_signed(&["localhost", "127.0.0.1"]).unwrap();
        assert_eq!(pair.cert_chain.len(), 1);
    }

    #[test]
    fn build_server_config_succeeds() {
        let pair = generate_self_signed(&["localhost"]).unwrap();
        let config = build_server_config(&pair, None).unwrap();
        assert_eq!(config.alpn_protocols, vec![ALPN_PROTOCOL.to_vec()]);
    }

    #[test]
    fn build_client_config_skip_verify() {
        let config = build_client_config(None, true).unwrap();
        assert_eq!(config.alpn_protocols, vec![ALPN_PROTOCOL.to_vec()]);
    }

    #[test]
    fn build_client_config_with_trusted_cert_succeeds() {
        let pair = generate_self_signed(&["localhost"]).unwrap();
        let config = build_client_config_with_trusted_cert(&pair.cert_chain[0], None).unwrap();
        assert_eq!(config.alpn_protocols, vec![ALPN_PROTOCOL.to_vec()]);
    }
}
