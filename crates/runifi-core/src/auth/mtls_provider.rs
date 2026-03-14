//! mTLS (mutual TLS) client certificate authentication provider.
//!
//! Extracts identity from X.509 client certificates presented during the TLS
//! handshake. Supports CN (Common Name) and SAN (Subject Alternative Name)
//! extraction with optional regex filtering.
//!
//! Configured via TOML under `[auth.mtls]`.

use std::collections::HashMap;

use serde::Deserialize;

use super::provider::{
    AuthCredentials, AuthError, AuthProvider, AuthResult, CredentialType, UserIdentity,
};

/// mTLS provider configuration from TOML.
#[derive(Debug, Deserialize, Clone)]
pub struct MtlsConfig {
    /// Path to CA certificate(s) for client certificate validation.
    pub ca_cert_path: String,
    /// Identity extraction field: `"cn"` or `"san"`. Default: `"cn"`.
    #[serde(default = "default_identity_field")]
    pub identity_field: String,
    /// Regex to apply to the extracted identity field (optional).
    pub identity_regex: Option<String>,
    /// Path to CRL file(s) for certificate revocation checking (optional).
    pub crl_path: Option<String>,
    /// DN-to-group mapping for role assignment.
    #[serde(default)]
    pub dn_group_mapping: HashMap<String, Vec<String>>,
}

fn default_identity_field() -> String {
    "cn".into()
}

/// mTLS authentication provider.
///
/// Extracts identity from DER-encoded X.509 client certificates.
/// Certificate validation (CA trust, revocation) is handled at the TLS layer
/// by rustls; this provider only extracts the identity fields.
#[derive(Debug)]
pub struct MtlsAuthProvider {
    config: MtlsConfig,
}

impl MtlsAuthProvider {
    pub fn new(config: MtlsConfig) -> Self {
        Self { config }
    }

    /// Get the mTLS configuration.
    pub fn config(&self) -> &MtlsConfig {
        &self.config
    }

    /// Extract the Common Name from an X.509 certificate subject.
    ///
    /// Parses the DER-encoded certificate and extracts the CN field from the
    /// subject distinguished name. Returns `None` if the certificate cannot
    /// be parsed or has no CN.
    pub fn extract_cn_from_der(der_bytes: &[u8]) -> Option<String> {
        // Parse the DER-encoded X.509 certificate.
        // OID for Common Name: 2.5.4.3
        let (_, cert) = x509_parser::parse_x509_certificate(der_bytes).ok()?;
        let subject = cert.subject();
        for rdn in subject.iter() {
            for attr in rdn.iter() {
                if attr.attr_type().to_id_string() == "2.5.4.3" {
                    return attr.as_str().ok().map(String::from);
                }
            }
        }
        None
    }

    /// Extract Subject Alternative Names (SANs) from an X.509 certificate.
    pub fn extract_sans_from_der(der_bytes: &[u8]) -> Vec<String> {
        let (_, cert) = match x509_parser::parse_x509_certificate(der_bytes) {
            Ok(result) => result,
            Err(_) => return Vec::new(),
        };

        let mut sans = Vec::new();
        for ext in cert.extensions() {
            if let x509_parser::extensions::ParsedExtension::SubjectAlternativeName(san) =
                ext.parsed_extension()
            {
                for name in &san.general_names {
                    match name {
                        x509_parser::extensions::GeneralName::DNSName(dns) => {
                            sans.push(dns.to_string());
                        }
                        x509_parser::extensions::GeneralName::RFC822Name(email) => {
                            sans.push(email.to_string());
                        }
                        _ => {}
                    }
                }
            }
        }
        sans
    }

    /// Extract identity from a certificate using the configured field and regex.
    pub fn extract_identity(&self, der_bytes: &[u8]) -> Result<String, AuthError> {
        let raw_identity = match self.config.identity_field.as_str() {
            "cn" => Self::extract_cn_from_der(der_bytes)
                .ok_or_else(|| AuthError::CertificateRejected("No CN in certificate".into()))?,
            "san" => {
                let sans = Self::extract_sans_from_der(der_bytes);
                sans.into_iter()
                    .next()
                    .ok_or_else(|| AuthError::CertificateRejected("No SAN in certificate".into()))?
            }
            field => {
                return Err(AuthError::ConfigError(format!(
                    "Unknown identity_field: {}",
                    field
                )));
            }
        };

        // Apply optional regex extraction.
        if let Some(regex_str) = &self.config.identity_regex {
            let re = regex_lite::Regex::new(regex_str)
                .map_err(|e| AuthError::ConfigError(format!("Invalid identity_regex: {}", e)))?;
            if let Some(caps) = re.captures(&raw_identity) {
                // Use first capture group, or the whole match if no groups.
                let matched = caps
                    .get(1)
                    .or_else(|| caps.get(0))
                    .map(|m| m.as_str().to_string())
                    .unwrap_or(raw_identity);
                return Ok(matched);
            }
            return Err(AuthError::CertificateRejected(format!(
                "CN/SAN '{}' did not match regex '{}'",
                raw_identity, regex_str
            )));
        }

        Ok(raw_identity)
    }

    /// Resolve groups for a given identity using the DN-to-group mapping.
    pub fn resolve_groups(&self, identity: &str) -> Vec<String> {
        self.config
            .dn_group_mapping
            .get(identity)
            .cloned()
            .unwrap_or_default()
    }
}

#[async_trait::async_trait]
impl AuthProvider for MtlsAuthProvider {
    fn name(&self) -> &str {
        "mtls"
    }

    fn supported_credentials(&self) -> &[CredentialType] {
        &[CredentialType::Certificate]
    }

    async fn authenticate(&self, credentials: &AuthCredentials) -> AuthResult {
        let cert_bytes = match credentials {
            AuthCredentials::Certificate(bytes) => bytes,
            _ => return AuthResult::Unsupported,
        };

        if cert_bytes.is_empty() {
            return AuthResult::Failed(AuthError::CertificateRejected("Empty certificate".into()));
        }

        match self.extract_identity(cert_bytes) {
            Ok(username) => {
                let groups = self.resolve_groups(&username);
                AuthResult::Authenticated(UserIdentity {
                    username,
                    display_name: None,
                    groups,
                    provider: "mtls".into(),
                    email: None,
                    expires_at: None,
                    provider_data: None,
                })
            }
            Err(err) => AuthResult::Failed(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> MtlsConfig {
        MtlsConfig {
            ca_cert_path: "/etc/runifi/ca.pem".into(),
            identity_field: "cn".into(),
            identity_regex: None,
            crl_path: None,
            dn_group_mapping: HashMap::from([
                ("alice".into(), vec!["admins".into()]),
                ("bob".into(), vec!["operators".into()]),
            ]),
        }
    }

    #[test]
    fn resolve_groups() {
        let provider = MtlsAuthProvider::new(test_config());
        assert_eq!(provider.resolve_groups("alice"), vec!["admins"]);
        assert_eq!(provider.resolve_groups("bob"), vec!["operators"]);
        assert!(provider.resolve_groups("unknown").is_empty());
    }

    #[test]
    fn default_identity_field_is_cn() {
        assert_eq!(default_identity_field(), "cn");
    }

    #[tokio::test]
    async fn empty_certificate_rejected() {
        let provider = MtlsAuthProvider::new(test_config());
        let creds = AuthCredentials::Certificate(vec![]);
        match provider.authenticate(&creds).await {
            AuthResult::Failed(AuthError::CertificateRejected(_)) => {}
            _ => panic!("Expected Failed(CertificateRejected)"),
        }
    }

    #[tokio::test]
    async fn unsupported_credential_type() {
        let provider = MtlsAuthProvider::new(test_config());
        let creds = AuthCredentials::Password {
            username: "test".into(),
            password: "pass".into(),
        };
        match provider.authenticate(&creds).await {
            AuthResult::Unsupported => {}
            _ => panic!("Expected Unsupported"),
        }
    }

    #[test]
    fn extract_identity_with_regex() {
        // Create a config with regex to extract username from a CN like "CN=alice@EXAMPLE.COM"
        let config = MtlsConfig {
            identity_regex: Some(r"^([^@]+)".into()),
            ..test_config()
        };
        let provider = MtlsAuthProvider::new(config);

        // Since we don't have a real cert, test the regex logic directly:
        // The extract_identity() method relies on parsing a real DER cert first,
        // so we test the regex application path separately.
        let re = regex_lite::Regex::new(r"^([^@]+)").unwrap();
        let caps = re.captures("alice@EXAMPLE.COM").unwrap();
        let matched = caps.get(1).unwrap().as_str();
        assert_eq!(matched, "alice");
    }
}
