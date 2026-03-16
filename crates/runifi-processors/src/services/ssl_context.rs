//! SSLContextService — reusable TLS configuration using rustls.

use std::collections::HashMap;
use std::fs;
use std::io::BufReader;
use std::sync::Arc;

use runifi_plugin_api::property::PropertyDescriptor;
use runifi_plugin_api::result::{PluginError, ProcessResult};
use runifi_plugin_api::service::{ControllerService, ControllerServiceDescriptor};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};

/// Shared TLS configuration service backed by rustls.
///
/// Processors reference this service to obtain pre-configured `ClientConfig`
/// or `ServerConfig` instances, avoiding per-processor TLS setup duplication.
pub struct SSLContextService {
    enabled: bool,
    keystore_path: Option<String>,
    keystore_type: String,
    truststore_path: Option<String>,
    truststore_type: String,
    tls_protocol: String,
    client_auth: String,
    client_config: Option<Arc<ClientConfig>>,
    server_config: Option<Arc<ServerConfig>>,
}

const PROP_KEYSTORE_FILENAME: PropertyDescriptor = PropertyDescriptor::new(
    "Keystore Filename",
    "Path to the keystore file containing the certificate and private key.",
)
.expression_language_supported();

const PROP_KEYSTORE_TYPE: PropertyDescriptor =
    PropertyDescriptor::new("Keystore Type", "The type of keystore.")
        .default_value("PEM")
        .allowed_values(&["PEM"]);

const PROP_KEYSTORE_PASSWORD: PropertyDescriptor = PropertyDescriptor::new(
    "Keystore Password",
    "Password for the keystore (if encrypted).",
)
.sensitive();

const PROP_TRUSTSTORE_FILENAME: PropertyDescriptor = PropertyDescriptor::new(
    "Truststore Filename",
    "Path to the truststore file containing trusted CA certificates.",
)
.expression_language_supported();

const PROP_TRUSTSTORE_TYPE: PropertyDescriptor =
    PropertyDescriptor::new("Truststore Type", "The type of truststore.")
        .default_value("PEM")
        .allowed_values(&["PEM"]);

const PROP_TLS_PROTOCOL: PropertyDescriptor =
    PropertyDescriptor::new("TLS Protocol", "The TLS protocol version to use.")
        .default_value("TLS 1.3")
        .allowed_values(&["TLS 1.2", "TLS 1.3"]);

const PROP_CLIENT_AUTH: PropertyDescriptor = PropertyDescriptor::new(
    "Client Auth",
    "Whether to require client certificate authentication.",
)
.default_value("NONE")
.allowed_values(&["REQUIRED", "WANT", "NONE"]);

impl SSLContextService {
    pub fn new() -> Self {
        Self {
            enabled: false,
            keystore_path: None,
            keystore_type: "PEM".to_string(),
            truststore_path: None,
            truststore_type: "PEM".to_string(),
            tls_protocol: "TLS 1.3".to_string(),
            client_auth: "NONE".to_string(),
            client_config: None,
            server_config: None,
        }
    }

    /// Get the configured TLS client configuration.
    pub fn client_config(&self) -> Option<Arc<ClientConfig>> {
        self.client_config.clone()
    }

    /// Get the configured TLS server configuration.
    pub fn server_config(&self) -> Option<Arc<ServerConfig>> {
        self.server_config.clone()
    }

    /// Load PEM certificates from a file.
    fn load_certs(path: &str) -> ProcessResult<Vec<CertificateDer<'static>>> {
        let file = fs::File::open(path).map_err(|e| {
            PluginError::ProcessingFailed(format!("failed to open certificate file '{path}': {e}"))
        })?;
        let mut reader = BufReader::new(file);
        let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| {
                PluginError::ProcessingFailed(format!(
                    "failed to parse certificates from '{path}': {e}"
                ))
            })?;
        if certs.is_empty() {
            return Err(PluginError::ProcessingFailed(format!(
                "no certificates found in '{path}'"
            )));
        }
        Ok(certs)
    }

    /// Load a PEM private key from a file.
    fn load_private_key(path: &str) -> ProcessResult<PrivateKeyDer<'static>> {
        let file = fs::File::open(path).map_err(|e| {
            PluginError::ProcessingFailed(format!("failed to open key file '{path}': {e}"))
        })?;
        let mut reader = BufReader::new(file);
        let key = rustls_pemfile::private_key(&mut reader)
            .map_err(|e| {
                PluginError::ProcessingFailed(format!(
                    "failed to parse private key from '{path}': {e}"
                ))
            })?
            .ok_or_else(|| {
                PluginError::ProcessingFailed(format!("no private key found in '{path}'"))
            })?;
        Ok(key)
    }
}

impl Default for SSLContextService {
    fn default() -> Self {
        Self::new()
    }
}

impl ControllerService for SSLContextService {
    fn on_configure(&mut self, properties: &HashMap<String, String>) -> ProcessResult {
        self.keystore_path = properties.get("Keystore Filename").cloned();
        self.keystore_type = properties
            .get("Keystore Type")
            .cloned()
            .unwrap_or_else(|| "PEM".to_string());
        self.truststore_path = properties.get("Truststore Filename").cloned();
        self.truststore_type = properties
            .get("Truststore Type")
            .cloned()
            .unwrap_or_else(|| "PEM".to_string());
        self.tls_protocol = properties
            .get("TLS Protocol")
            .cloned()
            .unwrap_or_else(|| "TLS 1.3".to_string());
        self.client_auth = properties
            .get("Client Auth")
            .cloned()
            .unwrap_or_else(|| "NONE".to_string());
        Ok(())
    }

    fn validate(&self) -> ProcessResult {
        if let Some(ref path) = self.truststore_path
            && !std::path::Path::new(path).exists()
        {
            return Err(PluginError::ProcessingFailed(format!(
                "truststore file does not exist: {path}"
            )));
        }
        if let Some(ref path) = self.keystore_path
            && !std::path::Path::new(path).exists()
        {
            return Err(PluginError::ProcessingFailed(format!(
                "keystore file does not exist: {path}"
            )));
        }
        Ok(())
    }

    fn enable(&mut self) -> ProcessResult {
        let provider = Arc::new(rustls::crypto::ring::default_provider());

        // Build client config with root certificates.
        let mut root_store = RootCertStore::empty();

        if let Some(ref truststore_path) = self.truststore_path {
            let certs = Self::load_certs(truststore_path)?;
            for cert in certs {
                root_store.add(cert).map_err(|e| {
                    PluginError::ProcessingFailed(format!(
                        "failed to add certificate to root store: {e}"
                    ))
                })?;
            }
        }

        let client_config = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()
            .map_err(|e| {
                PluginError::ProcessingFailed(format!("failed to set TLS protocol versions: {e}"))
            })?
            .with_root_certificates(root_store)
            .with_no_client_auth();
        self.client_config = Some(Arc::new(client_config));

        // Build server config if keystore is provided.
        if let Some(ref keystore_path) = self.keystore_path {
            let certs = Self::load_certs(keystore_path)?;
            let key = Self::load_private_key(keystore_path)?;

            let server_config = ServerConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|e| {
                    PluginError::ProcessingFailed(format!(
                        "failed to set TLS protocol versions: {e}"
                    ))
                })?
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|e| {
                    PluginError::ProcessingFailed(format!("failed to build server TLS config: {e}"))
                })?;
            self.server_config = Some(Arc::new(server_config));
        }

        self.enabled = true;
        tracing::info!(
            protocol = %self.tls_protocol,
            client_auth = %self.client_auth,
            "SSLContextService enabled"
        );
        Ok(())
    }

    fn disable(&mut self) -> ProcessResult {
        self.client_config = None;
        self.server_config = None;
        self.enabled = false;
        tracing::info!("SSLContextService disabled");
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.enabled
    }

    fn property_descriptors(&self) -> Vec<PropertyDescriptor> {
        vec![
            PROP_KEYSTORE_FILENAME,
            PROP_KEYSTORE_TYPE,
            PROP_KEYSTORE_PASSWORD,
            PROP_TRUSTSTORE_FILENAME,
            PROP_TRUSTSTORE_TYPE,
            PROP_TLS_PROTOCOL,
            PROP_CLIENT_AUTH,
        ]
    }
}

inventory::submit! {
    ControllerServiceDescriptor {
        type_name: "SSLContextService",
        description: "Provides reusable TLS/SSL configuration using rustls for secure communication",
        factory: || Box::new(SSLContextService::new()),
        tags: &["Security", "TLS"],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn property_descriptors_returned() {
        let svc = SSLContextService::new();
        let descriptors = svc.property_descriptors();
        assert_eq!(descriptors.len(), 7);

        let names: Vec<&str> = descriptors.iter().map(|d| d.name).collect();
        assert!(names.contains(&"Keystore Filename"));
        assert!(names.contains(&"Truststore Filename"));
        assert!(names.contains(&"TLS Protocol"));
        assert!(names.contains(&"Client Auth"));
        assert!(names.contains(&"Keystore Password"));
    }

    #[test]
    fn keystore_password_is_sensitive() {
        let svc = SSLContextService::new();
        let descriptors = svc.property_descriptors();
        let password = descriptors
            .iter()
            .find(|d| d.name == "Keystore Password")
            .unwrap();
        assert!(password.sensitive);
    }

    #[test]
    fn lifecycle_without_files() {
        let mut svc = SSLContextService::new();
        assert!(!svc.is_enabled());

        // Configure with no paths — should work.
        let props = HashMap::new();
        svc.on_configure(&props).unwrap();
        svc.validate().unwrap();
        svc.enable().unwrap();
        assert!(svc.is_enabled());

        // Client config should exist (empty root store).
        assert!(svc.client_config().is_some());
        // Server config should not exist (no keystore).
        assert!(svc.server_config().is_none());

        svc.disable().unwrap();
        assert!(!svc.is_enabled());
        assert!(svc.client_config().is_none());
        assert!(svc.server_config().is_none());
    }

    #[test]
    fn validate_fails_with_missing_truststore() {
        let mut svc = SSLContextService::new();
        let mut props = HashMap::new();
        props.insert(
            "Truststore Filename".to_string(),
            "/nonexistent/truststore.pem".to_string(),
        );
        svc.on_configure(&props).unwrap();
        assert!(svc.validate().is_err());
    }

    #[test]
    fn validate_fails_with_missing_keystore() {
        let mut svc = SSLContextService::new();
        let mut props = HashMap::new();
        props.insert(
            "Keystore Filename".to_string(),
            "/nonexistent/keystore.pem".to_string(),
        );
        svc.on_configure(&props).unwrap();
        assert!(svc.validate().is_err());
    }

    #[test]
    fn enable_with_generated_certs() {
        use rcgen::generate_simple_self_signed;
        use std::io::Write;

        let subject_alt_names = vec!["localhost".to_string()];
        let certified_key = generate_simple_self_signed(subject_alt_names).unwrap();
        let cert_pem = certified_key.cert.pem();
        let key_pem = certified_key.key_pair.serialize_pem();

        // Write combined cert+key PEM to a temp file for keystore.
        let dir = tempfile::tempdir().unwrap();
        let keystore_path = dir.path().join("keystore.pem");
        let truststore_path = dir.path().join("truststore.pem");

        {
            let mut f = fs::File::create(&keystore_path).unwrap();
            f.write_all(cert_pem.as_bytes()).unwrap();
            f.write_all(key_pem.as_bytes()).unwrap();
        }
        {
            let mut f = fs::File::create(&truststore_path).unwrap();
            f.write_all(cert_pem.as_bytes()).unwrap();
        }

        let mut svc = SSLContextService::new();
        let mut props = HashMap::new();
        props.insert(
            "Keystore Filename".to_string(),
            keystore_path.to_str().unwrap().to_string(),
        );
        props.insert(
            "Truststore Filename".to_string(),
            truststore_path.to_str().unwrap().to_string(),
        );

        svc.on_configure(&props).unwrap();
        svc.validate().unwrap();
        svc.enable().unwrap();

        assert!(svc.is_enabled());
        assert!(svc.client_config().is_some());
        assert!(svc.server_config().is_some());

        svc.disable().unwrap();
        assert!(!svc.is_enabled());
    }

    #[test]
    fn default_property_values() {
        let svc = SSLContextService::new();
        let descriptors = svc.property_descriptors();

        let tls = descriptors
            .iter()
            .find(|d| d.name == "TLS Protocol")
            .unwrap();
        assert_eq!(tls.default_value, Some("TLS 1.3"));

        let client_auth = descriptors
            .iter()
            .find(|d| d.name == "Client Auth")
            .unwrap();
        assert_eq!(client_auth.default_value, Some("NONE"));

        let keystore_type = descriptors
            .iter()
            .find(|d| d.name == "Keystore Type")
            .unwrap();
        assert_eq!(keystore_type.default_value, Some("PEM"));
    }
}
