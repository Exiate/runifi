use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

use crate::cluster::config::ClusterConfig;
use crate::cluster::load_balance::LoadBalanceConfig;

/// Top-level flow configuration, loaded from TOML.
#[derive(Debug, Default, Deserialize)]
pub struct FlowConfig {
    #[serde(default)]
    pub flow: FlowDefinition,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub engine: EngineConfig,
    #[serde(default)]
    pub audit: AuditConfig,
    #[serde(default)]
    pub auth: AuthConfig,
    #[serde(default)]
    pub cluster: ClusterConfig,
}

/// Configuration for the web API server.
#[derive(Debug, Deserialize, Clone)]
pub struct ApiConfig {
    /// Whether the API is enabled.
    #[serde(default = "default_api_enabled")]
    pub enabled: bool,
    /// Bind address for the API server.
    #[serde(default = "default_bind_address")]
    pub bind_address: String,
    /// Port for the API server.
    #[serde(default = "default_api_port")]
    pub port: u16,
    /// CORS allowed origins. Empty = same-origin only (no CORS headers sent).
    #[serde(default)]
    pub cors_allowed_origins: Vec<String>,
    /// Maximum request body size in bytes. Default: 1MB.
    #[serde(default = "default_max_request_body_bytes")]
    pub max_request_body_bytes: usize,
    /// Rate limit: maximum requests per second per client IP. Default: 100.
    #[serde(default = "default_rate_limit_per_second")]
    pub rate_limit_per_second: u32,
    /// Maximum concurrent SSE connections. Default: 50.
    #[serde(default = "default_max_sse_connections")]
    pub max_sse_connections: usize,
    /// Whether to include detailed error messages (e.g. processor names).
    /// Default: false (sanitized errors).
    #[serde(default)]
    pub detailed_errors: bool,
    /// Content encryption at rest configuration.
    #[serde(default)]
    pub encryption: Option<EncryptionConfig>,
    /// Security configuration (API key auth, TLS).
    #[serde(default)]
    pub security: SecurityConfig,
}

/// Security configuration for API authentication and TLS.
///
/// Supports two formats for `api_keys`:
///
/// **Simple format** (backward compatible — all keys get `Admin` role):
/// ```toml
/// [api.security]
/// api_keys = ["key-abc123", "key-def456"]
/// ```
///
/// **Role-based format**:
/// ```toml
/// [api.security]
/// [[api.security.api_keys]]
/// key = "admin-key-abc123"
/// role = "admin"
///
/// [[api.security.api_keys]]
/// key = "operator-key-def456"
/// role = "operator"
///
/// [[api.security.api_keys]]
/// key = "viewer-key-ghi789"
/// role = "viewer"
/// ```
#[derive(Debug, Default, Deserialize, Clone)]
pub struct SecurityConfig {
    /// API keys for bearer-token authentication. If empty, auth is disabled.
    /// Supports both simple string keys (all Admin) and structured key-role mappings.
    #[serde(default)]
    pub api_keys: Vec<ApiKeyEntry>,
    /// Whether TLS is enabled for the API server.
    #[serde(default)]
    pub tls_enabled: bool,
    /// Path to the TLS certificate file (PEM format).
    #[serde(default)]
    pub tls_cert_path: Option<String>,
    /// Path to the TLS private key file (PEM format).
    #[serde(default)]
    pub tls_key_path: Option<String>,
}

impl SecurityConfig {
    /// Returns `true` if API key authentication is enabled (at least one key configured).
    pub fn auth_enabled(&self) -> bool {
        !self.api_keys.is_empty()
    }

    /// Get the plain key strings for authentication validation.
    pub fn key_strings(&self) -> Vec<&str> {
        self.api_keys
            .iter()
            .map(|entry| match entry {
                ApiKeyEntry::Simple(key) => key.as_str(),
                ApiKeyEntry::WithRole(akr) => akr.key.as_str(),
            })
            .collect()
    }

    /// Look up the role name for a given key. Returns `None` if the key is not found.
    /// Simple string keys return `"admin"`.
    pub fn role_for_key(&self, provided: &str) -> Option<&str> {
        for entry in &self.api_keys {
            match entry {
                ApiKeyEntry::Simple(key) if key == provided => return Some("admin"),
                ApiKeyEntry::WithRole(akr) if akr.key == provided => return Some(&akr.role),
                _ => {}
            }
        }
        None
    }
}

/// An API key entry that supports both simple string keys (backward compatible)
/// and structured key-role mappings.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum ApiKeyEntry {
    /// Simple string key — treated as Admin role for backward compatibility.
    Simple(String),
    /// Structured key with an explicit role assignment.
    WithRole(ApiKeyWithRole),
}

/// A structured API key with an explicit role assignment.
#[derive(Debug, Clone, Deserialize)]
pub struct ApiKeyWithRole {
    /// The API key string.
    pub key: String,
    /// The role assigned to this key (e.g. "admin", "operator", "viewer").
    pub role: String,
}

/// Configuration for content encryption at rest.
///
/// When enabled, all content stored in the `ContentRepository` is encrypted
/// with AES-256-GCM. The key must be a hex-encoded 256-bit key (64 hex chars).
///
/// ```toml
/// [api.encryption]
/// enabled = true
/// key = "0123456789abcdef..."  # 64 hex chars
/// key_id = "key-2024-01"
/// ```
#[derive(Debug, Deserialize, Clone)]
pub struct EncryptionConfig {
    /// Whether encryption is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Hex-encoded 256-bit encryption key (64 hex characters).
    pub key: String,
    /// Identifier for this key, used in the encrypted envelope for key rotation.
    pub key_id: String,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: default_api_enabled(),
            bind_address: default_bind_address(),
            port: default_api_port(),
            cors_allowed_origins: Vec::new(),
            max_request_body_bytes: default_max_request_body_bytes(),
            rate_limit_per_second: default_rate_limit_per_second(),
            max_sse_connections: default_max_sse_connections(),
            detailed_errors: false,
            encryption: None,
            security: SecurityConfig::default(),
        }
    }
}

fn default_api_enabled() -> bool {
    true
}

fn default_bind_address() -> String {
    "127.0.0.1".to_string()
}

fn default_api_port() -> u16 {
    8080
}

fn default_max_request_body_bytes() -> usize {
    1_048_576 // 1 MB
}

fn default_rate_limit_per_second() -> u32 {
    100
}

fn default_max_sse_connections() -> usize {
    50
}

#[derive(Debug, Deserialize)]
pub struct FlowDefinition {
    #[serde(default = "default_flow_name")]
    pub name: String,
    #[serde(default)]
    pub processors: Vec<ProcessorConfig>,
    #[serde(default)]
    pub connections: Vec<ConnectionConfig>,
    #[serde(default)]
    pub services: Vec<ServiceConfig>,
    /// Process Groups for hierarchical flow organization.
    #[serde(default)]
    pub process_groups: Vec<ProcessGroupConfig>,
}

impl Default for FlowDefinition {
    fn default() -> Self {
        Self {
            name: default_flow_name(),
            processors: Vec::new(),
            connections: Vec::new(),
            services: Vec::new(),
            process_groups: Vec::new(),
        }
    }
}

/// Configuration for a Process Group — a hierarchical container for
/// processors, connections, ports, and child process groups.
///
/// ```toml
/// [[flow.process_groups]]
/// name = "data-enrichment"
///
/// [flow.process_groups.input_ports]
/// ports = ["raw-data"]
///
/// [flow.process_groups.output_ports]
/// ports = ["enriched-data"]
///
/// [[flow.process_groups.processors]]
/// name = "lookup"
/// type = "InvokeHTTP"
///
/// [[flow.process_groups.connections]]
/// source = "raw-data"
/// relationship = "success"
/// destination = "lookup"
///
/// [flow.process_groups.variables]
/// "api.url" = "https://example.com"
/// ```
#[derive(Debug, Deserialize)]
pub struct ProcessGroupConfig {
    /// Unique name for this process group.
    pub name: String,
    /// Input ports that receive FlowFiles from the parent group.
    #[serde(default)]
    pub input_ports: Option<PortsConfig>,
    /// Output ports that send FlowFiles to the parent group.
    #[serde(default)]
    pub output_ports: Option<PortsConfig>,
    /// Processors within this process group.
    #[serde(default)]
    pub processors: Vec<ProcessorConfig>,
    /// Connections within this process group.
    #[serde(default)]
    pub connections: Vec<ConnectionConfig>,
    /// Nested child process groups.
    #[serde(default)]
    pub process_groups: Vec<ProcessGroupConfig>,
    /// Group-scoped variables. Child groups inherit and can override.
    #[serde(default)]
    pub variables: HashMap<String, String>,
}

/// Port name list for Process Group input or output ports.
#[derive(Debug, Deserialize)]
pub struct PortsConfig {
    /// List of port names.
    pub ports: Vec<String>,
}

/// Configuration for a controller service instance in the flow.
///
/// ```toml
/// [[flow.services]]
/// name = "my-cache"
/// type = "DistributedMapCacheServer"
/// [flow.services.properties]
/// "Port" = "4557"
/// "Maximum Cache Entries" = "10000"
/// ```
#[derive(Debug, Deserialize)]
pub struct ServiceConfig {
    /// Unique instance name.
    pub name: String,
    /// Service type name — must match a registered `ControllerServiceDescriptor`.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Service-specific properties.
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

fn default_flow_name() -> String {
    "default-flow".to_string()
}

#[derive(Debug, Deserialize)]
pub struct ProcessorConfig {
    /// Unique instance name (e.g. "generate-test-data").
    pub name: String,
    /// Processor type name (e.g. "GenerateFlowFile") — must match a registered plugin.
    #[serde(rename = "type")]
    pub type_name: String,
    /// Scheduling strategy.
    #[serde(default)]
    pub scheduling: SchedulingConfig,
    /// Processor-specific properties.
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct SchedulingConfig {
    /// "timer", "event", or "cron".
    #[serde(default = "default_strategy")]
    pub strategy: String,
    /// Interval in milliseconds (for timer-driven).
    #[serde(default = "default_interval")]
    pub interval_ms: u64,
    /// CRON expression (for cron-driven), e.g. "0 */5 * * * *".
    #[serde(default)]
    pub expression: Option<String>,
}

impl Default for SchedulingConfig {
    fn default() -> Self {
        Self {
            strategy: default_strategy(),
            interval_ms: default_interval(),
            expression: None,
        }
    }
}

fn default_strategy() -> String {
    "timer".to_string()
}

fn default_interval() -> u64 {
    100
}

#[derive(Debug, Deserialize)]
pub struct ConnectionConfig {
    /// Source processor instance name.
    pub source: String,
    /// Relationship name from source.
    pub relationship: String,
    /// Destination processor instance name.
    pub destination: String,
    /// Back-pressure config (optional).
    #[serde(default)]
    pub back_pressure: Option<BackPressureConfigToml>,
    /// Load balancing config for cluster distribution (optional).
    #[serde(default)]
    pub load_balancing: Option<LoadBalanceConfig>,
    /// FlowFile expiration duration string, e.g. "5m", "1h", "30s".
    /// FlowFiles older than this are dropped from the queue.
    #[serde(default)]
    pub expiration: Option<String>,
    /// Queue priority strategy: "FIFO" (default), "NewestFirst", "PriorityAttribute".
    #[serde(default)]
    pub priority: Option<String>,
    /// Attribute name used for PriorityAttribute ordering.
    #[serde(default)]
    pub priority_attribute: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct BackPressureConfigToml {
    pub max_count: Option<usize>,
    pub max_bytes: Option<u64>,
}

/// Parse a human-readable duration string into seconds.
///
/// Supported suffixes: `s` (seconds), `m` (minutes), `h` (hours), `d` (days).
/// Examples: "30s", "5m", "1h", "2d".
pub fn parse_duration_str(s: &str) -> Option<std::time::Duration> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (num_str, multiplier) = if let Some(n) = s.strip_suffix('d') {
        (n, 86400u64)
    } else if let Some(n) = s.strip_suffix('h') {
        (n, 3600u64)
    } else if let Some(n) = s.strip_suffix('m') {
        (n, 60u64)
    } else if let Some(n) = s.strip_suffix('s') {
        (n, 1u64)
    } else {
        // Default to seconds if no suffix.
        (s, 1u64)
    };
    let num: u64 = num_str.trim().parse().ok()?;
    Some(std::time::Duration::from_secs(num * multiplier))
}

/// Engine-level configuration.
#[derive(Debug, Default, Deserialize)]
pub struct EngineConfig {
    /// Directory for runtime flow state persistence.
    /// Default: `./data/conf/`
    #[serde(default = "default_conf_dir")]
    pub conf_dir: PathBuf,
    #[serde(default)]
    pub content_repository: ContentRepositoryConfig,
    #[serde(default)]
    pub flowfile_repository: FlowFileRepositoryConfig,
    /// Provenance repository configuration.
    ///
    /// ```toml
    /// [engine.provenance]
    /// repo_type = "file"
    /// directory = "./data/provenance"
    /// retention_days = 7
    /// max_storage_gb = 5
    /// segment_size_mb = 100
    /// ```
    #[serde(default)]
    pub provenance: ProvenanceRepositoryConfig,
    /// Repository encryption at rest configuration.
    ///
    /// When enabled, encrypts both the content repository and the FlowFile
    /// WAL repository data at rest using AES-256-GCM.
    ///
    /// ```toml
    /// [engine.encryption]
    /// enabled = true
    /// algorithm = "AES-256-GCM"
    /// [engine.encryption.key_provider]
    /// type = "file"
    /// path = "/etc/runifi/keys.json"
    /// ```
    #[serde(default)]
    pub encryption: Option<RepositoryEncryptionConfig>,
}

/// Provenance repository type selection and configuration.
///
/// ```toml
/// [engine.provenance]
/// repo_type = "file"
/// directory = "./data/provenance"
/// retention_days = 7
/// max_storage_gb = 5
/// max_events = 1000000
/// segment_size_mb = 100
/// ```
#[derive(Debug, Deserialize)]
pub struct ProvenanceRepositoryConfig {
    /// "memory" (default) or "file".
    #[serde(default = "default_repo_type")]
    pub repo_type: String,
    /// Directory for provenance segment files (when repo_type = "file").
    #[serde(default = "default_provenance_dir")]
    pub directory: PathBuf,
    /// Retention period in days. Default: 1.
    #[serde(default = "default_provenance_retention_days")]
    pub retention_days: u32,
    /// Max total storage in GB. Default: 1.
    #[serde(default = "default_provenance_max_storage_gb")]
    pub max_storage_gb: u64,
    /// Max events in memory (for memory repo). Default: 1,000,000.
    #[serde(default = "default_provenance_max_events")]
    pub max_events: usize,
    /// Segment size in MB (for file repo). Default: 100.
    #[serde(default = "default_provenance_segment_size_mb")]
    pub segment_size_mb: u64,
}

impl Default for ProvenanceRepositoryConfig {
    fn default() -> Self {
        Self {
            repo_type: default_repo_type(),
            directory: default_provenance_dir(),
            retention_days: default_provenance_retention_days(),
            max_storage_gb: default_provenance_max_storage_gb(),
            max_events: default_provenance_max_events(),
            segment_size_mb: default_provenance_segment_size_mb(),
        }
    }
}

fn default_provenance_dir() -> PathBuf {
    PathBuf::from("./data/provenance")
}

fn default_provenance_retention_days() -> u32 {
    1
}

fn default_provenance_max_storage_gb() -> u64 {
    1
}

fn default_provenance_max_events() -> usize {
    1_000_000
}

fn default_provenance_segment_size_mb() -> u64 {
    100
}

/// Configuration for encrypted repositories (content + FlowFile WAL).
///
/// Supports multiple key provider types for key management:
/// - `"file"` — reads keys from a JSON file on disk
/// - `"env"` — reads keys from environment variables
/// - `"static"` — single key from config (backward compatible)
///
/// All providers support key rotation: new writes use the active key,
/// old content can still be decrypted using the key ID embedded in the
/// encrypted envelope.
#[derive(Debug, Deserialize, Clone)]
pub struct RepositoryEncryptionConfig {
    /// Whether encryption is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Encryption algorithm. Currently only "AES-256-GCM" is supported.
    #[serde(default = "default_algorithm")]
    pub algorithm: String,
    /// Key provider configuration.
    #[serde(default)]
    pub key_provider: KeyProviderConfig,
}

fn default_algorithm() -> String {
    "AES-256-GCM".to_string()
}

/// Key provider configuration for repository encryption.
///
/// ```toml
/// # File-based key provider
/// [engine.encryption.key_provider]
/// type = "file"
/// path = "/etc/runifi/keys.json"
///
/// # Environment variable key provider
/// [engine.encryption.key_provider]
/// type = "env"
/// active_key_id = "key-2024-01"
/// key_ids = ["key-2024-01", "key-2023-12"]
/// key_env_prefix = "RUNIFI_ENC_KEY_"
///
/// # Static key provider (single key, for simple setups)
/// [engine.encryption.key_provider]
/// type = "static"
/// key = "hex-encoded-256-bit-key"
/// key_id = "key-2024-01"
/// ```
#[derive(Debug, Deserialize, Clone)]
pub struct KeyProviderConfig {
    /// Key provider type: "file", "env", or "static".
    #[serde(rename = "type", default = "default_key_provider_type")]
    pub provider_type: String,
    /// Path to the key file (for "file" provider).
    pub path: Option<String>,
    /// Active key ID (for "env" provider).
    pub active_key_id: Option<String>,
    /// List of key IDs to load (for "env" provider).
    pub key_ids: Option<Vec<String>>,
    /// Environment variable prefix (for "env" provider). Default: "RUNIFI_ENC_KEY_".
    #[serde(default = "default_key_env_prefix")]
    pub key_env_prefix: String,
    /// Hex-encoded key (for "static" provider).
    pub key: Option<String>,
    /// Key identifier (for "static" provider).
    pub key_id: Option<String>,
}

impl Default for KeyProviderConfig {
    fn default() -> Self {
        Self {
            provider_type: default_key_provider_type(),
            path: None,
            active_key_id: None,
            key_ids: None,
            key_env_prefix: default_key_env_prefix(),
            key: None,
            key_id: None,
        }
    }
}

fn default_key_provider_type() -> String {
    "static".to_string()
}

fn default_key_env_prefix() -> String {
    "RUNIFI_ENC_KEY_".to_string()
}

fn default_conf_dir() -> PathBuf {
    PathBuf::from("./data/conf")
}

/// Content repository type selection.
#[derive(Debug, Deserialize)]
pub struct ContentRepositoryConfig {
    /// "memory" (default) or "file"
    #[serde(default = "default_repo_type")]
    pub repo_type: String,
    /// File-based repository settings (only used when repo_type = "file").
    pub file: Option<FileRepoConfigToml>,
}

impl Default for ContentRepositoryConfig {
    fn default() -> Self {
        Self {
            repo_type: default_repo_type(),
            file: None,
        }
    }
}

fn default_repo_type() -> String {
    "memory".to_string()
}

/// TOML configuration for the file-based content repository.
#[derive(Debug, Deserialize)]
pub struct FileRepoConfigToml {
    /// Container directories for segment files.
    pub containers: Vec<PathBuf>,
    /// Max size of a single segment file in bytes (default: 128MB).
    #[serde(default = "default_max_segment_size")]
    pub max_segment_size_bytes: u64,
    /// Memory threshold before eviction starts (default: 256MB).
    #[serde(default = "default_memory_threshold")]
    pub memory_threshold_bytes: u64,
    /// Content <= this size stays in memory (default: 64KB).
    #[serde(default = "default_inline_threshold")]
    pub inline_threshold_bytes: u64,
    /// Background cleanup interval in seconds (default: 30).
    #[serde(default = "default_cleanup_interval")]
    pub cleanup_interval_secs: u64,
}

fn default_max_segment_size() -> u64 {
    128 * 1024 * 1024
}

fn default_memory_threshold() -> u64 {
    256 * 1024 * 1024
}

fn default_inline_threshold() -> u64 {
    64 * 1024
}

fn default_cleanup_interval() -> u64 {
    30
}

/// FlowFile repository type selection.
#[derive(Debug, Deserialize)]
pub struct FlowFileRepositoryConfig {
    /// "memory" (default) or "wal"
    #[serde(default = "default_repo_type")]
    pub repo_type: String,
    /// WAL repository settings (only used when repo_type = "wal").
    pub wal: Option<WalRepoConfigToml>,
}

impl Default for FlowFileRepositoryConfig {
    fn default() -> Self {
        Self {
            repo_type: default_repo_type(),
            wal: None,
        }
    }
}

/// TOML configuration for the WAL-based FlowFile repository.
#[derive(Debug, Deserialize)]
pub struct WalRepoConfigToml {
    /// Directory for WAL and checkpoint files.
    pub dir: PathBuf,
    /// fsync mode: "always" (default) or "never".
    #[serde(default = "default_fsync_mode")]
    pub fsync_mode: String,
    /// Checkpoint interval in seconds (default: 120).
    #[serde(default = "default_checkpoint_interval")]
    pub checkpoint_interval_secs: u64,
}

fn default_fsync_mode() -> String {
    "always".to_string()
}

fn default_checkpoint_interval() -> u64 {
    120
}

/// Configuration for the structured audit trail.
#[derive(Debug, Deserialize)]
pub struct AuditConfig {
    /// Whether audit logging is enabled at all.
    #[serde(default = "default_audit_enabled")]
    pub enabled: bool,
    /// Path for the JSON-lines audit log file. If `None`, file logging is disabled.
    #[serde(default)]
    pub file_path: Option<String>,
    /// Whether to also emit audit events via the `tracing` framework.
    #[serde(default = "default_audit_log_to_tracing")]
    pub log_to_tracing: bool,
}

impl Default for AuditConfig {
    fn default() -> Self {
        Self {
            enabled: default_audit_enabled(),
            file_path: None,
            log_to_tracing: default_audit_log_to_tracing(),
        }
    }
}

fn default_audit_enabled() -> bool {
    true
}

fn default_audit_log_to_tracing() -> bool {
    true
}

/// Configuration for user management and JWT authentication.
///
/// Supports pluggable authentication providers: `"local"` (default, password+JWT),
/// `"oidc"` (OpenID Connect), `"ldap"` (LDAP/LDAPS), `"mtls"` (client certificates),
/// and `"chain"` (ordered list of providers).
///
/// ```toml
/// [auth]
/// enabled = true
/// provider = "local"
/// jwt_secret = "${RUNIFI_JWT_SECRET}"
/// jwt_expiry_secs = 3600
/// ```
#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    /// Whether JWT-based user authentication is enabled.
    /// When disabled, all requests bypass user auth (existing API key auth still applies).
    #[serde(default)]
    pub enabled: bool,
    /// Authentication provider type: `"local"` (default), `"oidc"`, `"ldap"`,
    /// `"mtls"`, or `"chain"` (ordered list).
    #[serde(default = "default_auth_provider")]
    pub provider: String,
    /// Single-user mode: auto-create a default admin account on first boot
    /// if no users exist. Intended for development and testing.
    #[serde(default = "default_single_user_mode")]
    pub single_user_mode: bool,
    /// HMAC secret for signing JWT tokens. **Must** be set when auth is enabled.
    /// Use env var substitution: `"${RUNIFI_JWT_SECRET}"`.
    #[serde(default = "default_jwt_secret")]
    pub jwt_secret: String,
    /// JWT token lifetime in seconds. Default: 3600 (1 hour).
    #[serde(default = "default_jwt_expiry")]
    pub jwt_expiry_secs: i64,
    /// Default admin username for single-user mode.
    #[serde(default = "default_admin_username")]
    pub default_admin_username: String,
    /// Default admin password for single-user mode.
    /// **Change this in production.**
    #[serde(default = "default_admin_password")]
    pub default_admin_password: String,
    /// OIDC provider configuration (used when `provider = "oidc"` or in chain).
    #[serde(default)]
    pub oidc: Option<crate::auth::oidc_provider::OidcConfig>,
    /// LDAP provider configuration (used when `provider = "ldap"` or in chain).
    #[serde(default)]
    pub ldap: Option<crate::auth::ldap_provider::LdapConfig>,
    /// mTLS provider configuration (used when `provider = "mtls"` or in chain).
    #[serde(default)]
    pub mtls: Option<crate::auth::mtls_provider::MtlsConfig>,
    /// Provider chain order (used when `provider = "chain"`).
    /// Example: `["mtls", "oidc", "local"]`.
    #[serde(default)]
    pub chain_order: Vec<String>,
    /// Group-to-role mapping for external providers.
    /// Maps identity provider group names to RuniFi roles.
    #[serde(default)]
    pub group_mapping: HashMap<String, String>,
    /// Default role when no group mapping matches.
    #[serde(default = "default_role")]
    pub default_role: String,
    /// Maximum concurrent sessions per user. `None` = unlimited.
    #[serde(default)]
    pub max_sessions_per_user: Option<usize>,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: default_auth_provider(),
            single_user_mode: default_single_user_mode(),
            jwt_secret: default_jwt_secret(),
            jwt_expiry_secs: default_jwt_expiry(),
            default_admin_username: default_admin_username(),
            default_admin_password: default_admin_password(),
            oidc: None,
            ldap: None,
            mtls: None,
            chain_order: Vec::new(),
            group_mapping: HashMap::new(),
            default_role: default_role(),
            max_sessions_per_user: None,
        }
    }
}

fn default_auth_provider() -> String {
    "local".to_string()
}

fn default_role() -> String {
    "viewer".to_string()
}

fn default_single_user_mode() -> bool {
    true
}

fn default_jwt_secret() -> String {
    "change-me-in-production".to_string()
}

fn default_jwt_expiry() -> i64 {
    3600
}

fn default_admin_username() -> String {
    "admin".to_string()
}

fn default_admin_password() -> String {
    "admin".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_flow_config_has_empty_flow() {
        let config = FlowConfig::default();
        assert_eq!(config.flow.name, "default-flow");
        assert!(config.flow.processors.is_empty());
        assert!(config.flow.connections.is_empty());
        assert!(config.api.enabled);
        assert_eq!(config.api.port, 8080);
    }

    #[test]
    fn empty_toml_deserializes_to_empty_flow() {
        let config: FlowConfig = toml::from_str("").unwrap();
        assert_eq!(config.flow.name, "default-flow");
        assert!(config.flow.processors.is_empty());
        assert!(config.flow.connections.is_empty());
    }

    #[test]
    fn minimal_toml_with_flow_name_only() {
        let toml_str = r#"
            [flow]
            name = "my-flow"
        "#;
        let config: FlowConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.flow.name, "my-flow");
        assert!(config.flow.processors.is_empty());
        assert!(config.flow.connections.is_empty());
    }

    #[test]
    fn process_group_config_deserializes() {
        let toml_str = r#"
            [flow]
            name = "grouped-flow"

            [[flow.process_groups]]
            name = "data-enrichment"

            [flow.process_groups.input_ports]
            ports = ["raw-data"]

            [flow.process_groups.output_ports]
            ports = ["enriched-data"]

            [flow.process_groups.variables]
            "api.url" = "https://example.com"

            [[flow.process_groups.processors]]
            name = "lookup"
            type = "GenerateFlowFile"

            [[flow.process_groups.connections]]
            source = "raw-data"
            relationship = "success"
            destination = "lookup"
        "#;

        let config: FlowConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.flow.process_groups.len(), 1);

        let pg = &config.flow.process_groups[0];
        assert_eq!(pg.name, "data-enrichment");

        let input_ports = pg.input_ports.as_ref().unwrap();
        assert_eq!(input_ports.ports, vec!["raw-data"]);

        let output_ports = pg.output_ports.as_ref().unwrap();
        assert_eq!(output_ports.ports, vec!["enriched-data"]);

        assert_eq!(pg.processors.len(), 1);
        assert_eq!(pg.processors[0].name, "lookup");

        assert_eq!(pg.connections.len(), 1);
        assert_eq!(pg.connections[0].source, "raw-data");

        assert_eq!(pg.variables.get("api.url").unwrap(), "https://example.com");
    }

    #[test]
    fn default_config_has_empty_process_groups() {
        let config = FlowConfig::default();
        assert!(config.flow.process_groups.is_empty());
    }

    #[test]
    fn nested_process_groups_deserialize() {
        let toml_str = r#"
            [flow]
            name = "nested-flow"

            [[flow.process_groups]]
            name = "outer-group"

            [[flow.process_groups.process_groups]]
            name = "inner-group"

            [flow.process_groups.process_groups.input_ports]
            ports = ["in"]
        "#;

        let config: FlowConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.flow.process_groups.len(), 1);

        let outer = &config.flow.process_groups[0];
        assert_eq!(outer.name, "outer-group");
        assert_eq!(outer.process_groups.len(), 1);

        let inner = &outer.process_groups[0];
        assert_eq!(inner.name, "inner-group");
        assert_eq!(inner.input_ports.as_ref().unwrap().ports, vec!["in"]);
    }

    #[test]
    fn connection_with_expiration_and_priority() {
        let toml_str = r#"
            [flow]
            name = "test-flow"

            [[flow.processors]]
            name = "gen"
            type = "GenerateFlowFile"

            [[flow.processors]]
            name = "log"
            type = "LogAttribute"

            [[flow.connections]]
            source = "gen"
            relationship = "success"
            destination = "log"
            expiration = "5m"
            priority = "NewestFirst"
        "#;
        let config: FlowConfig = toml::from_str(toml_str).unwrap();
        let conn = &config.flow.connections[0];
        assert_eq!(conn.expiration.as_deref(), Some("5m"));
        assert_eq!(conn.priority.as_deref(), Some("NewestFirst"));
    }

    #[test]
    fn cron_scheduling_config() {
        let toml_str = r#"
            [flow]
            name = "test-flow"

            [[flow.processors]]
            name = "batch"
            type = "GenerateFlowFile"
            [flow.processors.scheduling]
            strategy = "cron"
            expression = "0 */5 * * * *"
        "#;
        let config: FlowConfig = toml::from_str(toml_str).unwrap();
        let proc = &config.flow.processors[0];
        assert_eq!(proc.scheduling.strategy, "cron");
        assert_eq!(proc.scheduling.expression.as_deref(), Some("0 */5 * * * *"));
    }

    #[test]
    fn parse_duration_str_variants() {
        assert_eq!(
            parse_duration_str("30s"),
            Some(std::time::Duration::from_secs(30))
        );
        assert_eq!(
            parse_duration_str("5m"),
            Some(std::time::Duration::from_secs(300))
        );
        assert_eq!(
            parse_duration_str("2h"),
            Some(std::time::Duration::from_secs(7200))
        );
        assert_eq!(
            parse_duration_str("1d"),
            Some(std::time::Duration::from_secs(86400))
        );
        assert_eq!(
            parse_duration_str("60"),
            Some(std::time::Duration::from_secs(60))
        );
        assert_eq!(parse_duration_str(""), None);
        assert_eq!(parse_duration_str("abc"), None);
    }
}
