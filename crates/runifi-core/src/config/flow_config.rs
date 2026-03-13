use std::collections::HashMap;

use serde::Deserialize;

/// Top-level flow configuration, loaded from TOML.
#[derive(Debug, Deserialize)]
pub struct FlowConfig {
    pub flow: FlowDefinition,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub audit: AuditConfig,
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
        }
    }
}

fn default_api_enabled() -> bool {
    true
}

fn default_bind_address() -> String {
    "0.0.0.0".to_string()
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
    pub name: String,
    #[serde(default)]
    pub processors: Vec<ProcessorConfig>,
    #[serde(default)]
    pub connections: Vec<ConnectionConfig>,
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
    /// "timer" or "event".
    #[serde(default = "default_strategy")]
    pub strategy: String,
    /// Interval in milliseconds (for timer-driven).
    #[serde(default = "default_interval")]
    pub interval_ms: u64,
}

impl Default for SchedulingConfig {
    fn default() -> Self {
        Self {
            strategy: default_strategy(),
            interval_ms: default_interval(),
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
}

#[derive(Debug, Deserialize)]
pub struct BackPressureConfigToml {
    pub max_count: Option<usize>,
    pub max_bytes: Option<u64>,
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
