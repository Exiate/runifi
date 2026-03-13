use std::collections::HashMap;
use std::path::PathBuf;

use serde::Deserialize;

/// Top-level flow configuration, loaded from TOML.
#[derive(Debug, Deserialize)]
pub struct FlowConfig {
    pub flow: FlowDefinition,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub engine: EngineConfig,
}

/// Configuration for the web API server.
#[derive(Debug, Deserialize)]
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
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: default_api_enabled(),
            bind_address: default_bind_address(),
            port: default_api_port(),
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

/// Engine-level configuration.
#[derive(Debug, Default, Deserialize)]
pub struct EngineConfig {
    #[serde(default)]
    pub content_repository: ContentRepositoryConfig,
    #[serde(default)]
    pub flowfile_repository: FlowFileRepositoryConfig,
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
