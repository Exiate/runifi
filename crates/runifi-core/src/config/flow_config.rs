use std::collections::HashMap;

use serde::Deserialize;

/// Top-level flow configuration, loaded from TOML.
#[derive(Debug, Deserialize)]
pub struct FlowConfig {
    pub flow: FlowDefinition,
    #[serde(default)]
    pub api: ApiConfig,
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
