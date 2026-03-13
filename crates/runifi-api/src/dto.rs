use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use runifi_core::engine::bulletin::Bulletin;
use runifi_core::engine::handle::{PluginKind, ProcessorInfo};
use runifi_core::engine::metrics::MetricsSnapshot;

#[derive(Serialize)]
pub struct SystemResponse {
    pub flow_name: String,
    pub uptime_secs: u64,
    pub version: String,
    pub processor_count: usize,
    pub connection_count: usize,
}

#[derive(Serialize)]
pub struct ProcessorResponse {
    pub name: String,
    pub type_name: String,
    pub scheduling: String,
    pub state: String,
    pub metrics: MetricsResponse,
}

#[derive(Serialize)]
pub struct MetricsResponse {
    // Lifetime totals.
    pub total_invocations: u64,
    pub total_failures: u64,
    pub consecutive_failures: u64,
    pub circuit_open: bool,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub flowfiles_in: u64,
    pub flowfiles_out: u64,
    pub active: bool,
    // Rolling 5-minute window totals.
    pub flowfiles_in_5m: u64,
    pub flowfiles_out_5m: u64,
    pub bytes_in_5m: u64,
    pub bytes_out_5m: u64,
    // Rolling 5-minute per-second rates.
    pub flowfiles_in_rate: f64,
    pub flowfiles_out_rate: f64,
    pub bytes_in_rate: f64,
    pub bytes_out_rate: f64,
}

impl From<MetricsSnapshot> for MetricsResponse {
    fn from(s: MetricsSnapshot) -> Self {
        Self {
            total_invocations: s.total_invocations,
            total_failures: s.total_failures,
            consecutive_failures: s.consecutive_failures,
            circuit_open: s.circuit_open,
            bytes_in: s.bytes_in,
            bytes_out: s.bytes_out,
            flowfiles_in: s.flowfiles_in,
            flowfiles_out: s.flowfiles_out,
            active: s.active,
            flowfiles_in_5m: s.rolling.flowfiles_in_5m,
            flowfiles_out_5m: s.rolling.flowfiles_out_5m,
            bytes_in_5m: s.rolling.bytes_in_5m,
            bytes_out_5m: s.rolling.bytes_out_5m,
            flowfiles_in_rate: s.rolling.flowfiles_in_rate,
            flowfiles_out_rate: s.rolling.flowfiles_out_rate,
            bytes_in_rate: s.rolling.bytes_in_rate,
            bytes_out_rate: s.rolling.bytes_out_rate,
        }
    }
}

impl ProcessorResponse {
    pub fn from_info(info: &ProcessorInfo) -> Self {
        let snapshot = info.metrics.snapshot();
        let state = snapshot.state.as_str().to_string();
        Self {
            name: info.name.clone(),
            type_name: info.type_name.clone(),
            scheduling: info.scheduling_display.clone(),
            state,
            metrics: snapshot.into(),
        }
    }
}

#[derive(Serialize)]
pub struct ConnectionResponse {
    pub id: String,
    pub source_name: String,
    pub relationship: String,
    pub dest_name: String,
    pub queued_count: usize,
    pub queued_bytes: u64,
    pub back_pressured: bool,
}

// ── Canvas position ────────────────────────────────────────────────────

/// Canvas position for a processor node.
#[derive(Serialize, Deserialize, Debug, Clone, Copy)]
pub struct PositionResponse {
    pub x: f64,
    pub y: f64,
}

// ── Flow topology ─────────────────────────────────────────────────────

#[derive(Serialize)]
pub struct FlowResponse {
    pub name: String,
    pub processors: Vec<FlowNodeResponse>,
    pub connections: Vec<FlowEdgeResponse>,
}

#[derive(Serialize)]
pub struct FlowNodeResponse {
    pub name: String,
    pub type_name: String,
    pub position: Option<PositionResponse>,
}

#[derive(Serialize)]
pub struct FlowEdgeResponse {
    pub id: String,
    pub source: String,
    pub relationship: String,
    pub destination: String,
}

#[derive(Serialize)]
pub struct PluginResponse {
    pub type_name: String,
    pub kind: String,
}

impl PluginResponse {
    pub fn from_kind(type_name: &str, kind: PluginKind) -> Self {
        let kind_str = match kind {
            PluginKind::Processor => "processor",
            PluginKind::Source => "source",
            PluginKind::Sink => "sink",
        };
        Self {
            type_name: type_name.to_string(),
            kind: kind_str.to_string(),
        }
    }
}

/// A bulletin response for the API.
#[derive(Serialize)]
pub struct BulletinResponse {
    pub id: u64,
    pub timestamp_ms: u64,
    pub severity: String,
    pub processor_name: String,
    pub message: String,
}

impl From<Bulletin> for BulletinResponse {
    fn from(b: Bulletin) -> Self {
        Self {
            id: b.id,
            timestamp_ms: b.timestamp_ms,
            severity: b.severity.as_str().to_string(),
            processor_name: b.processor_name,
            message: b.message,
        }
    }
}

/// A queued FlowFile as returned by the queue inspection API.
#[derive(Serialize)]
pub struct QueuedFlowFileResponse {
    pub id: u64,
    pub attributes: Vec<FlowFileAttributeResponse>,
    pub size: u64,
    pub age_ms: u64,
    pub has_content: bool,
    pub position: usize,
}

/// A single FlowFile attribute key-value pair.
#[derive(Serialize)]
pub struct FlowFileAttributeResponse {
    pub key: String,
    pub value: String,
}

/// Paginated response for queue listing.
#[derive(Serialize)]
pub struct QueueListingResponse {
    pub connection_id: String,
    pub total_count: usize,
    pub offset: usize,
    pub limit: usize,
    pub flowfiles: Vec<QueuedFlowFileResponse>,
}

/// SSE event payload — all metrics in one event.
#[derive(Serialize)]
pub struct SseMetricsEvent {
    pub uptime_secs: u64,
    pub processors: Vec<ProcessorResponse>,
    pub connections: Vec<ConnectionResponse>,
    pub bulletins: Vec<BulletinResponse>,
}

// ── Processor configuration DTOs ─────────────────────────────────────

/// Response for `GET /api/v1/processors/{name}/config`.
#[derive(Serialize)]
pub struct ProcessorConfigResponse {
    pub processor_name: String,
    pub type_name: String,
    pub properties: HashMap<String, String>,
    pub property_descriptors: Vec<PropertyDescriptorResponse>,
    pub scheduling: SchedulingResponse,
    pub relationships: Vec<RelationshipResponse>,
}

#[derive(Serialize)]
pub struct PropertyDescriptorResponse {
    pub name: String,
    pub description: String,
    pub required: bool,
    pub default_value: Option<String>,
    pub sensitive: bool,
    pub allowed_values: Option<Vec<String>>,
}

#[derive(Serialize)]
pub struct SchedulingResponse {
    pub strategy: String,
    pub interval_ms: Option<u64>,
}

#[derive(Serialize)]
pub struct RelationshipResponse {
    pub name: String,
    pub description: String,
    pub auto_terminated: bool,
}

/// Request body for `PUT /api/v1/processors/{name}/config`.
#[derive(Deserialize)]
pub struct ProcessorConfigUpdateRequest {
    #[serde(default)]
    pub properties: Option<HashMap<String, String>>,
}

impl ProcessorConfigResponse {
    pub fn from_info(info: &ProcessorInfo) -> Self {
        // Parse the display string to extract strategy/interval for the config response.
        // Format is "timer-driven (Nms)" or "event-driven".
        let (strategy, interval_ms) = if let Some(rest) =
            info.scheduling_display.strip_prefix("timer-driven (")
        {
            let ms_str = rest.trim_end_matches("ms)");
            let interval = ms_str.parse::<u64>().unwrap_or(1000);
            ("timer".to_string(), Some(interval))
        } else {
            ("event".to_string(), None)
        };

        let raw_properties = info.properties.read().clone();

        // Build a set of sensitive property names for masking.
        let sensitive_names: std::collections::HashSet<&str> = info
            .property_descriptors
            .iter()
            .filter(|pd| pd.sensitive)
            .map(|pd| pd.name.as_str())
            .collect();

        // Mask sensitive property values in the response.
        let properties: HashMap<String, String> = raw_properties
            .into_iter()
            .map(|(k, v)| {
                if sensitive_names.contains(k.as_str()) && !v.is_empty() {
                    (k, "********".to_string())
                } else {
                    (k, v)
                }
            })
            .collect();

        let property_descriptors = info
            .property_descriptors
            .iter()
            .map(|pd| PropertyDescriptorResponse {
                name: pd.name.clone(),
                description: pd.description.clone(),
                required: pd.required,
                default_value: pd.default_value.clone(),
                sensitive: pd.sensitive,
                allowed_values: pd.allowed_values.clone(),
            })
            .collect();

        let relationships = info
            .relationships
            .iter()
            .map(|r| RelationshipResponse {
                name: r.name.clone(),
                description: r.description.clone(),
                auto_terminated: r.auto_terminated,
            })
            .collect();

        Self {
            processor_name: info.name.clone(),
            type_name: info.type_name.clone(),
            properties,
            property_descriptors,
            scheduling: SchedulingResponse {
                strategy,
                interval_ms,
            },
            relationships,
        }
    }
}

// ── CRUD request/response DTOs ─────────────────────────────────────────

/// Request body for `POST /api/v1/processors`.
#[derive(Deserialize)]
pub struct CreateProcessorRequest {
    /// Processor type name (must exist in PluginRegistry).
    #[serde(rename = "type")]
    pub type_name: String,
    /// Unique name for this processor instance.
    pub name: String,
    /// Optional canvas position.
    pub position: Option<PositionResponse>,
    /// Initial property values (optional; defaults applied by the processor).
    #[serde(default)]
    pub properties: HashMap<String, String>,
    /// Scheduling strategy: "timer" (default) or "event".
    #[serde(default = "default_scheduling_strategy")]
    pub scheduling_strategy: String,
    /// Scheduling interval in milliseconds (for "timer" strategy).
    #[serde(default = "default_interval_ms")]
    pub interval_ms: u64,
}

fn default_scheduling_strategy() -> String {
    "timer".to_string()
}

fn default_interval_ms() -> u64 {
    1000
}

/// Response for a created or retrieved processor (includes relationships).
#[derive(Serialize)]
pub struct ProcessorDetailResponse {
    pub name: String,
    pub type_name: String,
    pub state: String,
    pub scheduling: String,
    pub position: Option<PositionResponse>,
    pub relationships: Vec<RelationshipResponse>,
    pub properties: HashMap<String, String>,
}

/// Request body for `PUT /api/v1/processors/{name}/position`.
#[derive(Deserialize)]
pub struct UpdatePositionRequest {
    pub x: f64,
    pub y: f64,
}

/// Request body for `POST /api/v1/connections`.
#[derive(Deserialize)]
pub struct CreateConnectionRequest {
    pub source: String,
    pub relationship: String,
    pub destination: String,
    /// Maximum FlowFiles in queue before back-pressure (default: 10 000).
    pub max_queue_size: Option<usize>,
    /// Maximum bytes in queue before back-pressure (default: 100 MB).
    pub max_queue_bytes: Option<u64>,
}

/// Response for a created or retrieved connection.
#[derive(Serialize)]
pub struct ConnectionDetailResponse {
    pub id: String,
    pub source_name: String,
    pub relationship: String,
    pub dest_name: String,
    pub queued_count: usize,
    pub queued_bytes: u64,
    pub back_pressured: bool,
}
