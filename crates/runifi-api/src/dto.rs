use serde::Serialize;

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
        use runifi_core::engine::processor_node::SchedulingStrategy;
        let scheduling = match &info.scheduling {
            SchedulingStrategy::TimerDriven { interval_ms } => {
                format!("timer ({}ms)", interval_ms)
            }
            SchedulingStrategy::EventDriven => "event".to_string(),
        };
        let snapshot = info.metrics.snapshot();
        let state = snapshot.state.as_str().to_string();
        Self {
            name: info.name.clone(),
            type_name: info.type_name.clone(),
            scheduling,
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
}

#[derive(Serialize)]
pub struct FlowEdgeResponse {
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
}
