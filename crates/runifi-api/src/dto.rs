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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub validation_errors: Vec<String>,
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
        let validation_errors = snapshot.validation_errors.clone();
        Self {
            name: info.name.clone(),
            type_name: info.type_name.clone(),
            scheduling: info.scheduling_display.clone(),
            state,
            metrics: snapshot.into(),
            validation_errors,
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
    /// Load balance strategy in effect for this connection.
    /// Omitted from JSON when `None` (no load balancing).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balance_strategy: Option<String>,
    /// Partition attribute for `partition_by_attribute` strategy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balance_partition_attribute: Option<String>,
    /// Whether compression is enabled for cross-node transfer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balance_compression: Option<bool>,
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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub labels: Vec<FlowLabelResponse>,
    /// Number of process groups in the flow.
    #[serde(skip_serializing_if = "is_zero")]
    pub process_group_count: usize,
}

fn is_zero(v: &usize) -> bool {
    *v == 0
}

/// A label in the flow topology response.
#[derive(Serialize)]
pub struct FlowLabelResponse {
    pub id: String,
    pub text: String,
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
    pub background_color: String,
    pub font_size: f64,
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
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

impl PluginResponse {
    pub fn from_kind(type_name: &str, kind: PluginKind, tags: Vec<String>) -> Self {
        let kind_str = match kind {
            PluginKind::Processor => "processor",
            PluginKind::Source => "source",
            PluginKind::Sink => "sink",
            PluginKind::Service => "service",
            PluginKind::ReportingTask => "reporting_task",
        };
        Self {
            type_name: type_name.to_string(),
            kind: kind_str.to_string(),
            tags,
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
        let (strategy, interval_ms) =
            if let Some(rest) = info.scheduling_display.strip_prefix("timer-driven (") {
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
    /// Load balance strategy: "do_not_load_balance" (default), "round_robin",
    /// "partition_by_attribute", or "single_node".
    #[serde(default)]
    pub load_balance_strategy: Option<String>,
    /// Attribute name for `partition_by_attribute` strategy.
    #[serde(default)]
    pub load_balance_partition_attribute: Option<String>,
    /// Whether to compress FlowFile content during inter-node transfer.
    #[serde(default)]
    pub load_balance_compression: Option<bool>,
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
    /// Load balance strategy in effect for this connection.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balance_strategy: Option<String>,
    /// Partition attribute for `partition_by_attribute` strategy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balance_partition_attribute: Option<String>,
    /// Whether compression is enabled for cross-node transfer.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub load_balance_compression: Option<bool>,
}

// ── Controller service DTOs ────────────────────────────────────────────

/// Response for a controller service instance.
#[derive(Serialize)]
pub struct ServiceResponse {
    pub name: String,
    pub type_name: String,
    pub state: String,
    pub properties: HashMap<String, String>,
    pub property_descriptors: Vec<ServicePropertyDescriptorResponse>,
    pub referencing_processors: Vec<String>,
}

/// Property descriptor for a controller service.
#[derive(Serialize)]
pub struct ServicePropertyDescriptorResponse {
    pub name: String,
    pub description: String,
    pub required: bool,
    pub default_value: Option<String>,
    pub sensitive: bool,
}

/// Request body for `POST /api/v1/services`.
#[derive(Deserialize)]
pub struct CreateServiceRequest {
    /// Service type name (must exist in PluginRegistry).
    #[serde(rename = "type")]
    pub type_name: String,
    /// Unique name for this service instance.
    pub name: String,
    /// Initial property values.
    #[serde(default)]
    pub properties: HashMap<String, String>,
}

/// Request body for `PUT /api/v1/services/{name}/config`.
#[derive(Deserialize)]
pub struct UpdateServiceConfigRequest {
    pub properties: HashMap<String, String>,
}

// ── Reporting task DTOs ───────────────────────────────────────────────

/// Response for a reporting task instance.
#[derive(Serialize)]
pub struct ReportingTaskResponse {
    pub name: String,
    pub type_name: String,
    pub state: String,
    pub scheduling: String,
    pub properties: HashMap<String, String>,
    pub property_descriptors: Vec<ReportingTaskPropertyDescriptorResponse>,
    pub metrics: ReportingTaskMetricsResponse,
}

/// Property descriptor for a reporting task.
#[derive(Serialize)]
pub struct ReportingTaskPropertyDescriptorResponse {
    pub name: String,
    pub description: String,
    pub required: bool,
    pub default_value: Option<String>,
    pub sensitive: bool,
}

/// Metrics snapshot for a reporting task.
#[derive(Serialize)]
pub struct ReportingTaskMetricsResponse {
    pub total_invocations: u64,
    pub total_failures: u64,
    pub consecutive_failures: u64,
    pub circuit_open: bool,
}

/// Request body for `POST /api/v1/reporting-tasks`.
#[derive(Deserialize)]
pub struct CreateReportingTaskRequest {
    /// Reporting task type name (must exist in PluginRegistry).
    #[serde(rename = "type")]
    pub type_name: String,
    /// Unique name for this reporting task instance.
    pub name: String,
    /// Initial property values.
    #[serde(default)]
    pub properties: HashMap<String, String>,
    /// Scheduling strategy: "timer" or "cron".
    #[serde(default = "default_scheduling_strategy")]
    pub scheduling_strategy: String,
    /// Interval in ms for timer-driven scheduling. Default: 30000.
    #[serde(default = "default_reporting_interval")]
    pub interval_ms: u64,
    /// CRON expression for cron-driven scheduling (reserved for future use).
    #[serde(default)]
    #[allow(dead_code)]
    pub cron_expression: Option<String>,
}

fn default_reporting_interval() -> u64 {
    30_000
}

/// Request body for `PUT /api/v1/reporting-tasks/{name}/config`.
#[derive(Deserialize)]
pub struct UpdateReportingTaskConfigRequest {
    pub properties: HashMap<String, String>,
}

// ── Provenance DTOs ───────────────────────────────────────────────────

/// Response for a provenance event.
#[derive(Serialize)]
pub struct ProvenanceEventResponse {
    pub event_id: u64,
    pub flowfile_id: u64,
    pub event_type: String,
    pub processor_name: String,
    pub processor_type: String,
    pub timestamp_nanos: u64,
    pub timestamp_ms: u64,
    pub attributes: Vec<ProvenanceAttributeResponse>,
    pub content_size: u64,
    pub lineage_start_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub relationship: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_flowfile_id: Option<u64>,
    pub details: String,
}

/// Attribute snapshot in a provenance event.
#[derive(Serialize)]
pub struct ProvenanceAttributeResponse {
    pub key: String,
    pub value: String,
}

/// Paginated response for provenance search.
#[derive(Serialize)]
pub struct ProvenanceSearchResponse {
    pub events: Vec<ProvenanceEventResponse>,
    pub total_count: usize,
    pub offset: usize,
    pub max_results: usize,
}

/// Lineage graph response.
#[derive(Serialize)]
pub struct ProvenanceLineageResponse {
    pub flowfile_id: u64,
    pub lineage_start_id: u64,
    pub events: Vec<ProvenanceEventResponse>,
}

/// Query parameters for provenance search.
#[derive(Deserialize, Default)]
pub struct ProvenanceSearchParams {
    /// Filter by FlowFile ID.
    pub flowfile_id: Option<u64>,
    /// Filter by processor name.
    pub processor: Option<String>,
    /// Filter by event type (CREATE, SEND, RECEIVE, etc.).
    pub event_type: Option<String>,
    /// Filter events after this timestamp (milliseconds since epoch).
    pub start_time: Option<u64>,
    /// Filter events before this timestamp (milliseconds since epoch).
    pub end_time: Option<u64>,
    /// Maximum results (default 100).
    pub max_results: Option<usize>,
    /// Pagination offset (default 0).
    pub offset: Option<usize>,
}

/// Response for provenance replay.
#[derive(Serialize)]
pub struct ProvenanceReplayResponse {
    pub status: String,
    pub event_id: u64,
    pub flowfile_id: u64,
    pub processor_name: String,
    pub message: String,
}

impl ProvenanceEventResponse {
    pub fn from_event(event: &runifi_core::repository::provenance_repo::ProvenanceEvent) -> Self {
        Self {
            event_id: event.event_id,
            flowfile_id: event.flowfile_id,
            event_type: event.event_type.as_str().to_string(),
            processor_name: event.processor_name.clone(),
            processor_type: event.processor_type.clone(),
            timestamp_nanos: event.timestamp_nanos,
            timestamp_ms: event.timestamp_nanos / 1_000_000,
            attributes: event
                .attributes
                .iter()
                .map(|(k, v)| ProvenanceAttributeResponse {
                    key: k.clone(),
                    value: v.clone(),
                })
                .collect(),
            content_size: event.content_size,
            lineage_start_id: event.lineage_start_id,
            relationship: event.relationship.clone(),
            source_flowfile_id: event.source_flowfile_id,
            details: event.details.clone(),
        }
    }
}

// ── Label DTOs ────────────────────────────────────────────────────────

/// Response for a canvas label.
#[derive(Serialize)]
pub struct LabelResponse {
    pub id: String,
    pub text: String,
    pub x: f64,
    pub y: f64,
    pub width: f64,
    pub height: f64,
    pub background_color: String,
    pub font_size: f64,
}

/// Request body for `POST /api/v1/process-groups/root/labels`.
#[derive(Deserialize)]
pub struct CreateLabelRequest {
    /// Label text content.
    #[serde(default)]
    pub text: String,
    /// Canvas X position.
    #[serde(default)]
    pub x: f64,
    /// Canvas Y position.
    #[serde(default)]
    pub y: f64,
    /// Label width in pixels.
    #[serde(default = "default_label_width")]
    pub width: f64,
    /// Label height in pixels.
    #[serde(default = "default_label_height")]
    pub height: f64,
    /// Background colour (CSS hex string).
    #[serde(default)]
    pub background_color: String,
    /// Font size in pixels.
    #[serde(default = "default_font_size")]
    pub font_size: f64,
}

fn default_label_width() -> f64 {
    150.0
}
fn default_label_height() -> f64 {
    40.0
}
fn default_font_size() -> f64 {
    14.0
}

/// Request body for `PUT /api/v1/process-groups/root/labels/{id}`.
#[derive(Deserialize)]
pub struct UpdateLabelRequest {
    pub text: Option<String>,
    pub x: Option<f64>,
    pub y: Option<f64>,
    pub width: Option<f64>,
    pub height: Option<f64>,
    pub background_color: Option<String>,
    pub font_size: Option<f64>,
}

// ── Flow versioning DTOs ──────────────────────────────────────────────

/// Response for a single flow version.
#[derive(Serialize)]
pub struct FlowVersionResponse {
    pub id: String,
    pub full_id: String,
    pub comment: String,
    pub timestamp: i64,
    pub processor_count: usize,
    pub connection_count: usize,
}

/// Response for listing all flow versions.
#[derive(Serialize)]
pub struct FlowVersionListResponse {
    pub versions: Vec<FlowVersionResponse>,
    pub total: usize,
}

/// Request body for `POST /api/v1/flow/versions` — save a new version.
#[derive(Deserialize)]
pub struct SaveVersionRequest {
    /// Version comment / description.
    pub comment: String,
}

/// Response for a flow diff between the current state and a version.
#[derive(Serialize)]
pub struct FlowDiffResponse {
    pub version_id: String,
    pub processors_added: Vec<String>,
    pub processors_removed: Vec<String>,
    pub processors_changed: Vec<FlowDiffEntryResponse>,
    pub connections_added: Vec<String>,
    pub connections_removed: Vec<String>,
    pub services_added: Vec<String>,
    pub services_removed: Vec<String>,
    pub services_changed: Vec<FlowDiffEntryResponse>,
}

/// A single changed component in a diff.
#[derive(Serialize)]
pub struct FlowDiffEntryResponse {
    pub name: String,
    pub changes: Vec<String>,
}

/// Response after reverting to a version.
#[derive(Serialize)]
pub struct RevertResponse {
    pub reverted_to: String,
    pub comment: String,
    pub message: String,
}
