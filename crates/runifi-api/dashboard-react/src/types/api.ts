// API response types matching the Rust DTOs in runifi-api/src/dto.rs

export type ProcessorState = 'running' | 'paused' | 'stopped' | 'circuit-open' | 'invalid' | 'disabled';

export type SseStatus = 'connecting' | 'connected' | 'disconnected';

export interface MetricsResponse {
  total_invocations: number;
  total_failures: number;
  consecutive_failures: number;
  circuit_open: boolean;
  bytes_in: number;
  bytes_out: number;
  flowfiles_in: number;
  flowfiles_out: number;
  active: boolean;
  // Rolling 5-minute window totals
  flowfiles_in_5m: number;
  flowfiles_out_5m: number;
  bytes_in_5m: number;
  bytes_out_5m: number;
  // Rolling 5-minute per-second rates
  flowfiles_in_rate: number;
  flowfiles_out_rate: number;
  bytes_in_rate: number;
  bytes_out_rate: number;
}

export interface ProcessorResponse {
  name: string;
  type_name: string;
  scheduling: string;
  state: ProcessorState;
  metrics: MetricsResponse;
  validation_errors?: string[];
}

export interface ConnectionResponse {
  id: string;
  source_name: string;
  relationship: string;
  dest_name: string;
  queued_count: number;
  queued_bytes: number;
  back_pressured: boolean;
  back_pressure_object_threshold: number;
  back_pressure_bytes_threshold: number;
  fill_percentage: number;
}

export interface FlowNodeResponse {
  name: string;
  type_name: string;
  position?: { x: number; y: number };
}

export interface FlowEdgeResponse {
  source: string;
  relationship: string;
  destination: string;
}

export interface FlowLabelResponse {
  id: string;
  text: string;
  x: number;
  y: number;
  width: number;
  height: number;
  background_color: string;
  font_size: number;
}

export interface FlowProcessGroupResponse {
  id: string;
  name: string;
  processor_count: number;
  input_port_count: number;
  output_port_count: number;
  position?: { x: number; y: number };
}

export interface FlowResponse {
  name: string;
  processors: FlowNodeResponse[];
  connections: FlowEdgeResponse[];
  labels?: FlowLabelResponse[];
  process_groups?: FlowProcessGroupResponse[];
}

// ── Process group scoped flow ──────────────────────────────────────

/** Alias: same shape as FlowProcessGroupResponse. */
export type ProcessGroupSummary = FlowProcessGroupResponse;

export interface BreadcrumbSegment {
  id: string;
  name: string;
}

export interface PortSummary {
  id: string;
  name: string;
  port_type: string;
}

export interface ProcessGroupFlowResponse {
  id: string;
  name: string;
  processors: FlowNodeResponse[];
  connections: FlowEdgeResponse[];
  child_groups: ProcessGroupSummary[];
  input_ports: PortSummary[];
  output_ports: PortSummary[];
  breadcrumb: BreadcrumbSegment[];
}

export interface BulletinResponse {
  id: number;
  timestamp_ms: number;
  severity: 'warn' | 'error';
  processor_name: string;
  message: string;
}

export interface SseMetricsEvent {
  uptime_secs: number;
  processors: ProcessorResponse[];
  connections: ConnectionResponse[];
  bulletins: BulletinResponse[];
}

export interface MemoryInfo {
  resident_bytes: number;
  virtual_bytes: number;
  total_system_bytes: number;
  used_system_bytes: number;
}

export interface CpuInfo {
  process_cpu_percent: number;
  available_cores: number;
}

export interface ContentRepoInfo {
  entry_count: number;
  total_bytes: number;
  storage_type: string;
}

export interface FlowFileRepoInfo {
  entry_count: number;
  storage_bytes: number;
  storage_type: string;
}

export interface ProvenanceRepoInfo {
  event_count: number;
}

export interface RepositoryStorageInfo {
  content: ContentRepoInfo;
  flowfile: FlowFileRepoInfo;
  provenance: ProvenanceRepoInfo;
}

export interface RuntimeInfo {
  total_flowfiles_processed: number;
  total_bytes_processed: number;
}

export interface ConnectionsSummary {
  total_queued_flowfiles: number;
  total_queued_bytes: number;
  back_pressured_count: number;
}

export interface SystemResponse {
  flow_name: string;
  uptime_secs: number;
  version: string;
  processor_count: number;
  connection_count: number;
  memory: MemoryInfo;
  cpu: CpuInfo;
  repositories: RepositoryStorageInfo;
  runtime: RuntimeInfo;
  connections_summary: ConnectionsSummary;
}

// Plugin/processor type registry (GET /api/v1/plugins)
export type PluginKind = 'processor' | 'source' | 'sink' | 'service';

export interface PluginDescriptor {
  type_name: string;
  display_name?: string;
  description?: string;
  kind: PluginKind;
  relationships?: string[];
  properties?: PropertyDescriptor[];
  tags?: string[];
}

export interface PropertyDescriptor {
  name: string;
  display_name: string;
  description: string;
  default_value: string | null;
  required: boolean;
}

export interface PluginsResponse {
  plugins: PluginDescriptor[];
}

// CRUD request bodies
export interface CreateProcessorRequest {
  type: string;
  name: string;
  position: { x: number; y: number };
  properties: Record<string, string>;
}

export interface CreateConnectionRequest {
  source: string;
  relationship: string;
  destination: string;
}

export interface UpdatePositionRequest {
  x: number;
  y: number;
}

// ── Processor config ───────────────────────────────────────────────

export interface PropertyDescriptorFull {
  name: string;
  display_name: string;
  description: string;
  default_value: string | null;
  required: boolean;
  sensitive: boolean;
  allowed_values: string[] | null;
  expression_language_supported: boolean;
}

export interface RelationshipDescriptor {
  name: string;
  description: string;
  auto_terminated: boolean;
}

export interface SchedulingConfig {
  strategy: string;
  interval_ms: number | null;
  concurrent_tasks: number;
  execution_node: string;
}

export interface ProcessorConfigResponse {
  processor_name: string;
  type_name: string;
  properties: Record<string, string>;
  property_descriptors: PropertyDescriptorFull[];
  scheduling: SchedulingConfig;
  relationships: RelationshipDescriptor[];
  penalty_duration_ms: number;
  yield_duration_ms: number;
  bulletin_level: string;
  concurrent_tasks: number;
  comments: string;
  auto_terminated_relationships: string[];
  supports_dynamic_properties: boolean;
  supports_sensitive_dynamic_properties: boolean;
}

// ── Queue inspection ───────────────────────────────────────────────

export interface FlowFileEntry {
  id: number;
  position: number;
  size: number;
  age_ms: number;
  has_content: boolean;
  attributes: Array<{ key: string; value: string }>;
}

export interface QueueResponse {
  total_count: number;
  offset: number;
  limit: number;
  flowfiles: FlowFileEntry[];
}

// ── Controller service types ──────────────────────────────────────

export interface ServicePropertyDescriptor {
  name: string;
  description: string;
  required: boolean;
  default_value: string | null;
  sensitive: boolean;
}

export interface ServiceResponse {
  name: string;
  type_name: string;
  state: string;
  properties: Record<string, string>;
  property_descriptors: ServicePropertyDescriptor[];
  referencing_processors: string[];
}

export interface CreateServiceRequest {
  type: string;
  name: string;
  properties: Record<string, string>;
}

export interface UpdateServiceConfigRequest {
  properties: Record<string, string>;
}

// ── Data provenance types ─────────────────────────────────────

export interface ProvenanceAttribute {
  key: string;
  value: string;
}

export interface ProvenanceEvent {
  event_id: number;
  flowfile_id: number;
  event_type: string;
  processor_name: string;
  processor_type: string;
  timestamp_nanos: number;
  timestamp_ms: number;
  attributes: ProvenanceAttribute[];
  content_size: number;
  lineage_start_id: number;
  relationship?: string;
  source_flowfile_id?: number;
  details: string;
  parent_flowfile_ids: number[];
  child_flowfile_ids: number[];
  transit_uri?: string;
  content_claim_id?: number;
  previous_attributes: ProvenanceAttribute[];
}

export interface ProvenanceSearchResponse {
  events: ProvenanceEvent[];
  total_count: number;
  offset: number;
  max_results: number;
}

export interface ProvenanceLineageResponse {
  flowfile_id: number;
  lineage_start_id: number;
  events: ProvenanceEvent[];
}

export interface ProvenanceReplayResponse {
  status: string;
  event_id: number;
  flowfile_id: number;
  processor_name: string;
  message: string;
}

export interface ProvenanceStatsResponse {
  event_count: number;
  oldest_timestamp_ms?: number;
  newest_timestamp_ms?: number;
}

// ── Cluster management types ──────────────────────────────────

export type NodeState =
  | 'Connected'
  | 'Connecting'
  | 'Disconnected'
  | 'Decommissioning'
  | 'Removed';

export type ClusterRole = 'Primary' | 'Coordinator' | 'Node';

export interface NodeMetricsSummary {
  active_threads: number;
  queued_flowfiles: number;
  queued_bytes: number;
}

export interface ClusterNodeResponse {
  id: string;
  address: string;
  state: NodeState;
  roles: ClusterRole[];
  missed_heartbeats: number;
  flow_version: number;
  metrics?: NodeMetricsSummary;
  uptime_secs?: number;
}

export interface ClusterNodesResponse {
  nodes: ClusterNodeResponse[];
  connected_count: number;
  total_count: number;
  has_quorum: boolean;
}

export interface ClusterStatusResponse {
  enabled: boolean;
  node_id: string;
  state: NodeState;
  roles: ClusterRole[];
  connected_count: number;
  total_count: number;
  flow_version: number;
  election_term: number;
  has_quorum: boolean;
  coordinator_id?: string;
  primary_id?: string;
}
