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

export interface ProcessGroupSummary {
  id: string;
  name: string;
  processor_count: number;
  input_port_count: number;
  output_port_count: number;
  position?: { x: number; y: number };
}

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

export interface SystemResponse {
  flow_name: string;
  uptime_secs: number;
  version: string;
  processor_count: number;
  connection_count: number;
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
