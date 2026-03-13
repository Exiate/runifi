// API response types matching the Rust DTOs in runifi-api/src/dto.rs

export type ProcessorState = 'running' | 'paused' | 'stopped' | 'circuit-open';

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
  state: string;
  metrics: MetricsResponse;
}

export interface ConnectionResponse {
  id: string;
  source_name: string;
  relationship: string;
  dest_name: string;
  queued_count: number;
  queued_bytes: number;
  back_pressured: boolean;
}

export interface FlowNodeResponse {
  name: string;
  type_name: string;
}

export interface FlowEdgeResponse {
  source: string;
  relationship: string;
  destination: string;
}

export interface FlowResponse {
  name: string;
  processors: FlowNodeResponse[];
  connections: FlowEdgeResponse[];
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
