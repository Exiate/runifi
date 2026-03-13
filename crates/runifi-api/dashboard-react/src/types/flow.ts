// React Flow node and edge data types
// Note: @xyflow/react requires data types to satisfy Record<string, unknown>

import type { MetricsResponse, BulletinResponse } from './api';

export interface ProcessorNodeData extends Record<string, unknown> {
  label: string;
  typeName: string;
  state: string;
  metrics: MetricsResponse | null;
  bulletin: BulletinResponse | null;
}

export interface ConnectionEdgeData extends Record<string, unknown> {
  relationship: string;
  queuedCount: number;
  queuedBytes: number;
  backPressured: boolean;
  connectionId: string;
}
