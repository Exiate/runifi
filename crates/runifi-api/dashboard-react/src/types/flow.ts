// React Flow node and edge data types
// Note: @xyflow/react requires data types to satisfy Record<string, unknown>

import type { Position } from '@xyflow/react';
import type { MetricsResponse, BulletinResponse } from './api';

export interface ProcessorNodeData extends Record<string, unknown> {
  label: string;
  typeName: string;
  state: string;
  metrics: MetricsResponse | null;
  bulletin: BulletinResponse | null;
  // Relationships this processor can produce (drives source handle count)
  relationships: string[];
  // True when created optimistically before the backend confirms
  pending: boolean;
  // Custom color for visual grouping (hex string, empty = default)
  customColor: string;
}

export interface LabelNodeData extends Record<string, unknown> {
  labelId: string;
  text: string;
  width: number;
  height: number;
  backgroundColor: string;
  fontSize: number;
  pending: boolean;
}

export interface ConnectionEdgeData extends Record<string, unknown> {
  relationship: string;
  queuedCount: number;
  queuedBytes: number;
  backPressured: boolean;
  connectionId: string;
  // True when created optimistically before the backend confirms
  pending: boolean;
  // Optional callback injected from FlowCanvas to open the queue inspector
  onQueueClick?: (connectionId: string, label: string) => void;
  // Smart routing: dynamic handle positions based on node geometry
  smartSourcePosition?: Position;
  smartTargetPosition?: Position;
}
