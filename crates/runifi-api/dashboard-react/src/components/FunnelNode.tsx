import { memo } from 'react';
import { Handle, Position, type Node, type NodeProps } from '@xyflow/react';
import type { ProcessorNodeData } from '../types/flow';

/**
 * FunnelNode renders a NiFi-style funnel on the canvas.
 * Funnels are backed by a regular Funnel processor on the engine side,
 * but rendered with a distinctive trapezoid/funnel shape instead of
 * the standard processor box.
 */
export type FunnelNodeType = Node<ProcessorNodeData, 'funnelNode'>;

function FunnelNodeInner({ data }: NodeProps<FunnelNodeType>) {
  const { label, state, pending } = data;

  const stateClass = pending ? 'pending' : state;

  return (
    <div
      className={`funnel-node funnel-state-${stateClass}`}
      role="article"
      aria-label={`Funnel ${label}${pending ? ' (pending)' : ''}`}
    >
      <Handle
        type="target"
        position={Position.Top}
        id="target"
        className="funnel-handle-target"
      />

      {/* NiFi-style center connection handle */}
      <Handle
        type="source"
        position={Position.Bottom}
        id="center-source"
        className="center-connect-handle"
      />

      {/* Funnel SVG shape */}
      <svg
        className="funnel-shape"
        viewBox="0 0 60 60"
        width="60"
        height="60"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        <path
          d="M5 10 L55 10 L40 50 L20 50 Z"
          className="funnel-path"
        />
      </svg>

      <span className="funnel-label" title={label}>{label}</span>

      {/* Hidden per-relationship handle for edge anchoring */}
      <Handle
        type="source"
        position={Position.Bottom}
        id="success"
        className="edge-anchor-handle"
        style={{ bottom: 0 }}
      />
    </div>
  );
}

export const FunnelNode = memo(FunnelNodeInner);
