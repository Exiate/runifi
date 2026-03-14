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

/** All four sides for multi-directional handles */
const ALL_POSITIONS = [Position.Top, Position.Right, Position.Bottom, Position.Left] as const;

function FunnelNodeInner({ data }: NodeProps<FunnelNodeType>) {
  const { label, state, pending } = data;

  const stateClass = pending ? 'pending' : state;

  return (
    <div
      className={`funnel-node funnel-state-${stateClass}`}
      role="article"
      aria-label={`Funnel ${label}${pending ? ' (pending)' : ''}`}
    >
      {/* Target handles on all 4 sides */}
      {ALL_POSITIONS.map((pos) => (
        <Handle
          key={`target--${pos}`}
          type="target"
          position={pos}
          id={`target--${pos}`}
          className="funnel-handle-target edge-anchor-handle"
        />
      ))}

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

      {/* Per-relationship source handles on all 4 sides */}
      {ALL_POSITIONS.map((pos) => (
        <Handle
          key={`success--${pos}`}
          type="source"
          position={pos}
          id={`success--${pos}`}
          className="edge-anchor-handle"
        />
      ))}
    </div>
  );
}

export const FunnelNode = memo(FunnelNodeInner);
