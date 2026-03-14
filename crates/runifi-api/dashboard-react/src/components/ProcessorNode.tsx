import { memo, type CSSProperties } from 'react';
import { Handle, Position, type Node, type NodeProps } from '@xyflow/react';
import type { ProcessorNodeData } from '../types/flow';
import { stateColor } from '../utils/format';

export type ProcessorNodeType = Node<ProcessorNodeData, 'processorNode'>;

function stateBadgeClass(state: string): string {
  switch (state) {
    case 'running':
      return 'state-badge running';
    case 'paused':
      return 'state-badge paused';
    case 'circuit-open':
      return 'state-badge circuit-open';
    case 'invalid':
      return 'state-badge invalid';
    case 'disabled':
      return 'state-badge disabled';
    default:
      return 'state-badge stopped';
  }
}

function stateLabel(state: string): string {
  if (state === 'circuit-open') return 'Circuit Open';
  return state.charAt(0).toUpperCase() + state.slice(1);
}

function handleTopPercent(index: number, total: number): string {
  if (total <= 1) return '50%';
  const step = 100 / (total + 1);
  return `${step * (index + 1)}%`;
}

/** All four sides for multi-directional handles */
const ALL_POSITIONS = [Position.Top, Position.Right, Position.Bottom, Position.Left] as const;

function ProcessorNodeInner({ data }: NodeProps<ProcessorNodeType>) {
  const { label, typeName, state, metrics, bulletin, relationships, pending, customColor } = data;
  const borderColor: string = pending
    ? 'var(--warning)'
    : (customColor || stateColor(state));
  const rels = relationships && relationships.length > 0 ? relationships : ['success'];

  const headerStyle: CSSProperties | undefined = customColor
    ? { borderBottomColor: customColor as string, borderBottomWidth: '2px', borderBottomStyle: 'solid' }
    : undefined;

  return (
    <div
      className={`processor-node${pending ? ' processor-node-pending' : ''}`}
      style={{ borderColor }}
      role="article"
      aria-label={`Processor ${label}${pending ? ' (pending)' : ''}`}
    >
      {/* Target handles on all 4 sides */}
      {ALL_POSITIONS.map((pos) => (
        <Handle
          key={`target--${pos}`}
          type="target"
          position={pos}
          id={`target--${pos}`}
          className="edge-anchor-handle"
        />
      ))}

      {/* NiFi-style center connection handle — visible on hover */}
      <Handle
        type="source"
        position={Position.Right}
        id="center-source"
        className="center-connect-handle"
      />
      <div className="center-connect-indicator" aria-label="Drag to connect">
        <svg width="16" height="16" viewBox="0 0 16 16" fill="none" xmlns="http://www.w3.org/2000/svg">
          <path d="M3 8H13M13 8L9 4M13 8L9 12" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
        </svg>
      </div>

      <div className="proc-node-header" style={headerStyle}>
        <div className="proc-node-identity">
          <span className="proc-node-name" title={label}>
            {label}
          </span>
          <span className="proc-node-type" title={typeName}>
            {typeName}
          </span>
        </div>
        <div className="proc-node-badges">
          {pending ? (
            <span className="state-badge paused" title="Pending API confirmation">
              Pending
            </span>
          ) : (
            <span className={stateBadgeClass(state)}>{stateLabel(state)}</span>
          )}
          {bulletin && (
            <span
              className={`bulletin-indicator ${bulletin.severity}`}
              title={bulletin.message}
              aria-label={`${bulletin.severity} bulletin: ${bulletin.message}`}
            >
              <svg width="14" height="14" viewBox="0 0 14 14" fill="currentColor" aria-hidden="true">
                <path d="M2 1h10l-2 2v8L2 11V1z" />
                <path d="M10 1l2 2h-2V1z" opacity="0.6" />
              </svg>
            </span>
          )}
        </div>
      </div>

      {metrics && !pending && (
        <div className="proc-node-metrics">
          <div className="proc-node-metric">
            <span className="proc-node-metric-label">In</span>
            <span className="proc-node-metric-value">
              {metrics.flowfiles_in.toLocaleString()}
            </span>
          </div>
          <div className="proc-node-metric">
            <span className="proc-node-metric-label">Out</span>
            <span className="proc-node-metric-value">
              {metrics.flowfiles_out.toLocaleString()}
            </span>
          </div>
          <div className="proc-node-metric">
            <span className="proc-node-metric-label">Invocations</span>
            <span className="proc-node-metric-value">
              {metrics.total_invocations.toLocaleString()}
            </span>
          </div>
          <div className="proc-node-metric">
            <span className="proc-node-metric-label">Failures</span>
            <span
              className={`proc-node-metric-value${metrics.total_failures > 0 ? ' danger' : ''}`}
            >
              {metrics.total_failures.toLocaleString()}
            </span>
          </div>
        </div>
      )}

      {/* Per-relationship source handles on all 4 sides */}
      {rels.map((rel, idx) =>
        ALL_POSITIONS.map((pos) => (
          <Handle
            key={`${rel}--${pos}`}
            type="source"
            position={pos}
            id={`${rel}--${pos}`}
            className="edge-anchor-handle"
            style={pos === Position.Right || pos === Position.Left
              ? { top: handleTopPercent(idx, rels.length) }
              : undefined
            }
          />
        ))
      )}
    </div>
  );
}

export const ProcessorNode = memo(ProcessorNodeInner);
