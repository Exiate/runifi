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
      <Handle
        type="target"
        position={Position.Left}
        id="target"
        style={{ top: '50%' }}
      />

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
              className={`bulletin-dot ${bulletin.severity}`}
              title={bulletin.message}
              aria-label={`${bulletin.severity} bulletin`}
            />
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

      {/* Hidden per-relationship handles for edge anchoring */}
      {rels.map((rel, idx) => (
        <Handle
          key={rel}
          type="source"
          position={Position.Right}
          id={rel}
          className="edge-anchor-handle"
          style={{ top: handleTopPercent(idx, rels.length) }}
        />
      ))}
    </div>
  );
}

export const ProcessorNode = memo(ProcessorNodeInner);
