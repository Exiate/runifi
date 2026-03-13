import { memo } from 'react';
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

// Distribute multiple source handles evenly along the right edge
function handleTopPercent(index: number, total: number): string {
  if (total <= 1) return '50%';
  const step = 100 / (total + 1);
  return `${step * (index + 1)}%`;
}

function ProcessorNodeInner({ data }: NodeProps<ProcessorNodeType>) {
  const { label, typeName, state, metrics, bulletin, relationships, pending } = data;
  const borderColor = pending ? 'var(--warning)' : stateColor(state);
  const rels = relationships && relationships.length > 0 ? relationships : ['success'];

  return (
    <div
      className={`processor-node${pending ? ' processor-node-pending' : ''}`}
      style={{ borderColor }}
      role="article"
      aria-label={`Processor ${label}${pending ? ' (pending)' : ''}`}
    >
      {/* Target handle — left side, centered */}
      <Handle
        type="target"
        position={Position.Left}
        id="target"
        style={{ top: '50%' }}
      />

      <div className="proc-node-header">
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

      {/* Source handles — right side, one per relationship */}
      {rels.map((rel, idx) => (
        <Handle
          key={rel}
          type="source"
          position={Position.Right}
          id={rel}
          style={{ top: handleTopPercent(idx, rels.length) }}
          title={rel}
        />
      ))}
    </div>
  );
}

export const ProcessorNode = memo(ProcessorNodeInner);
