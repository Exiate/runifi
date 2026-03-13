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

function ProcessorNodeInner({ data }: NodeProps<ProcessorNodeType>) {
  const { label, typeName, state, metrics, bulletin } = data;
  const borderColor = stateColor(state);

  return (
    <div
      className="processor-node"
      style={{ borderColor }}
      role="article"
      aria-label={`Processor ${label}`}
    >
      <Handle type="target" position={Position.Left} />

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
          <span className={stateBadgeClass(state)}>{stateLabel(state)}</span>
          {bulletin && (
            <span
              className={`bulletin-dot ${bulletin.severity}`}
              title={bulletin.message}
              aria-label={`${bulletin.severity} bulletin`}
            />
          )}
        </div>
      </div>

      {metrics && (
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

      <Handle type="source" position={Position.Right} />
    </div>
  );
}

export const ProcessorNode = memo(ProcessorNodeInner);
