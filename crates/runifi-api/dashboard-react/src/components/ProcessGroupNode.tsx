import { memo, useCallback } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { ProcessGroupNodeData } from '../types/flow';

function ProcessGroupNodeInner({ data }: NodeProps) {
  const d = data as ProcessGroupNodeData;

  const handleDoubleClick = useCallback(() => {
    d.onEnterGroup?.(d.groupId);
  }, [d]);

  return (
    <div
      className={`process-group-node ${d.pending ? 'node-pending' : ''}`}
      onDoubleClick={handleDoubleClick}
    >
      {/* Target handles */}
      <Handle type="target" position={Position.Top} id="target--top" className="node-handle" />
      <Handle type="target" position={Position.Left} id="target--left" className="node-handle" />
      <Handle type="target" position={Position.Bottom} id="target--bottom" className="node-handle" />
      <Handle type="target" position={Position.Right} id="target--right" className="node-handle" />

      <div className="process-group-header">
        <span className="process-group-icon" aria-hidden="true">{'\u{1F4C1}'}</span>
        <span className="process-group-name">{d.name}</span>
      </div>

      <div className="process-group-stats">
        <div className="process-group-stat">
          <span className="process-group-stat-label">Processors</span>
          <span className="process-group-stat-value">{d.processorCount}</span>
        </div>
        {d.inputPortCount > 0 && (
          <div className="process-group-stat">
            <span className="process-group-stat-label">In Ports</span>
            <span className="process-group-stat-value">{d.inputPortCount}</span>
          </div>
        )}
        {d.outputPortCount > 0 && (
          <div className="process-group-stat">
            <span className="process-group-stat-label">Out Ports</span>
            <span className="process-group-stat-value">{d.outputPortCount}</span>
          </div>
        )}
      </div>

      {/* Source handles */}
      <Handle type="source" position={Position.Top} id="success--top" className="node-handle" />
      <Handle type="source" position={Position.Right} id="success--right" className="node-handle" />
      <Handle type="source" position={Position.Bottom} id="success--bottom" className="node-handle" />
      <Handle type="source" position={Position.Left} id="success--left" className="node-handle" />
    </div>
  );
}

export const ProcessGroupNode = memo(ProcessGroupNodeInner);
