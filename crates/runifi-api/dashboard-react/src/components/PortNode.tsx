import { memo } from 'react';
import { Handle, Position, type NodeProps } from '@xyflow/react';
import type { PortNodeData } from '../types/flow';

function PortNodeInner({ data }: NodeProps) {
  const d = data as PortNodeData;
  const isInput = d.portType === 'input';

  return (
    <div className={`port-node ${isInput ? 'port-node-input' : 'port-node-output'} ${d.pending ? 'node-pending' : ''}`}>
      {isInput ? (
        <>
          {/* Input port: data comes from parent, flows right into the group */}
          <Handle type="target" position={Position.Left} id="target--left" className="node-handle" />
          <div className="port-node-shape" aria-hidden="true">{'\u25B6'}</div>
          <span className="port-node-name">{d.name}</span>
          <Handle type="source" position={Position.Right} id="success--right" className="node-handle" />
        </>
      ) : (
        <>
          {/* Output port: data flows left out of the group to parent */}
          <Handle type="target" position={Position.Left} id="target--left" className="node-handle" />
          <span className="port-node-name">{d.name}</span>
          <div className="port-node-shape" aria-hidden="true">{'\u25C0'}</div>
          <Handle type="source" position={Position.Right} id="success--right" className="node-handle" />
        </>
      )}
    </div>
  );
}

export const PortNode = memo(PortNodeInner);
