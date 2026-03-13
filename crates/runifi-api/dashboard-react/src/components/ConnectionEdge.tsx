import { memo } from 'react';
import {
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  type Edge,
  type EdgeProps,
} from '@xyflow/react';
import type { ConnectionEdgeData } from '../types/flow';

export type ConnectionEdgeType = Edge<ConnectionEdgeData, 'connectionEdge'>;

function ConnectionEdgeInner({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  data,
}: EdgeProps<ConnectionEdgeType>) {
  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition,
    targetX,
    targetY,
    targetPosition,
  });

  const queuedCount = data?.queuedCount ?? 0;
  const backPressured = data?.backPressured ?? false;
  const relationship = data?.relationship ?? '';

  const edgeColor = backPressured ? 'var(--danger)' : 'var(--border)';

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        style={{ stroke: edgeColor, strokeWidth: 2 }}
      />
      <EdgeLabelRenderer>
        {/* position/transform must stay inline — required by React Flow's EdgeLabelRenderer */}
        <div
          style={{
            position: 'absolute',
            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
          }}
          className={`conn-edge-label nodrag nopan${backPressured ? ' back-pressured' : ''}`}
          aria-label={`Connection ${relationship} — ${queuedCount} queued`}
        >
          <span className="conn-edge-rel-badge">{relationship}</span>
          {queuedCount > 0 && (
            <span className={`conn-edge-queue-badge${backPressured ? ' back-pressured' : ''}`}>
              {queuedCount.toLocaleString()} queued
            </span>
          )}
        </div>
      </EdgeLabelRenderer>
    </>
  );
}

export const ConnectionEdge = memo(ConnectionEdgeInner);
