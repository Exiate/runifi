import { memo, useCallback } from 'react';
import {
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  type Edge,
  type EdgeProps,
} from '@xyflow/react';
import type { ConnectionEdgeData } from '../types/flow';

export type ConnectionEdgeType = Edge<ConnectionEdgeData, 'connectionEdge'>;

interface ConnectionEdgeInnerProps extends EdgeProps<ConnectionEdgeType> {
  onQueueClick?: (connectionId: string, label: string) => void;
}

function ConnectionEdgeInner({
  id,
  sourceX,
  sourceY,
  targetX,
  targetY,
  sourcePosition,
  targetPosition,
  source,
  target,
  data,
  // onQueueClick is passed via edge data or through a custom mechanism
}: ConnectionEdgeInnerProps) {
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
  const connectionId = data?.connectionId ?? '';
  const onQueueClick = data?.onQueueClick as
    | ((connectionId: string, label: string) => void)
    | undefined;

  const edgeColor = backPressured ? 'var(--danger)' : 'var(--border)';

  const handleQueueClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      if (onQueueClick && connectionId) {
        const label = `${source} \u2192 ${relationship} \u2192 ${target}`;
        onQueueClick(connectionId, label);
      }
    },
    [onQueueClick, connectionId, source, relationship, target],
  );

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
            <span
              className={`conn-edge-queue-badge${backPressured ? ' back-pressured' : ''}${connectionId ? ' clickable' : ''}`}
              onClick={connectionId ? handleQueueClick : undefined}
              title={connectionId ? 'Click to inspect queue' : undefined}
              role={connectionId ? 'button' : undefined}
              tabIndex={connectionId ? 0 : undefined}
              onKeyDown={
                connectionId
                  ? (e) => {
                      if (e.key === 'Enter' || e.key === ' ') {
                        e.preventDefault();
                        handleQueueClick(e as unknown as React.MouseEvent);
                      }
                    }
                  : undefined
              }
            >
              {queuedCount.toLocaleString()} queued
            </span>
          )}
        </div>
      </EdgeLabelRenderer>
    </>
  );
}

export const ConnectionEdge = memo(ConnectionEdgeInner);
