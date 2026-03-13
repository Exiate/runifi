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
  const labelBg = backPressured ? 'rgba(248,113,113,0.15)' : 'rgba(26,29,39,0.9)';
  const queueColor = backPressured ? 'var(--danger)' : 'var(--text-dim)';

  return (
    <>
      <BaseEdge
        id={id}
        path={edgePath}
        style={{ stroke: edgeColor, strokeWidth: 2 }}
      />
      <EdgeLabelRenderer>
        <div
          style={{
            position: 'absolute',
            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
            pointerEvents: 'all',
            display: 'flex',
            flexDirection: 'column',
            alignItems: 'center',
            gap: '2px',
          }}
          className="nodrag nopan"
          aria-label={`Connection ${relationship} — ${queuedCount} queued`}
        >
          <span
            style={{
              background: labelBg,
              border: `1px solid ${backPressured ? 'var(--danger)' : 'var(--border)'}`,
              borderRadius: '3px',
              padding: '1px 5px',
              fontSize: '10px',
              fontWeight: 600,
              color: 'var(--accent)',
              whiteSpace: 'nowrap',
            }}
          >
            {relationship}
          </span>
          {queuedCount > 0 && (
            <span
              style={{
                background: labelBg,
                border: `1px solid ${backPressured ? 'var(--danger)' : 'var(--border)'}`,
                borderRadius: '3px',
                padding: '1px 5px',
                fontSize: '10px',
                color: queueColor,
                fontVariantNumeric: 'tabular-nums',
                whiteSpace: 'nowrap',
              }}
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
