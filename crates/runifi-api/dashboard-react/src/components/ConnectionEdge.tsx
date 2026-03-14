import { memo, useCallback, useState } from 'react';
import {
  BaseEdge,
  EdgeLabelRenderer,
  getBezierPath,
  type Edge,
  type EdgeProps,
} from '@xyflow/react';
import type { ConnectionEdgeData } from '../types/flow';
import { backPressureEdgeColor, formatBytes } from '../utils/format';

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
}: ConnectionEdgeInnerProps) {
  const effectiveSourcePos = data?.smartSourcePosition ?? sourcePosition;
  const effectiveTargetPos = data?.smartTargetPosition ?? targetPosition;

  const [edgePath, labelX, labelY] = getBezierPath({
    sourceX,
    sourceY,
    sourcePosition: effectiveSourcePos,
    targetX,
    targetY,
    targetPosition: effectiveTargetPos,
  });

  const queuedCount = data?.queuedCount ?? 0;
  const queuedBytes = data?.queuedBytes ?? 0;
  const fillPercentage = data?.fillPercentage ?? 0;
  const backPressureObjectThreshold = data?.backPressureObjectThreshold ?? 10000;
  const backPressureBytesThreshold = data?.backPressureBytesThreshold ?? 1073741824;
  const relationship = data?.relationship ?? '';
  const connectionId = data?.connectionId ?? '';
  const onQueueClick = data?.onQueueClick as
    | ((connectionId: string, label: string) => void)
    | undefined;

  const edgeColor = backPressureEdgeColor(fillPercentage);
  const strokeWidth = fillPercentage > 0.85 ? 3 : 2;

  const [hovered, setHovered] = useState(false);

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

  // Badge text: show count + size when bytes > 0
  const badgeText =
    queuedBytes > 0
      ? `${queuedCount.toLocaleString()} (${formatBytes(queuedBytes)})`
      : `${queuedCount.toLocaleString()} queued`;

  // Badge color class based on fill percentage
  const badgeColorClass =
    fillPercentage > 0.85 ? ' danger' : fillPercentage > 0.60 ? ' warning' : '';

  return (
    <>
      {/* Animated flow direction dashes */}
      <path
        d={edgePath}
        className="conn-edge-flow-line"
        style={{ stroke: edgeColor }}
        fill="none"
      />
      <BaseEdge
        id={id}
        path={edgePath}
        style={{ stroke: edgeColor, strokeWidth }}
      />
      <EdgeLabelRenderer>
        <div
          style={{
            position: 'absolute',
            transform: `translate(-50%, -50%) translate(${labelX}px,${labelY}px)`,
          }}
          className={`conn-edge-label nodrag nopan${fillPercentage > 0.85 ? ' back-pressured' : ''}`}
          aria-label={`Connection ${relationship} — ${queuedCount} queued`}
          onMouseEnter={() => setHovered(true)}
          onMouseLeave={() => setHovered(false)}
        >
          <span className="conn-edge-rel-badge">{relationship}</span>
          {queuedCount > 0 && (
            <span
              className={`conn-edge-queue-badge${badgeColorClass}${connectionId ? ' clickable' : ''}`}
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
              {badgeText}
            </span>
          )}
          {/* Hover tooltip */}
          {hovered && (
            <div className="conn-edge-tooltip">
              <div className="conn-edge-tooltip-row">
                <span className="conn-edge-tooltip-label">Queue Size</span>
                <span className="conn-edge-tooltip-value">
                  {queuedCount.toLocaleString()} FlowFiles ({formatBytes(queuedBytes)})
                </span>
              </div>
              <div className="conn-edge-tooltip-row">
                <span className="conn-edge-tooltip-label">Object Threshold</span>
                <span className="conn-edge-tooltip-value">
                  {backPressureObjectThreshold.toLocaleString()}
                </span>
              </div>
              <div className="conn-edge-tooltip-row">
                <span className="conn-edge-tooltip-label">Size Threshold</span>
                <span className="conn-edge-tooltip-value">
                  {formatBytes(backPressureBytesThreshold)}
                </span>
              </div>
              <div className="conn-edge-tooltip-row">
                <span className="conn-edge-tooltip-label">Fill %</span>
                <span className="conn-edge-tooltip-value">
                  {(fillPercentage * 100).toFixed(1)}%
                </span>
              </div>
              <div className="conn-edge-tooltip-row">
                <span className="conn-edge-tooltip-label">Source</span>
                <span className="conn-edge-tooltip-value">{source}</span>
              </div>
              <div className="conn-edge-tooltip-row">
                <span className="conn-edge-tooltip-label">Destination</span>
                <span className="conn-edge-tooltip-value">{target}</span>
              </div>
            </div>
          )}
        </div>
      </EdgeLabelRenderer>
    </>
  );
}

export const ConnectionEdge = memo(ConnectionEdgeInner);
