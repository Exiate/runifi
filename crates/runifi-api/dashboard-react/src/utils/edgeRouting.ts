import { Position, type Node } from '@xyflow/react';

/**
 * Determines the optimal source and target handle positions for a connection
 * based on the relative geometry of the two nodes. This produces NiFi-style
 * smart edge routing where connections use the closest anchor points.
 */

// Approximate node dimensions for center calculation
const DEFAULT_NODE_WIDTH = 220;
const DEFAULT_NODE_HEIGHT = 120;
const FUNNEL_NODE_SIZE = 80;

interface NodeCenter {
  x: number;
  y: number;
}

function getNodeCenter(node: Node): NodeCenter {
  const isFunnel = node.type === 'funnelNode';
  const w = (node.measured?.width ?? node.width) ?? (isFunnel ? FUNNEL_NODE_SIZE : DEFAULT_NODE_WIDTH);
  const h = (node.measured?.height ?? node.height) ?? (isFunnel ? FUNNEL_NODE_SIZE : DEFAULT_NODE_HEIGHT);
  return {
    x: node.position.x + w / 2,
    y: node.position.y + h / 2,
  };
}

export interface HandlePositions {
  sourcePosition: Position;
  targetPosition: Position;
  sourceHandleSuffix: string;
  targetHandleSuffix: string;
}

/**
 * Given source and target nodes, compute the best handle positions.
 *
 * The algorithm picks the pair of sides that minimises the visual distance
 * between source output and target input.  It considers the dominant axis
 * (horizontal vs vertical offset) and the direction along that axis.
 */
export function getSmartHandlePositions(
  sourceNode: Node,
  targetNode: Node,
): HandlePositions {
  const src = getNodeCenter(sourceNode);
  const tgt = getNodeCenter(targetNode);

  const dx = tgt.x - src.x;
  const dy = tgt.y - src.y;

  let sourcePosition: Position;
  let targetPosition: Position;

  if (Math.abs(dx) >= Math.abs(dy)) {
    // Horizontal dominant
    if (dx >= 0) {
      // target is to the right → source right, target left
      sourcePosition = Position.Right;
      targetPosition = Position.Left;
    } else {
      // target is to the left → source left, target right
      sourcePosition = Position.Left;
      targetPosition = Position.Right;
    }
  } else {
    // Vertical dominant
    if (dy >= 0) {
      // target is below → source bottom, target top
      sourcePosition = Position.Bottom;
      targetPosition = Position.Top;
    } else {
      // target is above → source top, target bottom
      sourcePosition = Position.Top;
      targetPosition = Position.Bottom;
    }
  }

  return {
    sourcePosition,
    targetPosition,
    sourceHandleSuffix: positionToSuffix(sourcePosition),
    targetHandleSuffix: positionToSuffix(targetPosition),
  };
}

function positionToSuffix(pos: Position): string {
  switch (pos) {
    case Position.Top:
      return 'top';
    case Position.Right:
      return 'right';
    case Position.Bottom:
      return 'bottom';
    case Position.Left:
      return 'left';
  }
}

/**
 * Build the source handle id for a given relationship and position.
 * Format: "{relationship}--{side}" e.g. "success--right", "success--bottom"
 */
export function sourceHandleId(relationship: string, pos: Position): string {
  return `${relationship}--${positionToSuffix(pos)}`;
}

/**
 * Build the target handle id for a given position.
 * Format: "target--{side}" e.g. "target--left", "target--top"
 */
export function targetHandleId(pos: Position): string {
  return `target--${positionToSuffix(pos)}`;
}
