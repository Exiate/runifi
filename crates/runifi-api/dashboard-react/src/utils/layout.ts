// Kahn's algorithm for topological sort + layered layout for React Flow nodes

import type { Node } from '@xyflow/react';
import type { FlowEdgeResponse, FlowNodeResponse } from '../types/api';
import type { ProcessorNodeData } from '../types/flow';

export type ProcNode = Node<ProcessorNodeData, 'processorNode'>;

const NODE_WIDTH = 220;
const NODE_HEIGHT = 120;
const LAYER_GAP_X = 160;
const NODE_GAP_Y = 48;
const PADDING_X = 60;
const PADDING_Y = 60;

/**
 * Compute layered positions for React Flow nodes using Kahn's topological
 * sort. Returns fully formed ProcNode objects ready for React Flow.
 */
export function computeLayout(
  processors: FlowNodeResponse[],
  connections: FlowEdgeResponse[],
): ProcNode[] {
  const names = processors.map((p) => p.name);
  const nameSet = new Set(names);

  // Build adjacency and in-degree maps
  const inDegree = new Map<string, number>();
  const adjOut = new Map<string, string[]>();

  for (const name of names) {
    inDegree.set(name, 0);
    adjOut.set(name, []);
  }

  for (const edge of connections) {
    if (!nameSet.has(edge.source) || !nameSet.has(edge.destination)) continue;
    inDegree.set(edge.destination, (inDegree.get(edge.destination) ?? 0) + 1);
    adjOut.get(edge.source)!.push(edge.destination);
  }

  // Kahn's BFS — assign each node to a layer
  const layer = new Map<string, number>();
  const queue: string[] = [];

  for (const [name, deg] of inDegree) {
    if (deg === 0) queue.push(name);
  }

  while (queue.length > 0) {
    const current = queue.shift()!;
    const currentLayer = layer.get(current) ?? 0;

    for (const neighbor of adjOut.get(current) ?? []) {
      const newLayer = currentLayer + 1;
      if ((layer.get(neighbor) ?? 0) < newLayer) {
        layer.set(neighbor, newLayer);
      }
      const newDeg = (inDegree.get(neighbor) ?? 1) - 1;
      inDegree.set(neighbor, newDeg);
      if (newDeg === 0) queue.push(neighbor);
    }
  }

  // Group nodes by layer
  const layers = new Map<number, string[]>();
  for (const name of names) {
    const l = layer.get(name) ?? 0;
    if (!layers.has(l)) layers.set(l, []);
    layers.get(l)!.push(name);
  }

  // Compute (x, y) positions per layer/column
  const positions = new Map<string, { x: number; y: number }>();
  const sortedLayerKeys = [...layers.keys()].sort((a, b) => a - b);

  for (const [idx, layerKey] of sortedLayerKeys.entries()) {
    const nodesInLayer = layers.get(layerKey)!;
    const x = PADDING_X + idx * (NODE_WIDTH + LAYER_GAP_X);

    for (const [col, name] of nodesInLayer.entries()) {
      const y = PADDING_Y + col * (NODE_HEIGHT + NODE_GAP_Y);
      positions.set(name, { x, y });
    }
  }

  // Build React Flow ProcNode objects
  return processors.map((p): ProcNode => ({
    id: p.name,
    type: 'processorNode' as const,
    position: positions.get(p.name) ?? { x: PADDING_X, y: PADDING_Y },
    data: {
      label: p.name,
      typeName: p.type_name,
      state: 'stopped',
      metrics: null,
      bulletin: null,
    },
  }));
}
