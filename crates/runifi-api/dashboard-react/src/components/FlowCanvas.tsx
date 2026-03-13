// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore -- CSS import resolved by Vite at build time
import '@xyflow/react/dist/style.css';

import { useCallback, useEffect, useMemo, useRef } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  ReactFlowProvider,
  useNodesState,
  useEdgesState,
  BackgroundVariant,
  type Node,
  type Edge,
} from '@xyflow/react';

import { ProcessorNode } from './ProcessorNode';
import { ConnectionEdge } from './ConnectionEdge';
import { computeLayout } from '../utils/layout';
import { stateColor } from '../utils/format';
import type { FlowResponse, SseMetricsEvent } from '../types/api';
import type { ProcessorNodeData, ConnectionEdgeData } from '../types/flow';

// Concrete node and edge types for use throughout this file
type ProcNode = Node<ProcessorNodeData, 'processorNode'>;
type ConnEdge = Edge<ConnectionEdgeData, 'connectionEdge'>;

const nodeTypes = { processorNode: ProcessorNode } as const;
const edgeTypes = { connectionEdge: ConnectionEdge } as const;

interface FlowCanvasProps {
  topology: FlowResponse;
  liveMetrics: SseMetricsEvent | null;
}

function buildEdges(topology: FlowResponse, metrics: SseMetricsEvent | null): ConnEdge[] {
  return topology.connections.map((conn, idx) => {
    const live =
      metrics?.connections.find(
        (c) =>
          c.source_name === conn.source &&
          c.dest_name === conn.destination &&
          c.relationship === conn.relationship,
      ) ?? null;

    const backPressured = live?.back_pressured ?? false;

    return {
      id: `${conn.source}--${conn.relationship}--${conn.destination}--${idx}`,
      source: conn.source,
      target: conn.destination,
      type: 'connectionEdge' as const,
      data: {
        relationship: conn.relationship,
        queuedCount: live?.queued_count ?? 0,
        queuedBytes: live?.queued_bytes ?? 0,
        backPressured,
        connectionId: live?.id ?? '',
      },
      style: { stroke: backPressured ? 'var(--danger)' : 'var(--border)' },
    };
  });
}

function FlowCanvasInner({ topology, liveMetrics }: FlowCanvasProps) {
  const initialNodes = useMemo(
    () => computeLayout(topology.processors, topology.connections),
    [topology],
  );
  const initialEdges = useMemo(() => buildEdges(topology, null), [topology]);

  const [nodes, setNodes, onNodesChange] = useNodesState<ProcNode>(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState<ConnEdge>(initialEdges);

  // Guard to prevent re-layout on first metrics update
  const topologyKey = useRef(topology.name);

  useEffect(() => {
    if (topologyKey.current === topology.name) return;
    topologyKey.current = topology.name;
    setNodes(computeLayout(topology.processors, topology.connections));
    setEdges(buildEdges(topology, liveMetrics));
  }, [topology, liveMetrics, setNodes, setEdges]);

  // Update node data from live metrics without touching positions
  useEffect(() => {
    if (!liveMetrics) return;

    const procMap = new Map(liveMetrics.processors.map((p) => [p.name, p]));
    const bulletinMap = new Map(liveMetrics.bulletins.map((b) => [b.processor_name, b]));

    setNodes((prev) =>
      prev.map((node) => {
        const proc = procMap.get(node.id);
        if (!proc) return node;

        const bulletin = bulletinMap.get(node.id) ?? null;

        // Skip update if nothing changed
        if (
          node.data.state === proc.state &&
          node.data.metrics?.total_invocations === proc.metrics.total_invocations &&
          node.data.bulletin === bulletin
        ) {
          return node;
        }

        return {
          ...node,
          data: {
            ...node.data,
            state: proc.state,
            metrics: proc.metrics,
            bulletin,
          },
        };
      }),
    );

    setEdges((prev) =>
      prev.map((edge) => {
        const live = liveMetrics.connections.find(
          (c) => c.source_name === edge.source && c.dest_name === edge.target,
        );
        if (!live) return edge;

        // Skip update if nothing changed
        if (
          edge.data?.queuedCount === live.queued_count &&
          edge.data?.backPressured === live.back_pressured
        ) {
          return edge;
        }

        return {
          ...edge,
          data: {
            relationship: live.relationship,
            queuedCount: live.queued_count,
            queuedBytes: live.queued_bytes,
            backPressured: live.back_pressured,
            connectionId: live.id,
          },
          style: {
            stroke: live.back_pressured ? 'var(--danger)' : 'var(--border)',
          },
        };
      }),
    );
  }, [liveMetrics, setNodes, setEdges]);

  const nodeColor = useCallback((node: ProcNode) => {
    return stateColor(node.data?.state ?? 'stopped');
  }, []);

  return (
    <div className="flow-canvas-wrapper" aria-label="Flow topology canvas">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        fitViewOptions={{ padding: 0.15 }}
        minZoom={0.3}
        maxZoom={2}
        attributionPosition="bottom-right"
        colorMode="dark"
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={20}
          size={1}
          color="var(--border)"
        />
        <Controls
          showInteractive={false}
          style={{ background: 'var(--surface)', border: '1px solid var(--border)' }}
        />
        <MiniMap<ProcNode>
          nodeColor={nodeColor}
          maskColor="rgba(15,17,23,0.7)"
          style={{
            background: 'var(--surface)',
            border: '1px solid var(--border)',
          }}
        />
      </ReactFlow>
    </div>
  );
}

export function FlowCanvas(props: FlowCanvasProps) {
  return (
    <ReactFlowProvider>
      <FlowCanvasInner {...props} />
    </ReactFlowProvider>
  );
}
