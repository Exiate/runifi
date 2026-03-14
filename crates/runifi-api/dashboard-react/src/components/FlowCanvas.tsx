// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-ignore -- CSS import resolved by Vite at build time
import '@xyflow/react/dist/style.css';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  ReactFlowProvider,
  useNodesState,
  useEdgesState,
  useReactFlow,
  BackgroundVariant,
  addEdge,
  type Node,
  type Edge,
  type Connection,
  type OnConnect,
  type NodeMouseHandler,
  type EdgeMouseHandler,
  Panel,
} from '@xyflow/react';

import { ProcessorNode } from './ProcessorNode';
import { FunnelNode } from './FunnelNode';
import { LabelNode } from './LabelNode';
import { ConnectionEdge } from './ConnectionEdge';
import { AddProcessorModal } from './AddProcessorModal';
import { ConnectionModal } from './ConnectionModal';
import { ConfirmDialog } from './ConfirmDialog';
import { ContextMenu, type ContextMenuState, type AlignAction } from './ContextMenu';
import { CanvasSearch } from './CanvasSearch';
import {
  alignLeft, alignCenter, alignRight,
  alignTop, alignMiddle, alignBottom,
  distributeHorizontally, distributeVertically,
} from '../utils/alignment';
import { ProcessorConfigModal } from './ProcessorConfigModal';
import { QueueInspectorModal } from './QueueInspectorModal';
import { ColorPickerDialog } from './ColorPickerDialog';
import { computeLayout } from '../utils/layout';
import { stateColor } from '../utils/format';
import { getSmartHandlePositions, sourceHandleId, targetHandleId } from '../utils/edgeRouting';
import type { FlowResponse, SseMetricsEvent, PluginDescriptor } from '../types/api';
import type { ProcessorNodeData, ConnectionEdgeData, LabelNodeData } from '../types/flow';
import type { ToastKind } from '../hooks/useToast';

// Concrete node and edge types
type ProcNode = Node<ProcessorNodeData, 'processorNode'>;
type FunnelFlowNode = Node<ProcessorNodeData, 'funnelNode'>;
type LabelFlowNode = Node<LabelNodeData, 'labelNode'>;
type AnyNode = ProcNode | FunnelFlowNode | LabelFlowNode;
type ConnEdge = Edge<ConnectionEdgeData, 'connectionEdge'>;

const nodeTypes = {
  processorNode: ProcessorNode,
  funnelNode: FunnelNode,
  labelNode: LabelNode,
} as const;
const edgeTypes = { connectionEdge: ConnectionEdge } as const;

const POSITION_DEBOUNCE_MS = 800;
const LS_COLORS_KEY = 'runifi-processor-colors';

interface PendingDrop {
  plugin: PluginDescriptor;
  position: { x: number; y: number };
}

interface DeleteTarget {
  kind: 'node' | 'edge' | 'multi';
  id: string;
  label: string;
  nodeState?: string;
  nodeIds?: string[];
}

interface QueueInspectTarget {
  connectionId: string;
  label: string;
}

interface ColorPickerTarget {
  nodeId: string;
  currentColor: string;
}

interface ClipboardEntry {
  nodes: Array<{ id: string; type: string; position: { x: number; y: number }; data: Record<string, unknown> }>;
  edges: ConnEdge[];
}

export interface FlowCanvasProps {
  topology: FlowResponse;
  liveMetrics: SseMetricsEvent | null;
  plugins: PluginDescriptor[];
  onToast: (kind: ToastKind, message: string) => void;
  draggedPlugin: PluginDescriptor | null;
  addPluginAtCenter?: PluginDescriptor | null;
  onAddPluginHandled?: () => void;
  onSelectionChange?: (nodeIds: string[]) => void;
  colorRequestNodeIds?: string[] | null;
  onColorRequestHandled?: () => void;
}

function loadSavedColors(): Record<string, string> {
  try {
    const stored = localStorage.getItem(LS_COLORS_KEY);
    if (stored) return JSON.parse(stored) as Record<string, string>;
  } catch { /* ignore corrupt data */ }
  return {};
}

function saveColors(colors: Record<string, string>): void {
  try {
    localStorage.setItem(LS_COLORS_KEY, JSON.stringify(colors));
  } catch { /* quota exceeded, etc. */ }
}

function buildEdges(
  topology: FlowResponse,
  metrics: SseMetricsEvent | null,
  onQueueClick: (connectionId: string, label: string) => void,
  nodes: AnyNode[],
): ConnEdge[] {
  const nodeMap = new Map(nodes.map((n) => [n.id, n]));

  return topology.connections.map((conn, idx) => {
    const live =
      metrics?.connections.find(
        (c) =>
          c.source_name === conn.source &&
          c.dest_name === conn.destination &&
          c.relationship === conn.relationship,
      ) ?? null;

    const backPressured = live?.back_pressured ?? false;

    const srcNode = nodeMap.get(conn.source);
    const tgtNode = nodeMap.get(conn.destination);

    let sHandle = `${conn.relationship}--right`;
    let tHandle = 'target--left';
    let smartSourcePos: import('@xyflow/react').Position | undefined;
    let smartTargetPos: import('@xyflow/react').Position | undefined;

    if (srcNode && tgtNode) {
      const positions = getSmartHandlePositions(srcNode, tgtNode);
      sHandle = sourceHandleId(conn.relationship, positions.sourcePosition);
      tHandle = targetHandleId(positions.targetPosition);
      smartSourcePos = positions.sourcePosition;
      smartTargetPos = positions.targetPosition;
    }

    return {
      id: `${conn.source}--${conn.relationship}--${conn.destination}--${idx}`,
      source: conn.source,
      target: conn.destination,
      sourceHandle: sHandle,
      targetHandle: tHandle,
      type: 'connectionEdge' as const,
      data: {
        relationship: conn.relationship,
        queuedCount: live?.queued_count ?? 0,
        queuedBytes: live?.queued_bytes ?? 0,
        backPressured,
        connectionId: live?.id ?? '',
        pending: false,
        onQueueClick,
        smartSourcePosition: smartSourcePos,
        smartTargetPosition: smartTargetPos,
      },
      style: { stroke: backPressured ? 'var(--danger)' : 'var(--border)' },
    };
  });
}

function pluginForNode(
  nodes: AnyNode[],
  nodeId: string,
  plugins: PluginDescriptor[],
): PluginDescriptor | null {
  const node = nodes.find((n) => n.id === nodeId);
  if (!node || node.type === 'labelNode') return null;
  const procData = node.data as ProcessorNodeData;
  return plugins.find((p) => p.type_name === procData.typeName) ?? null;
}

function FlowCanvasInner({
  topology,
  liveMetrics,
  plugins,
  onToast,
  draggedPlugin,
  addPluginAtCenter,
  onAddPluginHandled,
  onSelectionChange,
  colorRequestNodeIds,
  onColorRequestHandled,
}: FlowCanvasProps) {
  const { screenToFlowPosition, getViewport, setCenter, fitView } = useReactFlow();

  const [queueInspectTarget, setQueueInspectTarget] = useState<QueueInspectTarget | null>(null);
  const handleQueueClick = useCallback((connectionId: string, label: string) => {
    setQueueInspectTarget({ connectionId, label });
  }, []);

  const savedColorsRef = useRef(loadSavedColors());

  const initialNodes = useMemo(
    () => {
      const colors = savedColorsRef.current;
      const procNodes: AnyNode[] = computeLayout(topology.processors, topology.connections).map((n) => {
        // Use funnelNode type for Funnel processors
        const isFunnel = n.data.typeName === 'Funnel';
        return {
          ...n,
          type: isFunnel ? ('funnelNode' as const) : ('processorNode' as const),
          data: {
            ...n.data,
            relationships: plugins.find((p) => p.type_name === n.data.typeName)?.relationships ?? [
              'success',
            ],
            pending: false,
            customColor: colors[n.id] ?? '',
          },
        };
      });

      // Add label nodes from topology
      const labelNodes: AnyNode[] = (topology.labels ?? []).map((lbl): LabelFlowNode => ({
        id: `label-${lbl.id}`,
        type: 'labelNode' as const,
        position: { x: lbl.x, y: lbl.y },
        data: {
          labelId: lbl.id,
          text: lbl.text,
          width: lbl.width,
          height: lbl.height,
          backgroundColor: lbl.background_color,
          fontSize: lbl.font_size,
          pending: false,
        },
        draggable: true,
        selectable: true,
        connectable: false,
      }));

      return [...procNodes, ...labelNodes];
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [topology],
  );
  const initialEdges = useMemo(
    () => buildEdges(topology, null, handleQueueClick, initialNodes),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [topology],
  );

  const [nodes, setNodes, onNodesChange] = useNodesState<AnyNode>(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState<ConnEdge>(initialEdges);

  // Keep a ref to the latest nodes for edge routing calculations
  const nodesRef = useRef(nodes);
  useEffect(() => { nodesRef.current = nodes; }, [nodes]);

  const [pendingDrop, setPendingDrop] = useState<PendingDrop | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<DeleteTarget | null>(null);
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [pendingConnectionContext, setPendingConnectionContext] = useState<{
    sourceId: string;
    targetId: string;
    relationships: string[];
    existingRels: string[];
  } | null>(null);
  const [configTarget, setConfigTarget] = useState<{ name: string; state: string } | null>(null);
  const [colorPickerTarget, setColorPickerTarget] = useState<ColorPickerTarget | null>(null);
  const batchColorNodeIdsRef = useRef<string[] | null>(null);

  // Handle batch color requests from OperatePalette
  useEffect(() => {
    if (colorRequestNodeIds && colorRequestNodeIds.length > 0) {
      batchColorNodeIdsRef.current = colorRequestNodeIds;
      const firstNode = nodes.find((n) => n.id === colorRequestNodeIds[0]);
      const currentColor = firstNode?.data ? (firstNode.data as ProcessorNodeData).customColor || '' : '';
      setColorPickerTarget({ nodeId: colorRequestNodeIds[0], currentColor });
      onColorRequestHandled?.();
    }
  }, [colorRequestNodeIds]); // eslint-disable-line react-hooks/exhaustive-deps

  const topologyKey = useRef(topology.name);

  useEffect(() => {
    if (topologyKey.current === topology.name) return;
    topologyKey.current = topology.name;
    const colors = loadSavedColors();
    savedColorsRef.current = colors;
    const procNodes: AnyNode[] = computeLayout(topology.processors, topology.connections).map((n) => {
      const isFunnel = n.data.typeName === 'Funnel';
      return {
        ...n,
        type: isFunnel ? ('funnelNode' as const) : ('processorNode' as const),
        data: {
          ...n.data,
          relationships: plugins.find((p) => p.type_name === n.data.typeName)?.relationships ?? [
            'success',
          ],
          pending: false,
          customColor: colors[n.id] ?? '',
        },
      };
    });

    const labelNodes: AnyNode[] = (topology.labels ?? []).map((lbl): LabelFlowNode => ({
      id: `label-${lbl.id}`,
      type: 'labelNode' as const,
      position: { x: lbl.x, y: lbl.y },
      data: {
        labelId: lbl.id,
        text: lbl.text,
        width: lbl.width,
        height: lbl.height,
        backgroundColor: lbl.background_color,
        fontSize: lbl.font_size,
        pending: false,
      },
      draggable: true,
      selectable: true,
      connectable: false,
    }));

    const allNodes = [...procNodes, ...labelNodes];
    setNodes(allNodes);
    setEdges(buildEdges(topology, liveMetrics, handleQueueClick, allNodes));
  }, [topology, liveMetrics, setNodes, setEdges, plugins, handleQueueClick]);

  useEffect(() => {
    if (!liveMetrics) return;

    const procMap = new Map(liveMetrics.processors.map((p) => [p.name, p]));
    const bulletinMap = new Map(liveMetrics.bulletins.map((b) => [b.processor_name, b]));

    setNodes((prev) =>
      prev.map((node) => {
        // Skip label nodes — they have no processor metrics
        if (node.type === 'labelNode') return node;

        const procData = node.data as ProcessorNodeData;
        const proc = procMap.get(node.id);
        if (!proc) return node;

        const bulletin = bulletinMap.get(node.id) ?? null;

        if (
          procData.state === proc.state &&
          procData.metrics?.total_invocations === proc.metrics.total_invocations &&
          procData.bulletin === bulletin
        ) {
          return node;
        }

        return {
          ...node,
          data: {
            ...procData,
            state: proc.state,
            metrics: proc.metrics,
            bulletin,
            pending: false,
          },
        };
      }),
    );

    setEdges((prev) =>
      prev.map((edge) => {
        const live = liveMetrics.connections.find(
          (c) =>
            c.source_name === edge.source &&
            c.dest_name === edge.target &&
            c.relationship === edge.data?.relationship,
        );
        if (!live) return edge;

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
            pending: false,
            onQueueClick: handleQueueClick,
            smartSourcePosition: edge.data?.smartSourcePosition,
            smartTargetPosition: edge.data?.smartTargetPosition,
          },
          style: {
            stroke: live.back_pressured ? 'var(--danger)' : 'var(--border)',
          },
        };
      }),
    );
  }, [liveMetrics, setNodes, setEdges, handleQueueClick]);

  const positionTimers = useRef<Map<string, ReturnType<typeof setTimeout>>>(new Map());

  const persistPosition = useCallback((nodeId: string, x: number, y: number) => {
    const existing = positionTimers.current.get(nodeId);
    if (existing) clearTimeout(existing);

    const timer = setTimeout(() => {
      positionTimers.current.delete(nodeId);

      // Labels use a different API endpoint
      if (nodeId.startsWith('label-')) {
        const labelId = nodeId.slice('label-'.length);
        fetch(`/api/v1/process-groups/root/labels/${encodeURIComponent(labelId)}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ x, y }),
        }).catch(() => {});
      } else {
        fetch(`/api/v1/processors/${encodeURIComponent(nodeId)}/position`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ x, y }),
        }).catch(() => {});
      }
    }, POSITION_DEBOUNCE_MS);

    positionTimers.current.set(nodeId, timer);
  }, []);

  /** Recalculate smart edge routing for all edges touching the given node ids */
  const recalcEdgeRouting = useCallback(
    (movedNodeIds: Set<string>) => {
      const currentNodes = nodesRef.current;
      const nodeMap = new Map(currentNodes.map((n) => [n.id, n]));

      setEdges((prevEdges) => {
        const needsUpdate = prevEdges.some(
          (e) => movedNodeIds.has(e.source) || movedNodeIds.has(e.target),
        );
        if (!needsUpdate) return prevEdges;

        return prevEdges.map((edge) => {
          if (!movedNodeIds.has(edge.source) && !movedNodeIds.has(edge.target)) {
            return edge;
          }

          const srcNode = nodeMap.get(edge.source);
          const tgtNode = nodeMap.get(edge.target);
          if (!srcNode || !tgtNode) return edge;

          const positions = getSmartHandlePositions(srcNode, tgtNode);
          const rel = edge.data?.relationship ?? 'success';
          const newSourceHandle = sourceHandleId(rel, positions.sourcePosition);
          const newTargetHandle = targetHandleId(positions.targetPosition);

          if (
            edge.sourceHandle === newSourceHandle &&
            edge.targetHandle === newTargetHandle
          ) {
            return edge;
          }

          return {
            ...edge,
            sourceHandle: newSourceHandle,
            targetHandle: newTargetHandle,
            data: {
              ...edge.data!,
              smartSourcePosition: positions.sourcePosition,
              smartTargetPosition: positions.targetPosition,
            },
          };
        });
      });
    },
    [setEdges],
  );

  const handleNodesChange: typeof onNodesChange = useCallback(
    (changes) => {
      const positionFinalized = new Set<string>();
      for (const change of changes) {
        if (change.type === 'position' && change.position && !change.dragging) {
          persistPosition(change.id, change.position.x, change.position.y);
          positionFinalized.add(change.id);
        }
      }
      onNodesChange(changes);

      // Recalculate edge routing for any nodes that stopped dragging
      if (positionFinalized.size > 0) {
        recalcEdgeRouting(positionFinalized);
      }
    },
    [onNodesChange, persistPosition, recalcEdgeRouting],
  );

  /** Recalculate during drag for real-time feedback */
  const handleNodeDrag: NodeMouseHandler<AnyNode> = useCallback(
    (_event, node) => {
      // During drag, React Flow has already updated the node position
      // before this callback fires. Update the ref so recalcEdgeRouting
      // reads the latest positions.
      nodesRef.current = nodesRef.current.map((n) =>
        n.id === node.id ? { ...n, position: node.position } : n,
      );
      recalcEdgeRouting(new Set([node.id]));
    },
    [recalcEdgeRouting],
  );

  useEffect(() => {
    const timers = positionTimers.current;
    return () => {
      for (const t of timers.values()) clearTimeout(t);
    };
  }, []);

  useEffect(() => {
    if (!addPluginAtCenter) return;
    const { x, y, zoom } = getViewport();
    const centerX = (-x + window.innerWidth / 2) / zoom;
    const centerY = (-y + window.innerHeight / 2) / zoom;
    setPendingDrop({ plugin: addPluginAtCenter, position: { x: centerX, y: centerY } });
    onAddPluginHandled?.();
  }, [addPluginAtCenter, getViewport, onAddPluginHandled]);

  const handleDragOver = useCallback((e: React.DragEvent) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'copy';
  }, []);

  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();

      // Handle component drops (funnel, label) from toolbar
      const componentType = e.dataTransfer.getData('application/runifi-component');
      if (componentType) {
        const position = screenToFlowPosition({ x: e.clientX, y: e.clientY });

        if (componentType === 'funnel') {
          // Create a funnel processor via the existing processor API
          const funnelPlugin = plugins.find((p) => p.type_name === 'Funnel');
          if (funnelPlugin) {
            setPendingDrop({ plugin: funnelPlugin, position });
          } else {
            onToast('error', 'Funnel processor type not found in plugins registry.');
          }
          return;
        }

        if (componentType === 'label') {
          // Create a label directly (no name dialog needed)
          const newLabel: LabelFlowNode = {
            id: `label-pending-${Date.now()}`,
            type: 'labelNode' as const,
            position,
            data: {
              labelId: '',
              text: '',
              width: 200,
              height: 60,
              backgroundColor: 'rgba(255, 255, 200, 0.12)',
              fontSize: 14,
              pending: true,
            },
            draggable: true,
            selectable: true,
            connectable: false,
          };

          setNodes((prev) => [...prev, newLabel]);

          fetch('/api/v1/process-groups/root/labels', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              text: '',
              x: position.x,
              y: position.y,
              width: 200,
              height: 60,
              background_color: 'rgba(255, 255, 200, 0.12)',
              font_size: 14,
            }),
          })
            .then((res) => {
              if (!res.ok) throw new Error(`HTTP ${res.status}`);
              return res.json() as Promise<{ id: string }>;
            })
            .then((created) => {
              setNodes((prev) =>
                prev.map((n): AnyNode =>
                  n.id === newLabel.id
                    ? ({
                        ...n,
                        id: `label-${created.id}`,
                        data: {
                          ...(n.data as LabelNodeData),
                          labelId: created.id,
                          pending: false,
                        },
                      } as AnyNode
                    : n,
                ),
              );
              onToast('success', 'Label added to canvas.');
            })
            .catch((err: unknown) => {
              const msg = err instanceof Error ? err.message : String(err);
              onToast('error', `Failed to create label: ${msg}`);
            });
          return;
        }

        if (componentType === 'input-port' || componentType === 'output-port') {
          const portLabel = componentType === 'input-port' ? 'Input' : 'Output';
          onToast('info', `${portLabel} ports require process groups (not yet implemented).`);
          return;
        }

        return;
      }

      // Handle plugin drops (processors) from the add dialog
      const typeName = e.dataTransfer.getData('application/runifi-plugin');
      if (!typeName) return;

      const plugin = plugins.find((p) => p.type_name === typeName);
      if (!plugin) return;

      const position = screenToFlowPosition({ x: e.clientX, y: e.clientY });
      setPendingDrop({ plugin, position });
    },
    [plugins, screenToFlowPosition, setNodes, onToast],
  );

  const handleAddProcessor = useCallback(
    (name: string) => {
      if (!pendingDrop) return;
      const { plugin, position } = pendingDrop;
      setPendingDrop(null);

      const isFunnel = plugin.type_name === 'Funnel';
      const newNode: AnyNode = {
        id: name,
        type: isFunnel ? ('funnelNode' as const) : ('processorNode' as const),
        position,
        data: {
          label: name,
          typeName: plugin.type_name,
          state: 'stopped',
          metrics: null,
          bulletin: null,
          relationships: plugin.relationships ?? ['success'],
          pending: true,
          customColor: '',
        },
      };

      setNodes((prev) => [...prev, newNode]);

      fetch('/api/v1/processors', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type: plugin.type_name,
          name,
          position,
          properties: {},
        }),
      })
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          setNodes((prev) =>
            prev.map((n): AnyNode =>
              n.id === name ? { ...n, data: { ...n.data, pending: false } } as AnyNode : n,
            ),
          );
          onToast('success', `Processor "${name}" added.`);
        })
        .catch((err: unknown) => {
          const msg = err instanceof Error ? err.message : String(err);
          onToast(
            'error',
            `Failed to save processor "${name}" to backend: ${msg}. Shown locally only.`,
          );
        });
    },
    [pendingDrop, setNodes, onToast],
  );

  const handleConnect: OnConnect = useCallback(
    (connection: Connection) => {
      const { source, target } = connection;
      if (!source || !target) return;

      if (source === target) {
        onToast('warning', 'Self-connections are not allowed.');
        return;
      }

      const srcPlugin = pluginForNode(nodes, source, plugins);
      const relationships = srcPlugin?.relationships ?? ['success'];

      const existingRels = edges
        .filter((e) => e.source === source && e.target === target)
        .map((e) => e.data?.relationship ?? '');

      setPendingConnectionContext({
        sourceId: source,
        targetId: target,
        relationships,
        existingRels,
      });
    },
    [nodes, edges, plugins, onToast],
  );

  const handleConfirmConnection = useCallback(
    (relationship: string) => {
      if (!pendingConnectionContext) return;
      const { sourceId, targetId } = pendingConnectionContext;
      setPendingConnectionContext(null);

      // Compute smart handle positions
      const srcNode = nodes.find((n) => n.id === sourceId);
      const tgtNode = nodes.find((n) => n.id === targetId);

      let sHandle = `${relationship}--right`;
      let tHandle = 'target--left';
      let smartSrcPos: import('@xyflow/react').Position | undefined;
      let smartTgtPos: import('@xyflow/react').Position | undefined;

      if (srcNode && tgtNode) {
        const positions = getSmartHandlePositions(srcNode, tgtNode);
        sHandle = sourceHandleId(relationship, positions.sourcePosition);
        tHandle = targetHandleId(positions.targetPosition);
        smartSrcPos = positions.sourcePosition;
        smartTgtPos = positions.targetPosition;
      }

      const edgeId = `${sourceId}--${relationship}--${targetId}--${Date.now()}`;
      const newEdge: ConnEdge = {
        id: edgeId,
        source: sourceId,
        target: targetId,
        sourceHandle: sHandle,
        targetHandle: tHandle,
        type: 'connectionEdge' as const,
        data: {
          relationship,
          queuedCount: 0,
          queuedBytes: 0,
          backPressured: false,
          connectionId: '',
          pending: true,
          onQueueClick: handleQueueClick,
          smartSourcePosition: smartSrcPos,
          smartTargetPosition: smartTgtPos,
        },
        style: { stroke: 'var(--border)' },
      };

      setEdges((prev) => addEdge(newEdge, prev));

      fetch('/api/v1/connections', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ source: sourceId, relationship, destination: targetId }),
      })
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          return res.json() as Promise<{ id: string }>;
        })
        .then((data) => {
          setEdges((prev) =>
            prev.map((e) =>
              e.id === edgeId
                ? { ...e, data: { ...e.data!, connectionId: data.id, pending: false } }
                : e,
            ),
          );
          onToast('success', `Connection ${sourceId} \u2192 ${targetId} (${relationship}) created.`);
        })
        .catch((err: unknown) => {
          const msg = err instanceof Error ? err.message : String(err);
          onToast(
            'error',
            `Failed to save connection to backend: ${msg}. Shown locally only.`,
          );
        });
    },
    [pendingConnectionContext, setEdges, onToast, handleQueueClick, nodes],
  );

  const initiateDelete = useCallback(
    (kind: 'node' | 'edge', id: string) => {
      if (kind === 'node') {
        const node = nodes.find((n) => n.id === id);
        if (!node) return;

        // Label nodes use 'text' for display, processor nodes use 'label'
        const displayLabel = node.type === 'labelNode'
          ? ((node.data as LabelNodeData).text || 'Label')
          : (node.data as ProcessorNodeData).label;
        const nodeState = node.type === 'labelNode'
          ? 'stopped'
          : (node.data as ProcessorNodeData).state;

        setDeleteTarget({
          kind: 'node',
          id,
          label: displayLabel,
          nodeState,
        });
      } else {
        const edge = edges.find((e) => e.id === id);
        if (!edge) return;
        setDeleteTarget({
          kind: 'edge',
          id,
          label: `${edge.source} \u2192 ${edge.target} (${edge.data?.relationship ?? ''})`,
        });
      }
    },
    [nodes, edges],
  );

  const handleDeleteConfirm = useCallback(() => {
    if (!deleteTarget) return;
    setDeleteTarget(null);

    if (deleteTarget.kind === 'multi') {
      const nodeIds = deleteTarget.nodeIds ?? [];
      const runningIds = nodes
        .filter((n) => nodeIds.includes(n.id) && n.data.state === 'running')
        .map((n) => n.id);

      if (runningIds.length > 0) {
        onToast('error', `Cannot delete running processors: ${runningIds.join(', ')}. Stop them first.`);
        return;
      }

      setNodes((prev) => prev.filter((n) => !nodeIds.includes(n.id)));
      setEdges((prev) => prev.filter((e) => !nodeIds.includes(e.source) && !nodeIds.includes(e.target)));

      for (const id of nodeIds) {
        fetch(`/api/v1/processors/${encodeURIComponent(id)}`, { method: 'DELETE' }).catch(() => {});
      }
      onToast('success', `Deleted ${nodeIds.length} processors.`);
      return;
    }

    if (deleteTarget.kind === 'node') {
      const { id, nodeState } = deleteTarget;

      // Handle label deletion
      if (id.startsWith('label-')) {
        const labelId = id.slice('label-'.length);
        setNodes((prev) => prev.filter((n) => n.id !== id));

        fetch(`/api/v1/process-groups/root/labels/${encodeURIComponent(labelId)}`, { method: 'DELETE' })
          .then((res) => {
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            onToast('success', 'Label deleted.');
          })
          .catch((err: unknown) => {
            const msg = err instanceof Error ? err.message : String(err);
            onToast('error', `Failed to delete label: ${msg}.`);
          });
        return;
      }

      if (nodeState === 'running') {
        onToast('error', `Cannot delete running processor "${id}". Stop it first.`);
        return;
      }

      setNodes((prev) => prev.filter((n) => n.id !== id));
      setEdges((prev) => prev.filter((e) => e.source !== id && e.target !== id));

      fetch(`/api/v1/processors/${encodeURIComponent(id)}`, { method: 'DELETE' })
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          onToast('success', `Processor "${id}" deleted.`);
        })
        .catch((err: unknown) => {
          const msg = err instanceof Error ? err.message : String(err);
          onToast('error', `Failed to delete processor from backend: ${msg}.`);
        });
    } else {
      const { id } = deleteTarget;
      const edge = edges.find((e) => e.id === id);
      const connId = edge?.data?.connectionId;

      setEdges((prev) => prev.filter((e) => e.id !== id));

      if (connId) {
        fetch(`/api/v1/connections/${encodeURIComponent(connId)}`, { method: 'DELETE' })
          .then((res) => {
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            onToast('success', 'Connection deleted.');
          })
          .catch((err: unknown) => {
            const msg = err instanceof Error ? err.message : String(err);
            onToast('error', `Failed to delete connection from backend: ${msg}.`);
          });
      } else {
        onToast('success', 'Connection removed (was pending, not yet persisted).');
      }
    }
  }, [deleteTarget, nodes, edges, setNodes, setEdges, onToast]);

  const controlProcessor = useCallback(
    (name: string, action: 'start' | 'stop' | 'pause' | 'resume' | 'reset-circuit') => {
      fetch(`/api/v1/processors/${encodeURIComponent(name)}/${action}`, { method: 'POST' })
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          const labels: Record<string, string> = {
            start: 'started',
            stop: 'stopped',
            pause: 'paused',
            resume: 'resumed',
            'reset-circuit': 'circuit reset',
          };
          onToast('success', `Processor "${name}" ${labels[action] ?? action}.`);
        })
        .catch((err: unknown) => {
          const msg = err instanceof Error ? err.message : String(err);
          onToast('error', `Failed to ${action} "${name}": ${msg}`);
        });
    },
    [onToast],
  );

  // Multi-select operations
  const getSelectedNodeIds = useCallback((): string[] => {
    return nodes.filter((n) => n.selected).map((n) => n.id);
  }, [nodes]);

  const handleStartSelected = useCallback(() => {
    const selectedIds = getSelectedNodeIds();
    for (const id of selectedIds) {
      const node = nodes.find((n) => n.id === id);
      if (node && node.data.state === 'stopped') {
        controlProcessor(id, 'start');
      }
    }
  }, [getSelectedNodeIds, nodes, controlProcessor]);

  const handleStopSelected = useCallback(() => {
    const selectedIds = getSelectedNodeIds();
    for (const id of selectedIds) {
      const node = nodes.find((n) => n.id === id);
      if (node && node.data.state === 'running') {
        controlProcessor(id, 'stop');
      }
    }
  }, [getSelectedNodeIds, nodes, controlProcessor]);

  const handleDeleteSelected = useCallback(() => {
    const selectedIds = getSelectedNodeIds();
    if (selectedIds.length === 0) return;
    setDeleteTarget({
      kind: 'multi',
      id: 'multi',
      label: `${selectedIds.length} processors`,
      nodeIds: selectedIds,
    });
  }, [getSelectedNodeIds]);

  const handleSelectAll = useCallback(() => {
    setNodes((prev) => prev.map((n) => ({ ...n, selected: true })));
    setContextMenu(null);
  }, [setNodes]);

  // --- Clipboard ---
  const clipboardRef = useRef<ClipboardEntry | null>(null);
  const pasteCountRef = useRef(0);

  const handleCopy = useCallback((silent = false) => {
    const selectedNodes = nodes.filter((n) => n.selected);
    if (selectedNodes.length === 0) return;
    const selectedIds = new Set(selectedNodes.map((n) => n.id));
    const internalEdges = edges.filter(
      (e) => selectedIds.has(e.source) && selectedIds.has(e.target),
    );
    clipboardRef.current = {
      nodes: selectedNodes.map((n) => ({
        id: n.id,
        type: n.type ?? 'processorNode',
        position: { ...n.position },
        data: { ...n.data },
      })),
      edges: internalEdges.map((e) => ({ ...e })),
    };
    pasteCountRef.current = 0;
    if (!silent) onToast('info', `Copied ${selectedNodes.length} component(s).`);
  }, [nodes, edges, onToast]);

  const handlePaste = useCallback(
    (offsetX = 40, offsetY = 40) => {
      const clip = clipboardRef.current;
      if (!clip || clip.nodes.length === 0) return;

      pasteCountRef.current += 1;
      const totalOffsetX = offsetX * pasteCountRef.current;
      const totalOffsetY = offsetY * pasteCountRef.current;

      const existingIds = new Set(nodes.map((n) => n.id));
      const idMap = new Map<string, string>();

      // Create new nodes with unique names
      for (const cn of clip.nodes) {
        let newId = cn.id;
        if (existingIds.has(newId)) {
          let suffix = 1;
          while (existingIds.has(`${cn.id} (copy${suffix > 1 ? ` ${suffix}` : ''})`)) suffix++;
          newId = `${cn.id} (copy${suffix > 1 ? ` ${suffix}` : ''})`;
        }
        idMap.set(cn.id, newId);
        existingIds.add(newId);
      }

      // Deselect current selection
      setNodes((prev) => prev.map((n) => ({ ...n, selected: false })));

      const newNodes: AnyNode[] = [];
      const apiCalls: Promise<void>[] = [];

      for (const cn of clip.nodes) {
        const newId = idMap.get(cn.id)!;
        const position = { x: cn.position.x + totalOffsetX, y: cn.position.y + totalOffsetY };

        if (cn.type === 'labelNode') {
          const labelData = cn.data as LabelNodeData;
          const newLabel: LabelFlowNode = {
            id: `label-pending-${Date.now()}-${newId}`,
            type: 'labelNode' as const,
            position,
            data: { ...labelData, labelId: '', pending: true },
            draggable: true,
            selectable: true,
            connectable: false,
            selected: true,
          };
          newNodes.push(newLabel);

          const tempId = newLabel.id;
          apiCalls.push(
            fetch('/api/v1/process-groups/root/labels', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                text: labelData.text,
                x: position.x,
                y: position.y,
                width: labelData.width,
                height: labelData.height,
                background_color: labelData.backgroundColor,
                font_size: labelData.fontSize,
              }),
            })
              .then((res) => {
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                return res.json() as Promise<{ id: string }>;
              })
              .then((created) => {
                setNodes((prev) =>
                  prev.map((n) =>
                    n.id === tempId
                      ? ({ ...n, id: `label-${created.id}`, data: { ...(n.data as LabelNodeData), labelId: created.id, pending: false } } as AnyNode)
                      : n,
                  ),
                );
              })
              .catch((err: unknown) => {
                setNodes((prev) => prev.filter((n) => n.id !== tempId));
                const msg = err instanceof Error ? err.message : String(err);
                onToast('error', `Failed to paste label: ${msg}`);
              }),
          );
        } else {
          const procData = cn.data as ProcessorNodeData;
          const newNode: AnyNode = {
            id: newId,
            type: cn.type as 'processorNode' | 'funnelNode',
            position,
            data: {
              ...procData,
              label: newId,
              state: 'stopped',
              metrics: null,
              bulletin: null,
              pending: true,
            },
            selected: true,
          };
          newNodes.push(newNode);

          apiCalls.push(
            fetch('/api/v1/processors', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                type: procData.typeName,
                name: newId,
                position,
                properties: {},
              }),
            })
              .then((res) => {
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                setNodes((prev) =>
                  prev.map((n) =>
                    n.id === newId ? ({ ...n, data: { ...n.data, pending: false } } as AnyNode) : n,
                  ),
                );
              })
              .catch((err: unknown) => {
                setNodes((prev) => prev.filter((n) => n.id !== newId));
                const msg = err instanceof Error ? err.message : String(err);
                onToast('error', `Failed to paste "${newId}": ${msg}`);
              }),
          );
        }
      }

      setNodes((prev) => [...prev, ...newNodes]);

      // Create connections between pasted nodes after API calls
      Promise.allSettled(apiCalls).then(() => {
        for (const ce of clip.edges) {
          const newSource = idMap.get(ce.source);
          const newTarget = idMap.get(ce.target);
          if (!newSource || !newTarget || !ce.data) continue;

          const relationship = ce.data.relationship;
          fetch('/api/v1/connections', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              source: newSource,
              relationship,
              destination: newTarget,
            }),
          })
            .then((res) => {
              if (!res.ok) throw new Error(`HTTP ${res.status}`);
              return res.json() as Promise<{ id: string }>;
            })
            .then((data) => {
              const edgeId = `${newSource}--${relationship}--${newTarget}--${Date.now()}`;
              const newEdge: ConnEdge = {
                id: edgeId,
                source: newSource,
                target: newTarget,
                sourceHandle: ce.sourceHandle,
                targetHandle: ce.targetHandle,
                type: 'connectionEdge' as const,
                data: {
                  relationship,
                  queuedCount: 0,
                  queuedBytes: 0,
                  backPressured: false,
                  connectionId: data.id,
                  pending: false,
                  onQueueClick: handleQueueClick,
                },
                style: { stroke: 'var(--border)' },
              };
              setEdges((prev) => addEdge(newEdge, prev));
            })
            .catch((err: unknown) => {
              const msg = err instanceof Error ? err.message : String(err);
              onToast('error', `Failed to recreate connection: ${msg}`);
            });
        }
      });

      onToast('info', `Pasting ${clip.nodes.length} component(s)...`);
    },
    [nodes, setNodes, setEdges, onToast, handleQueueClick],
  );

  const handleDuplicate = useCallback(() => {
    handleCopy(true);
    handlePaste(20, 20);
  }, [handleCopy, handlePaste]);

  // --- Alignment ---
  const handleAlign = useCallback(
    (action: AlignAction) => {
      const selectedNodes = nodes.filter((n) => n.selected);
      if (selectedNodes.length < 2) return;

      const alignFns: Record<AlignAction, (nodes: typeof selectedNodes) => Map<string, { x: number; y: number }>> = {
        'align-left': alignLeft,
        'align-center': alignCenter,
        'align-right': alignRight,
        'align-top': alignTop,
        'align-middle': alignMiddle,
        'align-bottom': alignBottom,
        'distribute-horizontal': distributeHorizontally,
        'distribute-vertical': distributeVertically,
      };

      const fn = alignFns[action];
      if (!fn) return;
      const newPositions = fn(selectedNodes);

      setNodes((prev) =>
        prev.map((n) => {
          const pos = newPositions.get(n.id);
          if (!pos) return n;
          return { ...n, position: pos };
        }),
      );

      // Persist positions and recalculate edge routing
      const movedIds = new Set<string>();
      for (const [id, pos] of newPositions) {
        persistPosition(id, pos.x, pos.y);
        movedIds.add(id);
      }
      recalcEdgeRouting(movedIds);
      setContextMenu(null);
    },
    [nodes, setNodes, persistPosition, recalcEdgeRouting],
  );

  // --- Canvas Search ---
  const handleSearchSelect = useCallback(
    (nodeId: string) => {
      const node = nodes.find((n) => n.id === nodeId);
      if (!node) return;
      // Deselect all, then select the found node
      setNodes((prev) =>
        prev.map((n) => ({
          ...n,
          selected: n.id === nodeId,
        })),
      );
      // Pan to node center
      const width = node.measured?.width ?? 220;
      const height = node.measured?.height ?? 100;
      setCenter(
        node.position.x + width / 2,
        node.position.y + height / 2,
        { zoom: 1, duration: 400 },
      );
    },
    [nodes, setNodes, setCenter],
  );

  // --- Fit View ---
  const handleFitView = useCallback(() => {
    fitView({ padding: 0.15, duration: 300 });
  }, [fitView]);

  // --- Check if any modal/dialog is open ---
  const isModalOpen = useCallback(() => {
    return !!(pendingDrop || deleteTarget || pendingConnectionContext || configTarget || colorPickerTarget || queueInspectTarget);
  }, [pendingDrop, deleteTarget, pendingConnectionContext, configTarget, colorPickerTarget, queueInspectTarget]);

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      const isInput =
        document.activeElement?.tagName === 'INPUT' ||
        document.activeElement?.tagName === 'TEXTAREA' ||
        (document.activeElement as HTMLElement)?.isContentEditable;
      const modalOpen = isModalOpen();

      // Escape always works
      if (e.key === 'Escape') {
        setContextMenu(null);
        setPendingDrop(null);
        setPendingConnectionContext(null);
        setDeleteTarget(null);
        setColorPickerTarget(null);
        setQueueInspectTarget(null);
        // Deselect all nodes
        if (!modalOpen) {
          setNodes((prev) => prev.map((n) => ({ ...n, selected: false })));
        }
        return;
      }

      // All other shortcuts disabled when modal open or input focused
      if (isInput || modalOpen) return;

      if (e.key === 'Delete' || e.key === 'Backspace') {
        const selectedIds = nodes.filter((n) => n.selected).map((n) => n.id);
        if (selectedIds.length > 1) {
          handleDeleteSelected();
          return;
        }

        const selectedNode = nodes.find((n) => n.selected);
        const selectedEdge = edges.find((edge) => edge.selected);

        if (selectedNode) {
          initiateDelete('node', selectedNode.id);
        } else if (selectedEdge) {
          initiateDelete('edge', selectedEdge.id);
        }
        return;
      }

      if (e.ctrlKey || e.metaKey) {
        switch (e.key) {
          case 'a':
            e.preventDefault();
            handleSelectAll();
            break;
          case 'c':
            e.preventDefault();
            handleCopy();
            break;
          case 'v':
            e.preventDefault();
            handlePaste();
            break;
          case 'd':
            e.preventDefault();
            handleDuplicate();
            break;
        }
      }
    },
    [nodes, edges, initiateDelete, handleDeleteSelected, handleSelectAll, handleCopy, handlePaste, handleDuplicate, isModalOpen, setNodes],
  );

  const handleNodeContextMenu: NodeMouseHandler<AnyNode> = useCallback(
    (event, node) => {
      event.preventDefault();
      const selectedIds = nodes.filter((n) => n.selected).map((n) => n.id);
      const nodeState = node.type === 'labelNode'
        ? 'stopped'
        : (node.data as ProcessorNodeData).state;
      setContextMenu({
        x: event.clientX,
        y: event.clientY,
        nodeId: node.id,
        edgeId: null,
        nodeState,
        selectedNodeIds: selectedIds.length > 1 ? selectedIds : undefined,
      });
    },
    [nodes],
  );

  const handleEdgeContextMenu: EdgeMouseHandler<ConnEdge> = useCallback(
    (event, edge) => {
      event.preventDefault();
      setContextMenu({
        x: event.clientX,
        y: event.clientY,
        nodeId: null,
        edgeId: edge.id,
      });
    },
    [],
  );

  const handleCanvasContextMenu = useCallback(
    (event: MouseEvent | React.MouseEvent) => {
      event.preventDefault();
      setContextMenu({
        x: event.clientX,
        y: event.clientY,
        nodeId: null,
        edgeId: null,
        isCanvas: true,
      });
    },
    [],
  );

  const handleContextDelete = useCallback(() => {
    if (!contextMenu) return;
    const { nodeId, edgeId } = contextMenu;
    setContextMenu(null);
    if (nodeId) initiateDelete('node', nodeId);
    else if (edgeId) initiateDelete('edge', edgeId);
  }, [contextMenu, initiateDelete]);

  const handleContextConfigure = useCallback(() => {
    if (!contextMenu?.nodeId) return;
    const nodeId = contextMenu.nodeId;
    const node = nodes.find((n) => n.id === nodeId);
    setContextMenu(null);
    if (node && node.type !== 'labelNode') {
      const procData = node.data as ProcessorNodeData;
      setConfigTarget({ name: node.id, state: procData.state });
    }
  }, [contextMenu, nodes]);

  const handleContextChangeColor = useCallback(() => {
    if (!contextMenu?.nodeId) return;
    const nodeId = contextMenu.nodeId;
    const node = nodes.find((n) => n.id === nodeId);
    setContextMenu(null);
    if (node && node.type !== 'labelNode') {
      const procData = node.data as ProcessorNodeData;
      setColorPickerTarget({
        nodeId: node.id,
        currentColor: procData.customColor ?? '',
      });
    }
  }, [contextMenu, nodes]);

  const handleContextViewQueue = useCallback(() => {
    if (!contextMenu?.edgeId) return;
    const edgeId = contextMenu.edgeId;
    const edge = edges.find((e) => e.id === edgeId);
    setContextMenu(null);
    if (edge && edge.data?.connectionId) {
      const label = `${edge.source} \u2192 ${edge.data.relationship} \u2192 ${edge.target}`;
      setQueueInspectTarget({ connectionId: edge.data.connectionId, label });
    }
  }, [contextMenu, edges]);

  const handleColorSelect = useCallback(
    (color: string) => {
      if (!colorPickerTarget) return;
      const targetIds = batchColorNodeIdsRef.current ?? [colorPickerTarget.nodeId];
      setColorPickerTarget(null);
      batchColorNodeIdsRef.current = null;
      const targetSet = new Set(targetIds);
      setNodes((prev) =>
        prev.map((n): AnyNode =>
          targetSet.has(n.id)
            ? { ...n, data: { ...n.data, customColor: color } } as AnyNode
            : n,
        ),
      );
      // Persist to localStorage
      const colors = loadSavedColors();
      for (const id of targetIds) {
        if (color) {
          colors[id] = color;
        } else {
          delete colors[id];
        }
      }
      saveColors(colors);
      savedColorsRef.current = colors;
    },
    [colorPickerTarget, setNodes],
  );

  const handleNodeDoubleClick: NodeMouseHandler<AnyNode> = useCallback(
    (_event, node) => {
      // Labels handle their own double-click (inline editing), skip config modal
      if (node.type === 'labelNode') return;
      const procData = node.data as ProcessorNodeData;
      setConfigTarget({ name: node.id, state: procData.state });
    },
    [],
  );

  const nodeColor = useCallback((node: AnyNode): string => {
    if (node.type === 'labelNode') {
      return (node.data as LabelNodeData).backgroundColor || 'rgba(255, 255, 200, 0.4)';
    }
    const procData = node.data as ProcessorNodeData;
    if (procData.pending) return 'var(--warning)';
    if (procData.customColor) return String(procData.customColor);
    return stateColor(procData.state ?? 'stopped');
  }, []);

  const existingNames = useMemo(() => new Set(nodes.map((n) => n.id)), [nodes]);

  return (
    <div
      className="flow-canvas-wrapper"
      aria-label="Flow topology canvas"
      onDragOver={handleDragOver}
      onDrop={handleDrop}
      onKeyDown={handleKeyDown}
      tabIndex={0}
    >
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={handleNodesChange}
        onEdgesChange={onEdgesChange}
        onConnect={handleConnect}
        onNodeDrag={handleNodeDrag}
        onNodeContextMenu={handleNodeContextMenu}
        onEdgeContextMenu={handleEdgeContextMenu}
        onPaneContextMenu={handleCanvasContextMenu}
        onNodeDoubleClick={handleNodeDoubleClick}
        onPaneClick={() => setContextMenu(null)}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        connectOnClick={false}
        selectionOnDrag
        onSelectionChange={(params) => {
          const nodeIds = (params.nodes ?? []).map((n: { id: string }) => n.id);
          onSelectionChange?.(nodeIds);
        }}
        fitView
        fitViewOptions={{ padding: 0.15 }}
        minZoom={0.2}
        maxZoom={3}
        attributionPosition="bottom-right"
        colorMode="dark"
        deleteKeyCode={null}
        multiSelectionKeyCode="Shift"
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
        <MiniMap<AnyNode>
          nodeColor={nodeColor}
          maskColor="rgba(15,17,23,0.7)"
          style={{
            background: 'var(--surface)',
            border: '1px solid var(--border)',
          }}
        />
        <Panel position="top-right" className="canvas-top-panel">
          <CanvasSearch nodes={nodes} onResultSelect={handleSearchSelect} />
          <button
            className="canvas-fit-btn"
            onClick={handleFitView}
            title="Fit to screen"
            aria-label="Fit all components to screen"
          >
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M15 3h6v6M9 21H3v-6M21 3l-7 7M3 21l7-7" />
            </svg>
          </button>
        </Panel>
      </ReactFlow>

      {draggedPlugin && (
        <div className="canvas-drop-hint" aria-hidden="true">
          Drop to add {draggedPlugin.display_name ?? draggedPlugin.type_name}
        </div>
      )}

      {pendingDrop && (
        <AddProcessorModal
          plugin={pendingDrop.plugin}
          existingNames={existingNames}
          onConfirm={handleAddProcessor}
          onCancel={() => setPendingDrop(null)}
        />
      )}

      {pendingConnectionContext && (
        <ConnectionModal
          sourceId={pendingConnectionContext.sourceId}
          targetId={pendingConnectionContext.targetId}
          relationships={pendingConnectionContext.relationships}
          existingRelationships={pendingConnectionContext.existingRels}
          onConfirm={handleConfirmConnection}
          onCancel={() => setPendingConnectionContext(null)}
        />
      )}

      {deleteTarget && (
        <ConfirmDialog
          title={
            deleteTarget.kind === 'multi'
              ? 'Delete Selected Items'
              : deleteTarget.kind === 'node'
                ? (deleteTarget.id.startsWith('label-') ? 'Delete Label' : 'Delete Processor')
                : 'Delete Connection'
          }
          message={
            deleteTarget.kind === 'multi'
              ? `Delete ${deleteTarget.nodeIds?.length ?? 0} selected items? This will also remove all their connections.`
              : deleteTarget.kind === 'node'
                ? (deleteTarget.id.startsWith('label-')
                    ? `Delete label "${deleteTarget.label}"?`
                    : `Delete processor "${deleteTarget.label}"? This will also remove all its connections.`)
                : `Delete connection ${deleteTarget.label}?`
          }
          confirmLabel="Delete"
          destructive
          onConfirm={handleDeleteConfirm}
          onCancel={() => setDeleteTarget(null)}
        />
      )}

      {contextMenu && (() => {
        const isLabelCtx = contextMenu.nodeId?.startsWith('label-');
        const isProcessorCtx = contextMenu.nodeId && !isLabelCtx;
        return (
          <ContextMenu
            menu={contextMenu}
            onDelete={handleContextDelete}
            onConfigure={isProcessorCtx ? handleContextConfigure : undefined}
            onStart={
              isProcessorCtx
                ? () => controlProcessor(contextMenu.nodeId!, 'start')
                : undefined
            }
            onStop={
              isProcessorCtx
                ? () => controlProcessor(contextMenu.nodeId!, 'stop')
                : undefined
            }
            onPause={
              isProcessorCtx
                ? () => controlProcessor(contextMenu.nodeId!, 'pause')
                : undefined
            }
            onResume={
              isProcessorCtx
                ? () => controlProcessor(contextMenu.nodeId!, 'resume')
                : undefined
            }
            onResetCircuit={
              isProcessorCtx
                ? () => controlProcessor(contextMenu.nodeId!, 'reset-circuit')
                : undefined
            }
            onChangeColor={isProcessorCtx ? handleContextChangeColor : undefined}
            onViewQueue={contextMenu.edgeId ? handleContextViewQueue : undefined}
            onSelectAll={contextMenu.isCanvas ? handleSelectAll : undefined}
            onStartSelected={handleStartSelected}
            onStopSelected={handleStopSelected}
            onDeleteSelected={handleDeleteSelected}
            onAlign={handleAlign}
            onClose={() => setContextMenu(null)}
          />
        );
      })()}

      {configTarget && (
        <ProcessorConfigModal
          processorName={configTarget.name}
          processorState={configTarget.state}
          onToast={onToast}
          onClose={() => setConfigTarget(null)}
        />
      )}

      {queueInspectTarget && (
        <QueueInspectorModal
          connectionId={queueInspectTarget.connectionId}
          connectionLabel={queueInspectTarget.label}
          onToast={onToast}
          onClose={() => setQueueInspectTarget(null)}
        />
      )}

      {colorPickerTarget && (
        <ColorPickerDialog
          processorName={colorPickerTarget.nodeId}
          currentColor={colorPickerTarget.currentColor}
          onSelect={handleColorSelect}
          onClose={() => setColorPickerTarget(null)}
        />
      )}
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
