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
} from '@xyflow/react';

import { ProcessorNode } from './ProcessorNode';
import { FunnelNode } from './FunnelNode';
import { LabelNode } from './LabelNode';
import { ConnectionEdge } from './ConnectionEdge';
import { AddProcessorModal } from './AddProcessorModal';
import { ConnectionModal } from './ConnectionModal';
import { ConfirmDialog } from './ConfirmDialog';
import { ContextMenu, type ContextMenuState } from './ContextMenu';
import { ProcessorConfigModal } from './ProcessorConfigModal';
import { QueueInspectorModal } from './QueueInspectorModal';
import { ColorPickerDialog } from './ColorPickerDialog';
import { computeLayout } from '../utils/layout';
import { stateColor } from '../utils/format';
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

export interface FlowCanvasProps {
  topology: FlowResponse;
  liveMetrics: SseMetricsEvent | null;
  plugins: PluginDescriptor[];
  onToast: (kind: ToastKind, message: string) => void;
  draggedPlugin: PluginDescriptor | null;
  addPluginAtCenter?: PluginDescriptor | null;
  onAddPluginHandled?: () => void;
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
): ConnEdge[] {
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
      sourceHandle: conn.relationship,
      targetHandle: 'target',
      type: 'connectionEdge' as const,
      data: {
        relationship: conn.relationship,
        queuedCount: live?.queued_count ?? 0,
        queuedBytes: live?.queued_bytes ?? 0,
        backPressured,
        connectionId: live?.id ?? '',
        pending: false,
        onQueueClick,
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
}: FlowCanvasProps) {
  const { screenToFlowPosition, getViewport } = useReactFlow();

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
    () => buildEdges(topology, null, handleQueueClick),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [topology],
  );

  const [nodes, setNodes, onNodesChange] = useNodesState<AnyNode>(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState<ConnEdge>(initialEdges);

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

    setNodes([...procNodes, ...labelNodes]);
    setEdges(buildEdges(topology, liveMetrics, handleQueueClick));
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

  const handleNodesChange: typeof onNodesChange = useCallback(
    (changes) => {
      for (const change of changes) {
        if (change.type === 'position' && change.position && !change.dragging) {
          persistPosition(change.id, change.position.x, change.position.y);
        }
      }
      onNodesChange(changes);
    },
    [onNodesChange, persistPosition],
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
                prev.map((n) =>
                  n.id === newLabel.id
                    ? {
                        ...n,
                        id: `label-${created.id}`,
                        data: {
                          ...(n.data as LabelNodeData),
                          labelId: created.id,
                          pending: false,
                        },
                      }
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
            prev.map((n) =>
              n.id === name ? { ...n, data: { ...n.data, pending: false } } : n,
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

      const edgeId = `${sourceId}--${relationship}--${targetId}--${Date.now()}`;
      const newEdge: ConnEdge = {
        id: edgeId,
        source: sourceId,
        target: targetId,
        sourceHandle: relationship,
        targetHandle: 'target',
        type: 'connectionEdge' as const,
        data: {
          relationship,
          queuedCount: 0,
          queuedBytes: 0,
          backPressured: false,
          connectionId: '',
          pending: true,
          onQueueClick: handleQueueClick,
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
    [pendingConnectionContext, setEdges, onToast, handleQueueClick],
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

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'Delete' || e.key === 'Backspace') {
        if (
          document.activeElement?.tagName === 'INPUT' ||
          document.activeElement?.tagName === 'TEXTAREA'
        )
          return;

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
      }
      if (e.key === 'Escape') {
        setContextMenu(null);
        setPendingDrop(null);
        setPendingConnectionContext(null);
        setDeleteTarget(null);
        setColorPickerTarget(null);
      }
      if ((e.ctrlKey || e.metaKey) && e.key === 'a') {
        if (
          document.activeElement?.tagName === 'INPUT' ||
          document.activeElement?.tagName === 'TEXTAREA'
        )
          return;
        e.preventDefault();
        handleSelectAll();
      }
    },
    [nodes, edges, initiateDelete, handleDeleteSelected, handleSelectAll],
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
      const { nodeId } = colorPickerTarget;
      setColorPickerTarget(null);
      setNodes((prev) =>
        prev.map((n) =>
          n.id === nodeId
            ? { ...n, data: { ...n.data, customColor: color } }
            : n,
        ),
      );
      // Persist to localStorage
      const colors = loadSavedColors();
      if (color) {
        colors[nodeId] = color;
      } else {
        delete colors[nodeId];
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
        onNodeContextMenu={handleNodeContextMenu}
        onEdgeContextMenu={handleEdgeContextMenu}
        onPaneContextMenu={handleCanvasContextMenu}
        onNodeDoubleClick={handleNodeDoubleClick}
        onPaneClick={() => setContextMenu(null)}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        connectOnClick={false}
        selectionOnDrag
        fitView
        fitViewOptions={{ padding: 0.15 }}
        minZoom={0.3}
        maxZoom={2}
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
