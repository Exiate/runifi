import { memo, useState, useEffect, useCallback, useMemo } from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  type Node,
  type Edge,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import type { ProvenanceEvent, ProvenanceLineageResponse } from '../types/api';
import { formatTimestamp } from '../utils/format';

interface ProvenanceLineageGraphProps {
  flowfileId: number;
  onClose: () => void;
  onSelectEvent: (event: ProvenanceEvent) => void;
}

const EVENT_TYPE_COLORS: Record<string, string> = {
  CREATE: '#34d399',
  SEND: '#4f8ff7',
  RECEIVE: '#4f8ff7',
  ROUTE: '#60a5fa',
  CLONE: '#a78bfa',
  FORK: '#a78bfa',
  JOIN: '#a78bfa',
  CONTENT_MODIFIED: '#fbbf24',
  ATTRIBUTES_MODIFIED: '#fbbf24',
  FETCH: '#38bdf8',
  DROP: '#f87171',
  EXPIRE: '#f87171',
  REPLAY: '#34d399',
  DOWNLOAD: '#38bdf8',
  ADDINFO: '#8b90a0',
};

function getNodeColor(eventType: string): string {
  return EVENT_TYPE_COLORS[eventType] ?? '#8b90a0';
}

function ProvenanceLineageGraphInner({
  flowfileId,
  onClose,
  onSelectEvent,
}: ProvenanceLineageGraphProps) {
  const [events, setEvents] = useState<ProvenanceEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchLineage = useCallback(() => {
    setLoading(true);
    setError(null);
    fetch(`/api/v1/provenance/${flowfileId}/lineage`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json() as Promise<ProvenanceLineageResponse>;
      })
      .then((data) => {
        setEvents(data.events ?? []);
      })
      .catch((err: unknown) => {
        setError(err instanceof Error ? err.message : String(err));
      })
      .finally(() => setLoading(false));
  }, [flowfileId]);

  useEffect(() => {
    fetchLineage();
  }, [fetchLineage]);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  const { nodes, edges } = useMemo(() => {
    if (events.length === 0) return { nodes: [] as Node[], edges: [] as Edge[] };

    // Sort events by timestamp
    const sorted = [...events].sort((a, b) => a.timestamp_ms - b.timestamp_ms);

    const nodeList: Node[] = sorted.map((evt, idx) => ({
      id: String(evt.event_id),
      position: { x: 60, y: idx * 100 },
      data: {
        label: (
          <div
            className="provenance-lineage-node"
            style={{ borderLeftColor: getNodeColor(evt.event_type) }}
          >
            <div className="provenance-lineage-node-type">{evt.event_type}</div>
            <div className="provenance-lineage-node-proc">{evt.processor_name}</div>
            <div className="provenance-lineage-node-time">{formatTimestamp(evt.timestamp_ms)}</div>
          </div>
        ),
      },
      style: {
        background: '#1a1d27',
        border: `1px solid ${getNodeColor(evt.event_type)}`,
        borderRadius: '6px',
        padding: 0,
        color: '#e1e4ed',
        width: 220,
      },
    }));

    const edgeList: Edge[] = [];
    const eventById = new Map(sorted.map((e) => [e.event_id, e]));

    // Connect consecutive events by timestamp for the same flowfile_id
    for (let i = 1; i < sorted.length; i++) {
      const prev = sorted[i - 1];
      const curr = sorted[i];

      // Only connect sequential events for the same flowfile
      if (prev.flowfile_id === curr.flowfile_id) {
        edgeList.push({
          id: `e-${prev.event_id}-${curr.event_id}`,
          source: String(prev.event_id),
          target: String(curr.event_id),
          style: { stroke: '#4f8ff7', strokeWidth: 1.5 },
          animated: false,
        });
      }
    }

    // Connect events with parent_flowfile_ids:
    // For FORK/CLONE child events, find the last event of the parent flowfile
    // and connect it to this event
    const lastEventByFlowfile = new Map<number, ProvenanceEvent>();
    for (const evt of sorted) {
      lastEventByFlowfile.set(evt.flowfile_id, evt);
    }

    for (const evt of sorted) {
      if (evt.parent_flowfile_ids && evt.parent_flowfile_ids.length > 0) {
        for (const parentFfId of evt.parent_flowfile_ids) {
          const parentEvt = lastEventByFlowfile.get(parentFfId);
          if (parentEvt && !edgeList.some((e) => e.source === String(parentEvt.event_id) && e.target === String(evt.event_id))) {
            edgeList.push({
              id: `e-parent-${parentEvt.event_id}-${evt.event_id}`,
              source: String(parentEvt.event_id),
              target: String(evt.event_id),
              style: { stroke: '#a78bfa', strokeWidth: 1.5, strokeDasharray: '4 2' },
              animated: true,
            });
          }
        }
      }
    }

    // Suppress unused-variable warning
    void eventById;

    return { nodes: nodeList, edges: edgeList };
  }, [events]);

  const handleNodeClick = useCallback(
    (_event: React.MouseEvent, node: Node) => {
      const evt = events.find((e) => String(e.event_id) === node.id);
      if (evt) onSelectEvent(evt);
    },
    [events, onSelectEvent],
  );

  return (
    <div
      className="modal-overlay"
      role="dialog"
      aria-modal="true"
      aria-labelledby="lineage-title"
      style={{ zIndex: 1100 }}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="modal-panel provenance-lineage-panel" onClick={(e) => e.stopPropagation()}>
        <div className="config-modal-header">
          <div>
            <h3 id="lineage-title" className="modal-title">
              FlowFile Lineage
            </h3>
            <span className="modal-type-tag">FlowFile #{flowfileId}</span>
          </div>
          <button className="config-close-btn" onClick={onClose} aria-label="Close">
            &times;
          </button>
        </div>

        <div className="provenance-lineage-container">
          {loading && (
            <div className="provenance-lineage-status">Loading lineage...</div>
          )}
          {error && (
            <div className="provenance-lineage-status provenance-lineage-error">
              Failed to load lineage: {error}
            </div>
          )}
          {!loading && !error && events.length === 0 && (
            <div className="provenance-lineage-status">No lineage events found.</div>
          )}
          {!loading && !error && events.length > 0 && (
            <ReactFlow
              nodes={nodes}
              edges={edges}
              onNodeClick={handleNodeClick}
              fitView
              fitViewOptions={{ padding: 0.3 }}
              proOptions={{ hideAttribution: true }}
              minZoom={0.3}
              maxZoom={2}
              nodesDraggable
              nodesConnectable={false}
              elementsSelectable
              colorMode="dark"
            >
              <Background color="#2a2e3d" gap={20} />
              <Controls showInteractive={false} />
            </ReactFlow>
          )}
        </div>

        <div className="modal-actions">
          <button className="btn btn-ghost" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

export const ProvenanceLineageGraph = memo(ProvenanceLineageGraphInner);
