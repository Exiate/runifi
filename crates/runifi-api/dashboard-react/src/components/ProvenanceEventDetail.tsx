import { memo, useState, useEffect, useCallback } from 'react';
import type { ProvenanceEvent } from '../types/api';
import type { ToastKind } from '../hooks/useToast';
import { ConfirmDialog } from './ConfirmDialog';
import { formatBytes } from '../utils/format';

type Tab = 'details' | 'attributes' | 'content';

interface ProvenanceEventDetailProps {
  event: ProvenanceEvent;
  onClose: () => void;
  onViewLineage: (flowfileId: number) => void;
  onToast: (kind: ToastKind, message: string) => void;
}

function formatFullTimestamp(ms: number): string {
  return new Date(ms).toLocaleString();
}

function ProvenanceEventDetailInner({
  event,
  onClose,
  onViewLineage,
  onToast,
}: ProvenanceEventDetailProps) {
  const [activeTab, setActiveTab] = useState<Tab>('details');
  const [confirmReplay, setConfirmReplay] = useState(false);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !confirmReplay) onClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose, confirmReplay]);

  const handleReplay = useCallback(() => {
    fetch(`/api/v1/provenance/${event.event_id}/replay`, { method: 'POST' })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data) => {
        onToast('success', `Replay initiated: ${data.message || 'Success'}`);
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        onToast('error', `Replay failed: ${msg}`);
      });
  }, [event.event_id, onToast]);

  // Build attribute diff data
  const prevAttrs = event.previous_attributes ?? [];
  const currAttrs = event.attributes ?? [];
  const hasDiff = prevAttrs.length > 0;

  const diffRows = (() => {
    if (!hasDiff) return null;
    const prevMap = new Map(prevAttrs.map((a) => [a.key, a.value]));
    const currMap = new Map(currAttrs.map((a) => [a.key, a.value]));
    const allKeys = new Set([...prevMap.keys(), ...currMap.keys()]);
    return Array.from(allKeys).sort().map((key) => ({
      key,
      prev: prevMap.get(key) ?? '',
      curr: currMap.get(key) ?? '',
      changed: (prevMap.get(key) ?? '') !== (currMap.get(key) ?? ''),
    }));
  })();

  const downloadUrl = `/api/v1/provenance/events/${event.event_id}/content`;

  return (
    <>
      <div
        className="modal-overlay"
        role="dialog"
        aria-modal="true"
        aria-labelledby="provenance-detail-title"
        style={{ zIndex: 1100 }}
        onClick={(e) => {
          if (e.target === e.currentTarget) onClose();
        }}
      >
        <div className="modal-panel provenance-detail-panel">
          <div className="config-modal-header">
            <div>
              <h3 id="provenance-detail-title" className="modal-title">
                Provenance Event #{event.event_id}
              </h3>
              <span className="modal-type-tag">{event.event_type}</span>
            </div>
            <button className="config-close-btn" onClick={onClose} aria-label="Close">
              &times;
            </button>
          </div>

          <div className="config-tabs">
            <button
              className={`config-tab${activeTab === 'details' ? ' active' : ''}`}
              onClick={() => setActiveTab('details')}
            >
              Details
            </button>
            <button
              className={`config-tab${activeTab === 'attributes' ? ' active' : ''}`}
              onClick={() => setActiveTab('attributes')}
            >
              Attributes
            </button>
            <button
              className={`config-tab${activeTab === 'content' ? ' active' : ''}`}
              onClick={() => setActiveTab('content')}
            >
              Content
            </button>
          </div>

          <div className="config-tab-content">
            {activeTab === 'details' && (
              <div className="ff-detail-meta">
                <div className="detail-row">
                  <span className="detail-label">Event ID</span>
                  <span className="detail-value">{event.event_id}</span>
                </div>
                <div className="detail-row">
                  <span className="detail-label">FlowFile ID</span>
                  <span className="detail-value">{event.flowfile_id}</span>
                </div>
                <div className="detail-row">
                  <span className="detail-label">Event Type</span>
                  <span className="detail-value">
                    <span className={`provenance-type-badge provenance-type-${event.event_type.toLowerCase()}`}>
                      {event.event_type}
                    </span>
                  </span>
                </div>
                <div className="detail-row">
                  <span className="detail-label">Processor</span>
                  <span className="detail-value">{event.processor_name}</span>
                </div>
                <div className="detail-row">
                  <span className="detail-label">Processor Type</span>
                  <span className="detail-value">{event.processor_type}</span>
                </div>
                <div className="detail-row">
                  <span className="detail-label">Timestamp</span>
                  <span className="detail-value">{formatFullTimestamp(event.timestamp_ms)}</span>
                </div>
                <div className="detail-row">
                  <span className="detail-label">Content Size</span>
                  <span className="detail-value">{formatBytes(event.content_size)}</span>
                </div>
                {event.relationship && (
                  <div className="detail-row">
                    <span className="detail-label">Relationship</span>
                    <span className="detail-value">{event.relationship}</span>
                  </div>
                )}
                <div className="detail-row">
                  <span className="detail-label">Lineage Start ID</span>
                  <span className="detail-value">{event.lineage_start_id}</span>
                </div>
                {event.details && (
                  <div className="detail-row">
                    <span className="detail-label">Details</span>
                    <span className="detail-value">{event.details}</span>
                  </div>
                )}
                {event.parent_flowfile_ids && event.parent_flowfile_ids.length > 0 && (
                  <div className="detail-row">
                    <span className="detail-label">Parent FlowFile IDs</span>
                    <span className="detail-value">{event.parent_flowfile_ids.join(', ')}</span>
                  </div>
                )}
                {event.child_flowfile_ids && event.child_flowfile_ids.length > 0 && (
                  <div className="detail-row">
                    <span className="detail-label">Child FlowFile IDs</span>
                    <span className="detail-value">{event.child_flowfile_ids.join(', ')}</span>
                  </div>
                )}
                {event.transit_uri && (
                  <div className="detail-row">
                    <span className="detail-label">Transit URI</span>
                    <span className="detail-value">{event.transit_uri}</span>
                  </div>
                )}
              </div>
            )}

            {activeTab === 'attributes' && (
              <div className="provenance-attr-section">
                {hasDiff && diffRows ? (
                  <>
                    <p className="provenance-attr-note">Showing attribute changes for this event.</p>
                    <table className="provenance-attr-table">
                      <thead>
                        <tr>
                          <th>Attribute</th>
                          <th>Previous Value</th>
                          <th>Current Value</th>
                        </tr>
                      </thead>
                      <tbody>
                        {diffRows.map((row) => (
                          <tr key={row.key} className={row.changed ? 'provenance-attr-changed' : ''}>
                            <td className="provenance-attr-key">{row.key}</td>
                            <td className="provenance-attr-prev">{row.prev || '\u2014'}</td>
                            <td className="provenance-attr-curr">{row.curr || '\u2014'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </>
                ) : (
                  <>
                    {currAttrs.length === 0 ? (
                      <p className="config-empty">No attributes</p>
                    ) : (
                      <table className="provenance-attr-table">
                        <thead>
                          <tr>
                            <th>Attribute</th>
                            <th>Value</th>
                          </tr>
                        </thead>
                        <tbody>
                          {currAttrs.map((attr) => (
                            <tr key={attr.key}>
                              <td className="provenance-attr-key">{attr.key}</td>
                              <td>{attr.value}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    )}
                  </>
                )}
              </div>
            )}

            {activeTab === 'content' && (
              <div className="provenance-content-section">
                <div className="ff-detail-meta">
                  <div className="detail-row">
                    <span className="detail-label">Content Size</span>
                    <span className="detail-value">{formatBytes(event.content_size)}</span>
                  </div>
                  {event.content_claim_id != null && (
                    <div className="detail-row">
                      <span className="detail-label">Content Claim ID</span>
                      <span className="detail-value">{event.content_claim_id}</span>
                    </div>
                  )}
                </div>
                <div className="provenance-content-actions">
                  <a
                    href={downloadUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="btn btn-ghost"
                  >
                    Download Content
                  </a>
                </div>
              </div>
            )}
          </div>

          <div className="modal-actions">
            <button
              className="btn btn-ghost"
              onClick={() => {
                onClose();
                onViewLineage(event.flowfile_id);
              }}
            >
              View Lineage
            </button>
            <button
              className="btn btn-primary"
              onClick={() => setConfirmReplay(true)}
            >
              Replay
            </button>
            <button className="btn btn-ghost" onClick={onClose}>
              Close
            </button>
          </div>
        </div>
      </div>

      {confirmReplay && (
        <ConfirmDialog
          title="Replay FlowFile"
          message={`Replay FlowFile #${event.flowfile_id} from event #${event.event_id} (${event.event_type} at ${event.processor_name})?`}
          confirmLabel="Replay"
          onConfirm={() => {
            setConfirmReplay(false);
            handleReplay();
          }}
          onCancel={() => setConfirmReplay(false)}
        />
      )}
    </>
  );
}

export const ProvenanceEventDetail = memo(ProvenanceEventDetailInner);
