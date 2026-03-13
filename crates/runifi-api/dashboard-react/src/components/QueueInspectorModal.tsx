import { memo, useState, useEffect, useCallback } from 'react';
import type { QueueResponse, FlowFileEntry } from '../types/api';
import { ConfirmDialog } from './ConfirmDialog';
import { formatBytes, formatAge } from '../utils/format';
import type { ToastKind } from '../hooks/useToast';

const PAGE_SIZE = 50;

interface QueueInspectorModalProps {
  connectionId: string;
  connectionLabel: string;
  onToast: (kind: ToastKind, message: string) => void;
  onClose: () => void;
}

interface FlowFileDetailModalProps {
  flowfile: FlowFileEntry;
  connectionId: string;
  onClose: () => void;
}

function FlowFileDetailModal({ flowfile, connectionId, onClose }: FlowFileDetailModalProps) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  const downloadUrl = `/api/v1/connections/${encodeURIComponent(connectionId)}/queue/${flowfile.id}/content`;

  return (
    <div
      className="modal-overlay"
      role="dialog"
      aria-modal="true"
      aria-labelledby="ff-detail-title"
      style={{ zIndex: 1100 }}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="modal-panel ff-detail-panel">
        <div className="config-modal-header">
          <h3 id="ff-detail-title" className="modal-title">
            FlowFile #{flowfile.id}
          </h3>
          <button className="config-close-btn" onClick={onClose} aria-label="Close">
            &times;
          </button>
        </div>

        <div className="ff-detail-meta">
          <div className="detail-row">
            <span className="detail-label">ID</span>
            <span className="detail-value">{flowfile.id}</span>
          </div>
          <div className="detail-row">
            <span className="detail-label">Size</span>
            <span className="detail-value">{formatBytes(flowfile.size)}</span>
          </div>
          <div className="detail-row">
            <span className="detail-label">Age</span>
            <span className="detail-value">{formatAge(flowfile.age_ms)}</span>
          </div>
          <div className="detail-row">
            <span className="detail-label">Has Content</span>
            <span className="detail-value">{flowfile.has_content ? 'Yes' : 'No'}</span>
          </div>
        </div>

        <div className="ff-attr-section">
          <h4 className="ff-attr-heading">Attributes</h4>
          {flowfile.attributes.length === 0 ? (
            <p className="config-empty">No attributes</p>
          ) : (
            <table className="rel-table">
              <thead>
                <tr>
                  <th>Key</th>
                  <th>Value</th>
                </tr>
              </thead>
              <tbody>
                {flowfile.attributes.map((attr) => (
                  <tr key={attr.key}>
                    <td>{attr.key}</td>
                    <td>{attr.value}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        <div className="modal-actions">
          {flowfile.has_content && (
            <a
              href={downloadUrl}
              target="_blank"
              rel="noreferrer"
              className="btn btn-ghost"
            >
              Download Content
            </a>
          )}
          <button className="btn btn-primary" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

function QueueInspectorModalInner({
  connectionId,
  connectionLabel,
  onToast,
  onClose,
}: QueueInspectorModalProps) {
  const [queueData, setQueueData] = useState<QueueResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [offset, setOffset] = useState(0);
  const [selectedFlowFile, setSelectedFlowFile] = useState<FlowFileEntry | null>(null);
  const [confirmEmpty, setConfirmEmpty] = useState(false);
  const [confirmRemoveId, setConfirmRemoveId] = useState<number | null>(null);

  const loadPage = useCallback(
    (pageOffset: number) => {
      setLoadError(null);
      const url = `/api/v1/connections/${encodeURIComponent(connectionId)}/queue?offset=${pageOffset}&limit=${PAGE_SIZE}`;
      fetch(url)
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          return res.json() as Promise<QueueResponse>;
        })
        .then((data) => setQueueData(data))
        .catch((err: unknown) => {
          setLoadError(err instanceof Error ? err.message : String(err));
        });
    },
    [connectionId],
  );

  useEffect(() => {
    loadPage(0);
  }, [loadPage]);

  // Close on Escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !selectedFlowFile && !confirmEmpty && confirmRemoveId === null) {
        onClose();
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose, selectedFlowFile, confirmEmpty, confirmRemoveId]);

  const handleEmptyQueue = useCallback(() => {
    fetch(`/api/v1/connections/${encodeURIComponent(connectionId)}/queue`, {
      method: 'DELETE',
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then(() => {
        onToast('success', 'Queue emptied.');
        setOffset(0);
        loadPage(0);
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        onToast('error', `Failed to empty queue: ${msg}`);
      });
  }, [connectionId, loadPage, onToast]);

  const handleRemoveFlowFile = useCallback(
    (ffId: number) => {
      fetch(
        `/api/v1/connections/${encodeURIComponent(connectionId)}/queue/${ffId}`,
        { method: 'DELETE' },
      )
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          return res.json();
        })
        .then(() => {
          onToast('success', `FlowFile #${ffId} removed.`);
          loadPage(offset);
        })
        .catch((err: unknown) => {
          const msg = err instanceof Error ? err.message : String(err);
          onToast('error', `Failed to remove FlowFile: ${msg}`);
        });
    },
    [connectionId, offset, loadPage, onToast],
  );

  const totalCount = queueData?.total_count ?? 0;
  const totalPages = Math.ceil(totalCount / PAGE_SIZE);
  const currentPage = Math.floor(offset / PAGE_SIZE) + 1;

  const goToPrev = () => {
    const newOffset = Math.max(0, offset - PAGE_SIZE);
    setOffset(newOffset);
    loadPage(newOffset);
  };

  const goToNext = () => {
    const newOffset = offset + PAGE_SIZE;
    setOffset(newOffset);
    loadPage(newOffset);
  };

  return (
    <>
      <div
        className="modal-overlay"
        role="dialog"
        aria-modal="true"
        aria-labelledby="queue-modal-title"
        onClick={(e) => {
          if (e.target === e.currentTarget) onClose();
        }}
      >
        <div className="modal-panel queue-panel">
          <div className="config-modal-header">
            <div>
              <h3 id="queue-modal-title" className="modal-title">
                Queue Inspector
              </h3>
              <span className="modal-type-tag">{connectionLabel}</span>
            </div>
            <button className="config-close-btn" onClick={onClose} aria-label="Close">
              &times;
            </button>
          </div>

          <div className="queue-summary-row">
            <span className="queue-summary-count">
              {totalCount.toLocaleString()} FlowFile{totalCount !== 1 ? 's' : ''} queued
            </span>
            <button
              className="btn btn-danger"
              style={{ fontSize: '0.78rem', padding: '0.3rem 0.7rem' }}
              disabled={totalCount === 0}
              onClick={() => setConfirmEmpty(true)}
            >
              Empty Queue
            </button>
          </div>

          {loadError && (
            <p className="config-load-error">Failed to load queue: {loadError}</p>
          )}

          {!loadError && !queueData && (
            <p className="config-loading">Loading...</p>
          )}

          {queueData && (
            <>
              <div className="queue-table-wrap">
                <table className="queue-table">
                  <thead>
                    <tr>
                      <th>#</th>
                      <th>ID</th>
                      <th>Size</th>
                      <th>Age</th>
                      <th>Content</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {queueData.flowfiles.length === 0 ? (
                      <tr>
                        <td colSpan={6} className="queue-empty-row">
                          Queue is empty
                        </td>
                      </tr>
                    ) : (
                      queueData.flowfiles.map((ff) => (
                        <tr
                          key={ff.id}
                          className="queue-row"
                          onClick={() => setSelectedFlowFile(ff)}
                          style={{ cursor: 'pointer' }}
                        >
                          <td className="queue-cell-dim">{ff.position + 1}</td>
                          <td className="queue-cell-mono">{ff.id}</td>
                          <td className="queue-cell-dim">{formatBytes(ff.size)}</td>
                          <td className="queue-cell-dim">{formatAge(ff.age_ms)}</td>
                          <td className="queue-cell-dim">{ff.has_content ? 'Yes' : 'No'}</td>
                          <td>
                            <div
                              className="queue-actions"
                              onClick={(e) => e.stopPropagation()}
                            >
                              <button
                                className="btn-link"
                                onClick={() => setSelectedFlowFile(ff)}
                              >
                                View
                              </button>
                              {ff.has_content && (
                                <a
                                  href={`/api/v1/connections/${encodeURIComponent(connectionId)}/queue/${ff.id}/content`}
                                  target="_blank"
                                  rel="noreferrer"
                                  className="btn-link"
                                >
                                  Download
                                </a>
                              )}
                              <button
                                className="btn-link btn-link-danger"
                                onClick={() => setConfirmRemoveId(ff.id)}
                              >
                                Remove
                              </button>
                            </div>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>

              {totalPages > 1 && (
                <div className="queue-pagination">
                  <button
                    className="btn btn-ghost"
                    style={{ fontSize: '0.78rem', padding: '0.3rem 0.7rem' }}
                    disabled={currentPage <= 1}
                    onClick={goToPrev}
                  >
                    Previous
                  </button>
                  <span className="queue-page-info">
                    Page {currentPage} of {totalPages}
                  </span>
                  <button
                    className="btn btn-ghost"
                    style={{ fontSize: '0.78rem', padding: '0.3rem 0.7rem' }}
                    disabled={currentPage >= totalPages}
                    onClick={goToNext}
                  >
                    Next
                  </button>
                </div>
              )}
            </>
          )}

          <div className="modal-actions">
            <button className="btn btn-ghost" onClick={onClose}>
              Close
            </button>
          </div>
        </div>
      </div>

      {selectedFlowFile && (
        <FlowFileDetailModal
          flowfile={selectedFlowFile}
          connectionId={connectionId}
          onClose={() => setSelectedFlowFile(null)}
        />
      )}

      {confirmEmpty && (
        <ConfirmDialog
          title="Empty Queue"
          message="Empty the entire queue? This will remove all FlowFiles from this connection."
          confirmLabel="Empty"
          destructive
          onConfirm={() => {
            setConfirmEmpty(false);
            handleEmptyQueue();
          }}
          onCancel={() => setConfirmEmpty(false)}
        />
      )}

      {confirmRemoveId !== null && (
        <ConfirmDialog
          title="Remove FlowFile"
          message={`Remove FlowFile #${confirmRemoveId} from the queue?`}
          confirmLabel="Remove"
          destructive
          onConfirm={() => {
            const id = confirmRemoveId;
            setConfirmRemoveId(null);
            handleRemoveFlowFile(id);
          }}
          onCancel={() => setConfirmRemoveId(null)}
        />
      )}
    </>
  );
}

export const QueueInspectorModal = memo(QueueInspectorModalInner);
