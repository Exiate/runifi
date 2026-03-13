import { memo, useState, useEffect, useCallback } from 'react';
import type { BulletinResponse } from '../types/api';
import { formatTimestamp } from '../utils/format';

interface BulletinBoardProps {
  processorNames: string[];
  onClose: () => void;
}

type SeverityFilter = '' | 'warn' | 'error';

function BulletinDetailModal({
  bulletin,
  onClose,
}: {
  bulletin: BulletinResponse;
  onClose: () => void;
}) {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  const time = new Date(bulletin.timestamp_ms).toLocaleString();

  return (
    <div
      className="modal-overlay"
      role="dialog"
      aria-modal="true"
      aria-labelledby="bulletin-detail-title"
      style={{ zIndex: 1200 }}
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="modal-panel" style={{ maxWidth: 480 }}>
        <h3 id="bulletin-detail-title" className="modal-title">
          Bulletin Detail
        </h3>
        <div className="ff-detail-meta">
          <div className="detail-row">
            <span className="detail-label">Severity</span>
            <span className={`detail-value bulletin-sev-${bulletin.severity}`}>
              {bulletin.severity.toUpperCase()}
            </span>
          </div>
          <div className="detail-row">
            <span className="detail-label">Processor</span>
            <span className="detail-value">{bulletin.processor_name}</span>
          </div>
          <div className="detail-row">
            <span className="detail-label">Time</span>
            <span className="detail-value">{time}</span>
          </div>
          <div className="detail-row">
            <span className="detail-label">Message</span>
            <span className="detail-value">{bulletin.message}</span>
          </div>
        </div>
        <div className="modal-actions">
          <button className="btn btn-primary" onClick={onClose}>
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

function BulletinBoardInner({ processorNames, onClose }: BulletinBoardProps) {
  const [bulletins, setBulletins] = useState<BulletinResponse[]>([]);
  const [severityFilter, setSeverityFilter] = useState<SeverityFilter>('');
  const [processorFilter, setProcessorFilter] = useState('');
  const [selectedBulletin, setSelectedBulletin] = useState<BulletinResponse | null>(null);

  const fetchBulletins = useCallback(
    (severity: SeverityFilter, processor: string) => {
      const params = new URLSearchParams();
      if (severity) params.set('severity', severity);
      if (processor) params.set('processor', processor);
      const qs = params.toString();
      fetch(`/api/v1/bulletins${qs ? `?${qs}` : ''}`)
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          return res.json() as Promise<BulletinResponse[]>;
        })
        .then((data) => setBulletins(data.slice().reverse()))
        .catch(() => {});
    },
    [],
  );

  useEffect(() => {
    fetchBulletins(severityFilter, processorFilter);
  }, [fetchBulletins, severityFilter, processorFilter]);

  return (
    <>
      <div className="bulletin-panel" role="complementary" aria-label="Bulletin board">
        <div className="bulletin-panel-header">
          <span className="bulletin-panel-title">Bulletin Board</span>
          <div className="bulletin-filters">
            <select
              className="bulletin-filter-select"
              value={severityFilter}
              onChange={(e) => setSeverityFilter(e.target.value as SeverityFilter)}
              aria-label="Filter by severity"
            >
              <option value="">All Severities</option>
              <option value="warn">Warnings</option>
              <option value="error">Errors</option>
            </select>
            <select
              className="bulletin-filter-select"
              value={processorFilter}
              onChange={(e) => setProcessorFilter(e.target.value)}
              aria-label="Filter by processor"
            >
              <option value="">All Processors</option>
              {processorNames.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))}
            </select>
            <button
              className="bulletin-refresh-btn"
              onClick={() => fetchBulletins(severityFilter, processorFilter)}
              title="Refresh"
              aria-label="Refresh bulletins"
            >
              Refresh
            </button>
          </div>
          <button
            className="config-close-btn"
            onClick={onClose}
            aria-label="Close bulletin board"
          >
            &times;
          </button>
        </div>

        <div className="bulletin-list" role="list">
          {bulletins.length === 0 ? (
            <div className="bulletin-empty">No bulletins to display.</div>
          ) : (
            bulletins.map((b) => (
              <div
                key={b.id}
                className="bulletin-item"
                role="listitem"
                onClick={() => setSelectedBulletin(b)}
                tabIndex={0}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    setSelectedBulletin(b);
                  }
                }}
                aria-label={`${b.severity} from ${b.processor_name}: ${b.message}`}
              >
                <span
                  className={`bulletin-severity-badge bulletin-sev-${b.severity}`}
                  aria-hidden="true"
                >
                  {b.severity}
                </span>
                <div className="bulletin-body">
                  <div className="bulletin-meta">
                    <span className="bulletin-processor">{b.processor_name}</span>
                    <span className="bulletin-time">{formatTimestamp(b.timestamp_ms)}</span>
                  </div>
                  <div className="bulletin-message">{b.message}</div>
                </div>
              </div>
            ))
          )}
        </div>
      </div>

      {selectedBulletin && (
        <BulletinDetailModal
          bulletin={selectedBulletin}
          onClose={() => setSelectedBulletin(null)}
        />
      )}
    </>
  );
}

export const BulletinBoard = memo(BulletinBoardInner);
