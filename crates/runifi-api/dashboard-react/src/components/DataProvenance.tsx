import { memo, useState, useEffect, useCallback } from 'react';
import type { ProvenanceEvent, ProvenanceSearchResponse } from '../types/api';
import type { ToastKind } from '../hooks/useToast';
import { ProvenanceEventDetail } from './ProvenanceEventDetail';
import { ProvenanceLineageGraph } from './ProvenanceLineageGraph';
import { formatBytes, formatTimestamp } from '../utils/format';

const PAGE_SIZE = 50;

const EVENT_TYPES = [
  'CREATE',
  'SEND',
  'RECEIVE',
  'CLONE',
  'CONTENT_MODIFIED',
  'ATTRIBUTES_MODIFIED',
  'ROUTE',
  'DROP',
  'FORK',
  'JOIN',
  'FETCH',
  'EXPIRE',
  'REPLAY',
  'DOWNLOAD',
  'ADDINFO',
];

interface DataProvenanceProps {
  onClose: () => void;
  onToast: (kind: ToastKind, message: string) => void;
}

function DataProvenanceInner({ onClose, onToast }: DataProvenanceProps) {
  const [events, setEvents] = useState<ProvenanceEvent[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [offset, setOffset] = useState(0);
  const [loading, setLoading] = useState(false);

  // Filter state
  const [flowfileId, setFlowfileId] = useState('');
  const [processorName, setProcessorName] = useState('');
  const [eventType, setEventType] = useState('');
  const [startTime, setStartTime] = useState('');
  const [endTime, setEndTime] = useState('');
  const [maxResults, setMaxResults] = useState(PAGE_SIZE);

  // Detail/lineage state
  const [selectedEvent, setSelectedEvent] = useState<ProvenanceEvent | null>(null);
  const [lineageFlowfileId, setLineageFlowfileId] = useState<number | null>(null);

  const fetchEvents = useCallback(
    (pageOffset: number) => {
      setLoading(true);
      const params = new URLSearchParams();
      if (flowfileId) params.set('flowfile_id', flowfileId);
      if (processorName) params.set('processor', processorName);
      if (eventType) params.set('event_type', eventType);
      if (startTime) params.set('start_time', new Date(startTime).toISOString());
      if (endTime) params.set('end_time', new Date(endTime).toISOString());
      params.set('max_results', String(maxResults));
      params.set('offset', String(pageOffset));

      const qs = params.toString();
      fetch(`/api/v1/provenance/search?${qs}`)
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          return res.json() as Promise<ProvenanceSearchResponse>;
        })
        .then((data) => {
          setEvents(data.events ?? []);
          setTotalCount(data.total_count);
          setOffset(data.offset);
        })
        .catch((err: unknown) => {
          const msg = err instanceof Error ? err.message : String(err);
          onToast('error', `Provenance search failed: ${msg}`);
        })
        .finally(() => setLoading(false));
    },
    [flowfileId, processorName, eventType, startTime, endTime, maxResults, onToast],
  );

  useEffect(() => {
    fetchEvents(0);
  }, [fetchEvents]);

  // Close on Escape (only when no sub-modal open)
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !selectedEvent && lineageFlowfileId === null) {
        onClose();
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose, selectedEvent, lineageFlowfileId]);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setOffset(0);
    fetchEvents(0);
  };

  const totalPages = Math.ceil(totalCount / maxResults);
  const currentPage = maxResults > 0 ? Math.floor(offset / maxResults) + 1 : 1;

  const goToPrev = () => {
    const newOffset = Math.max(0, offset - maxResults);
    setOffset(newOffset);
    fetchEvents(newOffset);
  };

  const goToNext = () => {
    const newOffset = offset + maxResults;
    setOffset(newOffset);
    fetchEvents(newOffset);
  };

  const handleViewLineage = (ffId: number) => {
    setLineageFlowfileId(ffId);
  };

  const handleLineageSelectEvent = (event: ProvenanceEvent) => {
    setLineageFlowfileId(null);
    setSelectedEvent(event);
  };

  return (
    <>
      <div className="provenance-panel" role="complementary" aria-label="Data Provenance">
        <div className="provenance-panel-header">
          <span className="provenance-panel-title">Data Provenance</span>
          <button
            className="provenance-refresh-btn"
            onClick={() => fetchEvents(offset)}
            title="Refresh"
            aria-label="Refresh provenance"
          >
            Refresh
          </button>
          <button
            className="config-close-btn"
            onClick={onClose}
            aria-label="Close data provenance"
          >
            &times;
          </button>
        </div>

        <form className="provenance-search-form" onSubmit={handleSearch}>
          <div className="provenance-filter-row">
            <label className="provenance-filter-field">
              <span className="provenance-filter-label">FlowFile ID</span>
              <input
                type="number"
                className="provenance-input"
                value={flowfileId}
                onChange={(e) => setFlowfileId(e.target.value)}
                placeholder="Any"
                min="0"
              />
            </label>
            <label className="provenance-filter-field">
              <span className="provenance-filter-label">Processor</span>
              <input
                type="text"
                className="provenance-input"
                value={processorName}
                onChange={(e) => setProcessorName(e.target.value)}
                placeholder="Any"
              />
            </label>
            <label className="provenance-filter-field">
              <span className="provenance-filter-label">Event Type</span>
              <select
                className="provenance-input"
                value={eventType}
                onChange={(e) => setEventType(e.target.value)}
              >
                <option value="">All Types</option>
                {EVENT_TYPES.map((t) => (
                  <option key={t} value={t}>{t}</option>
                ))}
              </select>
            </label>
          </div>
          <div className="provenance-filter-row">
            <label className="provenance-filter-field">
              <span className="provenance-filter-label">Start Time</span>
              <input
                type="datetime-local"
                className="provenance-input"
                value={startTime}
                onChange={(e) => setStartTime(e.target.value)}
              />
            </label>
            <label className="provenance-filter-field">
              <span className="provenance-filter-label">End Time</span>
              <input
                type="datetime-local"
                className="provenance-input"
                value={endTime}
                onChange={(e) => setEndTime(e.target.value)}
              />
            </label>
            <label className="provenance-filter-field">
              <span className="provenance-filter-label">Max Results</span>
              <input
                type="number"
                className="provenance-input"
                value={maxResults}
                onChange={(e) => setMaxResults(Math.max(1, Number(e.target.value)))}
                min="1"
                max="10000"
              />
            </label>
            <button type="submit" className="btn btn-primary provenance-search-btn">
              Search
            </button>
          </div>
        </form>

        <div className="provenance-results-info">
          {loading ? 'Searching...' : `${totalCount.toLocaleString()} event${totalCount !== 1 ? 's' : ''} found`}
        </div>

        <div className="provenance-table-wrap">
          <table className="provenance-table">
            <thead>
              <tr>
                <th>Time</th>
                <th>Type</th>
                <th>FlowFile ID</th>
                <th>Processor</th>
                <th>Size</th>
                <th>Details</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {events.length === 0 ? (
                <tr>
                  <td colSpan={7} className="provenance-empty-row">
                    {loading ? 'Loading...' : 'No provenance events found.'}
                  </td>
                </tr>
              ) : (
                events.map((evt) => (
                  <tr
                    key={evt.event_id}
                    className="provenance-row"
                    onClick={() => setSelectedEvent(evt)}
                    style={{ cursor: 'pointer' }}
                  >
                    <td className="provenance-cell-dim">{formatTimestamp(evt.timestamp_ms)}</td>
                    <td>
                      <span className={`provenance-type-badge provenance-type-${evt.event_type.toLowerCase()}`}>
                        {evt.event_type}
                      </span>
                    </td>
                    <td className="provenance-cell-mono">{evt.flowfile_id}</td>
                    <td className="provenance-cell-dim">{evt.processor_name}</td>
                    <td className="provenance-cell-dim">{formatBytes(evt.content_size)}</td>
                    <td className="provenance-cell-dim provenance-cell-details">{evt.details}</td>
                    <td>
                      <div
                        className="provenance-actions"
                        onClick={(e) => e.stopPropagation()}
                      >
                        <button
                          className="btn-link"
                          onClick={() => setSelectedEvent(evt)}
                        >
                          Detail
                        </button>
                        <button
                          className="btn-link"
                          onClick={() => handleViewLineage(evt.flowfile_id)}
                        >
                          Lineage
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
          <div className="provenance-pagination">
            <button
              className="btn btn-ghost"
              style={{ fontSize: '0.78rem', padding: '0.3rem 0.7rem' }}
              disabled={currentPage <= 1}
              onClick={goToPrev}
            >
              Previous
            </button>
            <span className="provenance-page-info">
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
      </div>

      {selectedEvent && (
        <ProvenanceEventDetail
          event={selectedEvent}
          onClose={() => setSelectedEvent(null)}
          onViewLineage={handleViewLineage}
          onToast={onToast}
        />
      )}

      {lineageFlowfileId !== null && (
        <ProvenanceLineageGraph
          flowfileId={lineageFlowfileId}
          onClose={() => setLineageFlowfileId(null)}
          onSelectEvent={handleLineageSelectEvent}
        />
      )}
    </>
  );
}

export const DataProvenance = memo(DataProvenanceInner);
