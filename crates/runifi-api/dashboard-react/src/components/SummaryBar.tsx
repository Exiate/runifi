import { memo, useMemo } from 'react';
import type { SseMetricsEvent } from '../types/api';
import { formatBytes } from '../utils/format';

interface SummaryBarProps {
  metrics: SseMetricsEvent | null;
  onOpenBulletins?: () => void;
}

function SummaryBarInner({ metrics, onOpenBulletins }: SummaryBarProps) {
  const stats = useMemo(() => {
    if (!metrics) return null;

    const procs = metrics.processors;
    const counts = { running: 0, stopped: 0, invalid: 0, disabled: 0 };
    for (const p of procs) {
      const s = p.state;
      if (s === 'running') counts.running++;
      else if (s === 'invalid') counts.invalid++;
      else if (s === 'disabled') counts.disabled++;
      else counts.stopped++;
    }

    const activeThreads = procs.filter((p) => p.metrics.active).length;
    const totalQueued = metrics.connections.reduce((s, c) => s + c.queued_count, 0);
    const totalQueuedBytes = metrics.connections.reduce((s, c) => s + c.queued_bytes, 0);

    const warnCount = metrics.bulletins.filter((b) => b.severity === 'warn').length;
    const errorCount = metrics.bulletins.filter((b) => b.severity === 'error').length;

    return {
      activeThreads,
      counts,
      totalQueued,
      totalQueuedBytes,
      warnCount,
      errorCount,
    };
  }, [metrics]);

  if (!stats) {
    return (
      <section className="nifi-status-bar" aria-label="System status">
        <div className="status-bar-group">
          <span className="status-bar-item">Loading...</span>
        </div>
      </section>
    );
  }

  const { activeThreads, counts, totalQueued, totalQueuedBytes, warnCount, errorCount } = stats;

  return (
    <section className="nifi-status-bar" aria-label="System status">
      <div className="status-bar-group">
        <span className="status-bar-segment" title="Currently executing processor tasks">
          <span className="status-bar-label">Active Threads:</span>
          <span className="status-bar-value">{activeThreads}</span>
        </span>
        <span className="status-bar-divider" aria-hidden="true">|</span>
        <span className="status-bar-segment" title={`${totalQueued.toLocaleString()} FlowFiles, ${formatBytes(totalQueuedBytes)}`}>
          <span className="status-bar-label">Queued:</span>
          <span className="status-bar-value">{totalQueued.toLocaleString()} ({formatBytes(totalQueuedBytes)})</span>
        </span>
      </div>

      <div className="status-bar-group">
        <span className="status-bar-segment status-running" title={`${counts.running} running processor${counts.running !== 1 ? 's' : ''}`}>
          <span className="status-bar-label">Running:</span>
          <span className="status-bar-value">{counts.running}</span>
        </span>
        <span className="status-bar-divider" aria-hidden="true">|</span>
        <span className="status-bar-segment status-stopped" title={`${counts.stopped} stopped processor${counts.stopped !== 1 ? 's' : ''}`}>
          <span className="status-bar-label">Stopped:</span>
          <span className="status-bar-value">{counts.stopped}</span>
        </span>
        <span className="status-bar-divider" aria-hidden="true">|</span>
        <span className="status-bar-segment status-invalid" title={`${counts.invalid} invalid processor${counts.invalid !== 1 ? 's' : ''}`}>
          <span className="status-bar-label">Invalid:</span>
          <span className="status-bar-value">{counts.invalid}</span>
        </span>
        <span className="status-bar-divider" aria-hidden="true">|</span>
        <span className="status-bar-segment status-disabled" title={`${counts.disabled} disabled processor${counts.disabled !== 1 ? 's' : ''}`}>
          <span className="status-bar-label">Disabled:</span>
          <span className="status-bar-value">{counts.disabled}</span>
        </span>
      </div>

      <div className="status-bar-group">
        <button
          className={`status-bar-bulletins${onOpenBulletins ? ' clickable' : ''}`}
          onClick={onOpenBulletins}
          disabled={!onOpenBulletins}
          title="Open bulletin board"
        >
          {errorCount > 0 && (
            <span className="bulletin-count error">{errorCount} err</span>
          )}
          {warnCount > 0 && (
            <span className="bulletin-count warn">{warnCount} warn</span>
          )}
          {errorCount === 0 && warnCount === 0 && (
            <span className="bulletin-count ok">0 bulletins</span>
          )}
        </button>
      </div>
    </section>
  );
}

export const SummaryBar = memo(SummaryBarInner);
