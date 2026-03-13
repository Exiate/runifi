import { memo, useMemo } from 'react';
import type { SseMetricsEvent } from '../types/api';
import { formatBytes, formatRate } from '../utils/format';

interface SummaryBarProps {
  metrics: SseMetricsEvent | null;
  onOpenBulletins?: () => void;
}

function SummaryBarInner({ metrics, onOpenBulletins }: SummaryBarProps) {
  const stats = useMemo(() => {
    if (!metrics) return null;

    const procs = metrics.processors;
    const counts = { running: 0, paused: 0, stopped: 0, 'circuit-open': 0 };
    for (const p of procs) {
      const s = p.state as keyof typeof counts;
      if (s in counts) counts[s]++;
    }

    const totalQueued = metrics.connections.reduce((s, c) => s + c.queued_count, 0);
    const totalQueuedBytes = metrics.connections.reduce((s, c) => s + c.queued_bytes, 0);
    const backPressureCount = metrics.connections.filter((c) => c.back_pressured).length;

    const totalBytesInRate = procs.reduce((s, p) => s + p.metrics.bytes_in_rate, 0);
    const totalBytesOutRate = procs.reduce((s, p) => s + p.metrics.bytes_out_rate, 0);
    const totalFlowFilesInRate = procs.reduce((s, p) => s + p.metrics.flowfiles_in_rate, 0);
    const totalFlowFilesOutRate = procs.reduce((s, p) => s + p.metrics.flowfiles_out_rate, 0);

    const warnCount = metrics.bulletins.filter((b) => b.severity === 'warn').length;
    const errorCount = metrics.bulletins.filter((b) => b.severity === 'error').length;

    return {
      counts,
      totalQueued,
      totalQueuedBytes,
      backPressureCount,
      totalBytesInRate,
      totalBytesOutRate,
      totalFlowFilesInRate,
      totalFlowFilesOutRate,
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

  const {
    counts,
    totalQueued,
    totalQueuedBytes,
    backPressureCount,
    totalBytesInRate,
    totalBytesOutRate,
    totalFlowFilesInRate,
    totalFlowFilesOutRate,
    warnCount,
    errorCount,
  } = stats;

  return (
    <section className="nifi-status-bar" aria-label="System status">
      <div className="status-bar-group">
        <span className="status-bar-item" title="Active threads (running processors)">
          {counts.running} active threads
        </span>
        <span className="status-bar-item" title={`${totalQueued} queued, ${formatBytes(totalQueuedBytes)}`}>
          {totalQueued.toLocaleString()} / {formatBytes(totalQueuedBytes)} queued
        </span>
      </div>

      <div className="status-bar-group">
        <span className="status-bar-item" title="FlowFile throughput (5-min rolling)">
          {formatRate(totalFlowFilesInRate, 'FF')} in
        </span>
        <span className="status-bar-item" title="FlowFile throughput (5-min rolling)">
          {formatRate(totalFlowFilesOutRate, 'FF')} out
        </span>
        <span className="status-bar-item" title="Bytes transferred in (5-min rolling)">
          {formatBytes(Math.round(totalBytesInRate))}/s in
        </span>
        <span className="status-bar-item" title="Bytes transferred out (5-min rolling)">
          {formatBytes(Math.round(totalBytesOutRate))}/s out
        </span>
      </div>

      <div className="status-bar-group">
        {counts.running > 0 && (
          <span className="state-pill running">{counts.running} running</span>
        )}
        {counts.stopped > 0 && (
          <span className="state-pill stopped">{counts.stopped} stopped</span>
        )}
        {counts.paused > 0 && (
          <span className="state-pill paused">{counts.paused} paused</span>
        )}
        {counts['circuit-open'] > 0 && (
          <span className="state-pill circuit-open">{counts['circuit-open']} open</span>
        )}
        {backPressureCount > 0 && (
          <span className="state-pill stopped">{backPressureCount} back-pressured</span>
        )}
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
