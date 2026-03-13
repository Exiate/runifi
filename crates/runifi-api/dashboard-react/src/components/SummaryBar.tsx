import { memo, useMemo } from 'react';
import type { SseMetricsEvent } from '../types/api';
import { formatRate } from '../utils/format';

interface SummaryBarProps {
  metrics: SseMetricsEvent | null;
}

function SummaryBarInner({ metrics }: SummaryBarProps) {
  const stats = useMemo(() => {
    if (!metrics) return null;

    const procs = metrics.processors;
    const counts = { running: 0, paused: 0, stopped: 0, 'circuit-open': 0 };
    for (const p of procs) {
      const s = p.state as keyof typeof counts;
      if (s in counts) counts[s]++;
    }

    const totalQueued = metrics.connections.reduce((s, c) => s + c.queued_count, 0);
    const backPressureCount = metrics.connections.filter((c) => c.back_pressured).length;

    const totalFFOutRate = procs.reduce((s, p) => s + p.metrics.flowfiles_out_rate, 0);

    const hasUnhealthy = counts['circuit-open'] > 0;
    const hasWarning = counts.stopped > 0 || counts.paused > 0 || backPressureCount > 0;
    const procHealth = hasUnhealthy ? 'danger' : hasWarning ? 'warning' : 'healthy';
    const bpHealth = backPressureCount > 0 ? 'danger' : 'healthy';

    return { counts, totalQueued, backPressureCount, totalFFOutRate, procHealth, bpHealth };
  }, [metrics]);

  if (!stats) {
    return (
      <section className="summary-bar" aria-label="System summary">
        <div className="summary-item">
          <span className="summary-label">Loading...</span>
        </div>
      </section>
    );
  }

  const { counts, totalQueued, backPressureCount, totalFFOutRate, procHealth, bpHealth } = stats;
  const procTotal = Object.values(counts).reduce((a, b) => a + b, 0);

  return (
    <section className="summary-bar" aria-label="System summary">
      <div className="summary-item">
        <span className="summary-label">Processors</span>
        <span className="summary-value">{procTotal}</span>
        <span
          className={`summary-indicator ${procHealth}`}
          aria-label={`Processor health: ${procHealth}`}
        />
      </div>

      <div className="summary-item">
        <span className="summary-label">Queued FlowFiles</span>
        <span className="summary-value">{totalQueued.toLocaleString()}</span>
      </div>

      <div className="summary-item">
        <span className="summary-label">Throughput</span>
        <span className="summary-value">{formatRate(totalFFOutRate, 'FF')}</span>
      </div>

      <div className="summary-item">
        <span className="summary-label">States</span>
        <div className="summary-state-pills">
          {counts.running > 0 && (
            <span className="state-pill running">{counts.running} running</span>
          )}
          {counts.paused > 0 && (
            <span className="state-pill paused">{counts.paused} paused</span>
          )}
          {counts.stopped > 0 && (
            <span className="state-pill stopped">{counts.stopped} stopped</span>
          )}
          {counts['circuit-open'] > 0 && (
            <span className="state-pill circuit-open">{counts['circuit-open']} open</span>
          )}
        </div>
      </div>

      <div className="summary-item">
        <span className="summary-label">Back-Pressure</span>
        <span className="summary-value">{backPressureCount}</span>
        <span
          className={`summary-indicator ${bpHealth}`}
          aria-label={`Back-pressure: ${bpHealth}`}
        />
      </div>
    </section>
  );
}

export const SummaryBar = memo(SummaryBarInner);
