import { memo } from 'react';
import { useSystemDiagnostics } from '../hooks/useSystemDiagnostics';
import { formatBytes, formatUptime } from '../utils/format';

interface SystemDiagnosticsModalProps {
  onClose: () => void;
}

function UsageBar({ label, used, total, format }: {
  label: string;
  used: number;
  total: number;
  format: (n: number) => string;
}) {
  const pct = total > 0 ? Math.min((used / total) * 100, 100) : 0;
  const color = pct >= 90 ? 'var(--danger)' : pct >= 75 ? 'var(--warning)' : 'var(--accent)';

  return (
    <div className="diag-usage-bar-wrap">
      <div className="diag-usage-bar-header">
        <span className="diag-usage-bar-label">{label}</span>
        <span className="diag-usage-bar-value">{format(used)} / {format(total)}</span>
      </div>
      <div className="diag-usage-bar-track" role="progressbar" aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100}>
        <div className="diag-usage-bar-fill" style={{ width: `${pct}%`, backgroundColor: color }} />
      </div>
      <span className="diag-usage-bar-pct">{pct.toFixed(1)}%</span>
    </div>
  );
}

function MetricRow({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="diag-metric-row">
      <span className="diag-metric-label">{label}</span>
      <span className="diag-metric-value">{value}</span>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="diag-section">
      <h4 className="diag-section-title">{title}</h4>
      <div className="diag-section-body">{children}</div>
    </div>
  );
}

function SystemDiagnosticsModalInner({ onClose }: SystemDiagnosticsModalProps) {
  const { data, loading, error, autoRefresh, setAutoRefresh, refresh } = useSystemDiagnostics(true);

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content diag-modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3>System Diagnostics</h3>
          <div className="diag-header-controls">
            <label className="diag-auto-refresh">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
              />
              Auto-refresh (5s)
            </label>
            <button className="diag-refresh-btn" onClick={refresh} title="Refresh now">
              &#x21bb;
            </button>
            <button className="modal-close" onClick={onClose} aria-label="Close">&times;</button>
          </div>
        </div>

        {loading && !data && (
          <div className="diag-loading">Loading diagnostics...</div>
        )}

        {error && (
          <div className="diag-error">Failed to load diagnostics: {error}</div>
        )}

        {data && (
          <div className="diag-body">
            <Section title="System">
              <MetricRow label="Version" value={data.version} />
              <MetricRow label="Flow" value={data.flow_name} />
              <MetricRow label="Uptime" value={formatUptime(data.uptime_secs)} />
              <MetricRow label="CPU Cores" value={data.cpu.available_cores} />
              <MetricRow label="Process CPU" value={`${data.cpu.process_cpu_percent.toFixed(1)}%`} />
            </Section>

            <Section title="Memory">
              <UsageBar
                label="Resident (RSS)"
                used={data.memory.resident_bytes}
                total={data.memory.total_system_bytes}
                format={formatBytes}
              />
              <MetricRow label="Virtual Memory" value={formatBytes(data.memory.virtual_bytes)} />
              <UsageBar
                label="System Memory"
                used={data.memory.used_system_bytes}
                total={data.memory.total_system_bytes}
                format={formatBytes}
              />
            </Section>

            <Section title="Repositories">
              <MetricRow label="Content Repo Type" value={data.repositories.content.storage_type} />
              <MetricRow label="Content Entries" value={data.repositories.content.entry_count.toLocaleString()} />
              <MetricRow label="Content Size" value={formatBytes(data.repositories.content.total_bytes)} />
              <MetricRow label="FlowFile Repo Type" value={data.repositories.flowfile.storage_type} />
              <MetricRow label="FlowFile Entries" value={data.repositories.flowfile.entry_count.toLocaleString()} />
              <MetricRow label="FlowFile WAL Size" value={formatBytes(data.repositories.flowfile.storage_bytes)} />
              <MetricRow label="Provenance Events" value={data.repositories.provenance.event_count.toLocaleString()} />
            </Section>

            <Section title="Connections">
              <MetricRow label="Total Connections" value={data.connection_count} />
              <MetricRow label="Queued FlowFiles" value={data.connections_summary.total_queued_flowfiles.toLocaleString()} />
              <MetricRow label="Queued Bytes" value={formatBytes(data.connections_summary.total_queued_bytes)} />
              <MetricRow label="Back-Pressured" value={data.connections_summary.back_pressured_count} />
            </Section>

            <Section title="Throughput">
              <MetricRow label="Processors" value={data.processor_count} />
              <MetricRow label="FlowFiles Processed" value={data.runtime.total_flowfiles_processed.toLocaleString()} />
              <MetricRow label="Bytes Processed" value={formatBytes(data.runtime.total_bytes_processed)} />
            </Section>
          </div>
        )}
      </div>
    </div>
  );
}

export const SystemDiagnosticsModal = memo(SystemDiagnosticsModalInner);
