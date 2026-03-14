// Formatting utilities for metrics display

export function formatBytes(bytes: number): string {
  if (bytes <= 0) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  const val = bytes / Math.pow(1024, i);
  return `${val.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

export function formatRate(rate: number, unit: string): string {
  if (rate === 0) return `0 ${unit}/s`;
  if (rate >= 1000) return `${(rate / 1000).toFixed(1)}k ${unit}/s`;
  return `${rate.toFixed(1)} ${unit}/s`;
}

export function formatUptime(secs: number): string {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  if (h > 0) return `${h}h ${m}m ${s}s`;
  if (m > 0) return `${m}m ${s}s`;
  return `${s}s`;
}

export function stateColor(state: string): string {
  switch (state) {
    case 'running':
      return 'var(--success)';
    case 'paused':
      return 'var(--warning)';
    case 'circuit-open':
    case 'invalid':
      return 'var(--danger)';
    case 'disabled':
      return 'var(--text-dim)';
    default:
      return 'var(--border)';
  }
}

export function backPressureColor(pct: number): string {
  if (pct >= 1.0) return 'var(--danger)';
  if (pct >= 0.75) return 'var(--warning)';
  return 'var(--accent)';
}

/** NiFi-style edge stroke color based on queue fill percentage. */
export function backPressureEdgeColor(fillPct: number): string {
  if (fillPct > 0.85) return '#dd0000';
  if (fillPct > 0.60) return '#ffaa00';
  return '#666666';
}

export function formatAge(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const secs = Math.floor(ms / 1000);
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ${secs % 60}s`;
  const hours = Math.floor(mins / 60);
  return `${hours}h ${mins % 60}m`;
}

export function formatTimestamp(ms: number): string {
  const d = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, '0');
  return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}
