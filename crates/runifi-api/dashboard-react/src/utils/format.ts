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
      return 'var(--danger)';
    default:
      return 'var(--border)';
  }
}

export function backPressureColor(pct: number): string {
  if (pct >= 1.0) return 'var(--danger)';
  if (pct >= 0.75) return 'var(--warning)';
  return 'var(--accent)';
}
