import { memo, useState, useEffect, useCallback, type FormEvent } from 'react';
import type { ConnectionResponse } from '../types/api';
import type { ToastKind } from '../hooks/useToast';
import { formatBytes } from '../utils/format';

type ActiveTab = 'details' | 'settings';

interface ConnectionConfigModalProps {
  connectionId: string;
  onToast: (kind: ToastKind, message: string) => void;
  onClose: () => void;
}

interface ConnectionConfig {
  back_pressure_object_threshold: number;
  back_pressure_bytes_threshold: number;
}

function parseSizeToBytes(input: string): number | null {
  const trimmed = input.trim().toUpperCase();
  const match = trimmed.match(/^(\d+(?:\.\d+)?)\s*(B|KB|MB|GB|TB)?$/);
  if (!match) return null;
  const value = parseFloat(match[1]);
  const unit = match[2] ?? 'B';
  const multipliers: Record<string, number> = {
    B: 1,
    KB: 1024,
    MB: 1024 * 1024,
    GB: 1024 * 1024 * 1024,
    TB: 1024 * 1024 * 1024 * 1024,
  };
  return Math.round(value * (multipliers[unit] ?? 1));
}

function formatSizeForInput(bytes: number): string {
  if (bytes >= 1024 * 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024 * 1024 * 1024)).toFixed(0)} TB`;
  if (bytes >= 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024 * 1024)).toFixed(0)} GB`;
  if (bytes >= 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(0)} MB`;
  if (bytes >= 1024) return `${(bytes / 1024).toFixed(0)} KB`;
  return `${bytes} B`;
}

function ConnectionConfigModalInner({
  connectionId,
  onToast,
  onClose,
}: ConnectionConfigModalProps) {
  const [activeTab, setActiveTab] = useState<ActiveTab>('details');
  const [config, setConfig] = useState<ConnectionResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  // Form state
  const [objectThreshold, setObjectThreshold] = useState('10000');
  const [sizeThreshold, setSizeThreshold] = useState('1 GB');

  // Load config on mount
  useEffect(() => {
    let cancelled = false;
    fetch(`/api/v1/connections/${encodeURIComponent(connectionId)}`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json() as Promise<ConnectionResponse>;
      })
      .then((data) => {
        if (cancelled) return;
        setConfig(data);
        setObjectThreshold(String(data.back_pressure_object_threshold));
        setSizeThreshold(formatSizeForInput(data.back_pressure_bytes_threshold));
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setLoadError(err instanceof Error ? err.message : String(err));
      });
    return () => { cancelled = true; };
  }, [connectionId]);

  // Close on Escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  const handleSave = useCallback(
    async (e: FormEvent) => {
      e.preventDefault();
      const objThreshold = parseInt(objectThreshold, 10);
      if (isNaN(objThreshold) || objThreshold <= 0) {
        onToast('error', 'Object threshold must be a positive number');
        return;
      }
      const bytesThreshold = parseSizeToBytes(sizeThreshold);
      if (bytesThreshold === null || bytesThreshold <= 0) {
        onToast('error', 'Size threshold must be a valid size (e.g., "1 GB", "500 MB")');
        return;
      }

      setSaving(true);
      try {
        const body: ConnectionConfig = {
          back_pressure_object_threshold: objThreshold,
          back_pressure_bytes_threshold: bytesThreshold,
        };
        const res = await fetch(
          `/api/v1/connections/${encodeURIComponent(connectionId)}/config`,
          {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
          },
        );
        if (!res.ok) {
          const text = await res.text();
          throw new Error(text || `HTTP ${res.status}`);
        }
        onToast('success', 'Connection configuration saved');
        onClose();
      } catch (err: unknown) {
        onToast('error', err instanceof Error ? err.message : String(err));
      } finally {
        setSaving(false);
      }
    },
    [connectionId, objectThreshold, sizeThreshold, onToast, onClose],
  );

  const tabs: { key: ActiveTab; label: string }[] = [
    { key: 'details', label: 'Details' },
    { key: 'settings', label: 'Settings' },
  ];

  return (
    <div className="modal-backdrop" onClick={onClose} role="presentation">
      <div
        className="config-panel"
        onClick={(e) => e.stopPropagation()}
        role="dialog"
        aria-label="Connection Configuration"
      >
        {/* Header */}
        <div className="config-panel-header">
          <h2 className="config-panel-title">
            {config
              ? `${config.source_name} \u2192 ${config.relationship} \u2192 ${config.dest_name}`
              : 'Connection Configuration'}
          </h2>
          <button
            className="config-panel-close"
            onClick={onClose}
            aria-label="Close"
          >
            \u00d7
          </button>
        </div>

        {/* Tabs */}
        <div className="config-tabs" role="tablist">
          {tabs.map((tab) => (
            <button
              key={tab.key}
              className={`config-tab${activeTab === tab.key ? ' active' : ''}`}
              onClick={() => setActiveTab(tab.key)}
              role="tab"
              aria-selected={activeTab === tab.key}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Content */}
        <div className="config-tab-content">
          {loadError && (
            <div className="config-error">Failed to load: {loadError}</div>
          )}

          {!config && !loadError && (
            <div className="config-loading">Loading...</div>
          )}

          {config && activeTab === 'details' && (
            <div className="config-section">
              <div className="config-field">
                <label className="config-label">Connection ID</label>
                <div className="config-value-display">{config.id}</div>
              </div>
              <div className="config-field">
                <label className="config-label">Source</label>
                <div className="config-value-display">{config.source_name}</div>
              </div>
              <div className="config-field">
                <label className="config-label">Relationship</label>
                <div className="config-value-display">{config.relationship}</div>
              </div>
              <div className="config-field">
                <label className="config-label">Destination</label>
                <div className="config-value-display">{config.dest_name}</div>
              </div>
              <div className="config-field">
                <label className="config-label">Queue Size</label>
                <div className="config-value-display">
                  {config.queued_count.toLocaleString()} FlowFiles ({formatBytes(config.queued_bytes)})
                </div>
              </div>
              <div className="config-field">
                <label className="config-label">Fill Percentage</label>
                <div className="config-value-display">
                  {(config.fill_percentage * 100).toFixed(1)}%
                </div>
              </div>
              <div className="config-field">
                <label className="config-label">Back-Pressure Active</label>
                <div className="config-value-display">
                  {config.back_pressured ? 'Yes' : 'No'}
                </div>
              </div>
            </div>
          )}

          {config && activeTab === 'settings' && (
            <form onSubmit={handleSave}>
              <div className="config-section">
                <div className="config-field">
                  <label className="config-label" htmlFor="conn-bp-object">
                    Back-Pressure Object Threshold
                  </label>
                  <p className="config-description">
                    Maximum FlowFiles in queue before back-pressure is applied.
                  </p>
                  <input
                    id="conn-bp-object"
                    type="number"
                    className="config-input"
                    value={objectThreshold}
                    onChange={(e) => setObjectThreshold(e.target.value)}
                    min={1}
                  />
                </div>
                <div className="config-field">
                  <label className="config-label" htmlFor="conn-bp-size">
                    Back-Pressure Data Size Threshold
                  </label>
                  <p className="config-description">
                    Maximum data size before back-pressure (e.g., &quot;1 GB&quot;, &quot;500 MB&quot;).
                  </p>
                  <input
                    id="conn-bp-size"
                    type="text"
                    className="config-input"
                    value={sizeThreshold}
                    onChange={(e) => setSizeThreshold(e.target.value)}
                  />
                </div>
              </div>
              <div className="modal-actions">
                <button
                  type="button"
                  className="btn btn-secondary"
                  onClick={onClose}
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="btn btn-primary"
                  disabled={saving}
                >
                  {saving ? 'Saving...' : 'Apply'}
                </button>
              </div>
            </form>
          )}
        </div>
      </div>
    </div>
  );
}

export const ConnectionConfigModal = memo(ConnectionConfigModalInner);
