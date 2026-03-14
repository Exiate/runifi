import { memo, useCallback, useEffect, useState } from 'react';

interface ProcessGroupConfig {
  id: string;
  name: string;
  comments: string;
  default_back_pressure_count: number | null;
  default_back_pressure_bytes: number | null;
  default_flowfile_expiration_ms: number | null;
}

interface ProcessGroupConfigModalProps {
  groupId: string;
  onClose: () => void;
  onSaved: () => void;
}

function ProcessGroupConfigModalInner({ groupId, onClose, onSaved }: ProcessGroupConfigModalProps) {
  const [config, setConfig] = useState<ProcessGroupConfig | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Fetch group config on mount.
  useEffect(() => {
    let cancelled = false;
    fetch(`/api/v1/process-groups/${groupId}`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data) => {
        if (!cancelled) {
          setConfig({
            id: data.id,
            name: data.name,
            comments: data.comments ?? '',
            default_back_pressure_count: data.default_back_pressure_count ?? null,
            default_back_pressure_bytes: data.default_back_pressure_bytes ?? null,
            default_flowfile_expiration_ms: data.default_flowfile_expiration_ms ?? null,
          });
          setLoading(false);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err));
          setLoading(false);
        }
      });
    return () => { cancelled = true; };
  }, [groupId]);

  const handleSave = useCallback(async () => {
    if (!config) return;
    setSaving(true);
    setError(null);

    try {
      const res = await fetch(`/api/v1/process-groups/${groupId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: config.name,
          comments: config.comments,
          default_back_pressure_count: config.default_back_pressure_count,
          default_back_pressure_bytes: config.default_back_pressure_bytes,
          default_flowfile_expiration_ms: config.default_flowfile_expiration_ms,
        }),
      });
      if (!res.ok) {
        const body = await res.text();
        throw new Error(body || `HTTP ${res.status}`);
      }
      onSaved();
      onClose();
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSaving(false);
    }
  }, [config, groupId, onClose, onSaved]);

  // Close on Escape.
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  if (loading) {
    return (
      <div className="modal-overlay" onClick={onClose}>
        <div className="modal-content" onClick={(e) => e.stopPropagation()}>
          <p>Loading...</p>
        </div>
      </div>
    );
  }

  if (!config) {
    return (
      <div className="modal-overlay" onClick={onClose}>
        <div className="modal-content" onClick={(e) => e.stopPropagation()}>
          <p>Process group not found</p>
          <button onClick={onClose}>Close</button>
        </div>
      </div>
    );
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content process-group-config-modal" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3>Configure Process Group</h3>
          <button className="modal-close" onClick={onClose} aria-label="Close">&times;</button>
        </div>

        <div className="modal-body">
          <div className="config-field">
            <label htmlFor="pg-id">Process Group ID</label>
            <input id="pg-id" type="text" value={config.id} readOnly className="config-input config-readonly" />
          </div>

          <div className="config-field">
            <label htmlFor="pg-name">Name</label>
            <input
              id="pg-name"
              type="text"
              value={config.name}
              onChange={(e) => setConfig({ ...config, name: e.target.value })}
              className="config-input"
            />
          </div>

          <div className="config-field">
            <label htmlFor="pg-comments">Comments</label>
            <textarea
              id="pg-comments"
              value={config.comments}
              onChange={(e) => setConfig({ ...config, comments: e.target.value })}
              className="config-input config-textarea"
              rows={3}
            />
          </div>

          <div className="config-field">
            <label htmlFor="pg-bp-count">Default Back Pressure Object Threshold</label>
            <input
              id="pg-bp-count"
              type="number"
              value={config.default_back_pressure_count ?? ''}
              onChange={(e) =>
                setConfig({
                  ...config,
                  default_back_pressure_count: e.target.value ? parseInt(e.target.value, 10) : null,
                })
              }
              className="config-input"
              placeholder="10000"
            />
          </div>

          <div className="config-field">
            <label htmlFor="pg-bp-bytes">Default Back Pressure Data Size (bytes)</label>
            <input
              id="pg-bp-bytes"
              type="number"
              value={config.default_back_pressure_bytes ?? ''}
              onChange={(e) =>
                setConfig({
                  ...config,
                  default_back_pressure_bytes: e.target.value ? parseInt(e.target.value, 10) : null,
                })
              }
              className="config-input"
              placeholder="104857600"
            />
          </div>

          <div className="config-field">
            <label htmlFor="pg-expiration">Default FlowFile Expiration (ms)</label>
            <input
              id="pg-expiration"
              type="number"
              value={config.default_flowfile_expiration_ms ?? ''}
              onChange={(e) =>
                setConfig({
                  ...config,
                  default_flowfile_expiration_ms: e.target.value ? parseInt(e.target.value, 10) : null,
                })
              }
              className="config-input"
              placeholder="0 (no expiration)"
            />
          </div>

          {error && <div className="config-error">{error}</div>}
        </div>

        <div className="modal-footer">
          <button className="btn-secondary" onClick={onClose}>Cancel</button>
          <button className="btn-primary" onClick={handleSave} disabled={saving || !config.name}>
            {saving ? 'Saving...' : 'Save'}
          </button>
        </div>
      </div>
    </div>
  );
}

export const ProcessGroupConfigModal = memo(ProcessGroupConfigModalInner);
