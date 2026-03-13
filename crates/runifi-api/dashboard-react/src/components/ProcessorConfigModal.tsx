import { memo, useState, useEffect, useCallback, type FormEvent } from 'react';
import type { ProcessorConfigResponse, PropertyDescriptorFull } from '../types/api';
import type { ToastKind } from '../hooks/useToast';

type ActiveTab = 'properties' | 'scheduling' | 'relationships';

interface ProcessorConfigModalProps {
  processorName: string;
  processorState: string;
  onToast: (kind: ToastKind, message: string) => void;
  onClose: () => void;
}

function validateProperties(
  descriptors: PropertyDescriptorFull[],
  values: Record<string, string>,
): string | null {
  for (const desc of descriptors) {
    if (desc.required && !values[desc.name] && !desc.default_value) {
      return `Required property "${desc.name}" is missing.`;
    }
  }
  return null;
}

function ProcessorConfigModalInner({
  processorName,
  processorState,
  onToast,
  onClose,
}: ProcessorConfigModalProps) {
  const [activeTab, setActiveTab] = useState<ActiveTab>('properties');
  const [config, setConfig] = useState<ProcessorConfigResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [formValues, setFormValues] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState<{ kind: 'error' | 'success'; message: string } | null>(null);

  const isStopped = processorState === 'stopped';

  // Load config on mount
  useEffect(() => {
    let cancelled = false;
    fetch(`/api/v1/processors/${encodeURIComponent(processorName)}/config`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json() as Promise<ProcessorConfigResponse>;
      })
      .then((data) => {
        if (cancelled) return;
        setConfig(data);
        // Initialise form with current values, falling back to descriptor defaults
        const initial: Record<string, string> = {};
        for (const desc of data.property_descriptors) {
          initial[desc.name] = data.properties[desc.name] ?? desc.default_value ?? '';
        }
        setFormValues(initial);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setLoadError(err instanceof Error ? err.message : String(err));
      });
    return () => {
      cancelled = true;
    };
  }, [processorName]);

  // Close on Escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  const handleFieldChange = useCallback((name: string, value: string) => {
    setFormValues((prev) => ({ ...prev, [name]: value }));
    setSaveStatus(null);
  }, []);

  const handleSave = useCallback(
    (e: FormEvent) => {
      e.preventDefault();
      if (!config || !isStopped) return;

      const validationError = validateProperties(config.property_descriptors, formValues);
      if (validationError) {
        setSaveStatus({ kind: 'error', message: validationError });
        return;
      }

      // Strip empty values before sending
      const properties: Record<string, string> = {};
      for (const [k, v] of Object.entries(formValues)) {
        if (v !== '') properties[k] = v;
      }

      setSaving(true);
      setSaveStatus(null);

      fetch(`/api/v1/processors/${encodeURIComponent(processorName)}/config`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ properties }),
      })
        .then((res) => {
          if (!res.ok) {
            return res.json().then((body: unknown) => {
              const msg =
                body && typeof body === 'object' && 'error' in body
                  ? String((body as { error: unknown }).error)
                  : `HTTP ${res.status}`;
              throw new Error(msg);
            });
          }
          return res.json();
        })
        .then(() => {
          setSaveStatus({ kind: 'success', message: 'Configuration saved.' });
          onToast('success', `Processor "${processorName}" configuration saved.`);
          setSaving(false);
        })
        .catch((err: unknown) => {
          const msg = err instanceof Error ? err.message : String(err);
          setSaveStatus({ kind: 'error', message: msg });
          setSaving(false);
        });
    },
    [config, isStopped, formValues, processorName, onToast],
  );

  return (
    <div
      className="modal-overlay"
      role="dialog"
      aria-modal="true"
      aria-labelledby="config-modal-title"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="modal-panel config-panel">
        <div className="config-modal-header">
          <div>
            <h3 id="config-modal-title" className="modal-title">
              {processorName}
            </h3>
            {config && (
              <span className="modal-type-tag">{config.type_name}</span>
            )}
          </div>
          <button className="config-close-btn" onClick={onClose} aria-label="Close">
            &times;
          </button>
        </div>

        {!isStopped && (
          <p className="config-warning">
            Stop the processor to edit its configuration.
          </p>
        )}

        {loadError && (
          <p className="config-load-error">Failed to load configuration: {loadError}</p>
        )}

        {!loadError && !config && (
          <p className="config-loading">Loading...</p>
        )}

        {config && (
          <form onSubmit={handleSave} noValidate>
            <div className="config-tabs" role="tablist">
              {(['properties', 'scheduling', 'relationships'] as ActiveTab[]).map((tab) => (
                <button
                  key={tab}
                  type="button"
                  role="tab"
                  className={`config-tab${activeTab === tab ? ' active' : ''}`}
                  aria-selected={activeTab === tab}
                  onClick={() => setActiveTab(tab)}
                >
                  {tab.charAt(0).toUpperCase() + tab.slice(1)}
                </button>
              ))}
            </div>

            {/* Properties tab */}
            {activeTab === 'properties' && (
              <div className="config-tab-content" role="tabpanel">
                {config.property_descriptors.length === 0 ? (
                  <p className="config-empty">This processor has no configurable properties.</p>
                ) : (
                  config.property_descriptors.map((desc) => (
                    <div key={desc.name} className="config-field">
                      <label className="config-label" htmlFor={`prop-${desc.name}`}>
                        {desc.display_name || desc.name}
                        {desc.required && (
                          <span className="config-required" title="Required">REQUIRED</span>
                        )}
                      </label>
                      <span className="config-description">{desc.description}</span>

                      {desc.allowed_values && desc.allowed_values.length > 0 ? (
                        <select
                          id={`prop-${desc.name}`}
                          className="form-select"
                          value={formValues[desc.name] ?? ''}
                          onChange={(e) => handleFieldChange(desc.name, e.target.value)}
                          disabled={!isStopped}
                        >
                          {!desc.required && <option value="">-- Select --</option>}
                          {desc.allowed_values.map((av) => (
                            <option key={av} value={av}>
                              {av}
                            </option>
                          ))}
                        </select>
                      ) : (
                        <input
                          id={`prop-${desc.name}`}
                          className="form-input"
                          type="text"
                          value={formValues[desc.name] ?? ''}
                          placeholder={
                            desc.default_value ? `Default: ${desc.default_value}` : ''
                          }
                          onChange={(e) => handleFieldChange(desc.name, e.target.value)}
                          disabled={!isStopped}
                          autoComplete="off"
                          spellCheck={false}
                        />
                      )}

                      {desc.default_value && (
                        <span className="config-default">Default: {desc.default_value}</span>
                      )}
                    </div>
                  ))
                )}
              </div>
            )}

            {/* Scheduling tab */}
            {activeTab === 'scheduling' && (
              <div className="config-tab-content" role="tabpanel">
                <div className="config-field">
                  <label className="config-label">Strategy</label>
                  <span className="config-description">How the processor is triggered</span>
                  <input
                    className="form-input"
                    type="text"
                    value={config.scheduling.strategy}
                    disabled
                    readOnly
                  />
                </div>
                {config.scheduling.interval_ms !== null &&
                  config.scheduling.interval_ms !== undefined && (
                    <div className="config-field">
                      <label className="config-label">Interval</label>
                      <span className="config-description">Trigger interval in milliseconds</span>
                      <input
                        className="form-input"
                        type="text"
                        value={`${config.scheduling.interval_ms} ms`}
                        disabled
                        readOnly
                      />
                    </div>
                  )}
              </div>
            )}

            {/* Relationships tab */}
            {activeTab === 'relationships' && (
              <div className="config-tab-content" role="tabpanel">
                {config.relationships.length === 0 ? (
                  <p className="config-empty">This processor has no relationships.</p>
                ) : (
                  <table className="rel-table">
                    <thead>
                      <tr>
                        <th>Name</th>
                        <th>Description</th>
                        <th>Auto-Terminated</th>
                      </tr>
                    </thead>
                    <tbody>
                      {config.relationships.map((rel) => (
                        <tr key={rel.name}>
                          <td>
                            <strong>{rel.name}</strong>
                          </td>
                          <td>{rel.description}</td>
                          <td>
                            <span
                              className={`rel-auto-term ${rel.auto_terminated ? 'yes' : 'no'}`}
                            >
                              {rel.auto_terminated ? 'Yes' : 'No'}
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            )}

            {saveStatus && (
              <p
                className={`config-save-status ${saveStatus.kind}`}
                role="status"
                aria-live="polite"
              >
                {saveStatus.message}
              </p>
            )}

            <div className="modal-actions">
              <button type="button" className="btn btn-ghost" onClick={onClose}>
                Cancel
              </button>
              <button
                type="submit"
                className="btn btn-primary"
                disabled={!isStopped || saving || activeTab !== 'properties'}
                title={!isStopped ? 'Stop the processor to save configuration' : undefined}
              >
                {saving ? 'Saving...' : 'Save'}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

export const ProcessorConfigModal = memo(ProcessorConfigModalInner);
