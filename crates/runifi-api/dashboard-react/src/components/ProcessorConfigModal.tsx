import { memo, useState, useEffect, useCallback, type FormEvent } from 'react';
import type { ProcessorConfigResponse, PropertyDescriptorFull } from '../types/api';
import type { ToastKind } from '../hooks/useToast';

type ActiveTab = 'settings' | 'scheduling' | 'properties' | 'relationships' | 'comments';

const TAB_ORDER: ActiveTab[] = ['settings', 'scheduling', 'properties', 'relationships', 'comments'];

const BULLETIN_LEVELS = ['DEBUG', 'INFO', 'WARN', 'ERROR'];

interface ProcessorConfigModalProps {
  processorName: string;
  processorState: string;
  onToast: (kind: ToastKind, message: string) => void;
  onClose: () => void;
}

/** Form state for all editable fields across all tabs. */
interface ConfigFormState {
  // Settings
  penaltyDurationMs: number;
  yieldDurationMs: number;
  bulletinLevel: string;
  // Scheduling
  concurrentTasks: number;
  // Properties
  properties: Record<string, string>;
  // Relationships
  autoTerminatedRelationships: string[];
  // Comments
  comments: string;
}

function validateProperties(
  descriptors: PropertyDescriptorFull[],
  values: Record<string, string>,
): string[] {
  const errors: string[] = [];
  for (const desc of descriptors) {
    if (desc.required && !values[desc.name] && !desc.default_value) {
      errors.push(`Required property "${desc.name}" is missing.`);
    }
  }
  return errors;
}

function initFormState(data: ProcessorConfigResponse): ConfigFormState {
  const properties: Record<string, string> = {};
  for (const desc of data.property_descriptors) {
    properties[desc.name] = data.properties[desc.name] ?? desc.default_value ?? '';
  }
  return {
    penaltyDurationMs: data.penalty_duration_ms,
    yieldDurationMs: data.yield_duration_ms,
    bulletinLevel: data.bulletin_level,
    concurrentTasks: data.scheduling.concurrent_tasks,
    properties,
    autoTerminatedRelationships: [...data.auto_terminated_relationships],
    comments: data.comments,
  };
}

function ProcessorConfigModalInner({
  processorName,
  processorState,
  onToast,
  onClose,
}: ProcessorConfigModalProps) {
  const [activeTab, setActiveTab] = useState<ActiveTab>('settings');
  const [config, setConfig] = useState<ProcessorConfigResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [form, setForm] = useState<ConfigFormState | null>(null);
  const [saving, setSaving] = useState(false);
  const [saveStatus, setSaveStatus] = useState<{ kind: 'error' | 'success'; message: string } | null>(null);
  // Sensitive property reveal toggle per property name.
  const [revealedSensitive, setRevealedSensitive] = useState<Set<string>>(new Set());

  const isStopped = processorState === 'stopped';

  // Load config on mount.
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
        setForm(initFormState(data));
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setLoadError(err instanceof Error ? err.message : String(err));
      });
    return () => {
      cancelled = true;
    };
  }, [processorName]);

  // Close on Escape.
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  const handlePropertyChange = useCallback((name: string, value: string) => {
    setForm((prev) => prev ? { ...prev, properties: { ...prev.properties, [name]: value } } : prev);
    setSaveStatus(null);
  }, []);

  const handleFormChange = useCallback(<K extends keyof ConfigFormState>(key: K, value: ConfigFormState[K]) => {
    setForm((prev) => prev ? { ...prev, [key]: value } : prev);
    setSaveStatus(null);
  }, []);

  const toggleAutoTerminate = useCallback((relName: string) => {
    setForm((prev) => {
      if (!prev) return prev;
      const current = prev.autoTerminatedRelationships;
      const next = current.includes(relName)
        ? current.filter((r) => r !== relName)
        : [...current, relName];
      return { ...prev, autoTerminatedRelationships: next };
    });
    setSaveStatus(null);
  }, []);

  const toggleSensitiveReveal = useCallback((name: string) => {
    setRevealedSensitive((prev) => {
      const next = new Set(prev);
      if (next.has(name)) {
        next.delete(name);
      } else {
        next.add(name);
      }
      return next;
    });
  }, []);

  // Compute per-tab validation errors.
  const tabErrors: Record<ActiveTab, string[]> = {
    settings: [],
    scheduling: [],
    properties: config && form ? validateProperties(config.property_descriptors, form.properties) : [],
    relationships: [],
    comments: [],
  };

  const handleSave = useCallback(
    (e: FormEvent) => {
      e.preventDefault();
      if (!config || !form || !isStopped) return;

      const propErrors = validateProperties(config.property_descriptors, form.properties);
      if (propErrors.length > 0) {
        setSaveStatus({ kind: 'error', message: propErrors[0] });
        setActiveTab('properties');
        return;
      }

      // Strip empty property values before sending.
      const properties: Record<string, string> = {};
      for (const [k, v] of Object.entries(form.properties)) {
        if (v !== '') properties[k] = v;
      }

      setSaving(true);
      setSaveStatus(null);

      const payload = {
        properties,
        penalty_duration_ms: form.penaltyDurationMs,
        yield_duration_ms: form.yieldDurationMs,
        bulletin_level: form.bulletinLevel,
        concurrent_tasks: form.concurrentTasks,
        auto_terminated_relationships: form.autoTerminatedRelationships,
        comments: form.comments,
      };

      fetch(`/api/v1/processors/${encodeURIComponent(processorName)}/config`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
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
    [config, form, isStopped, processorName, onToast],
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

        {config && form && (
          <form onSubmit={handleSave} noValidate>
            <div className="config-tabs" role="tablist">
              {TAB_ORDER.map((tab) => (
                <button
                  key={tab}
                  type="button"
                  role="tab"
                  className={`config-tab${activeTab === tab ? ' active' : ''}`}
                  aria-selected={activeTab === tab}
                  onClick={() => setActiveTab(tab)}
                >
                  {tab.charAt(0).toUpperCase() + tab.slice(1)}
                  {tabErrors[tab].length > 0 && (
                    <span className="config-tab-badge" title={`${tabErrors[tab].length} validation error(s)`} />
                  )}
                </button>
              ))}
            </div>

            {/* Settings Tab */}
            {activeTab === 'settings' && (
              <div className="config-tab-content" role="tabpanel">
                <div className="config-settings-section">
                  <h4 className="config-section-heading">Processor Details</h4>
                  <div className="config-field">
                    <label className="config-label">Name</label>
                    <input className="form-input" type="text" value={processorName} disabled readOnly />
                  </div>
                  <div className="config-field">
                    <label className="config-label">Type</label>
                    <input className="form-input" type="text" value={config.type_name} disabled readOnly />
                  </div>
                </div>

                <div className="config-settings-section">
                  <h4 className="config-section-heading">Behavior</h4>
                  <div className="config-field">
                    <label className="config-label" htmlFor="settings-penalty">Penalty Duration</label>
                    <span className="config-description">
                      Duration (ms) to penalize a FlowFile when the processor penalizes it
                    </span>
                    <div className="config-input-with-unit">
                      <input
                        id="settings-penalty"
                        className="form-input"
                        type="number"
                        min={0}
                        value={form.penaltyDurationMs}
                        onChange={(e) => handleFormChange('penaltyDurationMs', Number(e.target.value))}
                        disabled={!isStopped}
                      />
                      <span className="config-unit">ms</span>
                    </div>
                  </div>
                  <div className="config-field">
                    <label className="config-label" htmlFor="settings-yield">Yield Duration</label>
                    <span className="config-description">
                      Duration (ms) the processor will yield when there is no work to do
                    </span>
                    <div className="config-input-with-unit">
                      <input
                        id="settings-yield"
                        className="form-input"
                        type="number"
                        min={0}
                        value={form.yieldDurationMs}
                        onChange={(e) => handleFormChange('yieldDurationMs', Number(e.target.value))}
                        disabled={!isStopped}
                      />
                      <span className="config-unit">ms</span>
                    </div>
                  </div>
                  <div className="config-field">
                    <label className="config-label" htmlFor="settings-bulletin">Bulletin Level</label>
                    <span className="config-description">
                      Minimum severity level for bulletins generated by this processor
                    </span>
                    <select
                      id="settings-bulletin"
                      className="form-select"
                      value={form.bulletinLevel}
                      onChange={(e) => handleFormChange('bulletinLevel', e.target.value)}
                      disabled={!isStopped}
                    >
                      {BULLETIN_LEVELS.map((level) => (
                        <option key={level} value={level}>{level}</option>
                      ))}
                    </select>
                  </div>
                </div>
              </div>
            )}

            {/* Scheduling Tab */}
            {activeTab === 'scheduling' && (
              <div className="config-tab-content" role="tabpanel">
                <div className="config-field">
                  <label className="config-label">Scheduling Strategy</label>
                  <span className="config-description">How the processor is triggered</span>
                  <input
                    className="form-input"
                    type="text"
                    value={
                      config.scheduling.strategy === 'timer' ? 'Timer Driven'
                        : config.scheduling.strategy === 'cron' ? 'CRON Driven'
                        : 'Event Driven'
                    }
                    disabled
                    readOnly
                  />
                </div>
                {config.scheduling.interval_ms != null && (
                  <div className="config-field">
                    <label className="config-label">Run Schedule</label>
                    <span className="config-description">Trigger interval in milliseconds</span>
                    <div className="config-input-with-unit">
                      <input
                        className="form-input"
                        type="text"
                        value={config.scheduling.interval_ms}
                        disabled
                        readOnly
                      />
                      <span className="config-unit">ms</span>
                    </div>
                  </div>
                )}
                <div className="config-field">
                  <label className="config-label" htmlFor="sched-concurrent">Concurrent Tasks</label>
                  <span className="config-description">
                    Number of concurrent tasks for this processor
                  </span>
                  <input
                    id="sched-concurrent"
                    className="form-input"
                    type="number"
                    min={1}
                    max={100}
                    value={form.concurrentTasks}
                    onChange={(e) => handleFormChange('concurrentTasks', Number(e.target.value))}
                    disabled={!isStopped}
                  />
                </div>
              </div>
            )}

            {/* Properties Tab */}
            {activeTab === 'properties' && (
              <div className="config-tab-content" role="tabpanel">
                {config.property_descriptors.length === 0 ? (
                  <p className="config-empty">This processor has no configurable properties.</p>
                ) : (
                  config.property_descriptors.map((desc) => {
                    const isSensitive = desc.sensitive;
                    const isRevealed = revealedSensitive.has(desc.name);
                    const fieldValue = form.properties[desc.name] ?? '';
                    const isValid = !desc.required || fieldValue !== '' || desc.default_value !== null;

                    return (
                      <div key={desc.name} className="config-field">
                        <label className="config-label" htmlFor={`prop-${desc.name}`}>
                          {desc.display_name || desc.name}
                          {desc.required && (
                            <span className="config-required" title="Required">REQUIRED</span>
                          )}
                          {desc.expression_language_supported && (
                            <span className="config-el-badge" title="Supports Expression Language">EL</span>
                          )}
                          {isSensitive && (
                            <span className="config-sensitive-badge" title="Sensitive property">SENSITIVE</span>
                          )}
                        </label>
                        <span className="config-description">{desc.description}</span>

                        {desc.allowed_values && desc.allowed_values.length > 0 ? (
                          <select
                            id={`prop-${desc.name}`}
                            className={`form-select${!isValid ? ' form-input-error' : ''}`}
                            value={fieldValue}
                            onChange={(e) => handlePropertyChange(desc.name, e.target.value)}
                            disabled={!isStopped}
                          >
                            {!desc.required && <option value="">-- Select --</option>}
                            {desc.allowed_values.map((av) => (
                              <option key={av} value={av}>{av}</option>
                            ))}
                          </select>
                        ) : (
                          <div className="config-input-with-action">
                            <input
                              id={`prop-${desc.name}`}
                              className={`form-input${!isValid ? ' form-input-error' : ''}`}
                              type={isSensitive && !isRevealed ? 'password' : 'text'}
                              value={fieldValue}
                              placeholder={desc.default_value ? `Default: ${desc.default_value}` : ''}
                              onChange={(e) => handlePropertyChange(desc.name, e.target.value)}
                              disabled={!isStopped}
                              autoComplete="off"
                              spellCheck={false}
                            />
                            {isSensitive && (
                              <button
                                type="button"
                                className="config-eye-toggle"
                                onClick={() => toggleSensitiveReveal(desc.name)}
                                title={isRevealed ? 'Hide value' : 'Show value'}
                                aria-label={isRevealed ? 'Hide sensitive value' : 'Show sensitive value'}
                              >
                                {isRevealed ? '\u25C9' : '\u25CE'}
                              </button>
                            )}
                          </div>
                        )}

                        {desc.default_value && (
                          <span className="config-default">Default: {desc.default_value}</span>
                        )}
                      </div>
                    );
                  })
                )}
              </div>
            )}

            {/* Relationships Tab */}
            {activeTab === 'relationships' && (
              <div className="config-tab-content" role="tabpanel">
                {config.relationships.length === 0 ? (
                  <p className="config-empty">This processor has no relationships.</p>
                ) : (
                  <>
                    <span className="config-description" style={{ marginBottom: '12px', display: 'block' }}>
                      Check auto-terminate to discard FlowFiles routed to unconnected relationships.
                    </span>
                    <table className="rel-table">
                      <thead>
                        <tr>
                          <th>Name</th>
                          <th>Description</th>
                          <th>Auto-Terminate</th>
                        </tr>
                      </thead>
                      <tbody>
                        {config.relationships.map((rel) => (
                          <tr key={rel.name}>
                            <td><strong>{rel.name}</strong></td>
                            <td>{rel.description}</td>
                            <td>
                              <input
                                type="checkbox"
                                className="config-checkbox"
                                checked={form.autoTerminatedRelationships.includes(rel.name)}
                                onChange={() => toggleAutoTerminate(rel.name)}
                                disabled={!isStopped}
                                aria-label={`Auto-terminate ${rel.name}`}
                              />
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </>
                )}
              </div>
            )}

            {/* Comments Tab */}
            {activeTab === 'comments' && (
              <div className="config-tab-content" role="tabpanel">
                <div className="config-field">
                  <label className="config-label" htmlFor="proc-comments">
                    Processor Comments
                  </label>
                  <span className="config-description">
                    Add notes or documentation for this processor. Comments are saved with the processor configuration.
                  </span>
                  <textarea
                    id="proc-comments"
                    className="form-textarea"
                    rows={6}
                    value={form.comments}
                    onChange={(e) => handleFormChange('comments', e.target.value)}
                    placeholder="Enter comments about this processor..."
                    spellCheck={true}
                  />
                </div>
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
                disabled={!isStopped || saving}
                title={!isStopped ? 'Stop the processor to save configuration' : undefined}
              >
                {saving ? 'Applying...' : 'Apply'}
              </button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
}

export const ProcessorConfigModal = memo(ProcessorConfigModalInner);
