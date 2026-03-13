import { memo, useState, useCallback, useEffect } from 'react';
import type { PluginDescriptor, ServiceResponse } from '../types/api';
import { useServices } from '../hooks/useServices';

type ToastKind = 'info' | 'success' | 'error';

interface ControllerServicesPanelProps {
  plugins: PluginDescriptor[];
  onToast: (kind: ToastKind, message: string) => void;
  onClose: () => void;
}

function ControllerServicesPanelInner({ plugins, onToast, onClose }: ControllerServicesPanelProps) {
  const {
    services,
    loading,
    refresh,
    createService,
    deleteService,
    enableService,
    disableService,
    updateServiceConfig,
  } = useServices();

  const [showAddForm, setShowAddForm] = useState(false);
  const [newServiceType, setNewServiceType] = useState('');
  const [newServiceName, setNewServiceName] = useState('');
  const [creating, setCreating] = useState(false);
  const [configuringService, setConfiguringService] = useState<ServiceResponse | null>(null);
  const [configValues, setConfigValues] = useState<Record<string, string>>({});
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null);

  // Filter plugins to only service types
  const serviceTypes = plugins.filter((p) => p.kind === 'service');

  // Set default type when service types are available
  useEffect(() => {
    if (serviceTypes.length > 0 && !newServiceType) {
      setNewServiceType(serviceTypes[0].type_name);
    }
  }, [serviceTypes, newServiceType]);

  const handleCreate = useCallback(async () => {
    if (!newServiceName.trim() || !newServiceType) return;
    setCreating(true);
    try {
      await createService({ type: newServiceType, name: newServiceName.trim(), properties: {} });
      onToast('success', `Service "${newServiceName}" created`);
      setNewServiceName('');
      setShowAddForm(false);
    } catch (err: any) {
      onToast('error', `Failed to create service: ${err.message}`);
    } finally {
      setCreating(false);
    }
  }, [newServiceName, newServiceType, createService, onToast]);

  const handleEnable = useCallback(async (name: string) => {
    try {
      await enableService(name);
      onToast('success', `Service "${name}" enabled`);
    } catch (err: any) {
      onToast('error', `Failed to enable: ${err.message}`);
    }
  }, [enableService, onToast]);

  const handleDisable = useCallback(async (name: string) => {
    try {
      await disableService(name);
      onToast('success', `Service "${name}" disabled`);
    } catch (err: any) {
      onToast('error', `Failed to disable: ${err.message}`);
    }
  }, [disableService, onToast]);

  const handleDelete = useCallback(async (name: string) => {
    try {
      await deleteService(name);
      onToast('success', `Service "${name}" deleted`);
      setConfirmDelete(null);
    } catch (err: any) {
      onToast('error', `Failed to delete: ${err.message}`);
      setConfirmDelete(null);
    }
  }, [deleteService, onToast]);

  const openConfig = useCallback((svc: ServiceResponse) => {
    setConfiguringService(svc);
    setConfigValues({ ...svc.properties });
  }, []);

  const handleSaveConfig = useCallback(async () => {
    if (!configuringService) return;
    try {
      await updateServiceConfig(configuringService.name, configValues);
      onToast('success', `Configuration updated for "${configuringService.name}"`);
      setConfiguringService(null);
    } catch (err: any) {
      onToast('error', `Failed to update config: ${err.message}`);
    }
  }, [configuringService, configValues, updateServiceConfig, onToast]);

  const stateClass = (state: string) => {
    switch (state.toUpperCase()) {
      case 'ENABLED': return 'enabled';
      case 'DISABLED': return 'disabled';
      default: return 'created';
    }
  };

  return (
    <div className="modal-overlay" onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="modal-panel services-panel" onClick={(e) => e.stopPropagation()}>
        <div className="config-modal-header">
          <div>
            <h2 className="config-modal-title">Controller Services</h2>
            <p className="config-modal-subtitle">Manage shared services used by processors</p>
          </div>
          <button className="config-modal-close" onClick={onClose} aria-label="Close">&times;</button>
        </div>

        <div className="services-toolbar">
          <button className="btn btn-sm" onClick={refresh} title="Refresh">Refresh</button>
          <button
            className="btn btn-sm btn-primary"
            onClick={() => setShowAddForm(!showAddForm)}
            disabled={serviceTypes.length === 0}
            title={serviceTypes.length === 0 ? 'No service types available' : 'Add a new service'}
          >
            + Add Service
          </button>
        </div>

        {showAddForm && (
          <div className="add-service-form">
            <div className="add-service-field">
              <label>Service Type</label>
              <select
                className="config-input"
                value={newServiceType}
                onChange={(e) => setNewServiceType(e.target.value)}
              >
                {serviceTypes.map((st) => (
                  <option key={st.type_name} value={st.type_name}>
                    {st.display_name || st.type_name}
                  </option>
                ))}
              </select>
            </div>
            <div className="add-service-field">
              <label>Name</label>
              <input
                className="config-input"
                type="text"
                placeholder="my-service"
                value={newServiceName}
                onChange={(e) => setNewServiceName(e.target.value)}
                onKeyDown={(e) => { if (e.key === 'Enter') handleCreate(); }}
                maxLength={128}
              />
            </div>
            <button className="btn btn-sm btn-primary" onClick={handleCreate} disabled={creating || !newServiceName.trim()}>
              {creating ? 'Creating...' : 'Create'}
            </button>
          </div>
        )}

        <div className="services-list-wrap">
          {loading && <div className="services-empty">Loading services...</div>}

          {!loading && services.length === 0 && (
            <div className="services-empty">
              No controller services configured.
              {serviceTypes.length > 0 && ' Click "Add Service" to create one.'}
            </div>
          )}

          {!loading && services.length > 0 && (
            <table className="services-table">
              <thead>
                <tr>
                  <th>Name</th>
                  <th>Type</th>
                  <th>State</th>
                  <th>References</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {services.map((svc) => (
                  <tr key={svc.name}>
                    <td>{svc.name}</td>
                    <td>{svc.type_name}</td>
                    <td>
                      <span className={`service-state-badge ${stateClass(svc.state)}`}>
                        {svc.state}
                      </span>
                    </td>
                    <td className="service-refs">
                      {svc.referencing_processors.length > 0
                        ? svc.referencing_processors.join(', ')
                        : 'None'}
                    </td>
                    <td>
                      <div className="service-actions">
                        {svc.state.toUpperCase() !== 'ENABLED' && (
                          <button
                            className="service-action-btn"
                            onClick={() => handleEnable(svc.name)}
                            title="Enable service"
                          >
                            Enable
                          </button>
                        )}
                        {svc.state.toUpperCase() === 'ENABLED' && (
                          <button
                            className="service-action-btn"
                            onClick={() => handleDisable(svc.name)}
                            title="Disable service"
                          >
                            Disable
                          </button>
                        )}
                        <button
                          className="service-action-btn"
                          onClick={() => openConfig(svc)}
                          title="Configure service"
                        >
                          Configure
                        </button>
                        <button
                          className="service-action-btn danger"
                          onClick={() => setConfirmDelete(svc.name)}
                          title="Delete service"
                          disabled={svc.state.toUpperCase() === 'ENABLED'}
                        >
                          Delete
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>

        {/* Delete confirmation */}
        {confirmDelete && (
          <div className="modal-overlay" style={{ zIndex: 1200 }} onClick={() => setConfirmDelete(null)}>
            <div className="modal-panel" onClick={(e) => e.stopPropagation()} style={{ maxWidth: 400 }}>
              <h3 style={{ margin: '0 0 0.75rem', fontSize: '1rem' }}>Delete Service</h3>
              <p style={{ color: 'var(--text-dim)', fontSize: '0.85rem', margin: '0 0 1rem' }}>
                Are you sure you want to delete "{confirmDelete}"? This action cannot be undone.
              </p>
              <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                <button className="btn btn-sm" onClick={() => setConfirmDelete(null)}>Cancel</button>
                <button className="btn btn-sm btn-danger" onClick={() => handleDelete(confirmDelete)}>Delete</button>
              </div>
            </div>
          </div>
        )}

        {/* Service configuration dialog */}
        {configuringService && (
          <div className="modal-overlay" style={{ zIndex: 1200 }} onClick={() => setConfiguringService(null)}>
            <div className="modal-panel" onClick={(e) => e.stopPropagation()} style={{ maxWidth: 520, width: '100%' }}>
              <div className="config-modal-header">
                <div>
                  <h3 className="config-modal-title" style={{ fontSize: '1rem' }}>
                    Configure: {configuringService.name}
                  </h3>
                  <p className="config-modal-subtitle">{configuringService.type_name}</p>
                </div>
                <button className="config-modal-close" onClick={() => setConfiguringService(null)} aria-label="Close">&times;</button>
              </div>

              {configuringService.state.toUpperCase() === 'ENABLED' && (
                <div style={{
                  padding: '0.5rem 0.75rem',
                  background: 'rgba(251, 191, 36, 0.1)',
                  border: '1px solid var(--warning)',
                  borderRadius: 6,
                  color: 'var(--warning)',
                  fontSize: '0.8rem',
                  marginBottom: '0.75rem',
                }}>
                  Disable the service before modifying configuration.
                </div>
              )}

              {configuringService.property_descriptors.length === 0 && (
                <div className="services-empty">No configurable properties.</div>
              )}

              {configuringService.property_descriptors.map((pd) => (
                <div key={pd.name} className="config-prop-row" style={{ marginBottom: '0.75rem' }}>
                  <label className="config-prop-label">
                    {pd.name}
                    {pd.required && <span style={{ color: 'var(--danger)', marginLeft: 4 }}>*</span>}
                  </label>
                  <input
                    className="config-input"
                    type={pd.sensitive ? 'password' : 'text'}
                    value={configValues[pd.name] ?? pd.default_value ?? ''}
                    onChange={(e) => setConfigValues((prev) => ({ ...prev, [pd.name]: e.target.value }))}
                    disabled={configuringService.state.toUpperCase() === 'ENABLED'}
                    placeholder={pd.default_value ?? ''}
                  />
                  {pd.description && (
                    <span className="config-prop-desc" style={{ fontSize: '0.75rem', color: 'var(--text-dim)', marginTop: 2 }}>
                      {pd.description}
                    </span>
                  )}
                </div>
              ))}

              <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end', marginTop: '1rem' }}>
                <button className="btn btn-sm" onClick={() => setConfiguringService(null)}>Cancel</button>
                <button
                  className="btn btn-sm btn-primary"
                  onClick={handleSaveConfig}
                  disabled={configuringService.state.toUpperCase() === 'ENABLED'}
                >
                  Apply
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export const ControllerServicesPanel = memo(ControllerServicesPanelInner);
