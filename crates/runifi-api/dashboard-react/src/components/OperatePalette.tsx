import { memo, useState, useCallback } from 'react';
import type { ToastKind } from '../hooks/useToast';

export interface SelectedProcessor {
  name: string;
  state: string;
}

interface OperatePaletteProps {
  selectedProcessors: SelectedProcessor[];
  onToast: (kind: ToastKind, message: string) => void;
  onColorSelected?: (nodeIds: string[]) => void;
}

function OperatePaletteInner({ selectedProcessors, onToast, onColorSelected }: OperatePaletteProps) {
  const [collapsed, setCollapsed] = useState(false);
  const count = selectedProcessors.length;
  const hasSelection = count > 0;

  const stoppedCount = selectedProcessors.filter((p) => p.state === 'stopped').length;
  const runningCount = selectedProcessors.filter((p) => p.state === 'running').length;
  const disabledCount = selectedProcessors.filter((p) => p.state === 'disabled').length;
  const enabledCount = selectedProcessors.filter((p) => p.state !== 'disabled').length;

  const controlProcessor = useCallback(
    (name: string, action: 'start' | 'stop') => {
      fetch(`/api/v1/processors/${encodeURIComponent(name)}/${action}`, { method: 'POST' })
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          onToast('success', `${action === 'start' ? 'Started' : 'Stopped'} "${name}"`);
        })
        .catch((err: Error) => {
          onToast('error', `Failed to ${action} "${name}": ${err.message}`);
        });
    },
    [onToast],
  );

  const toggleProcessorState = useCallback(
    (name: string, action: 'enable' | 'disable') => {
      fetch(`/api/v1/processors/${encodeURIComponent(name)}/${action}`, { method: 'PUT' })
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
          onToast('success', `${action === 'enable' ? 'Enabled' : 'Disabled'} "${name}"`);
        })
        .catch((err: Error) => {
          onToast('error', `Failed to ${action} "${name}": ${err.message}`);
        });
    },
    [onToast],
  );

  const handleStartSelected = useCallback(() => {
    for (const proc of selectedProcessors) {
      if (proc.state === 'stopped') {
        controlProcessor(proc.name, 'start');
      }
    }
  }, [selectedProcessors, controlProcessor]);

  const handleStopSelected = useCallback(() => {
    for (const proc of selectedProcessors) {
      if (proc.state === 'running') {
        controlProcessor(proc.name, 'stop');
      }
    }
  }, [selectedProcessors, controlProcessor]);

  const handleEnableSelected = useCallback(() => {
    for (const proc of selectedProcessors) {
      if (proc.state === 'disabled') {
        toggleProcessorState(proc.name, 'enable');
      }
    }
  }, [selectedProcessors, toggleProcessorState]);

  const handleDisableSelected = useCallback(() => {
    for (const proc of selectedProcessors) {
      if (proc.state !== 'disabled') {
        toggleProcessorState(proc.name, 'disable');
      }
    }
  }, [selectedProcessors, toggleProcessorState]);

  const handleDeleteSelected = useCallback(() => {
    if (count === 0) return;
    if (!window.confirm(`Delete ${count} selected component${count > 1 ? 's' : ''}?`)) return;

    for (const proc of selectedProcessors) {
      fetch(`/api/v1/process-groups/root/processors/${encodeURIComponent(proc.name)}`, { method: 'DELETE' })
        .then((res) => {
          if (!res.ok) throw new Error(`HTTP ${res.status}`);
        })
        .catch((err: Error) => {
          onToast('error', `Failed to delete "${proc.name}": ${err.message}`);
        });
    }
  }, [count, selectedProcessors, onToast]);

  const handleColorSelected = useCallback(() => {
    if (!hasSelection || !onColorSelected) return;
    onColorSelected(selectedProcessors.map((p) => p.name));
  }, [hasSelection, onColorSelected, selectedProcessors]);

  return (
    <aside
      className={`operate-palette${collapsed ? ' operate-palette-collapsed' : ''}`}
      aria-label="Operate palette"
    >
      <button
        className="operate-palette-toggle"
        onClick={() => setCollapsed(!collapsed)}
        title={collapsed ? 'Expand operate palette' : 'Collapse operate palette'}
        aria-label={collapsed ? 'Expand operate palette' : 'Collapse operate palette'}
      >
        {collapsed ? '\u25B6' : '\u25C0'}
      </button>

      {!collapsed && (
        <div className="operate-palette-content">
          <div className="operate-palette-header">
            Operate
            {hasSelection && (
              <span className="operate-palette-count">{count}</span>
            )}
          </div>

          <div className="operate-palette-actions">
            <button
              className="operate-btn operate-btn-start"
              disabled={!hasSelection || stoppedCount === 0}
              onClick={handleStartSelected}
              title={!hasSelection ? 'Select components first' : stoppedCount === 0 ? 'No stopped processors' : `Start ${stoppedCount} stopped`}
              aria-label="Start selected"
            >
              <span className="operate-btn-icon" aria-hidden="true">{'\u25B6'}</span>
              <span className="operate-btn-label">Start</span>
            </button>

            <button
              className="operate-btn operate-btn-stop"
              disabled={!hasSelection || runningCount === 0}
              onClick={handleStopSelected}
              title={!hasSelection ? 'Select components first' : runningCount === 0 ? 'No running processors' : `Stop ${runningCount} running`}
              aria-label="Stop selected"
            >
              <span className="operate-btn-icon" aria-hidden="true">{'\u25A0'}</span>
              <span className="operate-btn-label">Stop</span>
            </button>

            <div className="operate-separator" aria-hidden="true" />

            <button
              className="operate-btn operate-btn-enable"
              disabled={!hasSelection || disabledCount === 0}
              onClick={handleEnableSelected}
              title={!hasSelection ? 'Select components first' : disabledCount === 0 ? 'No disabled processors' : `Enable ${disabledCount} disabled`}
              aria-label="Enable selected"
            >
              <span className="operate-btn-icon" aria-hidden="true">{'\u2713'}</span>
              <span className="operate-btn-label">Enable</span>
            </button>

            <button
              className="operate-btn operate-btn-disable"
              disabled={!hasSelection || enabledCount === 0}
              onClick={handleDisableSelected}
              title={!hasSelection ? 'Select components first' : enabledCount === 0 ? 'No enabled processors' : `Disable ${enabledCount} enabled`}
              aria-label="Disable selected"
            >
              <span className="operate-btn-icon" aria-hidden="true">{'\u2298'}</span>
              <span className="operate-btn-label">Disable</span>
            </button>

            <div className="operate-separator" aria-hidden="true" />

            <button
              className="operate-btn operate-btn-color"
              disabled={!hasSelection || !onColorSelected}
              onClick={handleColorSelected}
              title={!hasSelection ? 'Select components first' : `Change color for ${count} selected`}
              aria-label="Change color"
            >
              <span className="operate-btn-icon" aria-hidden="true">{'\u25CF'}</span>
              <span className="operate-btn-label">Color</span>
            </button>

            <button
              className="operate-btn operate-btn-delete"
              disabled={!hasSelection}
              onClick={handleDeleteSelected}
              title={!hasSelection ? 'Select components first' : `Delete ${count} selected`}
              aria-label="Delete selected"
            >
              <span className="operate-btn-icon" aria-hidden="true">{'\u2715'}</span>
              <span className="operate-btn-label">Delete</span>
            </button>
          </div>
        </div>
      )}
    </aside>
  );
}

export const OperatePalette = memo(OperatePaletteInner);
