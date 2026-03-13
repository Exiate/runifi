import { memo, useEffect, useRef } from 'react';

export interface ContextMenuState {
  x: number;
  y: number;
  nodeId: string | null;
  edgeId: string | null;
  nodeState?: string;
}

interface ContextMenuProps {
  menu: ContextMenuState;
  onDelete: () => void;
  onConfigure?: () => void;
  onStart?: () => void;
  onStop?: () => void;
  onPause?: () => void;
  onResume?: () => void;
  onResetCircuit?: () => void;
  onClose: () => void;
}

function ContextMenuInner({
  menu,
  onDelete,
  onConfigure,
  onStart,
  onStop,
  onPause,
  onResume,
  onResetCircuit,
  onClose,
}: ContextMenuProps) {
  const menuRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: MouseEvent | KeyboardEvent) => {
      if (e instanceof KeyboardEvent) {
        if (e.key === 'Escape') onClose();
        return;
      }
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        onClose();
      }
    };
    window.addEventListener('mousedown', handler);
    window.addEventListener('keydown', handler);
    return () => {
      window.removeEventListener('mousedown', handler);
      window.removeEventListener('keydown', handler);
    };
  }, [onClose]);

  const isNode = menu.nodeId !== null;
  const state = menu.nodeState ?? 'stopped';
  const isRunning = state === 'running';
  const isPaused = state === 'paused';
  const isStopped = state === 'stopped';
  const isCircuitOpen = state === 'circuit-open';
  const deleteLabel = isNode ? 'Delete Processor' : 'Delete Connection';

  return (
    <div
      ref={menuRef}
      className="context-menu"
      style={{ left: menu.x, top: menu.y }}
      role="menu"
      aria-label="Context menu"
    >
      {isNode && onConfigure && (
        <button
          className="context-menu-item"
          onClick={() => { onConfigure(); onClose(); }}
          role="menuitem"
        >
          Configure
        </button>
      )}

      {isNode && (onStart || onStop || onPause || onResume || onResetCircuit) && (
        <div className="context-menu-separator" aria-hidden="true" />
      )}

      {isNode && onStart && (
        <button
          className="context-menu-item"
          onClick={() => { onStart(); onClose(); }}
          disabled={isRunning || isPaused}
          role="menuitem"
        >
          Start
        </button>
      )}

      {isNode && onPause && (
        <button
          className="context-menu-item"
          onClick={() => { onPause(); onClose(); }}
          disabled={!isRunning}
          role="menuitem"
        >
          Pause
        </button>
      )}

      {isNode && onResume && (
        <button
          className="context-menu-item"
          onClick={() => { onResume(); onClose(); }}
          disabled={!isPaused}
          role="menuitem"
        >
          Resume
        </button>
      )}

      {isNode && onStop && (
        <button
          className="context-menu-item"
          onClick={() => { onStop(); onClose(); }}
          disabled={isStopped}
          role="menuitem"
        >
          Stop
        </button>
      )}

      {isNode && onResetCircuit && (
        <button
          className="context-menu-item"
          onClick={() => { onResetCircuit(); onClose(); }}
          disabled={!isCircuitOpen}
          role="menuitem"
          title={isCircuitOpen ? 'Reset circuit breaker' : 'Circuit breaker is not open'}
        >
          Reset Circuit
        </button>
      )}

      {isNode && (
        <div className="context-menu-separator" aria-hidden="true" />
      )}

      <button
        className="context-menu-item context-menu-item-danger"
        onClick={onDelete}
        disabled={isNode && isRunning}
        title={isNode && isRunning ? 'Stop the processor before deleting' : undefined}
        role="menuitem"
      >
        {deleteLabel}
        {isNode && isRunning && <span className="context-menu-hint"> (stop first)</span>}
      </button>
    </div>
  );
}

export const ContextMenu = memo(ContextMenuInner);
