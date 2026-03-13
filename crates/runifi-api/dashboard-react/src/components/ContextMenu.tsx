import { memo, useEffect, useRef } from 'react';

export interface ContextMenuState {
  x: number;
  y: number;
  nodeId: string | null;
  edgeId: string | null;
  nodeState?: string;
  isCanvas?: boolean;
  selectedNodeIds?: string[];
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
  onViewStatus?: () => void;
  onChangeColor?: () => void;
  onViewQueue?: () => void;
  onSelectAll?: () => void;
  onStartSelected?: () => void;
  onStopSelected?: () => void;
  onDeleteSelected?: () => void;
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
  onViewStatus,
  onChangeColor,
  onViewQueue,
  onSelectAll,
  onStartSelected,
  onStopSelected,
  onDeleteSelected,
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
  const isEdge = menu.edgeId !== null;
  const isCanvas = menu.isCanvas === true;
  const hasMultiSelect = (menu.selectedNodeIds?.length ?? 0) > 1;
  const state = menu.nodeState ?? 'stopped';
  const isRunning = state === 'running';
  const isPaused = state === 'paused';
  const isStopped = state === 'stopped';
  const isCircuitOpen = state === 'circuit-open';

  // Canvas context menu
  if (isCanvas) {
    return (
      <div
        ref={menuRef}
        className="context-menu"
        style={{ left: menu.x, top: menu.y }}
        role="menu"
        aria-label="Canvas context menu"
      >
        {onSelectAll && (
          <button
            className="context-menu-item"
            onClick={() => { onSelectAll(); onClose(); }}
            role="menuitem"
          >
            Select All
          </button>
        )}
      </div>
    );
  }

  // Multi-select context menu
  if (hasMultiSelect && isNode) {
    const count = menu.selectedNodeIds!.length;
    return (
      <div
        ref={menuRef}
        className="context-menu"
        style={{ left: menu.x, top: menu.y }}
        role="menu"
        aria-label="Multi-select context menu"
      >
        <div className="context-menu-header">{count} processors selected</div>
        <div className="context-menu-separator" aria-hidden="true" />
        {onStartSelected && (
          <button
            className="context-menu-item"
            onClick={() => { onStartSelected(); onClose(); }}
            role="menuitem"
          >
            Start Selected
          </button>
        )}
        {onStopSelected && (
          <button
            className="context-menu-item"
            onClick={() => { onStopSelected(); onClose(); }}
            role="menuitem"
          >
            Stop Selected
          </button>
        )}
        <div className="context-menu-separator" aria-hidden="true" />
        {onDeleteSelected && (
          <button
            className="context-menu-item context-menu-item-danger"
            onClick={() => { onDeleteSelected(); onClose(); }}
            role="menuitem"
          >
            Delete Selected
          </button>
        )}
      </div>
    );
  }

  // Edge context menu
  if (isEdge && !isNode) {
    return (
      <div
        ref={menuRef}
        className="context-menu"
        style={{ left: menu.x, top: menu.y }}
        role="menu"
        aria-label="Connection context menu"
      >
        {onViewQueue && (
          <button
            className="context-menu-item"
            onClick={() => { onViewQueue(); onClose(); }}
            role="menuitem"
          >
            View Queue
          </button>
        )}
        <div className="context-menu-separator" aria-hidden="true" />
        <button
          className="context-menu-item context-menu-item-danger"
          onClick={onDelete}
          role="menuitem"
        >
          Delete Connection
        </button>
      </div>
    );
  }

  // Node context menu (single processor)
  return (
    <div
      ref={menuRef}
      className="context-menu"
      style={{ left: menu.x, top: menu.y }}
      role="menu"
      aria-label="Processor context menu"
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

      {isNode && onViewStatus && (
        <button
          className="context-menu-item"
          onClick={() => { onViewStatus(); onClose(); }}
          role="menuitem"
        >
          View Status
        </button>
      )}

      {isNode && (
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

      {isNode && onChangeColor && (
        <button
          className="context-menu-item"
          onClick={() => { onChangeColor(); onClose(); }}
          role="menuitem"
        >
          Change Color
        </button>
      )}

      {isNode && onChangeColor && (
        <div className="context-menu-separator" aria-hidden="true" />
      )}

      <button
        className="context-menu-item context-menu-item-danger"
        onClick={onDelete}
        disabled={isNode && isRunning}
        title={isNode && isRunning ? 'Stop the processor before deleting' : undefined}
        role="menuitem"
      >
        {isNode ? 'Delete Processor' : 'Delete Connection'}
        {isNode && isRunning && <span className="context-menu-hint"> (stop first)</span>}
      </button>
    </div>
  );
}

export const ContextMenu = memo(ContextMenuInner);
