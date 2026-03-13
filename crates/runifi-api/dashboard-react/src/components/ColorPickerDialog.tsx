import { memo, useEffect, useRef } from 'react';

const PRESET_COLORS = [
  { name: 'Default', value: '' },
  { name: 'Blue', value: '#4f8ff7' },
  { name: 'Green', value: '#34d399' },
  { name: 'Yellow', value: '#fbbf24' },
  { name: 'Red', value: '#f87171' },
  { name: 'Purple', value: '#a78bfa' },
  { name: 'Orange', value: '#fb923c' },
  { name: 'Pink', value: '#f472b6' },
  { name: 'Teal', value: '#2dd4bf' },
];

interface ColorPickerDialogProps {
  processorName: string;
  currentColor: string;
  onSelect: (color: string) => void;
  onClose: () => void;
}

function ColorPickerDialogInner({
  processorName,
  currentColor,
  onSelect,
  onClose,
}: ColorPickerDialogProps) {
  const panelRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose]);

  return (
    <div
      className="modal-overlay"
      role="dialog"
      aria-modal="true"
      aria-labelledby="color-picker-title"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="modal-panel color-picker-panel" ref={panelRef}>
        <h3 id="color-picker-title" className="modal-title">
          Change Color: {processorName}
        </h3>

        <div className="color-grid">
          {PRESET_COLORS.map((c) => (
            <button
              key={c.name}
              className={`color-swatch${currentColor === c.value ? ' active' : ''}`}
              style={{
                backgroundColor: c.value || 'var(--bg)',
                borderColor: c.value || 'var(--border)',
              }}
              onClick={() => onSelect(c.value)}
              title={c.name}
              aria-label={`${c.name}${currentColor === c.value ? ' (current)' : ''}`}
            >
              {currentColor === c.value && (
                <span className="color-check" aria-hidden="true">&#x2713;</span>
              )}
            </button>
          ))}
        </div>

        <div className="modal-actions">
          <button className="btn btn-ghost" onClick={onClose}>
            Cancel
          </button>
        </div>
      </div>
    </div>
  );
}

export const ColorPickerDialog = memo(ColorPickerDialogInner);
