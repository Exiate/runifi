import { memo, useState, useEffect, useRef, type FormEvent } from 'react';
import type { PluginDescriptor } from '../types/api';

interface AddProcessorModalProps {
  plugin: PluginDescriptor;
  existingNames: Set<string>;
  onConfirm: (name: string) => void;
  onCancel: () => void;
}

function AddProcessorModalInner({
  plugin,
  existingNames,
  onConfirm,
  onCancel,
}: AddProcessorModalProps) {
  // Suggest a default name derived from the type
  const suggestName = (): string => {
    const base = plugin.type_name
      .replace(/([A-Z])/g, (m) => `-${m.toLowerCase()}`)
      .replace(/^-/, '');
    let candidate = base;
    let n = 1;
    while (existingNames.has(candidate)) {
      candidate = `${base}-${n++}`;
    }
    return candidate;
  };

  const [name, setName] = useState(() => suggestName());
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.select();
  }, []);

  // Close on Escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onCancel();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onCancel]);

  const validate = (value: string): string | null => {
    if (!value.trim()) return 'Name is required.';
    if (existingNames.has(value.trim()))
      return `A processor named "${value.trim()}" already exists.`;
    if (!/^[a-zA-Z0-9_-]+$/.test(value.trim()))
      return 'Name may only contain letters, numbers, hyphens, and underscores.';
    return null;
  };

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    const validationError = validate(name);
    if (validationError) {
      setError(validationError);
      return;
    }
    onConfirm(name.trim());
  };

  const handleChange = (value: string) => {
    setName(value);
    setError(validate(value));
  };

  return (
    <div
      className="modal-overlay"
      role="dialog"
      aria-modal="true"
      aria-labelledby="add-proc-title"
      onClick={(e) => {
        if (e.target === e.currentTarget) onCancel();
      }}
    >
      <div className="modal-panel add-proc-panel">
        <h3 id="add-proc-title" className="modal-title">
          Add Processor
        </h3>
        <p className="modal-subtitle">
          <span className="modal-type-tag">{plugin.display_name ?? plugin.type_name}</span>
          {plugin.description && <span className="modal-type-desc">{plugin.description}</span>}
        </p>

        <form onSubmit={handleSubmit} noValidate>
          <label className="form-label" htmlFor="proc-name">
            Processor Name
          </label>
          <input
            id="proc-name"
            ref={inputRef}
            className={`form-input${error ? ' form-input-error' : ''}`}
            type="text"
            value={name}
            onChange={(e) => handleChange(e.target.value)}
            autoComplete="off"
            spellCheck={false}
            placeholder="e.g. my-generate-flowfile"
          />
          {error && <p className="form-error">{error}</p>}

          <div className="modal-actions">
            <button type="button" className="btn btn-ghost" onClick={onCancel}>
              Cancel
            </button>
            <button type="submit" className="btn btn-primary" disabled={!!error || !name.trim()}>
              Add
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

export const AddProcessorModal = memo(AddProcessorModalInner);
