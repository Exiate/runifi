import { memo, useState, useEffect, useRef, type FormEvent } from 'react';

interface NamePromptDialogProps {
  title: string;
  label: string;
  placeholder?: string;
  onConfirm: (name: string) => void;
  onCancel: () => void;
}

function NamePromptDialogInner({
  title,
  label,
  placeholder,
  onConfirm,
  onCancel,
}: NamePromptDialogProps) {
  const [name, setName] = useState('');
  const [error, setError] = useState<string | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onCancel();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onCancel]);

  const validate = (value: string): string | null => {
    if (!value.trim()) return 'Name is required.';
    if (value.trim().length > 128) return 'Name must not exceed 128 characters.';
    if (!/^[a-zA-Z0-9_ -]+$/.test(value.trim()))
      return 'Name may only contain letters, numbers, hyphens, underscores, and spaces.';
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
      aria-labelledby="name-prompt-title"
      onClick={(e) => {
        if (e.target === e.currentTarget) onCancel();
      }}
    >
      <div className="modal-panel add-proc-panel">
        <h3 id="name-prompt-title" className="modal-title">
          {title}
        </h3>

        <form onSubmit={handleSubmit} noValidate>
          <label className="form-label" htmlFor="name-prompt-input">
            {label}
          </label>
          <input
            id="name-prompt-input"
            ref={inputRef}
            className={`form-input${error ? ' form-input-error' : ''}`}
            type="text"
            value={name}
            onChange={(e) => handleChange(e.target.value)}
            autoComplete="off"
            spellCheck={false}
            placeholder={placeholder}
          />
          {error && <p className="form-error">{error}</p>}

          <div className="modal-actions">
            <button type="button" className="btn btn-ghost" onClick={onCancel}>
              Cancel
            </button>
            <button type="submit" className="btn btn-primary" disabled={!!error || !name.trim()}>
              Create
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}

export const NamePromptDialog = memo(NamePromptDialogInner);
