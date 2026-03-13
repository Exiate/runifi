import { memo, useState, useEffect } from 'react';

interface ConnectionModalProps {
  sourceId: string;
  targetId: string;
  relationships: string[];
  existingRelationships: string[]; // already connected from this source to target
  onConfirm: (relationship: string) => void;
  onCancel: () => void;
}

function ConnectionModalInner({
  sourceId,
  targetId,
  relationships,
  existingRelationships,
  onConfirm,
  onCancel,
}: ConnectionModalProps) {
  const available = relationships.filter((r) => !existingRelationships.includes(r));
  const [selected, setSelected] = useState<string>(available[0] ?? '');

  // Close on Escape
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onCancel();
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onCancel]);

  if (available.length === 0) {
    return (
      <div
        className="modal-overlay"
        role="dialog"
        aria-modal="true"
        aria-labelledby="conn-title"
        onClick={(e) => {
          if (e.target === e.currentTarget) onCancel();
        }}
      >
        <div className="modal-panel confirm-panel">
          <h3 id="conn-title" className="modal-title">
            Cannot Connect
          </h3>
          <p className="modal-body">
            All relationships from <strong>{sourceId}</strong> to <strong>{targetId}</strong> are
            already connected.
          </p>
          <div className="modal-actions">
            <button className="btn btn-primary" onClick={onCancel}>
              Close
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div
      className="modal-overlay"
      role="dialog"
      aria-modal="true"
      aria-labelledby="conn-title"
      onClick={(e) => {
        if (e.target === e.currentTarget) onCancel();
      }}
    >
      <div className="modal-panel confirm-panel">
        <h3 id="conn-title" className="modal-title">
          Create Connection
        </h3>
        <p className="modal-body">
          Connect <strong>{sourceId}</strong> to <strong>{targetId}</strong>
        </p>

        {available.length > 1 ? (
          <div className="form-group">
            <label className="form-label" htmlFor="rel-select">
              Relationship
            </label>
            <select
              id="rel-select"
              className="form-select"
              value={selected}
              onChange={(e) => setSelected(e.target.value)}
            >
              {available.map((r) => (
                <option key={r} value={r}>
                  {r}
                </option>
              ))}
            </select>
          </div>
        ) : (
          <p className="conn-relationship-display">
            Relationship: <strong>{available[0]}</strong>
          </p>
        )}

        <div className="modal-actions">
          <button className="btn btn-ghost" onClick={onCancel}>
            Cancel
          </button>
          <button
            className="btn btn-primary"
            onClick={() => onConfirm(selected)}
            disabled={!selected}
          >
            Connect
          </button>
        </div>
      </div>
    </div>
  );
}

export const ConnectionModal = memo(ConnectionModalInner);
