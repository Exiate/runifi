import { memo } from 'react';
import type { Toast } from '../hooks/useToast';

interface ToastNotifierProps {
  toasts: Toast[];
  onDismiss: (id: number) => void;
}

function ToastNotifierInner({ toasts, onDismiss }: ToastNotifierProps) {
  if (toasts.length === 0) return null;

  return (
    <div className="toast-container" role="region" aria-live="polite" aria-label="Notifications">
      {toasts.map((toast) => (
        <div key={toast.id} className={`toast toast-${toast.kind}`} role="alert">
          <span className="toast-message">{toast.message}</span>
          <button
            className="toast-close"
            onClick={() => onDismiss(toast.id)}
            aria-label="Dismiss notification"
          >
            &times;
          </button>
        </div>
      ))}
    </div>
  );
}

export const ToastNotifier = memo(ToastNotifierInner);
