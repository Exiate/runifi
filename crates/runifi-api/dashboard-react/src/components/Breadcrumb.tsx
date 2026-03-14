import { memo } from 'react';
import type { BreadcrumbSegment } from '../types/api';

interface BreadcrumbProps {
  flowName: string;
  segments: BreadcrumbSegment[];
  onNavigate: (groupId: string | null) => void;
}

function BreadcrumbInner({ flowName, segments, onNavigate }: BreadcrumbProps) {
  if (segments.length === 0) return null;

  return (
    <nav className="breadcrumb-bar" aria-label="Process group navigation">
      <button
        className="breadcrumb-segment"
        onClick={() => onNavigate(null)}
        type="button"
      >
        {flowName || 'RuniFi Flow'}
      </button>

      {segments.map((seg, idx) => {
        const isCurrent = idx === segments.length - 1;
        return (
          <span key={seg.id}>
            <span className="breadcrumb-separator" aria-hidden="true">
              {' >> '}
            </span>
            {isCurrent ? (
              <span className="breadcrumb-current" aria-current="location">
                {seg.name}
              </span>
            ) : (
              <button
                className="breadcrumb-segment"
                onClick={() => onNavigate(seg.id)}
                type="button"
              >
                {seg.name}
              </button>
            )}
          </span>
        );
      })}
    </nav>
  );
}

export const Breadcrumb = memo(BreadcrumbInner);
