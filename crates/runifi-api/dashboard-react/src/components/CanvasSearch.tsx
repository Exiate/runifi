import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';

interface SearchResult {
  id: string;
  name: string;
  typeName: string;
}

interface CanvasSearchProps {
  nodes: Array<{
    id: string;
    type?: string;
    data: Record<string, unknown>;
  }>;
  onResultSelect: (nodeId: string) => void;
}

function CanvasSearchInner({ nodes, onResultSelect }: CanvasSearchProps) {
  const [query, setQuery] = useState('');
  const [isOpen, setIsOpen] = useState(false);
  const [selectedIndex, setSelectedIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  const results = useMemo<SearchResult[]>(() => {
    if (!query.trim()) return [];
    const q = query.toLowerCase();
    return nodes
      .filter((n) => n.type !== 'labelNode')
      .map((n) => ({
        id: n.id,
        name: (n.data.label as string) ?? n.id,
        typeName: (n.data.typeName as string) ?? '',
      }))
      .filter(
        (r) =>
          r.name.toLowerCase().includes(q) ||
          r.typeName.toLowerCase().includes(q),
      )
      .slice(0, 20);
  }, [query, nodes]);

  useEffect(() => {
    setSelectedIndex(0);
  }, [query]);

  const handleSelect = useCallback(
    (nodeId: string) => {
      onResultSelect(nodeId);
      setQuery('');
      setIsOpen(false);
      inputRef.current?.blur();
    },
    [onResultSelect],
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === 'ArrowDown') {
        e.preventDefault();
        setSelectedIndex((i) => Math.min(i + 1, results.length - 1));
      } else if (e.key === 'ArrowUp') {
        e.preventDefault();
        setSelectedIndex((i) => Math.max(i - 1, 0));
      } else if (e.key === 'Enter' && results.length > 0) {
        e.preventDefault();
        handleSelect(results[selectedIndex].id);
      } else if (e.key === 'Escape') {
        setQuery('');
        setIsOpen(false);
        inputRef.current?.blur();
      }
      e.stopPropagation();
    },
    [results, selectedIndex, handleSelect],
  );

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  return (
    <div className="canvas-search" ref={containerRef}>
      <div className="canvas-search-input-wrapper">
        <svg className="canvas-search-icon" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
          <circle cx="11" cy="11" r="8" />
          <path d="m21 21-4.3-4.3" />
        </svg>
        <input
          ref={inputRef}
          className="canvas-search-input"
          type="text"
          placeholder="Search components..."
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setIsOpen(true);
          }}
          onFocus={() => setIsOpen(true)}
          onKeyDown={handleKeyDown}
        />
        {query && (
          <button
            className="canvas-search-clear"
            onClick={() => {
              setQuery('');
              inputRef.current?.focus();
            }}
            aria-label="Clear search"
          >
            &times;
          </button>
        )}
      </div>
      {isOpen && results.length > 0 && (
        <div className="canvas-search-results" role="listbox">
          {results.map((r, i) => (
            <button
              key={r.id}
              className={`canvas-search-result${i === selectedIndex ? ' canvas-search-result-active' : ''}`}
              onClick={() => handleSelect(r.id)}
              role="option"
              aria-selected={i === selectedIndex}
            >
              <span className="canvas-search-result-name">{r.name}</span>
              <span className="canvas-search-result-type">{r.typeName}</span>
            </button>
          ))}
        </div>
      )}
      {isOpen && query.trim() && results.length === 0 && (
        <div className="canvas-search-results">
          <div className="canvas-search-empty">No matching components</div>
        </div>
      )}
    </div>
  );
}

export const CanvasSearch = memo(CanvasSearchInner);
