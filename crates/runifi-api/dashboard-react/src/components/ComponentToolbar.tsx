import { memo, useState, useMemo, useCallback, useRef, useEffect } from 'react';
import type { PluginDescriptor, PluginKind } from '../types/api';

interface ComponentToolbarProps {
  plugins: PluginDescriptor[];
  loading: boolean;
  onDragStart: (plugin: PluginDescriptor) => void;
}

const COMPONENT_TYPES = [
  { id: 'processor', label: 'Processor', icon: '\u25C6' },
  { id: 'input-port', label: 'Input Port', icon: '\u25B6' },
  { id: 'output-port', label: 'Output Port', icon: '\u25C0' },
  { id: 'funnel', label: 'Funnel', icon: '\u25BD' },
  { id: 'label', label: 'Label', icon: '\u25A1' },
] as const;

const CATEGORY_MAP: Record<string, string> = {
  GenerateFlowFile: 'Data Generation',
  GetFile: 'File System',
  PutFile: 'File System',
  LogAttribute: 'Debug',
  RouteOnAttribute: 'Routing',
  UpdateAttribute: 'Attribute Manipulation',
};

function getCategory(typeName: string, kind: PluginKind): string {
  if (CATEGORY_MAP[typeName]) return CATEGORY_MAP[typeName];
  switch (kind) {
    case 'source': return 'Data Sources';
    case 'sink': return 'Data Sinks';
    default: return 'General';
  }
}

function ComponentToolbarInner({ plugins, loading, onDragStart }: ComponentToolbarProps) {
  const [showAddDialog, setShowAddDialog] = useState(false);
  const [search, setSearch] = useState('');
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set());
  const dialogRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);

  const categorized = useMemo(() => {
    const q = search.toLowerCase();
    const filtered = plugins.filter(
      (p) =>
        (p.display_name ?? '').toLowerCase().includes(q) ||
        (p.type_name ?? '').toLowerCase().includes(q) ||
        (p.description ?? '').toLowerCase().includes(q),
    );

    const cats = new Map<string, PluginDescriptor[]>();
    for (const p of filtered) {
      const cat = getCategory(p.type_name, p.kind);
      if (!cats.has(cat)) cats.set(cat, []);
      cats.get(cat)!.push(p);
    }

    return new Map([...cats.entries()].sort((a, b) => a[0].localeCompare(b[0])));
  }, [plugins, search]);

  // Expand all categories when search is active
  useEffect(() => {
    if (search) {
      setExpandedCategories(new Set(categorized.keys()));
    }
  }, [search, categorized]);

  // Focus search input when dialog opens
  useEffect(() => {
    if (showAddDialog) {
      setTimeout(() => searchRef.current?.focus(), 50);
    }
  }, [showAddDialog]);

  // Close dialog on click outside
  useEffect(() => {
    if (!showAddDialog) return;
    const handler = (e: MouseEvent) => {
      if (dialogRef.current && !dialogRef.current.contains(e.target as Node)) {
        setShowAddDialog(false);
        setSearch('');
      }
    };
    const keyHandler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        setShowAddDialog(false);
        setSearch('');
      }
    };
    window.addEventListener('mousedown', handler);
    window.addEventListener('keydown', keyHandler);
    return () => {
      window.removeEventListener('mousedown', handler);
      window.removeEventListener('keydown', keyHandler);
    };
  }, [showAddDialog]);

  const toggleCategory = useCallback((cat: string) => {
    setExpandedCategories((prev) => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  }, []);

  const handleToolbarDrag = useCallback(
    (e: React.DragEvent, componentType: string) => {
      if (componentType === 'processor') {
        setShowAddDialog(true);
        e.preventDefault();
        return;
      }
      e.dataTransfer.effectAllowed = 'copy';
      e.dataTransfer.setData('application/runifi-component', componentType);
    },
    [],
  );

  return (
    <div className="component-toolbar">
      <div className="toolbar-items">
        {COMPONENT_TYPES.map((ct) => (
          <button
            key={ct.id}
            className="toolbar-item"
            draggable={ct.id !== 'processor'}
            onDragStart={(e) => handleToolbarDrag(e, ct.id)}
            onClick={ct.id === 'processor' ? () => setShowAddDialog(!showAddDialog) : undefined}
            title={ct.id === 'processor' ? 'Click to add a processor' : `Drag to add ${ct.label}`}
            aria-label={ct.label}
          >
            <span className="toolbar-item-icon" aria-hidden="true">
              {ct.icon}
            </span>
            <span className="toolbar-item-label">{ct.label}</span>
          </button>
        ))}
      </div>

      {showAddDialog && (
        <div className="toolbar-add-dialog" ref={dialogRef}>
          <div className="toolbar-add-header">
            <span className="toolbar-add-title">Add Processor</span>
            <button
              className="toolbar-add-close"
              onClick={() => { setShowAddDialog(false); setSearch(''); }}
              aria-label="Close"
            >
              &times;
            </button>
          </div>

          <div className="toolbar-add-search-wrap">
            <input
              ref={searchRef}
              className="toolbar-add-search"
              type="search"
              placeholder="Filter processors..."
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              aria-label="Filter processors"
            />
          </div>

          <div className="toolbar-add-list" role="list">
            {loading && <div className="toolbar-add-empty">Loading plugins...</div>}

            {!loading && categorized.size === 0 && (
              <div className="toolbar-add-empty">No processors match your filter.</div>
            )}

            {[...categorized.entries()].map(([cat, items]) => (
              <div key={cat} className="toolbar-add-category">
                <button
                  className="toolbar-add-category-header"
                  onClick={() => toggleCategory(cat)}
                  aria-expanded={expandedCategories.has(cat)}
                >
                  <span className="toolbar-add-category-arrow">
                    {expandedCategories.has(cat) ? '\u25BC' : '\u25B6'}
                  </span>
                  <span className="toolbar-add-category-name">{cat}</span>
                  <span className="toolbar-add-category-count">{items.length}</span>
                </button>

                {expandedCategories.has(cat) && (
                  <div className="toolbar-add-category-items">
                    {items.map((plugin) => (
                      <div
                        key={plugin.type_name}
                        className="toolbar-add-item"
                        draggable
                        onDragStart={(e) => {
                          e.dataTransfer.effectAllowed = 'copy';
                          e.dataTransfer.setData('application/runifi-plugin', plugin.type_name);
                          onDragStart(plugin);
                          setShowAddDialog(false);
                          setSearch('');
                        }}
                        role="listitem"
                        aria-label={`Drag to add ${plugin.display_name}`}
                      >
                        <div className="toolbar-add-item-info">
                          <span className="toolbar-add-item-name">{plugin.display_name}</span>
                          <span className="toolbar-add-item-desc">{plugin.description}</span>
                        </div>
                        <span className={`toolbar-add-item-kind kind-${plugin.kind}`}>
                          {plugin.kind}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export const ComponentToolbar = memo(ComponentToolbarInner);
