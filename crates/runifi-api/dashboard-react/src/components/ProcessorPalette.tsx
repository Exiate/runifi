import { memo, useState, useMemo } from 'react';
import type { PluginDescriptor, PluginKind } from '../types/api';

interface ProcessorPaletteProps {
  plugins: PluginDescriptor[];
  loading: boolean;
  collapsed: boolean;
  onToggle: () => void;
  onDragStart: (plugin: PluginDescriptor) => void;
}

const KIND_LABELS: Record<PluginKind, string> = {
  source: 'Sources',
  processor: 'Processors',
  sink: 'Sinks',
};

const KIND_ORDER: PluginKind[] = ['source', 'processor', 'sink'];

function kindIcon(kind: PluginKind): string {
  switch (kind) {
    case 'source':
      return '▶';
    case 'sink':
      return '◼';
    default:
      return '◆';
  }
}

function PaletteItem({
  plugin,
  onDragStart,
}: {
  plugin: PluginDescriptor;
  onDragStart: (plugin: PluginDescriptor) => void;
}) {
  return (
    <div
      className={`palette-item palette-item-${plugin.kind}`}
      draggable
      onDragStart={(e) => {
        e.dataTransfer.effectAllowed = 'copy';
        e.dataTransfer.setData('application/runifi-plugin', plugin.type_name);
        onDragStart(plugin);
      }}
      title={plugin.description ?? plugin.type_name}
      role="listitem"
      aria-label={`Drag to add ${plugin.display_name ?? plugin.type_name}`}
    >
      <span className="palette-item-icon" aria-hidden="true">
        {kindIcon(plugin.kind)}
      </span>
      <div className="palette-item-text">
        <span className="palette-item-name">{plugin.display_name ?? plugin.type_name}</span>
        {plugin.description && <span className="palette-item-desc">{plugin.description}</span>}
      </div>
    </div>
  );
}

function ProcessorPaletteInner({
  plugins,
  loading,
  collapsed,
  onToggle,
  onDragStart,
}: ProcessorPaletteProps) {
  const [search, setSearch] = useState('');

  const grouped = useMemo(() => {
    const q = search.toLowerCase();
    const filtered = plugins.filter(
      (p) =>
        (p.display_name ?? p.type_name).toLowerCase().includes(q) ||
        p.type_name.toLowerCase().includes(q) ||
        (p.description ?? '').toLowerCase().includes(q),
    );

    const groups = new Map<PluginKind, PluginDescriptor[]>();
    for (const kind of KIND_ORDER) {
      const items = filtered.filter((p) => p.kind === kind);
      if (items.length > 0) groups.set(kind, items);
    }
    return groups;
  }, [plugins, search]);

  if (collapsed) {
    return (
      <aside className="palette-sidebar palette-sidebar-collapsed" aria-label="Processor palette">
        <button
          className="palette-toggle"
          onClick={onToggle}
          aria-label="Expand processor palette"
          title="Expand palette"
        >
          &#9654;
        </button>
      </aside>
    );
  }

  return (
    <aside className="palette-sidebar" aria-label="Processor palette">
      <div className="palette-header">
        <span className="palette-title">Processors</span>
        <button
          className="palette-toggle"
          onClick={onToggle}
          aria-label="Collapse processor palette"
          title="Collapse palette"
        >
          &#9664;
        </button>
      </div>

      <div className="palette-search-wrap">
        <input
          className="palette-search"
          type="search"
          placeholder="Search..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          aria-label="Search processors"
        />
      </div>

      <div className="palette-list" role="list" aria-label="Available processor types">
        {loading && <div className="palette-loading">Loading plugins...</div>}

        {!loading && grouped.size === 0 && (
          <div className="palette-empty">No processors match your search.</div>
        )}

        {[...grouped.entries()].map(([kind, items]) => (
          <div key={kind} className="palette-group">
            <div className="palette-group-label">{KIND_LABELS[kind]}</div>
            {items.map((plugin) => (
              <PaletteItem key={plugin.type_name} plugin={plugin} onDragStart={onDragStart} />
            ))}
          </div>
        ))}
      </div>

      <div className="palette-hint">Drag a processor onto the canvas</div>
    </aside>
  );
}

export const ProcessorPalette = memo(ProcessorPaletteInner);
