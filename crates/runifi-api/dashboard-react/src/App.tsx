import { useMemo, useState } from 'react';
import { Header } from './components/Header';
import { SummaryBar } from './components/SummaryBar';
import { FlowCanvas } from './components/FlowCanvas';
import { ProcessorPalette } from './components/ProcessorPalette';
import { ToastNotifier } from './components/ToastNotifier';
import { BulletinBoard } from './components/BulletinBoard';
import { useFlowTopology } from './hooks/useFlowTopology';
import { useSseMetrics } from './hooks/useSseMetrics';
import { usePlugins } from './hooks/usePlugins';
import { useToast } from './hooks/useToast';
import type { PluginDescriptor } from './types/api';

export function App() {
  const { topology, error, loading } = useFlowTopology();
  const { latest: liveMetrics, status: sseStatus } = useSseMetrics();
  const { plugins, loading: pluginsLoading } = usePlugins();
  const { toasts, push: pushToast, dismiss: dismissToast } = useToast();

  const [paletteCollapsed, setPaletteCollapsed] = useState(false);
  const [draggedPlugin, setDraggedPlugin] = useState<PluginDescriptor | null>(null);
  const [bulletinOpen, setBulletinOpen] = useState(false);

  const uptimeSecs = liveMetrics?.uptime_secs ?? 0;
  const flowName = topology?.name ?? '';

  const canvasKey = useMemo(
    () => topology?.name ?? 'empty',
    [topology?.name],
  );

  const processorNames = useMemo(
    () => (liveMetrics?.processors ?? []).map((p) => p.name),
    [liveMetrics],
  );

  const handleDragEnd = () => setDraggedPlugin(null);

  return (
    <div className="app-layout" onDragEnd={handleDragEnd}>
      <Header flowName={flowName} uptimeSecs={uptimeSecs} sseStatus={sseStatus} />
      <SummaryBar
        metrics={liveMetrics}
        onOpenBulletins={() => setBulletinOpen((v) => !v)}
      />

      <div className="app-body">
        <ProcessorPalette
          plugins={plugins}
          loading={pluginsLoading}
          collapsed={paletteCollapsed}
          onToggle={() => setPaletteCollapsed((c) => !c)}
          onDragStart={setDraggedPlugin}
        />

        <main className="app-main">
          <section className="dag-section" aria-labelledby="dag-heading">
            <h2 id="dag-heading">Flow Topology</h2>

            {loading && (
              <div className="canvas-placeholder" role="status" aria-live="polite">
                Loading topology...
              </div>
            )}

            {error && (
              <div className="canvas-error" role="alert">
                Failed to load topology: {error}
              </div>
            )}

            {!loading && (
              <FlowCanvas
                key={canvasKey}
                topology={topology ?? { name: 'new-flow', processors: [], connections: [] }}
                liveMetrics={liveMetrics}
                plugins={plugins}
                onToast={pushToast}
                draggedPlugin={draggedPlugin}
              />
            )}
          </section>

          {bulletinOpen && (
            <BulletinBoard
              processorNames={processorNames}
              onClose={() => setBulletinOpen(false)}
            />
          )}
        </main>
      </div>

      <ToastNotifier toasts={toasts} onDismiss={dismissToast} />
    </div>
  );
}
