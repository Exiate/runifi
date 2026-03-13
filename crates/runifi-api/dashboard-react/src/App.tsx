import { useMemo } from 'react';
import { Header } from './components/Header';
import { SummaryBar } from './components/SummaryBar';
import { FlowCanvas } from './components/FlowCanvas';
import { useFlowTopology } from './hooks/useFlowTopology';
import { useSseMetrics } from './hooks/useSseMetrics';

export function App() {
  const { topology, error, loading } = useFlowTopology();
  const { latest: liveMetrics, status: sseStatus } = useSseMetrics();

  const uptimeSecs = liveMetrics?.uptime_secs ?? 0;
  const flowName = topology?.name ?? '';

  const canvasKey = useMemo(
    () => topology?.name ?? 'empty',
    [topology?.name],
  );

  return (
    <div className="app-layout">
      <Header flowName={flowName} uptimeSecs={uptimeSecs} sseStatus={sseStatus} />
      <SummaryBar metrics={liveMetrics} />

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

          {topology && !loading && (
            <FlowCanvas
              key={canvasKey}
              topology={topology}
              liveMetrics={liveMetrics}
            />
          )}

          {!topology && !loading && !error && (
            <div className="canvas-placeholder">No processors configured.</div>
          )}
        </section>
      </main>
    </div>
  );
}
