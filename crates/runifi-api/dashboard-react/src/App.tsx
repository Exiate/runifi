import { useCallback, useMemo, useState } from 'react';
import { Header } from './components/Header';
import { SummaryBar } from './components/SummaryBar';
import { FlowCanvas } from './components/FlowCanvas';
import { ComponentToolbar } from './components/ComponentToolbar';
import { OperatePalette, type SelectedProcessor } from './components/OperatePalette';
import { Breadcrumb } from './components/Breadcrumb';
import { ToastNotifier } from './components/ToastNotifier';
import { ControllerServicesPanel } from './components/ControllerServicesPanel';
import { BulletinBoard } from './components/BulletinBoard';
import { useFlowTopology } from './hooks/useFlowTopology';
import { useGroupNavigation } from './hooks/useGroupNavigation';
import { useSseMetrics } from './hooks/useSseMetrics';
import { usePlugins } from './hooks/usePlugins';
import { useToast } from './hooks/useToast';
import type { PluginDescriptor } from './types/api';

export function App() {
  const { topology, error, loading } = useFlowTopology();
  const { latest: liveMetrics, status: sseStatus } = useSseMetrics();
  const { plugins, loading: pluginsLoading } = usePlugins();
  const { toasts, push: pushToast, dismiss: dismissToast } = useToast();
  const {
    currentGroupId,
    breadcrumb,
    enterGroup,
    exitGroup,
    navigateTo,
    groupFlow,
    groupLoading,
  } = useGroupNavigation();

  const [draggedPlugin, setDraggedPlugin] = useState<PluginDescriptor | null>(null);
  const [addPluginAtCenter, setAddPluginAtCenter] = useState<PluginDescriptor | null>(null);
  const [bulletinOpen, setBulletinOpen] = useState(false);
  const [controllerServicesOpen, setControllerServicesOpen] = useState(false);
  const [selectedNodeIds, setSelectedNodeIds] = useState<string[]>([]);
  const [colorRequestNodeIds, setColorRequestNodeIds] = useState<string[] | null>(null);

  const uptimeSecs = liveMetrics?.uptime_secs ?? 0;
  const flowName = topology?.name ?? '';

  // When inside a group, use group flow data for the canvas.
  const canvasTopology = useMemo(() => {
    if (currentGroupId && groupFlow) {
      return {
        name: groupFlow.name,
        processors: groupFlow.processors,
        connections: groupFlow.connections,
      };
    }
    return topology ?? { name: 'new-flow', processors: [], connections: [] };
  }, [currentGroupId, groupFlow, topology]);

  // Canvas key changes on group navigation to force re-mount.
  const canvasKey = useMemo(
    () => currentGroupId ?? topology?.name ?? 'empty',
    [currentGroupId, topology?.name],
  );

  const processorNames = useMemo(
    () => (liveMetrics?.processors ?? []).map((p) => p.name),
    [liveMetrics],
  );

  const selectedProcessors = useMemo<SelectedProcessor[]>(() => {
    if (!liveMetrics || selectedNodeIds.length === 0) return [];
    const result: SelectedProcessor[] = [];
    for (const id of selectedNodeIds) {
      const proc = liveMetrics.processors.find((p) => p.name === id);
      if (proc) result.push({ name: proc.name, state: proc.state as string });
    }
    return result;
  }, [selectedNodeIds, liveMetrics]);

  const handleDragEnd = () => setDraggedPlugin(null);
  const handleAddPluginHandled = useCallback(() => setAddPluginAtCenter(null), []);
  const handleColorRequestHandled = useCallback(() => setColorRequestNodeIds(null), []);
  const toggleBulletins = useCallback(() => setBulletinOpen((v) => !v), []);

  const isLoading = loading || (currentGroupId !== null && groupLoading);

  return (
    <div className="app-layout" onDragEnd={handleDragEnd}>
      <Header
        flowName={flowName}
        uptimeSecs={uptimeSecs}
        sseStatus={sseStatus}
        onOpenBulletins={toggleBulletins}
        onOpenControllerServices={() => setControllerServicesOpen(true)}
      />
      <ComponentToolbar
        plugins={plugins}
        loading={pluginsLoading}
        onDragStart={setDraggedPlugin}
        onAddProcessor={setAddPluginAtCenter}
      />

      <Breadcrumb
        flowName={flowName}
        segments={breadcrumb}
        onNavigate={navigateTo}
      />

      <div className="app-body">
        <OperatePalette
          selectedProcessors={selectedProcessors}
          onToast={pushToast}
          onColorSelected={setColorRequestNodeIds}
        />
        <main className="app-main">
          <section className="dag-section" aria-labelledby="dag-heading">
            <h2 id="dag-heading" className="sr-only">Flow Topology</h2>

            {isLoading && (
              <div className="canvas-placeholder" role="status" aria-live="polite">
                Loading topology...
              </div>
            )}

            {error && !currentGroupId && (
              <div className="canvas-error" role="alert">
                Failed to load topology: {error}
              </div>
            )}

            {!isLoading && (
              <FlowCanvas
                key={canvasKey}
                topology={canvasTopology}
                liveMetrics={liveMetrics}
                plugins={plugins}
                onToast={pushToast}
                draggedPlugin={draggedPlugin}
                addPluginAtCenter={addPluginAtCenter}
                onAddPluginHandled={handleAddPluginHandled}
                onSelectionChange={setSelectedNodeIds}
                colorRequestNodeIds={colorRequestNodeIds}
                onColorRequestHandled={handleColorRequestHandled}
                onEnterGroup={enterGroup}
                onExitGroup={exitGroup}
                currentGroupId={currentGroupId}
                processGroups={
                  currentGroupId && groupFlow
                    ? groupFlow.child_groups
                    : topology?.process_groups
                }
                inputPorts={groupFlow?.input_ports}
                outputPorts={groupFlow?.output_ports}
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

      <SummaryBar
        metrics={liveMetrics}
        onOpenBulletins={toggleBulletins}
      />

      <ToastNotifier toasts={toasts} onDismiss={dismissToast} />

      {controllerServicesOpen && (
        <ControllerServicesPanel
          plugins={plugins}
          onToast={pushToast}
          onClose={() => setControllerServicesOpen(false)}
        />
      )}
    </div>
  );
}
