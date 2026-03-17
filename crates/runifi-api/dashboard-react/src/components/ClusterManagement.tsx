import { memo, useState, useEffect, useCallback } from 'react';
import { useCluster } from '../hooks/useCluster';
import { ConfirmDialog } from './ConfirmDialog';
import { formatUptime } from '../utils/format';
import type { ClusterNodeResponse, NodeState } from '../types/api';
import type { ToastKind } from '../hooks/useToast';

interface ClusterManagementProps {
  onClose: () => void;
  onToast: (kind: ToastKind, message: string) => void;
}

type NodeAction = {
  type: 'disconnect' | 'connect' | 'decommission' | 'remove' | 'primary';
  node: ClusterNodeResponse;
};

const STATE_BADGE_CLASS: Record<NodeState, string> = {
  Connected: 'cluster-badge-connected',
  Connecting: 'cluster-badge-connecting',
  Disconnected: 'cluster-badge-disconnected',
  Decommissioning: 'cluster-badge-decommissioning',
  Removed: 'cluster-badge-removed',
};

const ACTION_CONFIG: Record<
  NodeAction['type'],
  { title: string; message: (addr: string) => string; confirmLabel: string; destructive: boolean }
> = {
  disconnect: {
    title: 'Disconnect Node',
    message: (addr) => `Disconnect node "${addr}" from the cluster? The node will stop receiving work.`,
    confirmLabel: 'Disconnect',
    destructive: true,
  },
  connect: {
    title: 'Connect Node',
    message: (addr) => `Reconnect node "${addr}" to the cluster?`,
    confirmLabel: 'Connect',
    destructive: false,
  },
  decommission: {
    title: 'Decommission Node',
    message: (addr) =>
      `Begin graceful decommission of node "${addr}"? The node will drain queued data and then disconnect.`,
    confirmLabel: 'Decommission',
    destructive: true,
  },
  remove: {
    title: 'Remove Node',
    message: (addr) =>
      `Force-remove node "${addr}" from the cluster? This cannot be undone.`,
    confirmLabel: 'Remove',
    destructive: true,
  },
  primary: {
    title: 'Designate Primary',
    message: (addr) => `Designate node "${addr}" as the primary node?`,
    confirmLabel: 'Designate',
    destructive: false,
  },
};

function ClusterManagementInner({ onClose, onToast }: ClusterManagementProps) {
  const {
    nodes,
    status,
    loading,
    error,
    autoRefresh,
    setAutoRefresh,
    refresh,
    disconnectNode,
    connectNode,
    decommissionNode,
    removeNode,
    designatePrimary,
  } = useCluster(true);

  const [pendingAction, setPendingAction] = useState<NodeAction | null>(null);
  const [actionLoading, setActionLoading] = useState(false);

  // Close on Escape (only when no confirm dialog open)
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && !pendingAction) {
        onClose();
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose, pendingAction]);

  const executeAction = useCallback(async () => {
    if (!pendingAction) return;
    setActionLoading(true);
    try {
      const { type, node } = pendingAction;
      switch (type) {
        case 'disconnect':
          await disconnectNode(node.id);
          break;
        case 'connect':
          await connectNode(node.id);
          break;
        case 'decommission':
          await decommissionNode(node.id);
          break;
        case 'remove':
          await removeNode(node.id);
          break;
        case 'primary':
          await designatePrimary(node.id);
          break;
      }
      onToast('success', `${ACTION_CONFIG[type].title}: ${node.address}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      onToast('error', `Action failed: ${msg}`);
    } finally {
      setActionLoading(false);
      setPendingAction(null);
    }
  }, [pendingAction, disconnectNode, connectNode, decommissionNode, removeNode, designatePrimary, onToast]);

  const nodeList = nodes?.nodes ?? [];
  const primaryId = status?.primary_id;
  const coordinatorId = status?.coordinator_id;

  return (
    <>
      <div className="cluster-panel" role="complementary" aria-label="Cluster Management">
        <div className="cluster-panel-header">
          <span className="cluster-panel-title">Cluster Management</span>
          <div className="cluster-header-controls">
            <label className="cluster-auto-refresh">
              <input
                type="checkbox"
                checked={autoRefresh}
                onChange={(e) => setAutoRefresh(e.target.checked)}
              />
              Auto-refresh (5s)
            </label>
            <button className="cluster-refresh-btn" onClick={refresh} title="Refresh now">
              Refresh
            </button>
            <button
              className="config-close-btn"
              onClick={onClose}
              aria-label="Close cluster management"
            >
              &times;
            </button>
          </div>
        </div>

        {/* Cluster Overview */}
        {status && (
          <div className="cluster-overview">
            <div className="cluster-overview-item">
              <span className="cluster-overview-label">Nodes</span>
              <span className="cluster-overview-value">
                {status.connected_count} / {status.total_count}
              </span>
            </div>
            <div className="cluster-overview-item">
              <span className="cluster-overview-label">Quorum</span>
              <span
                className={`cluster-overview-value ${
                  status.has_quorum ? 'cluster-quorum-ok' : 'cluster-quorum-lost'
                }`}
              >
                {status.has_quorum ? 'Yes' : 'No'}
              </span>
            </div>
            <div className="cluster-overview-item">
              <span className="cluster-overview-label">Primary</span>
              <span className="cluster-overview-value">
                {primaryId
                  ? nodeList.find((n) => n.id === primaryId)?.address ?? primaryId
                  : 'None'}
              </span>
            </div>
            <div className="cluster-overview-item">
              <span className="cluster-overview-label">Coordinator</span>
              <span className="cluster-overview-value">
                {coordinatorId
                  ? nodeList.find((n) => n.id === coordinatorId)?.address ?? coordinatorId
                  : 'None'}
              </span>
            </div>
            <div className="cluster-overview-item">
              <span className="cluster-overview-label">Flow Version</span>
              <span className="cluster-overview-value">{status.flow_version}</span>
            </div>
            <div className="cluster-overview-item">
              <span className="cluster-overview-label">Election Term</span>
              <span className="cluster-overview-value">{status.election_term}</span>
            </div>
          </div>
        )}

        {loading && !nodes && (
          <div className="cluster-loading">Loading cluster info...</div>
        )}

        {error && (
          <div className="cluster-error">Failed to load cluster info: {error}</div>
        )}

        {/* Node Table */}
        {nodes && (
          <div className="cluster-table-wrap">
            <table className="cluster-table">
              <thead>
                <tr>
                  <th>Address</th>
                  <th>Status</th>
                  <th>Roles</th>
                  <th>Heartbeat</th>
                  <th>Active Threads</th>
                  <th>Queued</th>
                  <th>Uptime</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {nodeList.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="cluster-empty-row">
                      No cluster nodes found.
                    </td>
                  </tr>
                ) : (
                  nodeList.map((node) => (
                    <tr key={node.id} className="cluster-row">
                      <td className="cluster-cell-addr">{node.address}</td>
                      <td>
                        <span
                          className={`cluster-status-badge ${
                            STATE_BADGE_CLASS[node.state] ?? 'cluster-badge-disconnected'
                          }`}
                        >
                          {node.state}
                        </span>
                      </td>
                      <td>
                        <div className="cluster-roles">
                          {node.roles.map((role) => (
                            <span
                              key={role}
                              className={`cluster-role-badge ${
                                role === 'Primary'
                                  ? 'cluster-role-primary'
                                  : role === 'Coordinator'
                                    ? 'cluster-role-coordinator'
                                    : 'cluster-role-node'
                              }`}
                            >
                              {role}
                            </span>
                          ))}
                        </div>
                      </td>
                      <td className="cluster-cell-dim">
                        {node.missed_heartbeats === 0
                          ? 'OK'
                          : `${node.missed_heartbeats} missed`}
                      </td>
                      <td className="cluster-cell-dim">
                        {node.metrics?.active_threads ?? '-'}
                      </td>
                      <td className="cluster-cell-dim">
                        {node.metrics?.queued_flowfiles != null
                          ? node.metrics.queued_flowfiles.toLocaleString()
                          : '-'}
                      </td>
                      <td className="cluster-cell-dim">
                        {node.uptime_secs != null
                          ? formatUptime(node.uptime_secs)
                          : '-'}
                      </td>
                      <td>
                        <div className="cluster-actions">
                          {node.state === 'Connected' && (
                            <button
                              className="btn-link"
                              onClick={() =>
                                setPendingAction({ type: 'disconnect', node })
                              }
                              disabled={actionLoading}
                            >
                              Disconnect
                            </button>
                          )}
                          {node.state === 'Disconnected' && (
                            <>
                              <button
                                className="btn-link"
                                onClick={() =>
                                  setPendingAction({ type: 'connect', node })
                                }
                                disabled={actionLoading}
                              >
                                Connect
                              </button>
                              <button
                                className="btn-link cluster-action-danger"
                                onClick={() =>
                                  setPendingAction({ type: 'remove', node })
                                }
                                disabled={actionLoading}
                              >
                                Remove
                              </button>
                            </>
                          )}
                          {(node.state === 'Connected' ||
                            node.state === 'Disconnected') && (
                            <button
                              className="btn-link cluster-action-warn"
                              onClick={() =>
                                setPendingAction({ type: 'decommission', node })
                              }
                              disabled={actionLoading}
                            >
                              Decommission
                            </button>
                          )}
                          {node.state === 'Connected' &&
                            !node.roles.includes('Primary') && (
                              <button
                                className="btn-link"
                                onClick={() =>
                                  setPendingAction({ type: 'primary', node })
                                }
                                disabled={actionLoading}
                              >
                                Set Primary
                              </button>
                            )}
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {pendingAction && (
        <ConfirmDialog
          title={ACTION_CONFIG[pendingAction.type].title}
          message={ACTION_CONFIG[pendingAction.type].message(pendingAction.node.address)}
          confirmLabel={ACTION_CONFIG[pendingAction.type].confirmLabel}
          destructive={ACTION_CONFIG[pendingAction.type].destructive}
          onConfirm={executeAction}
          onCancel={() => setPendingAction(null)}
        />
      )}
    </>
  );
}

export const ClusterManagement = memo(ClusterManagementInner);
