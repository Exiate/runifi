import { useState, useEffect, useCallback, useRef } from 'react';
import type {
  ClusterNodesResponse,
  ClusterStatusResponse,
} from '../types/api';

interface UseClusterResult {
  nodes: ClusterNodesResponse | null;
  status: ClusterStatusResponse | null;
  loading: boolean;
  error: string | null;
  autoRefresh: boolean;
  setAutoRefresh: (enabled: boolean) => void;
  refresh: () => void;
  disconnectNode: (id: string) => Promise<void>;
  connectNode: (id: string) => Promise<void>;
  decommissionNode: (id: string) => Promise<void>;
  removeNode: (id: string) => Promise<void>;
  designatePrimary: (id: string) => Promise<void>;
}

function authHeaders(): Record<string, string> {
  const token = localStorage.getItem('runifi-token');
  const headers: Record<string, string> = {};
  if (token) headers['Authorization'] = `Bearer ${token}`;
  return headers;
}

export function useCluster(open: boolean): UseClusterResult {
  const [nodes, setNodes] = useState<ClusterNodesResponse | null>(null);
  const [status, setStatus] = useState<ClusterStatusResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchCluster = useCallback(async () => {
    try {
      const headers = authHeaders();
      const [nodesRes, statusRes] = await Promise.all([
        fetch('/api/v1/cluster/nodes', { headers }),
        fetch('/api/v1/cluster/status', { headers }),
      ]);

      if (!nodesRes.ok) throw new Error(`Nodes: HTTP ${nodesRes.status}`);
      if (!statusRes.ok) throw new Error(`Status: HTTP ${statusRes.status}`);

      const nodesData: ClusterNodesResponse = await nodesRes.json();
      const statusData: ClusterStatusResponse = await statusRes.json();

      setNodes(nodesData);
      setStatus(statusData);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (open) {
      setLoading(true);
      fetchCluster();
    } else {
      setNodes(null);
      setStatus(null);
      setError(null);
    }
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
    if (open && autoRefresh) {
      intervalRef.current = setInterval(fetchCluster, 5000);
    }
    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [open, autoRefresh, fetchCluster]);

  const nodeAction = useCallback(
    async (id: string, path: string, method: string) => {
      const headers = authHeaders();
      const res = await fetch(`/api/v1/cluster/nodes/${id}${path}`, {
        method,
        headers,
      });
      if (!res.ok) {
        const body = await res.text();
        throw new Error(body || `HTTP ${res.status}`);
      }
      await fetchCluster();
    },
    [fetchCluster],
  );

  const disconnectNode = useCallback(
    (id: string) => nodeAction(id, '/disconnect', 'POST'),
    [nodeAction],
  );

  const connectNode = useCallback(
    (id: string) => nodeAction(id, '/connect', 'POST'),
    [nodeAction],
  );

  const decommissionNode = useCallback(
    (id: string) => nodeAction(id, '/decommission', 'POST'),
    [nodeAction],
  );

  const removeNode = useCallback(
    (id: string) => nodeAction(id, '', 'DELETE'),
    [nodeAction],
  );

  const designatePrimary = useCallback(
    (id: string) => nodeAction(id, '/primary', 'POST'),
    [nodeAction],
  );

  return {
    nodes,
    status,
    loading,
    error,
    autoRefresh,
    setAutoRefresh,
    refresh: fetchCluster,
    disconnectNode,
    connectNode,
    decommissionNode,
    removeNode,
    designatePrimary,
  };
}
