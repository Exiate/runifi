// Fetches the flow topology once on mount (processors + connections DAG)

import { useState, useEffect } from 'react';
import type { FlowResponse } from '../types/api';

interface UseFlowTopologyResult {
  topology: FlowResponse | null;
  error: string | null;
  loading: boolean;
}

export function useFlowTopology(): UseFlowTopologyResult {
  const [topology, setTopology] = useState<FlowResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;

    fetch('/api/v1/flow')
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json() as Promise<FlowResponse>;
      })
      .then((data) => {
        if (!cancelled) {
          setTopology(data);
          setLoading(false);
        }
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : String(err));
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  return { topology, error, loading };
}
