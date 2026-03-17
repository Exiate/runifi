import { useState, useEffect, useCallback, useRef } from 'react';
import type { SystemResponse } from '../types/api';

interface UseSystemDiagnosticsResult {
  data: SystemResponse | null;
  loading: boolean;
  error: string | null;
  autoRefresh: boolean;
  setAutoRefresh: (enabled: boolean) => void;
  refresh: () => void;
}

export function useSystemDiagnostics(open: boolean): UseSystemDiagnosticsResult {
  const [data, setData] = useState<SystemResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const fetchDiagnostics = useCallback(async () => {
    try {
      const token = localStorage.getItem('runifi-token');
      const headers: Record<string, string> = {};
      if (token) headers['Authorization'] = `Bearer ${token}`;
      const res = await fetch('/api/v1/system', { headers });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json: SystemResponse = await res.json();
      setData(json);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, []);

  // Initial fetch when opened
  useEffect(() => {
    if (open) {
      setLoading(true);
      fetchDiagnostics();
    } else {
      setData(null);
      setError(null);
    }
  }, [open]); // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-refresh polling
  useEffect(() => {
    if (intervalRef.current) {
      clearInterval(intervalRef.current);
      intervalRef.current = null;
    }
    if (open && autoRefresh) {
      intervalRef.current = setInterval(fetchDiagnostics, 5000);
    }
    return () => {
      if (intervalRef.current) {
        clearInterval(intervalRef.current);
        intervalRef.current = null;
      }
    };
  }, [open, autoRefresh, fetchDiagnostics]);

  return { data, loading, error, autoRefresh, setAutoRefresh, refresh: fetchDiagnostics };
}
