// SSE connection to /api/v1/events — delivers live metrics every 1 second

import { useState, useEffect, useRef } from 'react';
import type { SseMetricsEvent } from '../types/api';

type SseStatus = 'connecting' | 'connected' | 'disconnected';

interface UseSseMetricsResult {
  latest: SseMetricsEvent | null;
  status: SseStatus;
}

export function useSseMetrics(): UseSseMetricsResult {
  const [latest, setLatest] = useState<SseMetricsEvent | null>(null);
  const [status, setStatus] = useState<SseStatus>('connecting');
  const esRef = useRef<EventSource | null>(null);

  useEffect(() => {
    let retryTimeout: ReturnType<typeof setTimeout> | null = null;
    let retryDelay = 1000;
    let destroyed = false;

    function connect() {
      if (destroyed) return;
      setStatus('connecting');

      const es = new EventSource('/api/v1/events');
      esRef.current = es;

      es.addEventListener('metrics', (e: MessageEvent) => {
        retryDelay = 1000;
        setStatus('connected');
        try {
          const payload = JSON.parse(e.data as string) as SseMetricsEvent;
          setLatest(payload);
        } catch {
          // Malformed payload — ignore
        }
      });

      es.onerror = () => {
        es.close();
        esRef.current = null;
        if (!destroyed) {
          setStatus('disconnected');
          retryTimeout = setTimeout(() => {
            retryDelay = Math.min(retryDelay * 2, 30_000);
            connect();
          }, retryDelay);
        }
      };
    }

    connect();

    return () => {
      destroyed = true;
      if (retryTimeout !== null) clearTimeout(retryTimeout);
      esRef.current?.close();
      esRef.current = null;
    };
  }, []);

  return { latest, status };
}
