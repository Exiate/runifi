import { useState, useEffect, useCallback } from 'react';
import type { ServiceResponse, CreateServiceRequest } from '../types/api';

interface UseServicesResult {
  services: ServiceResponse[];
  loading: boolean;
  error: string | null;
  refresh: () => void;
  createService: (req: CreateServiceRequest) => Promise<ServiceResponse>;
  deleteService: (name: string) => Promise<void>;
  enableService: (name: string) => Promise<void>;
  disableService: (name: string) => Promise<void>;
  updateServiceConfig: (name: string, properties: Record<string, string>) => Promise<void>;
}

export function useServices(): UseServicesResult {
  const [services, setServices] = useState<ServiceResponse[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const fetchServices = useCallback(() => {
    setLoading(true);
    fetch('/api/v1/services')
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data: ServiceResponse[]) => {
        setServices(data);
        setError(null);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  useEffect(() => {
    fetchServices();
  }, [fetchServices]);

  const createService = useCallback(async (req: CreateServiceRequest): Promise<ServiceResponse> => {
    const res = await fetch('/api/v1/services', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(req),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(body || `HTTP ${res.status}`);
    }
    const created = await res.json();
    fetchServices();
    return created;
  }, [fetchServices]);

  const deleteService = useCallback(async (name: string): Promise<void> => {
    const res = await fetch(`/api/v1/services/${encodeURIComponent(name)}`, {
      method: 'DELETE',
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(body || `HTTP ${res.status}`);
    }
    fetchServices();
  }, [fetchServices]);

  const enableService = useCallback(async (name: string): Promise<void> => {
    const res = await fetch(`/api/v1/services/${encodeURIComponent(name)}/enable`, {
      method: 'POST',
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(body || `HTTP ${res.status}`);
    }
    fetchServices();
  }, [fetchServices]);

  const disableService = useCallback(async (name: string): Promise<void> => {
    const res = await fetch(`/api/v1/services/${encodeURIComponent(name)}/disable`, {
      method: 'POST',
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(body || `HTTP ${res.status}`);
    }
    fetchServices();
  }, [fetchServices]);

  const updateServiceConfig = useCallback(async (name: string, properties: Record<string, string>): Promise<void> => {
    const res = await fetch(`/api/v1/services/${encodeURIComponent(name)}/config`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ properties }),
    });
    if (!res.ok) {
      const body = await res.text();
      throw new Error(body || `HTTP ${res.status}`);
    }
    fetchServices();
  }, [fetchServices]);

  return {
    services,
    loading,
    error,
    refresh: fetchServices,
    createService,
    deleteService,
    enableService,
    disableService,
    updateServiceConfig,
  };
}
