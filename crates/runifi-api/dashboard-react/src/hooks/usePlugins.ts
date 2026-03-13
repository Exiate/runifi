// Fetches available processor types from GET /api/v1/plugins

import { useState, useEffect } from 'react';
import type { PluginDescriptor, PluginsResponse } from '../types/api';

// Fallback plugins when the backend is not yet ready
const FALLBACK_PLUGINS: PluginDescriptor[] = [
  {
    type_name: 'GenerateFlowFile',
    display_name: 'Generate FlowFile',
    description: 'Generates synthetic FlowFiles for testing and benchmarking.',
    kind: 'source',
    relationships: ['success'],
    properties: [
      {
        name: 'File Size',
        display_name: 'File Size',
        description: 'Size of generated content in bytes.',
        default_value: '5120',
        required: false,
      },
    ],
  },
  {
    type_name: 'LogAttribute',
    display_name: 'Log Attribute',
    description: 'Logs FlowFile attributes to the bulletin board.',
    kind: 'processor',
    relationships: ['success'],
    properties: [],
  },
  {
    type_name: 'RouteOnAttribute',
    display_name: 'Route On Attribute',
    description: 'Routes FlowFiles based on attribute expressions.',
    kind: 'processor',
    relationships: ['matched', 'unmatched'],
    properties: [],
  },
  {
    type_name: 'UpdateAttribute',
    display_name: 'Update Attribute',
    description: 'Adds or updates FlowFile attributes.',
    kind: 'processor',
    relationships: ['success'],
    properties: [],
  },
  {
    type_name: 'GetFile',
    display_name: 'Get File',
    description: 'Reads files from a directory on disk.',
    kind: 'source',
    relationships: ['success'],
    properties: [
      {
        name: 'Input Directory',
        display_name: 'Input Directory',
        description: 'Directory to watch for files.',
        default_value: '/tmp/input',
        required: true,
      },
    ],
  },
  {
    type_name: 'PutFile',
    display_name: 'Put File',
    description: 'Writes FlowFiles to a directory on disk.',
    kind: 'sink',
    relationships: ['success', 'failure'],
    properties: [
      {
        name: 'Output Directory',
        display_name: 'Output Directory',
        description: 'Directory to write files to.',
        default_value: '/tmp/output',
        required: true,
      },
    ],
  },
];

interface UsePluginsResult {
  plugins: PluginDescriptor[];
  loading: boolean;
  error: string | null;
}

export function usePlugins(): UsePluginsResult {
  const [plugins, setPlugins] = useState<PluginDescriptor[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;

    fetch('/api/v1/plugins')
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
      })
      .then((data: PluginDescriptor[] | PluginsResponse) => {
        if (!cancelled) {
          // API may return a plain array or {plugins: [...]}
          const list = Array.isArray(data) ? data : data.plugins;
          setPlugins(list ?? FALLBACK_PLUGINS);
          setLoading(false);
        }
      })
      .catch(() => {
        if (!cancelled) {
          // Backend not ready yet — use fallback list
          setPlugins(FALLBACK_PLUGINS);
          setError('Using offline plugin list (backend unavailable)');
          setLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, []);

  return { plugins, loading, error };
}
