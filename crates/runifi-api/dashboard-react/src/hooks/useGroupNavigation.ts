import { useCallback, useEffect, useState } from 'react';
import type { BreadcrumbSegment, ProcessGroupFlowResponse } from '../types/api';

export interface GroupNavigationState {
  /** Current group ID, or null for root canvas. */
  currentGroupId: string | null;
  /** Breadcrumb path from root to current group. */
  breadcrumb: BreadcrumbSegment[];
  /** Navigate into a child process group. */
  enterGroup: (groupId: string) => void;
  /** Navigate up one level (or to root if at top-level group). */
  exitGroup: () => void;
  /** Navigate directly to a specific group (or null for root). */
  navigateTo: (groupId: string | null) => void;
  /** Group flow data for the current group (null when at root). */
  groupFlow: ProcessGroupFlowResponse | null;
  /** Loading state for group flow fetch. */
  groupLoading: boolean;
}

function getGroupIdFromUrl(): string | null {
  const params = new URLSearchParams(window.location.search);
  return params.get('group') || null;
}

function setGroupIdInUrl(groupId: string | null): void {
  const url = new URL(window.location.href);
  if (groupId) {
    url.searchParams.set('group', groupId);
  } else {
    url.searchParams.delete('group');
  }
  window.history.pushState({}, '', url.toString());
}

export function useGroupNavigation(): GroupNavigationState {
  const [currentGroupId, setCurrentGroupId] = useState<string | null>(getGroupIdFromUrl);
  const [breadcrumb, setBreadcrumb] = useState<BreadcrumbSegment[]>([]);
  const [groupFlow, setGroupFlow] = useState<ProcessGroupFlowResponse | null>(null);
  const [groupLoading, setGroupLoading] = useState(false);

  // Fetch group flow when currentGroupId changes.
  useEffect(() => {
    if (!currentGroupId) {
      setBreadcrumb([]);
      setGroupFlow(null);
      return;
    }

    let cancelled = false;
    setGroupLoading(true);

    fetch(`/api/v1/process-groups/${currentGroupId}/flow`)
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json() as Promise<ProcessGroupFlowResponse>;
      })
      .then((data) => {
        if (!cancelled) {
          setGroupFlow(data);
          setBreadcrumb(data.breadcrumb);
          setGroupLoading(false);
        }
      })
      .catch(() => {
        if (!cancelled) {
          // Group not found — navigate back to root.
          setCurrentGroupId(null);
          setGroupIdInUrl(null);
          setGroupLoading(false);
        }
      });

    return () => { cancelled = true; };
  }, [currentGroupId]);

  // Listen for browser back/forward.
  useEffect(() => {
    const handler = () => {
      setCurrentGroupId(getGroupIdFromUrl());
    };
    window.addEventListener('popstate', handler);
    return () => window.removeEventListener('popstate', handler);
  }, []);

  const enterGroup = useCallback((groupId: string) => {
    setCurrentGroupId(groupId);
    setGroupIdInUrl(groupId);
  }, []);

  const exitGroup = useCallback(() => {
    if (breadcrumb.length > 1) {
      // Navigate to parent (second-to-last in breadcrumb).
      const parentId = breadcrumb[breadcrumb.length - 2].id;
      setCurrentGroupId(parentId);
      setGroupIdInUrl(parentId);
    } else {
      // At top-level group — go to root.
      setCurrentGroupId(null);
      setGroupIdInUrl(null);
    }
  }, [breadcrumb]);

  const navigateTo = useCallback((groupId: string | null) => {
    setCurrentGroupId(groupId);
    setGroupIdInUrl(groupId);
  }, []);

  return {
    currentGroupId,
    breadcrumb,
    enterGroup,
    exitGroup,
    navigateTo,
    groupFlow,
    groupLoading,
  };
}
