import { memo, useState, useEffect, useCallback } from 'react';
import type { UserResponse, UserGroupResponse } from '../types/api';
import type { ToastKind } from '../hooks/useToast';
import { formatTimestamp } from '../utils/format';

type Tab = 'users' | 'groups';
type SortField = string;
type SortDir = 'asc' | 'desc';

interface UsersGroupsPanelProps {
  onClose: () => void;
  onToast: (kind: ToastKind, message: string) => void;
}

type DialogMode =
  | { kind: 'create-user' }
  | { kind: 'edit-user'; user: UserResponse }
  | { kind: 'delete-user'; user: UserResponse }
  | { kind: 'create-group' }
  | { kind: 'edit-group'; group: UserGroupResponse }
  | { kind: 'delete-group'; group: UserGroupResponse };

function UsersGroupsPanelInner({ onClose, onToast }: UsersGroupsPanelProps) {
  const [tab, setTab] = useState<Tab>('users');
  const [users, setUsers] = useState<UserResponse[]>([]);
  const [groups, setGroups] = useState<UserGroupResponse[]>([]);
  const [loadingUsers, setLoadingUsers] = useState(false);
  const [loadingGroups, setLoadingGroups] = useState(false);
  const [dialog, setDialog] = useState<DialogMode | null>(null);

  // Sort state
  const [userSort, setUserSort] = useState<SortField>('username');
  const [userSortDir, setUserSortDir] = useState<SortDir>('asc');
  const [groupSort, setGroupSort] = useState<SortField>('name');
  const [groupSortDir, setGroupSortDir] = useState<SortDir>('asc');

  // Dialog form state
  const [formUsername, setFormUsername] = useState('');
  const [formPassword, setFormPassword] = useState('');
  const [formEnabled, setFormEnabled] = useState(true);
  const [formGroupName, setFormGroupName] = useState('');
  const [formMembers, setFormMembers] = useState<string[]>([]);
  const [submitting, setSubmitting] = useState(false);

  const fetchUsers = useCallback(() => {
    setLoadingUsers(true);
    fetch('/api/v1/tenants/users')
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json() as Promise<UserResponse[]>;
      })
      .then((data) => setUsers(data))
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        onToast('error', `Failed to load users: ${msg}`);
      })
      .finally(() => setLoadingUsers(false));
  }, [onToast]);

  const fetchGroups = useCallback(() => {
    setLoadingGroups(true);
    fetch('/api/v1/tenants/user-groups')
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json() as Promise<UserGroupResponse[]>;
      })
      .then((data) => setGroups(data))
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        onToast('error', `Failed to load groups: ${msg}`);
      })
      .finally(() => setLoadingGroups(false));
  }, [onToast]);

  useEffect(() => {
    fetchUsers();
    fetchGroups();
  }, [fetchUsers, fetchGroups]);

  // Close on Escape (only when no dialog open)
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        if (dialog) {
          setDialog(null);
        } else {
          onClose();
        }
      }
    };
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, [onClose, dialog]);

  // ── Sorting helpers ────────────────────────────────────────

  function toggleUserSort(field: SortField) {
    if (userSort === field) {
      setUserSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setUserSort(field);
      setUserSortDir('asc');
    }
  }

  function toggleGroupSort(field: SortField) {
    if (groupSort === field) {
      setGroupSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    } else {
      setGroupSort(field);
      setGroupSortDir('asc');
    }
  }

  function sortIndicator(active: boolean, dir: SortDir): string {
    if (!active) return '';
    return dir === 'asc' ? ' \u25B2' : ' \u25BC';
  }

  const sortedUsers = [...users].sort((a, b) => {
    const dir = userSortDir === 'asc' ? 1 : -1;
    switch (userSort) {
      case 'username':
        return dir * a.username.localeCompare(b.username);
      case 'enabled':
        return dir * (Number(a.enabled) - Number(b.enabled));
      case 'created_at':
        return dir * a.created_at.localeCompare(b.created_at);
      case 'updated_at':
        return dir * a.updated_at.localeCompare(b.updated_at);
      default:
        return 0;
    }
  });

  const sortedGroups = [...groups].sort((a, b) => {
    const dir = groupSortDir === 'asc' ? 1 : -1;
    switch (groupSort) {
      case 'name':
        return dir * a.name.localeCompare(b.name);
      case 'members':
        return dir * (a.members.length - b.members.length);
      case 'created_at':
        return dir * a.created_at.localeCompare(b.created_at);
      case 'updated_at':
        return dir * a.updated_at.localeCompare(b.updated_at);
      default:
        return 0;
    }
  });

  // ── Dialog openers ────────────────────────────────────────

  function openCreateUser() {
    setFormUsername('');
    setFormPassword('');
    setDialog({ kind: 'create-user' });
  }

  function openEditUser(user: UserResponse) {
    setFormUsername(user.username);
    setFormPassword('');
    setFormEnabled(user.enabled);
    setDialog({ kind: 'edit-user', user });
  }

  function openDeleteUser(user: UserResponse) {
    setDialog({ kind: 'delete-user', user });
  }

  function openCreateGroup() {
    setFormGroupName('');
    setFormMembers([]);
    setDialog({ kind: 'create-group' });
  }

  function openEditGroup(group: UserGroupResponse) {
    setFormGroupName(group.name);
    setFormMembers([...group.members]);
    setDialog({ kind: 'edit-group', group });
  }

  function openDeleteGroup(group: UserGroupResponse) {
    setDialog({ kind: 'delete-group', group });
  }

  // ── CRUD operations ────────────────────────────────────────

  function handleCreateUser() {
    if (!formUsername.trim() || !formPassword.trim()) {
      onToast('error', 'Username and password are required');
      return;
    }
    setSubmitting(true);
    fetch('/api/v1/tenants/users', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username: formUsername.trim(), password: formPassword }),
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        onToast('success', `User "${formUsername.trim()}" created`);
        setDialog(null);
        fetchUsers();
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        onToast('error', `Failed to create user: ${msg}`);
      })
      .finally(() => setSubmitting(false));
  }

  function handleEditUser() {
    if (!dialog || dialog.kind !== 'edit-user') return;
    const body: Record<string, unknown> = {};
    if (formUsername.trim() && formUsername.trim() !== dialog.user.username) {
      body.username = formUsername.trim();
    }
    if (formEnabled !== dialog.user.enabled) {
      body.enabled = formEnabled;
    }
    if (formPassword) {
      body.password = formPassword;
    }
    if (Object.keys(body).length === 0) {
      setDialog(null);
      return;
    }
    setSubmitting(true);
    fetch(`/api/v1/tenants/users/${dialog.user.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        onToast('success', `User "${dialog.user.username}" updated`);
        setDialog(null);
        fetchUsers();
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        onToast('error', `Failed to update user: ${msg}`);
      })
      .finally(() => setSubmitting(false));
  }

  function handleDeleteUser() {
    if (!dialog || dialog.kind !== 'delete-user') return;
    setSubmitting(true);
    fetch(`/api/v1/tenants/users/${dialog.user.id}`, { method: 'DELETE' })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        onToast('success', `User "${dialog.user.username}" deleted`);
        setDialog(null);
        fetchUsers();
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        onToast('error', `Failed to delete user: ${msg}`);
      })
      .finally(() => setSubmitting(false));
  }

  function handleCreateGroup() {
    if (!formGroupName.trim()) {
      onToast('error', 'Group name is required');
      return;
    }
    setSubmitting(true);
    const body: Record<string, unknown> = { name: formGroupName.trim() };
    if (formMembers.length > 0) body.members = formMembers;
    fetch('/api/v1/tenants/user-groups', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        onToast('success', `Group "${formGroupName.trim()}" created`);
        setDialog(null);
        fetchGroups();
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        onToast('error', `Failed to create group: ${msg}`);
      })
      .finally(() => setSubmitting(false));
  }

  function handleEditGroup() {
    if (!dialog || dialog.kind !== 'edit-group') return;
    const body: Record<string, unknown> = {};
    if (formGroupName.trim() && formGroupName.trim() !== dialog.group.name) {
      body.name = formGroupName.trim();
    }
    // Always send members to allow add/remove
    body.members = formMembers;
    setSubmitting(true);
    fetch(`/api/v1/tenants/user-groups/${dialog.group.id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        onToast('success', `Group "${dialog.group.name}" updated`);
        setDialog(null);
        fetchGroups();
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        onToast('error', `Failed to update group: ${msg}`);
      })
      .finally(() => setSubmitting(false));
  }

  function handleDeleteGroup() {
    if (!dialog || dialog.kind !== 'delete-group') return;
    setSubmitting(true);
    fetch(`/api/v1/tenants/user-groups/${dialog.group.id}`, { method: 'DELETE' })
      .then((res) => {
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        onToast('success', `Group "${dialog.group.name}" deleted`);
        setDialog(null);
        fetchGroups();
      })
      .catch((err: unknown) => {
        const msg = err instanceof Error ? err.message : String(err);
        onToast('error', `Failed to delete group: ${msg}`);
      })
      .finally(() => setSubmitting(false));
  }

  function toggleMember(userId: string) {
    setFormMembers((prev) =>
      prev.includes(userId) ? prev.filter((id) => id !== userId) : [...prev, userId],
    );
  }

  function usernameForId(userId: string): string {
    const user = users.find((u) => u.id === userId);
    return user ? user.username : userId;
  }

  function formatDate(iso: string): string {
    const ms = new Date(iso).getTime();
    if (isNaN(ms)) return iso;
    return formatTimestamp(ms);
  }

  // ── Render ─────────────────────────────────────────────────

  const isLoading = tab === 'users' ? loadingUsers : loadingGroups;

  return (
    <>
      <div className="users-groups-panel" role="complementary" aria-label="Users and Groups Management">
        <div className="users-groups-panel-header">
          <span className="users-groups-panel-title">Users &amp; Groups</span>
          <button
            className="provenance-refresh-btn"
            onClick={() => { fetchUsers(); fetchGroups(); }}
            title="Refresh"
            aria-label="Refresh users and groups"
          >
            Refresh
          </button>
          <button
            className="config-close-btn"
            onClick={onClose}
            aria-label="Close users and groups panel"
          >
            &times;
          </button>
        </div>

        <div className="users-groups-tabs">
          <button
            className={`users-groups-tab${tab === 'users' ? ' active' : ''}`}
            onClick={() => setTab('users')}
            aria-label="Users tab"
          >
            Users ({users.length})
          </button>
          <button
            className={`users-groups-tab${tab === 'groups' ? ' active' : ''}`}
            onClick={() => setTab('groups')}
            aria-label="Groups tab"
          >
            Groups ({groups.length})
          </button>
        </div>

        <div className="users-groups-toolbar">
          {tab === 'users' ? (
            <button className="btn btn-primary" onClick={openCreateUser} style={{ fontSize: '0.78rem', padding: '0.3rem 0.8rem' }}>
              Create User
            </button>
          ) : (
            <button className="btn btn-primary" onClick={openCreateGroup} style={{ fontSize: '0.78rem', padding: '0.3rem 0.8rem' }}>
              Create Group
            </button>
          )}
        </div>

        <div className="users-groups-table-wrap">
          {tab === 'users' ? (
            <table className="users-groups-table">
              <thead>
                <tr>
                  <th onClick={() => toggleUserSort('username')} style={{ cursor: 'pointer' }}>
                    Username{sortIndicator(userSort === 'username', userSortDir)}
                  </th>
                  <th onClick={() => toggleUserSort('enabled')} style={{ cursor: 'pointer' }}>
                    Status{sortIndicator(userSort === 'enabled', userSortDir)}
                  </th>
                  <th onClick={() => toggleUserSort('created_at')} style={{ cursor: 'pointer' }}>
                    Created{sortIndicator(userSort === 'created_at', userSortDir)}
                  </th>
                  <th onClick={() => toggleUserSort('updated_at')} style={{ cursor: 'pointer' }}>
                    Updated{sortIndicator(userSort === 'updated_at', userSortDir)}
                  </th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {sortedUsers.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="users-groups-empty-row">
                      {isLoading ? 'Loading...' : 'No users found.'}
                    </td>
                  </tr>
                ) : (
                  sortedUsers.map((user) => (
                    <tr key={user.id}>
                      <td>{user.username}</td>
                      <td>
                        <span className={`users-groups-status-badge ${user.enabled ? 'enabled' : 'disabled'}`}>
                          {user.enabled ? 'Enabled' : 'Disabled'}
                        </span>
                      </td>
                      <td className="provenance-cell-dim">{formatDate(user.created_at)}</td>
                      <td className="provenance-cell-dim">{formatDate(user.updated_at)}</td>
                      <td>
                        <div className="users-groups-actions">
                          <button className="btn-link" onClick={() => openEditUser(user)}>Edit</button>
                          <button className="btn-link btn-link-danger" onClick={() => openDeleteUser(user)}>Delete</button>
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          ) : (
            <table className="users-groups-table">
              <thead>
                <tr>
                  <th onClick={() => toggleGroupSort('name')} style={{ cursor: 'pointer' }}>
                    Name{sortIndicator(groupSort === 'name', groupSortDir)}
                  </th>
                  <th onClick={() => toggleGroupSort('members')} style={{ cursor: 'pointer' }}>
                    Members{sortIndicator(groupSort === 'members', groupSortDir)}
                  </th>
                  <th onClick={() => toggleGroupSort('created_at')} style={{ cursor: 'pointer' }}>
                    Created{sortIndicator(groupSort === 'created_at', groupSortDir)}
                  </th>
                  <th onClick={() => toggleGroupSort('updated_at')} style={{ cursor: 'pointer' }}>
                    Updated{sortIndicator(groupSort === 'updated_at', groupSortDir)}
                  </th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {sortedGroups.length === 0 ? (
                  <tr>
                    <td colSpan={5} className="users-groups-empty-row">
                      {isLoading ? 'Loading...' : 'No groups found.'}
                    </td>
                  </tr>
                ) : (
                  sortedGroups.map((group) => (
                    <tr key={group.id}>
                      <td>{group.name}</td>
                      <td>
                        <span className="users-groups-member-count">{group.members.length}</span>
                      </td>
                      <td className="provenance-cell-dim">{formatDate(group.created_at)}</td>
                      <td className="provenance-cell-dim">{formatDate(group.updated_at)}</td>
                      <td>
                        <div className="users-groups-actions">
                          <button className="btn-link" onClick={() => openEditGroup(group)}>Edit</button>
                          <button className="btn-link btn-link-danger" onClick={() => openDeleteGroup(group)}>Delete</button>
                        </div>
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          )}
        </div>
      </div>

      {/* ── Dialogs ──────────────────────────────────────────── */}

      {dialog && dialog.kind === 'create-user' && (
        <div className="users-groups-dialog-overlay" onClick={() => setDialog(null)}>
          <div className="users-groups-dialog" onClick={(e) => e.stopPropagation()}>
            <div className="users-groups-dialog-header">
              <h3>Create User</h3>
              <button className="modal-close" onClick={() => setDialog(null)} aria-label="Close">&times;</button>
            </div>
            <div className="users-groups-dialog-body">
              <div className="users-groups-form-field">
                <label className="users-groups-form-label">Username</label>
                <input
                  className="users-groups-form-input"
                  type="text"
                  value={formUsername}
                  onChange={(e) => setFormUsername(e.target.value)}
                  placeholder="Enter username"
                  autoFocus
                />
              </div>
              <div className="users-groups-form-field">
                <label className="users-groups-form-label">Password</label>
                <input
                  className="users-groups-form-input"
                  type="password"
                  value={formPassword}
                  onChange={(e) => setFormPassword(e.target.value)}
                  placeholder="Enter password"
                />
              </div>
            </div>
            <div className="users-groups-dialog-footer">
              <button className="btn btn-ghost" onClick={() => setDialog(null)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleCreateUser} disabled={submitting}>
                {submitting ? 'Creating...' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {dialog && dialog.kind === 'edit-user' && (
        <div className="users-groups-dialog-overlay" onClick={() => setDialog(null)}>
          <div className="users-groups-dialog" onClick={(e) => e.stopPropagation()}>
            <div className="users-groups-dialog-header">
              <h3>Edit User</h3>
              <button className="modal-close" onClick={() => setDialog(null)} aria-label="Close">&times;</button>
            </div>
            <div className="users-groups-dialog-body">
              <div className="users-groups-form-field">
                <label className="users-groups-form-label">Username</label>
                <input
                  className="users-groups-form-input"
                  type="text"
                  value={formUsername}
                  onChange={(e) => setFormUsername(e.target.value)}
                  autoFocus
                />
              </div>
              <div className="users-groups-form-field">
                <label className="users-groups-form-label">Enabled</label>
                <label className="users-groups-checkbox-label">
                  <input
                    type="checkbox"
                    checked={formEnabled}
                    onChange={(e) => setFormEnabled(e.target.checked)}
                  />
                  <span>Account enabled</span>
                </label>
              </div>
              <div className="users-groups-form-field">
                <label className="users-groups-form-label">New Password (optional)</label>
                <input
                  className="users-groups-form-input"
                  type="password"
                  value={formPassword}
                  onChange={(e) => setFormPassword(e.target.value)}
                  placeholder="Leave blank to keep current"
                />
              </div>
            </div>
            <div className="users-groups-dialog-footer">
              <button className="btn btn-ghost" onClick={() => setDialog(null)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleEditUser} disabled={submitting}>
                {submitting ? 'Saving...' : 'Save'}
              </button>
            </div>
          </div>
        </div>
      )}

      {dialog && dialog.kind === 'delete-user' && (
        <div className="users-groups-dialog-overlay" onClick={() => setDialog(null)}>
          <div className="users-groups-dialog" onClick={(e) => e.stopPropagation()}>
            <div className="users-groups-dialog-header">
              <h3>Delete User</h3>
              <button className="modal-close" onClick={() => setDialog(null)} aria-label="Close">&times;</button>
            </div>
            <div className="users-groups-dialog-body">
              <p>Are you sure you want to delete user <strong>{dialog.user.username}</strong>? This action cannot be undone.</p>
            </div>
            <div className="users-groups-dialog-footer">
              <button className="btn btn-ghost" onClick={() => setDialog(null)}>Cancel</button>
              <button className="btn btn-danger" onClick={handleDeleteUser} disabled={submitting}>
                {submitting ? 'Deleting...' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}

      {dialog && dialog.kind === 'create-group' && (
        <div className="users-groups-dialog-overlay" onClick={() => setDialog(null)}>
          <div className="users-groups-dialog" onClick={(e) => e.stopPropagation()}>
            <div className="users-groups-dialog-header">
              <h3>Create Group</h3>
              <button className="modal-close" onClick={() => setDialog(null)} aria-label="Close">&times;</button>
            </div>
            <div className="users-groups-dialog-body">
              <div className="users-groups-form-field">
                <label className="users-groups-form-label">Group Name</label>
                <input
                  className="users-groups-form-input"
                  type="text"
                  value={formGroupName}
                  onChange={(e) => setFormGroupName(e.target.value)}
                  placeholder="Enter group name"
                  autoFocus
                />
              </div>
              {users.length > 0 && (
                <div className="users-groups-form-field">
                  <label className="users-groups-form-label">Members ({formMembers.length} selected)</label>
                  <div className="users-groups-checkbox-list">
                    {users.map((user) => (
                      <label key={user.id} className="users-groups-checkbox-item">
                        <input
                          type="checkbox"
                          checked={formMembers.includes(user.id)}
                          onChange={() => toggleMember(user.id)}
                        />
                        <span>{user.username}</span>
                      </label>
                    ))}
                  </div>
                </div>
              )}
            </div>
            <div className="users-groups-dialog-footer">
              <button className="btn btn-ghost" onClick={() => setDialog(null)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleCreateGroup} disabled={submitting}>
                {submitting ? 'Creating...' : 'Create'}
              </button>
            </div>
          </div>
        </div>
      )}

      {dialog && dialog.kind === 'edit-group' && (
        <div className="users-groups-dialog-overlay" onClick={() => setDialog(null)}>
          <div className="users-groups-dialog" onClick={(e) => e.stopPropagation()}>
            <div className="users-groups-dialog-header">
              <h3>Edit Group</h3>
              <button className="modal-close" onClick={() => setDialog(null)} aria-label="Close">&times;</button>
            </div>
            <div className="users-groups-dialog-body">
              <div className="users-groups-form-field">
                <label className="users-groups-form-label">Group Name</label>
                <input
                  className="users-groups-form-input"
                  type="text"
                  value={formGroupName}
                  onChange={(e) => setFormGroupName(e.target.value)}
                  autoFocus
                />
              </div>
              <div className="users-groups-form-field">
                <label className="users-groups-form-label">Members ({formMembers.length} selected)</label>
                <div className="users-groups-checkbox-list">
                  {users.map((user) => (
                    <label key={user.id} className="users-groups-checkbox-item">
                      <input
                        type="checkbox"
                        checked={formMembers.includes(user.id)}
                        onChange={() => toggleMember(user.id)}
                      />
                      <span>{user.username}</span>
                    </label>
                  ))}
                </div>
              </div>
              {formMembers.length > 0 && (
                <div className="users-groups-form-field">
                  <label className="users-groups-form-label">Current Members</label>
                  <div className="users-groups-member-list">
                    {formMembers.map((id) => (
                      <span key={id} className="users-groups-member-tag">{usernameForId(id)}</span>
                    ))}
                  </div>
                </div>
              )}
            </div>
            <div className="users-groups-dialog-footer">
              <button className="btn btn-ghost" onClick={() => setDialog(null)}>Cancel</button>
              <button className="btn btn-primary" onClick={handleEditGroup} disabled={submitting}>
                {submitting ? 'Saving...' : 'Save'}
              </button>
            </div>
          </div>
        </div>
      )}

      {dialog && dialog.kind === 'delete-group' && (
        <div className="users-groups-dialog-overlay" onClick={() => setDialog(null)}>
          <div className="users-groups-dialog" onClick={(e) => e.stopPropagation()}>
            <div className="users-groups-dialog-header">
              <h3>Delete Group</h3>
              <button className="modal-close" onClick={() => setDialog(null)} aria-label="Close">&times;</button>
            </div>
            <div className="users-groups-dialog-body">
              <p>Are you sure you want to delete group <strong>{dialog.group.name}</strong>? This action cannot be undone.</p>
            </div>
            <div className="users-groups-dialog-footer">
              <button className="btn btn-ghost" onClick={() => setDialog(null)}>Cancel</button>
              <button className="btn btn-danger" onClick={handleDeleteGroup} disabled={submitting}>
                {submitting ? 'Deleting...' : 'Delete'}
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}

export const UsersGroupsPanel = memo(UsersGroupsPanelInner);
