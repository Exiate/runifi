import { memo, useState, useRef, useEffect } from 'react';
import { formatUptime } from '../utils/format';
import type { SseStatus } from '../types/api';

interface HeaderProps {
  flowName: string;
  uptimeSecs: number;
  sseStatus: SseStatus;
  onOpenControllerServices?: () => void;
  onOpenBulletins?: () => void;
}

interface MenuItem {
  label: string;
  action: string;
  icon: string;
  adminOnly?: boolean;
  disabled?: boolean;
  disabledReason?: string;
  separator?: boolean;
}

const MENU_ITEMS: MenuItem[] = [
  { label: 'Bulletin Board', action: 'bulletin-board', icon: '\uD83D\uDCCB' },
  { label: 'Controller Services', action: 'controller-services', icon: '\u2699' },
  { label: 'Data Provenance', action: 'data-provenance', icon: '\uD83D\uDD0D', disabled: true, disabledReason: 'Not yet implemented', separator: true },
  { label: 'Flow Configuration History', action: 'flow-history', icon: '\uD83D\uDCC4', disabled: true, disabledReason: 'Not yet implemented' },
  { label: 'System Diagnostics', action: 'system-diagnostics', icon: '\uD83D\uDCCA', disabled: true, disabledReason: 'Not yet implemented', separator: true },
  { label: 'Users & Groups', action: 'users', icon: '\uD83D\uDC65', adminOnly: true, disabled: true, disabledReason: 'Not yet implemented', separator: true },
  { label: 'Help', action: 'help', icon: '?' },
  { label: 'About RuniFi', action: 'about', icon: '\u24D8', separator: true },
  { label: 'Logout', action: 'logout', icon: '\u2192' },
];

function HeaderInner({ flowName, uptimeSecs, sseStatus, onOpenControllerServices, onOpenBulletins }: HeaderProps) {
  const [menuOpen, setMenuOpen] = useState(false);
  const [aboutOpen, setAboutOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  const statusLabel =
    sseStatus === 'connected'
      ? 'Live'
      : sseStatus === 'connecting'
        ? 'Connecting...'
        : 'Disconnected';

  // Close menu on click outside or Escape
  useEffect(() => {
    if (!menuOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setMenuOpen(false);
      }
    };
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') setMenuOpen(false);
    };
    window.addEventListener('mousedown', handleClick);
    window.addEventListener('keydown', handleKey);
    return () => {
      window.removeEventListener('mousedown', handleClick);
      window.removeEventListener('keydown', handleKey);
    };
  }, [menuOpen]);

  const handleMenuAction = (action: string) => {
    setMenuOpen(false);
    switch (action) {
      case 'bulletin-board':
        onOpenBulletins?.();
        break;
      case 'controller-services':
        onOpenControllerServices?.();
        break;
      case 'about':
        setAboutOpen(true);
        break;
      case 'help':
        window.open('https://github.com/Exiate/runifi', '_blank', 'noopener');
        break;
      case 'logout':
        fetch('/api/v1/auth/logout', { method: 'POST' })
          .catch(() => { /* best-effort */ });
        localStorage.removeItem('runifi-token');
        window.location.reload();
        break;
    }
  };

  return (
    <header className="app-header" role="banner">
      <div className="header-left">
        <h1>RuniFi</h1>
      </div>
      <div className="header-right">
        {flowName && <span className="flow-name">{flowName}</span>}
        {flowName && <span className="separator">|</span>}
        <span className="uptime">{formatUptime(uptimeSecs)}</span>
        <span className="separator">|</span>
        <span className={`status ${sseStatus}`} aria-live="polite">
          {statusLabel}
        </span>
        <span className="separator">|</span>
        <div className="header-menu-wrap" ref={menuRef}>
          <button
            className="header-hamburger-btn"
            onClick={() => setMenuOpen(!menuOpen)}
            aria-label="Global menu"
            aria-haspopup="true"
            aria-expanded={menuOpen}
            title="Global Menu"
          >
            <svg width="18" height="18" viewBox="0 0 18 18" fill="currentColor" aria-hidden="true">
              <rect x="2" y="3" width="14" height="2" rx="1" />
              <rect x="2" y="8" width="14" height="2" rx="1" />
              <rect x="2" y="13" width="14" height="2" rx="1" />
            </svg>
          </button>
          {menuOpen && (
            <div className="header-global-menu" role="menu" aria-label="Global navigation">
              <div className="global-menu-header">Navigation</div>
              {MENU_ITEMS.map((item) => (
                <div key={item.action}>
                  {item.separator && <div className="header-menu-separator" aria-hidden="true" />}
                  <button
                    className={`global-menu-item${item.disabled ? ' global-menu-item-disabled' : ''}`}
                    onClick={() => !item.disabled && handleMenuAction(item.action)}
                    disabled={item.disabled}
                    role="menuitem"
                    title={item.disabled ? item.disabledReason : item.label}
                  >
                    <span className="global-menu-icon" aria-hidden="true">{item.icon}</span>
                    <span className="global-menu-label">{item.label}</span>
                    {item.adminOnly && <span className="global-menu-badge">Admin</span>}
                  </button>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {aboutOpen && (
        <div className="modal-overlay" onClick={() => setAboutOpen(false)}>
          <div className="modal-content about-modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>About RuniFi</h3>
              <button className="modal-close" onClick={() => setAboutOpen(false)} aria-label="Close">&times;</button>
            </div>
            <div className="about-body">
              <p className="about-tagline">High-Performance Data Flow Engine</p>
              <p className="about-desc">A Rust reimplementation of Apache NiFi, purpose-built for ultra-low-latency and high-throughput file transfers.</p>
              <div className="about-details">
                <span className="about-detail"><strong>Flow:</strong> {flowName || 'N/A'}</span>
                <span className="about-detail"><strong>Uptime:</strong> {formatUptime(uptimeSecs)}</span>
                <span className="about-detail"><strong>Status:</strong> {statusLabel}</span>
              </div>
            </div>
          </div>
        </div>
      )}
    </header>
  );
}

export const Header = memo(HeaderInner);
