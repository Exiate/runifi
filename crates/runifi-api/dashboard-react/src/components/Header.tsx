import { memo, useState, useRef, useEffect } from 'react';
import { formatUptime } from '../utils/format';
import type { SseStatus } from '../types/api';

interface HeaderProps {
  flowName: string;
  uptimeSecs: number;
  sseStatus: SseStatus;
  onOpenControllerServices?: () => void;
}

function HeaderInner({ flowName, uptimeSecs, sseStatus, onOpenControllerServices }: HeaderProps) {
  const [menuOpen, setMenuOpen] = useState(false);
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
        <div className="header-settings-wrap" ref={menuRef}>
          <button
            className="header-settings-btn"
            onClick={() => setMenuOpen(!menuOpen)}
            aria-label="Settings"
            aria-haspopup="true"
            aria-expanded={menuOpen}
            title="Global Settings"
          >
            <svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true">
              <path d="M8 4.754a3.246 3.246 0 100 6.492 3.246 3.246 0 000-6.492zM5.754 8a2.246 2.246 0 114.492 0 2.246 2.246 0 01-4.492 0z"/>
              <path d="M9.796 1.343c-.527-1.79-3.065-1.79-3.592 0l-.094.319a.873.873 0 01-1.255.52l-.292-.16c-1.64-.892-3.433.902-2.54 2.541l.159.292a.873.873 0 01-.52 1.255l-.319.094c-1.79.527-1.79 3.065 0 3.592l.319.094a.873.873 0 01.52 1.255l-.16.292c-.892 1.64.902 3.434 2.541 2.54l.292-.159a.873.873 0 011.255.52l.094.319c.527 1.79 3.065 1.79 3.592 0l.094-.319a.873.873 0 011.255-.52l.292.16c1.64.892 3.434-.902 2.54-2.541l-.159-.292a.873.873 0 01.52-1.255l.319-.094c1.79-.527 1.79-3.065 0-3.592l-.319-.094a.873.873 0 01-.52-1.255l.16-.292c.892-1.64-.902-3.433-2.541-2.54l-.292.159a.873.873 0 01-1.255-.52l-.094-.319zm-2.633.283c.246-.835 1.428-.835 1.674 0l.094.319a1.873 1.873 0 002.693 1.115l.291-.16c.764-.415 1.6.42 1.184 1.185l-.159.292a1.873 1.873 0 001.116 2.692l.318.094c.835.246.835 1.428 0 1.674l-.319.094a1.873 1.873 0 00-1.115 2.693l.16.291c.415.764-.42 1.6-1.185 1.184l-.291-.159a1.873 1.873 0 00-2.693 1.116l-.094.318c-.246.835-1.428.835-1.674 0l-.094-.319a1.873 1.873 0 00-2.692-1.115l-.292.16c-.764.415-1.6-.42-1.184-1.185l.159-.291a1.873 1.873 0 00-1.116-2.693l-.318-.094c-.835-.246-.835-1.428 0-1.674l.319-.094a1.873 1.873 0 001.115-2.692l-.16-.292c-.415-.764.42-1.6 1.185-1.184l.292.159a1.873 1.873 0 002.692-1.116l.094-.318z"/>
            </svg>
          </button>
          {menuOpen && (
            <div className="header-settings-menu">
              <button
                className="header-menu-item"
                onClick={() => {
                  onOpenControllerServices?.();
                  setMenuOpen(false);
                }}
              >
                Controller Services
              </button>
            </div>
          )}
        </div>
      </div>
    </header>
  );
}

export const Header = memo(HeaderInner);
