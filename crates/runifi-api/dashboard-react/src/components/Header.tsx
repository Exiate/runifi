import { memo } from 'react';
import { formatUptime } from '../utils/format';

type SseStatus = 'connecting' | 'connected' | 'disconnected';

interface HeaderProps {
  flowName: string;
  uptimeSecs: number;
  sseStatus: SseStatus;
}

function HeaderInner({ flowName, uptimeSecs, sseStatus }: HeaderProps) {
  const statusLabel =
    sseStatus === 'connected'
      ? 'Live'
      : sseStatus === 'connecting'
        ? 'Connecting...'
        : 'Disconnected';

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
      </div>
    </header>
  );
}

export const Header = memo(HeaderInner);
