(function () {
    'use strict';

    const $ = (sel) => document.querySelector(sel);
    const statusEl = $('#connection-status');
    const flowNameEl = $('#flow-name');
    const uptimeEl = $('#uptime');
    const versionEl = $('#version');
    const processorsGrid = $('#processors-grid');
    const connectionsGrid = $('#connections-grid');

    // Fetch initial system info
    fetch('/api/v1/system')
        .then(r => r.json())
        .then(data => {
            flowNameEl.textContent = data.flow_name;
            versionEl.textContent = 'v' + data.version;
        })
        .catch(() => {});

    // SSE connection
    let evtSource = null;

    function connect() {
        if (evtSource) evtSource.close();
        evtSource = new EventSource('/api/v1/events');

        evtSource.addEventListener('metrics', (e) => {
            const data = JSON.parse(e.data);
            statusEl.textContent = 'Connected';
            statusEl.className = 'status connected';
            updateDashboard(data);
        });

        evtSource.onerror = () => {
            statusEl.textContent = 'Disconnected';
            statusEl.className = 'status disconnected';
            evtSource.close();
            setTimeout(connect, 3000);
        };
    }

    function updateDashboard(data) {
        uptimeEl.textContent = formatUptime(data.uptime_secs);
        renderProcessors(data.processors);
        renderConnections(data.connections);
    }

    function renderProcessors(processors) {
        processorsGrid.innerHTML = '';
        for (const p of processors) {
            const card = document.createElement('div');
            card.className = 'card';
            const circuitClass = p.metrics.circuit_open ? 'open' : 'ok';
            const circuitText = p.metrics.circuit_open ? 'Circuit Open' : 'OK';
            const failClass = p.metrics.total_failures > 0 ? ' danger' : '';

            card.innerHTML = `
                <div class="card-header">
                    <div>
                        <div class="card-title">${esc(p.name)}</div>
                        <div class="card-type">${esc(p.type_name)} &middot; ${esc(p.scheduling)}</div>
                    </div>
                    <span class="circuit-badge ${circuitClass}"
                          ${p.metrics.circuit_open ? 'title="Click to reset"' : ''}
                          data-processor="${esc(p.name)}">${circuitText}</span>
                </div>
                <div class="metrics-grid">
                    <div class="metric">
                        <span class="metric-label">Invocations</span>
                        <span class="metric-value">${fmt(p.metrics.total_invocations)}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Failures</span>
                        <span class="metric-value${failClass}">${fmt(p.metrics.total_failures)}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">FlowFiles In</span>
                        <span class="metric-value">${fmt(p.metrics.flowfiles_in)}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">FlowFiles Out</span>
                        <span class="metric-value">${fmt(p.metrics.flowfiles_out)}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Bytes In</span>
                        <span class="metric-value">${fmtBytes(p.metrics.bytes_in)}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Bytes Out</span>
                        <span class="metric-value">${fmtBytes(p.metrics.bytes_out)}</span>
                    </div>
                </div>
            `;

            // Circuit reset click handler
            const badge = card.querySelector('.circuit-badge.open');
            if (badge) {
                badge.addEventListener('click', () => resetCircuit(p.name));
            }

            processorsGrid.appendChild(card);
        }
    }

    function renderConnections(connections) {
        connectionsGrid.innerHTML = '';
        for (const c of connections) {
            const card = document.createElement('div');
            card.className = 'conn-card';
            const bpClass = c.back_pressured ? 'pressured' : 'ok';
            const bpText = c.back_pressured ? 'Back-Pressured' : 'OK';
            // Estimate fill percentage (cap at 10000 for the bar)
            const fillPct = Math.min((c.queued_count / 10000) * 100, 100);
            const barClass = fillPct > 80 ? 'danger' : fillPct > 50 ? 'warning' : '';

            card.innerHTML = `
                <div class="conn-header">
                    <span class="conn-path">
                        ${esc(c.source_name)}
                        <span class="rel">&rarr; ${esc(c.relationship)} &rarr;</span>
                        ${esc(c.dest_name)}
                    </span>
                    <span class="bp-badge ${bpClass}">${bpText}</span>
                </div>
                <div class="queue-bar-container">
                    <div class="queue-bar ${barClass}" style="width: ${fillPct}%"></div>
                </div>
                <div class="conn-stats">
                    <span>Queued: ${fmt(c.queued_count)}</span>
                    <span>Bytes: ${fmtBytes(c.queued_bytes)}</span>
                </div>
            `;
            connectionsGrid.appendChild(card);
        }
    }

    function resetCircuit(name) {
        fetch('/api/v1/processors/' + encodeURIComponent(name) + '/reset-circuit', {
            method: 'POST'
        }).catch(() => {});
    }

    function formatUptime(secs) {
        const h = Math.floor(secs / 3600);
        const m = Math.floor((secs % 3600) / 60);
        const s = secs % 60;
        if (h > 0) return h + 'h ' + m + 'm ' + s + 's';
        if (m > 0) return m + 'm ' + s + 's';
        return s + 's';
    }

    function fmt(n) {
        return n.toLocaleString();
    }

    function fmtBytes(b) {
        if (b === 0) return '0 B';
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        const i = Math.floor(Math.log(b) / Math.log(1024));
        const val = b / Math.pow(1024, i);
        return val.toFixed(i > 0 ? 1 : 0) + ' ' + units[i];
    }

    function esc(s) {
        const d = document.createElement('div');
        d.textContent = s;
        return d.innerHTML;
    }

    connect();
})();
