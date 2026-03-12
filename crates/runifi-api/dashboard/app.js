(function () {
    'use strict';

    const $ = (sel) => document.querySelector(sel);
    const statusEl = $('#connection-status');
    const flowNameEl = $('#flow-name');
    const uptimeEl = $('#uptime');
    const versionEl = $('#version');
    const processorsGrid = $('#processors-grid');
    const connectionsGrid = $('#connections-grid');
    const dagCanvas = $('#dag-canvas');

    // Summary bar elements
    const summaryProcCount = $('#summary-proc-count');
    const summaryProcIndicator = $('#summary-proc-indicator');
    const summaryQueuedCount = $('#summary-queued-count');
    const summaryThroughputValue = $('#summary-throughput-value');
    const summaryStatePills = $('#summary-state-pills');
    const summaryBpCount = $('#summary-bp-count');
    const summaryBpIndicator = $('#summary-bp-indicator');

    // ── DAG state ──────────────────────────────────────────────
    const SVG_NS = 'http://www.w3.org/2000/svg';
    const NODE_WIDTH = 200;
    const NODE_HEIGHT = 100;
    const LAYER_GAP_X = 120;
    const NODE_GAP_Y = 40;
    const PADDING_X = 40;
    const PADDING_Y = 30;

    let flowTopology = null; // { processors: [], connections: [] }
    let dagLayout = null;    // { nodes: Map<name, {x,y,layer,col}>, layers: [] }
    let lastProcessorMetrics = {}; // name → metrics object
    let lastConnectionMetrics = {}; // "source→rel→dest" → {queued_count, queued_bytes, back_pressured}

    // ── Fetch initial system info ──────────────────────────────
    fetch('/api/v1/system')
        .then(r => r.json())
        .then(data => {
            flowNameEl.textContent = data.flow_name;
            versionEl.textContent = 'v' + data.version;
        })
        .catch(() => {});

    // ── Fetch flow topology and build initial DAG ──────────────
    fetch('/api/v1/flow')
        .then(r => r.json())
        .then(data => {
            flowTopology = data;
            dagLayout = computeLayout(data);
            renderDag();
        })
        .catch(() => {});

    // ── Layout algorithm ───────────────────────────────────────
    // Simple layered DAG layout via topological sort (Kahn's algorithm)
    function computeLayout(flow) {
        const procs = flow.processors;
        const conns = flow.connections;

        // Build adjacency and in-degree maps
        const nameSet = new Set(procs.map(p => p.name));
        const inDegree = new Map();
        const outEdges = new Map();
        const inEdges = new Map();

        for (const name of nameSet) {
            inDegree.set(name, 0);
            outEdges.set(name, []);
            inEdges.set(name, []);
        }

        for (const c of conns) {
            if (nameSet.has(c.source) && nameSet.has(c.destination)) {
                outEdges.get(c.source).push(c.destination);
                inEdges.get(c.destination).push(c.source);
                inDegree.set(c.destination, inDegree.get(c.destination) + 1);
            }
        }

        // Kahn's algorithm — assign layers
        const layers = [];
        const nodeLayer = new Map();
        const queue = [];

        for (const [name, deg] of inDegree) {
            if (deg === 0) queue.push(name);
        }

        while (queue.length > 0) {
            const current = [];
            const nextQueue = [];
            for (const name of queue) {
                current.push(name);
                const layerIdx = layers.length;
                nodeLayer.set(name, layerIdx);
            }
            layers.push(current);

            for (const name of current) {
                for (const dest of outEdges.get(name)) {
                    const newDeg = inDegree.get(dest) - 1;
                    inDegree.set(dest, newDeg);
                    if (newDeg === 0) {
                        nextQueue.push(dest);
                    }
                }
            }
            queue.length = 0;
            queue.push(...nextQueue);
        }

        // Any remaining nodes (cycles, shouldn't happen) go in last layer
        for (const name of nameSet) {
            if (!nodeLayer.has(name)) {
                if (layers.length === 0) layers.push([]);
                layers[layers.length - 1].push(name);
                nodeLayer.set(name, layers.length - 1);
            }
        }

        // Compute x,y positions
        const nodes = new Map();
        for (let li = 0; li < layers.length; li++) {
            const layer = layers[li];
            const x = PADDING_X + li * (NODE_WIDTH + LAYER_GAP_X);
            for (let ni = 0; ni < layer.length; ni++) {
                const y = PADDING_Y + ni * (NODE_HEIGHT + NODE_GAP_Y);
                nodes.set(layer[ni], { x, y, layer: li, col: ni });
            }
        }

        // Compute total SVG dimensions
        const totalWidth = PADDING_X * 2 + layers.length * NODE_WIDTH + (layers.length - 1) * LAYER_GAP_X;
        const maxNodesInLayer = Math.max(...layers.map(l => l.length));
        const totalHeight = PADDING_Y * 2 + maxNodesInLayer * NODE_HEIGHT + (maxNodesInLayer - 1) * NODE_GAP_Y;

        return { nodes, layers, totalWidth, totalHeight };
    }

    // ── DAG rendering ──────────────────────────────────────────
    function renderDag() {
        if (!flowTopology || !dagLayout) return;

        dagCanvas.innerHTML = '';
        const { nodes, totalWidth, totalHeight } = dagLayout;

        dagCanvas.setAttribute('width', Math.max(totalWidth, 400));
        dagCanvas.setAttribute('height', Math.max(totalHeight, 200));
        dagCanvas.setAttribute('viewBox', '0 0 ' + Math.max(totalWidth, 400) + ' ' + Math.max(totalHeight, 200));

        // Defs: arrowhead marker
        const defs = svgEl('defs');
        const marker = svgEl('marker', {
            id: 'dag-arrow',
            viewBox: '0 0 10 10',
            refX: '10',
            refY: '5',
            markerWidth: '8',
            markerHeight: '8',
            orient: 'auto-start-reverse'
        });
        const arrowPath = svgEl('path', { d: 'M 0 0 L 10 5 L 0 10 z' });
        arrowPath.classList.add('dag-arrowhead');
        marker.appendChild(arrowPath);
        defs.appendChild(marker);
        dagCanvas.appendChild(defs);

        // Draw edges first (so they appear behind nodes)
        for (const conn of flowTopology.connections) {
            const srcPos = nodes.get(conn.source);
            const dstPos = nodes.get(conn.destination);
            if (!srcPos || !dstPos) continue;
            renderEdge(conn, srcPos, dstPos);
        }

        // Draw nodes
        for (const proc of flowTopology.processors) {
            const pos = nodes.get(proc.name);
            if (!pos) continue;
            renderNode(proc, pos);
        }
    }

    function renderNode(proc, pos) {
        const g = svgEl('g', { 'data-processor': proc.name });
        g.setAttribute('transform', 'translate(' + pos.x + ',' + pos.y + ')');

        // Background rect
        const rect = svgEl('rect', {
            width: NODE_WIDTH,
            height: NODE_HEIGHT,
            x: 0,
            y: 0
        });
        rect.classList.add('dag-node-rect');

        // Apply state class from metrics
        const metrics = lastProcessorMetrics[proc.name];
        const state = metrics ? metrics.state : 'stopped';
        rect.classList.add('state-' + state);
        g.appendChild(rect);

        // Processor name (bold)
        const nameText = svgEl('text', { x: NODE_WIDTH / 2, y: 22 });
        nameText.classList.add('dag-node-name');
        nameText.setAttribute('text-anchor', 'middle');
        nameText.textContent = truncate(proc.name, 22);
        g.appendChild(nameText);

        // Processor type (subtitle)
        const typeText = svgEl('text', { x: NODE_WIDTH / 2, y: 38 });
        typeText.classList.add('dag-node-type');
        typeText.setAttribute('text-anchor', 'middle');
        typeText.textContent = proc.type_name;
        g.appendChild(typeText);

        // Separator line
        const sep = svgEl('line', { x1: 10, y1: 48, x2: NODE_WIDTH - 10, y2: 48 });
        sep.setAttribute('stroke', 'var(--border)');
        sep.setAttribute('stroke-width', '1');
        g.appendChild(sep);

        // Metrics row 1: In / Out
        const ffIn = metrics ? fmt(metrics.metrics.flowfiles_in) : '0';
        const ffOut = metrics ? fmt(metrics.metrics.flowfiles_out) : '0';

        const inLabel = svgEl('text', { x: 14, y: 66 });
        inLabel.classList.add('dag-node-metric');
        inLabel.innerHTML = 'In: <tspan class="dag-node-metric-value">' + ffIn + '</tspan>';
        g.appendChild(inLabel);

        const outLabel = svgEl('text', { x: NODE_WIDTH - 14, y: 66 });
        outLabel.classList.add('dag-node-metric');
        outLabel.setAttribute('text-anchor', 'end');
        outLabel.innerHTML = 'Out: <tspan class="dag-node-metric-value">' + ffOut + '</tspan>';
        g.appendChild(outLabel);

        // Metrics row 2: Invocations
        const invocations = metrics ? fmt(metrics.metrics.total_invocations) : '0';
        const invLabel = svgEl('text', { x: 14, y: 82 });
        invLabel.classList.add('dag-node-metric');
        invLabel.innerHTML = 'Invocations: <tspan class="dag-node-metric-value">' + invocations + '</tspan>';
        g.appendChild(invLabel);

        // State indicator
        const stateLabel = svgEl('text', { x: NODE_WIDTH - 14, y: 82 });
        stateLabel.classList.add('dag-node-metric');
        stateLabel.setAttribute('text-anchor', 'end');
        stateLabel.setAttribute('fill', stateColor(state));
        stateLabel.textContent = state;
        g.appendChild(stateLabel);

        dagCanvas.appendChild(g);
    }

    function renderEdge(conn, srcPos, dstPos) {
        const g = svgEl('g', { 'data-edge': conn.source + '\u2192' + conn.destination });

        // Calculate start and end points
        const x1 = srcPos.x + NODE_WIDTH;
        const y1 = srcPos.y + NODE_HEIGHT / 2;
        const x2 = dstPos.x;
        const y2 = dstPos.y + NODE_HEIGHT / 2;

        // Cubic bezier curve
        const midX = (x1 + x2) / 2;
        const pathD = 'M ' + x1 + ' ' + y1
            + ' C ' + midX + ' ' + y1 + ', ' + midX + ' ' + y2 + ', ' + x2 + ' ' + y2;

        const path = svgEl('path', {
            d: pathD,
            'marker-end': 'url(#dag-arrow)'
        });
        path.classList.add('dag-edge');
        g.appendChild(path);

        // Edge label: relationship name
        const labelX = midX;
        const labelY = (y1 + y2) / 2 - 10;
        const relLabel = svgEl('text', { x: labelX, y: labelY });
        relLabel.classList.add('dag-edge-label');
        relLabel.setAttribute('text-anchor', 'middle');
        relLabel.textContent = conn.relationship;
        g.appendChild(relLabel);

        // Queue depth label
        const key = conn.source + '\u2192' + conn.relationship + '\u2192' + conn.destination;
        const connMetrics = lastConnectionMetrics[key];
        const queueCount = connMetrics ? connMetrics.queued_count : 0;

        const queueLabel = svgEl('text', { x: labelX, y: labelY + 14 });
        queueLabel.classList.add('dag-edge-queue');
        queueLabel.setAttribute('text-anchor', 'middle');
        queueLabel.setAttribute('data-queue-label', key);
        queueLabel.textContent = 'queued: ' + fmt(queueCount);
        g.appendChild(queueLabel);

        dagCanvas.appendChild(g);
    }

    // ── DAG update (metrics only, no re-layout) ────────────────
    function updateDagMetrics() {
        if (!flowTopology || !dagLayout) return;

        // Update processor nodes
        for (const proc of flowTopology.processors) {
            const g = dagCanvas.querySelector('[data-processor="' + CSS.escape(proc.name) + '"]');
            if (!g) continue;

            const metrics = lastProcessorMetrics[proc.name];
            if (!metrics) continue;

            const state = metrics.state || 'stopped';
            const rect = g.querySelector('.dag-node-rect');
            if (rect) {
                rect.classList.remove('state-running', 'state-paused', 'state-stopped');
                rect.classList.add('state-' + state);
            }

            // Update metric texts
            const texts = g.querySelectorAll('.dag-node-metric');
            if (texts.length >= 3) {
                // In
                texts[0].innerHTML = 'In: <tspan class="dag-node-metric-value">'
                    + fmt(metrics.metrics.flowfiles_in) + '</tspan>';
                // Out
                texts[1].innerHTML = 'Out: <tspan class="dag-node-metric-value">'
                    + fmt(metrics.metrics.flowfiles_out) + '</tspan>';
                // Invocations
                texts[2].innerHTML = 'Invocations: <tspan class="dag-node-metric-value">'
                    + fmt(metrics.metrics.total_invocations) + '</tspan>';
            }
            if (texts.length >= 4) {
                // State text
                texts[3].textContent = state;
                texts[3].setAttribute('fill', stateColor(state));
            }
        }

        // Update connection queue labels
        for (const conn of flowTopology.connections) {
            const key = conn.source + '\u2192' + conn.relationship + '\u2192' + conn.destination;
            const label = dagCanvas.querySelector('[data-queue-label="' + CSS.escape(key) + '"]');
            if (!label) continue;
            const connMetrics = lastConnectionMetrics[key];
            const queueCount = connMetrics ? connMetrics.queued_count : 0;
            label.textContent = 'queued: ' + fmt(queueCount);
        }
    }

    // ── SVG helpers ────────────────────────────────────────────
    function svgEl(tag, attrs) {
        const el = document.createElementNS(SVG_NS, tag);
        if (attrs) {
            for (const [k, v] of Object.entries(attrs)) {
                el.setAttribute(k, v);
            }
        }
        return el;
    }

    function stateColor(state) {
        if (state === 'running') return 'var(--success)';
        if (state === 'paused') return 'var(--warning)';
        return 'var(--text-dim)';
    }

    function truncate(s, maxLen) {
        if (s.length <= maxLen) return s;
        return s.substring(0, maxLen - 1) + '\u2026';
    }

    // ── SSE connection ─────────────────────────────────────────
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

        // Update summary bar
        updateSummaryBar(data.processors, data.connections);

        // Cache metrics for DAG
        lastProcessorMetrics = {};
        for (const p of data.processors) {
            lastProcessorMetrics[p.name] = p;
        }
        lastConnectionMetrics = {};
        for (const c of data.connections) {
            const key = c.source_name + '\u2192' + c.relationship + '\u2192' + c.dest_name;
            lastConnectionMetrics[key] = c;
        }

        // Update DAG metrics (efficient — no re-render of layout)
        if (dagLayout) {
            updateDagMetrics();
        } else if (flowTopology) {
            // If we have topology but haven't rendered yet, do initial render
            dagLayout = computeLayout(flowTopology);
            renderDag();
        }

        // Update detail cards
        renderProcessors(data.processors);
        renderConnections(data.connections);
    }

    // ── Summary bar ────────────────────────────────────────────
    function updateSummaryBar(processors, connections) {
        const total = processors.length;
        const stateCounts = { running: 0, paused: 0, stopped: 0 };
        let circuitOpenCount = 0;
        let totalFfOutRate = 0;
        let totalBytesOutRate = 0;

        for (const p of processors) {
            const state = p.state || 'stopped';
            if (state in stateCounts) {
                stateCounts[state]++;
            } else {
                stateCounts[state] = (stateCounts[state] || 0) + 1;
            }
            if (p.metrics.circuit_open) {
                circuitOpenCount++;
            }
            totalFfOutRate += p.metrics.flowfiles_out_rate || 0;
            totalBytesOutRate += p.metrics.bytes_out_rate || 0;
        }

        const running = stateCounts.running;

        // Processor count + indicator
        summaryProcCount.textContent = running + ' / ' + total + ' Running';
        if (circuitOpenCount > 0) {
            summaryProcIndicator.className = 'summary-indicator danger';
        } else if (running === total && total > 0) {
            summaryProcIndicator.className = 'summary-indicator healthy';
        } else if (running > 0) {
            summaryProcIndicator.className = 'summary-indicator warning';
        } else {
            summaryProcIndicator.className = 'summary-indicator';
        }

        // Total queued FlowFiles
        let totalQueued = 0;
        let bpCount = 0;
        for (const c of connections) {
            totalQueued += c.queued_count;
            if (c.back_pressured) bpCount++;
        }
        summaryQueuedCount.textContent = fmt(totalQueued);

        // System throughput
        const ffRateStr = fmtRate(totalFfOutRate);
        const bytesRateStr = fmtBytes(totalBytesOutRate);
        summaryThroughputValue.textContent = ffRateStr + ' FF/s \u00B7 ' + bytesRateStr + '/s';

        // State pills
        summaryStatePills.innerHTML = '';
        if (stateCounts.running > 0) {
            summaryStatePills.innerHTML += '<span class="state-pill running">' + stateCounts.running + ' running</span>';
        }
        if (stateCounts.paused > 0) {
            summaryStatePills.innerHTML += '<span class="state-pill paused">' + stateCounts.paused + ' paused</span>';
        }
        if (stateCounts.stopped > 0) {
            summaryStatePills.innerHTML += '<span class="state-pill stopped">' + stateCounts.stopped + ' stopped</span>';
        }
        if (circuitOpenCount > 0) {
            summaryStatePills.innerHTML += '<span class="state-pill circuit-open">' + circuitOpenCount + ' circuit-open</span>';
        }

        // Back-pressure count + indicator
        summaryBpCount.textContent = bpCount + ' / ' + connections.length;
        if (bpCount > 0) {
            summaryBpIndicator.className = 'summary-indicator warning';
        } else if (connections.length > 0) {
            summaryBpIndicator.className = 'summary-indicator healthy';
        } else {
            summaryBpIndicator.className = 'summary-indicator';
        }
    }

    // ── Processor cards ────────────────────────────────────────
    function renderProcessors(processors) {
        processorsGrid.innerHTML = '';
        for (const p of processors) {
            const card = document.createElement('div');
            card.className = 'card';
            const circuitClass = p.metrics.circuit_open ? 'open' : 'ok';
            const circuitText = p.metrics.circuit_open ? 'Circuit Open' : 'OK';
            const failClass = p.metrics.total_failures > 0 ? ' danger' : '';
            const state = p.state || 'stopped';
            const isRunning = state === 'running';
            const isPaused = state === 'paused';
            const isStopped = state === 'stopped';

            card.innerHTML = `
                <div class="card-header">
                    <div>
                        <div class="card-title">${esc(p.name)}</div>
                        <div class="card-type">${esc(p.type_name)} &middot; ${esc(p.scheduling)}</div>
                    </div>
                    <div class="card-badges">
                        <span class="state-badge ${esc(state)}">${esc(state)}</span>
                        <span class="circuit-badge ${circuitClass}"
                              ${p.metrics.circuit_open ? 'title="Click to reset"' : ''}
                              data-processor="${esc(p.name)}">${circuitText}</span>
                    </div>
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
                <div class="rolling-header">5-Minute Window</div>
                <div class="metrics-grid rolling">
                    <div class="metric">
                        <span class="metric-label">FF In/s</span>
                        <span class="metric-value">${fmtRate(p.metrics.flowfiles_in_rate)}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">FF Out/s</span>
                        <span class="metric-value">${fmtRate(p.metrics.flowfiles_out_rate)}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">In (5m)</span>
                        <span class="metric-value">${fmt(p.metrics.flowfiles_in_5m)} FF / ${fmtBytes(p.metrics.bytes_in_5m)}</span>
                    </div>
                    <div class="metric">
                        <span class="metric-label">Out (5m)</span>
                        <span class="metric-value">${fmt(p.metrics.flowfiles_out_5m)} FF / ${fmtBytes(p.metrics.bytes_out_5m)}</span>
                    </div>
                </div>
                <div class="proc-controls">
                    <button class="btn-start" ${isRunning ? 'disabled' : ''} data-action="start" data-processor="${esc(p.name)}">Start</button>
                    <button class="btn-pause" ${!isRunning ? 'disabled' : ''} data-action="pause" data-processor="${esc(p.name)}">Pause</button>
                    <button class="btn-resume" ${!isPaused ? 'disabled' : ''} data-action="resume" data-processor="${esc(p.name)}">Resume</button>
                    <button class="btn-stop" ${isStopped ? 'disabled' : ''} data-action="stop" data-processor="${esc(p.name)}">Stop</button>
                </div>
                <button class="btn-config" data-config-processor="${esc(p.name)}">Configure</button>
            `;

            // Circuit reset click handler
            const badge = card.querySelector('.circuit-badge.open');
            if (badge) {
                badge.addEventListener('click', () => resetCircuit(p.name));
            }

            // Processor control button handlers
            card.querySelectorAll('.proc-controls button').forEach(btn => {
                btn.addEventListener('click', () => {
                    const action = btn.getAttribute('data-action');
                    const proc = btn.getAttribute('data-processor');
                    controlProcessor(proc, action);
                });
            });

            // Configure button handler
            const configBtn = card.querySelector('.btn-config');
            if (configBtn) {
                configBtn.addEventListener('click', () => {
                    openConfigModal(configBtn.getAttribute('data-config-processor'));
                });
            }

            processorsGrid.appendChild(card);
        }
    }

    // ── Connection cards ───────────────────────────────────────
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
                <div class="conn-inspect-hint">Click to inspect queue</div>
            `;

            // Click handler to open queue inspector
            (function(connId, srcName, relName, dstName) {
                card.addEventListener('click', function() {
                    openQueueInspector(connId, srcName, relName, dstName);
                });
            })(c.id, c.source_name, c.relationship, c.dest_name);

            connectionsGrid.appendChild(card);
        }
    }

    // ── Queue Inspector ───────────────────────────────────────
    const queueModal = $('#queue-modal');
    const queueModalTitle = $('#queue-modal-title');
    const queueModalClose = $('#queue-modal-close');
    const queueSummary = $('#queue-summary');
    const queueEmptyBtn = $('#queue-empty-btn');
    const queueTableBody = $('#queue-table-body');
    const queuePagination = $('#queue-pagination');

    const flowfileModal = $('#flowfile-modal');
    const flowfileModalTitle = $('#flowfile-modal-title');
    const flowfileModalClose = $('#flowfile-modal-close');
    const ffDetailMeta = $('#ff-detail-meta');
    const ffAttrBody = $('#ff-attr-body');

    const confirmModal = $('#confirm-modal');
    const confirmMessage = $('#confirm-message');
    const confirmCancel = $('#confirm-cancel');
    let confirmOkBtn = $('#confirm-ok');
    const confirmModalClose = $('#confirm-modal-close');

    let currentQueueConnId = null;
    let currentQueueOffset = 0;
    const QUEUE_PAGE_SIZE = 50;

    // Close handlers for queue modals
    if (queueModalClose) {
        queueModalClose.addEventListener('click', function() { queueModal.style.display = 'none'; });
    }
    if (flowfileModalClose) {
        flowfileModalClose.addEventListener('click', function() { flowfileModal.style.display = 'none'; });
    }
    if (confirmModalClose) {
        confirmModalClose.addEventListener('click', function() { confirmModal.style.display = 'none'; });
    }
    if (confirmCancel) {
        confirmCancel.addEventListener('click', function() { confirmModal.style.display = 'none'; });
    }

    // Close queue modals on overlay click
    [queueModal, flowfileModal, confirmModal].forEach(function(modal) {
        if (modal) {
            modal.addEventListener('click', function(e) {
                if (e.target === modal) modal.style.display = 'none';
            });
        }
    });

    function openQueueInspector(connId, source, rel, dest) {
        currentQueueConnId = connId;
        currentQueueOffset = 0;
        if (queueModalTitle) {
            queueModalTitle.textContent = source + ' \u2192 ' + rel + ' \u2192 ' + dest;
        }
        if (queueModal) {
            queueModal.style.display = 'flex';
        }
        loadQueuePage();
    }

    function loadQueuePage() {
        if (!currentQueueConnId) return;
        var url = '/api/v1/connections/' + encodeURIComponent(currentQueueConnId)
            + '/queue?offset=' + currentQueueOffset + '&limit=' + QUEUE_PAGE_SIZE;

        fetch(url)
            .then(function(r) { return r.json(); })
            .then(function(data) { renderQueueTable(data); })
            .catch(function() {
                if (queueTableBody) {
                    queueTableBody.innerHTML = '<tr class="empty-row"><td colspan="6">Failed to load queue</td></tr>';
                }
            });
    }

    function renderQueueTable(data) {
        if (queueSummary) {
            queueSummary.textContent = data.total_count + ' FlowFile' + (data.total_count !== 1 ? 's' : '') + ' queued';
        }
        if (!queueTableBody) return;
        queueTableBody.innerHTML = '';

        if (data.flowfiles.length === 0) {
            queueTableBody.innerHTML = '<tr class="empty-row"><td colspan="6">Queue is empty</td></tr>';
        } else {
            for (var idx = 0; idx < data.flowfiles.length; idx++) {
                var tr = document.createElement('tr');
                var ff = data.flowfiles[idx];
                var downloadLink = ff.has_content
                    ? '<a class="btn-link ff-download-btn" href="/api/v1/connections/'
                      + encodeURIComponent(currentQueueConnId)
                      + '/queue/' + ff.id + '/content" target="_blank">Download</a>'
                    : '';
                tr.innerHTML = '<td>' + (ff.position + 1) + '</td>'
                    + '<td>' + ff.id + '</td>'
                    + '<td>' + fmtBytes(ff.size) + '</td>'
                    + '<td>' + fmtAge(ff.age_ms) + '</td>'
                    + '<td>' + (ff.has_content ? 'Yes' : 'No') + '</td>'
                    + '<td>'
                    + '<button class="btn-link ff-view-btn" data-ff-id="' + ff.id + '">View</button>'
                    + downloadLink
                    + '<button class="btn-remove ff-remove-btn" data-ff-id="' + ff.id + '">Remove</button>'
                    + '</td>';

                (function(flowfile) {
                    tr.addEventListener('click', function(e) {
                        if (e.target.tagName === 'BUTTON' || e.target.tagName === 'A') return;
                        openFlowFileDetail(flowfile);
                    });
                    tr.querySelector('.ff-view-btn').addEventListener('click', function(e) {
                        e.stopPropagation();
                        openFlowFileDetail(flowfile);
                    });
                    tr.querySelector('.ff-remove-btn').addEventListener('click', function(e) {
                        e.stopPropagation();
                        showConfirmDialog('Remove FlowFile #' + flowfile.id + ' from the queue?', function() {
                            removeFlowFile(currentQueueConnId, flowfile.id);
                        });
                    });
                })(ff);

                queueTableBody.appendChild(tr);
            }
        }

        renderQueuePagination(data.total_count, data.offset, data.limit);
    }

    function renderQueuePagination(total, offset, limit) {
        if (!queuePagination) return;
        queuePagination.innerHTML = '';
        if (total <= limit) return;

        var totalPages = Math.ceil(total / limit);
        var currentPage = Math.floor(offset / limit) + 1;

        var prevBtn = document.createElement('button');
        prevBtn.textContent = 'Previous';
        prevBtn.disabled = currentPage <= 1;
        prevBtn.addEventListener('click', function() {
            currentQueueOffset = Math.max(0, currentQueueOffset - QUEUE_PAGE_SIZE);
            loadQueuePage();
        });
        queuePagination.appendChild(prevBtn);

        var info = document.createElement('span');
        info.textContent = 'Page ' + currentPage + ' of ' + totalPages;
        queuePagination.appendChild(info);

        var nextBtn = document.createElement('button');
        nextBtn.textContent = 'Next';
        nextBtn.disabled = currentPage >= totalPages;
        nextBtn.addEventListener('click', function() {
            currentQueueOffset += QUEUE_PAGE_SIZE;
            loadQueuePage();
        });
        queuePagination.appendChild(nextBtn);
    }

    function openFlowFileDetail(ff) {
        if (flowfileModalTitle) {
            flowfileModalTitle.textContent = 'FlowFile #' + ff.id;
        }
        if (ffDetailMeta) {
            ffDetailMeta.innerHTML = ''
                + '<div class="detail-row"><span class="detail-label">ID</span><span class="detail-value">' + ff.id + '</span></div>'
                + '<div class="detail-row"><span class="detail-label">Size</span><span class="detail-value">' + fmtBytes(ff.size) + '</span></div>'
                + '<div class="detail-row"><span class="detail-label">Age</span><span class="detail-value">' + fmtAge(ff.age_ms) + '</span></div>'
                + '<div class="detail-row"><span class="detail-label">Has Content</span><span class="detail-value">' + (ff.has_content ? 'Yes' : 'No') + '</span></div>';
        }
        if (ffAttrBody) {
            ffAttrBody.innerHTML = '';
            if (ff.attributes.length === 0) {
                ffAttrBody.innerHTML = '<tr><td colspan="2" style="color: var(--text-dim); text-align: center;">No attributes</td></tr>';
            } else {
                for (var i = 0; i < ff.attributes.length; i++) {
                    var attr = ff.attributes[i];
                    var atr = document.createElement('tr');
                    atr.innerHTML = '<td>' + esc(attr.key) + '</td><td>' + esc(attr.value) + '</td>';
                    ffAttrBody.appendChild(atr);
                }
            }
        }
        if (flowfileModal) {
            flowfileModal.style.display = 'flex';
        }
    }

    // Empty Queue button
    if (queueEmptyBtn) {
        queueEmptyBtn.addEventListener('click', function() {
            showConfirmDialog('Empty the entire queue? This will remove all FlowFiles from this connection.', function() {
                emptyQueue(currentQueueConnId);
            });
        });
    }

    function emptyQueue(connId) {
        fetch('/api/v1/connections/' + encodeURIComponent(connId) + '/queue', {
            method: 'DELETE'
        })
        .then(function(r) { return r.json(); })
        .then(function() {
            currentQueueOffset = 0;
            loadQueuePage();
        })
        .catch(function() {});
    }

    function removeFlowFile(connId, ffId) {
        fetch('/api/v1/connections/' + encodeURIComponent(connId) + '/queue/' + ffId, {
            method: 'DELETE'
        })
        .then(function(r) { return r.json(); })
        .then(function() { loadQueuePage(); })
        .catch(function() {});
    }

    function showConfirmDialog(message, onConfirm) {
        if (confirmMessage) confirmMessage.textContent = message;
        if (confirmModal) confirmModal.style.display = 'flex';
        if (confirmOkBtn) {
            var newOk = confirmOkBtn.cloneNode(true);
            confirmOkBtn.parentNode.replaceChild(newOk, confirmOkBtn);
            confirmOkBtn = newOk;
            newOk.addEventListener('click', function() {
                if (confirmModal) confirmModal.style.display = 'none';
                onConfirm();
            });
        }
    }

    function fmtAge(ms) {
        if (ms < 1000) return ms + 'ms';
        var secs = Math.floor(ms / 1000);
        if (secs < 60) return secs + 's';
        var mins = Math.floor(secs / 60);
        if (mins < 60) return mins + 'm ' + (secs % 60) + 's';
        var hours = Math.floor(mins / 60);
        return hours + 'h ' + (mins % 60) + 'm';
    }

    // ── API actions ────────────────────────────────────────────
    function resetCircuit(name) {
        fetch('/api/v1/processors/' + encodeURIComponent(name) + '/reset-circuit', {
            method: 'POST'
        }).catch(() => {});
    }

    function controlProcessor(name, action) {
        fetch('/api/v1/processors/' + encodeURIComponent(name) + '/' + action, {
            method: 'POST'
        }).catch(() => {});
    }

    // ── Configuration Modal ───────────────────────────────────
    const configModal = $('#config-modal');
    const configModalTitle = $('#config-modal-title');
    const configModalSubtitle = $('#config-modal-subtitle');
    const configModalStatus = $('#config-modal-status');
    const configModalSave = $('#config-modal-save');
    const tabProperties = $('#tab-properties');
    const tabScheduling = $('#tab-scheduling');
    const tabRelationships = $('#tab-relationships');

    let currentConfigProcessor = null;
    let currentConfigData = null;

    // Tab switching
    document.querySelectorAll('.modal-tabs .tab').forEach(tab => {
        tab.addEventListener('click', () => {
            document.querySelectorAll('.modal-tabs .tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(tc => tc.classList.remove('active'));
            tab.classList.add('active');
            const target = tab.getAttribute('data-tab');
            $('#tab-' + target).classList.add('active');
        });
    });

    // Close modal
    $('#config-modal-close').addEventListener('click', closeConfigModal);
    $('#config-modal-cancel').addEventListener('click', closeConfigModal);
    configModal.addEventListener('click', (e) => {
        if (e.target === configModal) closeConfigModal();
    });

    function closeConfigModal() {
        configModal.style.display = 'none';
        currentConfigProcessor = null;
        currentConfigData = null;
        configModalStatus.textContent = '';
        configModalStatus.className = 'modal-status';
    }

    function openConfigModal(processorName) {
        currentConfigProcessor = processorName;
        configModalStatus.textContent = 'Loading...';
        configModalStatus.className = 'modal-status';
        configModal.style.display = 'flex';

        // Reset to properties tab
        document.querySelectorAll('.modal-tabs .tab').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.tab-content').forEach(tc => tc.classList.remove('active'));
        document.querySelector('.modal-tabs .tab[data-tab="properties"]').classList.add('active');
        tabProperties.classList.add('active');

        // Fetch config
        fetch('/api/v1/processors/' + encodeURIComponent(processorName) + '/config')
            .then(r => {
                if (!r.ok) throw new Error('Failed to load config');
                return r.json();
            })
            .then(data => {
                currentConfigData = data;
                configModalTitle.textContent = data.processor_name;
                configModalSubtitle.textContent = data.type_name;
                configModalStatus.textContent = '';
                renderConfigTabs(data);
            })
            .catch(err => {
                configModalStatus.textContent = err.message;
                configModalStatus.className = 'modal-status error';
            });
    }

    function renderConfigTabs(data) {
        // Properties tab
        if (data.property_descriptors.length === 0) {
            tabProperties.innerHTML = '<div class="config-empty">This processor has no configurable properties.</div>';
        } else {
            let html = '';
            for (const desc of data.property_descriptors) {
                const currentVal = data.properties[desc.name] || '';
                const hasAllowed = desc.allowed_values && desc.allowed_values.length > 0;
                html += '<div class="config-field">';
                html += '<label class="config-label">' + esc(desc.name);
                if (desc.required) html += '<span class="config-required">REQUIRED</span>';
                html += '</label>';
                html += '<span class="config-description">' + esc(desc.description) + '</span>';

                if (hasAllowed) {
                    html += '<select class="config-select" data-prop="' + esc(desc.name) + '">';
                    if (!desc.required) {
                        html += '<option value="">-- Select --</option>';
                    }
                    for (const av of desc.allowed_values) {
                        const selected = currentVal === av ? ' selected' : '';
                        html += '<option value="' + esc(av) + '"' + selected + '>' + esc(av) + '</option>';
                    }
                    html += '</select>';
                } else {
                    const placeholder = desc.default_value ? 'Default: ' + desc.default_value : '';
                    html += '<input class="config-input" type="text" data-prop="' + esc(desc.name)
                        + '" value="' + esc(currentVal)
                        + '" placeholder="' + esc(placeholder) + '">';
                }

                if (desc.default_value) {
                    html += '<span class="config-default">Default: ' + esc(desc.default_value) + '</span>';
                }
                html += '</div>';
            }
            tabProperties.innerHTML = html;
        }

        // Scheduling tab
        {
            let html = '<div class="scheduling-info">';
            html += '<div class="config-field">';
            html += '<label class="config-label">Strategy</label>';
            html += '<span class="config-description">How the processor is triggered</span>';
            html += '<input class="config-input" type="text" value="' + esc(data.scheduling.strategy) + '" disabled>';
            html += '</div>';

            if (data.scheduling.interval_ms !== null && data.scheduling.interval_ms !== undefined) {
                html += '<div class="config-field">';
                html += '<label class="config-label">Interval</label>';
                html += '<span class="config-description">Trigger interval in milliseconds</span>';
                html += '<input class="config-input" type="text" value="' + data.scheduling.interval_ms + ' ms" disabled>';
                html += '</div>';
            }

            html += '</div>';
            tabScheduling.innerHTML = html;
        }

        // Relationships tab
        if (data.relationships.length === 0) {
            tabRelationships.innerHTML = '<div class="config-empty">This processor has no relationships.</div>';
        } else {
            let html = '<table class="rel-table">';
            html += '<thead><tr><th>Name</th><th>Description</th><th>Auto-Terminated</th></tr></thead>';
            html += '<tbody>';
            for (const rel of data.relationships) {
                const atClass = rel.auto_terminated ? 'yes' : 'no';
                const atText = rel.auto_terminated ? 'Yes' : 'No';
                html += '<tr>';
                html += '<td><strong>' + esc(rel.name) + '</strong></td>';
                html += '<td>' + esc(rel.description) + '</td>';
                html += '<td><span class="rel-auto-term ' + atClass + '">' + atText + '</span></td>';
                html += '</tr>';
            }
            html += '</tbody></table>';
            tabRelationships.innerHTML = html;
        }

        // Check processor state to enable/disable save
        const metrics = lastProcessorMetrics[data.processor_name];
        const state = metrics ? metrics.state : 'stopped';
        if (state !== 'stopped') {
            configModalSave.disabled = true;
            configModalStatus.textContent = 'Stop the processor to edit configuration';
            configModalStatus.className = 'modal-status';
        } else {
            configModalSave.disabled = false;
        }
    }

    // Save config
    configModalSave.addEventListener('click', () => {
        if (!currentConfigProcessor || !currentConfigData) return;

        // Check state
        const metrics = lastProcessorMetrics[currentConfigProcessor];
        const state = metrics ? metrics.state : 'stopped';
        if (state !== 'stopped') {
            configModalStatus.textContent = 'Processor must be stopped to save configuration';
            configModalStatus.className = 'modal-status error';
            return;
        }

        // Collect properties from form
        const properties = {};
        tabProperties.querySelectorAll('[data-prop]').forEach(el => {
            const name = el.getAttribute('data-prop');
            const value = el.value;
            if (value !== '') {
                properties[name] = value;
            }
        });

        // Validate required fields
        for (const desc of currentConfigData.property_descriptors) {
            if (desc.required && !properties[desc.name] && !desc.default_value) {
                configModalStatus.textContent = 'Required property "' + desc.name + '" is missing';
                configModalStatus.className = 'modal-status error';
                return;
            }
        }

        configModalStatus.textContent = 'Saving...';
        configModalStatus.className = 'modal-status';
        configModalSave.disabled = true;

        fetch('/api/v1/processors/' + encodeURIComponent(currentConfigProcessor) + '/config', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ properties: properties })
        })
        .then(r => {
            if (!r.ok) return r.json().then(data => { throw new Error(data.error || 'Save failed'); });
            return r.json();
        })
        .then(() => {
            configModalStatus.textContent = 'Configuration saved';
            configModalStatus.className = 'modal-status success';
            configModalSave.disabled = false;
            // Refresh config display
            setTimeout(() => openConfigModal(currentConfigProcessor), 800);
        })
        .catch(err => {
            configModalStatus.textContent = err.message;
            configModalStatus.className = 'modal-status error';
            configModalSave.disabled = false;
        });
    });

    // ── Formatters ─────────────────────────────────────────────
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

    function fmtRate(r) {
        if (r < 0.01) return '0';
        if (r < 10) return r.toFixed(2);
        if (r < 100) return r.toFixed(1);
        return Math.round(r).toLocaleString();
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
