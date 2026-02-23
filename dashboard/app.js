// dashboard/app.js — Mobile-first Mission Control

const API_BASE = '/api';

let currentTypeFilter = '';
let currentSortScore = '';

// ── Auth ─────────────────────────────────────────────────────────────────────
function getToken() { return localStorage.getItem('sentinel_token'); }
function setToken(t) { localStorage.setItem('sentinel_token', t); }
function logout() {
    localStorage.removeItem('sentinel_token');
    window.location.href = '/login.html';
}

// ── Format helpers ────────────────────────────────────────────────────────────
function formatCurrency(val) {
    if (val == null) return '—';
    return '$' + val.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}
function formatPercent(val) {
    if (val == null) return '—';
    return val.toFixed(1) + '%';
}
function formatDate(isoStr) {
    if (!isoStr) return '—';
    const d = new Date(isoStr);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}
function formatAge(isoStr) {
    if (!isoStr) return '—';
    const d = new Date(isoStr);
    const diff = Math.floor((Date.now() - d.getTime()) / 60000);
    if (diff < 1) return 'now';
    if (diff < 60) return diff + 'm';
    if (diff < 1440) return Math.floor(diff / 60) + 'h';
    return Math.floor(diff / 1440) + 'd';
}
function scoreColor(score) {
    if (score == null) return 'chip-neutral';
    if (score >= 85) return 'chip-green';
    if (score >= 60) return 'chip-yellow';
    return 'chip-neutral';
}

// ── API ───────────────────────────────────────────────────────────────────────
async function fetchApi(endpoint, opts = {}) {
    const token = getToken();
    const headers = { 'Content-Type': 'application/json', ...opts.headers };
    if (token) headers['Authorization'] = `Bearer ${token}`;
    try {
        const res = await fetch(`${API_BASE}${endpoint}`, { ...opts, headers });
        if (res.status === 401 && !location.pathname.includes('login')) { logout(); return null; }
        return await res.json();
    } catch (e) { console.error('API Error:', e); return null; }
}

// ── Health ────────────────────────────────────────────────────────────────────
async function updateHealth() {
    const [data, uw] = await Promise.all([fetchApi('/health'), fetchApi('/uw/status')]);
    if (data?.ok) {
        const el = document.getElementById('health-ts');
        if (el) el.textContent = `Updated ${formatDate(new Date())}`;
    }
    const badge = document.getElementById('uw-badge');
    if (badge) {
        if (uw && !uw.enabled) badge.classList.remove('hidden');
        else badge.classList.add('hidden');
    }
}

// ── Viewport helpers ──────────────────────────────────────────────────────────
function isMobile() { return window.innerWidth <= 430; }

// ── Sparkline Helper ──────────────────────────────────────────────────────────
function generateSparklineSVG(history, w = 90, h = 24) {
    if (!history || history.length < 2) return '';
    const points = history.slice(-40);
    const min = Math.min(...points);
    const max = Math.max(...points);
    const range = max - min;
    const padding = 2;
    const effH = h - (padding * 2);

    let path = '';
    points.forEach((val, i) => {
        const x = (i / (points.length - 1)) * w;
        let y = h / 2;
        if (range > 0) y = padding + effH - (((val - min) / range) * effH);
        path += `${i === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)} `;
    });

    const first = points[0];
    const last = points[points.length - 1];
    let color = '#94a3b8';
    let caret = '';
    if (last > first) { color = '#34d399'; caret = '▲'; }
    else if (last < first) { color = '#f87171'; caret = '▼'; }

    return `
        <div style="display:flex;align-items:center;gap:4px;width:100%" title="Current: ${last.toFixed(1)} | Initial: ${first.toFixed(1)}">
            <svg width="100%" height="${h}" viewBox="0 0 ${w} ${h}" fill="none" style="overflow:visible;flex:1">
                <path d="${path}" stroke="${color}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>
            </svg>
            <span style="font-size:9px;color:${color}">${caret}</span>
        </div>
    `;
}

// ── AOI toggle ────────────────────────────────────────────────────────────────
async function toggleAOI(contractKey, currentActive) {
    if (!contractKey) return;
    const btn = document.querySelector(`[data-ck="${CSS.escape(contractKey)}"]`);
    if (btn) { btn.disabled = true; btn.style.opacity = '0.5'; }
    await fetchApi('/watchlist/toggle', {
        method: 'POST',
        body: JSON.stringify({ contract_key: contractKey, is_active: currentActive ? 0 : 1 })
    });
    if (location.pathname.includes('monitor.html')) loadMonitors(true);
    else loadAlerts(true);
}

// ── Contract Detail Popup ─────────────────────────────────────────────────────
let _detailItem = null;

function ds(label, val) {
    return `<div class="detail-stat"><span class="ds-label">${label}</span><span class="ds-val">${val}</span></div>`;
}

function openDetailPopup(item) {
    _detailItem = item;
    const typeColor = item.opt_type === 'C' ? '#34d399' : '#f87171';
    const typeLabel = item.opt_type === 'C' ? 'CALL' : 'PUT';

    // Header
    document.getElementById('detail-title').innerHTML = `
        <span class="detail-ticker">${item.ticker}</span>
        <span class="chip" style="background:${item.opt_type === 'C' ? 'rgba(52,211,153,.12)' : 'rgba(248,113,113,.12)'};
              border-color:${item.opt_type === 'C' ? 'rgba(52,211,153,.3)' : 'rgba(248,113,113,.3)'};
              color:${typeColor}">${typeLabel}</span>
        <span style="font-size:16px;font-weight:700;color:${typeColor}">$${item.strike}</span>
        ${item.score_total != null ? `<span class="chip ${scoreColor(item.score_total)}">${item.score_total.toFixed(1)}</span>` : ''}
    `;

    // Stats grid
    const volOI = item.volume && item.oi ? (item.volume / item.oi).toFixed(1) + 'x' : '—';
    document.getElementById('detail-grid').innerHTML =
        ds('Expiry', item.exp || '—') +
        ds('DTE', item.dte != null ? item.dte + 'd' : '—') +
        ds('Premium', formatCurrency(item.premium)) +
        ds('Size', item.size || '—') +
        ds('Volume', item.volume != null ? item.volume.toLocaleString() : '—') +
        ds('Open Int', item.oi != null ? item.oi.toLocaleString() : '—') +
        ds('Vol/OI', volOI) +
        ds('Spot', formatCurrency(item.spot)) +
        ds('OTM %', formatPercent(item.otm_pct)) +
        ds('Bid', item.bid != null ? '$' + item.bid.toFixed(2) : '—') +
        ds('Ask', item.ask != null ? '$' + item.ask.toFixed(2) : '—') +
        ds('Spread %', formatPercent(item.spread_pct)) +
        ds('Time', formatDate(item.ts));

    // Tags
    const tags = (item.tags || '').split(',').filter(Boolean);
    document.getElementById('detail-tags').innerHTML = tags.length
        ? tags.map(t => `<span class="chip chip-neutral">${t.trim()}</span>`).join('')
        : '';

    // CTA button
    _refreshDetailCta(!!item.is_aoi);

    // Show
    document.getElementById('detail-backdrop').classList.add('open');
    document.body.style.overflow = 'hidden';
}

function _refreshDetailCta(isAoi) {
    const btn = document.getElementById('detail-cta');
    if (!btn) return;
    btn.disabled = false;
    btn.style.opacity = '';
    if (isAoi) {
        btn.className = 'detail-cta cta-remove';
        btn.innerHTML = `<span class="material-symbols-outlined">remove_circle</span> Remove from Monitor`;
    } else {
        btn.className = 'detail-cta cta-add';
        btn.innerHTML = `<span class="material-symbols-outlined">add_circle</span> Add to Monitor`;
    }
}

async function detailCtaClick() {
    if (!_detailItem) return;
    const btn = document.getElementById('detail-cta');
    const wasAoi = !!_detailItem.is_aoi;
    btn.disabled = true;
    btn.style.opacity = '0.6';
    await fetchApi('/watchlist/toggle', {
        method: 'POST',
        body: JSON.stringify({ contract_key: _detailItem.contract_key, is_active: wasAoi ? 0 : 1 })
    });
    _detailItem.is_aoi = wasAoi ? 0 : 1;
    _refreshDetailCta(!wasAoi);
    loadAlerts(true);
}

function closeDetailPopup() {
    document.getElementById('detail-backdrop').classList.remove('open');
    document.body.style.overflow = '';
    _detailItem = null;
}

// ── Alert card (mobile) ───────────────────────────────────────────────────────
function renderAlertCard(item) {
    const typeColor = item.opt_type === 'C' ? 'text-emerald-400' : 'text-rose-400';
    const typeBg = item.opt_type === 'C' ? 'bg-emerald-500/10 border-emerald-500/20 text-emerald-400'
        : 'bg-rose-500/10 border-rose-500/20 text-rose-400';
    const tags = (item.tags || '').split(',').filter(Boolean)
        .map(t => `<span class="chip chip-neutral">${t.trim()}</span>`).join('');
    const isAoi = !!item.is_aoi;
    const trackBtn = isAoi
        ? `<button class="btn-track btn-track-active" data-ck="${item.contract_key}" onclick="event.stopPropagation();toggleAOI('${item.contract_key}',1)" title="Remove from Watchlist">
               <span class="material-symbols-outlined">star</span>
           </button>`
        : `<button class="btn-track" data-ck="${item.contract_key}" onclick="event.stopPropagation();toggleAOI('${item.contract_key}',0)" title="Add to Watchlist">
               <span class="material-symbols-outlined">star</span>
           </button>`;
    const aoiChip = isAoi ? `<span class="chip chip-blue">AOI</span>` : '';

    const el = document.createElement('div');
    el.className = 'alert-card has-detail';
    if (item.score_total >= 90) el.classList.add('alert-card-hot');
    el.innerHTML = `
        <div class="alert-card-header">
            <div class="alert-card-contract">
                <span class="alert-ticker">${item.ticker}</span>
                <span class="chip ${typeBg}">${item.opt_type === 'C' ? 'CALL' : 'PUT'}</span>
                <span class="alert-strike ${typeColor}">$${item.strike}</span>
                ${aoiChip}
            </div>
            <div class="alert-card-right">
                <span class="chip ${scoreColor(item.score_total)}">${item.score_total != null ? item.score_total.toFixed(1) : '—'}</span>
                ${trackBtn}
            </div>
        </div>
        <div class="alert-card-meta">
            <span>${item.exp || '—'}</span>
            <span class="dot">·</span>
            <span>${item.dte != null ? item.dte + 'd' : '—'}</span>
            <span class="dot">·</span>
            <span class="text-slate-500" title="Ingested: ${formatAge(item.ingested_at || item.ts)} ago">${formatDate(item.ts)}</span>
        </div>
        <div class="alert-card-row1">
            <div class="stat-cell">
                <span class="stat-label">Premium</span>
                <span class="stat-val">${formatCurrency(item.premium)}</span>
            </div>
            <div class="stat-cell">
                <span class="stat-label">Size</span>
                <span class="stat-val">${item.size || '—'}</span>
            </div>
            <div class="stat-cell">
                <span class="stat-label">Vol/OI</span>
                <span class="stat-val">${item.volume && item.oi ? (item.volume / item.oi).toFixed(1) + 'x' : '—'}</span>
            </div>
        </div>
        <div class="alert-card-row2">
            <div class="stat-cell">
                <span class="stat-label">Spread</span>
                <span class="stat-val">${formatPercent(item.spread_pct)}</span>
            </div>
            <div class="stat-cell">
                <span class="stat-label">OTM</span>
                <span class="stat-val">${formatPercent(item.otm_pct)}</span>
            </div>
            <div class="stat-cell">
                <span class="stat-label">Spot</span>
                <span class="stat-val">${formatCurrency(item.spot)}</span>
            </div>
        </div>
        ${tags ? `<div class="alert-card-tags">${tags}</div>` : ''}
        <div style="font-size:10px;color:var(--muted);text-align:right;margin-top:2px">Tap for details →</div>
    `;
    el.addEventListener('click', () => openDetailPopup(item));
    return el;
}

// ── Alert row (desktop table) ─────────────────────────────────────────────────
function renderAlertRow(item) {
    const typeColor = item.opt_type === 'C' ? 'text-emerald-400' : 'text-rose-400';
    const isAoi = !!item.is_aoi;
    const tags = (item.tags || '').split(',').filter(Boolean)
        .map(t => `<span class="chip chip-neutral">${t.trim()}</span>`).join('');
    const aoiChip = isAoi ? `<span class="chip chip-blue ml-1">AOI</span>` : '';
    const tr = document.createElement('tr');
    tr.className = 'tbl-row has-detail';
    if (item.score_total >= 90) tr.classList.add('bg-primary/5');
    tr.innerHTML = `
        <td class="px-4 py-3 text-sm text-slate-400 whitespace-nowrap">
            <div>${formatDate(item.ts)}</div>
            <div class="text-[10px] text-slate-500 mt-0.5">${formatAge(item.ingested_at || item.ts)}</div>
        </td>
        <td class="px-4 py-3 text-sm font-bold text-white tracking-wide">${item.ticker}${aoiChip}</td>
        <td class="px-4 py-3 text-sm font-medium ${typeColor} whitespace-nowrap">$${item.strike}${item.opt_type} ${item.exp}</td>
        <td class="px-4 py-3 text-sm font-bold text-white text-right">${formatCurrency(item.premium)}</td>
        <td class="px-4 py-3 text-sm text-slate-300 text-right">${item.size || '—'}</td>
        <td class="px-4 py-3 text-sm text-slate-300 text-right">${item.volume && item.oi ? (item.volume / item.oi).toFixed(1) + 'x' : '—'}</td>
        <td class="px-4 py-3 text-sm text-slate-300 text-right">${formatPercent(item.spread_pct)}</td>
        <td class="px-4 py-3 text-sm text-slate-300 text-right">${formatPercent(item.otm_pct)}</td>
        <td class="px-4 py-3 text-center">
            <span class="chip ${scoreColor(item.score_total)}">${item.score_total != null ? item.score_total.toFixed(1) : '—'}</span>
        </td>
        <td class="px-4 py-3"><div class="flex gap-1 flex-wrap max-w-[140px]">${tags}</div></td>
        <td class="px-4 py-3 text-right">
            <button class="btn-track ${isAoi ? 'btn-track-active' : ''}" data-ck="${item.contract_key}"
                onclick="event.stopPropagation();toggleAOI('${item.contract_key}',${isAoi ? 1 : 0})" title="${isAoi ? 'Remove' : 'Watch'}">
                <span class="material-symbols-outlined">star</span>
            </button>
        </td>
    `;
    tr.addEventListener('click', () => openDetailPopup(item));
    return tr;
}

// ── Load Alerts ───────────────────────────────────────────────────────────────
async function loadAlerts(isBackground = false) {
    // Detect which container exists
    const cardsWrap = document.getElementById('alerts-cards');
    const tbody = document.querySelector('#alerts-table tbody');
    if (!cardsWrap && !tbody) return;
    if (!location.pathname.includes('index.html') && location.pathname !== '/') return;

    const loadingHtml = '<p class="empty-state">Loading alerts…</p>';
    const errorHtml = '<p class="empty-state text-rose-400">Error loading alerts</p>';

    if (!isBackground) {
        if (cardsWrap) cardsWrap.innerHTML = loadingHtml;
        if (tbody) tbody.innerHTML = `<tr><td colspan="11" class="text-center py-8 text-slate-400">Loading alerts…</td></tr>`;
    }

    const params = new URLSearchParams({ limit: 50 });
    const sym = document.getElementById('filter-symbol')?.value;
    if (sym) params.append('symbol', sym);
    if (currentTypeFilter) params.append('type', currentTypeFilter);
    if (currentSortScore) params.append('sort_score', currentSortScore);

    const data = await fetchApi(`/alerts?${params}`);
    if (!data?.items) {
        if (cardsWrap) cardsWrap.innerHTML = errorHtml;
        if (tbody) tbody.innerHTML = `<tr><td colspan="11" class="text-center py-8 text-rose-400">Error loading alerts</td></tr>`;
        return;
    }

    // Cards (mobile)
    if (cardsWrap) {
        if (!data.items.length) {
            cardsWrap.innerHTML = '<p class="empty-state">No alerts match your filters</p>';
        } else {
            cardsWrap.innerHTML = '';
            data.items.forEach(item => cardsWrap.appendChild(renderAlertCard(item)));
        }
    }

    // Table (desktop)
    if (tbody) {
        if (!data.items.length) {
            tbody.innerHTML = `<tr><td colspan="11" class="text-center py-10 text-slate-500">No alerts match your filters</td></tr>`;
        } else {
            tbody.innerHTML = '';
            data.items.forEach(item => tbody.appendChild(renderAlertRow(item)));
        }
    }
}

// ── Monitor Detail Popup ──────────────────────────────────────────────────────
function openMonitorDetailPopup(item) {
    const typeColor = item.opt_type === 'C' ? '#34d399' : '#f87171';
    const typeLabel = item.opt_type === 'C' ? 'CALL' : 'PUT';

    document.getElementById('detail-title').innerHTML = `
        <span class="detail-ticker">${item.ticker}</span>
        <span class="chip" style="background:${item.opt_type === 'C' ? 'rgba(52,211,153,.12)' : 'rgba(248,113,113,.12)'};
              border-color:${item.opt_type === 'C' ? 'rgba(52,211,153,.3)' : 'rgba(248,113,113,.3)'};
              color:${typeColor}">${typeLabel}</span>
        <span style="font-size:16px;font-weight:700;color:${typeColor}">$${item.strike}</span>
    `;

    document.getElementById('detail-grid').innerHTML =
        ds('Entry Score', item.entry_score != null ? item.entry_score.toFixed(1) : '—') +
        ds('Peak Score', item.peak_score != null ? item.peak_score.toFixed(1) : '—') +
        ds('Current Score', item.current_score != null ? item.current_score.toFixed(1) : '—');

    const trendWrap = document.getElementById('detail-sparkline-wrap');
    const trendCont = document.getElementById('detail-sparkline-cont');

    if (item.score_history && item.score_history.length > 0) {
        trendWrap.style.display = 'block';

        let histHtml = '<div style="margin-top:16px;font-size:11px;color:var(--muted);text-transform:uppercase;font-weight:700;letter-spacing:0.06em">Last 10 Ticks</div><div style="margin-top:8px;display:flex;flex-direction:column;gap:4px;">';
        const recent = [...item.score_history].slice(-10).reverse();
        recent.forEach((v, idx) => {
            histHtml += `<div style="display:flex;justify-content:space-between;padding:6px 12px;background:var(--surface2);border-radius:6px;font-size:12px;color:#fff;">
                <span style="color:var(--muted)">t-${idx}</span>
                <span>${v.toFixed(1)}</span>
            </div>`;
        });
        histHtml += '</div>';

        trendCont.innerHTML = generateSparklineSVG(item.score_history, 450, 80) + histHtml;
        trendCont.style.flexDirection = 'column';
        trendCont.style.alignItems = 'stretch';
    } else {
        trendWrap.style.display = 'none';
        trendCont.innerHTML = '';
    }

    document.getElementById('detail-backdrop').classList.add('open');
    document.body.style.overflow = 'hidden';
}

// ── Monitor card (mobile) ─────────────────────────────────────────────────────
function renderMonitorCard(item) {
    const statusClass = (item.status === 'ACTIVE' || item.status === 'Monitor')
        ? 'chip chip-blue' : 'chip chip-yellow';
    const deltaColor = item.delta_from_peak < 0 ? 'text-rose-400' : 'text-emerald-400';

    const el = document.createElement('div');
    el.className = 'monitor-card has-detail';
    el.onclick = (e) => {
        if (!e.target.closest('button')) openMonitorDetailPopup(item);
    };
    el.innerHTML = `
        <div class="monitor-card-header">
            <div class="flex items-center gap-2">
                <div class="ticker-icon">${(item.ticker || '??').substring(0, 2)}</div>
                <div>
                    <div class="monitor-ticker">${item.ticker}</div>
                    <div class="monitor-contract">$${item.strike}${item.opt_type} · ${item.exp}</div>
                </div>
            </div>
            <div class="flex items-center gap-2">
                <span class="${statusClass}">${item.status || 'TRACKING'}</span>
                <button class="btn-remove" onclick="toggleAOI('${item.contract_key}',1)" title="Remove">
                    <span class="material-symbols-outlined text-[18px]">close</span>
                </button>
            </div>
        </div>
        <div class="monitor-scores">
            <div class="stat-cell">
                <span class="stat-label">Entry</span>
                <span class="stat-val">${item.entry_score != null ? item.entry_score.toFixed(1) : '—'}</span>
            </div>
            <div class="stat-cell">
                <span class="stat-label">Current</span>
                <span class="stat-val text-white font-bold">${item.current_score != null ? item.current_score.toFixed(1) : '—'}</span>
            </div>
            <div class="stat-cell">
                <span class="stat-label">Peak</span>
                <span class="stat-val">${item.peak_score != null ? item.peak_score.toFixed(1) : '—'}</span>
            </div>
            <div class="stat-cell">
                <span class="stat-label">Δ Peak</span>
                <span class="stat-val ${deltaColor}">${item.delta_from_peak != null ? item.delta_from_peak : '—'}</span>
            </div>
        </div>
        <div class="monitor-card-footer flex justify-between items-end mt-1">
            <span class="text-[11px] text-slate-500">Updated ${formatDate(item.last_update_ts)}</span>
            <div class="sparkline-mini" style="height:22px;">
                ${generateSparklineSVG(item.score_history, 70, 22)}
            </div>
        </div>
    `;
    return el;
}

// ── Monitor row (desktop table) ───────────────────────────────────────────────
function renderMonitorRow(item) {
    const statusClass = (item.status === 'ACTIVE' || item.status === 'Monitor')
        ? 'chip chip-blue' : 'chip chip-yellow';
    const deltaColor = item.delta_from_peak < 0 ? 'text-rose-400' : 'text-emerald-400';
    const tr = document.createElement('tr');
    tr.className = 'tbl-row border-b border-slate-800/50 has-detail';
    tr.onclick = (e) => {
        if (!e.target.closest('button')) openMonitorDetailPopup(item);
    };
    tr.innerHTML = `
        <td class="px-5 py-4">
            <div class="flex items-center gap-2">
                <div class="ticker-icon">${(item.ticker || '??').substring(0, 2)}</div>
                <div>
                    <span class="text-sm font-bold text-white">${item.ticker}</span>
                    <span class="text-xs text-slate-400 ml-1">$${item.strike}${item.opt_type} ${item.exp}</span>
                </div>
            </div>
        </td>
        <td class="px-5 py-4 text-sm tabular-nums text-slate-400">${item.entry_score != null ? item.entry_score.toFixed(2) : '—'}</td>
        <td class="px-5 py-4 text-sm font-bold text-white tabular-nums">${item.current_score != null ? item.current_score.toFixed(2) : '—'}</td>
        <td class="px-5 py-4 text-sm tabular-nums text-slate-400">${item.peak_score != null ? item.peak_score.toFixed(2) : '—'}</td>
        <td class="px-5 py-4 text-sm tabular-nums ${deltaColor}">${item.delta_from_peak != null ? item.delta_from_peak : '—'}</td>
        <td class="px-5 py-4"><div class="sparkline-mini">${generateSparklineSVG(item.score_history, 90, 24)}</div></td>
        <td class="px-5 py-4"><span class="${statusClass}">${item.status || 'TRACKING'}</span></td>
        <td class="px-5 py-4 text-xs text-slate-400">${formatDate(item.last_update_ts)}</td>
        <td class="px-5 py-4 text-right">
            <button class="btn-remove" onclick="toggleAOI('${item.contract_key}',1)" title="Remove from Monitor">
                <span class="material-symbols-outlined text-[18px]">close</span>
            </button>
        </td>
    `;
    return tr;
}

// ── Load Monitors ─────────────────────────────────────────────────────────────
async function loadMonitors(isBackground = false) {
    const cardsWrap = document.getElementById('monitor-cards');
    const tbody = document.getElementById('monitor-tbody');
    if (!cardsWrap && !tbody) return;
    if (!location.pathname.includes('monitor.html')) return;

    const emptyHtml = `
        <div class="empty-state-block">
            <span class="material-symbols-outlined text-4xl text-slate-600 mb-3">monitoring</span>
            <p class="text-slate-400 font-medium">No contracts monitored</p>
            <p class="text-xs text-slate-500 mt-1">Star an alert to track it here</p>
        </div>`;

    const errHtml = '<p class="empty-state text-rose-400">Failed to load monitors</p>';

    if (!isBackground) {
        if (cardsWrap) cardsWrap.innerHTML = '<p class="empty-state">Loading…</p>';
        if (tbody) tbody.innerHTML = `<tr><td colspan="9" class="text-center py-8 text-slate-400">Loading…</td></tr>`;
    }

    const data = await fetchApi('/monitors');
    if (!data?.items) {
        if (cardsWrap) cardsWrap.innerHTML = errHtml;
        if (tbody) tbody.innerHTML = `<tr><td colspan="9" class="text-center py-8 text-rose-400">Failed to load</td></tr>`;
        return;
    }

    if (!data.items.length) {
        if (cardsWrap) cardsWrap.innerHTML = emptyHtml;
        if (tbody) tbody.innerHTML = `<tr><td colspan="9" class="py-12">${emptyHtml}</td></tr>`;
        return;
    }

    // Cards
    if (cardsWrap) {
        cardsWrap.innerHTML = '';
        data.items.forEach(item => cardsWrap.appendChild(renderMonitorCard(item)));
    }
    // Table
    if (tbody) {
        tbody.innerHTML = '';
        data.items.forEach(item => tbody.appendChild(renderMonitorRow(item)));
    }
}

// ── Recent Alerts Panel (sidebar) ─────────────────────────────────────────────
async function loadTopRecent(isBackground = false) {
    const listEl = document.getElementById('recent-alerts-list');
    if (!listEl) return;
    if (!isBackground) listEl.innerHTML = '<div class="p-4 text-sm text-slate-400 text-center">Loading…</div>';

    const data = await fetchApi('/alerts/recent?window_sec=900&limit=5');
    if (!data?.items?.length) {
        listEl.innerHTML = '<div class="p-4 text-sm text-slate-500 text-center">No recent significant alerts</div>';
        return;
    }
    listEl.innerHTML = '';
    data.items.forEach(item => {
        const d = document.createElement('div');
        d.className = 'recent-alert-item has-detail';
        d.title = 'Click to view details';
        d.innerHTML = `
            <div>
                <div class="font-bold text-white text-sm">${item.ticker}
                    <span class="${item.opt_type === 'C' ? 'text-emerald-400' : 'text-rose-400'}">${item.opt_type}</span>
                </div>
                <div class="text-xs text-slate-500">${formatCurrency(item.premium)}</div>
            </div>
            <div class="text-right">
                <div class="text-sm font-bold text-primary">${item.score_total.toFixed(1)}</div>
                <div class="text-[10px] text-slate-500">${formatDate(item.ts)}</div>
            </div>
        `;
        d.addEventListener('click', () => openDetailPopup(item));
        listEl.appendChild(d);
    });
}

// ── Filter drawer toggle ──────────────────────────────────────────────────────
function initFilterDrawer() {
    const toggle = document.getElementById('filter-toggle');
    const drawer = document.getElementById('filter-drawer');
    if (!toggle || !drawer) return;
    toggle.addEventListener('click', () => {
        const open = drawer.classList.toggle('drawer-open');
        toggle.setAttribute('aria-expanded', open);
    });
}

// ── Market Tide (MAX) ─────────────────────────────────────────────────────────
async function loadMarketTide(isBackground = false) {
    if (!location.pathname.includes('market_tide.html')) return;

    const disabledEl = document.getElementById('tide-disabled');
    const activeEl = document.getElementById('tide-active');
    if (!disabledEl || !activeEl) return;

    if (!isBackground) {
        document.getElementById('regime-label').textContent = 'Loading...';
    }

    const status = await fetchApi('/uw/status');
    const isReady = status && status.enabled;

    if (!isReady) {
        activeEl.classList.add('hidden');
        disabledEl.classList.remove('hidden');
        return;
    }

    activeEl.classList.remove('hidden');
    disabledEl.classList.add('hidden');

    const [tide1m, tide5m] = await Promise.all([
        fetchApi('/uw/tide/latest?interval=1m'),
        fetchApi('/uw/tide/latest?interval=5m')
    ]);

    const hasData = (tide1m?.items?.length > 0) || (tide5m?.items?.length > 0);
    const container = document.getElementById('tide-series-container');

    if (!hasData) {
        if (container) {
            container.innerHTML = `
                <div class="flex flex-col items-center">
                    <span class="material-symbols-outlined text-4xl text-primary mb-2">info</span>
                    <span>No tide data yet</span>
                </div>
            `;
        }
    } else {
        if (container) {
            container.innerHTML = `
                <div class="flex flex-col items-center">
                    <span class="material-symbols-outlined text-4xl text-primary mb-2">monitoring</span>
                    <span>Data Available: 1m (${tide1m?.items?.length || 0}) 5m (${tide5m?.items?.length || 0})</span>
                </div>
            `;
        }
    }
}

// ── DOMContentLoaded ──────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
    // Auth gate
    if (!getToken() && !location.pathname.includes('login')) {
        location.href = '/login.html';
        return;
    }

    // Inject JWT user info into header
    const token = getToken();
    if (token) {
        try {
            const payload = JSON.parse(atob(token.split('.')[1]));
            document.querySelectorAll('.user-email').forEach(el => el.textContent = payload.sub || '');
            document.querySelectorAll('.user-role').forEach(el => {
                el.textContent = (payload.role || '').toUpperCase();
                if (payload.role === 'sentinel') el.classList.add('chip-blue');
            });
        } catch (_) { }
    }

    // Login form
    const loginForm = document.getElementById('login-form');
    if (loginForm) {
        loginForm.addEventListener('submit', async e => {
            e.preventDefault();
            const btn = loginForm.querySelector('button[type=submit]');
            btn.disabled = true;
            btn.textContent = 'Signing in…';
            const email = document.getElementById('email').value.trim();
            const password = document.getElementById('password').value;
            try {
                const res = await fetch(`${API_BASE}/auth/login`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ email, password })
                });
                const data = await res.json();
                if (data.ok && data.token) {
                    setToken(data.token);
                    location.href = '/';
                } else {
                    showLoginError(data.detail || 'Invalid credentials');
                    btn.disabled = false; btn.textContent = 'Sign In';
                }
            } catch (_) {
                showLoginError('Connection error. Try again.');
                btn.disabled = false; btn.textContent = 'Sign In';
            }
        });
    }

    // Password visibility toggle
    const pwToggle = document.getElementById('pw-toggle');
    const pwInput = document.getElementById('password');
    if (pwToggle && pwInput) {
        pwToggle.addEventListener('click', () => {
            const isText = pwInput.type === 'text';
            pwInput.type = isText ? 'password' : 'text';
            pwToggle.querySelector('span').textContent = isText ? 'visibility' : 'visibility_off';
        });
    }

    // Logout buttons
    document.querySelectorAll('.logout-btn').forEach(btn => btn.addEventListener('click', logout));

    // Detail popup close
    document.getElementById('detail-close')?.addEventListener('click', closeDetailPopup);
    document.getElementById('detail-backdrop')?.addEventListener('click', (e) => {
        if (e.target === e.currentTarget) closeDetailPopup();
    });
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') closeDetailPopup();
    });
    // Filter drawer
    initFilterDrawer();

    // Ticker search
    const filterInput = document.getElementById('filter-symbol');
    if (filterInput) {
        let debounce;
        filterInput.addEventListener('input', () => {
            clearTimeout(debounce);
            debounce = setTimeout(() => loadAlerts(false), 400);
        });
    }

    // Type filter (Calls / Puts / All)
    const typeBtns = document.querySelectorAll('.type-filter-btn');
    typeBtns.forEach(btn => {
        btn.addEventListener('click', () => {
            typeBtns.forEach(b => {
                b.classList.remove('active');
                b.setAttribute('aria-pressed', 'false');
            });
            btn.classList.add('active');
            btn.setAttribute('aria-pressed', 'true');
            currentTypeFilter = btn.dataset.type;
            loadAlerts(false);
        });
    });

    // Score sort (desktop th)
    const thScore = document.getElementById('th-score');
    const mobileSortScore = document.getElementById('mobile-sort-score');

    function refreshSortUI() {
        // Desktop th sort UI
        const icon = document.getElementById('sort-icon-score');
        if (icon) {
            const map = { '': 'swap_vert', 'desc': 'arrow_downward', 'asc': 'arrow_upward' };
            icon.textContent = map[currentSortScore];
            icon.className = `material-symbols-outlined text-[14px] ${currentSortScore ? 'text-primary opacity-100' : 'opacity-0 group-hover:opacity-50'}`;
        }
        // Mobile button sort UI
        if (mobileSortScore) {
            if (currentSortScore === 'desc') {
                mobileSortScore.classList.add('active');
                mobileSortScore.setAttribute('aria-pressed', 'true');
            } else {
                mobileSortScore.classList.remove('active');
                mobileSortScore.setAttribute('aria-pressed', 'false');
            }
        }
    }

    if (thScore) {
        thScore.addEventListener('click', () => {
            currentSortScore = currentSortScore === '' ? 'desc' : currentSortScore === 'desc' ? 'asc' : '';
            refreshSortUI();
            loadAlerts(false);
        });
    }

    if (mobileSortScore) {
        mobileSortScore.addEventListener('click', () => {
            // Mobile toggle: Default -> Score DESC -> Default
            currentSortScore = currentSortScore === 'desc' ? '' : 'desc';
            refreshSortUI();
            loadAlerts(false);
        });
    }

    // Initial load
    updateHealth();
    loadAlerts(false);
    loadMonitors(false);
    loadTopRecent(false);
    loadMarketTide(false);

    // Auto-refresh every 5s
    setInterval(() => {
        updateHealth();
        loadAlerts(true);
        loadMonitors(true);
        loadTopRecent(true);
        loadMarketTide(true);
    }, 5000);
});

function showLoginError(msg) {
    let el = document.getElementById('login-error');
    if (!el) {
        el = document.createElement('p');
        el.id = 'login-error';
        el.className = 'login-error';
        const form = document.getElementById('login-form');
        form?.prepend(el);
    }
    el.textContent = msg;
}
