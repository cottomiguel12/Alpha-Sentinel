// dashboard/app.js

const API_BASE = '/api';

// Utilities
function getToken() {
    return localStorage.getItem('sentinel_token');
}

function setToken(token) {
    localStorage.setItem('sentinel_token', token);
}

function logout() {
    localStorage.removeItem('sentinel_token');
    window.location.href = '/login.html';
}

function formatCurrency(val) {
    if (val === null || val === undefined) return '-';
    if (val >= 1000000) return '$' + (val / 1000000).toFixed(1) + 'M';
    if (val >= 1000) return '$' + (val / 1000).toFixed(1) + 'K';
    return '$' + val.toFixed(2);
}

function formatPercent(val) {
    if (val === null || val === undefined) return '-';
    return val.toFixed(1) + '%';
}

function formatDate(isoStr) {
    if (!isoStr) return '-';
    const d = new Date(isoStr);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

// Fetch helper
async function fetchApi(endpoint, options = {}) {
    const token = getToken();
    const headers = {
        'Content-Type': 'application/json',
        ...options.headers
    };
    if (token) {
        headers['Authorization'] = `Bearer ${token}`;
    }

    try {
        const res = await fetch(`${API_BASE}${endpoint}`, { ...options, headers });
        if (res.status === 401 && !window.location.pathname.includes('login')) {
            logout();
            return null;
        }
        const data = await res.json();
        return data;
    } catch (err) {
        console.error('API Error:', err);
        return null;
    }
}

// Health Check
async function updateHealth() {
    const data = await fetchApi('/health');
    if (data && data.ok) {
        // Update any health indicators
        const healthEl = document.getElementById('health-ts');
        if (healthEl) healthEl.textContent = `Last update: ${formatDate(new Date())}`;
    }
}

// Load Alerts
async function loadAlerts(isBackground = false) {
    const tbody = document.querySelector('tbody');
    if (!tbody) return;

    // Check if we are on Alerts page
    if (!window.location.pathname.includes('index.html') && window.location.pathname !== '/') return;

    if (!isBackground) {
        tbody.innerHTML = '<tr><td colspan="11" class="text-center py-4 text-slate-400">Loading alerts...</td></tr>';
    }

    try {
        const urlParams = new URLSearchParams();
        urlParams.append('limit', '50');

        // basic filters logic
        const symbolInput = document.getElementById('filter-symbol');
        if (symbolInput && symbolInput.value) urlParams.append('symbol', symbolInput.value);

        const data = await fetchApi(`/alerts?${urlParams.toString()}`);
        if (!data || !data.items) {
            tbody.innerHTML = '<tr><td colspan="11" class="text-center py-4 text-rose-400">Error loading alerts</td></tr>';
            return;
        }

        tbody.innerHTML = '';
        data.items.forEach(item => {
            const tr = document.createElement('tr');
            tr.className = 'hover:bg-slate-800/30 transition-colors group';
            if (item.score_total > 90) tr.classList.add('bg-primary/5');

            tr.innerHTML = `
                <td class="px-4 py-3 text-sm text-slate-400">${formatDate(item.ts)}</td>
                <td class="px-4 py-3 text-sm font-bold text-white tracking-wide">${item.ticker}</td>
                <td class="px-4 py-3 text-sm font-medium ${item.opt_type === 'C' ? 'text-emerald-400' : 'text-rose-400'}">
                    $${item.strike}${item.opt_type} ${item.exp}
                </td>
                <td class="px-4 py-3 text-sm font-bold text-white text-right">${formatCurrency(item.premium)}</td>
                <td class="px-4 py-3 text-sm text-slate-300 text-right">${item.size || '-'}</td>
                <td class="px-4 py-3 text-sm text-slate-300 text-right">${item.volume ? (item.volume / (item.oi || 1)).toFixed(1) + 'x' : '-'}</td>
                <td class="px-4 py-3 text-sm text-slate-300 text-right">${formatPercent(item.spread_pct)}</td>
                <td class="px-4 py-3 text-sm text-slate-300 text-right">${formatPercent(item.otm_pct)}</td>
                <td class="px-4 py-3 text-center">
                    <span class="inline-flex items-center justify-center rounded ${item.score_total > 85 ? 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30' : 'bg-slate-700 text-slate-300 border-slate-600'} px-2 py-0.5 text-xs font-bold border">
                        ${item.score_total ? item.score_total.toFixed(1) : '-'}
                    </span>
                </td>
                <td class="px-4 py-3">
                    <div class="flex gap-1 flex-wrap w-32">
                        ${(item.tags || '').split(',').map(t => t ? `<span class="rounded bg-slate-800 px-1.5 py-0.5 text-[10px] font-bold text-slate-300">${t}</span>` : '').join('')}
                    </div>
                </td>
                <td class="px-4 py-3 text-right">
                    <button class="material-symbols-outlined text-${item.is_aoi ? 'primary' : 'slate-600'} hover:text-primary transition-colors" onclick="toggleAOI('${item.contract_key}', ${item.is_aoi})">star</button>
                </td>
            `;
            tbody.appendChild(tr);
        });
    } catch (e) {
        tbody.innerHTML = '<tr><td colspan="11" class="text-center py-4 text-rose-400">Error rendering alerts</td></tr>';
    }
}

// Load Monitors
async function loadMonitors(isBackground = false) {
    const tbody = document.querySelector('#monitor-tbody');
    if (!tbody) return;

    if (!window.location.pathname.includes('monitor.html')) return;

    if (!isBackground) {
        tbody.innerHTML = '<tr><td colspan="9" class="text-center py-4 text-slate-400">Loading monitors...</td></tr>';
    }

    const data = await fetchApi('/monitors');
    if (!data || !data.items) {
        tbody.innerHTML = '<tr><td colspan="9" class="text-center py-4 text-rose-400">Failed to load monitors</td></tr>';
        return;
    }

    if (data.items.length === 0) {
        tbody.innerHTML = `
            <tr>
                <td colspan="9" class="text-center py-12">
                    <div class="flex flex-col items-center justify-center">
                        <span class="material-symbols-outlined text-4xl text-slate-600 mb-3 opacity-50">monitoring</span>
                        <p class="text-slate-400 font-medium tracking-wide">No contracts are currently being monitored.</p>
                        <p class="text-xs text-slate-500 mt-2">Star a contract on the Alerts page to track it here in real-time.</p>
                    </div>
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = '';
    data.items.forEach(item => {
        const tr = document.createElement('tr');
        tr.className = 'hover:bg-slate-800/30 transition-colors group border-b border-slate-800/50';

        tr.innerHTML = `
            <td class="px-6 py-4">
                <div class="flex items-center gap-2">
                    <div class="w-8 h-8 rounded bg-gradient-to-br from-primary/20 to-primary/5 flex items-center justify-center border border-primary/20">
                        <span class="text-[10px] font-bold text-primary">${item.ticker.substring(0, 2)}</span>
                    </div>
                    <div>
                        <span class="text-sm font-bold text-white">${item.ticker}</span>
                        <span class="text-xs text-slate-400 ml-1">$${item.strike}${item.opt_type} ${item.exp}</span>
                    </div>
                </div>
            </td>
            <td class="px-6 py-4 text-sm tabular-nums text-slate-400">
                ${item.entry_score ? item.entry_score.toFixed(2) : '-'}
            </td>
            <td class="px-6 py-4">
                <span class="text-sm font-bold text-white tabular-nums">${item.current_score ? item.current_score.toFixed(2) : '-'}</span>
            </td>
            <td class="px-6 py-4 text-sm tabular-nums text-slate-400">
                ${item.peak_score ? item.peak_score.toFixed(2) : '-'}
            </td>
            <td class="px-6 py-4 text-sm tabular-nums ${item.delta_from_peak < 0 ? 'text-red-400' : 'text-emerald-500'}">
                ${item.delta_from_peak !== null ? item.delta_from_peak : '-'}
            </td>
            <td class="px-6 py-4">
                <div class="w-20 h-8 flex items-end gap-0.5 opacity-60">
                    <div class="flex-1 bg-primary/40 h-[40%] rounded-t-sm"></div>
                    <div class="flex-1 bg-primary/60 h-[60%] rounded-t-sm"></div>
                    <div class="flex-1 bg-primary/80 h-[80%] rounded-t-sm"></div>
                    <div class="flex-1 bg-primary h-[100%] rounded-t-sm"></div>
                </div>
            </td>
            <td class="px-6 py-4">
                <span class="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-bold ${item.status === 'ACTIVE' || item.status === 'Monitor' ? 'bg-primary/10 text-primary border-primary/20' : 'bg-amber-500/10 text-amber-500 border-amber-500/20'} uppercase border">
                    ${item.status || 'TRACKING'}
                </span>
            </td>
            <td class="px-6 py-4 text-sm text-slate-400">
                ${formatDate(item.last_update_ts)}
            </td>
            <td class="px-6 py-4 text-right">
                <button title="Remove from Monitor" class="text-slate-600 hover:text-red-500 transition-colors" onclick="toggleAOI('${item.contract_key}', 1)">
                    <span class="material-symbols-outlined">close</span>
                </button>
            </td>
        `;
        tbody.appendChild(tr);
    });
}

// Recent Top Alerts Panel
async function loadTopRecent(isBackground = false) {
    const listEl = document.getElementById('recent-alerts-list');
    if (!listEl) return;

    if (!isBackground) {
        listEl.innerHTML = '<div class="p-4 text-sm text-slate-400 text-center">Loading top alerts...</div>';
    }

    // Top alerts last 15 mins
    const data = await fetchApi('/alerts/recent?window_sec=900&limit=5');
    if (!data || !data.items || data.items.length === 0) {
        listEl.innerHTML = '<div class="p-4 text-sm text-slate-500 text-center">No recent significant alerts</div>';
        return;
    }

    listEl.innerHTML = '';
    data.items.forEach(item => {
        const d = document.createElement('div');
        d.className = 'flex items-center justify-between p-3 border-b border-slate-800/50 last:border-0 hover:bg-slate-800/30 transition-colors';
        d.innerHTML = `
            <div>
                <div class="font-bold text-white text-sm">${item.ticker} <span class="${item.opt_type === 'C' ? 'text-emerald-400' : 'text-rose-400'}">${item.opt_type}</span></div>
                <div class="text-xs text-slate-500">${formatCurrency(item.premium)}</div>
            </div>
            <div class="text-right">
                <div class="text-sm font-bold text-primary">${item.score_total.toFixed(1)}</div>
                <div class="text-[10px] text-slate-500">${formatDate(item.ts)}</div>
            </div>
        `;
        listEl.appendChild(d);
    });
}

async function toggleAOI(contractKey, currentActive) {
    if (!contractKey) return;
    const body = {
        contract_key: contractKey,
        is_active: currentActive ? 0 : 1
    };
    await fetchApi('/watchlist/toggle', {
        method: 'POST',
        body: JSON.stringify(body)
    });
    // refresh
    if (window.location.pathname.includes('monitor.html')) {
        loadMonitors(true);
    } else {
        loadAlerts(true);
    }
}

// App Logic
document.addEventListener('DOMContentLoaded', () => {
    // Check Auth initially
    if (!getToken() && !window.location.pathname.includes('login.html')) {
        window.location.href = '/login.html';
        return;
    }

    // Login Form logic
    const loginForm = document.getElementById('login-form');
    if (loginForm) {
        const emailInput = document.getElementById('email');
        if (emailInput) {
            emailInput.addEventListener('input', (e) => {
                e.target.value = e.target.value.trim();
            });
        }

        loginForm.addEventListener('submit', async (e) => {
            e.preventDefault();
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
                    window.location.href = '/';
                } else {
                    alert('Login failed: ' + (data.detail || 'Unknown error'));
                }
            } catch (err) {
                alert('Connection error');
            }
        });
    }

    // Logout wiring
    const logoutBtn = document.getElementById('logout-btn');
    if (logoutBtn) {
        logoutBtn.addEventListener('click', () => {
            logout();
        });
    }

    // Nav wiring
    const alertsLink = document.getElementById('nav-alerts');
    const monitorsLink = document.getElementById('nav-monitors');
    if (alertsLink) alertsLink.href = '/';
    if (monitorsLink) monitorsLink.href = '/monitor.html';

    // Filters logic
    const filterInput = document.getElementById('filter-symbol');
    if (filterInput) {
        filterInput.addEventListener('change', () => loadAlerts(false));
    }

    // Initial Load
    updateHealth();
    loadAlerts(false);
    loadMonitors(false);
    loadTopRecent(false);

    // Auto refresh every 5s
    setInterval(() => {
        updateHealth();
        loadAlerts(true);
        loadMonitors(true);
        loadTopRecent(true);
    }, 5000);
});
