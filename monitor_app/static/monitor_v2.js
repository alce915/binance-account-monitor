const summaryCards = document.getElementById('summaryCards');
const profitCards = document.getElementById('profitCards');
const groupsContainer = document.getElementById('groupsContainer');
const connectionBadge = document.getElementById('connectionBadge');
const authRoleBadge = document.getElementById('authRoleBadge');
const logoutButton = document.getElementById('logoutButton');
const messageText = document.getElementById('messageText');
const updatedAt = document.getElementById('updatedAt');
const toolbarStats = document.getElementById('toolbarStats');
const monitorToggle = document.getElementById('monitorToggle');
const downloadTemplateButton = document.getElementById('downloadTemplateButton');
const refreshButton = document.getElementById('refreshButton');
const importButton = document.getElementById('importButton');
const fundingTransferButton = document.getElementById('fundingTransferButton');
const importInput = document.getElementById('importInput');
const fundingModalShell = document.getElementById('fundingModalShell');
const fundingModalClose = document.getElementById('fundingModalClose');
const fundingRefreshButton = document.getElementById('fundingRefreshButton');
const fundingGroupSelect = document.getElementById('fundingGroupSelect');
const fundingModeDistribute = document.getElementById('fundingModeDistribute');
const fundingModeCollect = document.getElementById('fundingModeCollect');
const fundingAssetSelect = document.getElementById('fundingAssetSelect');
const fundingSelectAllCheckbox = document.getElementById('fundingSelectAllCheckbox');
const fundingSyncAmountCheckbox = document.getElementById('fundingSyncAmountCheckbox');
const fundingMainSummary = document.getElementById('fundingMainSummary');
const fundingRows = document.getElementById('fundingRows');
const fundingQuickActions = document.getElementById('fundingQuickActions');
const fundingQuickCollectButton = document.getElementById('fundingQuickCollectButton');
const fundingQuickClearButton = document.getElementById('fundingQuickClearButton');
const fundingSubmitButton = document.getElementById('fundingSubmitButton');
const fundingOperationMeta = document.getElementById('fundingOperationMeta');
const fundingOperationCopyButton = document.getElementById('fundingOperationCopyButton');
const fundingLogTabRuntime = document.getElementById('fundingLogTabRuntime');
const fundingLogTabAudit = document.getElementById('fundingLogTabAudit');
const fundingLogLatestTime = document.getElementById('fundingLogLatestTime');
const fundingLogList = document.getElementById('fundingLogList');

const query = new URLSearchParams(window.location.search);
const TEST_MODE = Boolean(window.__MONITOR_V2_TEST_MODE__);
const accountIds = query.get('account_ids');
const groupsUrl = accountIds
  ? `/api/monitor/groups?account_ids=${encodeURIComponent(accountIds)}`
  : '/api/monitor/groups';
const streamUrl = accountIds
  ? `/stream/monitor?account_ids=${encodeURIComponent(accountIds)}`
  : '/stream/monitor';

const statusTextMap = {
  ok: '正常',
  partial: '部分异常',
  error: '异常',
  idle: '待连接',
  reconnecting: '重连中',
  disabled: '监控已关闭',
};
const messageTextMap = {
  'Waiting for monitor connection': '正在等待监控连接',
  'No accounts available': '当前没有可用账户',
  'All accounts are healthy': '所有账户运行正常',
  'All accounts failed': '所有账户均拉取失败',
  'Some accounts failed': '部分账户拉取失败',
  'Monitoring disabled': '监控已关闭，已暂停自动刷新',
  'Monitor accounts reloaded': '监控账户配置已重载',
  'Refresh completed': '刷新完成',
};
const accountStatusTextMap = { NORMAL: '正常' };
const positionSideTextMap = { LONG: '多', SHORT: '空', BOTH: '双向' };
const groupTextMap = {
  expandAccounts: '展开子账号',
  collapseAccounts: '收起子账号',
  healthy: '正常',
  warning: '警惕',
  danger: '危险',
  accounts: '个账户',
  uniMmr: 'UniMMR',
};

const groupExpandedState = {};
const groupSelectedAccountState = {};
const renderedGroupSignatures = {};

let toggleBusy = false;
let refreshBusy = false;
let importBusy = false;
let refreshCooldownSeconds = 0;
let refreshCooldownTimer = null;
let latestPayload = null;

let fundingModalBusy = false;
let fundingRefreshBusy = false;
let fundingRefreshCooldownSeconds = 0;
let fundingRefreshCooldownTimer = null;
let fundingOverview = null;
let fundingDirection = 'distribute';
let fundingSelectedGroupId = '';
let fundingSelectedAsset = '';
let fundingSelectionState = {};
let fundingSyncAmountEnabled = false;
let fundingLogEntries = [];
let fundingLogCounter = 0;
let fundingLastCapabilitySignature = '';
let fundingAuditEntries = [];
let fundingAuditDetailsByOperationId = {};
let fundingAuditBusy = false;
let fundingActiveLogTab = 'runtime';
let fundingPendingOperationId = '';
let fundingAuditSelectedOperationId = '';
let fundingAuditFilter = '';

let toolbarStatsSignature = '';
let summarySignature = '';
let profitSummarySignature = '';
let pendingStreamPayload = null;
let pendingStreamFrame = null;
let authSession = {
  enabled: false,
  initialized: true,
  authenticated: true,
  whitelisted: false,
  role: 'admin',
  auth_source: 'disabled',
  csrf_token: '',
  last_activity_at: null,
};

const fmt = (value) => {
  const number = Number(value ?? 0);
  return Number.isFinite(number)
    ? number.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : String(value ?? '-');
};
const fmtCount = (value) => {
  const number = Number(value ?? 0);
  return Number.isFinite(number) ? number.toLocaleString('zh-CN') : String(value ?? '0');
};
const fmtTime = (value) => {
  if (!value) {
    return '-';
  }
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? String(value)
    : date.toLocaleString('zh-CN', { hour12: false });
};
const fmtPercent = (value) => {
  const number = Number(value ?? 0);
  return Number.isFinite(number)
    ? `${(number * 100).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}%`
    : '-';
};
const fmtOptionalPrice = (value) => {
  const number = Number(value ?? 0);
  if (!Number.isFinite(number) || number <= 0) {
    return '-';
  }
  return fmt(number);
};
const fmtUniMmr = (value) => {
  const number = Number(value);
  return Number.isFinite(number)
    ? number.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : '-';
};
const numberTone = (value) => {
  const number = Number(value);
  if (!Number.isFinite(number) || number === 0) {
    return '';
  }
  return number > 0 ? 'value-positive' : 'value-negative';
};
const escapeHtml = (value) =>
  String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#39;');
const maskUid = (value) => {
  const text = String(value ?? '').trim();
  if (!text) return '-';
  if (text.includes('*')) return text;
  if (text.length <= 2) return '*'.repeat(text.length);
  if (text.length <= 6) return `${text.slice(0, 1)}${'*'.repeat(Math.max(text.length - 2, 1))}${text.slice(-1)}`;
  return `${text.slice(0, 4)}${'*'.repeat(Math.max(text.length - 6, 2))}${text.slice(-2)}`;
};

const textStatus = (status) => statusTextMap[status] || status || '未知';
const textMessage = (message) => messageTextMap[message] || message || '-';
const textAccountStatus = (status) => accountStatusTextMap[status] || status || '-';
const textPositionSide = (side) => positionSideTextMap[side] || side || '-';
const statusClass = (status) => {
  if (status === 'ok') return 'status-ok';
  if (status === 'partial' || status === 'reconnecting') return 'status-partial';
  if (status === 'error') return 'status-error';
  if (status === 'disabled') return 'status-disabled';
  return '';
};
const positionSideTone = (side) => {
  if (side === 'LONG') return 'side-long';
  if (side === 'SHORT') return 'side-short';
  return '';
};
const uniMmrToneClass = (value) => {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return '';
  }
  if (number > 1.5) return 'uni-mmr-good';
  if (number > 1.2) return 'uni-mmr-warn';
  return 'uni-mmr-bad';
};
const uniMmrToneKey = (value) => {
  const toneClass = uniMmrToneClass(value);
  if (toneClass === 'uni-mmr-good') return 'good';
  if (toneClass === 'uni-mmr-warn') return 'warn';
  if (toneClass === 'uni-mmr-bad') return 'bad';
  return '';
};
const currentGroups = () => (Array.isArray(latestPayload?.groups) ? latestPayload.groups : []);
const refreshButtonLabel = () => (refreshCooldownSeconds > 0 ? `${refreshCooldownSeconds}秒` : '立即刷新');
const fundingRefreshButtonLabel = () => (fundingRefreshCooldownSeconds > 0 ? `${fundingRefreshCooldownSeconds}秒` : '立即刷新');
const fmtClock = (value = new Date()) => {
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime())
    ? '--:--:--'
    : date.toLocaleTimeString('zh-CN', { hour12: false });
};
const newOperationId = () => {
  if (window.crypto?.randomUUID) {
    return window.crypto.randomUUID();
  }
  return `op-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
};
const shortOperationId = (value) => {
  const text = String(value || '').trim();
  return text ? text.slice(0, 8) : '-';
};
const authRoleText = (value, authSource) => {
  if (authSource === 'whitelist') return '白名单管理员';
  if (authSource === 'disabled') return '开放模式';
  if (authSource === 'break_glass') return '应急管理员';
  if (value === 'guest') return '游客模式';
  return '管理员';
};
const canWrite = () => {
  if (!authSession.enabled) return true;
  if (authSession.auth_source === 'whitelist' || authSession.auth_source === 'break_glass') return true;
  return authSession.role === 'admin';
};

function handleAuthError(response) {
  if (response.status === 401 && authSession.enabled) {
    const next = `${window.location.pathname}${window.location.search || ''}`;
    window.location.replace(`/login?next=${encodeURIComponent(next)}`);
    return true;
  }
  return false;
}

async function apiFetch(url, options = {}) {
  const requestOptions = { ...options };
  const method = String(requestOptions.method || 'GET').toUpperCase();
  const headers = new Headers(requestOptions.headers || {});
  if (!headers.has('Cache-Control') && method === 'GET') {
    requestOptions.cache = requestOptions.cache || 'no-store';
  }
  if (method !== 'GET' && method !== 'HEAD' && method !== 'OPTIONS' && authSession.csrf_token) {
    headers.set('X-CSRF-Token', authSession.csrf_token);
  }
  requestOptions.headers = headers;
  const response = await fetch(url, requestOptions);
  handleAuthError(response);
  return response;
}

async function readApiPayload(response) {
  if (!response) return {};
  if (typeof response.text !== 'function') {
    if (typeof response.json === 'function') {
      try {
        return await response.json();
      } catch {
        return {};
      }
    }
    return {};
  }
  const text = await response.text();
  if (!text) return {};
  try {
    return JSON.parse(text);
  } catch {
    return { detail: text };
  }
}

async function loadAuthSession() {
  const response = await fetch('/api/auth/session', { cache: 'no-store' });
  const payload = await response.json();
  if (response.status === 503 && payload?.error?.code === 'AUTH_NOT_INITIALIZED') {
    window.location.replace('/login');
    throw new Error(payload?.error?.message || '认证未初始化');
  }
  if (!response.ok) {
    throw new Error(payload?.error?.message || `HTTP ${response.status}`);
  }
  authSession = {
    enabled: Boolean(payload.enabled),
    initialized: payload.initialized !== false,
    authenticated: Boolean(payload.authenticated),
    whitelisted: Boolean(payload.whitelisted),
    role: payload.role || (payload.enabled ? '' : 'admin'),
    auth_source: payload.auth_source || 'disabled',
    csrf_token: String(payload.csrf_token || ''),
    last_activity_at: payload.last_activity_at || null,
  };
  if (authSession.enabled && !authSession.authenticated) {
    const next = `${window.location.pathname}${window.location.search || ''}`;
    window.location.replace(`/login?next=${encodeURIComponent(next)}`);
    throw new Error('认证已失效');
  }
  syncAuthUi();
  return authSession;
}

function syncAuthUi() {
  if (authRoleBadge) {
    authRoleBadge.textContent = authRoleText(authSession.role, authSession.auth_source);
  }
  if (logoutButton) {
    logoutButton.hidden = !authSession.enabled || authSession.auth_source !== 'session';
  }
  applyActionButtonState();
}

function applyActionButtonState() {
  const writeAllowed = canWrite();
  refreshButton.textContent = refreshButtonLabel();
  refreshButton.disabled = !writeAllowed || importBusy || refreshBusy || toggleBusy || refreshCooldownSeconds > 0;
  downloadTemplateButton.disabled = !writeAllowed || importBusy || refreshBusy || toggleBusy;
  importButton.textContent = importBusy ? '导入中' : '导入 Excel';
  importButton.disabled = !writeAllowed || importBusy || refreshBusy || toggleBusy;
  fundingTransferButton.disabled = !writeAllowed || importBusy || refreshBusy || toggleBusy || currentGroups().length === 0;
  monitorToggle.disabled = !writeAllowed || toggleBusy || refreshBusy || importBusy;
  fundingSubmitButton.disabled = fundingModalBusy;
  if (fundingRefreshButton) {
    fundingRefreshButton.textContent = fundingRefreshButtonLabel();
    fundingRefreshButton.disabled = !writeAllowed || fundingModalBusy || fundingRefreshBusy || fundingRefreshCooldownSeconds > 0 || !fundingSelectedGroupId;
  }
  if (fundingQuickCollectButton) {
    fundingQuickCollectButton.disabled = !writeAllowed || fundingModalBusy || fundingDirection !== 'collect' || !fundingModeAvailable() || !fundingSelectedAsset;
  }
  if (fundingQuickClearButton) {
    fundingQuickClearButton.disabled = !writeAllowed || fundingModalBusy || fundingDirection !== 'collect';
  }
  if (fundingSubmitButton) {
    fundingSubmitButton.disabled = !writeAllowed || fundingModalBusy;
  }
  syncFundingAmountToggleState();
  syncFundingSelectAllState();
}

function startRefreshCooldown(seconds = 30) {
  if (refreshCooldownTimer !== null) {
    window.clearInterval(refreshCooldownTimer);
    refreshCooldownTimer = null;
  }
  refreshCooldownSeconds = Math.max(0, Number(seconds) || 0);
  applyActionButtonState();
  refreshCooldownTimer = window.setInterval(() => {
    refreshCooldownSeconds = Math.max(0, refreshCooldownSeconds - 1);
    if (refreshCooldownSeconds === 0 && refreshCooldownTimer !== null) {
      window.clearInterval(refreshCooldownTimer);
      refreshCooldownTimer = null;
    }
    applyActionButtonState();
  }, 1000);
}

function startFundingRefreshCooldown(seconds = 10) {
  if (fundingRefreshCooldownTimer !== null) {
    window.clearInterval(fundingRefreshCooldownTimer);
    fundingRefreshCooldownTimer = null;
  }
  fundingRefreshCooldownSeconds = Math.max(0, Number(seconds) || 0);
  applyActionButtonState();
  fundingRefreshCooldownTimer = window.setInterval(() => {
    fundingRefreshCooldownSeconds = Math.max(0, fundingRefreshCooldownSeconds - 1);
    if (fundingRefreshCooldownSeconds === 0 && fundingRefreshCooldownTimer !== null) {
      window.clearInterval(fundingRefreshCooldownTimer);
      fundingRefreshCooldownTimer = null;
    }
    applyActionButtonState();
  }, 1000);
}

function syncMonitorToggle(payload = {}) {
  monitorToggle.checked = payload.service?.monitor_enabled !== false;
  applyActionButtonState();
}

function renderToolbarStats(summary = {}) {
  const signature = JSON.stringify([
    summary.account_count ?? 0,
    summary.success_count ?? 0,
    summary.error_count ?? 0,
  ]);
  if (signature === toolbarStatsSignature) return;
  toolbarStatsSignature = signature;

  const stats = [
    { label: '总账户数', value: fmtCount(summary.account_count), tone: '' },
    { label: '正常账户', value: fmtCount(summary.success_count), tone: 'value-positive' },
    { label: '异常账户', value: fmtCount(summary.error_count), tone: 'value-negative' },
  ];
  toolbarStats.innerHTML = stats.map(({ label, value, tone }) => `
    <div class="stat-pill">
      <span class="stat-pill-label">${label}</span>
      <span class="stat-pill-value ${tone}">${value}</span>
    </div>
  `).join('');
}

function renderSummary(summary = {}) {
  const signature = JSON.stringify([
    summary.equity ?? 0,
    summary.margin ?? 0,
    summary.available_balance ?? 0,
    summary.unrealized_pnl ?? 0,
    summary.total_commission ?? 0,
    summary.distribution_apy_7d ?? 0,
  ]);
  if (signature === summarySignature) return;
  summarySignature = signature;

  const cards = [
    { label: '权益', value: fmt(summary.equity), tone: '' },
    { label: '保证金', value: fmt(summary.margin), tone: '' },
    { label: '可用余额', value: fmt(summary.available_balance), tone: '' },
    { label: '未实现盈亏', value: fmt(summary.unrealized_pnl), tone: numberTone(summary.unrealized_pnl) },
    { label: '手续费', value: fmt(summary.total_commission), tone: numberTone(summary.total_commission) },
    { label: '7日年化', value: fmtPercent(summary.distribution_apy_7d), tone: numberTone(summary.distribution_apy_7d) },
  ];
  summaryCards.innerHTML = cards.map(({ label, value, tone }) => `
    <article class="card">
      <div class="label">${label}</div>
      <div class="value ${tone}">${value}</div>
    </article>
  `).join('');
}

function renderProfitSummary(profitSummary = {}) {
  const periods = ['today', 'week', 'month', 'year', 'all']
    .map((key) => profitSummary[key])
    .filter(Boolean);
  const signature = JSON.stringify(periods);
  if (signature === profitSummarySignature) return;
  profitSummarySignature = signature;

  if (!periods.length) {
    profitCards.innerHTML = '';
    return;
  }

  profitCards.innerHTML = periods.map((period) => {
    const tone = numberTone(period.amount);
    const note = period.complete ? '' : '<div class="card-note">历史回补中</div>';
    return `
      <article class="card">
        <div class="label">${escapeHtml(period.label || '-')}</div>
        <div class="value ${tone}">${fmt(period.amount)} | ${fmtPercent(period.rate)}</div>
        ${note}
      </article>
    `;
  }).join('');
}

function renderRows(headers, rows, formatter, emptyLabel = '暂无数据') {
  if (!Array.isArray(rows) || rows.length === 0) {
    return `<div class="empty">${escapeHtml(emptyLabel)}</div>`;
  }
  return `
    <table>
      <thead><tr>${headers.map((header) => `<th>${header}</th>`).join('')}</tr></thead>
      <tbody>${rows.map((row) => formatter(row)).join('')}</tbody>
    </table>
  `;
}

function fundingGroupOptions() {
  return currentGroups()
    .map((group) => ({
      id: String(group.main_account_id || ''),
      name: String(group.main_account_name || group.main_account_id || '-'),
    }))
    .filter((group) => group.id);
}

function fundingRowState(accountId) {
  if (!fundingSelectionState[accountId]) {
    fundingSelectionState[accountId] = { checked: false, amount: '' };
  }
  return fundingSelectionState[accountId];
}

function resetFundingSelectionState() {
  fundingSelectionState = {};
  const rows = Array.isArray(fundingOverview?.children) ? fundingOverview.children : [];
  rows.forEach((row) => {
    fundingSelectionState[String(row.account_id || '')] = { checked: false, amount: '' };
  });
}

function resetFundingSyncAmountState() {
  fundingSyncAmountEnabled = false;
}

function fundingLogLevelLabel(level) {
  if (level === 'success') return 'SUCCESS';
  if (level === 'error') return 'ERROR';
  return 'INFO';
}

function fundingToneToLogLevel(tone = '') {
  if (tone === 'is-success') return 'success';
  if (tone === 'is-error') return 'error';
  return 'info';
}

function fundingOperationMetaText() {
  if (fundingOverview && fundingOverview.write_enabled === false) {
    return `写保护：${fundingOverview?.write_disabled_reason || '当前环境禁止真实划转'}`;
  }
  return `操作ID：${shortOperationId(fundingCurrentOperationId())}`;
}

function fundingOperationMetaFullText() {
  return fundingCurrentOperationId();
}

function fundingCurrentOperationId() {
  if (fundingActiveLogTab === 'audit') {
    const selectedEntry = ensureFundingAuditSelection();
    if (selectedEntry) {
      const entryKey = fundingAuditEntryKey(selectedEntry);
      const selectedDetail = getFundingAuditDetail(entryKey);
      const selectedOperationId = String(selectedDetail?.operation_id || selectedEntry.operation_id || '').trim();
      if (selectedOperationId) {
        return selectedOperationId;
      }
    }
  }
  const text = String(fundingPendingOperationId || '').trim();
  return text || '';
}

function fundingAuditStatusLabel(status = '') {
  if (status === 'operation_fully_succeeded') return 'SUCCESS';
  if (status === 'operation_partially_succeeded') return 'PARTIAL';
  if (status === 'operation_submitted') return 'PENDING';
  if (status === 'operation_failed') return 'FAILED';
  return 'INFO';
}

function fundingAuditStatusLevel(status = '') {
  if (status === 'operation_fully_succeeded') return 'success';
  if (status === 'operation_partially_succeeded') return 'info';
  if (status === 'operation_submitted') return 'info';
  if (status === 'operation_failed') return 'error';
  return 'info';
}

function fundingAuditFilteredEntries() {
  const keyword = String(fundingAuditFilter || '').trim().toLowerCase();
  if (!keyword) {
    return fundingAuditEntries;
  }
  return fundingAuditEntries.filter((entry) =>
    String(entry.operation_id || '').toLowerCase().startsWith(keyword)
    || String(entry.asset || '').toLowerCase().includes(keyword)
    || String(entry.message || '').toLowerCase().includes(keyword));
}

function fundingAuditEntryKey(entryOrDirection, operationId = '') {
  if (entryOrDirection && typeof entryOrDirection === 'object') {
    return `${String(entryOrDirection.direction || '').trim().toLowerCase()}::${String(entryOrDirection.operation_id || '').trim()}`;
  }
  return `${String(entryOrDirection || '').trim().toLowerCase()}::${String(operationId || '').trim()}`;
}

function parseFundingAuditEntryKey(entryKey) {
  const normalized = String(entryKey || '');
  const separatorIndex = normalized.indexOf('::');
  if (separatorIndex < 0) {
    return { direction: '', operationId: normalized };
  }
  return {
    direction: normalized.slice(0, separatorIndex),
    operationId: normalized.slice(separatorIndex + 2),
  };
}

function getFundingAuditDetail(entryKey) {
  return fundingAuditDetailsByOperationId[String(entryKey || '')] || null;
}

function ensureFundingAuditSelection() {
  const filteredEntries = fundingAuditFilteredEntries();
  if (!filteredEntries.length) {
    fundingAuditSelectedOperationId = '';
    return null;
  }
  const current = String(fundingAuditSelectedOperationId || '');
  const selected = filteredEntries.find((entry) => fundingAuditEntryKey(entry) === current) || filteredEntries[0];
  fundingAuditSelectedOperationId = fundingAuditEntryKey(selected);
  return selected;
}

function formatFundingAuditDetail(detail) {
  if (!detail) {
    return '<div class="funding-log-empty">请选择一条审计记录</div>';
  }
  const summary = detail.operation_summary || {};
  const reconciliation = detail.reconciliation || {};
  const precheck = detail.precheck || {};
  const results = Array.isArray(detail.results) ? detail.results : [];
  const unconfirmed = Array.isArray(summary.unconfirmed_account_ids) ? summary.unconfirmed_account_ids : [];
  const resultMarkup = results.length
    ? results.map((result) => `
        <div class="funding-audit-result">
          <div class="funding-audit-result-head">
            <strong>${escapeHtml(result.name || result.account_id || '-')}</strong>
            <div class="funding-log-badge is-${escapeHtml(result.success ? 'success' : result.transfer_attempted ? 'error' : 'info')}">
              ${escapeHtml(result.success ? 'SUCCESS' : result.transfer_attempted ? 'FAILED' : 'SKIPPED')}
            </div>
          </div>
          <div class="funding-audit-result-meta mono">
            <div>${escapeHtml(result.account_id || '-')} | UID ${escapeHtml(maskUid(result.uid || '-'))}</div>
            <div>请求 ${escapeHtml(result.requested_amount || '-')} / 执行 ${escapeHtml(result.executed_amount || '-')}</div>
            <div>预校验可用 ${escapeHtml(result.precheck_available_amount || '-')}</div>
          </div>
          <div>${escapeHtml(result.message || '-')}</div>
        </div>
      `).join('')
    : '<div class="funding-log-empty">暂无可展示的执行明细</div>';

  return `
    <div class="funding-audit-detail-head">
      <div class="funding-audit-detail-title">
        <strong>${escapeHtml(detail.direction === 'collect' ? '子账号归集' : '主账号分发')}</strong>
        <div class="mono">操作ID ${escapeHtml(shortOperationId(detail.operation_id || '-'))}</div>
      </div>
      <div class="funding-log-badge is-${escapeHtml(fundingAuditStatusLevel(detail.operation_status || ''))}">
        ${escapeHtml(fundingAuditStatusLabel(detail.operation_status || ''))}
      </div>
    </div>
    <div class="funding-audit-detail-grid">
      <div class="funding-audit-detail-card">
        <span>资产</span>
        <strong>${escapeHtml(summary.asset || detail.asset || '-')}</strong>
      </div>
      <div class="funding-audit-detail-card">
        <span>执行阶段</span>
        <strong>${escapeHtml(detail.execution_stage || '-')}</strong>
      </div>
      <div class="funding-audit-detail-card">
        <span>请求总额</span>
        <strong>${escapeHtml(summary.requested_total_amount || precheck.requested_total_amount || '0')}</strong>
      </div>
      <div class="funding-audit-detail-card">
        <span>到账确认</span>
        <strong>${escapeHtml(reconciliation.status || '-')}</strong>
      </div>
    </div>
    <div class="funding-audit-section">
      <h4>摘要</h4>
      <div class="funding-audit-result">
        <div class="funding-audit-result-meta">
          <div>预校验账号 ${escapeHtml(String(precheck.validated_account_count ?? 0))} 个</div>
          <div>尝试 ${escapeHtml(String(summary.attempted_count ?? 0))} / 成功 ${escapeHtml(String(summary.success_count ?? 0))} / 失败 ${escapeHtml(String(summary.failure_count ?? 0))}</div>
          <div>确认 ${escapeHtml(String(summary.confirmed_count ?? 0))} / 待确认 ${escapeHtml(String(summary.pending_confirmation_count ?? 0))}</div>
          <div>主账号变动方向 ${escapeHtml(summary.expected_main_direction || '-')}</div>
          <div>主账号前后可用 ${escapeHtml(summary.main_before_available_amount || '-')} -> ${escapeHtml(summary.main_after_available_amount || '-')}</div>
        </div>
        <div>${escapeHtml(detail.message || '操作已记录')}</div>
        ${unconfirmed.length ? `<div class="mono">待确认账号：${escapeHtml(unconfirmed.join(', '))}</div>` : ''}
      </div>
    </div>
    <div class="funding-audit-section">
      <h4>执行明细</h4>
      <div class="funding-audit-section-list">${resultMarkup}</div>
    </div>
  `;
}

function renderFundingAuditPanel() {
  const filteredEntries = fundingAuditFilteredEntries();
  const selectedEntry = ensureFundingAuditSelection();
  const selectedDetail = selectedEntry ? getFundingAuditDetail(fundingAuditEntryKey(selectedEntry)) : null;
  fundingLogLatestTime.textContent = filteredEntries[0]?.time || '--:--:--';
  fundingLogList.innerHTML = `
    <div class="funding-audit-shell">
      <div class="funding-audit-toolbar">
        <input
          class="funding-audit-filter"
          type="search"
          data-funding-audit-filter
          placeholder="按操作ID或资产筛选"
          value="${escapeHtml(fundingAuditFilter)}"
        >
      </div>
      <div class="funding-audit-body">
        <div class="funding-audit-list">
          ${filteredEntries.length ? filteredEntries.map((entry) => `
            <button
              class="funding-audit-item ${fundingAuditEntryKey(entry) === String(fundingAuditSelectedOperationId || '') ? 'is-selected' : ''}"
              type="button"
              data-funding-audit-select="${escapeHtml(fundingAuditEntryKey(entry))}"
            >
              <div class="funding-audit-item-head mono">
                <span>${escapeHtml(entry.time || '--:--:--')}</span>
                <span>${escapeHtml(shortOperationId(entry.operation_id || '-'))}</span>
              </div>
              <div class="funding-audit-item-main">
                <div class="funding-log-badge is-${escapeHtml(fundingAuditStatusLevel(entry.operation_status || ''))}">
                  ${escapeHtml(fundingAuditStatusLabel(entry.operation_status || ''))}
                </div>
                <span>${escapeHtml(entry.asset || '-')}</span>
                <span class="mono">${escapeHtml(entry.execution_stage || '-')}</span>
              </div>
              <div class="funding-audit-item-message">${escapeHtml(entry.message || '操作已记录')}</div>
            </button>
          `).join('') : '<div class="funding-log-empty">暂无审计记录</div>'}
        </div>
        <div class="funding-audit-detail">${formatFundingAuditDetail(selectedDetail)}</div>
      </div>
    </div>
  `;
}

function renderFundingLogPanel() {
  if (!fundingLogList || !fundingLogLatestTime || !fundingLogTabRuntime || !fundingLogTabAudit) {
    return;
  }

  fundingLogTabRuntime.classList.toggle('is-active', fundingActiveLogTab === 'runtime');
  fundingLogTabAudit.classList.toggle('is-active', fundingActiveLogTab === 'audit');

  if (fundingActiveLogTab === 'audit') {
    renderFundingAuditPanel();
    return;
  }

  const entries = fundingLogEntries;
  if (!entries.length) {
    fundingLogLatestTime.textContent = '--:--:--';
    fundingLogList.innerHTML = '<div class="funding-log-empty">暂无日志</div>';
    return;
  }

  fundingLogLatestTime.textContent = entries[0].time || '--:--:--';
  fundingLogList.innerHTML = entries.map((entry) => `
    <div class="funding-log-entry">
      <div class="funding-log-time mono">${escapeHtml(entry.time)}</div>
      <div class="funding-log-badge is-${escapeHtml(entry.level)}">${escapeHtml(fundingLogLevelLabel(entry.level))}</div>
      <div class="funding-log-message">${escapeHtml(entry.message)}</div>
    </div>
  `).join('');
}

function appendFundingLog(message, level = 'info') {
  const content = String(message || '').trim();
  if (!content) {
    return;
  }

  fundingLogEntries.unshift({
    id: `${Date.now()}-${++fundingLogCounter}`,
    time: fmtClock(new Date()),
    level,
    message: content,
  });
  if (fundingLogEntries.length > 300) {
    fundingLogEntries = fundingLogEntries.slice(0, 300);
  }
  renderFundingLogPanel();
}

function setFundingAuditEntries(entries = []) {
  fundingAuditEntries = entries
    .filter((entry) => entry && typeof entry === 'object')
    .map((entry) => ({
      created_at: entry.created_at || '',
      updated_at: entry.updated_at || '',
      time: fmtClock(entry.updated_at || entry.created_at || new Date()),
      message: String(entry.message || '').trim(),
      operation_status: String(entry.operation_status || ''),
      operation_id: String(entry.operation_id || ''),
      direction: String(entry.direction || '').trim().toLowerCase(),
      asset: String(entry.asset || ''),
      execution_stage: String(entry.execution_stage || ''),
      account_count: Number(entry.account_count || 0),
      success_count: Number(entry.success_count || 0),
      failure_count: Number(entry.failure_count || 0),
      confirmed_count: Number(entry.confirmed_count || 0),
      pending_confirmation_count: Number(entry.pending_confirmation_count || 0),
    }));
  const entryKeys = new Set(fundingAuditEntries.map((entry) => fundingAuditEntryKey(entry)));
  Object.keys(fundingAuditDetailsByOperationId).forEach((entryKey) => {
    if (!entryKeys.has(entryKey)) {
      delete fundingAuditDetailsByOperationId[entryKey];
    }
  });
  ensureFundingAuditSelection();
  renderFundingLogPanel();
}

function setFundingAuditDetail(detail) {
  if (!detail || typeof detail !== 'object') {
    return;
  }
  const operationId = String(detail.operation_id || '').trim();
  const direction = String(detail.direction || '').trim().toLowerCase();
  if (!operationId || !direction) {
    return;
  }
  const entryKey = fundingAuditEntryKey(direction, operationId);
  fundingAuditDetailsByOperationId[entryKey] = detail;
  if (!fundingAuditSelectedOperationId) {
    fundingAuditSelectedOperationId = entryKey;
  }
  renderFundingLogPanel();
}

async function loadFundingAuditDetail(mainAccountId, operationId, direction = '', { preserveOnError = true } = {}) {
  const normalizedOperationId = String(operationId || '').trim();
  const normalizedDirection = String(direction || '').trim().toLowerCase();
  const entryKey = fundingAuditEntryKey(normalizedDirection, normalizedOperationId);
  if (!mainAccountId || !normalizedOperationId || !normalizedDirection) {
    return { success: false, error: '暂无可加载的审计详情' };
  }
  try {
    const query = new URLSearchParams({ direction: normalizedDirection }).toString();
    const response = await apiFetch(
      `/api/funding/groups/${encodeURIComponent(mainAccountId)}/audit/${encodeURIComponent(normalizedOperationId)}?${query}`,
      { cache: 'no-store' },
    );
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || payload?.error?.message || `HTTP ${response.status}`);
    }
    setFundingAuditDetail(payload);
    return { success: true, error: '' };
  } catch (error) {
    if (!preserveOnError) {
      delete fundingAuditDetailsByOperationId[entryKey];
      renderFundingLogPanel();
    }
    appendFundingLog(`审计详情加载失败：${error}`, 'error');
    return { success: false, error: String(error) };
  }
}

async function loadFundingAudit(mainAccountId, { preserveOnError = true } = {}) {
  if (!mainAccountId) {
    fundingAuditEntries = [];
    fundingAuditDetailsByOperationId = {};
    fundingAuditSelectedOperationId = '';
    renderFundingLogPanel();
    return { success: false, error: '暂无分组' };
  }

  fundingAuditBusy = true;
  applyActionButtonState();
  try {
    const response = await apiFetch(`/api/funding/groups/${encodeURIComponent(mainAccountId)}/audit`, { cache: 'no-store' });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || `HTTP ${response.status}`);
    }
    setFundingAuditEntries(Array.isArray(payload.entries) ? payload.entries : []);
    const selectedEntry = ensureFundingAuditSelection();
    if (selectedEntry) {
      await loadFundingAuditDetail(mainAccountId, selectedEntry.operation_id, selectedEntry.direction, { preserveOnError: true });
    }
    return { success: true, error: '' };
  } catch (error) {
    if (!preserveOnError) {
      setFundingAuditEntries([]);
    }
    appendFundingLog(`审计日志加载失败：${error}`, 'error');
    return { success: false, error: String(error) };
  } finally {
    fundingAuditBusy = false;
    applyActionButtonState();
  }
}

async function ensureFundingAuditDetailLoaded(mainAccountId, { preserveOnError = true } = {}) {
  const selectedEntry = ensureFundingAuditSelection();
  if (!selectedEntry) {
    renderFundingLogPanel();
    renderFundingOperationMeta();
    return { success: false, error: '暂无审计记录' };
  }
  const entryKey = fundingAuditEntryKey(selectedEntry);
  if (getFundingAuditDetail(entryKey)) {
    renderFundingLogPanel();
    renderFundingOperationMeta();
    return { success: true, error: '' };
  }
  const result = await loadFundingAuditDetail(
    mainAccountId,
    selectedEntry.operation_id,
    selectedEntry.direction,
    { preserveOnError },
  );
  renderFundingOperationMeta();
  return result;
}

function renderFundingOperationMeta() {
  if (!fundingOperationMeta) {
    return;
  }
  fundingOperationMeta.textContent = fundingOperationMetaText();
  if (fundingOperationCopyButton instanceof HTMLButtonElement) {
    fundingOperationCopyButton.disabled = !fundingOperationMetaFullText();
  }
}

function fundingAvailableMap(row) {
  return row?.spot_available || row?.funding_available || {};
}

function fundingAssetValue(row) {
  const asset = String(fundingSelectedAsset || '');
  return asset ? fundingAvailableMap(row)[asset] || '0' : '0';
}

function fundingAssetNumber(row) {
  const amount = Number(fundingAssetValue(row));
  return Number.isFinite(amount) ? amount : 0;
}

function fundingRowEligible(row) {
  return fundingDirection === 'distribute' ? Boolean(row?.can_distribute) : Boolean(row?.can_collect);
}

function fundingRowReason(row) {
  if (fundingDirection === 'distribute') {
    return row?.reason_distribute || row?.reason || '不可用';
  }
  return row?.reason_collect || row?.reason || '不可用';
}

function fundingModeAvailable() {
  const rows = Array.isArray(fundingOverview?.children) ? fundingOverview.children : [];
  return rows.some((row) => fundingRowEligible(row));
}

function fundingSelectableRows() {
  const rows = Array.isArray(fundingOverview?.children) ? fundingOverview.children : [];
  return rows.filter((row) => fundingRowEligible(row));
}

function fundingMasterRow() {
  const rows = Array.isArray(fundingOverview?.children) ? fundingOverview.children : [];
  return rows[0] || null;
}

function fundingMasterAccountId() {
  return String(fundingMasterRow()?.account_id || '');
}

function syncFundingSelectAllState() {
  if (!(fundingSelectAllCheckbox instanceof HTMLInputElement)) {
    return;
  }
  const selectableRows = fundingSelectableRows();
  const selectedCount = selectableRows.filter((row) => {
    const accountId = String(row.account_id || '');
    return accountId && Boolean(fundingRowState(accountId).checked);
  }).length;
  fundingSelectAllCheckbox.disabled = fundingModalBusy || selectableRows.length === 0;
  fundingSelectAllCheckbox.checked = selectableRows.length > 0 && selectedCount === selectableRows.length;
  fundingSelectAllCheckbox.indeterminate = selectedCount > 0 && selectedCount < selectableRows.length;
}

function syncFundingAmountToggleState() {
  if (!(fundingSyncAmountCheckbox instanceof HTMLInputElement)) {
    return;
  }
  const rows = Array.isArray(fundingOverview?.children) ? fundingOverview.children : [];
  fundingSyncAmountCheckbox.checked = fundingSyncAmountEnabled;
  fundingSyncAmountCheckbox.disabled = fundingModalBusy || rows.length === 0;
}

function syncFundingAmountsFromMaster({ render = false } = {}) {
  if (!fundingSyncAmountEnabled) {
    return;
  }
  const masterAccountId = fundingMasterAccountId();
  if (!masterAccountId) {
    return;
  }
  const masterAmount = fundingRowState(masterAccountId).amount || '';
  const rows = Array.isArray(fundingOverview?.children) ? fundingOverview.children : [];
  rows.forEach((row) => {
    const accountId = String(row.account_id || '');
    if (!accountId || accountId === masterAccountId) {
      return;
    }
    if (fundingRowState(accountId).checked) {
      fundingRowState(accountId).amount = masterAmount;
    }
  });

  if (render) {
    renderFundingRows();
    return;
  }

  const inputs = fundingRows.querySelectorAll('[data-funding-amount-id]');
  inputs.forEach((input) => {
    if (!(input instanceof HTMLInputElement)) {
      return;
    }
    const accountId = String(input.getAttribute('data-funding-amount-id') || '');
    if (accountId && accountId !== masterAccountId && fundingRowState(accountId).checked) {
      input.value = masterAmount;
    }
  });
}

function applyFundingQuickCollectPreset() {
  if (fundingModalBusy || fundingDirection !== 'collect' || !fundingSelectedAsset) {
    return;
  }

  resetFundingSyncAmountState();
  const rows = Array.isArray(fundingOverview?.children) ? fundingOverview.children : [];
  rows.forEach((row) => {
    const accountId = String(row.account_id || '');
    if (!accountId) {
      return;
    }
    const state = fundingRowState(accountId);
    if (fundingRowEligible(row)) {
      state.checked = true;
      state.amount = fundingAssetValue(row);
    } else {
      state.checked = false;
    }
  });
  appendFundingLog(`已为当前分组可操作子账号填入 ${fundingSelectedAsset} 的最大归集金额。`, 'info');
  renderFundingRows();
  applyActionButtonState();
}

function applyFundingQuickClearPreset() {
  if (fundingModalBusy || fundingDirection !== 'collect') {
    return;
  }

  const rows = Array.isArray(fundingOverview?.children) ? fundingOverview.children : [];
  rows.forEach((row) => {
    const accountId = String(row.account_id || '');
    if (!accountId) {
      return;
    }
    const state = fundingRowState(accountId);
    state.checked = false;
    state.amount = '';
  });
  resetFundingSyncAmountState();
  appendFundingLog('已清空当前归集表单。', 'info');
  renderFundingRows();
  applyActionButtonState();
}

function fundingCapabilityState() {
  const modeAvailable = fundingModeAvailable();
  return {
    message: modeAvailable
      ? (fundingDirection === 'distribute' ? '当前分组可执行主账号现货分发。' : '当前分组可执行子账号现货归集。')
      : fundingOverview?.reason || (fundingDirection === 'distribute' ? '当前分组暂无可用现货分发子账号' : '当前分组暂无可用现货归集子账号'),
    tone: modeAvailable ? 'is-success' : 'is-error',
  };
}

function syncFundingCapabilityLog(force = false) {
  const capabilityState = fundingCapabilityState();
  const signature = JSON.stringify([
    fundingSelectedGroupId || '',
    fundingDirection,
    capabilityState.tone,
    capabilityState.message,
  ]);
  if (!force && signature === fundingLastCapabilitySignature) {
    return;
  }
  fundingLastCapabilitySignature = signature;
  appendFundingLog(capabilityState.message, fundingToneToLogLevel(capabilityState.tone));
}

function renderFundingMainSummary() {
  const mainAccount = fundingOverview?.main_account || {};
  fundingMainSummary.innerHTML = `
    <div class="funding-main-grid">
      <div class="funding-stat is-identity">
        <div class="funding-identity-head">
          <div>
            <div class="funding-identity-title">${escapeHtml(fundingOverview?.main_account_name || fundingSelectedGroupId || '-')}</div>
            <div class="funding-identity-meta mono">${escapeHtml(maskUid(mainAccount.uid || '-'))}</div>
          </div>
          <div class="badge funding-identity-badge ${mainAccount.transfer_ready ? 'status-ok' : 'status-error'}">
            ${escapeHtml(mainAccount.transfer_ready ? '可操作' : '不可用')}
          </div>
        </div>
      </div>
      <div class="funding-stat"><div class="label">当前代币</div><div class="value">${escapeHtml(fundingSelectedAsset || '-')}</div></div>
      <div class="funding-stat"><div class="label">主账号现货可用</div><div class="value">${fmt(fundingAssetValue(mainAccount))}</div></div>
      <div class="funding-stat"><div class="label">归集 API 状态</div><div class="value">${escapeHtml(mainAccount.transfer_ready ? '已配置' : mainAccount.reason || '未配置')}</div></div>
    </div>
  `;
}

function renderFundingRows() {
  const rows = Array.isArray(fundingOverview?.children) ? fundingOverview.children : [];
  const distributeMode = fundingDirection === 'distribute';
  const masterAccountId = fundingMasterAccountId();
  fundingRows.innerHTML = rows.length ? `${rows.map((row) => {
    const accountId = String(row.account_id || '');
    const state = fundingRowState(accountId);
    const eligible = fundingRowEligible(row);
    const reason = fundingRowReason(row);
    const isSyncMaster = fundingSyncAmountEnabled && accountId === masterAccountId;
    const isSyncLocked = fundingSyncAmountEnabled && accountId !== masterAccountId;
    const checkboxDisabled = !eligible || fundingModalBusy;
    const amountDisabled = fundingModalBusy || (isSyncMaster ? false : checkboxDisabled || !state.checked || isSyncLocked);
    const maxDisabled = fundingModalBusy || (isSyncMaster ? fundingAssetNumber(row) <= 0 : amountDisabled || isSyncLocked || fundingAssetNumber(row) <= 0);
    return `
      <tr class="${eligible ? '' : 'is-disabled'}">
        <td><input type="checkbox" data-funding-account-id="${escapeHtml(accountId)}" ${state.checked ? 'checked' : ''} ${checkboxDisabled ? 'disabled' : ''}></td>
        <td><div>${escapeHtml(row.name || accountId)}</div><div class="mono">${escapeHtml(accountId)}</div></td>
        <td class="mono">${escapeHtml(maskUid(row.uid || '-'))}</td>
        <td>${fmt(fundingAssetValue(row))}</td>
        <td>
          <div class="funding-amount-wrap">
            <input class="funding-amount-input" type="number" min="0" step="0.00000001" placeholder="${distributeMode ? '输入金额' : '输入归集金额'}" data-funding-amount-id="${escapeHtml(accountId)}" value="${escapeHtml(state.amount || '')}" ${amountDisabled ? 'disabled' : ''}>
            <button class="funding-max-button" type="button" data-funding-max-id="${escapeHtml(accountId)}" ${maxDisabled ? 'disabled' : ''}>最大</button>
          </div>
        </td>
        <td class="${eligible ? 'value-positive' : ''}">${escapeHtml(eligible ? '可操作' : reason)}</td>
      </tr>
    `;
  }).join('')}<tr class="funding-table-spacer" aria-hidden="true"><td colspan="6"></td></tr>` : '<tr><td colspan="6" class="empty">当前分组暂无可操作子账号</td></tr>';
  syncFundingAmountToggleState();
  syncFundingSelectAllState();
}

function renderFundingModal() {
  const groups = fundingGroupOptions();
  if (!groups.length) {
    fundingGroupSelect.innerHTML = '<option value="">暂无分组</option>';
    fundingAssetSelect.innerHTML = '<option value="">暂无代币</option>';
    fundingMainSummary.innerHTML = '';
    fundingRows.innerHTML = '<tr><td colspan="6" class="empty">当前暂无可用分组</td></tr>';
    if (fundingQuickActions) {
      fundingQuickActions.hidden = true;
    }
    if (fundingQuickCollectButton) {
      fundingQuickCollectButton.disabled = true;
    }
    if (fundingQuickClearButton) {
      fundingQuickClearButton.disabled = true;
    }
    syncFundingAmountToggleState();
    syncFundingSelectAllState();
    fundingSubmitButton.textContent = fundingDirection === 'distribute' ? '执行分发' : '执行归集';
    fundingSubmitButton.disabled = true;
    renderFundingOperationMeta();
    renderFundingLogPanel();
    return;
  }

  if (!groups.some((group) => group.id === fundingSelectedGroupId)) {
    fundingSelectedGroupId = groups[0].id;
  }
  fundingGroupSelect.innerHTML = groups.map((group) => `
    <option value="${escapeHtml(group.id)}" ${group.id === fundingSelectedGroupId ? 'selected' : ''}>${escapeHtml(group.name)}</option>
  `).join('');

  const assets = Array.isArray(fundingOverview?.assets) ? fundingOverview.assets : [];
  if (!assets.includes(fundingSelectedAsset)) {
    fundingSelectedAsset = assets[0] || '';
  }
  fundingAssetSelect.innerHTML = assets.length
    ? assets.map((asset) => `<option value="${escapeHtml(asset)}" ${asset === fundingSelectedAsset ? 'selected' : ''}>${escapeHtml(asset)}</option>`).join('')
    : '<option value="">暂无可用代币</option>';

  fundingModeDistribute.classList.toggle('is-active', fundingDirection === 'distribute');
  fundingModeCollect.classList.toggle('is-active', fundingDirection === 'collect');
  const modeAvailable = fundingModeAvailable();
  const writeEnabled = fundingOverview?.write_enabled !== false;
  renderFundingMainSummary();
  renderFundingRows();
  if (fundingQuickActions) {
    fundingQuickActions.hidden = fundingDirection !== 'collect';
  }
  if (fundingQuickCollectButton) {
    const showQuickCollect = fundingDirection === 'collect';
    fundingQuickCollectButton.disabled = fundingModalBusy || !showQuickCollect || !modeAvailable || !fundingSelectedAsset;
  }
  if (fundingQuickClearButton) {
    fundingQuickClearButton.disabled = fundingModalBusy || fundingDirection !== 'collect';
  }
  fundingSubmitButton.textContent = fundingDirection === 'distribute' ? '执行分发' : '执行归集';
  fundingSubmitButton.disabled = fundingModalBusy || !modeAvailable || !fundingSelectedAsset || !writeEnabled;
  renderFundingOperationMeta();
  renderFundingLogPanel();
  syncFundingCapabilityLog();
}
async function loadFundingOverview(mainAccountId, { resetState = false, preserveOverviewOnError = false } = {}) {
  if (!mainAccountId) {
    fundingOverview = null;
    renderFundingModal();
    return { success: false, error: '暂无分组' };
  }

  const previousOverview = fundingOverview;
  fundingModalBusy = true;
  applyActionButtonState();
  try {
    const response = await apiFetch(`/api/funding/groups/${encodeURIComponent(mainAccountId)}`, { cache: 'no-store' });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || `HTTP ${response.status}`);
    }
    fundingSelectedGroupId = mainAccountId;
    fundingOverview = payload;
    if (resetState) {
      resetFundingSelectionState();
      fundingAuditFilter = '';
      fundingAuditSelectedOperationId = '';
    }
    await loadFundingAudit(mainAccountId, { preserveOnError: true });
    return { success: true, error: '' };
  } catch (error) {
    if (!preserveOverviewOnError || !previousOverview) {
      fundingOverview = {
        main_account_id: mainAccountId,
        main_account_name: mainAccountId,
        available: false,
        reason: String(error),
        write_enabled: false,
        write_disabled_reason: String(error),
        assets: [],
        main_account: { uid: '', transfer_ready: false, reason: String(error), spot_assets: [], spot_available: {}, funding_assets: [], funding_available: {} },
        children: [],
      };
      resetFundingSelectionState();
    } else {
      fundingOverview = previousOverview;
    }
    return { success: false, error: String(error) };
  } finally {
    fundingModalBusy = false;
    applyActionButtonState();
    renderFundingModal();
  }
}

function openFundingModal() {
  const groups = fundingGroupOptions();
  if (!groups.length) {
    messageText.textContent = '当前没有可用分组，无法打开现货资金划转面板';
    return;
  }
  fundingSelectedGroupId = fundingSelectedGroupId || groups[0].id;
  fundingSelectedAsset = '';
  fundingDirection = 'distribute';
  fundingActiveLogTab = 'runtime';
  resetFundingSyncAmountState();
  fundingModalShell.classList.remove('hidden');
  document.body.style.overflow = 'hidden';
  appendFundingLog('已打开资金归集与分发面板。', 'info');
  loadFundingOverview(fundingSelectedGroupId, { resetState: true });
}

function closeFundingModal() {
  fundingModalShell.classList.add('hidden');
  document.body.style.overflow = '';
  fundingModalBusy = false;
  applyActionButtonState();
}

async function refreshMonitorAfterFundingOperation() {
  try {
    const response = await apiFetch('/api/monitor/refresh', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || `HTTP ${response.status}`);
    }
    render(payload);
    return { success: true, error: '' };
  } catch (error) {
    messageText.textContent = `资金操作后刷新监控失败：${error}`;
    return { success: false, error: String(error) };
  }
}

async function refreshFundingOverviewNow({ useCooldown = false, allowWhileBusy = false } = {}) {
  if (!fundingSelectedGroupId) {
    return { success: false, error: '暂无可刷新的分组' };
  }
  if (fundingRefreshBusy || (!allowWhileBusy && fundingModalBusy) || (useCooldown && fundingRefreshCooldownSeconds > 0)) {
    return { success: false, error: '当前刷新不可用' };
  }

  fundingRefreshBusy = true;
  if (useCooldown) {
    startFundingRefreshCooldown(10);
  }
  appendFundingLog('正在刷新当前分组资金信息…', 'info');
  renderFundingModal();
  applyActionButtonState();

  const result = await loadFundingOverview(fundingSelectedGroupId, {
    resetState: false,
    preserveOverviewOnError: true,
  });

  fundingRefreshBusy = false;
  applyActionButtonState();

  if (result.success) {
    appendFundingLog('当前分组资金信息刷新成功。', 'success');
  } else {
    appendFundingLog(`资金信息刷新失败：${result.error}`, 'error');
  }
  renderFundingModal();

  return result;
}

function parseFundingFailurePayload(payload = {}, response = null, fallbackOperationId = '') {
  const operationId = String(
    response?.headers?.get?.('X-Funding-Operation-Id')
    || payload?.error?.operation_id
    || payload?.operation_id
    || fallbackOperationId
    || '',
  ).trim();
  return {
    operationId,
    detail: String(payload?.detail || payload?.error?.message || '资金操作失败'),
    code: String(payload?.error?.code || 'PRECHECK_UNAVAILABLE'),
  };
}

async function copyFundingOperationId() {
  const operationId = fundingOperationMetaFullText();
  if (!operationId) {
    appendFundingLog('当前没有可复制的操作ID。', 'error');
    return;
  }
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(operationId);
    } else {
      const textArea = document.createElement('textarea');
      textArea.value = operationId;
      textArea.setAttribute('readonly', 'readonly');
      textArea.style.position = 'fixed';
      textArea.style.opacity = '0';
      textArea.style.pointerEvents = 'none';
      document.body.appendChild(textArea);
      textArea.focus();
      textArea.select();
      textArea.setSelectionRange(0, textArea.value.length);
      const copied = typeof document.execCommand === 'function' && document.execCommand('copy');
      textArea.remove();
      if (!copied) {
        throw new Error(`clipboard unavailable，请手动复制：${operationId}`);
      }
    }
    appendFundingLog(`已复制操作ID：${shortOperationId(operationId)}`, 'success');
  } catch (error) {
    appendFundingLog(`复制操作ID失败：${error}`, 'error');
  }
}

async function submitFundingOperation() {
  if (fundingModalBusy || !fundingOverview || !fundingSelectedGroupId) {
    return;
  }

  const rows = Array.isArray(fundingOverview?.children) ? fundingOverview.children : [];
  const rowById = new Map(rows.map((row) => [String(row.account_id || ''), row]));
  const selectedRows = Object.entries(fundingSelectionState)
    .filter(([, state]) => Boolean(state.checked))
    .map(([accountId, state]) => ({ account_id: accountId, amount: state.amount || '' }));

  if (fundingDirection === 'distribute' && !selectedRows.some((row) => Number(row.amount) > 0)) {
    appendFundingLog('请至少勾选一个子账号并填写大于 0 的分发金额。', 'error');
    return;
  }
  if (fundingDirection === 'collect' && !selectedRows.some((row) => Number(row.amount) > 0)) {
    appendFundingLog('请至少勾选一个子账号并填写大于 0 的归集金额。', 'error');
    return;
  }
  if (fundingOverview.write_enabled === false) {
    appendFundingLog(fundingOverview.write_disabled_reason || '当前环境禁止真实划转。', 'error');
    return;
  }
  for (const row of selectedRows) {
    const amount = Number(row.amount);
    if (!Number.isFinite(amount) || amount < 0) {
      appendFundingLog('请输入合法的金额。', 'error');
      return;
    }
    if (fundingDirection === 'collect') {
      const overviewRow = rowById.get(String(row.account_id || ''));
      const maxAmount = overviewRow ? fundingAssetNumber(overviewRow) : 0;
      if (amount > maxAmount + 1e-12) {
        appendFundingLog('请输入不大于最大可归集金额的数值。', 'error');
        return;
      }
    }
  }

  const operationId = newOperationId();
  fundingPendingOperationId = operationId;
  fundingModalBusy = true;
  applyActionButtonState();
  renderFundingOperationMeta();
  appendFundingLog(
    `${fundingDirection === 'distribute' ? '正在执行现货分发' : '正在执行现货归集'}… 操作ID ${shortOperationId(operationId)}`,
    'info',
  );
  try {
    const endpoint = fundingDirection === 'distribute'
      ? `/api/funding/groups/${encodeURIComponent(fundingSelectedGroupId)}/distribute`
      : `/api/funding/groups/${encodeURIComponent(fundingSelectedGroupId)}/collect`;
    const requestBody = fundingDirection === 'distribute'
      ? { asset: fundingSelectedAsset, operation_id: operationId, transfers: selectedRows }
      : { asset: fundingSelectedAsset, operation_id: operationId, transfers: selectedRows };

    const response = await apiFetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(requestBody),
    });
    const payload = await response.json();
    if (!response.ok) {
      const failure = parseFundingFailurePayload(payload, response, operationId);
      fundingPendingOperationId = failure.operationId || operationId;
      throw new Error(`${failure.code}：${failure.detail}`);
    }

    fundingPendingOperationId = String(response.headers.get('X-Funding-Operation-Id') || payload.operation_id || operationId);
    fundingOverview = payload.overview || fundingOverview;
    resetFundingSelectionState();
    renderFundingModal();
    if (payload.idempotent_hit) {
      appendFundingLog(`命中幂等返回：${shortOperationId(fundingPendingOperationId)}，当前仍按既有结果展示。`, 'info');
    }
    const allSucceeded = Array.isArray(payload.results) && payload.results.every((row) => row.success);
    const baseMessage = payload.message || '操作完成';
    appendFundingLog(`${baseMessage} | 操作ID ${shortOperationId(fundingPendingOperationId)}`, allSucceeded ? 'success' : 'error');
    if (payload.overview_refresh?.success === false) {
      appendFundingLog(`资金概览刷新未确认：${payload.overview_refresh.message || '请稍后手动刷新'} | 操作ID ${shortOperationId(fundingPendingOperationId)}`, 'error');
    }
    if (payload.reconciliation?.status) {
      const reconciliationLabel = payload.reconciliation.status === 'confirmed'
        ? '到账确认成功'
        : payload.reconciliation.status === 'partially_confirmed'
          ? '到账确认部分成功'
          : '到账确认未完成';
      appendFundingLog(`${reconciliationLabel}，操作ID ${shortOperationId(fundingPendingOperationId)}`, payload.reconciliation.status === 'confirmed' ? 'success' : 'info');
    }
    await loadFundingAudit(fundingSelectedGroupId, { preserveOnError: true });

    const monitorRefreshResult = await refreshMonitorAfterFundingOperation();
    appendFundingLog(
      monitorRefreshResult.success
        ? `主界面监控信息刷新成功。操作ID ${shortOperationId(fundingPendingOperationId)}`
        : `主界面监控信息刷新失败：${monitorRefreshResult.error} | 操作ID ${shortOperationId(fundingPendingOperationId)}`,
      monitorRefreshResult.success ? 'success' : 'error',
    );
  } catch (error) {
    appendFundingLog(`资金操作失败：${error} | 操作ID ${shortOperationId(fundingPendingOperationId || operationId)}`, 'error');
  } finally {
    fundingModalBusy = false;
    applyActionButtonState();
    renderFundingOperationMeta();
    renderFundingModal();
  }
}

function findGroupByMainId(mainAccountId) {
  return currentGroups().find((group) => String(group.main_account_id || '') === String(mainAccountId)) || null;
}

function findGroupNode(mainAccountId) {
  return groupsContainer.querySelector(`.group[data-main-account-id="${CSS.escape(String(mainAccountId))}"]`);
}

function normalizeGroupUiState(groups = currentGroups()) {
  const validMainIds = new Set();
  groups.forEach((group) => {
    const mainAccountId = String(group.main_account_id || '');
    if (!mainAccountId) {
      return;
    }
    validMainIds.add(mainAccountId);
    if (!(mainAccountId in groupExpandedState)) {
      groupExpandedState[mainAccountId] = false;
    }

    const accounts = Array.isArray(group.accounts) ? group.accounts : [];
    const selectedAccountId = String(groupSelectedAccountState[mainAccountId] || '');
    if (!accounts.some((account) => String(account.account_id || '') === selectedAccountId)) {
      groupSelectedAccountState[mainAccountId] = accounts[0]?.account_id || '';
    }
  });

  Object.keys(groupExpandedState).forEach((mainAccountId) => {
    if (!validMainIds.has(mainAccountId)) delete groupExpandedState[mainAccountId];
  });
  Object.keys(groupSelectedAccountState).forEach((mainAccountId) => {
    if (!validMainIds.has(mainAccountId)) delete groupSelectedAccountState[mainAccountId];
  });
  Object.keys(renderedGroupSignatures).forEach((mainAccountId) => {
    if (!validMainIds.has(mainAccountId)) delete renderedGroupSignatures[mainAccountId];
  });
}

function isGroupExpanded(mainAccountId) {
  return groupExpandedState[mainAccountId] === true;
}

function resolveSelectedAccount(group) {
  const mainAccountId = String(group.main_account_id || '');
  const accounts = Array.isArray(group.accounts) ? group.accounts : [];
  if (!accounts.length) {
    return null;
  }
  const selectedAccountId = String(groupSelectedAccountState[mainAccountId] || '');
  return accounts.find((account) => String(account.account_id || '') === selectedAccountId) || accounts[0];
}

function assetSpotBalance(row) {
  const walletBalance = Number(row?.wallet_balance ?? 0);
  const crossWalletBalance = Number(row?.cross_wallet_balance ?? 0);
  if (!Number.isFinite(walletBalance) || !Number.isFinite(crossWalletBalance)) {
    return 0;
  }
  const spotBalance = walletBalance - crossWalletBalance;
  return Math.abs(spotBalance) < 1e-9 ? 0 : spotBalance;
}

function distributionDisplayAmount(summary = {}, profitSummary = null, accounts = []) {
  const cumulativeAmount = profitSummary?.all?.amount;
  if (cumulativeAmount !== undefined && cumulativeAmount !== null && String(cumulativeAmount).trim() !== '') {
    return cumulativeAmount;
  }
  if (Array.isArray(accounts) && accounts.length) {
    const cumulativeTotal = accounts.reduce((total, account) => {
      const accountAmount = Number(account?.distribution_profit_summary?.all?.amount ?? 0);
      if (!Number.isFinite(accountAmount)) {
        return total;
      }
      return total + accountAmount;
    }, 0);
    if (Math.abs(cumulativeTotal) >= 1e-9) {
      return cumulativeTotal;
    }
  }
  return summary?.total_distribution ?? 0;
}

function renderUniMmrIndicator(account) {
  const value = account?.uni_mmr;
  const toneClass = uniMmrToneClass(value);
  return `<div class="uni-mmr-indicator${toneClass ? ` ${toneClass}` : ''}">${escapeHtml(groupTextMap.uniMmr)} ${escapeHtml(fmtUniMmr(value))}</div>`;
}

function renderAccount(account) {
  const totals = account.totals || {};
  const distributionAmount = distributionDisplayAmount(totals, account.distribution_profit_summary);
  return `
    <article class="account">
      <div class="account-head">
        <div>
          <h3>${escapeHtml(account.child_account_name || account.account_name || account.account_id || '-')}</h3>
          <div class="mono">${escapeHtml(account.account_id || '-')} | ${escapeHtml(textAccountStatus(account.account_status))}</div>
        </div>
        <div class="account-head-actions">
          ${renderUniMmrIndicator(account)}
          <div class="badge ${statusClass(account.status)}">${escapeHtml(textStatus(account.status))}</div>
        </div>
      </div>
      <div class="account-grid">
        ${[
          { label: '权益', value: fmt(totals.equity), tone: '' },
          { label: '保证金', value: fmt(totals.margin), tone: '' },
          { label: '可用余额', value: fmt(totals.available_balance), tone: '' },
          { label: '未实现盈亏', value: fmt(totals.unrealized_pnl), tone: numberTone(totals.unrealized_pnl) },
          { label: '分发收益', value: fmt(distributionAmount), tone: numberTone(distributionAmount) },
          { label: '7日年化', value: fmtPercent(totals.distribution_apy_7d), tone: numberTone(totals.distribution_apy_7d) },
        ].map(({ label, value, tone }) => `
          <div class="metric"><div class="label">${label}</div><div class="value ${tone}">${value}</div></div>
        `).join('')}
      </div>
      <div class="section"><h4>持仓</h4>${renderRows(
        ['交易对', '方向', '数量', '开仓价', '标记价', '爆仓价', '未实现盈亏', '名义价值', '杠杆'],
        account.positions || [],
        (row) => `
          <tr>
            <td class="mono">${escapeHtml(row.symbol || '-')}</td>
            <td class="${positionSideTone(row.position_side)}">${escapeHtml(textPositionSide(row.position_side))}</td>
            <td>${fmt(row.qty)}</td><td>${fmt(row.entry_price)}</td><td>${fmt(row.mark_price)}</td><td>${fmtOptionalPrice(row.liquidation_price)}</td>
            <td class="${numberTone(row.unrealized_pnl)}">${fmt(row.unrealized_pnl)}</td>
            <td>${fmt(row.notional)}</td><td>${fmtCount(row.leverage || 0)}x</td>
          </tr>
        `,
      )}</div>
      <div class="section"><h4>资产</h4>${renderRows(
        ['资产', '钱包余额', '现货余额', '可用余额', '保证金余额', '全仓未实现盈亏', '可提数量'],
        account.assets || [],
        (row) => `
          <tr>
            <td class="mono">${escapeHtml(row.asset || '-')}</td><td>${fmt(row.wallet_balance)}</td><td>${fmt(assetSpotBalance(row))}</td><td>${fmt(row.available_balance)}</td>
            <td>${fmt(row.margin_balance)}</td><td class="${numberTone(row.cross_unrealized_pnl)}">${fmt(row.cross_unrealized_pnl)}</td><td>${fmt(row.max_withdraw_amount)}</td>
          </tr>
        `,
      )}</div>
    </article>
  `;
}
function renderGroupStatusBadges(summary = {}) {
  const successCount = Number(summary.success_count || 0);
  const errorCount = Number(summary.error_count || 0);
  const badges = [];
  if (successCount > 0) badges.push(`<div class="badge status-ok">${fmtCount(successCount)} ${escapeHtml(groupTextMap.healthy)}</div>`);
  if (errorCount > 0) badges.push(`<div class="badge status-error">${fmtCount(errorCount)} 异常</div>`);
  if (!badges.length) badges.push(`<div class="badge">${fmtCount(summary.account_count || 0)} ${escapeHtml(groupTextMap.accounts)}</div>`);
  return badges.join('');
}

function renderGroupUniMmrSummary(group = {}) {
  const accounts = Array.isArray(group.accounts) ? group.accounts : [];
  const counts = { good: 0, warn: 0, bad: 0 };
  accounts.forEach((account) => {
    const toneKey = uniMmrToneKey(account?.uni_mmr);
    if (toneKey) {
      counts[toneKey] += 1;
    }
  });
  const items = [
    counts.good > 0 ? `<span class="group-unimmr-item uni-mmr-good">${fmtCount(counts.good)} ${escapeHtml(groupTextMap.healthy)}</span>` : '',
    counts.warn > 0 ? `<span class="group-unimmr-item uni-mmr-warn">${fmtCount(counts.warn)} ${escapeHtml(groupTextMap.warning)}</span>` : '',
    counts.bad > 0 ? `<span class="group-unimmr-item uni-mmr-bad">${fmtCount(counts.bad)} ${escapeHtml(groupTextMap.danger)}</span>` : '',
  ].filter(Boolean);
  if (!items.length) {
    return '';
  }
  const summaryToneClass = counts.bad > 0 ? 'uni-mmr-bad' : counts.warn > 0 ? 'uni-mmr-warn' : 'uni-mmr-good';
  return `<div class="group-unimmr-summary ${summaryToneClass}"><span class="group-unimmr-label">${escapeHtml(groupTextMap.uniMmr)}</span>${items.join('')}</div>`;
}

function renderAccountListContent(group) {
  const accounts = Array.isArray(group.accounts) ? group.accounts : [];
  const activeAccount = resolveSelectedAccount(group);
  const activeAccountId = String(activeAccount?.account_id || '');
  return `
    <div class="account-switcher">
      ${accounts.map((account) => {
        const accountId = String(account.account_id || '');
        const active = accountId === activeAccountId;
        return `
          <button class="account-switch-button${active ? ' is-active' : ''}" type="button" data-main-account-id="${escapeHtml(String(group.main_account_id || ''))}" data-account-id="${escapeHtml(accountId)}" aria-pressed="${active ? 'true' : 'false'}">
            ${escapeHtml(account.child_account_name || account.account_name || account.account_id || '-')}
          </button>
        `;
      }).join('')}
    </div>
    <div class="account-single-view">${activeAccount ? renderAccount(activeAccount) : '<div class="empty">暂无子账号</div>'}</div>
  `;
}

function renderGroup(group) {
  const summary = group.summary || {};
  const distributionAmount = distributionDisplayAmount(summary, group.profit_summary, group.accounts);
  const mainAccountId = String(group.main_account_id || '');
  const expanded = isGroupExpanded(mainAccountId);
  return `
    <section class="group" data-main-account-id="${escapeHtml(mainAccountId)}">
      <div class="group-head">
        <div><h2>${escapeHtml(group.main_account_name || group.main_account_id || '-')}</h2><div class="mono">${escapeHtml(mainAccountId)}</div></div>
        <div class="group-actions">
          ${renderGroupUniMmrSummary(group)}
          <div class="group-badges">${renderGroupStatusBadges(summary)}</div>
          <button class="group-toggle-button" type="button" data-main-account-id="${escapeHtml(mainAccountId)}" aria-expanded="${expanded ? 'true' : 'false'}">${expanded ? groupTextMap.collapseAccounts : groupTextMap.expandAccounts}</button>
        </div>
      </div>
      <div class="group-summary">
        ${[
          { label: '权益', value: fmt(summary.equity), tone: '' },
          { label: '保证金', value: fmt(summary.margin), tone: '' },
          { label: '可用余额', value: fmt(summary.available_balance), tone: '' },
          { label: '未实现盈亏', value: fmt(summary.unrealized_pnl), tone: numberTone(summary.unrealized_pnl) },
          { label: '分发收益', value: fmt(distributionAmount), tone: numberTone(distributionAmount) },
          { label: '7日年化', value: fmtPercent(summary.distribution_apy_7d), tone: numberTone(summary.distribution_apy_7d) },
        ].map(({ label, value, tone }) => `
          <div class="metric"><div class="label">${label}</div><div class="value ${tone}">${value}</div></div>
        `).join('')}
      </div>
      <div class="account-list${expanded ? '' : ' is-collapsed'}" data-rendered="${expanded ? 'true' : 'false'}">
        ${expanded ? renderAccountListContent(group) : ''}
      </div>
    </section>
  `;
}

function createNodeFromHtml(markup) {
  const template = document.createElement('template');
  template.innerHTML = markup.trim();
  return template.content.firstElementChild;
}

function groupRenderSignature(group) {
  const mainAccountId = String(group.main_account_id || '');
  const expanded = isGroupExpanded(mainAccountId);
  if (!expanded) {
    return JSON.stringify({
      expanded: false,
      main_account_id: mainAccountId,
      main_account_name: group.main_account_name || '',
      summary: group.summary || {},
      profit_summary: group.profit_summary || {},
    });
  }
  return JSON.stringify({ expanded: true, selected_account_id: String(groupSelectedAccountState[mainAccountId] || ''), group });
}

function renderEmptyGroups() {
  groupsContainer.innerHTML = '<section class="group"><div class="group-head"><h2>暂无分组</h2></div></section>';
  groupsContainer.dataset.empty = 'true';
}

function renderGroups(groups = []) {
  if (!groups.length) {
    renderEmptyGroups();
    Object.keys(renderedGroupSignatures).forEach((mainAccountId) => delete renderedGroupSignatures[mainAccountId]);
    return;
  }

  if (groupsContainer.dataset.empty === 'true') {
    groupsContainer.innerHTML = '';
    delete groupsContainer.dataset.empty;
  }

  const desiredIds = new Set();
  const nodeById = new Map(
    Array.from(groupsContainer.querySelectorAll('.group[data-main-account-id]')).map((node) => [String(node.dataset.mainAccountId || ''), node]),
  );

  let previousNode = null;
  for (const group of groups) {
    const mainAccountId = String(group.main_account_id || '');
    if (!mainAccountId) continue;

    desiredIds.add(mainAccountId);
    const signature = groupRenderSignature(group);
    let node = nodeById.get(mainAccountId) || null;

    if (!node) {
      node = createNodeFromHtml(renderGroup(group));
      groupsContainer.appendChild(node);
      nodeById.set(mainAccountId, node);
      renderedGroupSignatures[mainAccountId] = signature;
    } else if (renderedGroupSignatures[mainAccountId] !== signature) {
      const nextNode = createNodeFromHtml(renderGroup(group));
      node.replaceWith(nextNode);
      node = nextNode;
      nodeById.set(mainAccountId, node);
      renderedGroupSignatures[mainAccountId] = signature;
    }

    if (previousNode === null) {
      if (groupsContainer.firstElementChild !== node) {
        groupsContainer.insertBefore(node, groupsContainer.firstElementChild);
      }
    } else if (previousNode.nextElementSibling !== node) {
      groupsContainer.insertBefore(node, previousNode.nextElementSibling);
    }
    previousNode = node;
  }

  nodeById.forEach((node, mainAccountId) => {
    if (!desiredIds.has(mainAccountId)) {
      node.remove();
      delete renderedGroupSignatures[mainAccountId];
    }
  });
}

function replaceGroup(mainAccountId) {
  const group = findGroupByMainId(mainAccountId);
  const groupNode = findGroupNode(mainAccountId);
  if (!group) {
    if (groupNode) {
      groupNode.remove();
      delete renderedGroupSignatures[mainAccountId];
    }
    return;
  }
  if (!groupNode) {
    renderGroups(currentGroups());
    return;
  }
  const nextNode = createNodeFromHtml(renderGroup(group));
  groupNode.replaceWith(nextNode);
  renderedGroupSignatures[mainAccountId] = groupRenderSignature(group);
}

function handleGroupToggle(mainAccountId) {
  const group = findGroupByMainId(mainAccountId);
  const groupNode = findGroupNode(mainAccountId);
  if (!group || !groupNode) {
    replaceGroup(mainAccountId);
    return;
  }

  const expanded = !isGroupExpanded(mainAccountId);
  groupExpandedState[mainAccountId] = expanded;

  const toggleButton = groupNode.querySelector('.group-toggle-button');
  const accountList = groupNode.querySelector('.account-list');
  if (!(toggleButton instanceof HTMLButtonElement) || !(accountList instanceof HTMLElement)) {
    replaceGroup(mainAccountId);
    return;
  }

  toggleButton.textContent = expanded ? groupTextMap.collapseAccounts : groupTextMap.expandAccounts;
  toggleButton.setAttribute('aria-expanded', expanded ? 'true' : 'false');

  if (!expanded) {
    accountList.classList.add('is-collapsed');
    renderedGroupSignatures[mainAccountId] = groupRenderSignature(group);
    return;
  }

  if (accountList.dataset.rendered === 'true' && accountList.childElementCount > 0) {
    accountList.classList.remove('is-collapsed');
    renderedGroupSignatures[mainAccountId] = groupRenderSignature(group);
    return;
  }

  replaceGroup(mainAccountId);
}

function handleAccountSwitch(mainAccountId, accountId) {
  const group = findGroupByMainId(mainAccountId);
  const groupNode = findGroupNode(mainAccountId);
  if (!group || !groupNode) {
    replaceGroup(mainAccountId);
    return;
  }

  groupExpandedState[mainAccountId] = true;
  groupSelectedAccountState[mainAccountId] = accountId;

  const accountList = groupNode.querySelector('.account-list');
  const accountSingleView = groupNode.querySelector('.account-single-view');
  if (!(accountList instanceof HTMLElement) || !(accountSingleView instanceof HTMLElement) || accountList.dataset.rendered !== 'true') {
    replaceGroup(mainAccountId);
    return;
  }

  accountList.classList.remove('is-collapsed');
  groupNode.querySelectorAll('.account-switch-button').forEach((button) => {
    if (!(button instanceof HTMLButtonElement)) return;
    const active = String(button.dataset.accountId || '') === accountId;
    button.classList.toggle('is-active', active);
    button.setAttribute('aria-pressed', active ? 'true' : 'false');
  });

  const activeAccount = resolveSelectedAccount(group);
  accountSingleView.innerHTML = activeAccount ? renderAccount(activeAccount) : '<div class="empty">暂无子账号</div>';
  renderedGroupSignatures[mainAccountId] = groupRenderSignature(group);
}

function render(payload) {
  latestPayload = payload;
  const groups = Array.isArray(payload.groups) ? payload.groups : [];
  normalizeGroupUiState(groups);

  connectionBadge.textContent = textStatus(payload.status || 'idle');
  connectionBadge.className = `badge ${statusClass(payload.status || 'idle')}`;
  messageText.textContent = textMessage(payload.message);
  updatedAt.textContent = `更新时间：${fmtTime(payload.updated_at)}`;

  syncMonitorToggle(payload);
  renderToolbarStats(payload.summary || {});
  renderSummary(payload.summary || {});
  renderProfitSummary(payload.profit_summary || {});
  renderGroups(groups);
  applyActionButtonState();
}

function scheduleStreamRender(payload) {
  pendingStreamPayload = payload;
  if (pendingStreamFrame !== null) return;
  pendingStreamFrame = window.requestAnimationFrame(() => {
    pendingStreamFrame = null;
    const nextPayload = pendingStreamPayload;
    pendingStreamPayload = null;
    if (nextPayload) render(nextPayload);
  });
}

function resetMonitorV2TestState() {
  toggleBusy = false;
  refreshBusy = false;
  importBusy = false;
  refreshCooldownSeconds = 0;
  if (refreshCooldownTimer !== null) {
    window.clearInterval(refreshCooldownTimer);
    refreshCooldownTimer = null;
  }

  fundingModalBusy = false;
  fundingRefreshBusy = false;
  fundingRefreshCooldownSeconds = 0;
  if (fundingRefreshCooldownTimer !== null) {
    window.clearInterval(fundingRefreshCooldownTimer);
    fundingRefreshCooldownTimer = null;
  }

  fundingOverview = null;
  fundingDirection = 'distribute';
  fundingSelectedGroupId = '';
  fundingSelectedAsset = '';
  fundingSelectionState = {};
  fundingSyncAmountEnabled = false;
  fundingLogEntries = [];
  fundingLogCounter = 0;
  fundingLastCapabilitySignature = '';
  fundingAuditEntries = [];
  fundingAuditDetailsByOperationId = {};
  fundingAuditBusy = false;
  fundingActiveLogTab = 'runtime';
  fundingPendingOperationId = '';
  fundingAuditSelectedOperationId = '';
  fundingAuditFilter = '';

  toolbarStatsSignature = '';
  summarySignature = '';
  profitSummarySignature = '';
  pendingStreamPayload = null;
  if (pendingStreamFrame !== null) {
    window.cancelAnimationFrame(pendingStreamFrame);
    pendingStreamFrame = null;
  }
  latestPayload = null;

  if (fundingModalShell) fundingModalShell.classList.add('hidden');
  if (fundingRows) fundingRows.innerHTML = '';
  if (fundingMainSummary) fundingMainSummary.innerHTML = '';
  if (groupsContainer) groupsContainer.innerHTML = '';
  if (summaryCards) summaryCards.innerHTML = '';
  if (profitCards) profitCards.innerHTML = '';
  if (toolbarStats) toolbarStats.innerHTML = '';
  if (messageText) messageText.textContent = '';
  if (updatedAt) updatedAt.textContent = '-';

  renderFundingLogPanel();
  renderFundingOperationMeta();
  applyActionButtonState();
}

function describeRefreshResult(refreshResult, elapsedSeconds) {
  if (!refreshResult) return `刷新完成，耗时 ${elapsedSeconds} 秒`;
  if (refreshResult.success) {
    const fallbackSections = Array.isArray(refreshResult.fallback_sections) ? refreshResult.fallback_sections : [];
    return fallbackSections.length > 0
      ? `刷新成功，耗时 ${elapsedSeconds} 秒，部分数据沿用了上一轮成功结果`
      : `刷新成功，耗时 ${elapsedSeconds} 秒，数据已更新`;
  }
  if (refreshResult.timeout) return refreshResult.message || '刷新超时，已保留当前数据';
  return refreshResult.message || '刷新失败，已保留当前数据';
}
async function setMonitorEnabled(enabled) {
  toggleBusy = true;
  applyActionButtonState();
  try {
    const response = await apiFetch('/api/monitor/control', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ enabled }),
    });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
    render(payload);
  } catch (error) {
    monitorToggle.checked = !enabled;
    messageText.textContent = `监控状态更新失败：${error}`;
  } finally {
    toggleBusy = false;
    applyActionButtonState();
  }
}

async function refreshNow() {
  if (refreshBusy) {
    messageText.textContent = '上一轮刷新仍在进行中，当前数据保持不变';
    return;
  }
  refreshBusy = true;
  applyActionButtonState();
  const refreshStartedAt = Date.now();
  messageText.textContent = '正在刷新，当前数据保持不变，等待新数据返回后自动更新';
  try {
    const response = await apiFetch('/api/monitor/refresh', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
    render(payload);
    const elapsedSeconds = Math.max(1, Math.round((Date.now() - refreshStartedAt) / 1000));
    messageText.textContent = describeRefreshResult(payload.refresh_result, elapsedSeconds);
  } catch (error) {
    messageText.textContent = `立即刷新失败：${error}`;
  } finally {
    refreshBusy = false;
    applyActionButtonState();
  }
}

function describeImportResult(payload = {}) {
  const result = payload.import_result || {};
  const groupCount = fmtCount(result.main_account_count || 0);
  const accountCount = fmtCount(result.account_count || 0);
  const settingsOnly = result.mode === 'settings_only';
  const updatedSettingsCount = Array.isArray(result.updated_settings_keys) ? result.updated_settings_keys.length : 0;
  const updatedSettings = !settingsOnly && Array.isArray(result.updated_settings_keys) && result.updated_settings_keys.length > 0
    ? `；已更新 ${fmtCount(result.updated_settings_keys.length)} 项敏感配置`
    : '';
  const securityNotice = payload.security_notice ? `；${payload.security_notice}` : '';
  const baseMessage = settingsOnly
    ? `Excel 导入成功，已更新 ${fmtCount(updatedSettingsCount)} 项敏感配置`
    : payload.refresh_result?.success === false
      ? `Excel 导入成功，已覆盖 ${groupCount} 个分组 / ${accountCount} 个账户，但刷新失败：${payload.refresh_result.message || '-'}`
      : `Excel 导入成功，已覆盖 ${groupCount} 个分组 / ${accountCount} 个账户`;
  return `${baseMessage}${updatedSettings}${securityNotice}`;
}

function parseDownloadFilename(contentDisposition) {
  if (!contentDisposition) return 'monitor_accounts_template.xlsx';
  const match = contentDisposition.match(/filename="?([^";]+)"?/i);
  return match ? match[1] : 'monitor_accounts_template.xlsx';
}

async function downloadTemplate() {
  if (importBusy || refreshBusy || toggleBusy) return;
  messageText.textContent = '正在下载 Excel 模板';
  try {
    const response = await apiFetch('/api/config/import/excel-template', { method: 'GET', cache: 'no-store' });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const blob = await response.blob();
    const filename = parseDownloadFilename(response.headers.get('content-disposition'));
    const url = window.URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = filename;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    window.URL.revokeObjectURL(url);
    messageText.textContent = 'Excel 模板已开始下载';
  } catch (error) {
    messageText.textContent = `模板下载失败：${error}`;
  }
}

async function uploadExcel(file) {
  if (!file || importBusy) return;
  importBusy = true;
  applyActionButtonState();
  messageText.textContent = '正在导入 Excel 配置，现有数据保持不变';
  try {
    const formData = new FormData();
    formData.append('file', file);
    const response = await apiFetch('/api/config/import/excel', { method: 'POST', body: formData });
    const payload = await readApiPayload(response);
    if (!response.ok) throw new Error(payload.detail || payload.message || `HTTP ${response.status}`);
    render(payload);
    messageText.textContent = describeImportResult(payload);
  } catch (error) {
    messageText.textContent = `Excel 导入失败：${error}`;
  } finally {
    importBusy = false;
    importInput.value = '';
    applyActionButtonState();
  }
}

async function bootstrap() {
  try {
    await loadAuthSession();
    const response = await apiFetch(groupsUrl, { cache: 'no-store' });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
    render(payload);
  } catch (error) {
    messageText.textContent = `初始化失败：${error}`;
    applyActionButtonState();
  }

  monitorToggle.addEventListener('change', () => {
    if (!toggleBusy) setMonitorEnabled(monitorToggle.checked);
  });
  refreshButton.addEventListener('click', () => {
    if (refreshBusy || refreshCooldownSeconds > 0) return;
    startRefreshCooldown(30);
    refreshNow();
  });
  downloadTemplateButton.addEventListener('click', downloadTemplate);
  importButton.addEventListener('click', () => {
    if (!importBusy && !refreshBusy) importInput.click();
  });
  logoutButton?.addEventListener('click', async () => {
    try {
      const response = await apiFetch('/api/auth/logout', { method: 'POST' });
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
      }
    } catch (error) {
      messageText.textContent = `退出登录失败：${error}`;
      return;
    }
    window.location.replace('/login');
  });
  importInput.addEventListener('change', () => {
    const file = importInput.files?.[0];
    if (file) uploadExcel(file);
  });

  fundingTransferButton.addEventListener('click', () => {
    if (!fundingTransferButton.disabled) openFundingModal();
  });
  fundingRefreshButton.addEventListener('click', () => {
    if (!fundingRefreshButton.disabled) {
      refreshFundingOverviewNow({ useCooldown: true });
    }
  });
  fundingModalClose.addEventListener('click', closeFundingModal);
  fundingModalShell.addEventListener('click', (event) => {
    const target = event.target;
    if (target instanceof HTMLElement && target.dataset.closeFundingModal === 'true') closeFundingModal();
  });
  fundingGroupSelect.addEventListener('change', () => {
    fundingSelectedGroupId = fundingGroupSelect.value;
    fundingSelectedAsset = '';
    resetFundingSyncAmountState();
    appendFundingLog(`已切换分组：${fundingGroupSelect.selectedOptions[0]?.textContent || fundingSelectedGroupId || '-'}`, 'info');
    loadFundingOverview(fundingSelectedGroupId, { resetState: true });
  });
  fundingModeDistribute.addEventListener('click', () => {
    if (fundingDirection === 'distribute') {
      return;
    }
    fundingDirection = 'distribute';
    resetFundingSelectionState();
    resetFundingSyncAmountState();
    appendFundingLog('已切换到主账号分发模式。', 'info');
    renderFundingModal();
  });
  fundingModeCollect.addEventListener('click', () => {
    if (fundingDirection === 'collect') {
      return;
    }
    fundingDirection = 'collect';
    resetFundingSelectionState();
    resetFundingSyncAmountState();
    appendFundingLog('已切换到子账号归集模式。', 'info');
    renderFundingModal();
  });
  fundingAssetSelect.addEventListener('change', () => {
    fundingSelectedAsset = fundingAssetSelect.value;
    resetFundingSelectionState();
    resetFundingSyncAmountState();
    appendFundingLog(`已切换代币：${fundingSelectedAsset || '-'}`, 'info');
    renderFundingModal();
  });
  fundingSyncAmountCheckbox.addEventListener('change', () => {
    fundingSyncAmountEnabled = fundingSyncAmountCheckbox.checked;
    if (fundingSyncAmountEnabled) {
      syncFundingAmountsFromMaster({ render: true });
      return;
    }
    renderFundingRows();
  });
  fundingSelectAllCheckbox.addEventListener('change', () => {
    const shouldCheck = fundingSelectAllCheckbox.checked;
    fundingSelectableRows().forEach((row) => {
      const accountId = String(row.account_id || '');
      if (!accountId) {
        return;
      }
      fundingRowState(accountId).checked = shouldCheck;
    });
    if (fundingSyncAmountEnabled) {
      syncFundingAmountsFromMaster({ render: true });
      return;
    }
    renderFundingRows();
  });
  fundingRows.addEventListener('input', (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.matches('[data-funding-amount-id]')) {
      const accountId = String(target.getAttribute('data-funding-amount-id') || '');
      fundingRowState(accountId).amount = target.value;
      if (fundingSyncAmountEnabled && accountId === fundingMasterAccountId()) {
        syncFundingAmountsFromMaster({ render: false });
      }
    }
  });
  fundingRows.addEventListener('change', (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.matches('[data-funding-account-id]')) {
      const accountId = String(target.getAttribute('data-funding-account-id') || '');
      fundingRowState(accountId).checked = target.checked;
      if (fundingSyncAmountEnabled && target.checked && accountId !== fundingMasterAccountId()) {
        fundingRowState(accountId).amount = fundingRowState(fundingMasterAccountId()).amount || '';
      }
      renderFundingRows();
    }
  });
  fundingRows.addEventListener('click', (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const maxButton = target.closest('[data-funding-max-id]');
    if (!(maxButton instanceof HTMLButtonElement)) return;
    const accountId = String(maxButton.getAttribute('data-funding-max-id') || '');
    const row = Array.isArray(fundingOverview?.children)
      ? fundingOverview.children.find((entry) => String(entry.account_id || '') === accountId)
      : null;
    if (!row) return;
    const state = fundingRowState(accountId);
    state.amount = fundingAssetValue(row);
    if (fundingSyncAmountEnabled && accountId === fundingMasterAccountId()) {
      syncFundingAmountsFromMaster({ render: true });
      return;
    }
    renderFundingRows();
  });
  fundingQuickCollectButton.addEventListener('click', applyFundingQuickCollectPreset);
  fundingQuickClearButton.addEventListener('click', applyFundingQuickClearPreset);
  fundingSubmitButton.addEventListener('click', submitFundingOperation);
  fundingOperationCopyButton?.addEventListener('click', copyFundingOperationId);
  fundingLogTabRuntime.addEventListener('click', () => {
    fundingActiveLogTab = 'runtime';
    renderFundingLogPanel();
  });
  fundingLogTabAudit.addEventListener('click', async () => {
    fundingActiveLogTab = 'audit';
    renderFundingLogPanel();
    await ensureFundingAuditDetailLoaded(fundingSelectedGroupId, { preserveOnError: true });
  });
  fundingLogList.addEventListener('input', async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.matches('[data-funding-audit-filter]')) {
      fundingAuditFilter = target.value;
      ensureFundingAuditSelection();
      renderFundingLogPanel();
      renderFundingOperationMeta();
      if (fundingActiveLogTab === 'audit') {
        await ensureFundingAuditDetailLoaded(fundingSelectedGroupId, { preserveOnError: true });
      }
    }
  });
  fundingLogList.addEventListener('click', async (event) => {
    const target = event.target;
    if (!(target instanceof HTMLElement)) return;
    const auditSelectButton = target.closest('[data-funding-audit-select]');
    if (!(auditSelectButton instanceof HTMLElement)) return;
    const entryKey = String(auditSelectButton.getAttribute('data-funding-audit-select') || '');
    if (!entryKey) return;
    fundingAuditSelectedOperationId = entryKey;
    renderFundingLogPanel();
    if (!getFundingAuditDetail(entryKey)) {
      const { direction, operationId } = parseFundingAuditEntryKey(entryKey);
      if (!direction || !operationId) return;
      await loadFundingAuditDetail(fundingSelectedGroupId, operationId, direction, { preserveOnError: true });
    }
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && !fundingModalShell.classList.contains('hidden')) closeFundingModal();
  });

  groupsContainer.addEventListener('click', (event) => {
    const target = event.target;
    if (!(target instanceof Element)) return;

    const toggleButton = target.closest('.group-toggle-button');
    if (toggleButton instanceof HTMLElement) {
      const mainAccountId = String(toggleButton.dataset.mainAccountId || '');
      if (mainAccountId) handleGroupToggle(mainAccountId);
      return;
    }

    const switchButton = target.closest('.account-switch-button');
    if (switchButton instanceof HTMLElement) {
      const mainAccountId = String(switchButton.dataset.mainAccountId || '');
      const accountId = String(switchButton.dataset.accountId || '');
      if (mainAccountId && accountId) handleAccountSwitch(mainAccountId, accountId);
    }
  });

  const source = new EventSource(streamUrl);
  source.addEventListener('monitor_snapshot', (event) => {
    scheduleStreamRender(JSON.parse(event.data));
  });
  source.onerror = () => {
    connectionBadge.textContent = textStatus('reconnecting');
    connectionBadge.className = `badge ${statusClass('reconnecting')}`;
  };
  window.addEventListener('beforeunload', () => {
    if (pendingStreamFrame !== null) {
      window.cancelAnimationFrame(pendingStreamFrame);
      pendingStreamFrame = null;
    }
    source.close();
  });
}

window.__monitorV2 = {
  render,
  renderAccount,
  scheduleStreamRender,
  appendFundingLog,
  renderFundingLogPanel,
  renderFundingModal,
  openFundingModal,
  closeFundingModal,
  bootstrap,
  refreshFundingOverviewNow,
  submitFundingOperation,
  copyFundingOperationId,
  resetTestState: resetMonitorV2TestState,
  getFundingLogEntries: () => [...fundingLogEntries],
  setFundingOverview: (overview) => {
    fundingOverview = overview;
  },
  setFundingDirection: (direction) => {
    fundingDirection = direction;
  },
  setFundingSelectedGroupId: (groupId) => {
    fundingSelectedGroupId = groupId;
  },
  setFundingSelectedAsset: (asset) => {
    fundingSelectedAsset = asset;
  },
  setLatestPayload: (payload) => {
    latestPayload = payload;
  },
  setFundingSelectionState: (selectionState) => {
    fundingSelectionState = selectionState || {};
  },
  setFundingActiveLogTab: (tab) => {
    fundingActiveLogTab = tab === 'audit' ? 'audit' : 'runtime';
    renderFundingLogPanel();
  },
  getFundingAuditEntries: () => [...fundingAuditEntries],
  getFundingAuditDetail,
  loadFundingAudit,
  loadFundingAuditDetail,
  uploadExcel,
  setFundingAuditFilter: (value) => {
    fundingAuditFilter = String(value || '');
    renderFundingLogPanel();
    renderFundingOperationMeta();
    if (fundingActiveLogTab === 'audit') {
      return ensureFundingAuditDetailLoaded(fundingSelectedGroupId, { preserveOnError: true });
    }
  },
  setFundingPendingOperationId: (value) => {
    fundingPendingOperationId = String(value || '');
    renderFundingOperationMeta();
  },
  describeImportResult,
};

applyActionButtonState();
renderFundingLogPanel();
if (!TEST_MODE) {
  bootstrap();
}
