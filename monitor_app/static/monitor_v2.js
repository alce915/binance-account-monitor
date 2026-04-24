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
const fundingListBody = fundingModalShell?.querySelector('.funding-list-body') || null;
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

const uiMessages = window.MONITOR_V2_I18N?.messages || {};
function messageByPath(path) {
  return String(path || '').split('.').reduce((current, segment) => (
    current && Object.prototype.hasOwnProperty.call(current, segment) ? current[segment] : undefined
  ), uiMessages);
}

function applyTemplate(value, values = {}) {
  return String(value ?? '').replace(/\{([a-zA-Z0-9_]+)\}/g, (match, key) => (
    Object.prototype.hasOwnProperty.call(values, key) ? String(values[key]) : match
  ));
}

function t(path, values = {}) {
  const value = messageByPath(path);
  return applyTemplate(typeof value === 'string' ? value : path, values);
}

function tArray(path) {
  const value = messageByPath(path);
  return Array.isArray(value) ? value : [];
}

function applyStaticI18n(root = document) {
  root.querySelectorAll('[data-i18n]').forEach((element) => {
    element.textContent = t(element.dataset.i18n);
  });
  root.querySelectorAll('[data-i18n-aria-label]').forEach((element) => {
    element.setAttribute('aria-label', t(element.dataset.i18nAriaLabel));
  });
  const titleElement = root.querySelector('[data-i18n-title]');
  if (titleElement) {
    const title = t(titleElement.dataset.i18nTitle);
    titleElement.textContent = title;
    document.title = title;
  }
}

const statusTextMap = {
  ok: t('status.ok'),
  partial: t('status.partial'),
  error: t('status.error'),
  idle: t('status.idle'),
  reconnecting: t('status.reconnecting'),
  disabled: t('status.disabled'),
};
const messageTextMap = {
  'Waiting for monitor connection': t('backendMessages.waitingConnection'),
  'No accounts available': t('backendMessages.noAccounts'),
  'All accounts are healthy': t('backendMessages.allHealthy'),
  'All accounts failed': t('backendMessages.allFailed'),
  'Some accounts failed': t('backendMessages.someFailed'),
  'Monitoring disabled': t('backendMessages.monitoringDisabled'),
  'Monitor accounts reloaded': t('backendMessages.accountsReloaded'),
  'Refresh completed': t('backendMessages.refreshCompleted'),
};
const accountStatusTextMap = {
  NORMAL: t('accountStatus.normal'),
  ERROR: t('accountStatus.error'),
  ABNORMAL: t('accountStatus.abnormal'),
  DISABLED: t('accountStatus.disabled'),
};
const positionSideTextMap = { LONG: t('positionSide.long'), SHORT: t('positionSide.short'), BOTH: t('positionSide.both') };
const groupTextMap = {
  expandAccounts: t('group.expandAccounts'),
  collapseAccounts: t('group.collapseAccounts'),
  healthy: t('group.healthy'),
  warning: t('group.warning'),
  danger: t('group.danger'),
  accounts: t('group.accounts'),
  uniMmr: t('group.uniMmr'),
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

applyStaticI18n();

const fmt = (value) => {
  const number = Number(value ?? 0);
  return Number.isFinite(number)
    ? number.toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
    : String(value ?? '-');
};
const fmtCurrency = (value) => {
  const number = Number(value ?? 0);
  if (!Number.isFinite(number)) {
    return String(value ?? '-');
  }
  const formatted = Math.abs(number).toLocaleString('zh-CN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return `${number < 0 ? '-' : ''}$${formatted}`;
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
const maskMainUid = (value) => {
  const text = String(value ?? '').trim();
  if (!text) return '-';
  if (text.includes('*')) return text;
  if (text.length <= 2) return '*'.repeat(text.length);
  if (text.length <= 5) return `${text.slice(0, 1)}${'*'.repeat(Math.max(text.length - 2, 1))}${text.slice(-1)}`;
  return `${text.slice(0, 3)}${'*'.repeat(Math.max(text.length - 5, 3))}${text.slice(-2)}`;
};

const textStatus = (status) => statusTextMap[status] || status || t('status.unknown');
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
const accountStatusToneClass = (status) => {
  const normalized = String(status || '').trim().toUpperCase();
  if (!normalized || normalized === 'NORMAL') return 'account-subtitle-status-ok';
  return 'account-subtitle-status-error';
};
const accountSubtitleStatusText = (account) => {
  const accountStatus = String(account?.account_status || '').trim();
  if (accountStatus) {
    return textAccountStatus(accountStatus);
  }
  return textStatus(account?.status);
};
const accountSubtitleStatusToneClass = (account) => {
  const accountStatus = String(account?.account_status || '').trim().toUpperCase();
  if (accountStatus) {
    return accountStatusToneClass(accountStatus);
  }
  return String(account?.status || '') === 'ok' ? 'account-subtitle-status-ok' : 'account-subtitle-status-error';
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
const refreshButtonLabel = () => (refreshCooldownSeconds > 0
  ? t('actions.refreshCountdown', { seconds: refreshCooldownSeconds })
  : t('actions.refreshNow'));
const fundingRefreshButtonLabel = () => (fundingRefreshCooldownSeconds > 0
  ? t('actions.refreshCountdown', { seconds: fundingRefreshCooldownSeconds })
  : t('actions.refreshNow'));
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
  if (authSource === 'whitelist') return t('roles.whitelist');
  if (authSource === 'disabled') return t('roles.disabled');
  if (authSource === 'break_glass') return t('roles.breakGlass');
  if (value === 'guest') return t('roles.guest');
  return t('roles.admin');
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
    throw new Error(payload?.error?.message || t('auth.notInitialized'));
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
    throw new Error(t('auth.expired'));
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
  importButton.textContent = importBusy ? t('actions.importing') : t('actions.importExcel');
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
    { label: t('toolbar.totalAccounts'), value: fmtCount(summary.account_count), tone: '' },
    { label: t('toolbar.healthyAccounts'), value: fmtCount(summary.success_count), tone: 'value-positive' },
    { label: t('toolbar.abnormalAccounts'), value: fmtCount(summary.error_count), tone: 'value-negative' },
  ];
  toolbarStats.innerHTML = stats.map(({ label, value, tone }) => `
    <div class="stat-pill toolbar-stat-pill toolbar-stat-pill-refined toolbar-stat-pill-clipped">
      <span class="toolbar-stat-pill-surface" aria-hidden="true"></span>
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
    { label: t('summary.equity'), value: fmtCurrency(summary.equity), tone: '' },
    { label: t('summary.margin'), value: fmtCurrency(summary.margin), tone: '' },
    { label: t('summary.availableBalance'), value: fmtCurrency(summary.available_balance), tone: '' },
    { label: t('summary.unrealizedPnl'), value: fmtCurrency(summary.unrealized_pnl), tone: numberTone(summary.unrealized_pnl) },
    { label: t('summary.commission'), value: fmtCurrency(summary.total_commission), tone: numberTone(summary.total_commission) },
    { label: t('summary.apy7d'), value: fmtPercent(summary.distribution_apy_7d), tone: numberTone(summary.distribution_apy_7d) },
  ];
  summaryCards.innerHTML = cards.map(({ label, value, tone }) => `
    <article class="card summary-card summary-card-refined summary-card-proportional metric">
      <div class="label summary-card-label metric-label metric-label-emphasis">${label}</div>
      <div class="value summary-card-value metric-value metric-value-proportional ${tone}">${value}</div>
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
    const note = period.complete ? '' : `<div class="card-note profit-card-note">${escapeHtml(t('profit.historyBackfill'))}</div>`;
    return `
      <article class="profit-card linked-cell${period.complete ? '' : ' is-incomplete'}">
        <div class="label profit-card-label linked-label linked-label-emphasis">${escapeHtml(period.label || '-')}</div>
        <div class="value profit-card-value linked-value ${tone}">${fmtCurrency(period.amount)} | ${fmtPercent(period.rate)}</div>
        ${note}
      </article>
    `;
  }).join('');
}

function renderRows(headers, rows, formatter, emptyLabel = t('empty.data')) {
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
  if (level === 'success') return t('funding.logs.success');
  if (level === 'error') return t('funding.logs.error');
  return t('funding.logs.info');
}

function fundingToneToLogLevel(tone = '') {
  if (tone === 'is-success') return 'success';
  if (tone === 'is-error') return 'error';
  return 'info';
}

function fundingOperationMetaText() {
  if (fundingOverview && fundingOverview.write_enabled === false) {
    return t('funding.writeProtection', {
      reason: fundingOverview?.write_disabled_reason || t('funding.writeDisabledReason'),
    });
  }
  return t('funding.operationId', { id: shortOperationId(fundingCurrentOperationId()) });
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
    return `<div class="funding-log-empty">${escapeHtml(t('funding.audit.chooseRecord'))}</div>`;
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
            <div>${escapeHtml(t('funding.audit.requestedAndExecuted', {
              requested: result.requested_amount || '-',
              executed: result.executed_amount || '-',
            }))}</div>
            <div>${escapeHtml(t('funding.audit.precheckAvailable', { amount: result.precheck_available_amount || '-' }))}</div>
          </div>
          <div>${escapeHtml(result.message || '-')}</div>
        </div>
      `).join('')
    : `<div class="funding-log-empty">${escapeHtml(t('funding.audit.noDetails'))}</div>`;

  return `
    <div class="funding-audit-detail-head">
      <div class="funding-audit-detail-title">
        <strong>${escapeHtml(detail.direction === 'collect' ? t('funding.audit.collect') : t('funding.audit.distribute'))}</strong>
        <div class="mono">${escapeHtml(t('funding.audit.operationId', { id: shortOperationId(detail.operation_id || '-') }))}</div>
      </div>
      <div class="funding-log-badge is-${escapeHtml(fundingAuditStatusLevel(detail.operation_status || ''))}">
        ${escapeHtml(fundingAuditStatusLabel(detail.operation_status || ''))}
      </div>
    </div>
    <div class="funding-audit-detail-grid">
      <div class="funding-audit-detail-card">
        <span>${escapeHtml(t('funding.audit.asset'))}</span>
        <strong>${escapeHtml(summary.asset || detail.asset || '-')}</strong>
      </div>
      <div class="funding-audit-detail-card">
        <span>${escapeHtml(t('funding.audit.stage'))}</span>
        <strong>${escapeHtml(detail.execution_stage || '-')}</strong>
      </div>
      <div class="funding-audit-detail-card">
        <span>${escapeHtml(t('funding.audit.requestedTotal'))}</span>
        <strong>${escapeHtml(summary.requested_total_amount || precheck.requested_total_amount || '0')}</strong>
      </div>
      <div class="funding-audit-detail-card">
        <span>${escapeHtml(t('funding.audit.reconciliation'))}</span>
        <strong>${escapeHtml(reconciliation.status || '-')}</strong>
      </div>
    </div>
    <div class="funding-audit-section">
      <h4>${escapeHtml(t('funding.audit.summary'))}</h4>
      <div class="funding-audit-result">
        <div class="funding-audit-result-meta">
          <div>${escapeHtml(t('funding.audit.precheckAccounts', { count: String(precheck.validated_account_count ?? 0) }))}</div>
          <div>${escapeHtml(t('funding.audit.attempts', {
            attempted: String(summary.attempted_count ?? 0),
            success: String(summary.success_count ?? 0),
            failure: String(summary.failure_count ?? 0),
          }))}</div>
          <div>${escapeHtml(t('funding.audit.confirmations', {
            confirmed: String(summary.confirmed_count ?? 0),
            pending: String(summary.pending_confirmation_count ?? 0),
          }))}</div>
          <div>${escapeHtml(t('funding.audit.mainDirection', { direction: summary.expected_main_direction || '-' }))}</div>
          <div>${escapeHtml(t('funding.audit.mainBeforeAfter', {
            before: summary.main_before_available_amount || '-',
            after: summary.main_after_available_amount || '-',
          }))}</div>
        </div>
        <div>${escapeHtml(detail.message || t('funding.audit.recorded'))}</div>
        ${unconfirmed.length ? `<div class="mono">${escapeHtml(t('funding.audit.unconfirmedAccounts', { accounts: unconfirmed.join(', ') }))}</div>` : ''}
      </div>
    </div>
    <div class="funding-audit-section">
      <h4>${escapeHtml(t('funding.audit.details'))}</h4>
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
              <div class="funding-audit-item-message">${escapeHtml(entry.message || t('funding.audit.recorded'))}</div>
            </button>
          `).join('') : `<div class="funding-log-empty">${escapeHtml(t('funding.audit.noRecords'))}</div>`}
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
    fundingLogList.innerHTML = `<div class="funding-log-empty">${escapeHtml(t('funding.logs.empty'))}</div>`;
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
    return { success: false, error: t('funding.audit.noDetailToLoad') };
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
    appendFundingLog(t('funding.audit.loadDetailFailed', { error }), 'error');
    return { success: false, error: String(error) };
  }
}

async function loadFundingAudit(mainAccountId, { preserveOnError = true } = {}) {
  if (!mainAccountId) {
    fundingAuditEntries = [];
    fundingAuditDetailsByOperationId = {};
    fundingAuditSelectedOperationId = '';
    renderFundingLogPanel();
    return { success: false, error: t('funding.noGroups') };
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
    appendFundingLog(t('funding.audit.loadListFailed', { error }), 'error');
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
    return { success: false, error: t('funding.audit.noRecords') };
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
    return row?.reason_distribute || row?.reason || t('funding.unavailable');
  }
  return row?.reason_collect || row?.reason || t('funding.unavailable');
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
  appendFundingLog(t('funding.quickCollectFilled', { asset: fundingSelectedAsset }), 'info');
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
  appendFundingLog(t('funding.quickClearDone'), 'info');
  renderFundingRows();
  applyActionButtonState();
}

function fundingCapabilityState() {
  const modeAvailable = fundingModeAvailable();
  return {
    message: modeAvailable
      ? (fundingDirection === 'distribute' ? t('funding.distributeAvailable') : t('funding.collectAvailable'))
      : fundingOverview?.reason || (fundingDirection === 'distribute' ? t('funding.distributeUnavailable') : t('funding.collectUnavailable')),
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
      <div class="funding-stat funding-identity-card is-identity">
        <div class="funding-identity-head">
          <div class="funding-identity-content">
            <div class="funding-identity-title">${escapeHtml(fundingOverview?.main_account_name || fundingSelectedGroupId || '-')}</div>
            <div class="funding-identity-meta mono">${escapeHtml(maskMainUid(mainAccount.uid || '-'))}</div>
          </div>
          <div class="badge funding-identity-badge ${mainAccount.transfer_ready ? 'status-ok' : 'status-error'}">
            ${escapeHtml(mainAccount.transfer_ready ? t('funding.ready') : t('funding.unavailable'))}
          </div>
        </div>
      </div>
      <div class="funding-stat"><div class="label funding-stat-label">${escapeHtml(t('funding.currentToken'))}</div><div class="value funding-stat-value">${escapeHtml(fundingSelectedAsset || '-')}</div></div>
      <div class="funding-stat"><div class="label funding-stat-label">${escapeHtml(t('funding.mainSpotAvailable'))}</div><div class="value funding-stat-value">${fmt(fundingAssetValue(mainAccount))}</div></div>
      <div class="funding-stat"><div class="label funding-stat-label">${escapeHtml(t('funding.collectApiStatus'))}</div><div class="value funding-stat-value">${escapeHtml(mainAccount.transfer_ready ? t('funding.configured') : mainAccount.reason || t('funding.unconfigured'))}</div></div>
    </div>
  `;
}

function fundingDataRows() {
  if (!fundingRows) return [];
  return Array.from(fundingRows.querySelectorAll('tr')).filter((row) => {
    if (row.classList.contains('funding-table-spacer')) return false;
    return !row.querySelector('.empty');
  });
}

function syncFundingListBodyHeight() {
  if (!fundingListBody) return;
  const rows = fundingDataRows();
  if (!rows.length) {
    fundingListBody.style.removeProperty('--funding-list-max-height');
    fundingListBody.classList.remove('is-scrollable');
    return;
  }

  const tableHead = fundingListBody.querySelector('.funding-table thead');
  const measuredHeaderHeight = Math.ceil(tableHead?.getBoundingClientRect().height || tableHead?.offsetHeight || 0);
  const visibleRows = rows.slice(0, 5);
  const measuredRowsHeight = visibleRows.reduce((total, row) => {
    const rowHeight = Math.ceil(row.getBoundingClientRect().height || row.offsetHeight || 0);
    return total + Math.max(rowHeight, 72);
  }, 0);
  const headerHeight = Math.max(measuredHeaderHeight, 56);
  const targetHeight = headerHeight + measuredRowsHeight + 2;

  fundingListBody.style.setProperty('--funding-list-max-height', `${targetHeight}px`);
  fundingListBody.classList.toggle('is-scrollable', rows.length > visibleRows.length);
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
            <input class="funding-amount-input" type="number" min="0" step="0.00000001" placeholder="${escapeHtml(distributeMode ? t('funding.inputAmount') : t('funding.inputCollectAmount'))}" data-funding-amount-id="${escapeHtml(accountId)}" value="${escapeHtml(state.amount || '')}" ${amountDisabled ? 'disabled' : ''}>
            <button class="funding-max-button" type="button" data-funding-max-id="${escapeHtml(accountId)}" ${maxDisabled ? 'disabled' : ''}>${escapeHtml(t('funding.max'))}</button>
          </div>
        </td>
        <td class="${eligible ? 'value-positive' : ''}">${escapeHtml(eligible ? t('funding.ready') : reason)}</td>
      </tr>
    `;
  }).join('')}` : `<tr><td colspan="6" class="empty">${escapeHtml(t('funding.noOperableChildren'))}</td></tr>`;
  syncFundingAmountToggleState();
  syncFundingSelectAllState();
  syncFundingListBodyHeight();
}

function renderFundingModal() {
  const groups = fundingGroupOptions();
  if (!groups.length) {
    fundingGroupSelect.innerHTML = `<option value="">${escapeHtml(t('funding.noGroups'))}</option>`;
    fundingAssetSelect.innerHTML = `<option value="">${escapeHtml(t('funding.noTokens'))}</option>`;
    fundingMainSummary.innerHTML = '';
    fundingRows.innerHTML = `<tr><td colspan="6" class="empty">${escapeHtml(t('funding.noAvailableGroups'))}</td></tr>`;
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
    fundingSubmitButton.textContent = fundingDirection === 'distribute' ? t('funding.executeDistribute') : t('funding.executeCollect');
    fundingSubmitButton.disabled = true;
    renderFundingOperationMeta();
    renderFundingLogPanel();
    syncFundingListBodyHeight();
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
    : `<option value="">${escapeHtml(t('funding.noAvailableTokens'))}</option>`;

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
  fundingSubmitButton.textContent = fundingDirection === 'distribute' ? t('funding.executeDistribute') : t('funding.executeCollect');
  fundingSubmitButton.disabled = fundingModalBusy || !modeAvailable || !fundingSelectedAsset || !writeEnabled;
  renderFundingOperationMeta();
  renderFundingLogPanel();
  syncFundingCapabilityLog();
}
async function loadFundingOverview(mainAccountId, { resetState = false, preserveOverviewOnError = false } = {}) {
  if (!mainAccountId) {
    fundingOverview = null;
    renderFundingModal();
    return { success: false, error: t('funding.noGroups') };
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
    messageText.textContent = t('funding.openUnavailable');
    return;
  }
  fundingSelectedGroupId = fundingSelectedGroupId || groups[0].id;
  fundingSelectedAsset = '';
  fundingDirection = 'distribute';
  fundingActiveLogTab = 'runtime';
  resetFundingSyncAmountState();
  fundingModalShell.classList.remove('hidden');
  document.body.style.overflow = 'hidden';
  appendFundingLog(t('funding.opened'), 'info');
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
    messageText.textContent = t('funding.afterOperationRefreshFailed', { error });
    return { success: false, error: String(error) };
  }
}

async function refreshFundingOverviewNow({ useCooldown = false, allowWhileBusy = false } = {}) {
  if (!fundingSelectedGroupId) {
    return { success: false, error: t('refresh.noRefreshableGroups') };
  }
  if (fundingRefreshBusy || (!allowWhileBusy && fundingModalBusy) || (useCooldown && fundingRefreshCooldownSeconds > 0)) {
    return { success: false, error: t('refresh.unavailable') };
  }

  fundingRefreshBusy = true;
  if (useCooldown) {
    startFundingRefreshCooldown(10);
  }
  appendFundingLog(t('funding.refreshingOverview'), 'info');
  renderFundingModal();
  applyActionButtonState();

  const result = await loadFundingOverview(fundingSelectedGroupId, {
    resetState: false,
    preserveOverviewOnError: true,
  });

  fundingRefreshBusy = false;
  applyActionButtonState();

  if (result.success) {
    appendFundingLog(t('funding.overviewRefreshSuccess'), 'success');
  } else {
    appendFundingLog(t('funding.overviewRefreshFailed', { error: result.error }), 'error');
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
    detail: String(payload?.detail || payload?.error?.message || t('funding.operationFailed')),
    code: String(payload?.error?.code || 'PRECHECK_UNAVAILABLE'),
  };
}

async function copyFundingOperationId() {
  const operationId = fundingOperationMetaFullText();
  if (!operationId) {
    appendFundingLog(t('funding.noOperationId'), 'error');
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
        throw new Error(t('funding.clipboardUnavailable', { operationId }));
      }
    }
    appendFundingLog(t('funding.copiedOperationId', { id: shortOperationId(operationId) }), 'success');
  } catch (error) {
    appendFundingLog(t('funding.copyOperationIdFailed', { error }), 'error');
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
    appendFundingLog(t('funding.distributeSelectionRequired'), 'error');
    return;
  }
  if (fundingDirection === 'collect' && !selectedRows.some((row) => Number(row.amount) > 0)) {
    appendFundingLog(t('funding.collectSelectionRequired'), 'error');
    return;
  }
  if (fundingOverview.write_enabled === false) {
    appendFundingLog(fundingOverview.write_disabled_reason || t('funding.writeDisabledReason'), 'error');
    return;
  }
  for (const row of selectedRows) {
    const amount = Number(row.amount);
    if (!Number.isFinite(amount) || amount < 0) {
      appendFundingLog(t('funding.invalidAmount'), 'error');
      return;
    }
    if (fundingDirection === 'collect') {
      const overviewRow = rowById.get(String(row.account_id || ''));
      const maxAmount = overviewRow ? fundingAssetNumber(overviewRow) : 0;
      if (amount > maxAmount + 1e-12) {
        appendFundingLog(t('funding.exceedsCollectable'), 'error');
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
    `${fundingDirection === 'distribute' ? t('funding.executingDistribute') : t('funding.executingCollect')}… ${t('funding.audit.operationId', { id: shortOperationId(operationId) })}`,
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
      throw new Error(`${failure.code}: ${failure.detail}`);
    }

    fundingPendingOperationId = String(response.headers.get('X-Funding-Operation-Id') || payload.operation_id || operationId);
    fundingOverview = payload.overview || fundingOverview;
    resetFundingSelectionState();
    renderFundingModal();
    if (payload.idempotent_hit) {
      appendFundingLog(t('funding.idempotentHit', { id: shortOperationId(fundingPendingOperationId) }), 'info');
    }
    const allSucceeded = Array.isArray(payload.results) && payload.results.every((row) => row.success);
    const baseMessage = payload.message || t('funding.completed');
    appendFundingLog(`${baseMessage} | ${t('funding.audit.operationId', { id: shortOperationId(fundingPendingOperationId) })}`, allSucceeded ? 'success' : 'error');
    if (payload.overview_refresh?.success === false) {
      appendFundingLog(t('funding.overviewRefreshUnconfirmed', {
        message: payload.overview_refresh.message || t('funding.retryLater'),
        id: shortOperationId(fundingPendingOperationId),
      }), 'error');
    }
    if (payload.reconciliation?.status) {
      const reconciliationLabel = payload.reconciliation.status === 'confirmed'
        ? t('funding.reconciliationConfirmed')
        : payload.reconciliation.status === 'partially_confirmed'
          ? t('funding.reconciliationPartiallyConfirmed')
          : t('funding.reconciliationPending');
      appendFundingLog(`${reconciliationLabel}，${t('funding.audit.operationId', { id: shortOperationId(fundingPendingOperationId) })}`, payload.reconciliation.status === 'confirmed' ? 'success' : 'info');
    }
    await loadFundingAudit(fundingSelectedGroupId, { preserveOnError: true });

    const monitorRefreshResult = await refreshMonitorAfterFundingOperation();
    appendFundingLog(
      monitorRefreshResult.success
        ? t('funding.monitorRefreshSuccess', { id: shortOperationId(fundingPendingOperationId) })
        : t('funding.monitorRefreshFailed', { error: monitorRefreshResult.error, id: shortOperationId(fundingPendingOperationId) }),
      monitorRefreshResult.success ? 'success' : 'error',
    );
  } catch (error) {
    appendFundingLog(`${t('funding.operationFailed')}：${error} | ${t('funding.audit.operationId', { id: shortOperationId(fundingPendingOperationId || operationId) })}`, 'error');
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
  return `<div class="uni-mmr-indicator account-header-pill${toneClass ? ` ${toneClass}` : ''}">${escapeHtml(groupTextMap.uniMmr)} ${escapeHtml(fmtUniMmr(value))}</div>`;
}

function renderAccount(account) {
  const totals = account.totals || {};
  const distributionAmount = distributionDisplayAmount(totals, account.distribution_profit_summary);
  return `
    <article class="account">
      <div class="account-head">
        <div class="account-title-block">
          <h3>${escapeHtml(account.child_account_name || account.account_name || account.account_id || '-')}</h3>
          <div class="mono account-subtitle">
            <span class="account-subtitle-id">${escapeHtml(account.account_id || '-')}</span>
            <span class="account-subtitle-divider" aria-hidden="true">|</span>
            <span class="account-subtitle-status ${accountSubtitleStatusToneClass(account)}">${escapeHtml(accountSubtitleStatusText(account))}</span>
          </div>
        </div>
        <div class="account-head-actions">
          ${renderUniMmrIndicator(account)}
        </div>
      </div>
      <div class="account-grid">
        ${[
          { label: t('summary.equity'), value: fmtCurrency(totals.equity), tone: '' },
          { label: t('summary.margin'), value: fmtCurrency(totals.margin), tone: '' },
          { label: t('summary.availableBalance'), value: fmtCurrency(totals.available_balance), tone: '' },
          { label: t('summary.unrealizedPnl'), value: fmtCurrency(totals.unrealized_pnl), tone: numberTone(totals.unrealized_pnl) },
          { label: t('summary.distributionIncome'), value: fmtCurrency(distributionAmount), tone: numberTone(distributionAmount) },
          { label: t('summary.apy7d'), value: fmtPercent(totals.distribution_apy_7d), tone: numberTone(totals.distribution_apy_7d) },
        ].map(({ label, value, tone }) => `
          <div class="metric metric-card"><div class="label metric-label">${label}</div><div class="value metric-value ${tone}">${value}</div></div>
        `).join('')}
      </div>
      <div class="section"><h4>${escapeHtml(t('account.positions'))}</h4>${renderRows(
        tArray('account.positionHeaders'),
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
      <div class="section"><h4>${escapeHtml(t('account.assets'))}</h4>${renderRows(
        tArray('account.assetHeaders'),
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
  const pillBase = 'badge badge-status-reference status-pill-reference status-pill-scaled status-pill-scale-130 group-header-pill group-status-pill';
  if (successCount > 0) badges.push(`<div class="${pillBase} status-ok">${fmtCount(successCount)} ${escapeHtml(groupTextMap.healthy)}</div>`);
  if (errorCount > 0) badges.push(`<div class="${pillBase} status-error">${fmtCount(errorCount)} ${escapeHtml(t('accountStatus.error'))}</div>`);
  if (!badges.length) badges.push(`<div class="${pillBase} status-disabled">${fmtCount(summary.account_count || 0)} ${escapeHtml(groupTextMap.accounts)}</div>`);
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
    counts.good > 0 ? `<span class="group-unimmr-item group-unimmr-item-good">${fmtCount(counts.good)} ${escapeHtml(groupTextMap.healthy)}</span>` : '',
    counts.warn > 0 ? `<span class="group-unimmr-item group-unimmr-item-warn">${fmtCount(counts.warn)} ${escapeHtml(groupTextMap.warning)}</span>` : '',
    counts.bad > 0 ? `<span class="group-unimmr-item group-unimmr-item-bad">${fmtCount(counts.bad)} ${escapeHtml(groupTextMap.danger)}</span>` : '',
  ].filter(Boolean);
  if (!items.length) {
    return '';
  }
  const summaryToneClass = counts.bad > 0
    ? 'uni-mmr-bad status-error'
    : counts.warn > 0
      ? 'uni-mmr-warn status-partial'
      : 'uni-mmr-good status-ok';
  return `<div class="group-unimmr-summary badge badge-status-reference status-pill-reference status-pill-scaled status-pill-scale-130 group-header-pill group-unimmr-pill ${summaryToneClass}"><span class="group-unimmr-label">${escapeHtml(groupTextMap.uniMmr)}</span>${items.join('')}</div>`;
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
    <div class="account-single-view">${activeAccount ? renderAccount(activeAccount) : `<div class="empty">${escapeHtml(t('empty.subAccounts'))}</div>`}</div>
  `;
}

function renderGroup(group) {
  const summary = group.summary || {};
  const distributionAmount = distributionDisplayAmount(summary, group.profit_summary, group.accounts);
  const mainAccountId = String(group.main_account_id || '');
  const expanded = isGroupExpanded(mainAccountId);
  const groupStatusBadges = renderGroupStatusBadges(summary);
  return `
    <section class="group" data-main-account-id="${escapeHtml(mainAccountId)}">
      <div class="group-head">
        <div class="group-title-block group-title-inline">
          <h2>${escapeHtml(group.main_account_name || group.main_account_id || '-')}</h2>
          <span class="group-title-divider" aria-hidden="true"></span>
          <span class="mono group-subtitle group-subtitle-inline">${escapeHtml(mainAccountId)}</span>
          <span class="group-title-divider group-title-divider-meta" aria-hidden="true"></span>
          <div class="group-badges group-title-badges">${groupStatusBadges}</div>
        </div>
        <div class="group-actions">
          ${renderGroupUniMmrSummary(group)}
          <button class="group-toggle-button" type="button" data-main-account-id="${escapeHtml(mainAccountId)}" aria-expanded="${expanded ? 'true' : 'false'}">${expanded ? groupTextMap.collapseAccounts : groupTextMap.expandAccounts}</button>
        </div>
      </div>
      <div class="group-summary">
        ${[
          { label: t('summary.equity'), value: fmtCurrency(summary.equity), tone: '' },
          { label: t('summary.margin'), value: fmtCurrency(summary.margin), tone: '' },
          { label: t('summary.availableBalance'), value: fmtCurrency(summary.available_balance), tone: '' },
          { label: t('summary.unrealizedPnl'), value: fmtCurrency(summary.unrealized_pnl), tone: numberTone(summary.unrealized_pnl) },
          { label: t('summary.distributionIncome'), value: fmtCurrency(distributionAmount), tone: numberTone(distributionAmount) },
          { label: t('summary.apy7d'), value: fmtPercent(summary.distribution_apy_7d), tone: numberTone(summary.distribution_apy_7d) },
        ].map(({ label, value, tone }) => `
          <div class="metric metric-card"><div class="label metric-label">${label}</div><div class="value metric-value ${tone}">${value}</div></div>
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
  groupsContainer.innerHTML = `<section class="group"><div class="group-head"><h2>${escapeHtml(t('empty.groups'))}</h2></div></section>`;
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
  accountSingleView.innerHTML = activeAccount ? renderAccount(activeAccount) : `<div class="empty">${escapeHtml(t('empty.subAccounts'))}</div>`;
  renderedGroupSignatures[mainAccountId] = groupRenderSignature(group);
}

function render(payload) {
  latestPayload = payload;
  const groups = Array.isArray(payload.groups) ? payload.groups : [];
  normalizeGroupUiState(groups);
  const monitorStatus = payload.status || 'idle';

  connectionBadge.textContent = textStatus(monitorStatus);
  connectionBadge.className = `badge badge-status-reference status-pill-reference status-pill-scaled status-pill-scale-130 ${statusClass(monitorStatus)}`;
  connectionBadge.dataset.monitorStatus = monitorStatus;
  messageText.textContent = textMessage(payload.message);
  updatedAt.textContent = t('toolbar.updatedAt', { time: fmtTime(payload.updated_at) });

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
  if (fundingListBody) {
    fundingListBody.style.removeProperty('--funding-list-max-height');
    fundingListBody.classList.remove('is-scrollable');
  }

  renderFundingLogPanel();
  renderFundingOperationMeta();
  applyActionButtonState();
}

function payloadContainsMonitorSnapshot(payload = {}) {
  return Array.isArray(payload.groups)
    || typeof payload.summary === 'object'
    || typeof payload.profit_summary === 'object'
    || payload.updated_at !== undefined;
}

function describeRefreshResult(refreshResult, elapsedSeconds, { snapshotApplied = true } = {}) {
  if (!refreshResult) {
    return snapshotApplied
      ? t('refresh.completed', { seconds: elapsedSeconds })
      : t('refresh.requestSent', { seconds: elapsedSeconds });
  }
  if (refreshResult.success) {
    const fallbackSections = Array.isArray(refreshResult.fallback_sections) ? refreshResult.fallback_sections : [];
    if (!snapshotApplied) {
      return t('refresh.requestSent', { seconds: elapsedSeconds });
    }
    return fallbackSections.length > 0
      ? t('refresh.successWithFallback', { seconds: elapsedSeconds })
      : t('refresh.success', { seconds: elapsedSeconds });
  }
  if (refreshResult.timeout) return refreshResult.message || t('refresh.timeout');
  return refreshResult.message || t('refresh.failed');
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
    messageText.textContent = t('refresh.stateFailed', { error });
  } finally {
    toggleBusy = false;
    applyActionButtonState();
  }
}

async function refreshNow() {
  if (refreshBusy) {
    messageText.textContent = t('refresh.busy');
    return;
  }
  refreshBusy = true;
  applyActionButtonState();
  const refreshStartedAt = Date.now();
  messageText.textContent = t('refresh.inProgress');
  try {
    const response = await apiFetch('/api/monitor/refresh', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
    const payload = await response.json();
    if (!response.ok) throw new Error(payload.detail || `HTTP ${response.status}`);
    const snapshotApplied = payloadContainsMonitorSnapshot(payload);
    if (snapshotApplied) {
      render(payload);
    }
    const elapsedSeconds = Math.max(1, Math.round((Date.now() - refreshStartedAt) / 1000));
    messageText.textContent = describeRefreshResult(payload.refresh_result, elapsedSeconds, { snapshotApplied });
  } catch (error) {
    messageText.textContent = t('refresh.nowFailed', { error });
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
    ? t('import.sensitiveSettingsSuffix', { count: fmtCount(result.updated_settings_keys.length) })
    : '';
  const securityNotice = payload.security_notice ? `；${payload.security_notice}` : '';
  const baseMessage = settingsOnly
    ? t('import.successWithSettings', { count: fmtCount(updatedSettingsCount) })
    : payload.refresh_result?.success === false
      ? t('import.successRefreshFailed', { groupCount, accountCount, message: payload.refresh_result.message || '-' })
      : t('import.success', { groupCount, accountCount });
  return `${baseMessage}${updatedSettings}${securityNotice}`;
}

function parseDownloadFilename(contentDisposition) {
  if (!contentDisposition) return 'monitor_accounts_template.xlsx';
  const match = contentDisposition.match(/filename="?([^";]+)"?/i);
  return match ? match[1] : 'monitor_accounts_template.xlsx';
}

async function downloadTemplate() {
  if (importBusy || refreshBusy || toggleBusy) return;
  messageText.textContent = t('import.downloadingTemplate');
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
    messageText.textContent = t('import.templateStarted');
  } catch (error) {
    messageText.textContent = t('import.templateFailed', { error });
  }
}

async function uploadExcel(file) {
  if (!file || importBusy) return;
  importBusy = true;
  applyActionButtonState();
  messageText.textContent = t('import.importingConfig');
  try {
    const formData = new FormData();
    formData.append('file', file);
    const response = await apiFetch('/api/config/import/excel', { method: 'POST', body: formData });
    const payload = await readApiPayload(response);
    if (!response.ok) throw new Error(payload.detail || payload.message || `HTTP ${response.status}`);
    render(payload);
    messageText.textContent = describeImportResult(payload);
  } catch (error) {
    messageText.textContent = t('import.failed', { error });
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
    messageText.textContent = t('import.initFailed', { error });
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
      messageText.textContent = t('auth.logoutFailed', { error });
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
    appendFundingLog(t('funding.switchedGroup', { group: fundingGroupSelect.selectedOptions[0]?.textContent || fundingSelectedGroupId || '-' }), 'info');
    loadFundingOverview(fundingSelectedGroupId, { resetState: true });
  });
  fundingModeDistribute.addEventListener('click', () => {
    if (fundingDirection === 'distribute') {
      return;
    }
    fundingDirection = 'distribute';
    resetFundingSelectionState();
    resetFundingSyncAmountState();
    appendFundingLog(t('funding.switchedDistribute'), 'info');
    renderFundingModal();
  });
  fundingModeCollect.addEventListener('click', () => {
    if (fundingDirection === 'collect') {
      return;
    }
    fundingDirection = 'collect';
    resetFundingSelectionState();
    resetFundingSyncAmountState();
    appendFundingLog(t('funding.switchedCollect'), 'info');
    renderFundingModal();
  });
  fundingAssetSelect.addEventListener('change', () => {
    fundingSelectedAsset = fundingAssetSelect.value;
    resetFundingSelectionState();
    resetFundingSyncAmountState();
    appendFundingLog(t('funding.switchedAsset', { asset: fundingSelectedAsset || '-' }), 'info');
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
    connectionBadge.className = `badge badge-status-reference status-pill-reference status-pill-scaled status-pill-scale-130 ${statusClass('reconnecting')}`;
    connectionBadge.dataset.monitorStatus = 'reconnecting';
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
  refreshNow,
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
  getUiText: t,
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

window.addEventListener('resize', syncFundingListBodyHeight);
