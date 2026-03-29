const summaryCards = document.getElementById('summaryCards');
const profitCards = document.getElementById('profitCards');
const groupsContainer = document.getElementById('groupsContainer');
const connectionBadge = document.getElementById('connectionBadge');
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

let toolbarStatsSignature = '';
let summarySignature = '';
let profitSummarySignature = '';
let pendingStreamPayload = null;
let pendingStreamFrame = null;

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
const currentGroups = () => (Array.isArray(latestPayload?.groups) ? latestPayload.groups : []);
const refreshButtonLabel = () => (refreshCooldownSeconds > 0 ? `${refreshCooldownSeconds}秒` : '立即刷新');
const fundingRefreshButtonLabel = () => (fundingRefreshCooldownSeconds > 0 ? `${fundingRefreshCooldownSeconds}秒` : '立即刷新');
const fmtClock = (value = new Date()) => {
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime())
    ? '--:--:--'
    : date.toLocaleTimeString('zh-CN', { hour12: false });
};

function applyActionButtonState() {
  refreshButton.textContent = refreshButtonLabel();
  refreshButton.disabled = importBusy || refreshBusy || toggleBusy || refreshCooldownSeconds > 0;
  downloadTemplateButton.disabled = importBusy || refreshBusy || toggleBusy;
  importButton.textContent = importBusy ? '导入中' : '导入 Excel';
  importButton.disabled = importBusy || refreshBusy || toggleBusy;
  fundingTransferButton.disabled = importBusy || refreshBusy || toggleBusy || currentGroups().length === 0;
  monitorToggle.disabled = toggleBusy || refreshBusy || importBusy;
  fundingSubmitButton.disabled = fundingModalBusy;
  if (fundingRefreshButton) {
    fundingRefreshButton.textContent = fundingRefreshButtonLabel();
    fundingRefreshButton.disabled = fundingModalBusy || fundingRefreshBusy || fundingRefreshCooldownSeconds > 0 || !fundingSelectedGroupId;
  }
  if (fundingQuickCollectButton) {
    fundingQuickCollectButton.disabled = fundingModalBusy || fundingDirection !== 'collect' || !fundingModeAvailable() || !fundingSelectedAsset;
  }
  if (fundingQuickClearButton) {
    fundingQuickClearButton.disabled = fundingModalBusy || fundingDirection !== 'collect';
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

function renderFundingLogPanel() {
  if (!fundingLogList || !fundingLogLatestTime) {
    return;
  }

  if (!fundingLogEntries.length) {
    fundingLogLatestTime.textContent = '--:--:--';
    fundingLogList.innerHTML = '<div class="funding-log-empty">暂无日志</div>';
    return;
  }

  fundingLogLatestTime.textContent = fundingLogEntries[0].time;
  fundingLogList.innerHTML = fundingLogEntries.map((entry) => `
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
  fundingSubmitButton.disabled = fundingModalBusy || !modeAvailable || !fundingSelectedAsset;
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
    const response = await fetch(`/api/funding/groups/${encodeURIComponent(mainAccountId)}`, { cache: 'no-store' });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || `HTTP ${response.status}`);
    }
    fundingSelectedGroupId = mainAccountId;
    fundingOverview = payload;
    if (resetState) {
      resetFundingSelectionState();
    }
    return { success: true, error: '' };
  } catch (error) {
    if (!preserveOverviewOnError || !previousOverview) {
      fundingOverview = {
        main_account_id: mainAccountId,
        main_account_name: mainAccountId,
        available: false,
        reason: String(error),
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
    const response = await fetch('/api/monitor/refresh', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
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

  fundingModalBusy = true;
  applyActionButtonState();
  appendFundingLog(fundingDirection === 'distribute' ? '正在执行现货分发…' : '正在执行现货归集…', 'info');
  try {
    const endpoint = fundingDirection === 'distribute'
      ? `/api/funding/groups/${encodeURIComponent(fundingSelectedGroupId)}/distribute`
      : `/api/funding/groups/${encodeURIComponent(fundingSelectedGroupId)}/collect`;
    const requestBody = fundingDirection === 'distribute'
      ? { asset: fundingSelectedAsset, transfers: selectedRows }
      : { asset: fundingSelectedAsset, transfers: selectedRows };

    const response = await fetch(endpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(requestBody),
    });
    const payload = await response.json();
    if (!response.ok) {
      throw new Error(payload.detail || `HTTP ${response.status}`);
    }

    fundingOverview = payload.overview || fundingOverview;
    resetFundingSelectionState();
    renderFundingModal();
    const allSucceeded = Array.isArray(payload.results) && payload.results.every((row) => row.success);
    const baseMessage = payload.message || '操作完成';
    appendFundingLog(baseMessage, allSucceeded ? 'success' : 'error');

    await refreshFundingOverviewNow({ useCooldown: false, allowWhileBusy: true });

    const monitorRefreshResult = await refreshMonitorAfterFundingOperation();
    appendFundingLog(
      monitorRefreshResult.success ? '主界面监控信息刷新成功。' : `主界面监控信息刷新失败：${monitorRefreshResult.error}`,
      monitorRefreshResult.success ? 'success' : 'error',
    );
  } catch (error) {
    appendFundingLog(`资金操作失败：${error}`, 'error');
  } finally {
    fundingModalBusy = false;
    applyActionButtonState();
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

function renderAccount(account) {
  const totals = account.totals || {};
  return `
    <article class="account">
      <div class="account-head">
        <div>
          <h3>${escapeHtml(account.child_account_name || account.account_name || account.account_id || '-')}</h3>
          <div class="mono">${escapeHtml(account.account_id || '-')} | ${escapeHtml(textAccountStatus(account.account_status))}</div>
        </div>
        <div class="badge ${statusClass(account.status)}">${escapeHtml(textStatus(account.status))}</div>
      </div>
      <div class="account-grid">
        ${[
          { label: '权益', value: fmt(totals.equity), tone: '' },
          { label: '保证金', value: fmt(totals.margin), tone: '' },
          { label: '可用余额', value: fmt(totals.available_balance), tone: '' },
          { label: '未实现盈亏', value: fmt(totals.unrealized_pnl), tone: numberTone(totals.unrealized_pnl) },
          { label: '分发收益', value: fmt(totals.total_distribution), tone: numberTone(totals.total_distribution) },
          { label: '7日年化', value: fmtPercent(totals.distribution_apy_7d), tone: numberTone(totals.distribution_apy_7d) },
        ].map(({ label, value, tone }) => `
          <div class="metric"><div class="label">${label}</div><div class="value ${tone}">${value}</div></div>
        `).join('')}
      </div>
      <div class="section"><h4>持仓</h4>${renderRows(
        ['交易对', '方向', '数量', '开仓价', '标记价', '未实现盈亏', '名义价值', '杠杆'],
        account.positions || [],
        (row) => `
          <tr>
            <td class="mono">${escapeHtml(row.symbol || '-')}</td>
            <td class="${positionSideTone(row.position_side)}">${escapeHtml(textPositionSide(row.position_side))}</td>
            <td>${fmt(row.qty)}</td><td>${fmt(row.entry_price)}</td><td>${fmt(row.mark_price)}</td>
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
  if (successCount > 0) badges.push(`<div class="badge status-ok">${fmtCount(successCount)} 正常</div>`);
  if (errorCount > 0) badges.push(`<div class="badge status-error">${fmtCount(errorCount)} 异常</div>`);
  if (!badges.length) badges.push(`<div class="badge">${fmtCount(summary.account_count || 0)} 个账户</div>`);
  return badges.join('');
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
  const mainAccountId = String(group.main_account_id || '');
  const expanded = isGroupExpanded(mainAccountId);
  return `
    <section class="group" data-main-account-id="${escapeHtml(mainAccountId)}">
      <div class="group-head">
        <div><h2>${escapeHtml(group.main_account_name || group.main_account_id || '-')}</h2><div class="mono">${escapeHtml(mainAccountId)}</div></div>
        <div class="group-actions">
          <button class="group-toggle-button" type="button" data-main-account-id="${escapeHtml(mainAccountId)}" aria-expanded="${expanded ? 'true' : 'false'}">${expanded ? '收起子账号' : '展开子账号'}</button>
          <div class="group-badges">${renderGroupStatusBadges(summary)}</div>
        </div>
      </div>
      <div class="group-summary">
        ${[
          { label: '权益', value: fmt(summary.equity), tone: '' },
          { label: '保证金', value: fmt(summary.margin), tone: '' },
          { label: '可用余额', value: fmt(summary.available_balance), tone: '' },
          { label: '未实现盈亏', value: fmt(summary.unrealized_pnl), tone: numberTone(summary.unrealized_pnl) },
          { label: '分发收益', value: fmt(summary.total_distribution), tone: numberTone(summary.total_distribution) },
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
    return JSON.stringify({ expanded: false, main_account_id: mainAccountId, main_account_name: group.main_account_name || '', summary: group.summary || {} });
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

  toggleButton.textContent = expanded ? '收起子账号' : '展开子账号';
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
    const response = await fetch('/api/monitor/control', {
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
    const response = await fetch('/api/monitor/refresh', { method: 'POST', headers: { 'Content-Type': 'application/json' } });
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
  return payload.refresh_result?.success === false
    ? `Excel 导入成功，已覆盖 ${groupCount} 个分组 / ${accountCount} 个账户，但刷新失败：${payload.refresh_result.message || '-'}`
    : `Excel 导入成功，已覆盖 ${groupCount} 个分组 / ${accountCount} 个账户`;
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
    const response = await fetch('/api/config/import/excel-template', { method: 'GET', cache: 'no-store' });
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
    const response = await fetch('/api/config/import/excel', { method: 'POST', body: formData });
    const payload = await response.json();
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
    const response = await fetch(groupsUrl, { cache: 'no-store' });
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
  scheduleStreamRender,
  appendFundingLog,
  renderFundingLogPanel,
  renderFundingModal,
  openFundingModal,
  closeFundingModal,
  bootstrap,
  refreshFundingOverviewNow,
  submitFundingOperation,
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
};

applyActionButtonState();
renderFundingLogPanel();
if (!TEST_MODE) {
  bootstrap();
}
