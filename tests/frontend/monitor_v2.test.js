import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

import { JSDOM } from 'jsdom';
import { afterEach, describe, expect, it, vi } from 'vitest';

const projectRoot = resolve(fileURLToPath(new URL('.', import.meta.url)), '..', '..');
const htmlPath = resolve(projectRoot, 'monitor_app', 'static', 'monitor_v2.html');
const scriptPath = resolve(projectRoot, 'monitor_app', 'static', 'monitor_v2.js');

function baseAccount(accountId = 'group_a.sub1', childName = 'Sub 1') {
  return {
    status: 'ok',
    account_id: accountId,
    account_name: childName,
    child_account_name: childName,
    account_status: 'NORMAL',
    uni_mmr: '1.63',
    totals: {
      equity: '100',
      margin: '50',
      available_balance: '40',
      unrealized_pnl: '0',
      total_distribution: '0',
      distribution_apy_7d: '0',
    },
    positions: [],
    assets: [],
  };
}

function basePayload(overrides = {}) {
  return {
    status: 'ok',
    message: 'ready',
    updated_at: '2026-03-29T12:00:00+08:00',
    service: { monitor_enabled: true },
    summary: {
      account_count: 1,
      success_count: 1,
      error_count: 0,
      equity: '100',
      margin: '50',
      available_balance: '40',
      unrealized_pnl: '0',
      total_commission: '0',
      distribution_apy_7d: '0',
    },
    profit_summary: {},
    groups: [
      {
        main_account_id: 'group_a',
        main_account_name: 'Group A',
        summary: {
          account_count: 1,
          success_count: 1,
          error_count: 0,
          equity: '100',
          margin: '50',
          available_balance: '40',
          unrealized_pnl: '0',
          total_distribution: '0',
          distribution_apy_7d: '0',
        },
        accounts: [baseAccount()],
      },
    ],
    ...overrides,
  };
}

function baseFundingOverview(overrides = {}) {
  return {
    main_account_id: 'group_a',
    main_account_name: 'Group A',
    available: true,
    reason: '',
    write_enabled: true,
    write_disabled_reason: '',
    assets: ['BNB'],
    main_account: {
      uid: '13133777',
      transfer_ready: true,
      reason: '',
      spot_assets: [{ asset: 'BNB', free: '10', locked: '0', total: '10' }],
      spot_available: { BNB: '10' },
      funding_assets: [{ asset: 'BNB', free: '10', locked: '0', total: '10' }],
      funding_available: { BNB: '10' },
    },
    children: [
      {
        account_id: 'group_a.sub1',
        child_account_id: 'sub1',
        name: 'Sub 1',
        uid: '223456789',
        can_distribute: true,
        can_collect: true,
        reason_distribute: '',
        reason_collect: '',
        reason: '',
        spot_assets: [{ asset: 'BNB', free: '3.2', locked: '0', total: '3.2' }],
        spot_available: { BNB: '3.2' },
        funding_assets: [{ asset: 'BNB', free: '3.2', locked: '0', total: '3.2' }],
        funding_available: { BNB: '3.2' },
      },
    ],
    ...overrides,
  };
}

function auditPayload(overrides = {}) {
  return {
    main_account_id: 'group_a',
    entries: [],
    updated_at: '2026-03-29T12:00:00+08:00',
    ...overrides,
  };
}

function createApp() {
  const dom = new JSDOM(readFileSync(htmlPath, 'utf8'), {
    url: 'http://127.0.0.1:8010/',
    pretendToBeVisual: true,
    runScripts: 'outside-only',
  });
  const { window } = dom;
  const rafQueue = [];

  window.__MONITOR_V2_TEST_MODE__ = true;
  window.fetch = vi.fn(async () => ({ ok: true, json: async () => ({}) }));
  window.EventSource = class FakeEventSource {
    addEventListener() {}
    close() {}
  };
  window.navigator.clipboard = {
    writeText: vi.fn(async () => {}),
  };
  window.document.execCommand = vi.fn(() => true);
  if (!window.CSS) {
    window.CSS = { escape: (value) => String(value) };
  }
  window.requestAnimationFrame = (callback) => {
    rafQueue.push(callback);
    return rafQueue.length;
  };
  window.cancelAnimationFrame = (id) => {
    rafQueue[id - 1] = null;
  };

  window.eval(readFileSync(scriptPath, 'utf8'));

  return {
    window,
    document: window.document,
    api: window.__monitorV2,
    flushAnimationFrames() {
      while (rafQueue.length) {
        const callback = rafQueue.shift();
        if (typeof callback === 'function') {
          callback();
        }
      }
    },
    close() {
      dom.window.close();
    },
  };
}

const apps = [];

afterEach(() => {
  while (apps.length) {
    apps.pop().close();
  }
});

describe('monitor_v2.js', () => {
  it('marks the monitor document and key metric regions for the black-gold theme', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload({
      profit_summary: {
        today: { label: '今日收益', amount: '12.34', rate: '0.01', complete: true },
      },
    }));

    expect(app.document.body.dataset.theme).toBe('black-gold');
    expect(app.document.querySelectorAll('#summaryCards .summary-card')).toHaveLength(6);
    expect(app.document.querySelector('#profitCards')?.className).toContain('profit-strip');
    expect(app.document.querySelectorAll('#profitCards .profit-card')).toHaveLength(1);
  });

  it('formats top-level monetary cards and profit strip amounts with a leading dollar sign while keeping rates as percentages', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload({
      summary: {
        account_count: 1,
        success_count: 1,
        error_count: 0,
        equity: '14793.16',
        margin: '14887.94',
        available_balance: '0',
        unrealized_pnl: '-74449.18',
        total_commission: '-0.0001',
        distribution_apy_7d: '0.1297',
      },
      profit_summary: {
        today: { label: '今日收益', amount: '8.22', rate: '0.0006', complete: true },
      },
    }));

    const summaryText = app.document.getElementById('summaryCards').textContent;
    const profitText = app.document.getElementById('profitCards').textContent;

    expect(summaryText).toContain('$14,793.16');
    expect(summaryText).toContain('-$74,449.18');
    expect(profitText).toContain('$8.22');
    expect(profitText).toContain('0.06%');
  });

  it('uses the frameless hero and luxury switch hooks for the refined header treatment', () => {
    const app = createApp();
    apps.push(app);

    expect(app.document.querySelector('.hero')?.className).toContain('hero-seamless');
    expect(app.document.querySelector('.switch')?.className).toContain('switch-luxury');
  });

  it('applies the reference-sized status pill and switch hooks on the hero controls', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload());

    expect(app.document.getElementById('connectionBadge')?.className).toContain('status-pill-reference');
    expect(app.document.querySelector('.switch')?.className).toContain('switch-reference');
  });

  it('marks the hero status pill and switch with the scaled alignment hooks for the larger header proportion', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload());

    expect(app.document.getElementById('connectionBadge')?.className).toContain('status-pill-scaled');
    expect(app.document.querySelector('.switch')?.className).toContain('switch-scaled');
  });

  it('marks the hero status pill with the 1.3x scaling hook for the enlarged badge request', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload());

    expect(app.document.getElementById('connectionBadge')?.className).toContain('status-pill-scale-130');
  });

  it('marks the three hero action buttons with the stretched-width hook so only the header controls grow longer', () => {
    const app = createApp();
    apps.push(app);

    expect(app.document.querySelectorAll('.hero-controls .hero-action-button-stretched')).toHaveLength(3);
  });

  it('applies the same stretched-width hook to the toolbar refresh button so it matches the top action buttons', () => {
    const app = createApp();
    apps.push(app);

    expect(app.document.getElementById('refreshButton')?.className).toContain('hero-action-button-stretched');
  });

  it('renders group titles as a single inline row with a divider before the group id metadata', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload({
      groups: [
        {
          ...basePayload().groups[0],
          main_account_id: 'group_a',
          main_account_name: 'isaga',
        },
      ],
    }));

    const titleRow = app.document.querySelector('.group-title-inline');
    expect(titleRow).not.toBeNull();
    expect(titleRow?.querySelector('h2')?.textContent).toContain('isaga');
    expect(titleRow?.querySelectorAll('.group-title-divider')).toHaveLength(2);
    expect(titleRow?.querySelector('.group-subtitle-inline')?.textContent).toContain('group_a');
    expect(titleRow?.querySelector('.group-badges')?.className).toContain('group-title-badges');
  });

  it('uses the refined status pill and compact summary-card hooks from the reference treatment', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload());

    expect(app.document.getElementById('connectionBadge')?.className).toContain('badge-status-reference');
    expect(app.document.querySelectorAll('#summaryCards .summary-card-refined')).toHaveLength(6);
  });

  it('maps the replica metric-card and linked-strip classes onto the top overview region', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload({
      profit_summary: {
        today: { label: '今日收益 | 收益率', amount: '20.03', rate: '0.05', complete: true },
        week: { label: '本周收益 | 收益率', amount: '60.10', rate: '0.16', complete: true },
      },
    }));

    expect(app.document.querySelectorAll('#summaryCards .metric')).toHaveLength(6);
    expect(app.document.getElementById('profitCards')?.className).toContain('linked-strip');
    expect(app.document.querySelectorAll('#profitCards .linked-cell')).toHaveLength(2);
  });

  it('marks overview labels with the emphasis hooks so the descriptive text can scale independently from the values', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload({
      profit_summary: {
        today: { label: '今日收益 | 收益率', amount: '20.03', rate: '0.05', complete: true },
      },
    }));

    expect(app.document.querySelectorAll('#summaryCards .metric-label-emphasis')).toHaveLength(6);
    expect(app.document.querySelectorAll('#profitCards .linked-label-emphasis')).toHaveLength(1);
  });

  it('marks the top overview cards with the proportional sizing hook for the larger reference ratio', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload());

    expect(app.document.querySelectorAll('#summaryCards .summary-card-proportional')).toHaveLength(6);
    expect(app.document.querySelectorAll('#summaryCards .metric-value-proportional')).toHaveLength(6);
  });

  it('renders toolbar stat pills with the refined clipping hook so their rounded corners do not show square background bleed', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload());

    expect(app.document.querySelectorAll('#toolbarStats .toolbar-stat-pill-refined')).toHaveLength(3);
    expect(app.document.querySelectorAll('#toolbarStats .toolbar-stat-pill-clipped')).toHaveLength(3);
    expect(app.document.querySelectorAll('#toolbarStats .toolbar-stat-pill-surface')).toHaveLength(3);
  });

  it('tracks the monitor status on the connection badge so partial anomalies can render as danger without affecting reconnecting', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload({ status: 'partial' }));

    expect(app.document.getElementById('connectionBadge')?.dataset.monitorStatus).toBe('partial');
  });

  it('disables write controls for guest sessions after bootstrap', async () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.window.fetch = vi.fn(async (input) => {
      const path = new URL(String(input), 'http://127.0.0.1:8010').pathname;
      if (path === '/api/auth/session') {
        return {
          ok: true,
          status: 200,
          json: async () => ({
            enabled: true,
            initialized: true,
            authenticated: true,
            whitelisted: false,
            role: 'guest',
            auth_source: 'session',
            csrf_token: 'csrf-test-only',
            last_activity_at: '2026-04-19T12:00:00+08:00',
          }),
        };
      }
      if (path === '/api/monitor/groups') {
        return {
          ok: true,
          status: 200,
          json: async () => basePayload(),
        };
      }
      throw new Error(`Unexpected fetch: ${path}`);
    });

    await app.api.bootstrap();

    expect(app.document.getElementById('authRoleBadge').textContent).toContain('游客');
    expect(app.document.getElementById('refreshButton').disabled).toBe(true);
    expect(app.document.getElementById('importButton').disabled).toBe(true);
    expect(app.document.getElementById('fundingTransferButton').disabled).toBe(true);
    expect(app.document.getElementById('logoutButton').hidden).toBe(false);
  });

  it('renders the main view for normal payloads and empty groups', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload());
    expect(app.document.getElementById('messageText').textContent).toContain('ready');
    expect(app.document.querySelectorAll('.groups .group').length).toBe(1);
    expect(app.document.querySelector('.group-toggle-button').textContent).toContain('展开子账号');

    app.api.render(basePayload({ message: 'empty', groups: [] }));
    expect(app.document.getElementById('messageText').textContent).toContain('empty');
    expect(app.document.getElementById('groupsContainer').textContent).toContain('暂无分组');
  });

  it('renders group UniMMR summary in the actions row while the title row carries the group status badges', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload({
      groups: [
        {
          main_account_id: 'group_a',
          main_account_name: 'Group A',
          summary: {
            account_count: 3,
            success_count: 3,
            error_count: 0,
            equity: '100',
            margin: '50',
            available_balance: '40',
            unrealized_pnl: '0',
            total_distribution: '0',
            distribution_apy_7d: '0',
          },
          accounts: [
            { ...baseAccount('group_a.sub1', 'Sub 1'), uni_mmr: '1.63' },
            { ...baseAccount('group_a.sub2', 'Sub 2'), uni_mmr: '1.35' },
            { ...baseAccount('group_a.sub3', 'Sub 3'), uni_mmr: '1.12' },
          ],
        },
      ],
    }));

    const actions = app.document.querySelector('.group-actions');
    const children = Array.from(actions.children);
    const titleRow = app.document.querySelector('.group-title-inline');

    expect(children[0].className).toContain('group-unimmr-summary');
    expect(children[0].className).toContain('uni-mmr-bad');
    expect(children[0].className).toContain('badge-status-reference');
    expect(children[1].className).toContain('group-toggle-button');
    expect(titleRow?.querySelector('.group-badges')?.className).toContain('group-title-badges');
    expect(titleRow?.querySelector('.group-badges .badge')?.className).toContain('status-pill-reference');
    expect(children[0].textContent).toContain('UniMMR');
    expect(children[0].textContent).toContain('1 正常');
    expect(children[0].textContent).toContain('1 警惕');
    expect(children[0].textContent).toContain('1 危险');
  });

  it('uses warning tone for the group UniMMR capsule when the worst account is warning', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload({
      groups: [
        {
          main_account_id: 'group_a',
          main_account_name: 'Group A',
          summary: {
            account_count: 5,
            success_count: 5,
            error_count: 0,
            equity: '100',
            margin: '50',
            available_balance: '40',
            unrealized_pnl: '0',
            total_distribution: '0',
            distribution_apy_7d: '0',
          },
          accounts: [
            { ...baseAccount('group_a.sub1', 'Sub 1'), uni_mmr: '1.63' },
            { ...baseAccount('group_a.sub2', 'Sub 2'), uni_mmr: '1.61' },
            { ...baseAccount('group_a.sub3', 'Sub 3'), uni_mmr: '1.60' },
            { ...baseAccount('group_a.sub4', 'Sub 4'), uni_mmr: '1.59' },
            { ...baseAccount('group_a.sub5', 'Sub 5'), uni_mmr: '1.35' },
          ],
        },
      ],
    }));

    const summary = app.document.querySelector('.group-unimmr-summary');
    expect(summary.className).toContain('uni-mmr-warn');
    expect(summary.textContent).toContain('4 正常');
    expect(summary.textContent).toContain('1 警惕');
    expect(summary.textContent).not.toContain('危险');
  });

  it('hides zero-count group UniMMR states', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload({
      groups: [
        {
          main_account_id: 'group_a',
          main_account_name: 'Group A',
          summary: {
            account_count: 2,
            success_count: 2,
            error_count: 0,
            equity: '100',
            margin: '50',
            available_balance: '40',
            unrealized_pnl: '0',
            total_distribution: '0',
            distribution_apy_7d: '0',
          },
          accounts: [
            { ...baseAccount('group_a.sub1', 'Sub 1'), uni_mmr: '1.63' },
            { ...baseAccount('group_a.sub2', 'Sub 2'), uni_mmr: '1.58' },
          ],
        },
      ],
    }));

    const summary = app.document.querySelector('.group-unimmr-summary');
    expect(summary.className).toContain('uni-mmr-good');
    expect(summary.textContent).toContain('2 正常');
    expect(summary.textContent).not.toContain('警惕');
    expect(summary.textContent).not.toContain('危险');
  });

  it('renders liquidation price after mark price in positions', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    const account = baseAccount();
    account.positions = [
      {
        symbol: 'ETHUSDC',
        position_side: 'LONG',
        qty: '116.66',
        entry_price: '2138.90',
        mark_price: '2450.55',
        liquidation_price: '1888.88',
        unrealized_pnl: '36339.33',
        notional: '285869.85',
        leverage: 75,
      },
    ];

    app.document.getElementById('groupsContainer').innerHTML = app.api.renderAccount(account);
    const text = app.document.getElementById('groupsContainer').textContent;
    expect(text).toContain('爆仓价');
    expect(text).toContain('1,888.88');
    expect(text).toContain('2,450.55');
  });

  it('renders UniMMR in the account header with the configured severity colors', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    const highAccount = baseAccount('group_a.sub1', 'High');
    highAccount.uni_mmr = '1.63';
    app.document.getElementById('groupsContainer').innerHTML = app.api.renderAccount(highAccount);
    let badge = app.document.querySelector('.account-head .uni-mmr-indicator');
    expect(badge).not.toBeNull();
    expect(badge.textContent).toContain('UniMMR');
    expect(badge.textContent).toContain('1.63');
    expect(badge.className).toContain('uni-mmr-good');
    expect(badge.className).toContain('account-header-pill');

    const warnAccount = baseAccount('group_a.sub2', 'Warn');
    warnAccount.uni_mmr = '1.35';
    app.document.getElementById('groupsContainer').innerHTML = app.api.renderAccount(warnAccount);
    badge = app.document.querySelector('.account-head .uni-mmr-indicator');
    expect(badge.className).toContain('uni-mmr-warn');

    const badAccount = baseAccount('group_a.sub3', 'Bad');
    badAccount.uni_mmr = '1.20';
    app.document.getElementById('groupsContainer').innerHTML = app.api.renderAccount(badAccount);
    badge = app.document.querySelector('.account-head .uni-mmr-indicator');
    expect(badge.className).toContain('uni-mmr-bad');
  });

  it('renders the account subtitle as a larger inline id and colored status text while keeping the header status pill styled consistently', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    const account = baseAccount('group_a.sub1', 'Sub 1');
    account.status = 'error';
    account.account_status = 'ERROR';

    app.document.getElementById('groupsContainer').innerHTML = app.api.renderAccount(account);

    const subtitle = app.document.querySelector('.account-subtitle');
    expect(subtitle?.querySelector('.account-subtitle-id')?.textContent).toContain('group_a.sub1');
    expect(subtitle?.querySelector('.account-subtitle-divider')).not.toBeNull();
    expect(subtitle?.querySelector('.account-subtitle-status')?.textContent).toContain('异常');
    expect(subtitle?.querySelector('.account-subtitle-status')?.className).toContain('account-subtitle-status-error');
    expect(app.document.querySelector('.account-head-actions .badge')?.className).toContain('account-header-pill');
    expect(app.document.querySelector('.account-head-actions .badge')?.className).toContain('status-pill-reference');
  });

  it('falls back to the monitor status text in the account subtitle when account_status is missing', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    const account = baseAccount('group_a.sub5', 'Sub 5');
    account.status = 'error';
    delete account.account_status;

    app.document.getElementById('groupsContainer').innerHTML = app.api.renderAccount(account);

    const subtitleStatus = app.document.querySelector('.account-subtitle-status');
    expect(subtitleStatus?.textContent).toContain('异常');
    expect(subtitleStatus?.className).toContain('account-subtitle-status-error');
  });

  it('refreshes monitor data into the existing cards and groups after the refresh request resolves', async () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload({
      summary: {
        account_count: 1,
        success_count: 1,
        error_count: 0,
        equity: '100',
        margin: '50',
        available_balance: '40',
        unrealized_pnl: '0',
        total_commission: '0',
        distribution_apy_7d: '0',
      },
      groups: [{ ...basePayload().groups[0], main_account_name: 'Before Refresh' }],
    }));

    app.window.fetch = vi.fn(async (url) => {
      const path = new URL(String(url), app.window.location.origin).pathname;
      if (path === '/api/monitor/refresh') {
        return {
          ok: true,
          json: async () => basePayload({
            message: 'monitor refreshed',
            updated_at: '2026-04-22T17:30:00+08:00',
            refresh_result: { success: true },
            summary: {
              account_count: 1,
              success_count: 1,
              error_count: 0,
              equity: '200',
              margin: '80',
              available_balance: '60',
              unrealized_pnl: '20',
              total_commission: '-1.2',
              distribution_apy_7d: '0.01',
            },
            groups: [{ ...basePayload().groups[0], main_account_name: 'After Refresh' }],
          }),
        };
      }
      throw new Error(`unexpected url: ${url}`);
    });

    await app.api.refreshNow();

    expect(app.document.getElementById('summaryCards').textContent).toContain('$200.00');
    expect(app.document.getElementById('groupsContainer').textContent).toContain('After Refresh');
    expect(app.document.getElementById('messageText').textContent).toContain('刷新成功');
  });

  it('keeps the current monitor cards in place when refresh only acknowledges the request and waits for the next snapshot', async () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.render(basePayload({
      summary: {
        account_count: 1,
        success_count: 1,
        error_count: 0,
        equity: '321',
        margin: '50',
        available_balance: '40',
        unrealized_pnl: '0',
        total_commission: '0',
        distribution_apy_7d: '0',
      },
      groups: [{ ...basePayload().groups[0], main_account_name: 'Stable Group' }],
    }));

    app.window.fetch = vi.fn(async (url) => {
      const path = new URL(String(url), app.window.location.origin).pathname;
      if (path === '/api/monitor/refresh') {
        return {
          ok: true,
          json: async () => ({
            message: 'Refresh completed',
            refresh_result: { success: true },
          }),
        };
      }
      throw new Error(`unexpected url: ${url}`);
    });

    await app.api.refreshNow();

    expect(app.document.getElementById('summaryCards').textContent).toContain('321.00');
    expect(app.document.getElementById('groupsContainer').textContent).toContain('Stable Group');
    expect(app.document.getElementById('messageText').textContent).toContain('等待新数据返回后自动更新');
  });

  it('renders cumulative distribution amounts in cards instead of 7-day rolling totals', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    const account = baseAccount();
    account.totals.total_distribution = '4.51899612';
    account.distribution_profit_summary = {
      all: {
        amount: '16.12597054',
      },
    };

    app.api.render(basePayload({
      summary: {
        account_count: 1,
        success_count: 1,
        error_count: 0,
        equity: '100',
        margin: '50',
        available_balance: '40',
        unrealized_pnl: '0',
        total_commission: '0',
        total_distribution: '22.59498060',
        distribution_apy_7d: '0.0310956332',
      },
      profit_summary: {
        all: {
          amount: '80.62985270',
        },
      },
      groups: [
        {
          main_account_id: 'group_a',
          main_account_name: 'Group A',
          summary: {
            account_count: 1,
            success_count: 1,
            error_count: 0,
            equity: '100',
            margin: '50',
            available_balance: '40',
            unrealized_pnl: '0',
            total_distribution: '22.59498060',
            distribution_apy_7d: '0.0310956332',
          },
          profit_summary: {
            all: {
              amount: '16.12597054',
            },
          },
          accounts: [account],
        },
      ],
    }));

    app.document.querySelector('.group-toggle-button')?.click();

    const text = app.document.body.textContent;
    expect(text).toContain('80.63');
    expect(text).toContain('16.13');
    expect(text).not.toContain('22.59');
    expect(text).not.toContain('4.52');
  });

  it('coalesces stream renders and only applies the latest payload', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.scheduleStreamRender(basePayload({
      message: 'first',
      groups: [{ ...basePayload().groups[0], main_account_name: 'First Group' }],
    }));
    app.api.scheduleStreamRender(basePayload({
      message: 'second',
      groups: [{ ...basePayload().groups[0], main_account_name: 'Second Group' }],
    }));
    app.flushAnimationFrames();

    expect(app.document.getElementById('messageText').textContent).toContain('second');
    expect(app.document.getElementById('groupsContainer').textContent).toContain('Second Group');
    expect(app.document.getElementById('groupsContainer').textContent).not.toContain('First Group');
  });

  it('caps funding logs at 300 entries in memory', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    for (let index = 0; index < 305; index += 1) {
      app.api.appendFundingLog(`log ${index}`, index % 2 === 0 ? 'info' : 'success');
    }

    const entries = app.api.getFundingLogEntries();
    expect(entries).toHaveLength(300);
    expect(entries[0].message).toBe('log 304');
    expect(entries.at(-1).message).toBe('log 5');
  }, 15000);

  it('renders masked UIDs in the funding modal', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();
    app.api.setLatestPayload(basePayload());
    app.api.setFundingOverview(baseFundingOverview());
    app.api.setFundingSelectedGroupId('group_a');
    app.api.setFundingSelectedAsset('BNB');
    app.api.renderFundingModal();

    const text = app.document.body.textContent;
    expect(text).toContain('1313**77');
    expect(text).toContain('2234***89');
    expect(text).not.toContain('13133777');
    expect(text).not.toContain('223456789');
  });

  it('caps the funding account list to five visible rows and enables internal scrolling when more sub-accounts are present', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();
    app.api.setLatestPayload(basePayload());
    app.api.setFundingSelectedGroupId('group_a');
    app.api.setFundingSelectedAsset('BNB');
    app.api.setFundingOverview(baseFundingOverview({
      children: Array.from({ length: 7 }, (_, index) => ({
        account_id: `group_a.sub${index + 1}`,
        child_account_id: `sub${index + 1}`,
        name: `Sub ${index + 1}`,
        uid: `22345678${index}`,
        can_distribute: true,
        can_collect: true,
        reason_distribute: '',
        reason_collect: '',
        reason: '',
        spot_assets: [{ asset: 'BNB', free: '1', locked: '0', total: '1' }],
        spot_available: { BNB: '1' },
        funding_assets: [{ asset: 'BNB', free: '1', locked: '0', total: '1' }],
        funding_available: { BNB: '1' },
      })),
    }));
    app.api.renderFundingModal();

    const listBody = app.document.querySelector('.funding-list-body');
    expect(listBody?.className).toContain('is-scrollable');
    expect(listBody?.style.getPropertyValue('--funding-list-max-height')).toBe('418px');
  });

  it('disables submit when write protection is enabled by backend', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();
    app.api.setLatestPayload(basePayload());
    app.api.setFundingOverview(baseFundingOverview({
      write_enabled: false,
      write_disabled_reason: '当前环境禁止真实划转',
    }));
    app.api.setFundingSelectedGroupId('group_a');
    app.api.setFundingSelectedAsset('BNB');
    app.api.renderFundingModal();

    expect(app.document.getElementById('fundingSubmitButton').disabled).toBe(true);
    expect(app.document.getElementById('fundingOperationMeta').textContent).toContain('写保护');
  });

  it('refreshes funding overview and writes success and failure logs', async () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();
    app.api.setFundingSelectedGroupId('group_a');
    app.api.setFundingSelectedAsset('BNB');
    app.api.setFundingOverview(baseFundingOverview());
    app.api.renderFundingModal();

    app.window.fetch = vi.fn(async (url) => {
      const path = new URL(String(url), app.window.location.origin).pathname;
      if (path === '/api/funding/groups/group_a') {
        return { ok: true, json: async () => baseFundingOverview() };
      }
      if (path === '/api/funding/groups/group_a/audit') {
        return { ok: true, json: async () => auditPayload() };
      }
      throw new Error(`unexpected url: ${url}`);
    });

    const successResult = await app.api.refreshFundingOverviewNow({ useCooldown: false });
    expect(successResult.success).toBe(true);
    expect(app.api.getFundingLogEntries().some((entry) => entry.message.includes('正在刷新当前分组资金信息'))).toBe(true);
    expect(app.api.getFundingLogEntries().some((entry) => entry.message.includes('当前分组资金信息刷新成功'))).toBe(true);

    app.window.fetch = vi.fn(async (url) => {
      const path = new URL(String(url), app.window.location.origin).pathname;
      if (path === '/api/funding/groups/group_a') {
        return {
          ok: false,
          json: async () => ({ detail: 'boom' }),
        };
      }
      throw new Error(`unexpected url: ${url}`);
    });

    const failureResult = await app.api.refreshFundingOverviewNow({ useCooldown: false });
    expect(failureResult.success).toBe(false);
    expect(app.api.getFundingLogEntries()[0].message).toContain('boom');
  });

  it('submits a funding distribute action with operation_id and renders audit entries', async () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();
    app.api.setLatestPayload(basePayload());
    app.api.setFundingOverview(baseFundingOverview());
    app.api.setFundingDirection('distribute');
    app.api.setFundingSelectedGroupId('group_a');
    app.api.setFundingSelectedAsset('BNB');
    app.api.setFundingSelectionState({
      'group_a.sub1': { checked: true, amount: '1.5' },
    });
    app.api.renderFundingModal();

    const auditEntries = [
      {
        operation_id: 'funding-op-123',
        direction: 'distribute',
        operation_status: 'operation_fully_succeeded',
        message: 'Distribute succeeded for 1 sub-accounts',
        asset: 'BNB',
        updated_at: '2026-03-29T12:00:00+08:00',
      },
    ];
    let submittedOperationId = '';

    app.window.fetch = vi.fn(async (url, options = {}) => {
      const path = new URL(String(url), app.window.location.origin).pathname;
      if (path === '/api/funding/groups/group_a/distribute') {
        expect(options.method).toBe('POST');
        const requestBody = JSON.parse(options.body);
        submittedOperationId = String(requestBody.operation_id || '');
        expect(requestBody.asset).toBe('BNB');
        expect(submittedOperationId.length).toBeGreaterThan(0);
        expect(requestBody.transfers).toEqual([{ account_id: 'group_a.sub1', amount: '1.5' }]);
        return {
          ok: true,
          json: async () => ({
            operation_id: submittedOperationId,
            idempotent_hit: false,
            operation_status: 'operation_fully_succeeded',
            message: 'Distribute succeeded for 1 sub-accounts',
            results: [{
              account_id: 'group_a.sub1',
              success: true,
              message: 'Distribute succeeded',
              requested_amount: '1.5',
              executed_amount: '1.5',
              transfer_attempted: true,
            }],
            precheck: {
              asset: 'BNB',
              requested_total_amount: '1.5',
              validated_account_count: 1,
              main_available_amount: '10',
            },
            overview: baseFundingOverview(),
            overview_refresh: { success: true, message: '' },
            reconciliation: {
              status: 'confirmed',
              confirmed_count: 1,
              failed_count: 0,
              results: [],
            },
          }),
        };
      }
      if (path === '/api/funding/groups/group_a/audit') {
        return {
          ok: true,
          json: async () => auditPayload({ entries: auditEntries }),
        };
      }
      if (path === '/api/funding/groups/group_a/audit/funding-op-123') {
        expect(new URL(String(url), app.window.location.origin).searchParams.get('direction')).toBe('distribute');
        return {
          ok: true,
          json: async () => ({
            operation_id: 'funding-op-123',
            direction: 'distribute',
            asset: 'BNB',
            execution_stage: 'completed',
            operation_status: 'operation_fully_succeeded',
            message: 'Distribute succeeded for 1 sub-accounts',
            operation_summary: {
              asset: 'BNB',
              requested_total_amount: '1.5',
              attempted_count: 1,
              success_count: 1,
              failure_count: 0,
              confirmed_count: 1,
              pending_confirmation_count: 0,
              main_before_available_amount: '10',
              main_after_available_amount: '8.5',
              expected_main_direction: 'decrease',
              unconfirmed_account_ids: [],
            },
            precheck: {
              asset: 'BNB',
              requested_total_amount: '1.5',
              validated_account_count: 1,
              main_available_amount: '10',
              accounts: [{ account_id: 'group_a.sub1', precheck_available_amount: '3.2' }],
            },
            overview_refresh: { success: true, message: '' },
            reconciliation: { status: 'confirmed', confirmed_count: 1, failed_count: 0, results: [] },
            results: [{
              account_id: 'group_a.sub1',
              name: 'Sub 1',
              uid: '2234***89',
              success: true,
              requested_amount: '1.5',
              executed_amount: '1.5',
              precheck_available_amount: '3.2',
              transfer_attempted: true,
              message: 'Distribute succeeded',
            }],
          }),
        };
      }
      if (path === '/api/monitor/refresh') {
        return {
          ok: true,
          json: async () => basePayload({ message: 'monitor refreshed' }),
        };
      }
      throw new Error(`unexpected url: ${url}`);
    });

    await app.api.submitFundingOperation();

    const messages = app.api.getFundingLogEntries().map((entry) => entry.message);
    expect(messages.some((message) => message.includes('正在执行现货分发'))).toBe(true);
    expect(app.document.getElementById('fundingOperationMeta').textContent).toContain(submittedOperationId.slice(0, 8));

    await app.api.loadFundingAudit('group_a');
    app.api.setFundingActiveLogTab('audit');
    const auditText = app.document.getElementById('fundingLogList').textContent;
    expect(auditText).toContain('Distribute succeeded for 1 sub-accounts');
    expect(auditText).toContain('BNB');
    expect(auditText).toContain('1.5');
  });

  it('renders audit summary/detail split, filters by operation_id, and copies operation id', async () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();
    app.api.setLatestPayload(basePayload());
    app.api.setFundingOverview(baseFundingOverview());
    app.api.setFundingSelectedGroupId('group_a');
    app.api.setFundingSelectedAsset('BNB');
    app.api.setFundingPendingOperationId('funding-op-xyz-1234');
    app.api.renderFundingModal();

    app.window.fetch = vi.fn(async (url) => {
      const path = new URL(String(url), app.window.location.origin).pathname;
      if (path === '/api/funding/groups/group_a/audit') {
        return {
          ok: true,
          json: async () => auditPayload({
            entries: [
              {
                operation_id: 'funding-op-xyz-1234',
                direction: 'collect',
                operation_status: 'operation_fully_succeeded',
                execution_stage: 'completed',
                message: 'Collect succeeded for 1 sub-accounts',
                asset: 'BNB',
                created_at: '2026-03-29T12:00:00+08:00',
              },
              {
                operation_id: 'funding-op-older-0001',
                direction: 'distribute',
                operation_status: 'operation_submitted',
                execution_stage: 'executing',
                message: 'Collect submitted for 1 sub-accounts',
                asset: 'BNB',
                created_at: '2026-03-29T11:00:00+08:00',
              },
            ],
          }),
        };
      }
      if (path === '/api/funding/groups/group_a/audit/funding-op-xyz-1234') {
        expect(new URL(String(url), app.window.location.origin).searchParams.get('direction')).toBe('collect');
        return {
          ok: true,
          json: async () => ({
            operation_id: 'funding-op-xyz-1234',
            direction: 'collect',
            asset: 'BNB',
            execution_stage: 'completed',
            operation_status: 'operation_fully_succeeded',
            message: 'Collect succeeded for 1 sub-accounts',
            operation_summary: {
              asset: 'BNB',
              requested_total_amount: '2',
              attempted_count: 1,
              success_count: 1,
              failure_count: 0,
              confirmed_count: 1,
              pending_confirmation_count: 0,
              main_before_available_amount: '10',
              main_after_available_amount: '12',
              expected_main_direction: 'increase',
              unconfirmed_account_ids: [],
            },
            precheck: {
              asset: 'BNB',
              requested_total_amount: '2',
              validated_account_count: 1,
              main_available_amount: '10',
              accounts: [{ account_id: 'group_a.sub1', precheck_available_amount: '2' }],
            },
            overview_refresh: { success: true, message: '' },
            reconciliation: { status: 'confirmed', confirmed_count: 1, failed_count: 0, results: [] },
            results: [{
              account_id: 'group_a.sub1',
              name: 'Sub 1',
              uid: '2234***89',
              success: true,
              requested_amount: '2',
              executed_amount: '2',
              precheck_available_amount: '2',
              transfer_attempted: true,
              message: 'Collect succeeded',
            }],
          }),
        };
      }
      throw new Error(`unexpected url: ${url}`);
    });

    await app.api.loadFundingAudit('group_a');
    app.api.setFundingActiveLogTab('audit');
    const list = app.document.getElementById('fundingLogList');
    expect(list.textContent).toContain('Collect succeeded for 1 sub-accounts');
    expect(list.textContent).toContain('completed');

    await app.api.setFundingAuditFilter('funding-op-older');
    expect(app.document.getElementById('fundingLogList').textContent).toContain('Collect submitted for 1 sub-accounts');
    expect(app.document.getElementById('fundingLogList').textContent).not.toContain('Collect succeeded for 1 sub-accounts');

    await app.api.copyFundingOperationId();
    expect(app.window.navigator.clipboard.writeText).toHaveBeenCalledWith('funding-op-older-0001');
  });

  it('falls back to execCommand copy when clipboard API is unavailable', async () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();
    app.api.setFundingPendingOperationId('funding-op-fallback-0001');
    app.window.navigator.clipboard = undefined;
    app.window.document.execCommand = vi.fn(() => true);

    await app.api.copyFundingOperationId();

    expect(app.window.document.execCommand).toHaveBeenCalledWith('copy');
    expect(app.api.getFundingLogEntries()[0].message).toContain('已复制操作ID');
  });

  it('does not render the audit search input in the funding audit panel', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.setFundingActiveLogTab('audit');

    expect(app.document.querySelector('[data-funding-audit-filter]')).toBeNull();
    expect(app.document.querySelector('.funding-audit-filter')).toBeNull();
  });

  it('requests audit detail with direction when operation ids are reused across modes', async () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();
    app.api.setLatestPayload(basePayload());
    app.api.setFundingOverview(baseFundingOverview());
    app.api.setFundingSelectedGroupId('group_a');
    app.api.setFundingSelectedAsset('BNB');
    app.api.renderFundingModal();

    const detailDirections = [];
    app.window.fetch = vi.fn(async (url) => {
      const parsed = new URL(String(url), app.window.location.origin);
      const path = parsed.pathname;
      if (path === '/api/funding/groups/group_a/audit') {
        return {
          ok: true,
          json: async () => auditPayload({
            entries: [
              {
                operation_id: 'shared-op',
                direction: 'distribute',
                operation_status: 'operation_fully_succeeded',
                execution_stage: 'completed',
                message: 'Distribute succeeded',
                asset: 'BNB',
                created_at: '2026-03-29T12:00:00+08:00',
              },
              {
                operation_id: 'shared-op',
                direction: 'collect',
                operation_status: 'operation_fully_succeeded',
                execution_stage: 'completed',
                message: 'Collect succeeded',
                asset: 'BNB',
                created_at: '2026-03-29T11:00:00+08:00',
              },
            ],
          }),
        };
      }
      if (path === '/api/funding/groups/group_a/audit/shared-op') {
        detailDirections.push(parsed.searchParams.get('direction'));
        return {
          ok: true,
          json: async () => ({
            operation_id: 'shared-op',
            direction: parsed.searchParams.get('direction'),
            execution_stage: 'completed',
            operation_status: 'operation_fully_succeeded',
            asset: 'BNB',
            message: 'ok',
            operation_summary: {},
            precheck: {},
            overview_refresh: { success: true, message: '' },
            reconciliation: { status: 'confirmed', confirmed_count: 0, failed_count: 0, results: [] },
            results: [],
          }),
        };
      }
      throw new Error(`unexpected url: ${url}`);
    });

    await app.api.loadFundingAudit('group_a');
    await app.api.loadFundingAuditDetail('group_a', 'shared-op', 'collect');

    expect(detailDirections).toContain('distribute');
    expect(detailDirections).toContain('collect');
  });

  it('describes settings-only import without account replacement wording', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    const message = app.api.describeImportResult({
      import_result: {
        mode: 'settings_only',
        main_account_count: 0,
        account_count: 0,
        updated_settings_keys: ['telegram.bot_token'],
      },
      refresh_result: { success: true },
      security_notice: '敏感信息已转入加密仓库',
    });

    expect(message).toContain('已更新 1 项敏感配置');
    expect(message).not.toContain('已覆盖 0 个分组 / 0 个账户');
  });

  it('surfaces plain-text import errors instead of JSON syntax failures', async () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.window.fetch = vi.fn(async (url) => {
      const path = new URL(String(url), app.window.location.origin).pathname;
      if (path === '/api/config/import/excel') {
        return {
          ok: false,
          status: 500,
          headers: { get: () => 'text/plain; charset=utf-8' },
          text: async () => 'Internal Server Error',
        };
      }
      throw new Error(`unexpected url: ${url}`);
    });

    await app.api.uploadExcel(
      new app.window.File(['xlsx'], 'accounts.xlsx', {
        type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      }),
    );

    expect(app.document.getElementById('messageText').textContent).toContain('Internal Server Error');
    expect(app.document.getElementById('messageText').textContent).not.toContain('Unexpected token');
  });
});
