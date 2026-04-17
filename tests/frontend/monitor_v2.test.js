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

  it('coalesces stream renders and only applies the latest payload', () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();

    app.api.scheduleStreamRender(basePayload({ message: 'first', groups: [{ ...basePayload().groups[0], main_account_name: 'First Group' }] }));
    app.api.scheduleStreamRender(basePayload({ message: 'second', groups: [{ ...basePayload().groups[0], main_account_name: 'Second Group' }] }));
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

  it('refreshes funding overview and writes success and failure logs', async () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();
    app.api.setFundingSelectedGroupId('group_a');
    app.api.setFundingSelectedAsset('BNB');
    app.api.setFundingOverview(baseFundingOverview());
    app.api.renderFundingModal();

    app.window.fetch = vi.fn(async (url) => {
      if (String(url).includes('/api/funding/groups/group_a')) {
        return { ok: true, json: async () => baseFundingOverview() };
      }
      throw new Error(`unexpected url: ${url}`);
    });

    const successResult = await app.api.refreshFundingOverviewNow({ useCooldown: false });
    expect(successResult.success).toBe(true);
    expect(app.api.getFundingLogEntries()[0].message).toBe('当前分组资金信息刷新成功。');
    expect(app.api.getFundingLogEntries()[1].message).toBe('正在刷新当前分组资金信息…');

    app.window.fetch = vi.fn(async () => ({
      ok: false,
      json: async () => ({ detail: 'boom' }),
    }));

    const failureResult = await app.api.refreshFundingOverviewNow({ useCooldown: false });
    expect(failureResult.success).toBe(false);
    expect(app.api.getFundingLogEntries()[0].message).toContain('资金信息刷新失败：');
  });

  it('submits a funding distribute action and appends operation logs', async () => {
    const app = createApp();
    apps.push(app);
    app.api.resetTestState();
    app.api.setFundingOverview(baseFundingOverview());
    app.api.setFundingDirection('distribute');
    app.api.setFundingSelectedGroupId('group_a');
    app.api.setFundingSelectedAsset('BNB');
    app.api.setFundingSelectionState({
      'group_a.sub1': { checked: true, amount: '1.5' },
    });
    app.api.renderFundingModal();

    app.window.fetch = vi.fn(async (url, options = {}) => {
      if (String(url).includes('/api/funding/groups/group_a/distribute')) {
        expect(options.method).toBe('POST');
        return {
          ok: true,
          json: async () => ({
            message: 'Distribute succeeded for 1 sub-accounts',
            results: [{ account_id: 'group_a.sub1', success: true, message: 'Distribute succeeded', amount: '1.5' }],
            overview: baseFundingOverview(),
          }),
        };
      }
      if (String(url).includes('/api/funding/groups/group_a')) {
        return {
          ok: true,
          json: async () => baseFundingOverview(),
        };
      }
      if (String(url).includes('/api/monitor/refresh')) {
        return {
          ok: true,
          json: async () => basePayload({ message: 'monitor refreshed' }),
        };
      }
      throw new Error(`unexpected url: ${url}`);
    });

    await app.api.submitFundingOperation();

    const messages = app.api.getFundingLogEntries().map((entry) => entry.message);
    expect(messages).toContain('正在执行现货分发…');
    expect(messages).toContain('Distribute succeeded for 1 sub-accounts');
    expect(messages).toContain('当前分组资金信息刷新成功。');
    expect(messages).toContain('主界面监控信息刷新成功。');
  });
});
