import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

import { JSDOM } from 'jsdom';
import { afterEach, describe, expect, it, vi } from 'vitest';

const projectRoot = resolve(fileURLToPath(new URL('.', import.meta.url)), '..', '..');
const htmlPath = resolve(projectRoot, 'monitor_app', 'static', 'login.html');
const cssPath = resolve(projectRoot, 'monitor_app', 'static', 'login.css');
const loginI18n = {
  title: '认证登录',
  description: '请输入游客密码或管理员密码。非白名单地址在登录前只能访问此页面。',
  password_label: '访问密码',
  password_placeholder: '请输入密码',
  submit_button: '进入系统',
  not_initialized_description: '认证未初始化，请先完成 access_control.json 配置。',
  not_initialized_error: '认证未初始化',
  password_required_error: '请输入访问密码',
  auth_failed_error: '认证失败',
  request_failed_error: '登录请求失败，请稍后重试',
  session_failed_error: '无法获取认证状态，请稍后重试',
};

function createApp(url = 'http://127.0.0.1:8010/login') {
  const html = readFileSync(htmlPath, 'utf8').replace('__LOGIN_I18N__', JSON.stringify(loginI18n));
  const dom = new JSDOM(html, {
    url,
    pretendToBeVisual: true,
    runScripts: 'outside-only',
  });
  const { window } = dom;
  const script = window.document.querySelector('script');
  const replaceSpy = vi.fn();

  window.__LOGIN_TEST_MODE__ = true;
  window.fetch = vi.fn(async () => ({
    ok: true,
    status: 200,
    json: async () => ({
      enabled: true,
      initialized: true,
      authenticated: false,
      whitelisted: false,
      role: '',
      auth_source: 'none',
      csrf_token: '',
      last_activity_at: null,
    }),
  }));
  Object.defineProperty(window, '__loginReplaceSpy__', { value: replaceSpy, configurable: true });
  window.eval(script.textContent);

  return {
    window,
    replaceSpy,
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

describe('login.html', () => {
  it('keeps the login field compact and the submit button in the highlighted black-gold active style', () => {
    const css = readFileSync(cssPath, 'utf8');

    expect(css).not.toContain('min-height: 54px;');
    expect(css).toContain('box-sizing: border-box;');
    expect(css).toContain('padding: 14px 16px;');
    expect(css).toContain('min-height: 56px;');
    expect(css).toContain('color: var(--login-text-primary);');
    expect(css).toContain('radial-gradient(circle at 12% 10%, rgba(247, 220, 168, 0.34), transparent 34%)');
    expect(css).toContain('linear-gradient(180deg, rgba(255, 226, 170, 0.26) 0%, rgba(186, 133, 61, 0.14) 54%, rgba(47, 38, 25, 0.92) 100%)');
  });

  it('marks the login document for the black-gold theme', () => {
    const app = createApp();
    apps.push(app);

    expect(app.window.document.body.dataset.theme).toBe('black-gold');
    expect(app.window.document.querySelector('.card')).not.toBeNull();
  });

  it('sanitizes next targets to same-origin relative paths', () => {
    const app = createApp('http://127.0.0.1:8010/login?next=https://evil.example/phish');
    apps.push(app);

    expect(app.window.__loginPage.sanitizeNextPath('/api/auth/session')).toBe('/api/auth/session');
    expect(app.window.__loginPage.sanitizeNextPath('https://evil.example/phish')).toBe('/');
    expect(app.window.__loginPage.sanitizeNextPath('//evil.example/phish')).toBe('/');
    expect(app.window.__loginPage.sanitizeNextPath('javascript:alert(1)')).toBe('/');
  });

  it('redirects authenticated users only to sanitized same-origin paths', async () => {
    const app = createApp('http://127.0.0.1:8010/login?next=https://evil.example/phish');
    apps.push(app);

    app.window.fetch = vi.fn(async () => ({
      ok: true,
      status: 200,
      json: async () => ({
        enabled: true,
        initialized: true,
        authenticated: true,
        whitelisted: false,
        role: 'guest',
        auth_source: 'session',
      }),
    }));

    await app.window.__loginPage.loadSession();

    expect(app.replaceSpy).toHaveBeenCalledWith('/');
  });
});
