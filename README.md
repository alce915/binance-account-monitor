# binance-account-monitor

独立部署的币安统一账户监控 Web 服务，支持分组监控、SSE 实时推送、资金归集/分发、Telegram 告警，以及本地加密 Secret 存储。

## 核心能力

- 主账号 / 子账号分组式监控
- 统一账户快照聚合与状态页展示
- REST API + SSE 实时推送
- Excel 批量导入账号配置
- 资金归集 / 分发操作审计
- 访问控制：
  - 白名单 IP 免登录
  - 游客 / 管理员双密码
  - 服务端权限收口
- 本地加密 Secret 仓库：
  - API key / secret
  - Telegram token / chat id
  - 登录密码 / `session_secret`

## 快速开始

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .[dev]
Copy-Item .env.example .env
Copy-Item config\binance_monitor_accounts.example.json config\binance_monitor_accounts.json
```

如果配置文件缺失，服务启动脚本会按模板补齐，但首次运行前仍需要你填写真实配置。

## 启动方式

```powershell
binance-account-monitor
```

或直接使用脚本：

```powershell
重启监控服务.bat
```

默认监听：

- `127.0.0.1:8010`

如果要让局域网设备访问，再把 `.env` 里的 `MONITOR_API_HOST` 改成 `0.0.0.0` 后重启。

## 配置结构

### 普通配置

- `.env`
- `config/access_control.json`
- `config/binance_monitor_accounts.json`

### 敏感信息

真实 secret 不再直接保存在上面的业务配置里，而是进入：

- `config/secrets.enc.json`

解密依赖主密钥：

- `MONITOR_MASTER_KEY_FILE`
- 或 `MONITOR_MASTER_KEY`

推荐使用：

- `MONITOR_MASTER_KEY_FILE`

主密钥文件应放在仓库外，并限制为当前服务账号可读。

## 部署顺序 / 初始化顺序

建议按下面顺序初始化，能避免先启动后补配置时反复踩坑：

1. 复制基础文件
   - `Copy-Item .env.example .env`
   - `Copy-Item config\binance_monitor_accounts.example.json config\binance_monitor_accounts.json`
2. 准备主密钥文件
   - 先在仓库外创建主密钥文件
   - 再把 `.env` 里的 `MONITOR_MASTER_KEY_FILE` 指向它
3. 初始化或迁移本地 Secret 仓库
   - 首次部署：先执行本地密钥初始化 / 迁移工具
   - 已有明文配置：先迁移到 `config/secrets.enc.json`
4. 填写访问控制配置
   - 编辑 `config/access_control.json`
   - 默认使用 `*_secret_ref`
   - 只有显式开启兼容模式时才允许明文字段
5. 准备账号配置
   - 直接编辑 `config/binance_monitor_accounts.json`
   - 或者更推荐：通过 Excel 模板导入
6. 检查 Telegram / 其他全局密钥
   - `.env` 里只保留 `*_SECRET_REF`
   - 不再回填明文 token / password
7. 启动服务并做自查
   - `重启监控服务.bat`
   - 先访问 `GET /healthz`
   - 再确认 `/api/auth/session`、首页、Excel 下载模板是否正常
   - 如果 secret ref 悬空或仓库与配置不一致，服务会在启动阶段直接报错，而不是静默带病运行

如果是新机器首装，最稳的顺序是：

- 先 `.env`
- 再主密钥文件
- 再 secret 迁移 / 初始化
- 再 `access_control.json`
- 最后账号配置 / Excel 导入

## Excel 导入

导入入口：

- `POST /api/config/import/excel`

模板下载：

- `GET /api/config/import/excel-template`

当前模板包含 3 个工作表：

- `accounts`
- `settings`
- `guide`

导入语义：

- `accounts`：整表替换（`replace_all`）
- `settings`：增量覆盖，只更新填写过的项

导入后：

- 敏感信息自动写入加密 Secret 仓库
- 业务配置只保留 `*_secret_ref`
- 服务端不会长期保存上传的 Excel 文件

说明文档见：

- [Excel导入模板说明](docs/Excel导入模板说明.md)

## 本地 Secret 管理

Windows 常用入口：

- [管理本地密钥.bat](管理本地密钥.bat)
- [迁移本地密钥.bat](迁移本地密钥.bat)

底层 CLI：

```powershell
python -m monitor_app.secrets_cli list
python -m monitor_app.secrets_cli set access_control.admin_password
python -m monitor_app.secrets_cli doctor
```

速查表见：

- [本地密钥管理速查表](docs/本地密钥管理速查表.md)

## 访问控制

当前访问控制支持：

- 白名单 IP 直通
- 游客 / 管理员密码登录
- CSRF 校验
- break-glass 本机应急入口
- 服务端 capability 权限控制

接入说明见：

- [访问控制模块接入说明](docs/访问控制模块接入说明.md)

## Linux 部署

Linux 部署和 systemd 示例见：

- [Linux部署示例](docs/Linux部署示例.md)

## 常用接口

- `GET /healthz`
- `GET /api/auth/session`
- `GET /api/auth/audit`
- `GET /api/monitor/summary`
- `GET /api/monitor/groups`
- `GET /api/monitor/accounts`
- `POST /api/monitor/control`
- `POST /api/monitor/refresh`
- `POST /api/config/import/excel`
- `GET /api/config/import/excel-template`
- `GET /api/funding/groups/{main_id}`
- `POST /api/funding/groups/{main_id}/distribute`
- `POST /api/funding/groups/{main_id}/collect`
- `POST /api/alerts/telegram/test`
- `GET /api/alerts/unimmr/status`
- `POST /api/alerts/unimmr/simulate`
- `GET /stream/monitor`

## 说明

- Excel 文件本身仍然是明文敏感文件；导入成功后建议立即删除本地副本。
- 当前仓库已经切到 `refs-only` 默认模式，后续不要再把真实 secret 手工写回业务配置文件。
- 所有浏览器写请求现在还会额外校验同源 `Origin/Referer`；跨站请求即使带了 cookie 和 CSRF 也会被拒绝。
- 会话默认空闲超时：
  - `admin` 30 分钟
  - `guest` 120 分钟
- 登录限流状态现在会持久化到本地 SQLite；服务重启后，仍在冷却中的 IP 不会被自动清空。
