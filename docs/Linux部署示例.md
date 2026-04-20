# Linux 部署示例

这份示例用于把当前项目部署到 Linux 服务器，并启用：

- 访问控制
- 本地加密 Secret 仓库
- Excel 导入
- systemd 常驻运行

## 目标结构

建议约定：

- 项目目录：`/opt/binance-account-monitor`
- 服务账号：`monitor`
- 主密钥文件：`/etc/monitor-secrets/monitor-master-key`

核心原则：

- 业务配置只保留 `*_secret_ref`
- 真正的 secret 保存在：
  - `config/secrets.enc.json`
- 主密钥文件放在仓库外，并只给服务账号读

## 1. 创建服务账号和目录

```bash
sudo useradd --system --create-home --shell /usr/sbin/nologin monitor
sudo mkdir -p /opt/binance-account-monitor
sudo chown -R monitor:monitor /opt/binance-account-monitor
```

## 2. 安装项目与虚拟环境

```bash
cd /opt/binance-account-monitor
python3.12 -m venv .venv
. .venv/bin/activate
pip install --upgrade pip
pip install -e .[dev]
```

## 3. 初始化配置文件

```bash
cp .env.example .env
cp config/binance_monitor_accounts.example.json config/binance_monitor_accounts.json
```

如需启用访问控制，确认：

- `config/access_control.json`

已经存在并填写好对应配置。

## 4. 创建主密钥文件

```bash
sudo install -d -m 700 -o monitor -g monitor /etc/monitor-secrets
sudo touch /etc/monitor-secrets/monitor-master-key
sudo chown monitor:monitor /etc/monitor-secrets/monitor-master-key
sudo chmod 600 /etc/monitor-secrets/monitor-master-key
```

说明：

- 这不是游客密码、管理员密码，也不是 Binance API key
- 它只是解密本地密文 secret 仓库的主密钥

## 5. 配置 `.env`

最少建议这样配：

```env
MONITOR_APP_NAME=Binance Account Monitor
MONITOR_API_HOST=0.0.0.0
MONITOR_API_PORT=8010

ACCESS_CONTROL_CONFIG_FILE=config/access_control.json
MONITOR_ACCOUNTS_FILE=config/binance_monitor_accounts.json
SECRETS_FILE=config/example-secrets.enc.json
MONITOR_MASTER_KEY_FILE=/etc/monitor-secrets/example-monitor-master-key

TG_ENABLED=true
TG_BOT_TOKEN_SECRET_REF=telegram.bot_token_example
TG_CHAT_ID_SECRET_REF=telegram.chat_id_example
TG_PROXY_URL=
TG_MAX_QUEUE_SIZE=50
TG_DRY_RUN=false

UNI_MMR_ALERTS_ENABLED=true
```

关键项：

- `MONITOR_API_HOST=0.0.0.0`
- `SECRETS_FILE=config/secrets.enc.json`
- `MONITOR_MASTER_KEY_FILE=/etc/monitor-secrets/monitor-master-key`

## 6. 配置访问控制

推荐配置示例：

```json
{
  "enabled": true,
  "whitelist_ips": ["127.0.0.1", "::1"],
  "allow_plaintext_secrets": false,
  "cookie_secure_mode": "auto",
  "guest_password_secret_ref": "access_control.guest_password_example",
  "admin_password_secret_ref": "access_control.admin_password_example",
  "session_secret_secret_ref": "access_control.session_secret_example"
}
```

说明：

- Linux 上也不再推荐在这里写明文密码
- 只保留 ref
- 真实密码在加密 secret 仓库中

## 7. 一次性迁移现有明文 secret

### 初始化主密钥

```bash
cd /opt/binance-account-monitor
. .venv/bin/activate
python -m monitor_app.secrets_cli init --master-key-file /etc/monitor-secrets/monitor-master-key
```

### 执行迁移

```bash
python -m monitor_app.secrets_cli migrate \
  --write-config \
  --master-key-file /etc/monitor-secrets/monitor-master-key \
  --env-file .env
```

迁移完成后：

- `config/access_control.json` 只保留 `*_secret_ref`
- `config/binance_monitor_accounts.json` 只保留 `*_secret_ref`
- `.env` 不再保留 `TG_BOT_TOKEN` / `TG_CHAT_ID` 明文
- 新生成：
  - `config/secrets.enc.json`

## 8. Excel 导入是否还能继续使用

支持，方式不变。

Excel 模板现在包含：

- `accounts`
- `settings`
- `guide`

语义：

- `accounts`：整表替换
- `settings`：增量覆盖

导入后：

- Excel 里的敏感值会自动写进：
  - `config/secrets.enc.json`
- 配置文件只保留 `*_secret_ref`

说明文档见：

- [Excel导入模板说明](./Excel导入模板说明.md)

## 9. 创建 systemd 服务

新建：

```bash
sudo nano /etc/systemd/system/binance-account-monitor.service
```

示例内容：

```ini
[Unit]
Description=Binance Account Monitor
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=monitor
Group=monitor
WorkingDirectory=/opt/binance-account-monitor
Environment=MONITOR_MASTER_KEY_FILE=/etc/monitor-secrets/monitor-master-key
ExecStart=/opt/binance-account-monitor/.venv/bin/python -m monitor_app.main
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

加载并启动：

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now binance-account-monitor
```

查看状态：

```bash
sudo systemctl status binance-account-monitor
journalctl -u binance-account-monitor -f
```

## 10. 日常维护 Secret

以后不要再把密码、Token、API key 明文写回配置文件。

常用命令：

```bash
python -m monitor_app.secrets_cli list
python -m monitor_app.secrets_cli doctor
python -m monitor_app.secrets_cli set access_control.admin_password
python -m monitor_app.secrets_cli set access_control.guest_password
python -m monitor_app.secrets_cli set access_control.session_secret
python -m monitor_app.secrets_cli set telegram.bot_token
python -m monitor_app.secrets_cli set telegram.chat_id
```

账号类 Secret Ref 规则：

- `main_accounts.<main_id>.transfer_api_key`
- `main_accounts.<main_id>.transfer_api_secret`
- `accounts.<main_id>.<account_id>.api_key`
- `accounts.<main_id>.<account_id>.api_secret`

## 11. 部署后自查

### 探活

```bash
curl http://127.0.0.1:8010/healthz
```

### 登录页

```bash
curl -I http://127.0.0.1:8010/login
```

### 会话状态

```bash
curl http://127.0.0.1:8010/api/auth/session
```

### Secret 权限

```bash
ls -l /etc/monitor-secrets/monitor-master-key
ls -l /opt/binance-account-monitor/config/secrets.enc.json
```

## 12. 安全建议

- 主密钥文件不要提交到 Git
- 主密钥文件权限至少控制到 `600`
- 尽量只允许服务账号读取主密钥文件
- 迁移完成后，建议逐步轮换：
  - Binance API key / secret
  - guest / admin password
  - `session_secret`
  - Telegram token

## 一句话总结

Linux 上最推荐的部署方式是：

- 项目目录里放业务配置和 `config/secrets.enc.json`
- 仓库外放 `/etc/monitor-secrets/monitor-master-key`
- 用 `python -m monitor_app.secrets_cli ...` 管理 secret
- 用 `systemd` 常驻运行服务

这样既兼容 Linux，也避免把真实 secret 长期明文保存在业务配置文件里。
