# binance-account-monitor

独立部署的币安统一账户监控 Web 服务。

## 功能说明

- 可与配对开单项目分离部署
- 支持主账户 -> 子账户的分层监控配置
- 支持统一账户快照聚合
- 提供 REST API 与 SSE 实时推送
- 自带独立的监控 Web 控制台

## 环境准备

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -e .[dev]
Copy-Item .env.example .env
Copy-Item config\binance_monitor_accounts.example.json config\binance_monitor_accounts.json
```

如果你跳过最后两步复制命令，重启脚本会在首次运行时自动根据模板补齐缺失的配置文件，然后提示你先填写真实配置再重新启动。

## 启动方式

```powershell
binance-account-monitor
```

或者使用：

```powershell
scripts\restart_monitor_service.bat
```

当 `.env` 或 `config\binance_monitor_accounts.json` 缺失时，重启脚本会自动从示例模板生成文件，并停止启动流程，提醒你先填写自己的 Binance API 凭证。

本地默认监听地址为 `127.0.0.1:8010`，配置来源于 `.env`。  
本项目明确不使用 `8000` 端口。

## 接口列表

- `GET /healthz`
- `GET /api/monitor/summary`
- `GET /api/monitor/groups`
- `GET /api/monitor/accounts`
- `GET /stream/monitor`

## 配置说明

服务会从以下文件读取子账户 API 凭证：

- `config/binance_monitor_accounts.json`

主账户只作为分组节点使用，真正的 Binance API 凭证由子账户承担。
