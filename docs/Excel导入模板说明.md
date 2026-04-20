# Excel 导入模板说明

## 适用范围
- 用于批量导入 Binance 监控账号配置和受支持的全局敏感配置
- 导入入口：`POST /api/config/import/excel`
- 模板下载：`GET /api/config/import/excel-template`
- 当前导入语义分两部分：
  - `accounts` 分页：`replace_all`
    - 每次成功导入后，会用 Excel 里的账号内容整体替换当前账号配置
  - `settings` 分页：增量覆盖
    - 只会覆盖填写过的敏感配置
    - 空白不会删除旧值

## 模板列说明
当前模板固定包含这些列：

| 列名 | 是否必填 | 说明 |
| --- | --- | --- |
| `main_id` | 是 | 主分组 ID，只能包含小写字母、数字、`_`、`-` |
| `main_name` | 是 | 主分组显示名，同一个 `main_id` 下必须保持一致 |
| `account_id` | 是 | 子账号 ID；如果这一行是主账号转账配置，固定填 `main` |
| `name` | 是 | 账号显示名；`account_id=main` 时可填主账号描述 |
| `api_key` | 是 | API Key；`account_id=main` 时表示 `transfer_api_key` |
| `api_secret` | 是 | API Secret；`account_id=main` 时表示 `transfer_api_secret` |
| `uid` | 否 | 账号 UID；但 `account_id=main` 时必填，作为 `transfer_uid` |
| `use_testnet` | 否 | 是否使用测试网，支持：`true/false/1/0/yes/no/on/off` |
| `rest_base_url` | 否 | 自定义 REST 地址 |
| `ws_base_url` | 否 | 自定义 WebSocket 地址 |

## 下载模板现在包含什么
下载得到的 Excel 模板现在包含三个工作表：

1. `accounts`
   - 真正用于填写和导入的主表
   - 列结构必须保持不变
2. `settings`
   - 用于填写受支持的全局敏感配置
   - 当前内置白名单：
     - `telegram.bot_token`
     - `telegram.chat_id`
     - `access_control.guest_password`
     - `access_control.admin_password`
     - `access_control.session_secret`
   - 只有填写了 `value` 的键才会覆盖旧值
3. `guide`
   - 模板内置说明页
   - 用来提醒主账号行怎么填、子账号行怎么填、`settings` 怎么用，以及导入后的 secret 存储方式

注意：
- 导入时会先校验 `guide` 里的 `template_version`
- `accounts` 参与账号导入
- `settings` 参与受支持 key 的增量更新
- `guide` 只做说明，不参与业务数据导入

## 主账号转账行怎么填
主账号转账配置不是单独的列，而是单独的一行。

写法固定是：
- `account_id = main`
- `api_key` 填主账号 `transfer_api_key`
- `api_secret` 填主账号 `transfer_api_secret`
- `uid` 填主账号 `transfer_uid`

示例：

| main_id | main_name | account_id | name | api_key | api_secret | uid |
| --- | --- | --- | --- | --- | --- | --- |
| `group_a` | `Group A` | `main` | `Main Transfer` | 主账号转账 Key | 主账号转账 Secret | 主账号 UID |

## 子账号行怎么填
普通子账号按每行一个账号填写：

| main_id | main_name | account_id | name | api_key | api_secret | uid |
| --- | --- | --- | --- | --- | --- | --- |
| `group_a` | `Group A` | `sub1` | `Sub One` | 子账号 Key | 子账号 Secret | 子账号 UID |
| `group_a` | `Group A` | `sub2` | `Sub Two` | 子账号 Key | 子账号 Secret | 子账号 UID |

## 导入校验规则
导入时当前会检查这些规则：

- 必须包含模板要求的表头
- 模板版本必须受支持
- `main_id` 和 `account_id` 必须是规范 ID
- 同一个 `main_id` 下，`main_name` 必须保持一致
- 同一个 `main_id` 下，子账号 `account_id` 不能重复
- 同一个 `main_id` 下，保留主账号行 `account_id=main` 最多只能出现一次
- `account_id=main` 时，`uid` 必填
- 子账号必须同时提供 `api_key` 和 `api_secret`
- `settings.key` 必须命中白名单
- 同一个 `settings.key` 不能重复
- 空白行会被自动忽略

## settings 分页怎么填
`settings` 分页采用固定键值结构：

| 列名 | 是否必填 | 说明 |
| --- | --- | --- |
| `key` | 是 | 只能填写模板内置支持的 key |
| `value` | 否 | 留空表示“保持旧值不变” |
| `notes` | 否 | 备注说明，不参与导入 |

示例：

| key | value | notes |
| --- | --- | --- |
| `telegram.bot_token` | `你的TG Bot Token` | 更新 TG token |
| `telegram.chat_id` | `你的TG Chat ID` | 更新 TG chat id |
| `access_control.admin_password` | `新的管理员密码` | 轮换管理员密码 |

语义说明：
- `accounts` 分页：整页替换
- `settings` 分页：增量覆盖
- `settings` 分页中留空不会清空原有 secret

## 导入后的存储方式
Excel 里仍然是明文填写，但导入成功后，系统不会再把这些明文长期保存在账号 JSON 里。

现在的落盘方式是：

1. 真实 secret 写入加密仓库：
   - [config/secrets.enc.json](../config/secrets.enc.json)
2. 账号配置文件只保留引用：
   - [config/binance_monitor_accounts.json](../config/binance_monitor_accounts.json)
3. 访问控制和 Telegram 也只保留 `*_secret_ref`
4. 解密主密钥来自：
   - `.env` 里的 `MONITOR_MASTER_KEY_FILE`
   - 默认指向本地受限文件，例如 `.local-secrets/monitor-master-key`

所以导入完成后你会看到：
- `transfer_api_key_secret_ref`
- `transfer_api_secret_secret_ref`
- `api_key_secret_ref`
- `api_secret_secret_ref`
- `TG_BOT_TOKEN_SECRET_REF`
- `TG_CHAT_ID_SECRET_REF`
- `guest_password_secret_ref`
- `admin_password_secret_ref`
- `session_secret_secret_ref`

而不会再在账号配置 JSON 里看到明文 key/secret。

## 安全说明
- Excel 文件本身仍然是明文文件
- 导入过程里服务端接收到的也是明文
- 当前这轮改造解决的是“不要把明文长期保存在项目配置文件里”

建议：
- 导入成功后，不要长期保留带真实 key/secret 的 Excel 文件
- 导入成功后，前端也会提示你删除本地 Excel 明文文件
- 如无需要，导入完成后及时删除或转移到更安全的位置
- 由于这些 secret 之前已经在工作区里出现过，建议后续逐步轮换真实 API key / secret

## 常见问题
### 1. 以后还支持 Excel 导入吗？
支持，导入方式不变。

### 2. 为什么导入后在 `binance_monitor_accounts.json` 里看不到明文？
因为现在导入后会把 secret 写进加密仓库，只在 JSON 里保留 `*_secret_ref`。

### 3. 现在密码、TG token 也能通过 Excel 导入吗？
支持，但只支持 `settings` 分页里的白名单 key。

### 4. 如果我要改 API key，还需要重新导 Excel 吗？
不一定。

现在有两种方式：
- 继续用 Excel 导入整批替换
- 用本地 secret 管理工具直接更新指定 ref

### 5. 如果我要新增账号，推荐怎么做？
如果你本来就习惯用 Excel 批量维护，继续用 Excel 最方便。  
如果只是改单个 secret，建议直接走本地 secret 管理工具。
