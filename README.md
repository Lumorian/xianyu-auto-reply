# 🐟 闲鱼自动发货工具

买家拍下并付款后，自动把卡密 / 发货文案发到私信里，并按需点掉「无需物流发货」。
纯 Python + SQLite，自带 Web 管理后台，不需要 MySQL / Redis。

也可以接上 **new-api + new-api-cost** 全自动开卡：按订单实付金额建对应额度的 Key，
再把 Key 换成卡密发给买家 —— 原来手工做的三步全部省掉。

## 功能

- **扫码登录**：网页后台直接出二维码，手机扫一下就行，不用再从 F12 抠 Cookie（也保留手工粘贴）。
- **付款即发货**：识别闲鱼「我已付款，等待你发货」系统卡片，命中规则后立刻发送发货内容。
- **订单轮询兜底**：IM 推送会因为断线/风控漏消息，所以另外定时拉一次「待发货」订单；两条路都靠 `delivered` 去重，不会重复发。
- **快发货**：收到任何 IM 推送就提前叫醒轮询（`XY_ORDER_HINT_POLL_GAP`），订单里已经带了金额就不再查一次详情，买家的会话能复用就不重新建；
  同时待发货订单并行处理（`XY_ORDER_POLL_CONCURRENCY`）。实测「付款 → 收到卡密」通常只要 1~2 秒。
- **按金额自动开卡**（可选）：查订单实付金额 → new-api 建同额度令牌 → 卡密站生成卡密 → 发给买家，失败会回滚令牌。
  令牌名按 `买家用户名#订单号` 生成（如 `测试小鱼#2500000000000000003`），方便在 new-api 后台按买家对账。
  买家名取不到时会依次退：会话资料 → 聊天记录 → 订单详情 → 「买家」，拿到真名后连历史流水一起回填。
- **Key 限模型**：新建的令牌默认只允许 deepseek-v4.1-flash（model_limits_enabled 与 model_limits 一起下发），
  买家拿 Key 也调不了别的模型。**模型白名单直接在后台「设置」页改，保存即生效，不用重启、不用改 .env**；
  `.env` 里的 `XY_NEWAPI_MODEL_LIMITS` 只在后台没保存过时作为默认值。
- **卡密池**：按商品分池 + 通用池，出库即锁定（`UPDATE ... WHERE status = available`），不会重复发同一张。
- **自动确认发货**：可选调用 `mtop.taobao.idle.logistic.consign.dummy` 完成虚拟商品免物流发货。
- **自动回复**：两条独立的路子 —— **商品自动回复**（买家针对某商品说任何话就回一条）和
  **关键词自动回复**（话里命中关键词才回）。两者都支持商品范围、包含/完全一致/正则匹配、
  「同一会话只回一次」与冷却，规则都在后台「自动回复」页维护。
- **收消息有两条路**：WebSocket 推送（快）+ 定时拉会话列表（一定有）。闲鱼的推送经常只带
  「某会话有新消息」的未读标记、**不带正文**，只靠推送会出现「自动回复配了但从不触发」。
- **防重复**：同一订单号只发一次（数据库唯一索引），另有冷却期兜底。
- **多账号**：每个账号一条独立长连接任务，断线按 5/10/30/60/120 秒退避重连；风控/滑块单独长退避重试，只有真正 Cookie 失效才停下并报错。
- **可观测**：消息流水、发货记录、账号连接状态都能在后台看到。
- **DRY-RUN**：`XY_DRY_RUN=1` 时只打日志不真的发消息，适合先跑通再上线。

## 快速开始

```bash
cp .env.example .env      # 填上游配置，见下一节
./start.sh                # 启动（首次自动建 .venv 并装依赖），打开 http://127.0.0.1:8000/
```

停服务就是 `./start.sh stop` —— **不用再 `ps aux` 找 pid**，见下方「启动与停止」。

打开后台后只有三步：

**第一步 · 登录闲鱼** —— 点「生成二维码」，用手机闲鱼 App 右上角扫一扫，确认后自动保存账号并开始监听私信。
扫不出来就退回到「手工粘贴 Cookie」（F12 复制整条，必须含 `unb` 与 `_m_h5_tk`）。

**第二步 · 配置发货规则** —— 选「按订单金额建 Key」，商品 ID 可以直接从「我的在售商品」下拉里选，
保存前下面会实时显示「订单 ¥10 → $10 额度」这种换算预览。

**第三步 · 试一单** —— 用另一个号拍下付款，看「发货记录」里状态是不是 `delivered`。

> 建议第一次把 `XY_DRY_RUN=1` 打开跑一单。DRY-RUN 只打日志，连订单查询都不做，更不会真去建 Key。

**可选 · 配自动回复** —— 后台「自动回复」页有两块：

- **商品自动回复**：买家针对这个商品发来任何一句话就回一条，常见用法是「在的，拍下自动发货」；
- **关键词自动回复**：买家说的话里命中关键词才回，常见用法是把「发货」「多久」「怎么用」
  映射到各自的答复。

自动回复和发货互不干扰：付款卡片不会触发自动回复（那一单该走发货），
发货成功也不会被自动回复打断。

## 启动与停止

整套服务只有一个入口：**`start.sh`**。不写子命令就是启动：

```bash
./start.sh                # 启动（首次自动建 .venv + 装依赖）；等端口真的通了才返回
./start.sh stop           # 停止（SIGTERM，15 秒不退才 SIGKILL）
./start.sh restart        # 重启（改了代码 / 配置后用）
./start.sh status         # 在不在跑、pid、地址、运行多久
./start.sh logs -f        # 跟着看日志（Ctrl-C 只退出查看，服务照跑）
./start.sh fg             # 前台运行，日志打在终端（调试用）
```

**为什么不用 `python main.py &`** —— 前台跑当然也行（`./start.sh fg`），
但后台运行有几个坑，`start.sh` 都替你挡了：

| 场景 | 裸跑 `python main.py &` | `./start.sh` |
| --- | --- | --- |
| 忘了 pid | 得 `ps aux \| grep main.py` | `./start.sh status` 直接告诉你 |
| 杀掉后不确定死没死 | 不知道，可能还在跑 | 等到**端口真的释放**才返回 |
| 重复启动两个实例 | 两个 Worker 抢同一个 Cookie，闲鱼报「被挤爆啦」 | 检测到已在运行就**拒绝启动** |
| 机器重启后遗留 pid | 照着旧 pid `kill` 可能**误杀别人的进程** | 核对命令行确实是本项目的 `main.py`，对不上就不动它 |
| 关掉终端 / SSH 断开 | 进程可能被 SIGHUP 带走 | 独立会话运行，不受影响 |
| 启动失败 | 只在终端滚过一段报错 | 直接打印**日志最后几行**，一眼看到错在哪 |
| 换机器 / 新克隆 | 要先手动建 venv、装依赖 | 首次运行自动建 `.venv` 并装依赖（pip 缺失时自动改用 uv） |

实现上就三件事：`data/app.pid` 里存 pid + 命令行 + 端口（只用于「下次核对是谁」，**不存任何凭据**）；
停止走 SIGTERM → 超时 SIGKILL → 等端口释放；启动前确认没有实例在跑、端口也没被别人占。

> 想让服务开机自启 / 崩溃自动拉起，还是用 systemd 最稳（见下方「用 systemd 常驻」），
> `start.sh` 负责日常启停和排查，两者不冲突。

### 用 systemd 常驻

```ini
[Unit]
Description=xianyu-auto-reply
After=network-online.target

[Service]
Type=simple
User=你的用户名
WorkingDirectory=/path/to/xianyu-auto-reply
ExecStart=/path/to/xianyu-auto-reply/.venv/bin/python main.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

`systemctl enable --now xianyu-auto-reply` 之后用 `systemctl status|restart` 管理，
日志走 `journalctl -u xianyu-auto-reply -f`。注意：这时候**别再用 `./start.sh start`**，
两套东西同时管一个服务会互相打架。

## 接上 new-api + new-api-cost（全自动开卡）

你原来的流程是手工三步：新建对应金额的 Key → 导入卡密站生成卡密 → 把卡密发给买家。
配置好后这三步由程序完成，顺序不变：

```
闲鱼订单实付 ¥10.00
  └─ 换算额度：10 ÷ 1 = $10.00          （XY_CNY_PER_USD=1，1 元 = 1 美元）
      └─ new-api  POST /api/token/      建一个额度 $1.37、名为「买家用户名#订单号」的令牌
          └─ 取回 sk-xxx
              └─ 卡密站 POST /api/admin/cards  用这个 Key 生成卡密（名称同样是「买家用户名#订单号」）
                  └─ 卡密发到闲鱼私信 + 自动点「无需物流发货」
```

### 前提

- **new-api** 站点可访问，账号能在后台「令牌」页新建令牌；
- **卡密站（new-api-cost）** 的 `ADMIN_TOKEN` 已填（或用后台账号密码登录）；
- 卡密站的 `CARDS_SECRET` 已备份 —— 它丢了以后卡密就换不出 Key。

### 配置

```bash
# 上游一：new-api
XY_NEWAPI_BASE_URL=https://api.lumorian.org
XY_NEWAPI_ACCESS_TOKEN=          # 登录响应里的 access_token，或留空用下面两个
XY_NEWAPI_USERNAME=
XY_NEWAPI_PASSWORD=
XY_NEWAPI_GROUP=                 # 令牌落在哪个分组，留空用默认
XY_NEWAPI_TOKEN_PREFIX=           # 可选前缀；不填时令牌名就是「买家用户名#订单号」
XY_NEWAPI_MODEL_LIMITS=deepseek-v4.1-flash   # 仅作默认值；改限制建议用后台「设置」页（留空 = 不限）

# 上游二：卡密站
XY_CARDSHOP_BASE_URL=https://shop.lumorian.org
XY_CARDSHOP_ADMIN_TOKEN=

# 换算（1:1 是有意为之：订单 10 元 → $10 额度）
XY_CNY_PER_USD=1
XY_MAX_ORDER_CNY=200             # 单笔上限，防标错价被刷；0 = 不限
```

> **模型限制只作用于新建的 Key。** 之前已经发出去的旧 Key 不会自动补上限制，
> 需要在 new-api 后台「令牌」页逐个编辑（勾选「模型限制」并填 deepseek-v4.1-flash），
> 或者让买家换新 Key。

> `quota_per_unit`（$1 = 多少 quota）会实时读 `/api/status`，读不到才用兜底值 500000。
> 你这台站是 500000，所以 ¥10 → $10 → `remain_quota = 5000000`。

### new-api 凭据怎么拿（两个值，别漏第二个）

需要用 **access_token** 和 **user_id** 两个值，原因见下。

**① user_id（New-Api-User）**

new-api 0.13.x 起，所有 `/api/*` 后台接口都强制要求请求头带 `New-Api-User: <你的用户id>`，
少这个头直接 401 —— 报错是 `Unauthorized, New-Api-User header not provided`。

拿法（任选一）：

- 登录 <https://api.lumorian.org> → 右上角头像 → 个人设置，页面上能看到用户 ID；
- 或直接看数据库 `SELECT id FROM users WHERE username='你的用户名';`；
- 或 F12 → Network → 任意一个 `/api/user/self` 请求 → 看请求头里的 `New-Api-User`。

**② access_token（系统访问令牌）**

这是**长期有效**的凭据（不像登录态那样过期），正是给脚本用的：

1. 登录 <https://api.lumorian.org>；
2. 进「个人设置」（右上角头像 → 个人设置）；
3. 找到 **「系统访问令牌」/「访问令牌」/ Access Token** 那一栏；
4. 点「生成」/「重置」按钮，复制出来的那串就是。

> 它是 base64 风格的随机串（约 40 字符），**不是** `sk-` 开头的 API Key。
> `sk-` 那些是「令牌」页里给客户端调模型用的，跟这个不是一回事，别搞混。

也可以直接调接口生成（用浏览器登录态）：

```bash
# 在浏览器已登录的前提下，从 F12 → Network 复制 Cookie，然后：
curl -X GET 'https://api.lumorian.org/api/user/token' \
  -H 'New-Api-User: <你的user_id>' \
  -H 'Cookie: <浏览器Cookie>'
# 返回 {"success":true,"data":"<access_token>"}
```

**③ 其实还有个更省事的办法**

不填 access_token，直接填账号密码：

```bash
XY_NEWAPI_USERNAME=你的用户名
XY_NEWAPI_PASSWORD=你的密码
```

程序登录时会自动从响应里拿 user id 和 access_token，**这两个都不用你填**。
代价是把密码写进 `.env`（文件权限设 600），并且每次启动多一次登录请求。

> ⚠️ 光有 access_token 是**没法自动补 user_id** 的 —— 因为查用户信息本身就要 New-Api-User 头。
> 所以要么用账号密码，要么两个值都手工填。

### 卡密站凭据

`https://shop.lumorian.org` 已内置为默认地址。凭据二选一：

- `XY_CARDSHOP_ADMIN_TOKEN`：卡密站 `.env` 里的 `ADMIN_TOKEN`（默认是空的，需要你填一个）；
- 或 `XY_CARDSHOP_USERNAME` / `XY_CARDSHOP_PASSWORD`：后台管理员账号密码。

> 卡密站首次部署后必须先登录 `/admin` 改掉 `admin`/`admin`，
> 否则所有业务接口返回 428「首次登录必须先修改密码」。

### 上线前先自查

管理后台「发货规则」上方会显示一行状态：

- ✅ **上游已接通** —— 可以选「按订单金额建 Key」了
- ⚠️ **上游未配置齐全** —— 上面那些环境变量还缺

再打开 `XY_DRY_RUN=1`（只打日志不真发，**也不会真的去建 Key**），用测试账号拍一单看日志，
确认金额/额度换算对了再关掉。

### 失败时会怎样

| 情况 | 行为 |
| --- | --- |
| 查不到订单金额，且规则填了固定额度 | 用固定额度继续发货 |
| 查不到订单金额，也没填固定额度 | 不发，记 `upstream_failed`（原因：查不到订单金额） |
| 金额超过 `XY_MAX_ORDER_CNY` | 不发，记 `upstream_failed`（原因：订单金额超上限） |
| new-api 建令牌失败 | 不发，记 `upstream_failed` |
| 令牌建好了但卡密生成失败 | **自动删除刚建的令牌**，不发，记 `upstream_failed` |
| 卡密生成好了但闲鱼消息发不出去 | 记 `send_failed`，卡密留在记录里可人工补发 |

```sql
-- 事后查事故：看看哪些订单没发出去
SELECT created_at, order_id, status, detail FROM orders WHERE status != 'delivered';
```

### Key 能调哪些模型（后台直接改）

打开后台左侧「**设置**」页 → **Key 模型限制**：

1. 填允许的模型，逗号 / 空格 / 换行分隔都行（中文逗号也可以），例如 `deepseek-v4.1-flash, deepseek-v4.1`；
2. 点「保存设置」——**立刻对新订单生效，不用重启、也不用改 .env**。

页面会实时显示「保存后新 Key 会变成这样」，也就是实际下发给 new-api 的
`model_limits_enabled` / `model_limits`。

- **留空 = 不限制模型**（买家拿 Key 能调任意模型）。这种状态顶部会挂一条红色警告，规则页的横幅也会标红。
- 保存过的值存在数据库 `app_config` 表里，**优先级高于 `.env` 的 `XY_NEWAPI_MODEL_LIMITS`**；
  `.env` 里那个值只在后台从没保存过时作为默认值（老部署升级上来行为不变）。
- 只影响**之后新建**的 Key。已经发出去的旧 Key 是建令牌时就写死在 new-api 里的，
  要收紧得去 new-api 后台逐个改。
- 这里只存运营参数（模型白名单）。access_token、密码这类凭据仍然只从 `.env` 读，不会写进数据库。

> 注意：这道保护依赖 new-api 自己的 `model_limits`，改了要确认 new-api 版本支持
> （0.13.x 需要同时带 `model_limits_enabled: true`，本工具已经这么发了）。

## 发货规则怎么配

| 字段 | 说明 |
| --- | --- |
| 商品 ID | 目标商品 ID；`*` 为通配规则。有商品专属规则时优先用它 |
| 模式 | `upstream` 按订单金额建 Key（推荐）；`card` 本地卡密池；`text` 固定文案 |
| 发货文案 | 支持变量 `{card}` `{buyer}` `{item_id}` `{order_id}` `{date}` `{amount}` `{quota}` |
| 金额倍率 | 上游模式：额度 = 订单金额 ÷ 汇率 × 倍率 |
| 固定额度 | 上游模式：填了就忽略订单金额，直接建这么多 USD 的 Key |
| 自动确认 | 勾选后，文案发完会调用免物流发货接口把订单标记成已发货 |
| 优先级 | 数字越大越优先（同商品多规则时） |

典型文案：

```
你好 {buyer}，这是你购买的卡密：
{card}
如有问题请直接回复本消息。
```

## 自动回复怎么配

后台「自动回复」页分成两块，规则字段几乎一样：

| 字段 | 说明 |
| --- | --- |
| 规则名 | 自己看得懂就行，日志里会带上 |
| 回复类型 | `item` 商品自动回复 / `keyword` 关键词自动回复 |
| 商品 ID | 限定只对某个商品生效；`*` = 所有商品。专属规则优先于 `*` |
| 关键词 | 仅关键词类型：逗号 / 分号 / 竖线 / 换行分隔，如 `发货, 多久, 怎么用` |
| 匹配方式 | `contains` 包含（推荐）/ `exact` 整句完全一致 / `regex` 正则（高级） |
| 回复内容 | 支持变量 `{buyer}` `{item_id}` `{order_id}` `{keyword}` `{date}` |
| 同一会话只回一次 | 勾上后这个会话不会再被同一条规则打扰；不勾就按冷却来控制频率 |
| 冷却秒数 | 同一买家多少秒内不再回同一条规则；0 = 不限 |
| 优先级 | 数字越大越先匹配（同商品多条规则时） |

匹配顺序：**关键词规则 > 商品规则**；同类里商品专属优先于 `*` 通配。

几个必须知道的行为：

- **付款卡片不会自动回复** —— `[我已付款，等待你发货]` 这类系统卡片只走发货，
  不然买家会先收到一句「在的」再收到卡密；
- **系统消息也不回** —— `[你已发货]`、`[请确认微信收款]`、评价提醒这些
  `custom.type != 1` 的消息只记录、不自动回复，避免对着系统提示说「在的」；
- **自动回复是后台任务** —— 带 `XY_AUTO_REPLY_DELAY` 的延迟不会卡住收帧循环，
  紧接着的付款卡片照样立刻处理；
- **发送失败不记入去重** —— 没发出去就不算「回过」，下次买家再问还会回；
- **处理途中出错会重试** —— 消息是「先标记已处理再干活」的（否则推送和拉取会各回一遍），
  所以一旦下游抛异常，程序会**撤销那个标记**，下一轮重新处理；同一条连续失败 3 次后放弃，
  免得一条坏消息把日志刷满。

接口（和页面对应，方便脚本化）：

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| GET | `/api/reply-rules` | 列出全部自动回复规则 |
| POST | `/api/reply-rules` | 新建 |
| PUT | `/api/reply-rules/{id}` | 修改（会顺手清掉这条规则的回复记录） |
| DELETE | `/api/reply-rules/{id}` | 删除（连同回复记录） |
| POST | `/api/reply-rules/{id}/toggle?enabled=false` | 启用 / 停用（不改其它字段） |
| POST | `/api/reply-rules/{id}/reset` | 只清回复记录，让「会话只回一次」重新生效 |

## 消息是怎么收进来的（重要）

闲鱼的私信正文**不保证走 WebSocket 推送**。线上实测：连接正常、心跳也正常时，
`syncPushPackage` 里可能只有

- 会话引导（`contentType` = 8，一次上百条，纯噪声）；
- `{"1": [{"1": "68250276867@goofish", "2": 1, "3": 1, "4": "<我的unb>@goofish"}]}`
  —— **只有会话 ID 和未读标记，没有正文**。

只靠推送的话，日志里就是「收到无法解析的负载」，看起来像没人说话，
实际上买家的话一直都在。所以收消息有两条路：

| 路径 | 触发 | 特点 |
| --- | --- | --- |
| WebSocket 推送 | 实时 | 快，但正文可能缺失 |
| `/r/Conversation/listNewestPagination` | 每 `XY_MESSAGE_POLL_INTERVAL` 秒 | 每个会话的**最后一条**消息（含正文） |
| `/r/MessageManager/listUserMessages` | 上面发现会话有新动静时 | 该会话最近 `XY_MESSAGE_HISTORY_LIMIT` 条，防止连发被漏 |

两条路都走同一个处理入口，靠服务端 `messageId` 去重（`seen_messages` 表），
所以同一条消息不会既被推送回一次、又被拉取回一次。

> 参考 [xianyu-pilot](https://github.com/dameng2026/xianyu-pilot) 的做法：会话列表只给每个会话的
> 最后一条，买家连发两句（「在吗」「发货吗」）时，关键词正好落在被顶掉的那条上，
> 规则就「看起来没生效」。所以发现会话有新动静后再补拉一次该会话的历史，一起处理。
> 补拉是一趟额外的 WS 往返，因此每轮只给最新的 `XY_MESSAGE_HISTORY_CONVERSATIONS` 个会话补
> （默认 5），其余只用最后一条；设成 0 就完全不补。

**启动基线**：第一次拉到的会话里，比「本次连接建立时刻」更早的都算历史消息，
只记已读不回复 —— 否则一重启就会把昨天的消息全部自动回复一遍。
比连接时刻晚的（哪怕只早到几百毫秒）照常处理，不会漏掉启动瞬间到达的那条。

> 代价是自动回复的延迟约等于 `XY_MESSAGE_POLL_INTERVAL`（默认 20 秒）。
> 想更即时可以调到 10，但别低于 5：这是每次都要打一次 WebSocket RPC 的路径。

### 注册流程也有坑（已修）

两个细节会导致「连上了、也收到帧，但永远收不到正文」：

1. **心跳必须在 `/reg` 之前启动**。注册期间长时间没有 `/!`，服务端会把连接判成
   不活跃，之后只推框架帧（`/s/sync`、`/s/vulcan`）不再推消息内容。
   现在 `connect()` 里先发首跳、再注册。
2. **`/reg` 与 ackDiff 之间要留一秒**（`XY_REGISTER_GAP`）。紧凑连发会让注册在
   服务端不生效，症状和上面完全一样。
3. **ackDiff 要周期性重发**（`XY_SYNC_POLL_INTERVAL`，默认 30 秒）。增量推送是服务端
   对 ackDiff 的响应；只发一次的话，除了连接瞬间那批快照包（`/s/sync`、`/s/vulcan`）
   就再没有新消息下来了 —— 这也是「连上了但收不到新消息」的成因之一。

### 排查「为什么没触发」

日志按这个顺序看：

| 日志 | 含义 |
| --- | --- |
| `已建立会话基线（N 个会话）` | 拉取通道通了，N 是当前会话数 |
| `会话 X（买家名）拉到 N 条新消息` | 从拉取通道收到新消息了 |
| `拉取到 X 的消息：…` | 同上，逐条明细（INFO 以下级别） |
| `会话 X 有新消息，但这一帧只有未读标记、没有正文` | 推送通道只给了标记，正文靠轮询补 |
| `这一帧里有 N 条负载解不开（lwp=…）` | 推送格式变了，把整行发我 |
| `已自动回复 X：命中商品自动回复「…」` | 真的发出去了 |
| `不自动回复：命中的规则都在冷却或已回过` | 规则命中了，但被「会话只回一次」或冷却挡住 |

### 额度按站点货币换算（重要）

new-api 的 `quota` 是按**站点展示货币**计价的，由后台 `quota_display_type` 决定：

| 站点货币 | 换算 | 1 元订单得到 |
| --- | --- | --- |
| `USD` | `quota = 金额 × quota_per_unit` | 500000 quota = $1 |
| `CNY`（汇率 7.3） | `quota = 金额 ÷ 7.3 × quota_per_unit` | ≈68493 quota = **¥1** |
| `CUSTOM` | 按 `custom_currency_exchange_rate` | 同 CNY 逻辑 |

程序启动时会读 `/api/status` 拿到 `quota_display_type` / `usd_exchange_rate` 自动换算，
**不需要你手动补偿**。后台「发货规则」页的额度预览也会跟着显示 $ 或 ¥，避免误读。

> 历史坑：早期版本无条件把 `quota_per_unit` 当成「$1 = 500000」。在 CNY 站点上
> 这会让 1 元订单发出 ¥7.30 的额度（多给 7.3 倍）。已修复，且有回归测试锁住。

## 配置项

全部走环境变量（启动时自动读取工作目录下的 `.env`，模板见 `.env.example`）：

| 变量 | 默认 | 作用 |
| --- | --- | --- |
| `XY_DB_PATH` | `./data/app.db` | SQLite 文件位置 |
| `XY_HOST` / `XY_PORT` | `127.0.0.1` / `8000` | 后台监听地址 |
| `XY_ADMIN_TOKEN` | 空 | 管理口令。设置后所有 `/api` 需带 `X-Admin-Token`（或 `Authorization: Bearer`）；页面可用 `?token=xxx` |
| `XY_ADMIN_ALLOW_REMOTE` | `0` | **没配口令时的兜底**：默认只允许本机访问管理接口；改成 `1` 才会对外网放行。**打开前必须先配 `XY_ADMIN_TOKEN`** —— 这个开关只是「放宽来源」，它不校验口令；两者都没配等于把后台交给任何人（启动时会打警告）。另外 uvicorn 已固定 `proxy_headers=False`，不会相信客户端自带的 `X-Forwarded-For` |
| `XY_DELIVERY_COOLDOWN` | `600` | 同一订单的重复发货冷却秒数 |
| `XY_SEND_RETRY` | `2` | 发送失败重试次数 |
| `XY_DRY_RUN` | `0` | 只打日志不真发（也不会真的建 Key） |
| `XY_LOG_LEVEL` | `INFO` | 日志级别 |
| `XY_LOG_FILE` | `./logs/server.log` | 日志文件（自动轮转）；留空只输出到控制台 |
| `XY_LOG_MAX_BYTES` | `10485760` | 单个日志上限（10 MB） |
| `XY_LOG_BACKUP_COUNT` | `5` | 保留几份历史 |
| `XY_IM_TOKEN_TTL` | `14400` | IM accessToken 进程内缓存秒数，0 = 不缓存 |
| `XY_MH5TK_REFRESH_MIN` / `XY_MH5TK_REFRESH_MAX` | `5400` / `9000` | `_m_h5_tk` 主动刷新间隔（秒，随机） |
| `XY_SESSION_KEEPALIVE_INTERVAL` | `1800` | Cookie 保活间隔（秒），0 = 关闭 |
| `XY_MESSAGE_POLL_INTERVAL` | `20` | 拉会话列表的间隔（秒），0 = 关闭。**自动回复的主要来源**，见下节 |
| `XY_MESSAGE_POLL_LIMIT` | `20` | 每次拉多少个会话（最多 50） |
| `XY_MESSAGE_HISTORY_LIMIT` | `20` | 发现新消息时往回补拉多少条历史（最多 50） |
| `XY_MESSAGE_HISTORY_CONVERSATIONS` | `5` | 每轮最多给几个会话补拉历史；0 = 只看最后一条 |
| `XY_MESSAGE_RETENTION_DAYS` | `30` | 消息流水保留天数；0 = 永久保留。**只清 `messages`** —— `seen_messages` / `reply_hits` 清了会让「同一会话只回一次」和去重失效，宁可留着 |
| `XY_SYNC_POLL_INTERVAL` | `30` | ackDiff 同步位点重发间隔（秒），0 = 只发一次（不推荐） |
| `XY_HEARTBEAT_INTERVAL` | `10` | WebSocket 心跳间隔（秒）。首跳立即发，必须在注册之前 |
| `XY_REGISTER_GAP` | `1` | `/reg` 与 ackDiff 之间的间隔（秒），太小会导致注册不生效 |
| `XY_AUTO_REPLY` | `1` | 自动回复总开关；0 = 全关（只发货，不搭理） |
| `XY_AUTO_REPLY_DELAY` | `1` | 自动回复前等几秒，避免「秒回」像机器人；0 = 立刻回 |
| `XY_CNY_PER_USD` | `1` | 多少元 = 1 美元额度 |
| `XY_ORDER_POLL_INTERVAL` | `30` | 订单轮询间隔（秒），0 = 关闭。这是「完全没有推送」时的兜底节奏 |
| `XY_ORDER_HINT_POLL_GAP` | `5` | 收到推送后最少隔几秒才允许再补查一次订单；0 = 关闭加速。查不到东西会自动放慢到最多 30 秒 |
| `XY_ORDER_POLL_CONCURRENCY` | `3` | 一轮轮询里同时处理几张待发货订单，1 = 串行 |
| `XY_ORDER_CACHE_TTL` | `30` | 同一订单详情缓存秒数（补商品 ID 与取金额共用一次查询），0 = 不缓存 |
| `XY_ORDER_AMOUNT_UNIT` | `auto` | 订单金额单位：`auto` / `yuan` / `fen` |
| `XY_MAX_ORDER_CNY` | `0` | 单笔订单金额上限（元），0 = 不限 |
| `XY_NEWAPI_BASE_URL` | 空 | new-api 站点地址 |
| `XY_NEWAPI_ACCESS_TOKEN` | 空 | new-api 后台 access_token |
| `XY_NEWAPI_USERNAME` / `XY_NEWAPI_PASSWORD` | 空 | 没给 token 时用它俩自动登录 |
| `XY_NEWAPI_GROUP` | 空 | 令牌分组 |
| `XY_NEWAPI_TOKEN_PREFIX` | 空 | 可选前缀。令牌名 = `[前缀]买家用户名#订单号`，整体控制在 50 字节内；超长时先截用户名、订单号保持完整 |
| `XY_NEWAPI_MODEL_LIMITS` | `deepseek-v4.1-flash` | 新建 Key 只允许使用的模型（逗号分隔）；留空 = 不限制，慎用。**仅在后台「设置」页没保存过时生效**，优先级低于后台 |
| `XY_NEWAPI_QUOTA_PER_UNIT` | 空 | $1 对应多少 quota，留空自动读 /api/status |
| `XY_CARDSHOP_BASE_URL` | `https://shop.lumorian.org` | 卡密站地址 |
| `XY_CARDSHOP_ADMIN_TOKEN` | 空 | 卡密站管理令牌 |
| `XY_CARDSHOP_GROUPS` | `5` | 卡密分组数 |

> 卡密名称固定为 `买家用户名#订单号`（与令牌名一致，不带价格和平台前缀）。
> 原先的 `XY_CARDSHOP_LABEL` 名称前缀已下线，配了也不会生效。

## 代码结构

```
main.py            启动入口（uvicorn）
app/config.py      环境变量配置
app/sign.py        mtop 签名 / 设备指纹 / Cookie 解析
app/codec.py       msgpack+base64 解包、卡片消息里提 itemId / orderId
app/client.py      mtop HTTP + WebSocket 长连接、发消息、确认发货
app/engine.py      发货规则匹配 → 卡密出库 / 上游建 Key → 文案渲染（纯逻辑，好测）
app/autoreply.py   自动回复匹配（商品 / 关键词）→ 文案渲染（纯逻辑，好测）
app/money.py       订单金额解析（元/分自动识别）与额度换算
app/upstream.py    new-api 建令牌 + 卡密站生成卡密的客户端
app/qrlogin.py     闲鱼扫码登录（passport 二维码流程）
app/db.py          SQLite 存储（账号 / 规则 / 卡密 / 订单 / 消息）
app/runner.py      账号级 Worker：连接、收帧、回执、决策、发货、记录
app/web.py         FastAPI 管理接口 + 内嵌 HTML 后台
app/daemon.py      进程控制：pid 文件、单实例锁、SIGTERM/SIGKILL、端口等待
start.sh           唯一入口：自动建 venv / 装依赖，再转给 ctl.py
ctl.py             启停子命令实现（一般不用直接调）
tests/             369 个单元 / 集成测试，全部离线运行
```

## 测试

```bash
.venv/bin/python -m pytest -q
```

测试不触网：HTTP / WebSocket 全部用假客户端替代，消息则是真实抓包结构。

## 已知边界

- **发货必须可核实**：只有「系统交易卡片形态（`contentType=26` / `dxCard` 键）
  **且**订单号来自服务端结构化字段（卡片 JSON、`reminderUrl` 参数）」才免核实直接信任；
  其余一律去闲鱼查一次订单，确认「状态是待发货 + 买家对得上」才发。
  所以买家自己打「我已付款，等待你发货」不会触发发货，只会在日志和 `orders` 里
  留一条 `blocked` 记录。没有订单号的消息一律不发（没法去重也没法核归属）。
  附注：判断「是不是卡片」只看**键名**，绝不扫字符串值 —— 正文就存在 payload 里，
  买家在聊天框里打一句带 `order_detail?id=…` 的话，以前会被误判成交易卡片并跳过核实。
- 只发**文本**消息（卡密、链接、账号密码都够用）；图片发货需要额外的上传接口，暂未实现。
- **自动回复的实时性受轮询间隔限制**：正文主要靠拉会话列表拿到，默认 20 秒一轮。
  推送里如果能解析出正文，当条就会立即回复；拿不到就等下一轮。
- **每条自动回复只回一条**：同一会话里买家连发 3 句，会分别判断，命中的规则若勾了
  「同一会话只回一次」则只回第一条；想每句都回就取消勾选并设好冷却。
- **买家昵称**：付款卡片本身不带昵称（它的 `reminderTitle` 是「等待你发货」这类标题），
  新买家又可能一句话没说过。所以名字按「会话资料 → 聊天记录 → 订单详情」的顺序找，
  全都没有才退回「买家」；一旦拿到真名，同一个人历史上记成「买家」的订单/消息会一并回填。
  只有付款触发语在名字还没落定时才会为此多查一次订单，其它情况复用已在缓存里的订单详情。
- 上游模式依赖闲鱼「卖家订单列表」接口拿金额；接口字段变了会退回 `upstream_failed` 并在 `detail` 里写明原因，不会瞎发。
- 订单轮询路径需要先 `create_chat` 建会话才能发消息。回包必须由收帧主循环分发（`dispatch`），
  **不能在业务代码里再开一个 `recv`** —— websockets 会报 `cannot call recv while another coroutine is already running recv`。
- 闲鱼的消息推送格式有三代：**嵌套数字键**（`"1"` → `"6"` → `"3"`，真正的聊天消息）、
  **flat**（`chatType` + `operation`，会话引导/部分通知）、**老提醒格式**（`"1"` → `"10"`）。三种都支持。
  遇到第四种时日志会打出「收到无法解析为买家消息的负载（形状 …）」并附上完整内容，把那段发我就行。
- 消息协议是逆向的网页端行为，闲鱼改版时可能需要跟进（`app/codec.py` 的解析、`app/client.py` 的帧格式最可能受影响）。
- Cookie 有效期通常几天到几周，过期后账号状态会变成 `cookie_expired`，重新粘贴即可。
- **掉登录/风控**：如果日志里出现 `FAIL_SYS_USER_VALIDATE`、`RGV587_ERROR`、`被挤爆啦`，
  那是闲鱼的滑块/风控，不是 Cookie 失效。程序会标成 `risk_control` 并长退避重试，
  不会再把 worker 停掉；此时重新扫码也会自动重启该账号的 worker。
  真正过期只会是 `FAIL_SYS_SESSION_EXPIRED` / `请登录`，状态才是 `cookie_expired`。
- 请只用于自有商品的虚拟发货，遵守平台规则。
