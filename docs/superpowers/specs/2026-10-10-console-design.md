# Rustorage 控制面板（Console）设计

> 状态：**设计已定，未实现。**
> 本文是 `docs/DESIGN.md` 的增量：它不修改既有契约，只在既有约束下加一个只读的运维/浏览界面。
> 与 `docs/DESIGN.md` 冲突时以本文为准（本文更新）；与代码冲突时以代码为准。

---

## 1. 目标与非目标

### 1.1 目标

给 Rustorage 加一个类似 MinIO Console 的**只读**控制面板：浏览器里看桶、浏览对象、
下载对象、看服务健康与指标。三条硬约束：

1. **仍然是单二进制、无外部依赖**——面板静态资源编译进 `rstore-server`，不引入 Node
   构建链，不引入 `node_modules`，不新增 CI 步骤；
2. **不改 S3 数据面契约**——面板不新增服务端 API，所有数据来自既有 S3 接口与既有的
   `/health` `/ready` `/metrics` 三条运维端点；
3. **不假装支持没有的能力**——服务端不存的字段（对象 Content-Type、桶创建时间……）
   界面上不出现，而不是显示空白或假值。

### 1.2 非目标（这一版明确不做）

| 不做 | 理由 |
|---|---|
| 上传 / 删除 / 建桶 / 删桶 | 只读版先把「看得见」做扎实；写操作涉及 8 MiB PUT 上限、批量删除确认等一串独立决策 |
| Multipart | 服务端刻意 501（`docs/DESIGN.md` §1.2），面板不绕开 |
| 版本化 / 对象锁 / IAM / 用户与策略管理 | 服务端根本没有这些子系统，界面无从生成 |
| 多节点 / 集群拓扑图 | 服务端是单节点 |
| 服务端侧会话与代签代理 | 见 §4 的认证决策 |
| MinIO Admin API 兼容 | `docs/DESIGN.md` §1.2 的既有非目标，本文不推翻 |

---

## 2. 总体架构

```
浏览器
  │
  ├─ GET /_console/**  ──►  service_fn 拦截（早于 s3s，与 /health /ready /metrics 同一层）
  │                          └─► 内嵌静态资源表（include_str! 编译进二进制）
  │
  └─ fetch 同源 :9000/<bucket>/<key>  ──►  s3s（Auth / Readiness / S3 数据面）
     └─ SigV4 签名头由前端 WebCrypto 现算
```

**同源是本设计的支点**：面板与 S3 都在 `127.0.0.1:9000`，因此

- **不需要 CORS**——服务端一行 CORS 头都不用加，也不用改 s3s 的中间件栈；
- 不需要为了跨源去放宽监听地址（服务端监听地址写死 `127.0.0.1`，见 `startup.rs`）。

### 2.1 与既有路由决定的关系

`startup.rs` 的 `service_fn` 里注释写明了：`/health` `/ready` `/metrics` 用**精确匹配**而
不是前缀，理由是「`GET /metrics/` 必须落到 s3s 去（一个名叫 metrics 的桶）」。

**这条决定本文照旧遵守，不推翻。** console 是同一个 `match` 里新增的一支，且它的路径
选择（§3）就是为了让「路径与桶名不可能相撞」成为**结构性事实**，而不是靠约定。

### 2.2 与 readiness 的关系

console 静态资源**不受 readiness 门控制**：进程活着就能拿到页面。理由是概览页本身要
展示 `Booting / StorageReady / FullReady` 三阶段——如果资源也要等 `StorageReady` 才给，
启动失败时用户看到的将是一个永远转圈的空白页，而不是「卡在 Booting」这个信息。

S3 数据面的请求照旧受 ready 门控制（`503 + Retry-After: 5` 不变）；面板对这类响应要
显示成人话（「服务启动中，重试中」），而不是抛一个裸的 503。

---

## 3. 挂载点：`/_console`（决策 B2）

### 3.1 问题

若把面板挂在 `/console/`，就会**遮蔽**名为 `console` 的桶——这正是 `startup.rs` 那条
「精确匹配」注释要避免的事。两个候选：

| | B1：保留桶名 `console` | **B2：`/_console`（采纳）** |
|---|---|---|
| 路径 | 精确 `/console` + 前缀 `/console/` | 精确 `/_console` + 前缀 `/_console/` |
| 配套 | 无——只能靠「`console` 是个禁词」的文档约定 | 无——**上游的桶名规则已经挡在这里**（§3.2） |
| 代价 | 一个正常名字被占用，而它本可以是一个合法桶 | 无 |
| 收益 | 无 | 冲突被结构性消除，且不需要我们做任何事 |

**采纳 B2。** 关键差别在于：B2 的路径**不可能是任何合法桶名**；B1 则要依赖「大家记得
别建名叫 console 的桶」，而面板一旦挂上去，那个桶就再也没法通过 HTTP 访问了。

### 3.2 为什么 `_` 开头是安全的（上游给的保证）

> **本节经过一次修正。** 初稿写的是「桶名当前完全不校验，需要新增
> `validate_bucket_name` 拒绝首字符为 `_` 的桶名」，并按此实现过一版。那个前提是
> **错的**，规则已回退（见本节的推断错在哪）。按本项目「不允许文档漂移」的规矩，
> 这里留着修正的痕迹，而不是静默改成对的。

桶名**不是**「完全不校验」。s3s 的 `S3ServiceBuilder` 在未调用 `set_validation` 时
默认装上 `AwsNameValidation`（`s3s-0.17.0/src/service.rs`），它在**路径解析阶段**
执行 `check_bucket_name`（`s3s-0.17.0/src/path.rs:111`），逐条比对 AWS 的
[bucket naming rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)：

- 长度 3..=63；
- 字符集只允许小写字母、数字、`.`、`-`；
- 首尾必须是小写字母或数字；
- 不允许出现连续的 `..`；
- 不允许是 IP 地址形态；
- 不允许以 `xn--` 开头。
  （AWS 另外还禁了 `.-` / `-.` 相邻与 `-s3alias` 后缀，上游这份没覆盖——与本节无关。）

**下划线出现在任何位置都非法**，所以 `_console` 从来不是合法桶名，`/_console/*`
撞不上任何真实桶。B2 要的那个「结构性事实」本来就成立——我们只是选了一个恰好落在
上游禁区里的前缀，不需要自己写一行校验。

**原先的推断错在哪：** 看到 `impl_s3.rs` 的 `create_bucket` 里没有校验调用，就断定
桶名没被校验。漏掉的一环是——**校验跑在比 `create_bucket` 更早的路径解析阶段，代码
在上游**。实测很容易戳破：`PUT /_private`（未接线时）就返回 400，`create_bucket`
根本没被执行。教训是「哪一层先跑」比「我们的代码里有没有这一句」靠谱。

**因此未做的事：** 没有新增任何桶名校验。上游这份已经覆盖了 AWS 规则的绝大多数，
缺的只是 `.-` / `-.` 相邻与 `-s3alias` 后缀这类边角——与本设计无关，留给独立的一次
任务，别顺手补在半路上。

### 3.3 路由规则

常量集中定义，落在 `crates/common/src/consts.rs`（与 `RESERVED_PREFIX` 同一处）：

```rust
pub const CONSOLE_PREFIX: &str = "/_console";
```

匹配规则：

- `/_console` 与 `/_console/` → `index.html`
- `/_console/<已知资源名>` → 该资源（`style.css` / `app.js` / `sigv4.js` / `s3api.js` / `metrics.js`）
- `/_console/<未知名>` → `404`（**不降级到 index.html**：不用 SPA fallback，见 §4.3）
- 其他任何路径 → 原样交给 s3s（**包括 `/_consoleX`、`/_console.html` 这类同前缀但不同段
  的路径**——与 `/metrics/` 的既有处理同构）

大小写敏感、不做重定向，`/console`（旧候选）不是保留路径。

---

## 4. 前端形态

### 4.1 技术选型：零构建链

- 原生 ES module + 原生 DOM API，**不引框架、不引 npm**。
- 文件：`index.html`、`style.css`、`app.js`、`ui.js`、`sigv4.js`、`s3api.js`、
  `metrics.js`、`favicon.svg`。
- 资源总预算控制在 100 KB 以内（无压缩、无压缩包格式——`include_str!` 编的是源码文本）；
  当前约 72 KB。
- **`ui.js` 是 DOM 原语层**（`h` / 图标 / toast / 抽屉 / 剪贴板 / 骨架 / 空态），
  `app.js` 只负责「哪个视图、取什么数据、渲染成什么」。**`ui.js` 里不许出现 `id` 查找**：
  §8.1 那条「app.js 取的 id 必须存在于 index.html」的守卫**只扫 app.js**，
  新文件里的 id 查找会落在护栏之外，而「取了不存在的 id」正是白屏的头号原因。
  它需要的挂载点（toast 区、抽屉）一律自建并 `append` 到 `body`。
- **样式一律走 `style.css` 的类名，不写内联 `style` 属性**：CSP 是 `default-src 'self'`，
  内联样式会被浏览器**静默**拒掉（只在 DevTools 里报错）。`ui.js` 的 `h()` 遇到
  `style` 属性直接抛异常，`tests/console.sh` 另有一条 `grep` 护栏盯着这件事。

**不引 `rust-embed` / `include_dir` 的理由**：workspace 依赖刻意极简、`unsafe_code` 与
clippy 门禁严格，而我们需要的只是「路径 → (mime, 内容)」一张静态表。手写表的 `route(path)`
是个**纯函数**，可以脱离 HTTP 直接测——这比引入一个宏 crate 再测它生成的东西更干净。

### 4.2 服务端资源表

```rust
struct Asset { path: &'static str, mime: &'static str, body: &'static str }
const ASSETS: &[Asset] = &[ /* include_str! 各项 */ ];

/// 纯函数：路径 → 响应（或 None 表示不是 console 路径，交给 s3s）。
pub fn route(path: &str) -> Option<http::Response<bytes::Bytes>>
```

`route` 只做字符串匹配与响应构造，**不碰文件系统、不碰 IO**，与 `metrics.rs`
「只提供构造响应的纯函数，不建服务器、不做路由」的既有分工同形。

响应头：`Content-Type` 取表里的 mime；`Cache-Control: no-store`（本地面板，别让浏览器
缓存一个 stale 的 `app.js` 掩盖更新）；不加 `Content-Security-Policy` 之外的花哨头，
但**要加一条最小 CSP**：`default-src 'self'; connect-src 'self'; img-src 'self' data:`
——面板不引任何外部资源，这条 CSP 是对「未来有人往 HTML 里塞 CDN `<script>`」的护栏。

### 4.3 前端路由：hash

用 `#/b/<bucket>/<prefix>` 形式的 hash 路由。好处是服务端**不需要 SPA fallback 的
catch-all 规则**（即不需要「未知路径降级到 index.html」）——那正好与 §3.3 的路由规则
相容，也让 s3s 的路径空间不被进一步侵蚀。

### 4.4 页面清单

（本表在 2026-10-10 的界面精修后与实现对齐：原先把「概览」与「就绪态」写成同一页，
实现上是分开的，以实现为准。）

| 页面 | 数据来源 | 展示内容 |
|---|---|---|
| 登录 | `GET /` | access key / secret key 表单；校验方式＝打一次 `ListBuckets`，签名错自然拿到 403 |
| 概览（桶列表） | `GET /` | 桶名列表（**无创建时间**，见 §5.2）+ 搜索过滤 + 复制桶名 |
| 对象浏览 | `GET /<b>?list-type=2&delimiter=/&prefix=` | 目录式前缀导航、搜索过滤、**连续翻页**、行内复制 key / 下载 |
| 对象详情 | `HEAD /<b>/<k>` | 右侧抽屉：key、size、ETag、Last-Modified（**不显示** Accept-Ranges——服务端不返回） |
| 下载 | `GET /<b>/<k>` | 见 §4.6 |
| 服务状态 | `/ready`、`/metrics` | 就绪探针徽章 + 指标条数卡片 + 指标表 |

三条与交互有关的约定：

- **搜索只过滤「已加载」的行**，不重新发请求。服务端 `LIST` 是全盘遍历，每敲一个字
  打一次请求太贵。
- **翻页是追加**，不是替换：`IsTruncated` 为真**且**拿到 `NextContinuationToken` 时
  才保留「加载更多」按钮。只看前者的话，令牌缺失时再点一次会把第一页的行**重复追加**一遍。
- **行内动作按钮一律 `stopPropagation`**，否则点击会同时触发整行的「打开详情」。

### 4.5 凭据存放

**只存内存**：`s3api.js` 里的一个模块级变量，页面刷新即丢，要重新登录。

不用 `sessionStorage`，更不用 `localStorage`：面板是本地运维工具，没有「记住我」的
价值，而落盘的凭据只会在磁盘上多留一份明文。

（M-console 期间登录页的文案写的是「存在本标签页的 `sessionStorage` 里」，而实现
从来不是——**这是文案错，不是实现漏**。精修时按实现改了文案，而不是反过来加上持久化：
加持久化是**安全相关的行为变更**，不该顺手做掉。）

### 4.6 下载路径（含待验证项）

**默认实现走「前端 `fetch` 取字节 → `Blob` → `URL.createObjectURL` 下载」**，不依赖任何
未验证的服务端行为。

预签名 URL（把签字放进 query 的 `?X-Amz-*`，直接把链接交给浏览器原生下载、前端不必碰
字节流）是**优化项**，只在 §9 的验证通过后才采用；验证不通过就维持默认实现，不阻塞 M2。

两条路都要注意：**服务端不返回 `Content-Type` 与 `Content-Disposition`**
（`HeadObjectOutput` 里根本没有这两个字段），所以浏览器只会按 URL 末段猜文件名与类型；
界面上的文件名由前端从 key 末段自行推断，而不是读服务端头。

---

## 5. 认证：浏览器端 SigV4

### 5.1 机制

- 登录页收 access / secret key → 存 `sessionStorage`。
- `sigv4.js` 用 `crypto.subtle` 的 SHA-256 / HMAC-SHA256 逐请求现算签名头
  （`Authorization`、`x-amz-date`、`x-amz-content-sha256`），约 120 行。
- 凭据不在服务端落地、不经服务端转发：面板拿的就是 S3 凭据本身，
  权限面与 `aws-cli` / `mc` 完全一致，不多不少。

### 5.2 为什么不是服务端代签

服务端会话 + 代签代理能带来「密钥不出服务端」，代价是要在 Rust 侧新写会话管理、
Cookie 安全、CSRF 防护、转发层——**一个独立子系统**，而收益只是把凭据的驻留位置从
浏览器内存挪到服务端内存。对本项目的体量不成比例。

### 5.3 硬约束：secure context

`crypto.subtle` 只在**可信来源**可用。`127.0.0.1` 与 `localhost` 都属于可信来源，而
服务端监听地址在 `startup.rs` 里写死为 `127.0.0.1`，所以今天成立。

**这条耦合必须写进文档**：一旦将来允许 `--bind 0.0.0.0` 或加 TLS，通过局域网 IP
（`http://192.168.x.x:9000`）访问的面板会**直接拿不到 `crypto.subtle`**，届时只有两条
出路——上 TLS，或改用服务端代签（§5.2）。不要以为这只是「将来再说」。

### 5.4 界面要对限制诚实

面板**不展示**服务端根本不存的字段：

| 字段 | 为什么不显示 |
|---|---|
| 对象 `Content-Type` | `HeadObjectOutput` 里没有这个字段 |
| 对象自定义元数据（`x-amz-meta-*`） | 服务端不存储（`docs/MVP.md` 已记为已知限制） |
| 桶创建时间 | `bucket.meta` 的内容就是 `{}`，没存 |
| 版本 / 版本号 | 无版本化 |
| 存储类 | 无存储类概念 |

同理，`ListBuckets` 的 `creation_date` 在服务端是 `None` 且刻意不补假值，面板也不补。

---

## 6. 服务端改动清单（文件级）

| 文件 | 改动 | 备注 |
|---|---|---|
| `crates/common/src/consts.rs` | 新增 `CONSOLE_PREFIX` | 与 `RESERVED_PREFIX` 同处集中定义 |
| `crates/server/src/console.rs`（新） | `Asset` 表 + `route()` 纯函数 | 不碰 IO，可单测 |
| `crates/server/console/`（新） | 前端源文件（`include_str!` 目标） | 无构建步骤 |
| `crates/server/src/config.rs` | 新增 `--console` 开关 | **默认关闭** |
| `crates/server/src/startup.rs` | `service_fn` 的 `match` 新增一支 | 仅在开关开启时生效 |

**`crates/s3` 一行都没改**——桶名的把关在上游的路径解析里（§3.2），本设计没有理由
去碰 S3 协议层。初稿曾在这里列过 `validate.rs` / `impl_s3.rs` 两行，已随该前提一并撤回。

`--console` 默认关闭的理由：与 `--metrics` 一致（同样是 opt-in），且不会让既有的
`tests/acceptance.sh` 与 `tests/compat/` 的行为发生任何变化。

---

## 7. 错误处理

| 情形 | 面板行为 |
|---|---|
| 签名错 / 凭据错 | 403 → 回到登录页，提示「凭据错误」 |
| 桶/对象不存在 | 404 → 显示「已被删除或不存在」，并提供刷新 |
| 服务端未就绪 | 503 → 显示「服务启动中，重试中」并自动退避重试 |
| 对象 key 非法（`a//b`、`dir/` 等） | 400 → 如实显示服务端的错误码与 `Message`，不美化 |
| `LIST` 慢（全盘遍历） | 显式上限 + 进度提示；不用无限滚动 |
| 500 | 显示服务端返回的 `Code` + `Message`，并提供 request 摘要便于运维排查 |

面板**不吞错**：服务端返回的 `Code` / `Message` 原样可见。这是本项目一贯的
「不静默」原则在界面上的延续。

---

## 8. 测试策略

### 8.1 Rust 侧（自动化）

复用既有的 `tower::ServiceExt::oneshot`（`crates/s3` 与 `crates/api` 已在用）：

- `--console` 开 → `GET /_console/` 200 且 `Content-Type: text/html`;
- `--console` 关 → `GET /_console/` 落到 s3s。**期望值是 400 `InvalidBucketName`，不是
  403**（初稿猜的是 403，实测推翻了）：s3s 在路径解析阶段就把 `_` 开头的段判为非法桶名，
  请求走不到鉴权那一步。这条断言的价值恰在于此——它证明关闭开关时面板**一个字节都没拦**；
- `GET /_console/nope` → 404（不降级到 index.html）；
- **回归**：`GET /metrics/`（带尾斜杠）行为不变，仍落到 s3s。它和上面那条正好构成对照：
  `metrics` 是**合法**桶名，所以能一路走到鉴权（未签名 → 403），而 `_console` 走不到；
- 桶名（放在 HTTP 层，钉的是**上游的行为**而不是我们的规则）：
  `_x` / `put` 到 `/_private` → 400 `InvalidBucketName`；`console`（无下划线）→ 仍能正常
  建桶。后者正是 B2 相对 B1 的收益——**没有**占用任何真实桶名，要有测试钉住。

### 8.2 前端（验收脚本）

前端无构建链 → 不引前端测试框架。改用一个 `tests/console.sh`，与 `tests/` 下既有脚本
同风格：起服务 → 断言静态资源可达（含 mime）→ 断言每条响应都带 CSP → 断言未登记资源
404 → **断言每个资源的正文含一个只属于它的标识符**（状态码 200 只证明"有个文件被送出来了"，
登记表写错、内容串了文件时它一样绿）→ 有 `node` 时顺带 `node --check` 一遍模块语法。

两条护栏值得单列，因为它们把**静默失败**变成脚本里的响亮失败：

- **不得有内联样式**：`index.html` 里不许有 `style=` 或 `<style>`，`app.js` / `ui.js`
  里不许写 `node.style.*`。CSP 是 `default-src 'self'`，违规只会让样式"没生效"，
  报错只在 DevTools 里；
- **id 守卫**：`console.rs` 的 `app_looks_up_ids_that_index_actually_provides` 从
  `app.js` 抠出所有 id 查找、逐个在 `index.html` 里找。它按**单引号**切分——`app.js`
  里换成双引号会让它静默退化成一条永远绿的假测试。**注释里也不能出现那三个字符的连写**：
  守卫不做语法分析，它只切字符串。

### 8.3 手动验收

浏览器里走一遍：登录（应自动聚焦第一个输入框）→ 概览搜索过滤 → 进桶 → 层层进入前缀
→ 连续翻页 → 点行开抽屉 → `Esc` 关且焦点还给触发行 → 复制 key → 看 toast → 服务状态页
→ 缩到 700px 看侧栏收窄且不隐藏 → 下载。**DevTools Console 必须零 CSP 违规、零 403**。

登录后还应跑一次 `aws-cli` 的 PUT 制造流量，让指标页有个非零计数器。

（2026-10-10 精修时这套路径是用 CDP 驱动无头 Chrome 实跑过的：42 条断言全绿，
控制台零报错、零 CSP 违规、零 403。`tests/console.sh` 覆盖不到交互——它起不了浏览器；
驱动脚本是一次性的，不进仓库。）

---

## 9. 风险与待实跑验证的假设

| 风险 | 说明与对策 |
|---|---|
| **浏览器 SigV4 与服务端校验不一致** | 最可能翻车的一点。规范化细节（URI 编码、`Host`、`x-amz-content-sha256`）任何一处不符都是 403。对策：M2 的第一件事就是打通它，用 §8.2 的脚本钉死 |
| s3s 是否接受预签名 URL | 待验证；不影响主流程，只影响下载实现（§4.6 有退路） |
| `LIST` 全盘遍历 | 大桶下页面慢。对策：单页硬上限 + 显式提示，不做无限滚动 |
| `crypto.subtle` 依赖可信来源 | 今天成立（§5.3），但绑地址一改就断。写进文档并在此登记 |
| 误以为桶名已被完整校验 | 上游只兜住了「`_` 非法」这一条（§3.2），长度/大小写/`..` 等仍未校验。文档不要写成「桶名已校验」 |
| 前端零测试 | 无构建链的必然代价。用 `tests/console.sh` + 手动验收兜底，不做「假装有单测」的补偿 |

---

## 10. 里程碑

| 阶段 | 内容 | 完成判据 |
|---|---|---|
| **M1 服务端骨架** | `CONSOLE_PREFIX`、`console.rs`、`--console` 开关、§8.1 全部测试 | `cargo test --workspace` 全绿，且 `--console` 关闭时行为零变化 |
| **M2 前端骨架** | 登录 + SigV4 + 桶列表 + 对象浏览 + 下载 | `tests/console.sh` 通过；浏览器里能走通主流程 |
| **M3 指标与概览** | `/metrics` 解析 + 折线 + `/ready` 状态呈现 | 跑一次 PUT 后概览页看到非零计数器 |
| **M4 文档同步** | README 与 `docs/DESIGN.md` 增补 console 一节，更新既有「非目标」里 Console 那条说明 | 文档不漂移：本文与两份文档口径一致 |

---

## 11. 与既有文档的关系

- `docs/DESIGN.md` §18（运维面）新增 console 小节；§20 路线图内登记本项。
- `docs/DESIGN.md` §1.2 与 `README.md` §7 现有的「不做 MinIO Admin API / Console 协议」
  依然成立：**本文不实现 Console 协议**（不是 MinIO 的那套 RPC 契约），只是一个挂在
  既有路由上的静态页面。这句话要在两处都写清楚，否则后人会以为口径变了。
- `crates/api` 的 `ObjectStore` trait **不动**——只读面板用不到新的引擎能力。
