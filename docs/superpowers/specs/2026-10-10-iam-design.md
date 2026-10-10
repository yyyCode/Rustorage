# 身份与访问管理（IAM）设计

**目标**：`rstore-server` 从「一对硬编码凭证、拿到即全权 root」变成
「有身份、有策略、默认拒绝」——即 MinIO 那种 PBAC 模型的最小可用版本。

**范围**：本次（M1）只做**内部 IDP + 身份策略**这两件事。组、条件键、OIDC、AD/LDAP、
认证/访问管理插件、临时凭证都只写轮廓（§10），各自需要独立的后续设计。

**依据**：`storage-design.md` §9.4/§9.5、§12.1（crate 职责划分）、§12.3（授权点）、
`docs/DESIGN.md` §18.3。本文与它们有**实质偏离**，逐条列在 §13。

---

## 1. 先说三条实测事实

它们决定了这份设计的形状——两条让它变小，一条让它变危险。

### 1.1 上游把认证做完了，把授权点也留好了

`s3s-0.17.0` 同时提供两个扩展点：

- **`S3Auth`**（`auth/mod.rs:117`）：`async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey>`。
  签名验证本身（SigV4 / SigV2）由上游完成，我们只负责「这个 access key 的 secret 是什么」。
  现状用的 `SimpleAuth::from_single` 就是它的一个实现（`auth/simple_auth.rs`）。
- **`S3Access`**（`access/generated.rs:15`）：一个通用 `check(&self, cx: &mut S3AccessContext)`，
  外加每个操作一个的细粒度方法。上下文里带 `credentials()` / `s3_path()` / `s3_op()` /
  `method()` / `uri()` / `headers()` / `extensions_mut()`。

**结论：本次既不碰 HTTP 层，也不写认证中间件。** 工作全在策略引擎与两个适配器上，
与控制面板那次「要在 `accept_loop` 里插一层路由」的形态完全不同。

### 1.2 操作名已经就是 AWS 的动作名

`ops/mod.rs:612` 构造上下文时传的是 `op.name()`，取值如 `GetObject` / `PutObject` /
`ListObjectsV2` / `DeleteObjects`（定义在 `ops/generated/*.rs` 的 `NAME` 常量里）。

于是 **IAM 动作名 = `"s3:" + cx.s3_op().name()`**，不需要自己维护一张
「S3 操作 ↔ 动作名」的映射表。只有少数几个与 AWS 的命名习惯不一致，用一张 8 行的
规范化表吸收掉（§4.2）。

### 1.3 但现在那个 403 是「兜底」给的 —— 这是本次最大的风险

今天 `build_service` 只调了 `set_auth`，从没调过 `set_access`。那么未签名的请求
（例如 `startup.rs` 测试里的 `GET /metrics/`）为什么是 403？

因为 `ops/mod.rs:595` 的 `authorize()` 在 `ccx.access` 为 `None` 时会落到
`access::default_check`：

```rust
pub(crate) fn default_check(cx: &mut S3AccessContext<'_>) -> S3Result<()> {
    match cx.credentials() {
        Some(_) => Ok(()),
        None => Err(s3_error!(AccessDenied, "Signature is required")),
    }
}
```

**这是 s3s 的默认授权策略，不是验签失败。**（验签在 `ops/mod.rs:945`，排在
`authorize()` 的 987 行之前；`scx.check()` 对未签名请求返回 `Ok(None)` 而非报错，
所以在装了自己的 access 之后，匿名请求**照样会走到我们的 `check()` 里**。）

**一旦 `set_access` 装上我们自己的实现，这层兜底就没了。** 一个忘了处理
`credentials() == None` 的 `check()`，会让服务从「未签名全部 403」变成
「未签名全部 200」——公开可读写的存储桶，静默地。

这条会作为不变量 §9 #1 写死，并配一条**反向**回归测试（§8.2）：先断言匿名被拒，
再断言 `check()` 里那条分支被删掉时测试会红。

### 1.4 另一个上游陷阱：只装 access 不装 auth = 检查被整体跳过

`access/mod.rs` 的模块文档明写：`S3Access` 只在配置了 auth provider 时才会被调用
（`authorize()` 的第一行就是 `if ccx.auth.is_none() { return Ok(()) }`）。

**所以 `set_auth` 与 `set_access` 必须成对出现**，且这一点要有测试钉住——
它属于「改错了不会报错、只会静默失去防护」的那类配置。

---

## 2. 关键决策

### 决策一：新增叶子 crate `rstore-iam`（不是塞进 `rstore-s3`）

| | A：写进 `crates/s3` | **B：新 crate `rstore-iam`（采纳）** |
|---|---|---|
| 单元测试 | 要构造 `S3Service` + 签名请求才能测一条通配符 | **纯函数，`cargo test -p rstore-iam` 秒过** |
| 对 s3s 的依赖 | 策略引擎里混着 `S3Path` / `S3Operation` | **零依赖 s3s**，动作名与资源名都是 `&str` |
| 未来的写路径（M3） | 组合根要反过来依赖 s3 的内部模块 | 组合根直接依赖它，方向正确 |
| 成本 | — | 白名单加一行、`Cargo.toml` 加一个 crate |

`rstore-iam` 的依赖集合是**空集**：不依赖任何 `rstore-*` crate，只有 `serde` /
`serde_json` / `thiserror`。它不需要 `rstore-common`——盘上目录由调用方拼好路径传进来，
`RESERVED_PREFIX` 的引用留在 `rstore-server`。

### 决策二：只实现 `S3Access::check()`，不实现 per-op 方法

上下文里已经有 `credentials` / `s3_path` / `s3_op`，够 MVP 用；一个函数覆盖**全部**
操作，包括 P2 的 multipart——那时授权零改动。

代价是拿不到**反序列化之后**的请求参数，所以 `s3:prefix`、`s3:max-keys`、
`aws:SourceIp` 这类条件键做不了（`headers()` 能拿到 IP 与头，但拿不到 body/query 里的
语义参数）。**这是 M4 的分水岭**：一旦要按请求参数授权，就必须从 `check()` 迁到 per-op
方法（它们的入参是 `S3Request<XxxInput>`）。§10 的 M4 会写清这一点，避免后来的人
以为 `check()` 是终点。

### 决策三：策略语法取 AWS 子集，加一张动作名规范化表

| | A：自造一套简化规则 | **B：兼容 AWS IAM policy 子集（采纳）** |
|---|---|---|
| 解析代码 | 更少 | 多一个 `OneOrMany`（`"Action"` 可以是字符串或数组） |
| 用户上手 | 要读新文档 | **照抄 AWS 文档与 MinIO 教程即可** |
| 未来接 OIDC/LDAP | 每种身份源都要重翻一遍语义 | 策略不重写，只换「谁拥有这些策略」 |
| 工具生态 | 无 | `mc` 导出的策略、现成的策略样例都能用 |

这是 MinIO 的核心卖点，成本只有一个 untagged enum。取。

### 决策四：用户与策略落盘 `.rstore/iam/`，只读盘 0

| | A：只在命令行/内存 | **B：盘上目录（采纳）** | C：存成普通对象（`meta.xl`） |
|---|---|---|---|
| 重启后 | 全部丢失 | **保留** | 保留 |
| 写入路径 | 不需要 | **不需要**（M1 只读） | 需要，且要设计原子性 |
| 自举问题 | 无 | 无 | **「谁有权读 IAM 数据」循环依赖** |
| 出现在 LIST 里 | — | 否 | 会（除非特殊处理） |

选 B。M1 **没有任何程序化写入**——运维手工编辑 JSON、重启生效。因此不需要设计
「原子写 + 多盘一致提交」这套东西；M3 做管理 API 时再补（§10）。

**只读 `--volumes[0]`** 是一处有意的单点妥协，理由要写清楚：没有写路径，多盘副本的
一致性根本无从谈起，假装有反而更糟。而 `open_disks` 本来就要求所有盘可读（任一盘
`NotFound` 就拒绝启动），所以「盘 0 可读」不是新增的运行前提。

### 决策五：root 不受策略约束

照搬 MinIO 与 AWS 根账号的语义：`--access-key` / `--secret-key` 指定的那对凭证**恒
Allow**，策略对它无效。

理由是运维需要一个永不锁死自己的后门——否则一条写错的策略就能让整个部署无法管理，
而 M1 又没有管理 API 可以救回来。**这是有意的例外，必须写进文档**，否则「root 为什么
绕过策略」看起来像个 bug。

### 决策六：未知字段一律拒绝加载

策略里出现 `Condition` / `NotAction` / `NotResource` / `Principal`，或者字段名拼错
（`"Actions"`），**在启动期报错退出**，而不是忽略。

理由不是洁癖：静默忽略一个 `Condition` 会让策略**比作者预期更宽**（`"Condition":
{"IpAddress": ...}` 被丢掉后，一条本该只在办公网生效的规则变成了对全网生效）。这是
安全缺陷，不是便利问题。`Sid` 例外——它是纯元数据，允许并忽略。

---

## 3. 数据模型

### 3.1 身份

三类主体，MVP 只有前两类：

| 主体 | 来源 | 能否被策略限制 |
|---|---|---|
| **root** | 命令行 `--access-key` / `--secret-key` | **否**（决策五） |
| **user** | `<iam-dir>/users/<access-key>.json` | 是（deny-by-default） |
| group / STS / OIDC / LDAP | — | M2 / M5 / M6 |

**access key 就是身份标识**，也是 `S3AccessContext` 唯一能给出的主体信息
（`credentials.access_key`）。这正是它必须是用户主键的原因。

**用户文件名（去掉 `.json`）就是 access key**，文件里不重复声明。这带来两个好处：
结构上不可能出现「两个文件声明同一个 access key」的歧义；operator 一眼能看出
哪对凭证是谁。与 MinIO 的模型一致。

### 3.2 策略文档

`<iam-dir>/policies/<name>.json`，就是 AWS policy JSON：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject"],
      "Resource": ["arn:aws:s3:::photos/*"]
    },
    {
      "Effect": "Deny",
      "Action": ["s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::photos/locked/*"]
    }
  ]
}
```

Rust 侧：

```rust
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyDoc {
    #[serde(rename = "Version")]
    pub version: String,
    #[serde(rename = "Statement")]
    pub statement: Vec<Statement>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Statement {
    /// 纯元数据，允许出现但本实现不使用。
    #[serde(rename = "Sid", default)]
    pub sid: Option<String>,
    #[serde(rename = "Effect")]
    pub effect: Effect,
    #[serde(rename = "Action")]
    pub action: OneOrMany,
    #[serde(rename = "Resource")]
    pub resource: OneOrMany,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Effect { Allow, Deny }

/// AWS 允许这两个字段是字符串或字符串数组，两种都得收。
#[derive(Deserialize)]
#[serde(untagged)]
pub enum OneOrMany { One(String), Many(Vec<String>) }
```

`Version` **必填但不校验取值**：它在本实现里不影响任何语义，把一个语义无关的字符串
变成加载失败只会制造无谓的摩擦。

### 3.3 用户文件

`<iam-dir>/users/<access-key>.json`：

```json
{
  "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "status": "enabled",
  "policies": ["readonly"]
}
```

```rust
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserRecord {
    pub secret_key: String,
    pub status: UserStatus,
    #[serde(default)]
    pub policies: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UserStatus { Enabled, Disabled }
```

**必填与缺省的分界是「猜错会不会放宽权限」**：

- `secret_key` 必填（没有它就认证不了）；
- `status` **必填**——缺省成 `enabled` 会在作者本想停用某个用户时留下一对可用凭证，
  是放宽；
- `policies` **缺省为 `[]`**——空策略在 deny-by-default 下是**最窄**的取值（什么都做不了），
  是收紧。

### 3.4 盘上布局

```
<volume>/.rstore/iam/
    users/
        alice.json          # access key = "alice"
        robot-backup.json
    policies/
        readonly.json
        photos-rw.json
```

- 落在 `.rstore` 保留前缀下：`crates/common/src/consts.rs` 的 `RESERVED_PREFIX`
  保证用户 key 的首段与桶根下的系统目录都不以它开头，**所以 S3 命名空间结构上够不到
  这个目录**——既能 GET 也能 PUT 的一个名叫 `.rstore` 的桶不存在。
- 加载时只读 `*.json` 的普通文件；**遇到子目录报错**（说明放错了层级），
  非 `.json` 的文件忽略（编辑器的 `.swp` 之类不该让服务起不来）。
- 目录不存在 = 没有非 root 用户，**不是错误**。

---

## 4. 求值语义

### 4.1 算法

写死如下，不留歧义：

```
1. credentials == None              → Deny(Anonymous)
2. access_key == root               → Allow
3. 用户不存在                        → Deny(UnknownPrincipal)
4. status == Disabled               → Deny(Disabled)
5. action   = "s3:" + canonical(op_name)
   resource = arn(s3_path)
6. 把用户所有策略里、action 与 resource 都命中的语句收成一个集合
7. 集合里任一是 Deny                 → Deny(ExplicitDeny)
8. 集合里任一是 Allow                → Allow
9. 集合为空                          → Deny(NoMatchingStatement)
```

第 3 步与认证阶段**冗余**：`get_secret_key` 找不到 key 时已经拒了请求，走不到这里。
保留它是因为 `authorize()` 必须能独立地正确——它是唯一的安全边界，不该依赖
「上游一定先跑过认证」这个假设。这是有意的纵深防御，不是死代码。

Rust 签名上有一个刻意的选择：

```rust
pub fn authorize(&self, access_key: Option<&str>, action: &str, resource: &str) -> Decision;
```

`Option<&str>` 而不是 `&str` ——**让「没有凭证」成为类型的一部分**，而不是调用方
可能忘记写的 `if`。§1.3 那条风险正是从这个缝里钻进来的。

### 4.2 动作名规范化表

`canonical()` 把两边都映射到 AWS 的规范名，**只列与 s3s 命名不一致的**：

| s3s 的 op 名 | 规范名（AWS 动作） | 原因 |
|---|---|---|
| `ListBuckets` | `ListAllMyBuckets` | AWS 的 `ListBuckets` API 对应动作叫 `s3:ListAllMyBuckets` |
| `ListObjects` / `ListObjectsV2` | `ListBucket` | 两个 API 共用 `s3:ListBucket` |
| `DeleteObjects` | `DeleteObject` | 批量删除用的仍是 `s3:DeleteObject` |
| （其余） | 同名 | `GetObject` / `PutObject` / `HeadObject` / `CreateBucket` / `GetBucketLocation` … |

表里每一条配一个测试（§8.1）。策略里写 `s3:ListBucket` 能命中 `ListObjectsV2`，
就是这张表在起作用——它存在的唯一目的就是让照抄 AWS 文档写的策略可以直接用。

### 4.3 资源 ARN

由 `S3Path` 决定（映射写在 `rstore-s3`，ARN 构造函数放在 `rstore-iam`，
格式只在一处定义）：

| `S3Path` | 资源 | 例 |
|---|---|---|
| `Root`（`ListBuckets`） | `arn:aws:s3:::*` | — |
| `Bucket{bucket}` | `arn:aws:s3:::<bucket>` | `arn:aws:s3:::photos` |
| `Object{bucket,key}` | `arn:aws:s3:::<bucket>/<key>` | `arn:aws:s3:::photos/a/b.jpg` |

对应关系与 AWS 一致：桶级操作（`HeadBucket` / `ListBucket` / `DeleteBucket` /
`CreateBucket`）判桶 ARN，对象级操作（`GetObject` / `PutObject` / `HeadObject` /
`DeleteObject`）判对象 ARN。

### 4.4 通配符

只支持两个，语义照 AWS：

| 字符 | 语义 |
|---|---|
| `*` | 匹配任意长度（**含 `/`**） |
| `?` | 匹配恰好一个字符 |

`arn:aws:s3:::photos/*` 因此能匹配 `arn:aws:s3:::photos/a/b/c.jpg`。
区分大小写。不用 `regex` ——依赖换不来什么，而 `*` / `?` 的语义与正则本来就不同
（正则的 `*` 只作用于前一个字符）。

---

## 5. 装配与接线

### 5.1 `IamStore` 的公开面

```rust
// crates/iam/src/lib.rs
pub struct IamStore { /* root + users + policies */ }

impl IamStore {
    /// 从盘上加载。**唯一的构造入口**，所有校验都在这里发生（§7.1）。
    pub fn load(dir: &Path, root_access_key: &str, root_secret_key: &str)
        -> Result<Self, IamError>;

    /// 只给 root 的 store：测试与「还没有 IAM 目录」的场景用。
    pub fn root_only(access_key: &str, secret_key: &str) -> Self;

    /// 认证用：这个 access key 的 secret 是什么。
    pub fn secret_key(&self, access_key: &str) -> Option<&str>;

    /// 授权用：见 §4.1。
    pub fn authorize(&self, access_key: Option<&str>, action: &str, resource: &str)
        -> Decision;

    /// 启动期日志用。
    pub fn user_count(&self) -> usize;
    pub fn policy_count(&self) -> usize;
}

pub enum Decision { Allow, Deny(DenyReason) }

pub enum DenyReason {
    Anonymous,
    UnknownPrincipal,
    Disabled,
    NoMatchingStatement,
    ExplicitDeny,
}
```

`DenyReason` 是公开的：§7.2 的拒绝日志要打出原因，否则运维面对一个 403 无从下手
（是没这份策略？还是被一条 Deny 压掉了？）。

### 5.2 `rstore-s3` 的两个适配器

```rust
// crates/s3/src/iam.rs（新文件）
struct IamAuth { iam: Arc<IamStore> }
struct IamAccess { iam: Arc<IamStore> }

#[async_trait::async_trait]
impl S3Auth for IamAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        self.iam.secret_key(access_key)
            .ok_or_else(|| s3_error!(InvalidAccessKeyId))
            .map(|s| SecretKey::from(s.to_owned()))
    }
}

#[async_trait::async_trait]
impl S3Access for IamAccess {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        let ak = cx.credentials().map(|c| c.access_key.as_str());
        let action = format!("s3:{}", cx.s3_op().name());
        let resource = resource_arn(cx.s3_path());
        match self.iam.authorize(ak, &action, &resource) {
            Decision::Allow => Ok(()),
            Decision::Deny(reason) => {
                tracing::warn!(/* access_key, action, resource, ?reason */);
                Err(s3_error!(AccessDenied))
            }
        }
    }
}
```

**拆成两个类型而不是一个同时实现两个 trait 的类型**：它们的失败模式与可观测性不同
（认证失败是「这个 key 不存在」，授权失败是「这个 key 存在但这件事不许做」），
分开更好读、也更好单测。

一处要注意的行为变化：`SimpleAuth` 对未知 access key 返回的是
**`NotSignedUp`**，上面写的是 `InvalidAccessKeyId`（AWS 的归位）。`impl_s3.rs:714`
的 `rejects_bad_signature` 测的是签名错误、不是未知 key，但落地时要核对该文件里还有没有
别的测试钉住了 `NotSignedUp` 这个码。

### 5.3 `build_service` 的签名

```rust
pub fn build_service(
    store: Arc<dyn ObjectStore>,
    iam: Arc<IamStore>,
    base_domain: Option<&str>,
) -> Result<S3Service, String> {
    let mut builder = S3ServiceBuilder::new(RstoreFs { store });
    // 两者必须成对设置：只设 access 不设 auth 时，authorize() 会整体跳过（§1.4）。
    builder.set_auth(IamAuth { iam: Arc::clone(&iam) });
    builder.set_access(IamAccess { iam });
    /* base_domain 分支不变 */
}
```

`access_key` / `secret_key` 两个参数消失了——它们转到 `IamStore` 的构造里
（`startup.rs` 负责从命令行取）。这样 `rstore-s3` 不需要知道 root 的概念。

### 5.4 启动期加载

`startup.rs` 的 `Running::bind` 里，`open_disks` 之后、`build_service` 之前：

```rust
let iam = Arc::new(
    IamStore::load(&cfg.iam_dir(), &cfg.access_key, &cfg.secret_key)
        .map_err(|e| anyhow!("加载 IAM 配置失败：{e}"))?,
);
tracing::info!(users = iam.user_count(), policies = iam.policy_count(),
               root = %cfg.access_key, "IAM 已加载");
```

配置项（`crates/server/src/config.rs`）：

```rust
/// IAM 配置目录。默认 `<volumes[0]>/.rstore/iam`。
#[arg(long)]
pub(crate) iam_dir: Option<PathBuf>,
```

`Option` + 访问器方法，**不能用 clap 的 `default_value`**：默认值依赖 `--volumes`，
与 `parity()` 是同一个理由（`config.rs:53` 已有这个模式）。

**M1 没有热加载**：改完文件要重启。这写在文档里、也写在 `--help` 的说明里，
免得有人改完文件等半天以为是 bug。

---

## 6. 边界：IAM 管不到什么

单列一节，因为这最容易被误读成「装了 IAM 就全站受控了」。

**IAM 只覆盖 S3 协议面。** 下面这些在 `accept_loop` 里就被截走了，根本不进 s3s，
因此不受任何策略约束：

| 路径 | 谁在管 |
|---|---|
| `/health`、`/ready` | `readiness.rs`，无条件应答 |
| `/metrics` | `metrics.rs`，**只要 `--metrics` 打开，任何能连上 127.0.0.1 的人都能读** |
| `/_console` 及其静态资源 | `console.rs`，无条件应答 |

加上服务只绑回环、没有 TLS，实际的安全边界仍然主要是「谁能连上这台机器的 127.0.0.1」。
**IAM 管的是「连上来之后能做什么」，不是「谁能连上来」。** 这句话值得原样进 README。

反过来有一个免费的好性质：**控制面板零改动**。它本来就是浏览器端 SigV4 直连同一个
S3 端点（`console/sigv4.js`），用户拿自己那对凭证登录，自然只看到自己有权的东西——
面板甚至不需要知道 IAM 存在。

---

## 7. 错误模型

### 7.1 启动期（fail-loud，拒绝启动）

与 `format.json` 的处理哲学一致：配置错误必须在进程起来之前炸掉，并报出**文件名**。

| 情况 | 行为 |
|---|---|
| JSON 语法错、必填字段缺失（`Version` / `secret_key` / `status`） | 拒绝启动，报文件名与 serde 的解析错误 |
| 未知字段（含 `Condition` / `NotAction` / `NotResource` / `Principal`） | 拒绝启动（决策六） |
| `users/` 下出现子目录 | 拒绝启动 |
| 用户引用不存在的策略 | 拒绝启动，报「用户 X 引用了策略 Y，但 `policies/Y.json` 不存在」 |
| 用户 access key 与 root 相同 | 拒绝启动（会出现「既是 root 又受限」的矛盾身份） |
| 策略名为空 / 文件名为空 | 拒绝启动 |
| `iam-dir` 不存在 | **正常**：等于没有非 root 用户 |
| `users/` 或 `policies/` 缺失 | **正常**：等于空 |

### 7.2 请求期

| 情况 | 响应 |
|---|---|
| 未知 access key | `InvalidAccessKeyId`（认证阶段，§5.2） |
| 匿名请求 | `403 AccessDenied`（`DenyReason::Anonymous`） |
| 有身份但策略不允许 | `403 AccessDenied` |
| 显式 Deny 命中 | `403 AccessDenied` |

**每次 Deny 记一条 `tracing::warn!`**，字段 `access_key` / `action` / `resource` /
`reason`。这是 M1 里最重要的一条可观测性——没有它，运维面对一个 403 只能靠猜。

`secret_key` **永不入日志**。`access_key` 记完整值（它是身份标识，不是凭证）；
`docs/DESIGN.md` §18.3 目前把 access_key 也列为脱敏字段，需要在那里对齐（见 §13）。

**不在响应体里区分「key 不存在」与「策略不允许」**：两者都是 `403 AccessDenied`，
不给凭证枚举留便利。日志里的区别是给运维看的，不是给调用方看的。

---

## 8. 测试策略

### 8.1 `rstore-iam` 单元测试（不起 HTTP）

- **通配符**：`*` 跨 `/`、`?` 单字符、前缀、精确匹配、`*` 单独出现、大小写敏感；
- **deny-by-default**：空策略 → `NoMatchingStatement`；用户存在但 `policies: []` → 同上；
- **显式 Deny 压过 Allow**：同一份策略里同时命中 Allow 与 Deny，断言 `ExplicitDeny`；
- **root 恒 Allow**：给 root 绑一份全是 Deny 的策略，断言仍 Allow；
- **`Option<&str>` 那条路**：`authorize(None, ..)` → `Anonymous`；
- **规范化表**：§4.2 每一行一个断言（`s3:ListBucket` 命中 `ListObjectsV2` 等）；
- **加载校验**：§7.1 表格里每一条一个 `tempfile::TempDir` 用例，断言 `Err` 且
  错误信息里含文件名；
- **`OneOrMany`**：`"Action": "s3:GetObject"` 与 `["s3:GetObject"]` 等价。

### 8.2 `rstore-s3` 集成测试（真实签名请求）

复用 `impl_s3.rs` 测试模块已有的脚手架——`NopStore` + `service()`（`impl_s3.rs:564`，
内含 `S3ServiceBuilder`）与三个签名辅助 `signed_request` / `signed_request_with_secret` /
`signed_list_buckets`（`impl_s3.rs:634–694`）——只把 `service()` 里的
`SimpleAuth::from_single` 换成 `IamStore::root_only` + 两个适配器：

- **匿名回归（这条最重要）**：未签名请求必须仍 403。§1.3 说的就是这个——
  `default_check` 不再兜底了，安全性完全落在我们的 `check()` 上；
- 签名正确 + root → 200；
- 签名正确 + 只读用户 → GET 200 / PUT 403；
- 签名正确 + `status: disabled` → 认证阶段就 403；
- 未知 access key → `InvalidAccessKeyId`；
- 显式 Deny 命中 → 403（哪怕同一用户也有 Allow）。

**既有测试的影响**：`impl_s3.rs` 的测试全部用 root 凭据签名，所以换成
`root_only` 之后应当**全绿**，不需要改断言。`startup.rs` 的 `console_route_is_wired_and_off_by_default`
里那条「`/metrics/` 是 403」也照旧——但它的含义变了：从前是 `default_check` 给的，
现在是我们的 `DenyReason::Anonymous` 给的。**在注释里写明这个含义变化**，
否则下一个人会以为它测的还是上游的兜底。

### 8.3 验收脚本 `tests/iam.sh`

起真实服务（`--iam-dir` 指向一份临时造的配置），用 `aws-cli` 验：

| 步骤 | 期望 |
|---|---|
| root 建桶、PUT、GET | 全 200 |
| 只读用户 GET | 200 |
| 只读用户 PUT | 403 |
| 只读用户 `s3api list-buckets` | 403 |
| 匿名 `curl` 直连 | 403 |
| 停用用户 | 403 |

沿用 `tests/acceptance.sh` 的凭据前置写法（`AWS_ACCESS_KEY_ID` 等三个 export 必须在
脚本里显式给，不能依赖开发机上的 `~/.aws/credentials`）。

### 8.4 必须改掉的既有测试

- `crates/s3/src/impl_s3.rs` 的 `service()` 夹具（`:564`）：换成
  `IamStore::root_only` 装配。**断言一个都不用动**——那些测试全用 root 凭据签名。
- `crates/server/src/startup.rs` 的 `make_config`：加 `iam_dir: None` 字段。
- `scripts/check_layer_deps.py`：`ALLOWED` 加 `rstore-iam`，并给 `rstore-s3` /
  `rstore-server` 补上这条边（§13）。
- `scripts/tests/test_check_layer_deps.py`：如果里面有「白名单表的形状」类断言，
  一并更新。

---

## 9. 限制与不变量

### 不变量（每条都要有测试）

1. **未签名请求一律被拒。** 安装 `set_access` 不得让匿名访问变为可行（§1.3）。
2. **空 IAM 目录 ⇒ 除 root 外无人能通过认证。** 「没配 IAM」不等于「不设防」。
3. **任一命中语句是 Deny ⇒ 必拒**，哪怕同一主体同时命中 Allow。
4. **没有任何 Allow ⇒ 必拒**（deny-by-default）。
5. **root 恒 Allow**，不受策略影响。
6. **引用不存在的策略 ⇒ 启动失败**，不是运行时静默降级成「没这条策略」。
7. **`set_auth` 与 `set_access` 成对存在**（§1.4）。

### 限制表

| 限制 | 说明 |
|---|---|
| 无 TLS、只绑 `127.0.0.1` | 与今天一致，IAM 不改变这一点 |
| 默认凭据仍是硬编码的 `rustorage` / `rustorage-secret` | IAM 不改变这一点 |
| secret key **明文落盘** | 与 MinIO 一致；文件权限（`chmod 600`）是部署侧的事，MVP 不设 |
| 无热加载 | 改完要重启 |
| 无 `Condition` / `NotAction` / `NotResource` / `Principal` | 出现即拒绝加载（决策六） |
| 只读 `--volumes[0]` 的 IAM 目录 | 有意的单点妥协（决策四） |
| 不覆盖 `/health` `/ready` `/metrics` `/_console` | §6 |
| 只有身份策略，没有 bucket policy | bucket policy 是 M6 |
| 无组、无临时凭证、无外部身份源 | M2 / M5 / M6 |

---

## 10. 里程碑

**M1（本文，本次实现）** —— 内部 IDP + 身份策略

`crates/iam` 新 crate；动作名规范化表；通配符匹配；`IamStore::load` 的全部校验；
`IamAuth` / `IamAccess` 两个适配器；`build_service` 换签名；`--iam-dir` 配置项；
`tests/iam.sh`；层依赖白名单与 README 同步。

**M2 —— 组**

`groups/<name>.json`（含 `policies` 与 `members`），用户可属于多个组。
求值从 `user.policies` 变成 `user.policies ∪ ⋃(所属组的 policies)`——**只动 §4.1 的第 6 步**，
其余语义（deny-by-default、显式拒绝优先、root 绕过）不变。

**M3 —— 管理面**

控制面板加管理页 + 管理 REST API（要求 root 签名），才能把「手工编辑 JSON + 重启」
换掉。**这一阶段的难点不是 API 而是写入**：需要原子替换、`sync_file_and_parent`，
以及「盘 0 单点」在多副本下是否还成立（决策四留下的账）。

**M4 —— 条件键**

`aws:SourceIp`、`s3:prefix`、`s3:max-keys`、`s3:x-amz-acl` 等。
**架构上的分水岭**：这些必须读反序列化后的请求参数，因此要从 `check()` 迁到
`S3Access` 的 per-op 方法（入参是 `S3Request<XxxInput>`）。届时 `check()` 退化为
「兜底的通用检查」，per-op 方法做细粒度判断——**两者都要保留**，不能只留后者，
否则未建模的操作会失去保护。

**M5 —— 外部身份源**

- **OIDC**：JWKS 获取与缓存、JWT 签名校验、`policy` claim 决定该 token 带哪些策略
  （MinIO 的模型）。需要一个 HTTP 客户端依赖，这是 `rstore-iam` 第一次引入「有网络
  副作用」的代码，会打破当前「纯函数」的性质——**建议为它单独开一个 crate**。
- **AD / LDAP**：DN → 策略的映射表。
- **认证插件 / 访问管理插件**：把「认证」与「授权」两个决策点外化成 webhook。
  `S3Auth` / `S3Access` 的形状天然适合这件事。

**M6 —— 临时凭证与其余**

STS 会话凭证（带过期时间与 `session policy`）、service account、
预签名 URL 的策略约束、bucket policy。

> M2 之后的每一项都各自值得一份独立设计文档。本文只给轮廓，以及它们对本架构的影响。

---

## 11. 风险

| 风险 | 缓解 |
|---|---|
| **安装 `set_access` 后忘了拒匿名 → 服务变成公开存储桶** | 不变量 #1 + §8.2 的匿名回归测试 + 反向验证（删掉那条分支必须让测试变红） |
| 只装 `set_access` 忘了 `set_auth` → 检查被整体跳过 | 不变量 #7；`build_service` 里两行写在一起，中间不放别的东西 |
| 策略写宽了（`Resource: "*"`）而无感知 | 启动期日志打印「用户 → 策略 → 语句条数」；§7.2 的 Deny 日志给出原因 |
| 运维改了文件忘了重启，以为策略没生效 | `--help` 与 README 都写明；M3 的管理 API 是根治 |
| 上游 s3s 升级改变了 `authorize()` 的调用时机 | 不变量 #1 的测试会在升级时立刻变红——这正是它存在的意义 |
| `NotSignedUp` → `InvalidAccessKeyId` 影响既有客户端行为 | 落地前核对 `impl_s3.rs` 与验收脚本里有没有钉住这个码 |

---

## 12. 与 MinIO 的对照

| MinIO 的概念 | 本设计 | 状态 |
|---|---|---|
| AWS SigV4 认证 | s3s 内建（`S3Auth`），本项目只实现 `get_secret_key` | ✅ 已有 |
| SigV2（已废弃） | s3s 也支持，本项目不做额外工作 | ✅ 上游 |
| PBAC / IAM policy 语法 | AWS 子集（无 `Condition`） | ✅ M1 |
| deny-by-default | 同一语义 | ✅ M1 |
| 内部 IDP（用户 + 访问密钥） | `users/<access-key>.json` | ✅ M1 |
| root 账号 | 命令行凭据，不受策略约束 | ✅ 已有（本次把语义明确化） |
| `mc admin policy attach` | 用户 JSON 里的 `policies` 数组 | ✅ M1（**手工编辑，无 API**） |
| 组（group） | — | M2 |
| 管理 API / 控制台管理页 | — | M3 |
| 条件键 | — | M4 |
| OpenID Connect（JWT `policy` claim） | — | M5 |
| AD / LDAP（DN → 策略） | — | M5 |
| MinIO 认证插件 | — | M5 |
| 访问管理插件（webhook 授权） | — | M5 |
| STS 临时凭证 / service account | — | M6 |
| bucket policy（匿名只读桶等） | — | M6 |

---

## 13. 与既有文档的偏离（必须同步）

| 文档 | 需要改什么 |
|---|---|
| `storage-design.md` §12.1 | 它是按 rustfs 的 11 个 crate 划分写的（`credentials` `crypto` `iam` `keystone` `kms` `policy` `signer` …）。本仓库只落一个 `rstore-iam`，且**不拆 `credentials` / `policy` 两层**——M1 的量（约 300 行）拆开只会让「改一处要动三个 crate」。这是本文与它最主要的偏离 |
| `storage-design.md` §12.3 | 它列的授权点是「桶策略 deny → IAM action → 兜底」，且只有健康探针是鉴权旁路。本设计的顺序是「匿名 → root → 身份策略」，**M1 没有桶策略**；旁路则**不止健康探针**（还有 `/metrics` 与 `/_console` 静态资源，见 §6）。两处都要在那里注明 |
| `docs/DESIGN.md` §18.3 | 它把 `access_key` 列为脱敏字段；§7.2 决定把完整 access key 记进拒绝日志（身份标识 ≠ 凭证）。两处要选一个说法并对齐 |
| `docs/DESIGN.md` §5 | 依赖规则表要加 `rstore-iam` 一行（叶子，无内部依赖） |
| `scripts/check_layer_deps.py` | `ALLOWED` 加 `"rstore-iam": set()`；`rstore-s3` 追加 `rstore-iam`；`rstore-server` 追加 `rstore-iam`。闭包天然成立（iam 无内部依赖），但 `table_errors` 会强制你写全 |
| `README.md` | 新增 `--iam-dir` 参数；新增「身份与权限」一节；§7 的「已知限制」表补一行「只有 root 时无授权，多用户需要 IAM 目录」；**§6 附近加一句「IAM 只管 S3 协议面，`/metrics` 等运维端点不受其约束」**（§6 的那句话值得原样进 README） |
| `docs/MVP.md` | 历史计划文档，不改 |
