# Rustorage

从零实现的 **S3 兼容对象存储服务**，走 MinIO / RustFS 那条架构路线：**纠删码**而非多副本，
**元数据与数据同盘**（sidecar 文件），**单个可执行文件**、无外部依赖，从第一天就为多节点
预留物理层级（MVP 只做单节点多盘）。

> **状态：MVP 已完成。**
> 单节点多盘纠删码可用，S3 数据面能被 `aws-cli` / `mc` / `rclone` 直接使用。
> **MVP 明确不做 multipart 上传**，因此单次 PUT 上限约 8 MiB——这是刻意的范围决策，
> 不是遗漏。完整清单见 [已知限制](#7-已知限制)。

- 设计文档：[`docs/DESIGN.md`](docs/DESIGN.md)
- 实施计划（含每条限制的来龙去脉）：[`docs/MVP.md`](docs/MVP.md)

---

## 目录

- [1. 它是什么](#1-它是什么)
- [2. 环境要求](#2-环境要求)
- [3. 安装与构建](#3-安装与构建)
- [4. 启动服务](#4-启动服务)
- [5. 客户端使用](#5-客户端使用)
- [6. 容错能力与 quorum](#6-容错能力与-quorum)
- [6.5 控制面板（可选）](#65-控制面板可选)
- [7. 已知限制](#7-已知限制)
- [8. 开发](#8-开发)
- [9. 许可](#9-许可)

---

## 1. 它是什么

| 能力 | 说明 |
|---|---|
| 纠删码 | Reed–Solomon（`reed-solomon-simd`）。`N` 块盘 = `data + parity`，`2 ≤ N ≤ 16`，每块盘持有一份分片 |
| 单节点多盘 | 所有盘构成一个 erasure set；掉盘只要不破 quorum 就照常读写 |
| S3 数据面 | 桶与对象的增删查列、范围读（Range）、条件请求（GET/HEAD 子集）、虚拟主机寻址（需 `--base-domain`）、SigV4 认证 |
| 完整性 | 每份分片带 keyed BLAKE3 校验和；静默位翻转会被检出并报 `BitrotMismatch`，绝不返回坏数据 |
| 崩溃一致性 | `.staging-*` 写入 + rename 提交 + fsync 文件与父目录；崩溃后看不到半成品对象 |
| 运维面 | `/health` 存活、`/ready` 就绪、`/metrics` Prometheus 文本、Ctrl-C 优雅关闭 |

**不是**这些（写下来是为了防止范围蔓延，详见 [DESIGN §1.2](docs/DESIGN.md)）：

- 不兼容 MinIO 的盘格式，也不实现 MinIO Admin API / Console 协议（[§6.5](#65-控制面板可选) 的控制面板是自研静态页面，不是这套协议）；
- 不做多存储后端（网关模式）、不做站点复制；
- MVP 不做多节点、不做 TLS、不做 multipart、不做对象锁。

## 2. 环境要求

| 项 | 要求 |
|---|---|
| 操作系统 | Linux (x86_64) 与 Windows (x86_64) 均已验证；macOS 未验证 |
| Rust | **1.97.1**（`rust-toolchain.toml` 已固定，`rustup` 会自动装好） |
| C 工具链 | **必需**。依赖链里的 BLAKE3 会在构建期用 `cc` 编译 C / 汇编实现，因此本机要有可用的编译器（Linux：`gcc` / `clang`；Windows：MSVC） |
| 磁盘 | 至少 2 个目录，且分属不同物理盘才有多盘容错意义（服务端不检查是否为同一块盘） |

平台验证依据：Linux 侧是仓库的 GitHub Actions（`ubuntu-latest`）全绿；Windows 侧是本机
（Windows 11 + MSVC）构建与端到端脚本通过。

## 3. 安装与构建

项目目前**不发布预编译二进制**，安装 = 装好 Rust 工具链后从源码构建。

### 3.1 Linux

```bash
# 1) 装 Rust（没装过的话；装完重开 shell 或 source "$HOME/.cargo/env"）
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 2) 装 C 编译器（Debian/Ubuntu 系）
sudo apt-get update && sudo apt-get install -y build-essential

# 3) 取源码并构建
git clone https://github.com/yyyCode/Rustorage.git
cd Rustorage
cargo build --release --locked
```

产物在 `target/release/rstore-server`。

装进 `PATH`（可选）：

```bash
cargo install --path crates/server --locked    # -> ~/.cargo/bin/rstore-server
```

### 3.2 Windows

```powershell
# 1) 装 Rust：从 https://rustup.rs 下载 rustup-init.exe 运行。
#    rustup 默认使用 MSVC 工具链，它需要「Visual Studio Build Tools」的
#    「使用 C++ 的桌面开发」工作负载（含 MSVC 编译器与 Windows SDK）。
#    只想要编译器、不装完整 IDE 的话，装 Build Tools 即可。

# 2) 取源码并构建（PowerShell / CMD 均可）
git clone https://github.com/yyyCode/Rustorage.git
cd Rustorage
cargo build --release --locked
```

产物在 `target\release\rstore-server.exe`。

装进 `PATH`（可选）：

```powershell
cargo install --path crates/server --locked    # -> %USERPROFILE%\.cargo\bin\rstore-server.exe
```

> **Git Bash 用户**：本仓库的 `tests/*.sh` 脚本都在 Git Bash 下验证过；只要
> `cargo` 与 C 工具链就位，命令与 Linux 一节完全相同。

### 3.3 验证构建

```bash
cargo test --workspace --locked
```

全绿即构建正确（当前 200 个用例）。若只想快速冒烟，也可以直接跳到
[第 4 节](#4-启动服务)起一个实例。

## 4. 启动服务

### 4.1 最小示例

```bash
# 准备 4 块「盘」（各自独立目录；服务端会自动创建缺失的目录）
mkdir -p /srv/rs/d{1,2,3,4}

rstore-server --volumes /srv/rs/d1 /srv/rs/d2 /srv/rs/d3 /srv/rs/d4 --parity 2
```

Windows PowerShell 同样：

```powershell
rstore-server --volumes D:\rs\d1 D:\rs\d2 D:\rs\d3 D:\rs\d4 --parity 2
```

启动成功会看到日志 `服务已就绪 addr=127.0.0.1:9000`。按 `Ctrl-C` 优雅关闭
（先停止接受新连接，再等在飞请求跑完，最多 5 秒）。

### 4.2 命令行参数

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--volumes <PATH>...` | **必填** | 盘根目录列表，**2 ~ 16 个**。个数即 erasure set 宽度 `N` |
| `--port <PORT>` | `9000` | HTTP 端口。**只绑 `127.0.0.1`**，不对外 |
| `--parity <N>` | 按盘数推导 | 校验分片数（详见 [§6](#6-容错能力与-quorum)） |
| `--access-key <KEY>` | `rustorage` | S3 Access Key |
| `--secret-key <SECRET>` | `rustorage-secret` | S3 Secret Key |
| `--base-domain <DOMAIN>` | 不设 | 开启虚拟主机寻址（`Host: bucket.example.com`）；不设 = 纯 path-style |
| `--metrics` | 关 | 打开 `/metrics` 指标计数 |
| `--console` | 关 | 打开只读控制面板（[§6.5](#65-控制面板可选)） |
| `--iam-dir <PATH>` | `<volumes[0]>/.rstore/iam` | IAM 配置目录（用户与策略），详见 [§4.5](#45-身份与权限iam)。目录不存在 = 没有非 root 用户 |
| `-h` / `--help` | | 帮助 |

> ⚠️ 默认凭据 `rustorage` / `rustorage-secret` 只适合本地试用。**对外提供服务前务必用
> `--access-key` / `--secret-key` 改掉。** MVP 没有 TLS，凭据在链路上是明文，
> 服务又只监听回环地址——请勿直接暴露到公网。

### 4.3 数据目录与重启语义

每块盘根目录下会出现：

```
<volume>/
├── format.json          # 部署拓扑：部署 id、本盘 id、分片分组
├── .rstore.sys/
│   └── disk_id
└── <bucket>/...         # 桶与对象数据（含元数据 sidecar）
```

几条**必须知道**的运维语义：

- **首次启动**：所有盘都为空 → 服务端按 `--volumes` 的顺序初始化，逐块写
  `format.json`。这个顺序决定了分片下标的分工。
- **重启时盘顺序可以变**：盘的身份来自盘上的 `format.json`，分片下标按它恢复，
  与 `--volumes` 给出的顺序无关。
- **`--parity` 必须与初始化时一致**。`parity` 决定了分片几何，但它**不在
  `format.json` 里**——服务端完全按命令行取值。重启/重建时改了这个值，等于把
  分片按错误的几何解读，数据将不可读。**换配置请当作重新部署**：新建一组空目录，
  用客户端把数据搬过去。
- **拒绝「部分空盘」**：若一部分盘有 `format.json`、另一部分是空目录，服务会
  **拒绝启动**并列出这些路径。这是刻意的——把空盘顺手初始化会把它悄悄认成老成员。
  要么补齐这些盘，要么把它们从 `--volumes` 里去掉。
- **拓扑不一致同样拒绝启动**：各盘 `format.json` 的部署身份对不上时，启动失败并
  按「盘路径 → 分组」打印清单，告诉你该去修哪块盘。

### 4.4 用 systemd 常驻（Linux，可选）

```ini
# /etc/systemd/system/rustorage.service
[Unit]
Description=Rustorage S3 server
After=network.target

[Service]
User=rustorage
ExecStart=/usr/local/bin/rstore-server --volumes /srv/rs/d1 /srv/rs/d2 /srv/rs/d3 /srv/rs/d4 --parity 2
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload && sudo systemctl enable --now rustorage
```

Windows 上可用 NSSM 或「任务计划程序」把可执行文件注册成服务，本项目不附带安装器。

### 4.5 身份与权限（IAM）

默认只有一对 root 凭据（`--access-key` / `--secret-key`），拿到它就是全权，没有
「只读用户」这种东西。要区分权限，就在 IAM 目录下放**用户**与**策略**：

```
<volumes[0]>/.rstore/iam/
    users/<access-key>.json      # 文件名（去掉 .json）就是 access key
    policies/<name>.json         # AWS IAM policy JSON
```

`users/alice.json`：

```json
{"secret_key": "alice-secret", "status": "enabled", "policies": ["readonly"]}
```

`policies/readonly.json`：

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::photos/*"]}
  ]
}
```

规则：

- **默认拒绝**——没有任何策略允许，就什么都做不了；
- **显式拒绝优先**——同一个请求上 `Allow` 与 `Deny` 同时命中时，`Deny` 赢；
- **root 不受策略约束**，专门留作永不锁死自己的后门；
- 策略是 AWS 的子集。`Action` 用 `s3:GetObject` 这种写法；`Condition` 与其他未知
  字段会被**拒绝加载**（而不是静默忽略——忽略条件等于把策略悄悄放宽）；
- **改完文件要重启**，M1 没有热加载。

启动日志会打出 `IAM 已加载 users=.. policies=.. dir=..`；启动失败时错误信息里带
文件名，且服务**不会开始监听**。

> ⚠️ **IAM 只管 S3 协议面。** `/health`、`/ready`、`/metrics` 与 `/_console` 的静态
> 资源在 s3s 之前就被截走，不受任何策略约束——只要 `--metrics` 开着，任何能连上
> `127.0.0.1` 的人都能读指标。加上没有 TLS、只绑回环，**真正的边界仍然是「谁能连上
> 这台机器」**。

## 5. 客户端使用

默认端点 `http://127.0.0.1:9000`、凭据 `rustorage` / `rustorage-secret`、区域
`us-east-1`（区域值只要客户端与服务端之间自洽即可，服务端不校验）。

> **载荷请小于 8 MiB。** 服务端不支持 multipart，而 `aws-cli` 的 `s3 cp` 对超过
> 8 MiB 的文件会自动改走分片上传，从而拿到 `501 NotImplemented`。

### 5.1 aws-cli

```bash
export AWS_ACCESS_KEY_ID=rustorage
export AWS_SECRET_ACCESS_KEY=rustorage-secret
export AWS_DEFAULT_REGION=us-east-1
export EP=http://127.0.0.1:9000

aws --endpoint-url $EP s3 mb s3://demo
aws --endpoint-url $EP s3 cp ./photo.jpg s3://demo/photo.jpg
aws --endpoint-url $EP s3 cp s3://demo/photo.jpg ./photo-back.jpg
aws --endpoint-url $EP s3 ls s3://demo/
aws --endpoint-url $EP s3 rm s3://demo/photo.jpg

# 对象级 API
aws --endpoint-url $EP s3api list-objects-v2 --bucket demo --prefix "" --max-keys 10
aws --endpoint-url $EP s3api head-object --bucket demo --key photo.jpg
aws --endpoint-url $EP s3api delete-objects --bucket demo \
    --delete 'Objects=[{Key=photo.jpg}],Quiet=false'
```

> 命中错误时注意：**aws-cli v1 打印状态码**（`(503)`），**v2 打印错误码**
> （`(ServiceUnavailable)`）。排查时两者都要认得。

### 5.2 mc（MinIO Client）

```bash
# 用隔离的配置目录，避免污染开发机上已有的 ~/.mc
mc --config-dir /tmp/mc-demo alias set rs http://127.0.0.1:9000 rustorage rustorage-secret

mc --config-dir /tmp/mc-demo mb rs/demo
mc --config-dir /tmp/mc-demo cp ./photo.jpg rs/demo/photo.jpg
mc --config-dir /tmp/mc-demo ls rs/demo/
mc --config-dir /tmp/mc-demo cat rs/demo/photo.jpg > ./photo-back.jpg
mc --config-dir /tmp/mc-demo rm rs/demo/photo.jpg
```

> mc 的写法是 `别名/桶`（**斜杠、无冒号**），和 rclone 刻意不同，别记混。
> `mc rm` 即便只删一个对象也会走批量 `POST ?delete=`。

### 5.3 rclone

```bash
# 用环境变量定义 remote，不写 ~/.config/rclone/rclone.conf
export RCLONE_CONFIG_RS_TYPE=s3
export RCLONE_CONFIG_RS_PROVIDER=Minio
export RCLONE_CONFIG_RS_ENDPOINT=http://127.0.0.1:9000
export RCLONE_CONFIG_RS_ACCESS_KEY_ID=rustorage
export RCLONE_CONFIG_RS_SECRET_ACCESS_KEY=rustorage-secret
export RCLONE_CONFIG_RS_FORCE_PATH_STYLE=true

rclone mkdir rs:demo
rclone copy ./photo.jpg rs:demo/ --no-update-modtime
rclone ls rs:demo
rclone copyto rs:demo/photo.jpg ./photo-back.jpg   # 注意是 copyto
rclone check ./local-dir rs:demo --one-way
```

> **`rs:demo` 里的冒号不能省。** 写成 `rs/demo` 会被 rclone 当作**本地相对目录**：
> 全程不碰服务端，还会静默建出一个同名本地目录、退出码 0。
> 同理，`rclone copy <文件> <路径>` 把目标当**目录**，文件→文件要用 **`copyto`**。

> **为什么要加 `--no-update-modtime`：** 当目标是**已存在且大小相同**的对象时，rclone
> 判定为「内容已有、只是修改时间对不上」，于是它不重传，而是发一个 `CopyObject`
> （自拷贝、只替换 metadata）去修 mtime。MVP 没有实现 `CopyObject`（返回
> `501 NotImplemented`），rclone 因此报错并以**退出码 1** 结束——**尽管对象内容其实
> 已经是对的**。实测：目标不存在时 `rclone copy` 退出码 0；目标已存在（同大小）时
> 退出码 1，日志里出现 `Failed to set modification time ... CopyObject ... NotImplemented`。
> 加上 `--no-update-modtime` 即可跳过这一步。

### 5.4 boto3

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://127.0.0.1:9000",
    aws_access_key_id="rustorage",
    aws_secret_access_key="rustorage-secret",
    region_name="us-east-1",
)

s3.create_bucket(Bucket="demo")
s3.put_object(Bucket="demo", Key="hello.txt", Body=b"hello rustorage")
print(s3.get_object(Bucket="demo", Key="hello.txt")["Body"].read())  # b'hello rustorage'

for obj in s3.list_objects_v2(Bucket="demo").get("Contents", []):
    print(obj["Key"], obj["Size"])
```

> boto3 未纳入仓库的冒烟脚本，上面是常规用法而非实测记录；如果踩到坑，欢迎补进
> `tests/compat/`。

### 5.5 运维端点

| 路径 | 行为 |
|---|---|
| `GET /health` | 进程活着就 `200`（不看就绪状态），适合做 liveness probe |
| `GET /ready` | 存储层就绪后 `200`；之前 `503` + `Retry-After: 5`，适合做 readiness probe |
| `GET /metrics` | Prometheus 文本格式。**需启动时带 `--metrics`**，否则返回 `200` 但正文为空 |
| `GET /_console/` | 只读控制面板。**需启动时带 `--console`**，见 [§6.5](#65-控制面板可选) |

```bash
curl -i http://127.0.0.1:9000/health
curl -i http://127.0.0.1:9000/ready
curl -s http://127.0.0.1:9000/metrics
```

这些路径都是**精确匹配**：`/metrics/`（带尾斜杠）不会被当成指标端点，而是走进 S3 的
请求路径——未签名时先撞上 SigV4 认证，返回 `403 AccessDenied`（签名后才会是
`404 NoSuchBucket`）。

`/_console/` 在没有 `--console` 时的表现**与 `/metrics/` 不同**：它返回 `400
InvalidBucketName`。因为 `_console` 首字符是下划线，s3s 在**路径解析阶段**就判它
不是合法桶名，请求根本走不到认证那一步。两种表现都说明同一件事：面板没开关时
不会被截获，请求原样落到 S3 层。

## 6. 容错能力与 quorum

`N` 块盘 = `data + parity`，每块盘一份分片。`--parity` 不指定时按下表推导
（`crates/store/src/set.rs` 的 `default_parity`）：

| 盘数 `N` | 默认 `parity` | `data` | 读 quorum | 写 quorum | 可容忍掉盘（读） |
|---|---|---|---|---|---|
| 4 | 2 | 2 | 2 | 3 | 2 |
| 5 | 2 | 3 | 3 | 3 | 2 |
| 6 | 3 | 3 | 3 | 4 | 3 |
| 7 | 3 | 4 | 4 | 4 | 3 |
| 8 | 4 | 4 | 4 | 5 | 4 |

公式（`set.rs`）：

- `read_quorum  = N - parity`
- `write_quorum = data`，但当 `data == parity` 时为 `data + 1`（保证 `读 + 写 > N`）
- `delete_quorum = N / 2 + 1`

**两个数量都要满足，操作才成功**；任一不足即返回 `503 ServiceUnavailable`，
而**绝不会**返回残缺或错误的数据——宁可报错，也不给坏数据。

已在 **6 块盘、`--parity 2`（即 4+2）** 上端到端验证过：

- 掉 2 块（剩 4 = 读 quorum = 写 quorum）→ **读成功、写成功，内容逐字节一致**；
- 再掉 1 块（剩 3 < 4）→ **读失败**，返回 `503`。

复现这套验收（会把服务起在 9000 端口，需要 `aws` / `curl` / `cmp`）：

```bash
bash tests/acceptance.sh          # 期望最后一行输出 ACCEPTANCE: OK
```

客户端兼容冒烟（需要一个已在跑的实例）：

```bash
bash tests/compat/aws_cli.sh
bash tests/compat/mc.sh
bash tests/compat/rclone.sh
```

## 6.5 控制面板（可选）

加 `--console` 启动后，浏览器打开 <http://127.0.0.1:9000/_console/> 就能看到只读面板：
桶列表、目录式对象浏览、对象详情与下载、服务状态与指标。

- **只读**：本版没有上传、删除、建桶——面板不提供任何写操作；
- **凭据在浏览器端**：登录时输入的 access/secret key **只存在页面内存里，刷新即丢**，
  要重新登录；签名（SigV4）在浏览器里现算，服务端不经手；权限与 `aws-cli` / `mc` 完全一致；
- **登录不预检权限**：填完凭据直接进概览，由概览页的错误卡报出真实原因。
  这样可以区分「凭据错」和「凭据对但权限不够」——IAM 用户往往能登录却没权限；
- **侧栏显示当前 access key**（access key 是标识不是秘密，它随每个请求的
  `Authorization` 头明文出去；secret 在页面上没有任何出口）；
- **列不出桶时会给出路**：`ListBuckets` 是**账号级**操作，只被授予某个桶权限的
  IAM 用户必然拿到 403。此时侧栏如实显示「无权列出桶」而不是「还没有桶」，
  概览页多出一个输入框，直接输入桶名就能进那个桶；
- **只在 `127.0.0.1` 可用**：签名依赖 `crypto.subtle`，而它要求可信来源。
  服务端目前把监听地址写死为 `127.0.0.1`，所以成立；**一旦允许绑别的地址或改用
  局域网 IP 访问，面板必须先上 TLS**；
- 交互上有搜索过滤、连续翻页、对象详情走右侧抽屉（`Esc` 关闭、焦点归还）、
  一键复制 key/ETag；对象行可用键盘 `Tab` 聚焦、`Enter` 打开；
- 指标页需要服务端同时以 `--metrics` 启动，否则计数部分是空的。

界面的硬约束是**零构建链 + CSP `default-src 'self'`**：没有框架、没有 npm、没有 CDN，
样式一律走 `style.css` 的类名（内联 `style` 会被浏览器**静默**拒掉）。
`tests/console.sh` 里有一条 `grep` 护栏盯着这件事。

这不是 MinIO 的 Console 协议（那套 RPC 契约本项目依然不做），只是一个挂在既有路由上的
静态页面。`/_console` 之所以不会撞上任何桶，是因为 s3s 的桶名校验不允许下划线
（详见 [DESIGN §18.4](docs/DESIGN.md)）。

## 7. 已知限制

**这是「刻意不做」的清单，不是待办列表。** 每条都能在代码里找到对应的拒绝路径。
完整的推导与「日后要补时该动哪里」见 [`docs/MVP.md` 的限制表](docs/MVP.md)。

| 限制 | 表现 |
|---|---|
| **不支持 multipart** | 六个 multipart 操作一律 `501 NotImplemented`。**连带把单次 PUT 限制在约 8 MiB**，因为 aws-cli 等客户端超阈值会自动改走分片上传 |
| **未实现的 S3 操作是「静默 501」** | s3s 的每个 trait 方法默认就返回 501，所以「漏实现」和「刻意不支持」在响应上长得一样。判据：`Message` 里带 `is not implemented yet` 的是上游默认实现 |
| **`CopyObject` 未实现** | 服务端复制类操作一律 `501`。**rclone 会踩到**：更新已存在对象的 mtime 时它发 `CopyObject`，于是「目标已存在且大小相同」的上传以退出码 1 收场（**内容其实已正确**）。规避见 [§5.3](#53-rclone) |
| **条件请求只覆盖 GET / HEAD** | `PUT` 带 `If-None-Match: *`（条件创建）**不求值**，按普通 PUT 处理并真的覆盖；`If-Range` 不支持 |
| **某些对象 key 直接 400** | 含空段 / `.` / `..` / 首尾斜杠的 key（如 `a//b`、`dir/`）一律 `400 InvalidArgument`，**与 AWS 行为不同**（AWS 把它们当成独立 key）。实际会撞上的是「目录占位对象」 |
| **`ListObjects` 是全盘遍历** | 没有索引，大数据集上很慢；分页只在 S3 层做 |
| **无并发锁** | 同一 key 上的两个并发 PUT **不保证先后**。`.staging-*` + rename 保证看不到半成品，但不排序 |
| **无背压** | 突发大并发下内存与磁盘队列无界增长 |
| **单节点** | 无多节点、无自动 heal。`Corrupt` 会被检出、记录、参与 quorum，但不会自动修复 |
| **只绑 `127.0.0.1`、无 TLS、不支持 HTTP/2** | MVP 只服务本机 HTTP/1.1。四个目标客户端默认都走 HTTP/1.1，故不构成功能缺口 |
| **错误 XML 只有 `Code` + `Message`** | 没有 `Resource` / `RequestId`（上游序列化器的限制） |
| **虚拟主机寻址默认关闭** | 要按 `Host: bucket.example.com` 寻址必须显式传 `--base-domain`；开了之后，任何非 base、非 IP 的 host 会被整体当作桶名 |
| **默认凭据是硬编码的** | `rustorage` / `rustorage-secret`，务必在对外前改掉 |
| **没有组与外部身份源** | 用户只能一个个建，不能建组、不能接 OIDC / LDAP，也没有管理 API（改配置 = 改文件 + 重启） |
| **IAM 不覆盖运维端点** | `/health` `/ready` `/metrics` `/_console` 不走鉴权，见 [§4.5](#45-身份与权限iam) |

## 8. 开发

Crate 分层（只允许向下依赖，由 `scripts/check-layer-deps.sh` 在 CI 强制）：

```
rstore-server        二进制入口，唯一的装配点
 ├── rstore-s3         s3s 的 S3 trait 实现
 ├── rstore-store      引擎核心：ErasureSet / 读写路径 / quorum / 提交
 │    ├── rstore-disk      DiskAPI、LocalDisk、fsync 原语
 │    ├── rstore-erasure   纠删编解码与 codec 缓存
 │    ├── rstore-meta      format.json 与 meta.xl 容器
 │    └── rstore-checksum  keyed BLAKE3 校验
 └── rstore-api        契约 trait（不依赖任何实现）
```

提交前把 CI 跑的那几道门禁在本地过一遍：

```bash
cargo fmt --all --check
cargo build --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
bash scripts/check-layer-deps.sh
python scripts/tests/test_check_layer_deps.py
```

端到端验收脚本（各自起一个实例，需要 `curl`；`console.sh` 还会用到可选的 `node`）：

```bash
bash tests/acceptance.sh   # 纠删码容错与 quorum 边界
bash tests/console.sh      # 控制面板（见 §6.5）
```

## 9. 许可

MIT（见工作区 `Cargo.toml` 的 `license` 字段）。
