# Rustorage MVP 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: 使用 `superpowers:subagent-driven-development`（推荐）
> 或 `superpowers:executing-plans` 逐任务执行本计划。步骤采用 `- [ ]` 复选框语法以便跟踪进度。

**目标：** 实现一个单进程、多块本地盘的 S3 兼容对象存储，具备完整的纠删编码/解码、
读写 quorum、bitrot 校验、同盘元数据容器与 rename 提交协议。

**架构：** 参照 `docs/DESIGN.md`。物理层级为 `Node → Pool → ErasureSet → Disk → Shard`；
元数据以 `meta.xl` sidecar 与分片同盘存放；一致性由「读写 quorum + 目录 rename 提交」保证。

**技术栈：** Rust 2021、`tokio`、`s3s`（S3 协议）、`hyper`/`tower`、`reed-solomon-simd`（纠删码）、
`blake3`（bitrot）、`rmp-serde`（元数据编码）、`proptest`（属性测试）、`thiserror`。

**验收标准（MVP 完成的定义）：** `aws-cli` 与 `mc` 能对 **6 盘、`4+2`** 纠删配置的实例完成
bucket 创建、对象 CRUD、Range 读取、Multipart 上传与列出；拔掉任意 2 块盘后读操作仍成功
（`6-2 = 4 = read_quorum`），此时写操作也仍成功（`write_quorum = 4`）；
拔掉 3 块盘后读返回 `ErasureReadQuorum` 而**不是**错误数据；损坏 1 块盘上的字节后
读取能返回正确数据并记录到修复队列。

> 术语约定：`N+parity` 中的 N 指**数据分片数**，总盘数为 `N + parity`。
> `4+2` 因此需要 6 块盘——盘数与分片数一一对应，每块盘持有一份分片。

---

## 里程碑总览

| 里程碑 | 内容 | 产出 | 依赖 |
|---|---|---|---|
| **M0** | 项目骨架与架构护栏 | 可编译的空 workspace + 依赖检查脚本 | — |
| **M1** | 基础原语：校验和、分布排列、纠删码 | 三个纯函数 crate，属性测试通过 | M0 |
| **M2** | 元数据容器 `meta.xl` | 编码/解码 + 损坏防御 | M1 |
| **M3** | 盘抽象 | `DiskAPI` + `LocalDisk` + `FaultyDisk` | M2 |
| **M4** | 存储引擎核心 | 对象 PUT/GET/DELETE + **桶操作与对象列举** + quorum + 提交协议 | M3 |
| **M5** | S3 接入 | s3s 的 `S3` trait 实现 + 兼容层 | M4 |
| **M6** | 运维面与验收 | readiness、metrics、启动编排、端到端 | M5 |

**开发节奏：** 每个 Task 走 TDD 循环（写失败测试 → 跑失败 → 最小实现 → 跑通过 → 提交）。
每个 Task 结束时仓库必须处于可编译、测试全绿的状态。

---

## 文件结构

在开始写代码前锁定。**后续所有 Task 的路径以此为准。**

```
Cargo.toml                          # workspace 根
rust-toolchain.toml                 # 固定工具链版本
.gitattributes                      # 强制 LF，否则 shell 脚本在 Windows checkout 后失效
.github/workflows/ci.yml            # 护栏 + lint + test
scripts/check-layer-deps.sh         # M0 依赖方向护栏（shell 包装）
scripts/check_layer_deps.py         # 护栏策略与检查逻辑
scripts/tests/test_check_layer_deps.py  # 护栏自身的回归测试

crates/common/
  src/lib.rs
  src/error.rs                      # DiskError / CorruptKind / TransientKind / FatalKind
  src/id.rs                         # DeploymentId / DiskId / DataDirId（16 字节 UUID 包装）
  src/config.rs                     # 全局配置模型（唯一定义处）
  src/consts.rs                     # BLOCK_SIZE / HASH_LEN / MAX_SHARDS / RESERVED_PREFIX 等

crates/checksum/
  src/lib.rs                        # bitrot_hash / bitrot_size / KAT

crates/erasure/
  src/lib.rs                        # Codec 门面（encode/decode）
  src/cache.rs                      # 编解码器 LRU 缓存
  src/error.rs                      # ErasureConstructionError / ErasureError

crates/meta/
  src/lib.rs
  src/distribution.rs               # 分片分布排列
  src/container.rs                  # meta.xl 编解码
  src/fileinfo.rs                   # ObjectMeta / ShallowVersion / FileVersionHeader / ObjectBody
  src/inline.rs                     # 内联数据帧
  src/format.rs                     # format.json 模型
  src/keys.rs                       # 元数据键常量（x-rs-*）

crates/disk/
  src/lib.rs                        # DiskAPI trait
  src/local.rs                      # LocalDisk
  src/fsx.rs                        # fsync / rename / walk 原语
  src/error_map.rs                  # std::io::Error → DiskError

crates/store/
  src/lib.rs
  src/pool.rs                       # Pool（持有 Vec<ErasureSet>）
  src/set.rs                        # ErasureSet（读写路径入口）
  src/writer.rs                     # BitrotShardWriter / MultiWriter
  src/reader.rs                     # BitrotShardReader / ParallelReader
  src/put.rs                        # PUT 路径
  src/get.rs                        # GET 路径（含 resolve_version / Resolved）
  src/delete.rs                     # DELETE 路径 + gc_superseded
  src/commit.rs                     # rename 提交协议
  src/quorum.rs                     # quorum 规则与元数据仲裁
  src/error.rs                      # StoreError（**不是** errs.rs，也**不是** reduce_errs）
  src/bucket.rs                     # 桶操作（Task 4.11）
  src/list.rs                       # 对象列举（Task 4.11）
  src/reconcile.rs                  # 对账（Task 4.10）
  src/quorum_boundaries.rs          # quorum 边界矩阵（Task 4.9，#[cfg(test)]）
  src/testutil.rs                   # 测试夹具（#[cfg(test)]）

crates/api/
  src/lib.rs                        # ObjectStore trait + 传输类型（ByteRange / ObjectInfo / …）
  src/error.rs                      # ApiError

crates/s3/
  src/lib.rs                        # s3s S3Service 装配（构造时接收 Arc<dyn ObjectStore>，见 DESIGN §5 R4）
  src/impl_s3.rs                    # impl S3 for RstoreFs —— 只持有 api trait，不依赖 rstore-store
  src/errors.rs                     # ApiError → S3 错误码
  src/validate.rs                   # 对象 key 校验：保留前缀 + 盘上会碰撞的 key 形状（Task 5.7）
  src/conditional.rs                # HTTP 条件请求求值（Task 5.10）
  src/mock.rs                       # 内存版 ObjectStore，仅供测试（#[cfg(test)]，Task 5.2）
  # 没有 auth.rs：凭证走 s3s::auth::SimpleAuth::from_single，见 Task 5.1
  # 没有 host.rs：虚拟主机寻址用 s3s 的 SingleDomain，见 Task 5.11

crates/s3-compat/
  src/lib.rs                        # compat 中间件栈（按 §DESIGN 15.3 准入）

crates/server/
  src/lib.rs                        # 可测试的服务装配（供集成测试调用）；/health /ready /metrics
                                    #   的**路由挂载**在这里——与 S3 API 共用同一个端口
  src/main.rs                       # 瘦二进制入口，只调 lib（Task 6.3 创建）
  src/wiring.rs                     # 组合根：把 rstore-store 的实现绑定到 rstore-api trait（DESIGN §5 R4）
  src/startup.rs                    # 启动编排
  src/readiness.rs                  # SystemStage 与 /health /ready 的处理
  src/metrics.rs                    # Prometheus 文本格式
  src/config.rs                     # 命令行参数 → Config（**不是** config_load.rs：不读配置文件）

# 集成测试放在各自 crate 的 tests/ 下 —— 根目录是虚拟 workspace（无 [package]），
# 根 tests/ 不会被 cargo 编译。根 tests/ 只放 shell 脚本。
crates/disk/tests/
  faulty_disk.rs                    # Task 3.4
crates/s3/tests/
  compat_smoke.rs                   # Task 5.9 的 Rust 侧冒烟

# **store 没有 tests/ 目录**：Task 4.9 / 4.10 原计划把用例放在
# crates/store/tests/ 下，但集成测试是独立编译的 crate，看不到内部的
# #[cfg(test)] mod testutil。两者都改成了 src/ 内的 #[cfg(test)] 模块
# （quorum_boundaries.rs / reconcile.rs），见各自开头的说明。
tests/acceptance.sh                 # 仓库根，M6 的端到端验收驱动
tests/compat/                       # 仓库根，仅 shell 脚本，不参与 cargo 编译
  aws_cli.sh
  mc.sh
  rclone.sh
```

**设计单元边界：** 每个文件单一职责。`store/` 下按**操作**分文件（put/get/delete）而非按层，
因为这些操作各自改动时天然一起变。`meta/` 下按**关注点**分（容器格式 / 数据模型 / 分布算法），
因为它们被不同的调用方消费。

**新增 crate 的固定动作：** 在 `crates/` 下新建一个 crate 时，护栏会立刻以
`UNKNOWN CRATE` 拦下它（这是刻意的），但另外两项加固**不会**自动继承，必须手动补：

1. `Cargo.toml` 的 `[package]` 段加 `publish.workspace = true`
2. `Cargo.toml` 末尾加 `[lints] workspace = true`
3. 在 `scripts/check_layer_deps.py` 的 `ALLOWED` 表中登记它的允许依赖
   （若它有内部依赖，记得按传递闭包补齐，否则闭环自检会以退出码 2 报错）

---

## M0 — 项目骨架与架构护栏

### Task 0.1: 建立 workspace 与 crate 骨架

**Files:**
- Create: `Cargo.toml`
- Create: `rust-toolchain.toml`
- Create: `crates/common/Cargo.toml`、`crates/common/src/lib.rs`
- Create: `crates/{checksum,erasure,meta,disk,store,api,s3,s3-compat,server}/Cargo.toml` 与各自的 `src/lib.rs`
- Test: `cargo build` 通过

- [ ] **Step 1: 写 workspace 根 Cargo.toml**

```toml
[workspace]
resolver = "2"
members = ["crates/*"]

[workspace.package]
edition = "2021"
version = "0.1.0"
license = "MIT"

[workspace.dependencies]
# 内部
rstore-common   = { path = "crates/common" }
rstore-checksum = { path = "crates/checksum" }
rstore-erasure  = { path = "crates/erasure" }
rstore-meta     = { path = "crates/meta" }
rstore-disk     = { path = "crates/disk" }
rstore-store    = { path = "crates/store" }
rstore-api      = { path = "crates/api" }
rstore-s3       = { path = "crates/s3" }
rstore-s3-compat = { path = "crates/s3-compat" }

# 外部
tokio = { version = "1", features = ["full"] }
thiserror = "2"
anyhow = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
bytes = "1"
blake3 = "1"
crc32c = "0.6"
rmp-serde = "1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
uuid = { version = "1", features = ["v4", "serde"] }
reed-solomon-simd = "3"
proptest = "1"
tempfile = "3"
futures = "0.3"
async-trait = "0.1"
lru = "0.12"
```

> `proptest` / `tempfile` 目前暂无引用方，它们会在 M1/M3 作为 `[dev-dependencies]` 加入。
> 这是有意保留的，不是遗漏。

- [ ] **Step 2: 写 rust-toolchain.toml**

```toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 3: 为每个 crate 建最小骨架**

每个 crate 的 `Cargo.toml` 形如：

```toml
[package]
name = "rstore-common"
edition.workspace = true
version.workspace = true

[dependencies]
thiserror.workspace = true
```

每个 `src/lib.rs` 先只放一行注释说明职责：

```rust
//! 基础类型、错误模型、配置。不得依赖任何其他内部 crate。
```

`crates/common` 额外加 `serde`、`uuid` 依赖；`crates/store` 加 `tokio`、`futures`；
`crates/erasure` 加 `reed-solomon-simd`、`lru`；`crates/checksum` 加 `blake3`。

- [ ] **Step 4: 验证编译**

Run: `cargo build --workspace`
Expected: `Finished` 且无 warning

- [ ] **Step 5: 提交**

```bash
git add Cargo.toml rust-toolchain.toml crates/ .gitignore
git commit -m "chore: scaffold workspace with per-domain crates

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 0.2: 依赖方向护栏 + workspace 清单加固

**Files:**
- Create: `scripts/check-layer-deps.sh`
- Create: `scripts/check_layer_deps.py`
- Create: `scripts/tests/test_check_layer_deps.py`
- Create: `.gitattributes`
- Modify: `.gitignore`（忽略 `__pycache__/`、`*.pyc`）
- Modify: `Cargo.toml`（`[workspace.package]` 加 `publish = false`；新增 `[workspace.lints]`）
- Modify: 10 × `crates/*/Cargo.toml`（各加 `[lints] workspace = true` 与 `publish.workspace = true`）
- Modify: `rust-toolchain.toml`（固定版本）

- [ ] **Step 1: 写**白名单**护栏脚本**

**不要写成黑名单。** 黑名单（「这些边禁止」）有两个缺陷：新增跨层依赖时默认放行，
以及看不见传递违规。改为**白名单**：为每个内部 crate 声明它允许直接依赖的内部 crate 集合。

集合**已按传递闭包补齐**，因此只要每条直接边都在白名单内，就不可能存在
「`api → X → store`」这类绕道违规——闭包完备性由本脚本的静态表保证。
新增 crate 或新增跨层依赖都必须显式修改本表，改动会出现在 review diff 里。

产出**两个**文件：shell 包装（找解释器、调 cargo）与 Python 检查器（装策略）。
拆开是必需的——塞在 heredoc 里的程序无法单独运行，也就无法回归测试，
而它本身就是其余一切测试基础设施的看门人。

**1a. `scripts/check-layer-deps.sh`（shell 包装）**

```bash
#!/usr/bin/env bash
# 校验 DESIGN §5 的依赖方向规则（R1–R4）。策略在 check_layer_deps.py。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 解释器探测：必须真正执行一段程序并核对输出，退出码不可信。
# Windows 上 python3 可能是 Store/MSIX 别名：`python3 --version` 正常、
# `python3 -c ''` 返回 0，但 `python3 -c 'print(1)'` 却是 Permission denied(126)。
# 所以判据是「跑得出 1」，而不是「命令存在」或「退出码为 0」。
PYTHON=""
for cand in python3 python; do
    if command -v "$cand" >/dev/null 2>&1 \
       && [ "$("$cand" -c 'print(1)' 2>/dev/null)" = "1" ]; then
        PYTHON="$cand"
        break
    fi
done
if [ -z "$PYTHON" ]; then
    echo "ERROR: 未找到可用的 python3/python 解释器" >&2
    exit 2
fi

# 注意：管道右侧的命令绝不能带 heredoc 重定向。
# `cargo metadata ... | python3 - <<'PY'` 是错的——管道虽先绑定 fd 0，
# 但同一条命令上的 heredoc 会覆盖它，于是 python 读到的是程序而非 JSON。
# `set -o pipefail` 让 cargo 失败时整条管道失败；检查器的 2 号退出码
# 再把「护栏自身跑不起来」与「发现违规」区分开。
#
# 左侧保持在工作区根目录执行（cargo 需要它）。右侧把 SCRIPT_DIR 作为参数
# 传给原生 python 时，Git Bash 会做 POSIX→Windows 路径转换，
# 因此 Windows 上也能跑通。若日后报 “No such file”，
# 说明该转换失效，改用 `(cd "$SCRIPT_DIR" && "$PYTHON" ./check_layer_deps.py)`。
cargo metadata --format-version 1 --no-deps | "$PYTHON" "$SCRIPT_DIR/check_layer_deps.py"
```

**1b. `scripts/check_layer_deps.py`（检查器）**

```python
#!/usr/bin/env python3
"""校验 DESIGN §5 的依赖方向规则（R1–R4）。

从 stdin 读 `cargo metadata --format-version 1 --no-deps` 的 JSON。

退出码：
  0  合规
  1  发现违规（未登记 crate，或跨层边）
  2  护栏自身无法工作（输入不是合法 JSON，或白名单未按传递闭包补齐）
     必须与 1 分开，否则 CI 日志会把基础设施故障读成「有人加了违规依赖」。
"""
import json
import os
import sys

ALLOWED = {
    "rstore-common":    set(),
    "rstore-checksum":  {"rstore-common"},
    "rstore-erasure":   {"rstore-common"},
    "rstore-meta":      {"rstore-common", "rstore-checksum"},
    "rstore-disk":      {"rstore-common", "rstore-meta", "rstore-checksum"},
    "rstore-store":     {"rstore-common", "rstore-checksum", "rstore-erasure",
                         "rstore-meta", "rstore-disk"},
    "rstore-api":       {"rstore-common"},
    "rstore-s3":        {"rstore-common", "rstore-api"},
    "rstore-s3-compat": {"rstore-common"},
    # 组合根：允许看见全部（DESIGN §5 R4 规定绑定实现只在这里发生）
    "rstore-server":    {"rstore-common", "rstore-checksum", "rstore-erasure",
                         "rstore-meta", "rstore-disk", "rstore-store",
                         "rstore-api", "rstore-s3", "rstore-s3-compat"},
}


def table_errors(table):
    """白名单必须自洽：按传递闭包补齐，且不引用未登记的名字。

    闭包：否则 `api → X → store` 这类绕道违规会溜过去——api 只直接依赖 X，
    看似合规，但 X 依赖 store，实际传递依赖已经越界。

    悬空引用：`ALLOWED[c]` 里出现了表中不存在条目。正常表里每个内部 crate
    都是键，所以这多半是拼写错误（`rstore-meta` 写成 `rstore-metaa`）。
    这种情况绝不能静默当叶子节点放过——那等于把一条依赖边从图上抹掉，
    护栏会转而"证明"一个不存在的结论。

    返回错误列表，空列表表示自洽。这里刻意不抛异常：任何未捕获的异常都会以
    退出码 1 结束，而 1 在本脚本里表示"发现违规"，正好是 docstring 警告的
    那种混淆。
    """
    errors = []
    for crate, deps in table.items():
        for reachable in deps:
            if reachable not in table:
                errors.append(
                    f"白名单引用了未登记的 crate：{crate} → {reachable}；"
                    f"表里没有 {reachable} 这一项（拼写错误？）"
                )
                continue
            extra = table[reachable] - deps
            if extra:
                errors.append(
                    f"白名单未闭包：{crate} → {reachable} → {sorted(extra)}；"
                    f"应把 {sorted(extra)} 并入 {crate} 的允许集合"
                )
    return errors


class MetadataShapeError(Exception):
    """cargo metadata 的结构和预期不符。

    单独定义一个类型，是为了让 main() 能精确接住"输入不是我们以为的东西"。
    靠枚举 KeyError/TypeError/AttributeError……是在猜自己已知的坏法，而契约
    要求的是**任何**坏法都归 2。check() 里所有结构断言统一抛它，main() 里再
    加一层 except Exception 兜底，这样"意外异常以 1 逃逸"在结构上不可能发生。
    """


def require(cond, what):
    """结构断言。不成立就抛 MetadataShapeError，由 main() 转成退出码 2。"""
    if not cond:
        raise MetadataShapeError(what)


def is_inside(path, root):
    """path 是否落在 workspace 根目录内。

    用来判"内部 crate"：判据是 workspace 边界，而不是"这个依赖带不带 path
    字段"。后者会把本地 fork 的外部 crate（`serde = { path = "../forks/serde" }`）
    一起卷进来——它同样带 path 字段，但显然不是内部 crate，报违规就是误伤。

    必须用 commonpath 而不是字符串前缀比较：`E:\\rust\\Rustorage-other` 以
    `E:\\rust\\Rustorage` 开头，但不在里面。commonpath 懂这一点。

    判断不了（字段缺失、相对路径、跨盘符）时返回 True，即按内部依赖处理。
    失效方向必须是 fail-closed：宁可多查一条边，也不要因为路径解析失败
    就静默放过一条依赖边。
    """
    if not isinstance(path, str) or not isinstance(root, str):
        return True
    if not os.path.isabs(path) or not os.path.isabs(root):
        return True                      # 相对路径无从比较；cargo 实际给绝对路径
    try:
        root = os.path.realpath(root)
        return os.path.commonpath([root, os.path.realpath(path)]) == root
    except ValueError:
        return True                      # 跨盘符等，判断不了


def check(meta):
    """返回违规消息列表。空列表表示合规。

    结构不符时抛 MetadataShapeError，而不是返回违规——「cargo 的输出看不懂」
    和「架构违规」必须分开报，前者是护栏故障（2），后者才是发现违规（1）。

    内部依赖的判据是"名字带 rstore- 前缀，或路径落在 workspace 根内"，不是
    "名字带前缀"：只按前缀过滤的话，一个放在 crates/ 之外、名字又没前缀的
    内部 crate 会同时漏掉 UNKNOWN CRATE（它不是 workspace 成员，--no-deps
    不列它）和这条边。
    """
    require(isinstance(meta, dict), f"顶层不是对象：{type(meta).__name__}")
    packages = meta.get("packages")
    require(isinstance(packages, list), f"packages 不是列表：{type(packages).__name__}")
    require(packages, "packages 为空——workspace 里应当有 crate")
    workspace_root = meta.get("workspace_root")

    violations = []
    for pkg in packages:
        require(isinstance(pkg, dict), "packages 的元素不是对象")
        name = pkg.get("name")
        require(isinstance(name, str), f"包的 name 不是字符串：{name!r}")

        if name not in ALLOWED:
            violations.append(
                f"UNKNOWN CRATE: {name} 未在护栏表中登记 —— 新增 crate 必须显式登记其允许依赖"
            )
            continue

        deps = pkg.get("dependencies")
        require(isinstance(deps, list), f"{name} 的 dependencies 不是列表")
        for dep in deps:
            require(isinstance(dep, dict), f"{name} 的依赖项不是对象")
            dep_name = dep.get("name")
            require(isinstance(dep_name, str), f"{name} 的依赖名不是字符串：{dep_name!r}")

            if not dep_name.startswith("rstore-"):
                # 不带前缀的依赖分三类：注册表依赖（无 path，比如 serde）、
                # 仓内路径依赖（内部 crate）、仓外路径依赖（本地 fork 的外部
                # crate）。只有中间那类受内部层次约束。
                dep_path = dep.get("path")
                if dep_path is None or not is_inside(dep_path, workspace_root):
                    continue
            if dep_name not in ALLOWED[name]:
                kind = dep.get("kind") or "normal"
                violations.append(f"FORBIDDEN EDGE: {name} -> {dep_name}  (kind={kind})")
    return violations


def main():
    errors = table_errors(ALLOWED)
    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        return 2

    # 明确按 UTF-8 解码，不用 sys.stdin。JSON 规范即要求 UTF-8，cargo 也按 UTF-8
    # 输出；而 sys.stdin 用的是**区域编码**（Windows 上是 GBK）。依赖它意味着
    # 解码行为随环境漂移，两种结果都是坏的：
    #   - 解不开：checkout 路径里出现「一」(U+4E00) 这类常见汉字，GBK 就会在
    #     E4 B8 80 的尾字节 0x80 上抛 UnicodeDecodeError；
    #   - 解得开：字节碰巧凑成合法 GBK 对，于是静默乱码。当前 check() 只读
    #     name/dependencies（纯 ASCII），乱码落在 manifest_path 之类字段上时
    #     没有可见症状——但「护栏读到的输入随本机区域设置漂移」本身就是缺陷，
    #     哪天多读一个字段就会变成误报或漏报。
    # 前者以退出码 1 结束，正是最容易被误读成「发现违规」的那种失败。
    # CI 在 Linux 上永远是 UTF-8，这类问题只在本机出现，最难发现。
    try:
        meta = json.loads(sys.stdin.buffer.read().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        print(f"ERROR: 无法解析 cargo metadata 输出：{e}", file=sys.stderr)
        return 2

    # 合法 JSON 不等于预期的结构。cargo 若改了输出格式，这里必须落回 2
    # （护栏故障），而不是让异常冒出去变成 1。
    #
    # 第二层 except Exception 是兜底，不是冗余：check() 内部已经用 require()
    # 显式断言了结构，但那段代码将来会长出新的字段访问。契约是"除真正的违规
    # 之外一律不返回 1"，能表达这个契约的只有"接住一切"——枚举异常类型永远
    # 慢一步，而漏掉的那种会以"发现违规"的假象出现在 CI 里。
    try:
        violations = check(meta)
    except MetadataShapeError as e:
        print(f"ERROR: cargo metadata 结构不符合预期：{e}", file=sys.stderr)
        return 2
    except Exception as e:                    # noqa: BLE001
        print(f"ERROR: 检查元数据时发生意外异常：{type(e).__name__}: {e}", file=sys.stderr)
        return 2

    for v in violations:
        print(v)
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 2: 验证脚本在当前（合规）状态下通过**

Run: `bash scripts/check-layer-deps.sh`
Expected: 退出码 0，无输出

- [ ] **Step 3: 写检查器的回归测试**

护栏是其余一切测试基础设施的看门人，它自己必须有自动化测试。人工"改一下再改回来"
的验证记录不算——没有东西会在后续变更时重跑它。

为什么用 fixtures 而不是真去改 `crates/*/Cargo.toml`：改真仓库既慢又会污染工作区，
而且测的是 cargo 而不是检查器。检查器的输入契约就是"stdin 上的 cargo metadata JSON"，
直接喂 JSON 即可。

新建 `scripts/tests/test_check_layer_deps.py`：

```python
#!/usr/bin/env python3
"""scripts/check_layer_deps.py 的回归测试。

用 subprocess 走真实接口（stdin 喂 JSON，断言退出码与输出），
测的是守卫本身而不是它的复刻。

运行：python3 scripts/tests/test_check_layer_deps.py
"""
import contextlib
import inspect
import io
import json
import os
import subprocess
import sys
import types
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
CHECKER = SCRIPTS_DIR / "check_layer_deps.py"
REPO_ROOT = str(SCRIPTS_DIR.parent)

sys.path.insert(0, str(SCRIPTS_DIR))
import check_layer_deps as chk  # noqa: E402


def pkg(name, *deps):
    return {"name": name, "dependencies": [{"name": d} for d in deps]}


# (用例名, stdin 载荷, 期望退出码, 期望出现在 stdout 的片段)
CASES = [
    ("合规输入", {"packages": [
        pkg("rstore-common"),
        pkg("rstore-checksum", "rstore-common"),
        pkg("rstore-api", "rstore-common"),
    ]}, 0, None),

    ("非法边 api -> store", {"packages": [
        pkg("rstore-api", "rstore-store"),
    ]}, 1, "FORBIDDEN EDGE: rstore-api -> rstore-store  (kind=normal)"),

    ("非法边 s3-compat -> store", {"packages": [
        pkg("rstore-s3-compat", "rstore-store"),
    ]}, 1, "FORBIDDEN EDGE: rstore-s3-compat -> rstore-store  (kind=normal)"),

    ("未登记 crate", {"packages": [
        pkg("rstore-scratch"),
    ]}, 1, "UNKNOWN CRATE: rstore-scratch"),

    ("外部依赖不算违规", {"packages": [
        pkg("rstore-common", "serde", "bytes", "tokio"),
    ]}, 0, None),

    ("未登记 crate 也要继续查其余包", {"packages": [
        pkg("rstore-scratch"),
        pkg("rstore-api", "rstore-store"),
    ]}, 1, "FORBIDDEN EDGE: rstore-api -> rstore-store"),

    # 路径依赖带着 path 字段，落在 workspace 内、名字又没前缀的，必须被约束——
    # 否则放在 crates/ 之外的内部 crate 两头都漏。不能写成 pkg(...)：那个辅助
    # 函数只造 {"name": ...}，造不出 path 字段。
    #
    # 路径由 REPO_ROOT 现算而不是写死，才能同时在本机和 Linux CI 上成立。
    ("仓内路径依赖绕过前缀过滤", {"workspace_root": REPO_ROOT, "packages": [
        {"name": "rstore-server", "dependencies": [
            {"name": "evilhelper",
             "path": str(Path(REPO_ROOT) / "tools" / "evilhelper")}]},
    ]}, 1, "FORBIDDEN EDGE: rstore-server -> evilhelper"),

    # 反面：本地 fork 的外部 crate 同样带 path 字段，但落在 workspace 之外，
    # 不是内部 crate。判据若只看"有没有 path"，这条会被误报成架构违规。
    ("仓外路径依赖（本地 fork）不算违规", {"workspace_root": REPO_ROOT, "packages": [
        {"name": "rstore-common", "dependencies": [
            {"name": "serde",
             "path": str(Path(REPO_ROOT).parent / "forks" / "serde")}]},
    ]}, 0, None),
]


def run_checker(payload):
    """跑一次检查器，返回 CompletedProcess。

    编码必须两端都钉死成 UTF-8。`text=True` 只让 Python 用**区域编码**解码子进程
    输出——Windows 上是 GBK。子进程若按另一种编码写，父进程解码失败后
    `proc.stdout` 会静默变成 `None`，测试随即以 `TypeError: argument of type
    'NoneType' is not iterable` 崩掉，看不出真正原因。这里的断言比对的是中文消息，
    所以编码必须确定，而不是碰巧两边一致。
    """
    stdin = "" if payload is None else json.dumps(payload)
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    return subprocess.run(
        [sys.executable, str(CHECKER)],
        input=stdin, capture_output=True, text=True,
        encoding="utf-8", env=env,
    )


def run_fixture_cases():
    """执行全部 fixture 用例，返回失败描述列表。供 main() 与 pytest 共用。"""
    failures = []
    for label, payload, want_code, want_substr in CASES:
        proc = run_checker(payload)
        problems = []
        if proc.returncode != want_code:
            problems.append(f"退出码 {proc.returncode}，期望 {want_code}")
        if want_substr is not None and want_substr not in (proc.stdout or ""):
            problems.append(f"stdout 缺少 {want_substr!r}（实际 {proc.stdout!r}）")
        if want_substr is None and want_code == 0 and (proc.stdout or "").strip():
            problems.append(f"合规输入不该有 stdout 输出，却有 {proc.stdout!r}")
        # 每个用例**至多**记一条：main() 的通过计数按「用例数 - 失败数」算，
        # 一个用例记多条会让计数变成负数或虚高。
        if problems:
            failures.append(f"{label}: {'；'.join(problems)}")
    return failures


def test_fixture_cases():
    """pytest 入口。

    fixture 用例原本直接写在 main() 里，那样 `pytest` 只会收集到几个 test_*
    函数，子进程用例一条都不跑——测试看着全绿，实测只覆盖了一小部分。
    整进一个 test_* 函数后，两种跑法覆盖同一批用例。
    """
    failures = run_fixture_cases()
    assert not failures, "\n".join(failures)


def test_metadata_shape_errors_exit_2():
    """结构不符必须一律归 2，逐条钉住。

    这些形状在真实 cargo 输出里不该出现，所以它们的作用是"cargo 改格式时
    及时报警"。关键在于报警方式：必须报成"护栏故障"(2)，不能报成
    "发现违规"(1)——后者会让人去翻 manifest，而真正出问题的是护栏自己。

    第一版只 except 了 (KeyError, TypeError)，漏掉了 dep name 非字符串时
    `int.startswith` 抛的 AttributeError——它带着完整 traceback 以 1 逃逸。
    所以现在 check() 用 require() 显式断言结构，main() 再加 except Exception
    兜底，两层各自独立成立。
    """
    shapes = [
        {},                                                    # 没有 packages
        {"packages": None},                                    # 类型不对
        {"packages": []},                                      # 空 workspace
        {"packages": [{}]},                                    # 包没有 name
        {"packages": [{"name": 123}]},                         # name 不是字符串
        {"packages": [{"name": "rstore-common"}]},             # 没有 dependencies
        {"packages": [{"name": "rstore-common",
                       "dependencies": [{"name": 123}]}]},     # 依赖名不是字符串
        {"packages": [{"name": "rstore-common",
                       "dependencies": [123]}]},               # 依赖项不是对象
    ]
    for shape in shapes:
        proc = run_checker(shape)
        assert proc.returncode == 2, (
            f"{shape} 期望退出码 2，实际 {proc.returncode}；stderr={proc.stderr!r}"
        )
        assert (proc.stdout or "").strip() == "", (
            f"{shape} 的诊断不该出现在 stdout：{proc.stdout!r}"
        )


def test_invalid_json_reports_on_stderr_only():
    """退出码 2 的路径必须只写 stderr。

    诊断若混进 stdout，CI 里会被当成违规清单。
    """
    proc = run_checker(None)
    assert proc.returncode == 2, f"期望退出码 2，实际 {proc.returncode}"
    assert (proc.stdout or "").strip() == "", f"stdout 必须为空，实际 {proc.stdout!r}"
    assert "无法解析" in (proc.stderr or ""), f"stderr 缺少诊断：{proc.stderr!r}"


def test_non_closed_table_makes_main_exit_2():
    """白名单不自洽时 main 必须以 2 退出（护栏故障），而不是 1（发现违规）。

    这条分支在 table_errors 之后、读 stdin 之前，所以 stdin 内容无关紧要。

    stderr 必须捕获：main 会往那里打诊断，不拦的话自测跑绿也会在终端上
    印出一行 `ERROR: 白名单引用了未登记的 crate`，看着像失败。
    """
    original_table, original_stdin = chk.ALLOWED, sys.stdin
    chk.ALLOWED = {"rstore-api": {"rstore-typo"}}
    sys.stdin = types.SimpleNamespace(buffer=io.BytesIO(b""))
    try:
        with contextlib.redirect_stderr(io.StringIO()) as err:
            code = chk.main()
    finally:
        chk.ALLOWED = original_table
        sys.stdin = original_stdin
    assert code == 2, f"期望退出码 2，实际 {code}"
    assert "rstore-typo" in err.getvalue(), f"stderr 应指出违规项：{err.getvalue()!r}"


def test_real_table_is_closed():
    """真实白名单必须自洽——这是「绕道违规不可能」这条论证的全部依据。"""
    assert chk.table_errors(chk.ALLOWED) == []


def test_closure_check_catches_non_closed_table():
    """未闭包的表必须被检出，否则检查器形同虚设。

    api → mid → store 是一条绕道：api 只直接依赖 mid，但 mid 依赖 store，
    实际传递依赖已经越界。三个名字都在表中登记，所以这是纯粹的闭包违规，
    不掺杂悬空引用。
    """
    bad = {
        "rstore-api": {"rstore-mid"},
        "rstore-mid": {"rstore-store"},
        "rstore-store": set(),
    }
    assert chk.table_errors(bad) != []


def test_dangling_reference_is_a_table_error():
    """允许集合里出现表中没有的名字，必须报错，既不能崩也不能放过。

    崩（KeyError）会以退出码 1 结束，被误读成「发现违规」；
    静默当叶子节点放过，等于把一条依赖边从图上抹掉，
    护栏会转而"证明"一个不存在的结论。
    """
    bad = {"rstore-api": {"rstore-typo"}}
    errs = chk.table_errors(bad)
    assert errs != [], "悬空引用必须被检出"
    assert any("rstore-typo" in e for e in errs), f"错误信息应指出该名字：{errs}"


def main():
    failures = run_fixture_cases()

    # 按 test_ 前缀**自动发现**，不手写清单。脚本方式才是 CI 的入口，手写清单
    # 漏掉一个测试就等于 CI 静默跳过它——而"被跳过"和"通过"在日志里长得一模
    # 一样，这正是最难发现的失败。skip 里只放那些已知会被重复执行的包装函数。
    #
    # test_fixture_cases 只是 run_fixture_cases 的 pytest 包装，放进来会把同一批
    # fixture 用例跑两遍、并让计数重复。它是唯一需要排除的。
    skip = {"test_fixture_cases"}
    unit_tests = [
        fn for nm, fn in sorted(globals().items())
        if nm.startswith("test_") and inspect.isfunction(fn) and nm not in skip
    ]
    for fn in unit_tests:
        try:
            fn()
        except Exception as e:                # noqa: BLE001
            # 放宽到 Exception，不是只接 AssertionError：一个签名不对的 test_
            # 函数（比如漏了参数）抛 TypeError 会带着 traceback 直接结束进程，
            # 汇总行都不打印，之前累积的失败也一并丢掉。这里要的是"记一笔、
            # 继续跑、最后汇总"。
            failures.append(f"{fn.__name__}: {type(e).__name__}: {e}")

    for f in failures:
        print(f"FAIL {f}")
    print(f"\n{len(CASES) + len(unit_tests) - len(failures)} 项通过，{len(failures)} 项失败")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
```

Run: `python3 scripts/tests/test_check_layer_deps.py`
Expected: 全部通过（`14 项通过，0 项失败`），退出码 0

- [ ] **Step 3b: 端到端确认——真仓库上护栏仍能抓到违规**

fixtures 测的是检查器；这一步测的是"包装脚本 + 真 cargo metadata"这条链路。

**注意改动位置**：必须把 `rstore-store.workspace = true` 加进对应清单的 `[dependencies]`
**表内**。这几个文件末尾现在是 `[lints]`，直接追加到文件尾会落进错误的表，
cargo 会报 `invalid type: boolean 'true', expected a string or map`。

**另注意**：撤销时不要用 `git checkout -- <文件>`——如果工作区还有本任务未提交的
`publish.workspace = true` 改动，那会把它们一并丢掉。手工删掉那一行。

用例 A —— 临时给 `crates/api/Cargo.toml` 的 `[dependencies]` 加 `rstore-store.workspace = true`：

Run: `bash scripts/check-layer-deps.sh`
Expected: `FORBIDDEN EDGE: rstore-api -> rstore-store  (kind=normal)`，退出码 1

**撤销**（确认 `git diff` 只剩 `publish` 那几行的预期改动）后，用例 B —— 临时给
`crates/s3-compat/Cargo.toml` 的 `[dependencies]` 加 `rstore-store.workspace = true`：

Run: `bash scripts/check-layer-deps.sh`
Expected: `FORBIDDEN EDGE: rstore-s3-compat -> rstore-store  (kind=normal)`，退出码 1

**撤销**后，用例 C —— 临时新建 `crates/scratch/`（含 `Cargo.toml` 与空的 `src/lib.rs`）：

Run: `bash scripts/check-layer-deps.sh`
Expected: `UNKNOWN CRATE: rstore-scratch 未在护栏表中登记`，退出码 1

三个用例全部**撤销**后，再跑一次确认回到退出码 0，且 `git status` 无残留。

- [ ] **Step 4: 清单加固**

在根 `Cargo.toml` 的 `[workspace.package]` 中加一行：

```toml
publish = false
```

> **只加这一行是不够的，而且失败方式很隐蔽。** Cargo 的 workspace 继承是**逐字段
> 显式 opt-in**：成员只有写了 `字段.workspace = true` 才会拿到 `[workspace.package]`
> 里的值。只在根部写 `publish = false`，cargo 实际解析出来的每个 crate 仍然是
> `publish = None`，也就是**可发布**。它比什么都不做更糟，因为它看起来像是设好了。
> 所以下面每个 crate 都必须同时加 `publish.workspace = true`。

在根 `Cargo.toml` 末尾新增共享 lint 配置：

```toml
[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
all = { level = "warn", priority = -1 }
await_holding_lock = "deny"
```

> `all = { level = "warn", priority = -1 }` 的 `priority` 不能省。`await_holding_lock`
> 本身属于 `all` 这个 lint group，而 Cargo **忽略表中的书写顺序**。两者同优先级时
> clippy 报 `lint_groups_priority`（同优先级下的设置二义），`-D warnings` 会把它升级成
> 硬错误，`clippy` 直接失败。降一档优先级才能让 `deny` 真正压住 group 的 `warn`。

> `await_holding_lock = "deny"` 是刻意的：本项目大量使用锁保护磁盘状态，
> 跨 `.await` 持有锁会静默造成死锁。让编译器替我们拦住它。

然后在**每一个** `crates/*/Cargo.toml` 末尾加：

```toml
[lints]
workspace = true
```

并在**每一个** `crates/*/Cargo.toml` 的 `[package]` 段内加：

```toml
publish.workspace = true
```

加完**必须验证**继承真的生效，而不是看着根部那行就放心：

Run: `cargo metadata --format-version 1 --no-deps | python3 -c "import json,sys; print({p['name']: p['publish'] for p in json.load(sys.stdin)['packages']})"`
Expected: 每个 crate 的 `publish` 都是 `[]`（空数组 = 禁止发布）。
若显示 `None`，说明继承没生效，`publish.workspace = true` 漏了或写错了位置。

把 `rust-toolchain.toml` 的 channel 从浮动的 `"stable"` 固定到已验证的版本：

```toml
[toolchain]
channel = "1.97.1"
components = ["rustfmt", "clippy"]
```

- [ ] **Step 5: 锁定 shell 脚本行尾**

本仓库 `core.autocrlf=true`。`scripts/check-layer-deps.sh` 若在 checkout 时被转成 CRLF，
`set -euo pipefail` 会带上一串 `\r`，bash 在每个词后面都报 `command not found`，
`#!/usr/bin/env bash` 这个 shebang 也会失效——守卫脚本在 Windows 上直接废掉。

新建 `.gitattributes`：

```
* text=auto eol=lf
```

`eol=lf` 让工作区、索引、CI 三处的行尾一致；`text=auto` 已含二进制自动识别，
无需再为 png/jpg 之类单列规则。加完确认脚本仍是 LF：

Run: `file scripts/check-layer-deps.sh`
Expected: 输出中不含 `CRLF`

在 `.gitignore` 里补上 Python 字节码缓存：

```
__pycache__/
*.pyc
```

自测脚本会 `import check_layer_deps`，Python 随即在 `scripts/` 下生成
`__pycache__/`。不忽略的话，一条 `git add scripts/` 就会把编译缓存提交进去。

- [ ] **Step 6: 验证加固后仍然全绿**

Run: `cargo build --workspace && cargo clippy --workspace --all-targets -- -D warnings`
Expected: 均通过，无 warning

Run: `cargo fmt --all -- --check`
Expected: 退出码 0（后续 CI 会跑这一步，先确认本地是干净的）

Run: `bash scripts/check-layer-deps.sh`
Expected: 退出码 0

Run: `python3 scripts/tests/test_check_layer_deps.py`
Expected: `14 项通过，0 项失败`，退出码 0

- [ ] **Step 7: 提交**

```bash
git add scripts/ .gitattributes .gitignore Cargo.toml rust-toolchain.toml crates/
git commit -m "chore: allowlist-based layer guard and workspace lint hardening

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 0.3: CI 接线

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: 说明为什么这一步不能省**

DESIGN §5 和 §19.5 都写着护栏"由 CI 强制"。在接线之前那句话是假的：
脚本存在但没有任何东西运行它，一条 `api → store` 的违规边可以静默合入，
除非恰好有人记得手动跑一次。整个任务的价值主张——「用工具而非自觉维持架构」——
只有在这一步之后才成立。

- [ ] **Step 2: 新建 `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  # 所有分支的 push 都跑：PR 触发只覆盖"开/更新 PR"这条路径，
  # 而"推上去先看看红不红"是更早、更常用的一环。限定 branches: [main]
  # 会让功能分支在开 PR 之前完全没有反馈。
  push:
  pull_request:

env:
  CARGO_TERM_COLOR: always

jobs:
  ci:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      # rust-toolchain.toml 固定了 1.97.1；rustup 会按它自动装好工具链。
      # 显式调一次让"装工具链"这一步在日志里可见，而不是混在 build 里。
      - name: 安装固定工具链
        run: rustup show

      - name: 缓存
        uses: actions/cache@v4
        with:
          path: |
            ~/.cargo/registry
            ~/.cargo/git
            target
          key: ${{ runner.os }}-cargo-${{ hashFiles('**/Cargo.lock') }}
          restore-keys: ${{ runner.os }}-cargo-

      # 护栏排在最前：架构违规要第一条报出来，不要等 build 跑完。
      # 解释器探测在 check-layer-deps.sh 里自己做，找不到会以退出码 2 硬失败。
      - name: 架构护栏（依赖方向）
        run: bash scripts/check-layer-deps.sh

      # 这里刻意不复用上面那个 shell 包装器：它跑的是护栏本身，不是自测。
      # 所以探测逻辑必须在这里重来一遍——直接写 `python3` 会把 Windows 上
      # 那套 Store 别名问题原样搬进 CI（该假设偶然成立一次，不代表成立）。
      - name: 护栏自测
        run: |
          PYTHON=""
          for cand in python3 python; do
              if command -v "$cand" >/dev/null 2>&1 \
                 && [ "$("$cand" -c 'print(1)' 2>/dev/null)" = "1" ]; then
                  PYTHON="$cand"; break
              fi
          done
          if [ -z "$PYTHON" ]; then
              echo "ERROR: 找不到可用的 Python 解释器" >&2
              exit 2
          fi
          "$PYTHON" scripts/tests/test_check_layer_deps.py

      - name: 格式
        run: cargo fmt --all -- --check

      # --locked 不能省：Cargo.lock 是提交进仓库的，缓存键也按它算。不带它的话
      # cargo 会在锁文件与 manifest 不一致时默默重新解析依赖，CI 照样绿——
      # 于是"锁定依赖"这件事在 CI 里从未被真正验证过。
      - name: 构建
        run: cargo build --workspace --all-targets --locked

      - name: Clippy
        run: cargo clippy --workspace --all-targets --locked -- -D warnings

      - name: 测试
        run: cargo test --workspace --locked
```

- [ ] **Step 3: 本地预演，避免推上去才发现 CI 红**

Run: `bash scripts/check-layer-deps.sh && python3 scripts/tests/test_check_layer_deps.py && cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings && cargo test --workspace --locked`
Expected: 全部退出码 0

YAML 语法本地校验（只解析，不执行）：

Run: `python3 -c "import pathlib, yaml; yaml.safe_load(pathlib.Path('.github/workflows/ci.yml').read_text(encoding='utf-8')); print('yaml ok')"`
Expected: `yaml ok`。若本机没有 PyYAML，跳过并在提交信息里注明未做语法校验，不要为此装依赖。
（`encoding='utf-8'` 不能省：Windows 上 `read_text()` 默认用区域编码 GBK，
读含中文注释的文件会抛 `UnicodeDecodeError`。）

- [ ] **Step 4: 提交**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: run layer guard, lint, and tests on every push and PR

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

> **本任务的边界**：护栏只能校验 crate 之间的依赖边。DESIGN §5 规则 R4 还有一半是
> 文件级约定（实现绑定只允许出现在 `rstore-server/src/wiring.rs`），
> 依赖图看不出这一点——那一半仍然靠 review。别把这个脚本当成 R4 的完整保险。

> **两处想清楚后留下的取舍**，都不是遗漏：
>
> 1. **解释器探测在两个地方各写了一遍**（`scripts/check-layer-deps.sh` 与
>    `ci.yml` 的自测步骤）。抽成 `scripts/find-python.sh` 再 source 能消除重复，
>    但会多一个文件、多一处 source 路径假设。两份拷贝漂移的代价不对称：
>    写坏的是本机，而 CI 跑在 ubuntu-latest 上 `python3` 必然存在，不会因此变红。
>    等第三处需要它时再抽。
> 2. **CI 只跑 `scripts/tests/test_check_layer_deps.py` 这一个文件**，同目录将来
>    新增的 `test_*.py` 不会被收集。现在只有一个测试文件，加一层"遍历目录"的
>    机制是为不存在的场景付复杂度。新增测试文件时记得同步 CI——这条写在这里，
>    就是因为那一天的作者多半不会想到。

---

## M1 — 基础原语

> 这三个 crate 是纯函数式的，不碰 IO，因此可以完全用属性测试覆盖。
> 它们是整个系统里唯一能被「数学证明」的部分，值得多花时间。

### Task 1.1: 分片分布排列

**Files:**
- Create: `crates/meta/src/distribution.rs`
- Modify: `crates/meta/src/lib.rs`
- Test: 同文件 `#[cfg(test)]` 模块

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn rejects_out_of_range_n() {
        assert!(distribution("k", 1).is_err());
        assert!(distribution("k", 17).is_err());
    }

    #[test]
    fn is_valid_distribution_rejects_malformed_slices() {
        // distribution() 输出的守门人，DESIGN §9.3 要求它绝不 panic。
        // 属性测试只喂 distribution() 的合法输出，拒绝路径完全没被覆盖——
        // 而畸形输入正是它唯一会被真正调用的场合，所以边界要在这里钉住。
        assert!(is_valid_distribution(&[1])); // 长度 1 合法
        assert!(is_valid_distribution(&[3, 1, 2])); // 循环移位合法
        assert!(!is_valid_distribution(&[])); // 空
        assert!(!is_valid_distribution(&[0])); // 0 不在 1..=len 内
        assert!(!is_valid_distribution(&[1, 1])); // 重复
        assert!(!is_valid_distribution(&[1, 3])); // 3 > len = 2
        let seventeen: Vec<u8> = (1..=17).collect();
        assert!(!is_valid_distribution(&seventeen)); // len > 16
    }

    proptest! {
        #[test]
        fn always_a_strict_permutation(key in ".*", n in 2u8..=16) {
            let d = distribution(&key, n).unwrap();
            prop_assert_eq!(d.len(), n as usize);
            prop_assert!(is_valid_distribution(&d));
            let mut sorted = d.clone();
            sorted.sort();
            prop_assert_eq!(sorted, (1..=n).collect::<Vec<_>>());
        }

        #[test]
        fn deterministic_for_same_input(key in ".*", n in 2u8..=16) {
            prop_assert_eq!(distribution(&key, n).unwrap(), distribution(&key, n).unwrap());
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-meta distribution`
Expected: 编译失败，`distribution` 未定义

- [ ] **Step 3: 实现**

```rust
//! 分片分布排列：把逻辑块号映射到物理槽位，使同一对象的各分片在盘间错开。
//!
//! 对应 DESIGN §9.3。算法固定为 CRC32C 旋转；**不实现多代算法**，
//! 稳定性由「deployment id 不可变」这一约束保证，而非算法版本兜底。

use rstore_common::error::PipeError;

/// 返回长度为 `n` 的排列，元素取值 `1..=n`。
/// `dist[k] - 1` 即逻辑块 `k` 的物理槽位下标。
pub fn distribution(object_key: &str, n: u8) -> Result<Vec<u8>, PipeError> {
    if !(2..=16).contains(&n) {
        return Err(PipeError::InvalidShardCount(n));
    }
    let n_usize = n as usize;
    let start = (crc32c::crc32c(object_key.as_bytes()) as usize) % n_usize;
    let mut d = Vec::with_capacity(n_usize);
    for k in 1..=n_usize {
        d.push(((start + k) % n_usize + 1) as u8);
    }
    Ok(d)
}

/// 校验 `d` 是 `1..=d.len()` 的严格排列。
/// **绝不 panic**：所有访问都经过范围检查（DESIGN §9.3）。
pub fn is_valid_distribution(d: &[u8]) -> bool {
    if d.is_empty() || d.len() > 16 {
        return false;
    }
    let mut seen: u32 = 0;
    for &x in d {
        if x == 0 || x as usize > d.len() {
            return false;
        }
        let bit = 1u32 << (x - 1);
        if seen & bit != 0 {
            return false;
        }
        seen |= bit;
    }
    seen == (1u32 << d.len()) - 1
}
```

在 `crates/common/src/error.rs` 中加上 `PipeError::InvalidShardCount(u8)` 变体。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p rstore-meta distribution`
Expected: 4 个测试全部 PASS

- [ ] **Step 5: 提交**

```bash
git add crates/meta/src/distribution.rs crates/meta/src/lib.rs crates/common/src/error.rs
git commit -m "feat(meta): shard distribution permutation with property tests

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 1.2: bitrot 校验和

**Files:**
- Create: `crates/checksum/src/lib.rs`
- Modify: `crates/checksum/Cargo.toml`（加 `[dev-dependencies] proptest.workspace = true`；
  `blake3.workspace = true` 已在 `[dependencies]` 里，无需再加）
- Test: 同文件 `#[cfg(test)]`

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// 钉住常量：首次运行时用 `cargo test kat_pin -- --nocapture` 打印实际值，
    /// 粘贴回 `KAT_EMPTY_V1`。作用是在依赖升级后立刻发现哈希行为变化。
    const KAT_EMPTY_V1: [u8; 32] = [0u8; 32];

    #[test]
    fn kat_pin() {
        let got = bitrot_hash(b"");
        if KAT_EMPTY_V1 == [0u8; 32] {
            println!("KAT_EMPTY_V1 = {:?}", got);
            return;
        }
        assert_eq!(got, KAT_EMPTY_V1, "bitrot hash behavior changed!");
    }

    #[test]
    fn deterministic() {
        assert_eq!(bitrot_hash(b"hello"), bitrot_hash(b"hello"));
    }

    #[test]
    fn detects_single_bit_flip() {
        let a = bitrot_hash(b"aaaaaaaa");
        let b = bitrot_hash(b"aaaaaaab");
        assert_ne!(a, b);
    }

    #[test]
    fn size_arithmetic() {
        assert_eq!(bitrot_size(0, 1024), 0);
        assert_eq!(bitrot_size(1, 1024), 32 + 1);
        assert_eq!(bitrot_size(1024, 1024), 32 + 1024);
        assert_eq!(bitrot_size(1025, 1024), 64 + 1025);
    }

    proptest! {
        #[test]
        fn size_is_monotonic(a in 0u64..1_000_000, b in 0u64..1_000_000) {
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            prop_assert!(bitrot_size(lo, 1024) <= bitrot_size(hi, 1024));
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-checksum`
Expected: 编译失败，`bitrot_hash` 未定义

- [ ] **Step 3: 实现**

```rust
//! bitrot 校验和。对应 DESIGN §11。
//!
//! 落盘格式为逐块交错 `[hash(32B)][data]`，由 `disk` 层的 writer/reader 负责，
//! 本 crate 只提供哈希与尺寸计算。

pub const HASH_LEN: usize = 32;

/// 具名 key 常量。**不得内联到调用点**——改变它会让所有既有数据校验失败。
///
/// 长度必须恰好 32：`"rustorage.bitrot.key.v1"` 是 23 字节，补 **9** 个 `\0`
/// 凑满 32。下面的编译期断言兜住数错 `\0` 的情况（数错会直接编译失败）。
pub const BITROT_KEY_V1: [u8; 32] = *b"rustorage.bitrot.key.v1\0\0\0\0\0\0\0\0\0";
const _: () = assert!(BITROT_KEY_V1.len() == 32);

pub fn bitrot_hash(block: &[u8]) -> [u8; HASH_LEN] {
    let mut h = blake3::Hasher::new_keyed(&BITROT_KEY_V1);
    h.update(block);
    *h.finalize().as_bytes()
}

/// 单个分片落盘后的字节数：每个 block 前加一个摘要。
pub fn bitrot_size(size: u64, shard_size: u64) -> u64 {
    if size == 0 {
        return 0;
    }
    let blocks = size.div_ceil(shard_size);
    blocks * HASH_LEN as u64 + size
}
```

> **已知边界**：`bitrot_size(size, 0)` 会因 `div_ceil` 除零 panic。调用方保证
> `shard_size > 0`（分片尺寸来自纠删码布局，恒为正），本层不做防御——MVP 阶段
> 记录在案，不额外加断言。

- [ ] **Step 4: 跑测试，并钉住 KAT**

Run: `cargo test -p rstore-checksum kat_pin -- --nocapture`
把打印出的数组粘贴进 `KAT_EMPTY_V1`，再次运行：

Run: `cargo test -p rstore-checksum`
Expected: 全部 PASS

- [ ] **Step 5: 提交**

```bash
git add crates/checksum/ Cargo.lock
git commit -m "feat(checksum): keyed blake3 bitrot hashing with pinned KAT

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 1.3: 纠删码门面

**Files:**
- Create: `crates/erasure/src/error.rs`
- Modify: `crates/erasure/src/lib.rs`（现只有一行 doc comment）
- Modify: `crates/erasure/Cargo.toml`（加 `[dev-dependencies] proptest.workspace = true`；
  `reed-solomon-simd` / `thiserror` / `lru` 已在 `[dependencies]` 里）
- Test: `crates/erasure/src/lib.rs` 的 `#[cfg(test)]`

- [ ] **Step 1: 定义接口（这就是契约，先写下来）**

```rust
/// 纠删码门面。对应 DESIGN §10。
/// 上层只见这个接口，不感知底层库（`reed-solomon-simd`）的存在。
/// 这样库被替换时，只有在 `encode`/`decode` 内部需要改动。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Codec {
    data: usize,
    parity: usize,
    shard_size: usize,
}

impl Codec {
    /// 校验几何合法性：`data >= 1`、`2 <= data+parity <= 16`、`parity >= 1`、
    /// `shard_size > 0` 且为偶数。
    pub fn new(data: usize, parity: usize, shard_size: usize) -> Result<Self, ErasureConstructionError>;

    pub fn data_shards(&self) -> usize;
    pub fn parity_shards(&self) -> usize;
    pub fn total_shards(&self) -> usize;

    /// 输入 `data` 个等长分片，输出 `parity` 个校验分片。
    pub fn encode(&self, data_shards: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, ErasureError>;

    /// 输入长度为 `total_shards` 的槽位数组（`None` 表示该槽位缺失），
    /// 输出全部 `data` 个数据分片。缺失数 > `parity` 时返回 `ErasureError::TooFewShards`。
    pub fn decode(&self, slots: &[Option<Vec<u8>>]) -> Result<Vec<Vec<u8>>, ErasureError>;
}
```

```rust
// crates/erasure/src/error.rs
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ErasureConstructionError {
    #[error(
        "invalid geometry: data={data} parity={parity} (need data>=1, 2<=data+parity<=16, parity>=1)"
    )]
    InvalidGeometry { data: usize, parity: usize },
    /// 分片长度必须为正且为偶数：后端在 GF(2^16) 上运算，按 2 字节符号处理，
    /// 奇数长度根本无法编码。放在构造期拒绝，避免拖到 `encode` 才报 Backend。
    #[error("shard_size must be > 0 and even (got {0})")]
    InvalidShardSize(usize),
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ErasureError {
    #[error("expected {expected} shards, got {got}")]
    WrongShardCount { expected: usize, got: usize },
    #[error("shards are not equal length")]
    UnequalShardLength,
    #[error("only {available} shards available, need {needed}")]
    TooFewShards { available: usize, needed: usize },
    #[error("codec backend error: {0}")]
    Backend(String),
}
```

- [ ] **Step 2: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn pad_chunks(payload: &[u8], count: usize, shard_size: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let lo = (i * shard_size).min(payload.len());
            let hi = ((i + 1) * shard_size).min(payload.len());
            let mut s = vec![0u8; shard_size];
            s[..hi - lo].copy_from_slice(&payload[lo..hi]);
            out.push(s);
        }
        out
    }

    #[test]
    fn rejects_invalid_geometry() {
        assert!(Codec::new(1, 0, 1024).is_err()); // parity >= 1
        assert!(Codec::new(17, 1, 1024).is_err()); // data+parity <= 16
        assert!(Codec::new(4, 0, 1024).is_err()); // parity >= 1
        assert!(Codec::new(4, 2, 0).is_err()); // shard_size > 0
        assert!(Codec::new(0, 2, 1024).is_err()); // data 必须 >= 1
        assert!(Codec::new(4, 2, 1025).is_err()); // shard_size 必须是偶数（GF(2^16)）
    }

    proptest! {
        /// 核心不变量：任意数据、任意几何，编码后再用任意 >= data 份分片解码，结果恒等。
        #[test]
        fn roundtrip_with_arbitrary_loss(
            data in 1usize..=8,
            parity in 1usize..=8,
            payload in prop::collection::vec(any::<u8>(), 1..2048),
            drop_mask in any::<u16>(),
        ) {
            prop_assume!(data + parity <= 16);
            let shard_size = 1024;
            let codec = Codec::new(data, parity, shard_size).unwrap();
            let originals = pad_chunks(&payload, data, shard_size);
            let checks = codec.encode(&originals).unwrap();
            prop_assert_eq!(checks.len(), parity);

            // 拼出全部 N 个槽位
            let mut slots: Vec<Option<Vec<u8>>> =
                originals.iter().cloned().map(Some).collect();
            slots.extend(checks.into_iter().map(Some));

            // 按 drop_mask 丢弃若干槽位。
            // 用 iter_mut().enumerate() 而不是 `for i in 0..n { slots[i] = .. }`：
            // 后者会触发 clippy::needless_range_loop，在 `-D warnings` 下直接失败。
            let mut kept = 0;
            for (i, slot) in slots.iter_mut().enumerate() {
                if drop_mask & (1 << i) != 0 {
                    *slot = None;
                } else {
                    kept += 1;
                }
            }
            prop_assume!(kept >= data);

            let recovered = codec.decode(&slots).unwrap();
            prop_assert_eq!(recovered.len(), data);
            for (a, b) in recovered.iter().zip(originals.iter()) {
                prop_assert_eq!(a, b);
            }
        }

        /// 丢太多必须报错，绝不返回错误数据。
        #[test]
        fn too_few_shards_fails_closed(
            data in 2usize..=6,
            parity in 1usize..=4,
            payload in prop::collection::vec(any::<u8>(), 1..512),
        ) {
            let shard_size = 512;
            let codec = Codec::new(data, parity, shard_size).unwrap();
            let originals = pad_chunks(&payload, data, shard_size);
            let checks = codec.encode(&originals).unwrap();
            let mut slots: Vec<Option<Vec<u8>>> =
                originals.iter().cloned().map(Some).collect();
            slots.extend(checks.into_iter().map(Some));

            // 只留 data-1 份
            let mut kept = 0;
            for s in slots.iter_mut() {
                if kept < data - 1 { kept += 1; } else { *s = None; }
            }
            // 先绑定成 bool 再断言：proptest 的单参数 `prop_assert!` 会把条件
            // 字符串化丢进 format!，`{ .. }` 会被当成 format 占位符而编译失败。
            let failed_closed =
                matches!(codec.decode(&slots), Err(ErasureError::TooFewShards { .. }));
            prop_assert!(failed_closed);
        }
    }
}
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cargo test -p rstore-erasure`
Expected: 编译失败，`Codec` 方法未实现

- [ ] **Step 4: 实现**

实现 `encode`/`decode` 时：**先查 `reed-solomon-simd` v3 的 `encode`/`decode` 函数签名**
（`encode(original_count, recovery_count, shards)` 与
`decode(original_count, recovery_count, original_shards, recovery_shards)`，
以 `HashMap<usize, Vec<u8>>` 传入索引），把库调用完全包在门面内。

`lib.rs` 顶部要 `pub mod error;` 并 `pub use error::{ErasureConstructionError, ErasureError};`，
外部只见门面类型，不感知 `error` 模块路径。

关键点：
- **长度语义要钉死**，两种不符要分开报：
  - **分片个数**不符 → `WrongShardCount`（`encode`：`data_shards.len() != data`；
    `decode`：`slots.len() != total_shards`）；
  - **单个分片长度**不符 → `UnequalShardLength`（任一分片长度 != `self.shard_size`）。
  不要交给库去 panic。不做"长度相等即可"的宽松判定——把 `shard_size` 收严，
  畸形输入才有唯一的解释。
- **槽位如何映射到库**：`slots[0..data]` 是数据分片，`slots[data..total]` 是校验分片。
  `Some` 的槽位按各自下标放进库要求的 `HashMap<usize, Vec<u8>>`；
  两个 map 都为空时库会报错，但那时必然已经被 `TooFewShards` 拦下。
- 库返回 `Result`，映射到 `ErasureError::Backend`（用 `format!("{e}")` 存字符串）。
- `decode` 中**先**统计非 `None` 槽位数，`< data` 时立即返回 `TooFewShards`，不调用库。

- [ ] **Step 5: 跑测试确认通过**

Run: `cargo test -p rstore-erasure`
Expected: 全部 PASS（属性测试默认 256 次）

- [ ] **Step 6: 提交**

```bash
git add crates/erasure/ Cargo.lock
git commit -m "feat(erasure): codec facade with roundtrip and fail-closed property tests

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 1.4: 编解码器缓存

**Files:**
- Create: `crates/erasure/src/cache.rs`
- Modify: `crates/erasure/src/lib.rs`（加 `pub mod cache;` 与 `pub use cache::CodecCache;`）
- Test: 同文件 `#[cfg(test)]`

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_same_codec_for_same_key() {
        let c = CodecCache::new(4);
        let a = c.get(4, 2, 1024).unwrap();
        let b = c.get(4, 2, 1024).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn evicts_beyond_capacity() {
        let c = CodecCache::new(2);
        c.get(4, 2, 1024).unwrap();
        c.get(6, 3, 1024).unwrap();
        c.get(8, 4, 1024).unwrap();
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn invalid_geometry_is_not_cached() {
        let c = CodecCache::new(4);
        assert!(c.get(0, 0, 1024).is_err());
        assert_eq!(c.len(), 0);
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-erasure cache`
Expected: 编译失败，`CodecCache` 未定义

- [ ] **Step 3: 实现**

用 `std::sync::Mutex<lru::LruCache<(usize, usize, usize), Codec>>` 包一层。
`get` 时先查缓存，未命中则 `Codec::new` 并插入；**构造失败不插入**。
默认容量 32（对应 DESIGN §10.2）。

> **`lru` 的构造参数是 `NonZeroUsize`，不是 `usize`**（`lru = "0.12"`）——直接传 `usize` 编译不过。
> 容量 0 无意义（什么都存不下），收敛到 1 而不是 panic：
> ```rust
> pub fn new(capacity: usize) -> Self {
>     let cap = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN);
>     Self { inner: Mutex::new(LruCache::new(cap)) }
> }
> ```
>
> 注意 `CodecCache::new(4)` 接收的是 `usize`（测试就是这么调的），转换发生在内部。
> `Mutex` 提供内部可变性，所以 `get` / `len` 都用 `&self`——测试里的 `let c = ...` 是不可变绑定。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-erasure`
Expected: 全部 PASS

```bash
git add crates/erasure/
git commit -m "feat(erasure): LRU cache for codec shells

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## M2 — 元数据容器 `meta.xl`

### Task 2.1: 数据模型

**Files:**
- Create: `crates/meta/src/fileinfo.rs`
- Create: `crates/meta/src/keys.rs`
- Modify: `crates/common/src/error.rs`（加 `DiskError` / `CorruptKind`，见 Step 3 末尾）
- Modify: `crates/meta/src/lib.rs`（加 `pub mod fileinfo;`、`pub mod keys;` 与相应 `pub use`）
- Test: 同文件 `#[cfg(test)]`
- 无需改 `Cargo.toml`：`serde` / `uuid` / `rmp-serde` / `thiserror` 已在 `[dependencies]`，`proptest` 已在 `[dev-dependencies]`

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_version_id_differs_from_absent() {
        let with_nil = FileVersionHeader { version_id: Some(Uuid::nil()), ..Default::default() };
        let without = FileVersionHeader { version_id: None, ..Default::default() };
        assert_ne!(with_nil.version_id, without.version_id);
        assert!(with_nil.version_id.is_some());

        // 内存里不同还不够——线格式上必须也不同，否则落盘后区分不出来。
        let a = encode_header(&with_nil).unwrap();
        let b = encode_header(&without).unwrap();
        assert_ne!(a, b, "nil UUID 与 None 编码成了同样的字节");
        // uuid 必须走原始 16 字节（bin），不是带连字符的字符串——
        // 线格式一旦落盘不可改（DESIGN §8.3），在这里钉住。
        assert!(!a.contains(&b'-'), "uuid 被编码成了人类可读字符串: {a:?}");
    }

    #[test]
    fn epoch_decodes_to_none_mod_time() {
        let h = FileVersionHeader { mod_time: Some(0), ..Default::default() };
        let enc = encode_header(&h).unwrap();
        let dec = decode_header(&enc).unwrap();
        assert_eq!(dec.mod_time, None);
    }

    #[test]
    fn caps_geometry_is_readable_from_header() {
        let h = FileVersionHeader { ec_m: 4, ec_n: 6, ..Default::default() };
        assert_eq!(h.data_shards(), 4);
        assert_eq!(h.total_shards(), 6);
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-meta fileinfo`
Expected: 编译失败

- [ ] **Step 3: 实现**

按 DESIGN §8.2 定义 `FileVersionHeader`、`VersionType`、`Flags`、`ShallowVersion`、
`ObjectMeta`、`ObjectBody`、`PartInfo`。要点：

- `mod_time: Option<u64>`：`None` 编码为线格式的 `0`，解码时 `0` 还原为 `None`；
  ——注意这是**对 DESIGN §8.2 原文 `mod_time: u64` 的有意收紧**（DESIGN 已同步改为
  `Option<u64>`）：让「未设置」在类型里显式可表达，而不是靠 0 这个魔数。
  代价是 epoch（真实的 0）被折叠为 `None`，这是可接受的——文件不可能真有 1970 年的 mtime。
- `version_id: Option<Uuid>`：**nil UUID 与 `None` 必须在语义上不同**，编码时保留区别；
- `data_dir: Option<Uuid>` 走 **16 字节原始 UUID**；载荷里长度不是 16 → `Corrupt`，不是 `None`；
- header 必须携带 `ec_m` / `ec_n`，使 quorum 决策无需解析 body。

`fileinfo.rs` 另外提供 header 级的编解码（container 级的编解码是 Task 2.2 的事）：

```rust
/// header 的 msgpack 编解码。`Option` ↔ 线格式的映射**只发生在这一处**。
pub fn encode_header(h: &FileVersionHeader) -> Result<Vec<u8>, DiskError>;
pub fn decode_header(bytes: &[u8]) -> Result<FileVersionHeader, DiskError>;
```

另外先钉住这几个最小形状，避免 M2 内部的任务依赖倒置（Task 2.3 才给
`InlineData` 加帧化逻辑，但 Task 2.2 的样本就要构造它）：

```rust
/// 内联数据帧：version-key -> 原始字节（DESIGN §8.4）。
/// **必须是 newtype 而不是 `type` 别名**——Task 2.3 要在它上面挂 `encode`/`decode`，
/// 而类型别名不能带固有方法。`#[serde(transparent)]` 让它在 msgpack 上就是那个 map。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct InlineData(BTreeMap<String, Vec<u8>>);

impl InlineData {
    pub fn new() -> Self { Self(BTreeMap::new()) }
    pub fn insert(&mut self, k: impl Into<String>, v: Vec<u8>) -> Option<Vec<u8>>;
    pub fn get(&self, k: &str) -> Option<&[u8]>;
    // encode / decode 在 Task 2.3 的 inline.rs 里实现（同一 crate 内可以跨模块 impl）
}

pub type OpaqueBody = Vec<u8>;                     // 懒解析的 body 原始字节
```

`Flags` 用 `u8` newtype + const 位（`FREE_VERSION` / `USES_DATA_DIR` / `INLINE_DATA`），
提供 `empty()` / `contains()` / `insert()`；**不引入 `bitflags` 依赖**（workspace 里没有）。
`VersionType` 是 `Object | DeleteMarker` 的普通 enum。凡是需要过 msgpack 的类型都 derive
`serde::{Serialize, Deserialize}`。

`keys.rs` 定义内部键常量（全部以 `x-rs-` 开头）与 `RUSTORAGE_KEY_PREFIX` 常量。

**同时在 `crates/common/src/error.rs` 加共享错误词汇表**——`DiskError` 定义在 `common`
而不是 `disk`，因为 meta 层也要表达「确定性损坏」，而依赖方向是 `disk → meta`，
meta 不能反向依赖 disk：

```rust
/// 确定性损坏的种类。重试无意义，应触发 repair（DESIGN §17）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CorruptKind {
    #[error("bad magic")]
    BadMagic,
    #[error("unsupported format version")]
    UnsupportedVersion,
    #[error("length mismatch")]
    LengthMismatch,
    #[error("malformed header")]
    MalformedHeader,
    #[error("crc mismatch")]
    CrcMismatch,
    #[error("bitrot checksum mismatch")]
    BitrotMismatch,
    #[error("invalid shard distribution")]
    InvalidDistribution,
}

/// 磁盘/存储层错误。`#[non_exhaustive]`：`Transient` / `Fatal` 两个变体
/// 在 M3 引入，届时属于非破坏性变更（DESIGN §17）。
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DiskError {
    /// 对象/分片不存在。参与 quorum 计数时计为「缺失」，不计为「失败」。
    #[error("not found")]
    NotFound,
    /// 确定性损坏。重试无意义，应触发 repair。
    #[error("corrupt: {0}")]
    Corrupt(CorruptKind),
}
```

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-meta fileinfo`
Expected: PASS

```bash
git add crates/meta/ crates/common/src/error.rs
git commit -m "feat(meta): object metadata data model with nil/epoch semantics

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2.2: 容器编码与解码

**Files:**
- Create: `crates/meta/src/container.rs`
- Modify: `crates/meta/src/lib.rs`（加 `pub mod container;` 与 `pub use container::{encode, decode};`）
- Test: 同文件 `#[cfg(test)]`

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// 两个版本的样本：一个普通对象 + 一个删除标记。
    /// 刻意让两个 header 在 version_id / mod_time / data_dir 上都不同，
    /// 这样任何一个字段的编解码出错都会在 roundtrip 里暴露。
    fn sample_meta() -> ObjectMeta {
        let obj = FileVersionHeader {
            version_id: Some(Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888)),
            ty: VersionType::Object,
            size: 1234,
            mod_time: Some(1_700_000_000_000_000_000),
            ec_m: 4,
            ec_n: 6,
            flags: Flags::empty(),
            data_dir: Some(Uuid::from_u128(0x9999_aaaa_bbbb_cccc_dddd_eeee_ffff_0001)),
        };
        let marker = FileVersionHeader {
            version_id: None,           // 与上面的 Some(Uuid) 形成对照
            ty: VersionType::DeleteMarker,
            size: 0,
            mod_time: None,             // 与上面的 Some(..) 形成对照
            ec_m: 4,
            ec_n: 6,
            flags: Flags::empty(),
            data_dir: None,
        };
        ObjectMeta {
            versions: vec![
                ShallowVersion { header: obj, body: vec![0xde, 0xad, 0xbe, 0xef] },
                ShallowVersion { header: marker, body: Vec::new() },
            ],
            inline: InlineData::new(),
            meta_ver: 1,
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let m = sample_meta();
        let bytes = encode(&m).unwrap();
        let back = decode(&bytes).unwrap();
        assert_eq!(back.versions.len(), m.versions.len());
        assert_eq!(back.versions[0].header, m.versions[0].header);
    }

    #[test]
    fn detects_bad_magic() {
        let mut bytes = encode(&sample_meta()).unwrap();
        bytes[0] = b'X';
        assert!(matches!(decode(&bytes), Err(DiskError::Corrupt(CorruptKind::BadMagic))));
    }

    #[test]
    fn detects_crc_mismatch() {
        let bytes = encode(&sample_meta()).unwrap();
        // 必须翻转 body 里的字节：body 是不透明的，结构解析不受影响，才会稳定
        // 走到 CRC 校验。翻转 header 区可能先报 MalformedHeader。
        let needle = [0xde, 0xad, 0xbe, 0xef];
        let pos = bytes
            .windows(needle.len())
            .position(|w| w == needle)
            .expect("样本 body 应当出现在字节流中");
        let mut tampered = bytes.clone();
        tampered[pos] ^= 0xFF;
        assert!(matches!(
            decode(&tampered),
            Err(DiskError::Corrupt(CorruptKind::CrcMismatch))
        ));
    }

    #[test]
    fn rejects_truncated_buffer() {
        let bytes = encode(&sample_meta()).unwrap();
        assert!(decode(&bytes[..bytes.len() / 2]).is_err());
    }

    /// 核心鲁棒性属性：任意单字节翻转都必须报 Corrupt，绝不 panic、绝不返回 Ok。
    proptest! {
        #[test]
        fn never_panics_on_corruption(pos_frac in 0.0f64..1.0, mask in 1u8..=255) {
            let mut bytes = encode(&sample_meta()).unwrap();
            if bytes.is_empty() { return Ok(()); }
            let pos = ((pos_frac * bytes.len() as f64) as usize).min(bytes.len() - 1);
            bytes[pos] ^= mask;
            // 只要不 panic 且不返回 Ok 就算通过（翻转后仍合法的情况需排除 CRC 区）
            match decode(&bytes) {
                // 翻转落在 inline_data 区时 CRC 依然匹配（CRC 不覆盖内联帧），
                // 解出 Ok 是合法结果。
                Ok(_) => {}
                Err(DiskError::Corrupt(_)) => {}
                Err(other) => prop_assert!(false, "unexpected error: {other:?}"),
            }
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-meta container`
Expected: 编译失败

- [ ] **Step 3: 实现**

严格按 DESIGN §8.1 的布局实现：

```
magic "RSM1" (4B) | major u16 LE | minor u16 LE | version_count u16 LE
[ header(msgpack，自描述) | body_len u32 BE | body ] × version_count
trailer CRC32C (u32 LE，覆盖从 magic 到最后一个 body 的字节)
inline_data (msgpack map；可为空)
```

```rust
pub fn encode(meta: &ObjectMeta) -> Result<Vec<u8>, DiskError>;
pub fn decode(bytes: &[u8]) -> Result<ObjectMeta, DiskError>;
```

**CRC 在 `inline_data` 之前，所以它不在文件末尾**——`crc32c(&bytes[..len-4])` 这种写法是错的。
DESIGN §8.1 有意如此：CRC 只保护结构部分，且让「只读前缀即可完成 LIST/HEAD」的
增量读成为可能。代价是解码必须**顺序解析**，走到记录结束处才知道 CRC 在哪。

防御**必须按此顺序**：

1. `bytes.len() < 14`（= 4+2+2+2+4）→ `Corrupt(LengthMismatch)`；
2. magic 不符 → `Corrupt(BadMagic)`；
3. `major != 1` → `Corrupt(UnsupportedVersion)`；
4. `minor > 0` → `Corrupt(UnsupportedVersion)`（DESIGN §8.1：minor 过新也是确定性损坏）；
5. 读 `version_count`（偏移 **8..10**：`magic(4) + major(2) + minor(2)` 之后）。
   **在分配之前**用「剩余字节数 / 最小记录尺寸」做上界检查 → `Corrupt(LengthMismatch)`。
   记录从偏移 10 开始；上面那个 `bytes.len() < 14` 正是 `10 + 4`（4 = trailer CRC），
   任何把 `version_count` 读在 10..12 的实现都会让这个下限自相矛盾。
   下界用**真下界**：`HEADER_MIN_BYTES = 15`（`FileVersionHeader::default()` 的实测
   编码长度，由 `record_min_bytes_is_a_true_lower_bound` 钉住——**只能调小不能调大**，
   调大就会误拒合法输入）；
6. 逐条解析记录：用 `rmp_serde::from_read::<_, FileVersionHeader>(&mut cursor)`
   从 `&mut Cursor` 读一个 header（msgpack 自描述，读完 `cursor.position()` 就是边界，
   **不需要长度前缀**）；随后读 `body_len u32 BE`，**分配之前**检查
   `body_len <= 剩余字节` → `Corrupt(LengthMismatch)`；再读 `body_len` 字节作为
   不透明 `Vec<u8>`。msgpack 解析失败 → `Corrupt(MalformedHeader)`；
7. 记录读完处就是 CRC：算 `crc32c(&bytes[..crc_pos])`，与
   `u32::from_le_bytes(bytes[crc_pos..crc_pos + 4])` 比对，不符 → `Corrupt(CrcMismatch)`；
8. CRC 通过后，剩余字节 `bytes[crc_pos + 4..]` 用 msgpack 解成 `InlineData`
   （空切片 → 空 map）。失败 → `Corrupt(MalformedHeader)`。

`encode` 侧对称：记录写完后写 u32 LE 的 CRC，再写 `rmp_serde::to_vec(&meta.inline)`。

> **`meta_ver` 映射到容器的 `major`，不是 `minor`。** 容器只有 major/minor 两个版本位，
> 而 `ObjectMeta.meta_ver` 必须落在其中之一。取 `major`：它与魔数 `RSM1` 里的 `1` 同源，
> 也与样例里的 `meta_ver: 1` 一致。`minor` 承载兼容性的字段增删（DESIGN §8.3），
> 不在 `ObjectMeta` 里暴露。
>
> **`encode` 必须校验 `meta_ver == MAJOR`（= 1），不符返回 `UnsupportedVersion`。**
> 不校验的话，`encode` 会写出自己 `decode` 读不回来的字节——而 `decode` 把它报成
> `Corrupt`，DESIGN §17 又规定「观察到 Corrupt 即触发 repair」，于是编码期的错误
> 会伪装成盘损坏，对健康数据发起修复。这是一处必须堵死的不对称。

> 这里直接调 `rmp_serde::to_vec`（Task 2.3 才会给 `InlineData` 加上
> `encode`/`decode` 方法）。等 2.3 落地后，把这一处和对应的解码处替换成
> `meta.inline.encode()` / `InlineData::decode(tail)`——别留着两份做着同一件事的代码。

> **依赖 Task 2.1 的两个前提，二者都已被测试钉住：**
>
> 1. `FileVersionHeader` 通过 `#[serde(from = "HeaderWire", into = "HeaderWire")]`
>    桥接到 msgpack。**不要**试图给它直接 `derive(Serialize, Deserialize)`——
>    那会绕过 `None ↔ 0` 映射和 16 字节 UUID 编码，破坏线格式。
> 2. `rmp_serde` 在 `Read` 上**不预读**（`msgpack_does_not_overread_on_a_cursor`
>    钉住的正是这一点）。整个记录边界方案建立在这条之上：若它预读，读完 header 后
>    `cursor.position()` 会跑过头，随后的 `body_len` 就读到垃圾。**改动这里前先确认那条测试还在。**

> **第 5–6 步在 CRC 之前，这不是疏忽。** CRC 的物理位置由记录长度决定，不解析就找不到它——
> 把「先校验 CRC 再解析任何内容」写进计划是自相矛盾的，别照做。这么做的安全性由
> 两个「分配之前」的上界检查，加上 msgpack 解码器对畸形输入返回 `Err` 而非 panic 来兜。
>
> 副作用：被篡改的输入可能报 `MalformedHeader` 而不是 `CrcMismatch`。两者都是
> `Corrupt`，对 heal 的触发条件（DESIGN §17：观察到 `Corrupt` 即触发）没有区别。
> 正因如此，`detects_crc_mismatch` 必须**翻转 body 里的字节**（body 是不透明的，
> 解析不受影响，才会稳定走到 CRC）——见 Step 1 的测试。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-meta container`
Expected: 全部 PASS

```bash
git add crates/meta/
git commit -m "feat(meta): meta.xl container codec with corruption defenses

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2.3: 内联数据帧

**Files:**
- Create: `crates/common/src/consts.rs`（**目前不存在**，需要新建）
- Create: `crates/meta/src/inline.rs`
- Modify: `crates/common/src/lib.rs`（加 `pub mod consts;`）
- Modify: `crates/meta/src/lib.rs`（加 `pub mod inline;`）
- Modify: `crates/meta/src/fileinfo.rs`（给 `InlineData` 补 `encode`/`decode`，也可放在 inline.rs，同一 crate 内均可）
- **Modify: `crates/meta/src/container.rs`**（收掉 Task 2.2 留下的两处 `TODO(Task 2.3)`——
  `rmp_serde::to_vec(&meta.inline)` → `meta.inline.encode()`，以及解码侧那段
  `if tail.is_empty()` 手工判空 → 一行 `InlineData::decode(tail)`。**不收就会留下
  两份做着同一件事的代码**，且 `container.rs` 里的判空与 `InlineData::decode` 的判空会各自演化。）
- Test: 同文件 `#[cfg(test)]`

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_multiple_versions() {
        let mut d = InlineData::default();
        d.insert("null", b"hello".to_vec());
        d.insert("v1", b"world".to_vec());
        let bytes = d.encode().unwrap();
        let back = InlineData::decode(&bytes).unwrap();
        assert_eq!(back.get("null"), Some(&b"hello"[..]));
        assert_eq!(back.get("v1"), Some(&b"world"[..]));
        assert_eq!(back.get("missing"), None);
    }

    #[test]
    fn empty_input_decodes_to_empty_map() {
        // 容器尾部理论上可能什么都没有（DESIGN §8.4）。这是解码侧要容忍的形状，
        // 不能被当成畸形输入——否则一个合法容器会因为「没有内联数据」而读不出来。
        assert_eq!(InlineData::decode(&[]).unwrap(), InlineData::new());
    }

    #[test]
    fn garbage_decodes_to_malformed_header() {
        // 0x91 = fixarray(1)，不是 map——解不成 `InlineData`。
        let err = InlineData::decode(&[0x91, 0x01]).unwrap_err();
        assert!(
            matches!(&err, DiskError::Corrupt(CorruptKind::MalformedHeader)),
            "got {err:?}"
        );
    }

    #[test]
    fn should_inline_respects_thresholds() {
        // 原名 `known_input_is_under_threshold` 声称只测「在门限下」，但实际同时
        // 断言了超门限的情况——名字会误导后续维护者。
        assert!(rstore_common::consts::should_inline(64 * 1024, false));
        assert!(!rstore_common::consts::should_inline(256 * 1024, false));
        // 版本化桶门限更严格（1/8）
        assert!(!rstore_common::consts::should_inline(32 * 1024, true));
        assert!(rstore_common::consts::should_inline(8 * 1024, true));
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-meta inline`
Expected: 编译失败

- [ ] **Step 3: 实现**

按 DESIGN §8.4，内联帧就是纯 msgpack map：`version-key → bytes`。

> **不要引入 `INLINE_DATA_VER` 之类的帧内版本字节。** 整个 `meta.xl` 已经有
> `major`/`minor` 承载格式演进（DESIGN §8.3：「字段增删一律通过新的 minor 版本承载」），
> 内联帧再挂一套版本号是重复的版本承载点，将来两边怎么对齐是个麻烦。
> `InlineData` 靠 `#[serde(transparent)]` 直接序列化成那个 map。

在 `crates/common/src/consts.rs`（新建）加：

```rust
//! 跨层共享的常量与门限。

/// 常量名对齐 DESIGN §8.4 的 `DEFAULT_INLINE_BLOCK`（原计划写的 `INLINE_BLOCK` 与 DESIGN 不一致）。
pub const DEFAULT_INLINE_BLOCK: u64 = 128 * 1024;

/// 版本化桶取 1/8；MVP 未启用版本化，但函数签名保留该维度。
pub fn should_inline(size: u64, versioned_bucket: bool) -> bool {
    let threshold = if versioned_bucket { DEFAULT_INLINE_BLOCK / 8 } else { DEFAULT_INLINE_BLOCK };
    size <= threshold
}
```

`InlineData` 上的两个方法（`crates/meta/src/inline.rs`）：

```rust
impl InlineData {
    /// 编码成 msgpack map。
    pub fn encode(&self) -> Result<Vec<u8>, DiskError>;

    /// 从字节解出。空输入视为空 map——容器解码时尾部可能什么都没有。
    pub fn decode(bytes: &[u8]) -> Result<Self, DiskError>;
}
```

失败一律映射为 `DiskError::Corrupt(CorruptKind::MalformedHeader)`。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-meta inline`
Expected: PASS

```bash
git add crates/meta/ crates/common/
git commit -m "feat(meta): inline data framing with size thresholds

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## M3 — 盘抽象

### Task 3.1: DiskAPI trait

**Files:**
- Create: `crates/common/src/disk_id.rs`（`DiskId`——**必须放 common**）
- Create: `crates/disk/src/lib.rs`
- Modify: `crates/common/src/lib.rs`（加 `pub mod disk_id;`）
- Modify: `crates/common/src/error.rs`（把 `DiskError` 补全为 DESIGN §17 的四个变体）
- Modify: `crates/disk/Cargo.toml`（`tokio`/`async-trait`/`thiserror` 已在 `[dependencies]`；
  需加 `[dev-dependencies] tempfile.workspace = true`）

> `DiskId` 定义在 **`rstore-common`** 而不是 disk：Task 3.3 的 `meta::format` 也要用它，
> 而 meta 只允许依赖 common/checksum（护栏方向 `disk → meta`）。
> `FileStat` 只有 disk 层用，直接定义在 `crates/disk/src/lib.rs`。

**先把 `DiskError` 补全**——Task 2.1 出于 `#[non_exhaustive]` 只定义了
`NotFound` / `Corrupt`，但 Task 3.2 立刻要用到 `Transient` 和 `Fatal`
（短读、路径逃逸）。补到 `crates/common/src/error.rs`：

```rust
/// 瞬时故障：重试有意义，**不计入损坏统计**（DESIGN §17）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TransientKind {
    #[error("io error")]
    Io,
    #[error("timeout")]
    Timeout,
    #[error("short read")]
    ShortRead,
}

/// 致命故障：需要人工介入，重试无意义。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FatalKind {
    #[error("permission denied")]
    PermissionDenied,
    #[error("read-only disk")]
    ReadOnly,
    #[error("path escapes disk root")]
    PathEscape,
    #[error("no space left")]
    NoSpace,
}
```

并在 `DiskError` 上加：

```rust
    #[error("transient: {0}")]
    Transient(TransientKind),
    #[error("fatal: {0}")]
    Fatal(FatalKind),
```

> 用无载荷的 `Kind` 枚举而不是塞进 `io::Error`：`DiskError` 要能 `PartialEq`
> （quorum 投票、测试断言都依赖它），而 `io::Error` 不是 `PartialEq`。
> 具体错误细节走日志，不走错误值。

- [ ] **Step 1: 定义 trait（这是契约）**

```rust
/// 一块盘。对应 DESIGN §4。
///
/// **所有方法都返回 `Result<_, DiskError>`，不允许 panic。**
/// 实现必须把 IO 错误映射为 `DiskError` 的三级分类（见 `error_map.rs`）。
#[async_trait::async_trait]
pub trait DiskAPI: Send + Sync {
    async fn write_all(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError>;
    async fn read_exact_at(&self, rel_path: &str, offset: u64, len: usize) -> Result<Vec<u8>, DiskError>;
    async fn rename(&self, from_rel: &str, to_rel: &str) -> Result<(), DiskError>;
    async fn remove_dir_all(&self, rel_path: &str) -> Result<(), DiskError>;
    async fn list_dir(&self, rel_path: &str) -> Result<Vec<String>, DiskError>;
    async fn stat(&self, rel_path: &str) -> Result<Option<FileStat>, DiskError>;
    /// fsync 文件本身与父目录（保证 rename 的持久性）。
    async fn sync_file_and_parent(&self, rel_path: &str) -> Result<(), DiskError>;
    fn disk_id(&self) -> &DiskId;
    fn is_local(&self) -> bool;
}
```

`FileStat` 与 `DiskId` 的形状（原计划未给出，这里补上——否则实现者只能自己拍脑袋）：

- `FileStat` 只有 disk 层用（不是线格式），直接定义在 `crates/disk/src/lib.rs`：

  ```rust
  #[derive(Debug, Clone, PartialEq, Eq)]
  pub struct FileStat { pub size: u64, pub is_dir: bool }
  ```

  `size` 是 M4 读路径需要的（决定读多少字节），**别省**。
- `DiskId` 放 `crates/common/src/disk_id.rs`（定义在 common 而非 disk：Task 3.3 的
  `format.json` 也要用它，而依赖方向是 `disk → meta`，meta 无法反向依赖 disk）。
  API：`new_v4()` / `from_bytes([u8;16])` / `as_bytes() -> &[u8;16]`，
  另加 `Debug/Clone/Copy/PartialEq/Eq/Hash/Ord` 与 `serde`（Task 3.3 要把它写进 `format.json`）。
  `Display` 用 uuid 的 canonical 形式，**别自己拼十六进制**。

- [ ] **Step 2: 写契约测试（对任意实现都应通过）**

`crates/disk/src/lib.rs` 中放一个 `pub mod contract_tests`，内含一个
`pub async fn run_all<D: DiskAPI + ?Sized>(disk: &D)`——盘根由 `disk` 自身携带，
不另传 `tmp`（Task 3.2 / 3.4 的调用点都是 `run_all(&d).await`）。覆盖：
写后读回；**读不存在的路径**返回 `NotFound` 而非 panic；rename 后旧路径 `NotFound`；
`list_dir` 排序稳定；`sync_file_and_parent` 幂等。

> 区分两种「读不到」：**路径不存在 → `NotFound`**（确定性，参与 quorum 时计为「缺失」）；
> **文件存在但 `offset + len` 越界 → `Transient`**（短读，DESIGN §17 把它归入可重试）。
> 别合并成一种——Task 3.2 的 `read_past_eof_is_transient_not_corrupt` 钉的就是后者。
>
> 契约测试对**任意** `D: DiskAPI` 都要通过，所以不能依赖具体实现的路径布局。
> `run_all` 内部自己造一条临时相对路径（如 `__contract__/probe`），用完删掉。

- [ ] **Step 3: 门禁与提交（此时还没有实现，仅契约）**

本任务**没有「跑测试看它变红」的环节**（无实现），门禁是**编译通过 + clippy 干净**：

```bash
cargo fmt --all && cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
bash scripts/check-layer-deps.sh ; echo "guard exit=$?"
```

```bash
git add crates/disk/ crates/common/
git commit -m "feat(disk): DiskAPI trait and shared contract test suite

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

> **原计划这里写的是 `git add crates/disk/src/lib.rs`——错的**：会漏掉本任务同时改动的
> `crates/common/src/disk_id.rs`、`crates/common/src/error.rs`、`crates/common/src/lib.rs`
> 和 `crates/disk/Cargo.toml`，提交出来直接编译不过。

> `run_all` 在 3.1 阶段无人调用——这是**刻意的**，契约先于实现存在。因为是 `pub`，
> 不会有 dead-code 警告，别为了消除警告加 `#[allow]` 或提前写实现。

---

### Task 3.2: LocalDisk 实现

**Files:**
- Create: `crates/disk/src/local.rs`
- Create: `crates/disk/src/fsx.rs`
- Create: `crates/disk/src/error_map.rs`
- **Modify: `crates/disk/src/lib.rs`**（加 `pub mod local; pub mod fsx; pub mod error_map;`
  与 `pub use local::LocalDisk;`）——原计划漏了这条。**不加模块声明，这三个文件根本不会被编译**，
  `cargo test -p rstore-disk` 会报「找不到 `LocalDisk`」而不是你预期的编译错误。
- Test: `crates/disk/src/local.rs` 的 `#[cfg(test)]`，用 `tempfile::TempDir`

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn write_then_read() {
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        d.write_all("a/b.txt", b"hello").await.unwrap();
        let got = d.read_exact_at("a/b.txt", 0, 5).await.unwrap();
        assert_eq!(got, b"hello");
    }

    #[tokio::test]
    async fn read_past_eof_is_transient_not_corrupt() {
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        d.write_all("a.txt", b"hello").await.unwrap();
        let err = d.read_exact_at("a.txt", 3, 10).await.unwrap_err();
        assert!(matches!(err, DiskError::Transient(_)), "short read must not be Corrupt");
    }

    #[tokio::test]
    async fn missing_path_is_not_found() {
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        assert!(matches!(d.read_exact_at("nope", 0, 1).await, Err(DiskError::NotFound)));
    }

    #[tokio::test]
    async fn rename_moves_and_old_path_gone() {
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        d.write_all("staging/f", b"x").await.unwrap();
        d.rename("staging", "final").await.unwrap();
        assert!(d.stat("staging/f").await.unwrap().is_none());
        assert!(d.stat("final/f").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn rejects_path_escape() {
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        let r = d.write_all("../escape", b"x").await;
        assert!(matches!(r, Err(DiskError::Fatal(_))));
    }

    #[tokio::test]
    async fn rejects_embedded_path_escape() {
        // `../escape` 用字符串前缀检查也能拦下；这个不行——它证明检查是逐段做的。
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        let r = d.write_all("a/../../escape", b"x").await;
        assert!(matches!(r, Err(DiskError::Fatal(_))), "a/../../escape 逃出了盘根");

        // 绝对路径也必须拒绝（Windows 上也包括盘符前缀）。
        assert!(matches!(
            d.write_all("/etc/passwd", b"x").await,
            Err(DiskError::Fatal(_))
        ));
    }

    #[tokio::test]
    async fn passes_shared_contract_suite() {
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        crate::contract_tests::run_all(&d).await;
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-disk`
Expected: 编译失败

- [ ] **Step 3: 实现**

```rust
impl LocalDisk {
    /// 打开一块盘。`root` 是盘根目录；`disk_id` 来自 format.json，首次初始化时新生成。
    pub fn open(root: impl AsRef<Path>, disk_id: DiskId) -> Result<Self, DiskError>;
}
```

`fsx.rs` 提供阻塞原语（`write_all_fsync`、`rename_fsync`、`walk`），
`local.rs` 用 `tokio::task::spawn_blocking` 包装。**注意 `spawn_blocking` 要求闭包是
`'static + Send`**——不能捕获 `&self` 或 `&str` 借用的路径。先 clone 出
`Arc<PathBuf>`（盘根）与 `PathBuf`（目标）再 move 进闭包；
`&self` 上的方法这么写：
```rust
let root = Arc::clone(&self.root);
spawn_blocking(move || fsx::write_all_fsync(&root, &rel_path, data))
    .await
    .map_err(|_| DiskError::Transient(TransientKind::Io))?
```
另：workspace 开了 `clippy::await_holding_lock = "deny"`，**不要在 `await` 期间持有
`std::sync::Mutex` 守卫**——若为跨平台读写做了加锁兜底，用 `tokio::sync::Mutex`
或把临界区整个移进 `spawn_blocking` 里。**关键约束**：

- **路径逃逸检查**：把所有 `rel_path` 规范化后确认仍在盘根之下，否则 `Fatal(PathEscape)`。
  **用 `Path::components()` 逐段判断，不要用 `starts_with("..")` 字符串前缀检查**——
  后者漏掉 `a/../../b`。且目标路径可能尚不存在，**不能依赖 `canonicalize()`**
  （它会去访问文件系统并失败）。同时要拒绝绝对路径与 Windows 盘符前缀
  （`C:\...`、`\...`、`/...`）；
- `read_exact_at` **要跨平台**：本机是 Windows，CI 是 Linux——
  `std::os::unix::fs::FileExt::read_at` 与 `std::os::windows::fs::FileExt::seek_read`
  是两个不同的 trait，需要 `#[cfg]` 分流（或退回 seek+read，但那要处理并发读的共享游标）。
  **原计划只提了 unix 那条**。短读 → `Transient(ShortRead)`；
- `sync_file_and_parent` 必须先 fsync 文件再 fsync 父目录（顺序不可颠倒，否则 rename 可能不持久）；
- `error_map.rs`：`NotFound` → `DiskError::NotFound`；`UnexpectedEof`/`WouldBlock`/`TimedOut`
  → `Transient`；权限/只读挂载 → `Fatal`；**其余默认 `Transient`**（宁可重试，不误判为损坏）。
  **IO 层永远不产生 `Corrupt`**——DESIGN §17 规定 heal 的触发条件是「观察到 `Corrupt`」，
  由 IO 层猜出来的损坏会去修健康数据。`Corrupt` 只能由 meta 层的校验产生。

**三个语义细节，测试会检验（原计划未写明）：**

- `write_all` 要**自动创建父目录**——测试写的是 `"a/b.txt"`，而 `a/` 不存在；
- `stat` 对不存在的路径返回 **`Ok(None)`**，不是 `Err(NotFound)`（签名的返回类型就是
  `Result<Option<FileStat>, DiskError>`）；
- `remove_dir_all` **幂等**：路径不存在时返回 `Ok(())`（契约套件用它做清理）。
  M4 若需要区分「删了」与「本来就没有」，先 `stat` 再删。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-disk`
Expected: 全部 PASS

```bash
git add crates/disk/
git commit -m "feat(disk): LocalDisk with fsync-aware rename and path escape guard

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3.3: format.json 与拓扑校验

**Files:**
- Create: `crates/meta/src/format.rs`
- Modify: `crates/meta/src/lib.rs`（加 `pub mod format;`）
- Test: 同文件 `#[cfg(test)]`

`DiskId` 从 `rstore_common::disk_id` 取（见 Task 3.1）；meta 只依赖 common/checksum，够用。

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_identity_excludes_this_disk() {
        let a = FormatV1::sample(DiskId::new_v4());
        let mut b = a.clone();
        b.erasure.this = DiskId::new_v4();
        assert_eq!(a.shared_identity(), b.shared_identity());
    }

    #[test]
    fn shared_identity_includes_topology() {
        let a = FormatV1::sample(DiskId::new_v4());
        let mut b = a.clone();
        b.erasure.sets[0][1] = DiskId::new_v4();
        assert_ne!(a.shared_identity(), b.shared_identity());
    }

    #[test]
    fn shared_identity_excludes_disk_info() {
        // `disk_info.free` 每块盘必然不同。若把它算进 identity，同一 pool 的盘
        // 永远凑不出多数派，quorum 协商整体失效——这是设计文档里一处真实的错
        // （DESIGN §7 原文写「除 this 之外的全部字段」），必须由测试钉住。
        let a = FormatV1::sample(DiskId::new_v4());
        let mut b = a.clone();
        b.disk_info = DiskInfo { total: 999, free: 123 };
        assert_eq!(a.shared_identity(), b.shared_identity());
    }

    #[test]
    fn rejects_unknown_format() {
        let mut a = FormatV1::sample(DiskId::new_v4());
        a.format = "xl".into();
        assert!(a.validate().is_err());
    }

    #[test]
    fn rejects_inconsistent_set_sizes() {
        let mut a = FormatV1::sample(DiskId::new_v4());
        a.erasure.sets[0].pop();
        assert!(a.validate().is_err());
    }

    /// quorum 投票：多数派的 identity 胜出，少数派被忽略。
    /// 比较 identity 而不是内部字段，避免绑死 `sample` 的具体内容。
    #[test]
    fn quorum_picks_the_majority_identity() {
        let id = DiskId::new_v4();
        let a = FormatV1::sample(id);
        let mut same = a.clone();
        same.erasure.this = DiskId::new_v4();     // 只有 this 不同 → 同一 identity
        let mut other = FormatV1::sample(id);
        other.erasure.sets[0][1] = DiskId::new_v4(); // 拓扑不同 → 另一种 identity

        let chosen = select_authoritative(&[a.clone(), same, other]).unwrap();
        assert_eq!(chosen.shared_identity(), a.shared_identity());
    }

    /// 票数打平（无多数）必须报错，不能随便挑一个。
    #[test]
    fn no_quorum_is_an_error() {
        let id = DiskId::new_v4();
        let a = FormatV1::sample(id);
        let mut b = FormatV1::sample(id);
        b.erasure.sets[0][1] = DiskId::new_v4();
        assert!(select_authoritative(&[a, b]).is_err());
    }

    /// 初始化闸门：**只要有一块盘不是 NotFound 就不能当新拓扑**。
    /// 这条是 DESIGN §7「网络不可达的盘绝不被当作新拓扑的证据」的直接落地。
    #[test]
    fn init_only_when_every_disk_is_missing() {
        assert!(should_initialize(&[DiskError::NotFound, DiskError::NotFound]));
        assert!(!should_initialize(&[
            DiskError::NotFound,
            DiskError::Transient(TransientKind::Timeout),
        ]));
        assert!(!should_initialize(&[]));
    }

    /// format.json 是运维会直接打开看的文件，字段名与 `DiskId` 的**字符串**形态
    /// 都是对外契约。`DiskId` 是 Uuid 的 newtype——万一将来有人把它换成
    /// `[u8; 16]`，JSON 里就会冒出 `[17,34,...]` 这样的数组，人读不了，
    /// 而只有这条测试会拦下来。
    #[test]
    fn format_json_shape_is_stable() {
        let v = serde_json::to_value(FormatV1::sample(DiskId::new_v4())).unwrap();
        assert_eq!(v["format"], "erasure");
        assert_eq!(v["erasure"]["distribution_algo"], "crc32c-rot-v1");
        assert!(v["erasure"]["this"].is_string(), "got {v:#?}");
        assert!(v["erasure"]["sets"][0][0].is_string(), "got {v:#?}");
    }

    /// 集合的**计数**必须进 identity。丢了计数的话，同样的盘按不同方式分组会编出
    /// 同一串字节——`[[d1,d2]]` 与 `[[d1],[d2]]` 是两种完全不同的纠删拓扑
    /// （前者 1 个 2 盘组，后者 2 个 1 盘组），却会互相投票凑成多数派。
    #[test]
    fn shared_identity_distinguishes_set_grouping() {
        let (d1, d2) = (DiskId::new_v4(), DiskId::new_v4());
        let mut a = FormatV1::sample(DiskId::new_v4());
        a.erasure.sets = vec![vec![d1, d2]];
        let mut b = FormatV1::sample(DiskId::new_v4());
        b.erasure.sets = vec![vec![d1], vec![d2]];
        assert_ne!(a.shared_identity(), b.shared_identity());
    }
}
```

> `Transient` / `TransientKind` / `Fatal` / `FatalKind` 已在 **Task 3.1** 补进
> `crates/common/src/error.rs`——磁盘层从 Task 3.2 起就返回它们了，不能等到这里。

```rust
/// 拓扑协商失败。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FormatError {
    #[error("unknown format: {0}")]
    UnknownFormat(String),
    #[error("inconsistent topology: {0}")]
    Inconsistent(String),
    #[error("no quorum among {total} disks (best identity got {best} votes)")]
    NoQuorum { total: usize, best: usize },
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-meta format`
Expected: 编译失败

- [ ] **Step 3: 实现**

形状（照 DESIGN §7 的 JSON 逐字段对应；原计划只说「按 §7 定义」而没给字段，
实现者无从下手）：

```rust
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FormatV1 {
    pub version: String,        // "1"（DESIGN §7 用的是字符串，不是数字）
    pub format: String,         // "erasure"
    pub id: String,             // deployment uuid
    pub erasure: FormatErasureV1,
    pub disk_info: DiskInfo,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FormatErasureV1 {
    pub version: String,        // "1"
    pub this: DiskId,
    pub sets: Vec<Vec<DiskId>>,
    pub distribution_algo: String,   // 本项目只认 "crc32c-rot-v1"
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DiskInfo { pub total: u64, pub free: u64 }
```

另需一个测试辅助 `impl FormatV1 { pub fn sample(this: DiskId) -> Self }`——
测试全靠它构造样本（至少 1 个 set、每 set ≥2 块盘，让 `sets[0][1]` 可寻址）。
`sample` 的 `this` **必须只影响 `erasure.this`**：`id`/`version`/`format`/
`distribution_algo`/`sets` 一律取固定常量。否则 `shared_identity` 的那几条测试
（比较两个 `sample` 的 identity）就失去意义了。

**`crates/meta/Cargo.toml` 加 `serde_json`——放 `[dev-dependencies]`，不是 `[dependencies]`。**
本任务的库代码一行 JSON 都不用（`Serialize`/`Deserialize` 派生只需 `serde`），
只有 `format_json_shape_is_stable` 这条测试读 JSON。放错会白搭进 meta 的每次构建。
真正读盘上 format.json 的是 Task 6.3 的 `crates/server/`，那个 crate 自己带依赖。
（原计划的 Files 清单漏了这条。）

按 DESIGN §7 实现：

- `shared_identity()` → 返回**逻辑拓扑**的全部字段，即**除 `this` 与 `disk_info` 之外**
  的一切。返回类型取 `Vec<u8>`—— `Vec<u8>` 天然 `PartialEq + Debug`，且是
  **精确比对而非指纹**，没有哈希碰撞的语义问题。
  > **`disk_info` 必须排除**：它含 `free`，每块盘必然不同。算进去的话，
  > 同一 pool 的盘永远凑不出多数派，quorum 协商整体失效。
  > **DESIGN §7 原文写的是「除 `this` 之外的全部字段」——那是错的，已改。**
  编码**手写**，不要走 serde：`serde_json::to_vec` 会引入一条不可能失败、
  只能 `expect` 的 panic 路径，而这里要的是一个不可失败的函数。
  规则：每个字符串先写 `u32 LE` 长度再写字节；`sets` 先写 set 数量，
  每个 set 先写盘数，再逐块写 `as_bytes()`（16B）。**长度/计数前缀不是装饰**，
  见 `shared_identity_distinguishes_set_grouping`。
- `validate() -> Result<(), FormatError>` → `format == "erasure"`、
  `distribution_algo == "crc32c-rot-v1"`、`sets` 非空、所有 set 长度一致且 `2..=16`；
- `select_authoritative(formats: &[FormatV1]) -> Result<FormatV1, FormatError>`：
  按 `shared_identity()` 分组计票，**多数派 = `总数 / 2 + 1`**，未达 quorum 报错。
  选出赢家后再对它跑一次 `validate()`（`?` 直接往上抛）——投票只保证大家
  「彼此一致」，不保证一致的那份是合法的；不校验就可能把一个非法拓扑扶正。
- `should_initialize(errs: &[DiskError]) -> bool`：**仅当所有盘都返回 `NotFound` 时**为真
  （对应 DESIGN §7「网络不可达的盘绝不被当作新拓扑的证据」；空切片为 false）。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-meta format`
Expected: PASS

```bash
git add crates/meta/ Cargo.lock
git commit -m "feat(meta): format.json with shared identity quorum and strict init gate

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

> `Cargo.lock` 必须一起提交：它按包记录依赖边，给 meta 加依赖会改动
> `name = \"rstore-meta\"` 那条的 `dependencies` 列表。只 `git add crates/meta/`
> 会留下一个脏的 lockfile，下一个人 `--locked` 直接失败。

---

### Task 3.4: FaultyDisk 测试基础设施

**Files:**
- Create: `crates/disk/src/faulty.rs`
- Create: `crates/disk/tests/faulty_disk.rs`
- Modify: `crates/disk/src/lib.rs`（`pub use local::LocalDisk;`、`pub use error::DiskError;`
  等 re-export，以及下面说的门控模块声明）
- Modify: `crates/disk/Cargo.toml`（**只需**新增 `[features] fault-injection = []`）

> **为什么要 feature**：`FaultyDisk` 必须能被 `rstore-store` 的集成测试用到，
> 而集成测试是**独立编译的 crate**，`#[cfg(test)]` 在那里不生效。
> 因此模块门控写作 `#[cfg(any(test, feature = "fault-injection"))]`：
> crate 内单测自动可见，跨 crate 由 feature 显式开启。

> **依赖不用加**（原计划说还要给 `[dev-dependencies]` 补 `tokio`，是多余的）：
> `tests/` 下的集成测试除了 `[dev-dependencies]` 之外**也能看到 `[dependencies]`**，
> 所以 `tokio`（已在 `[dependencies]`，`features = ["full"]`）和 `async-trait` 直接可用；
> `tempfile` 已在 Task 3.1 进 `[dev-dependencies]`。加一份冗余的 tokio dev-dep
> 只会让人以为两处配置都有关。

- [ ] **Step 1: 写失败测试**

```rust
// `faulty` 模块由 feature 门控，而集成测试是**独立编译**的 crate，
// 不带 `--features fault-injection` 时 `rstore_disk::faulty` 根本不存在。
// 没有这一行，`cargo test --workspace`（不带 feature）会**编译失败**——
// 门禁命令就会红，而红的原因跟被测代码毫无关系。
#![cfg(feature = "fault-injection")]

use rstore_common::disk_id::DiskId;
use rstore_disk::faulty::{Fault, FaultKind, FaultyDisk};
use rstore_disk::{DiskAPI, DiskError, LocalDisk};

#[tokio::test]
async fn can_drop_writes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let inner = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
    let d = FaultyDisk::wrap(inner).with(Fault::DropWrites);
    d.write_all("f", b"x").await.unwrap();       // 对外报成功
    assert!(matches!(d.read_exact_at("f", 0, 1).await, Err(DiskError::NotFound)));
}

#[tokio::test]
async fn can_corrupt_bytes_silently() {
    let tmp = tempfile::TempDir::new().unwrap();
    let inner = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
    let d = FaultyDisk::wrap(inner);
    d.write_all("f", b"hello").await.unwrap();
    d.set_fault(Fault::CorruptBytes { at: 0, mask: 0xFF });
    d.write_all("g", b"hello").await.unwrap();
    // 读回来内容与写入不同，且没有任何 API 报错 —— 模拟静默损坏
    let got = d.read_exact_at("g", 0, 5).await.unwrap();
    assert_ne!(got, b"hello");
}

#[tokio::test]
async fn can_fail_after_n_calls() {
    let tmp = tempfile::TempDir::new().unwrap();
    let inner = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
    let d = FaultyDisk::wrap(inner).with(Fault::FailAfter { calls: 2, kind: FaultKind::Transient });
    d.write_all("a", b"1").await.unwrap();
    d.write_all("b", b"2").await.unwrap();
    assert!(matches!(d.write_all("c", b"3").await, Err(DiskError::Transient(_))));
}

#[tokio::test]
async fn can_return_to_healthy_after_fault() {
    // 故障注入必须是可逆的：否则「故障排除后原数据还读得回来吗」这类断言写不出来。
    let tmp = tempfile::TempDir::new().unwrap();
    let inner = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
    let d = FaultyDisk::wrap(inner).with(Fault::Offline);
    assert!(matches!(
        d.write_all("f", b"x").await,
        Err(DiskError::Transient(_))
    ));

    d.clear_fault();
    d.write_all("f", b"x").await.unwrap();
    assert_eq!(d.read_exact_at("f", 0, 1).await.unwrap(), b"x");
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-disk --features fault-injection --test faulty_disk`
Expected: 编译失败

- [ ] **Step 3: 实现**

`FaultyDisk` 用 `AtomicUsize` 计调用次数、`Mutex<Option<Fault>>` 存当前故障
（默认 `None`，即行为与内层盘完全一致）。

```rust
/// 注入的故障。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// 写调用对外报成功，但数据不落盘（模拟丢失 fsync）。
    DropWrites,
    /// 只写入前一半字节（模拟撕裂写）。
    PartialWrite,
    /// 写入时在偏移 `at` 处按 `mask` 异或——**静默损坏**：不报任何错，读回来的字节就是错的。
    CorruptBytes { at: usize, mask: u8 },
    /// 只写入前 `len` 字节，其余丢弃（`PartialWrite` 的带参形式）。
    Truncate { len: usize },
    /// **前 `calls` 次调用正常**，第 `calls + 1` 次起一律返回 `kind` 对应的错误。
    /// （原文写的是「第 `calls` 次调用起」，与 `can_fail_after_n_calls` 的期望
    /// 「写 1、2 成功，写 3 失败」以及测试名 `fail_after_n_calls` 都矛盾。）
    FailAfter { calls: usize, kind: FaultKind },
    /// 所有调用都返回 `Transient`（模拟盘离线）。
    Offline,
}

/// `FailAfter` / `Offline` 要伪造的错误种类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    Transient,
    Corrupt,
    NotFound,
}
```

`FaultKind` 到 `DiskError` 的映射（原计划只说「返回 `kind` 对应的错误」，
没定 `Corrupt` 具体是哪种——`Corrupt` 带载荷 `CorruptKind`，必须挑一个）：

| `FaultKind` | `DiskError` |
|---|---|
| `Transient` | `Transient(TransientKind::Io)` |
| `Corrupt` | `Corrupt(CorruptKind::BitrotMismatch)` —— 模拟 bitrot，这是 M4 的 heal 路径最关心的种类 |
| `NotFound` | `NotFound` |

```rust

impl DiskAPI for FaultyDisk { /* 每个方法先看故障，再委托给内层 */ }

impl FaultyDisk {
    /// 包一层。初始无故障。
    pub fn wrap(inner: impl DiskAPI + 'static) -> Self;

    /// builder 风格：消耗并返回，用于构造后立即设一次故障。
    pub fn with(self, fault: Fault) -> Self;

    /// 原地改故障（测试中途切换用）。`&self` —— 内部靠 `Mutex` 提供可变性，
    /// 所以测试里的 `let d = ...` 不需要 `mut`。
    pub fn set_fault(&self, fault: Fault);

    /// 清除故障，回到与内层盘完全一致的行为。
    /// **原计划漏了这个口子**：内部状态是 `Mutex<Option<Fault>>`、初值 `None`，
    /// 但没有任何 API 能再回到 `None`——注入故障后就成了单行道，M4 里
    /// 「故障恢复后原数据读得回来吗」这类断言根本写不出来。
    pub fn clear_fault(&self);
}
```

**`CorruptBytes` / `Truncate` / `PartialWrite` 一律是「交给内层盘之前」对 payload 做变换**，
不是写完之后再去改盘上的文件。原文把 `Truncate` 写成「写入后把文件截断到 `len` 字节」——
那**根本实现不了**：`DiskAPI` 没有 `truncate`/`set_len`，`FaultyDisk` 拿不到任何能在写入后
缩短文件的手段。改成写前变换后语义一致（`LocalDisk::write_all` 是创建即截断，只写前 `len`
字节得到的就是一个 `len` 字节的文件），而且不需要读-改-写。

**读路径一律不动**：只把已经损坏的字节原样交出去，不额外校验——否则就模拟不出「静默损坏」了。

**`FailAfter` 的计数器计的是「任意 `DiskAPI` 方法的调用次数」，不区分读写。** 这一点必须写进
doc 注释：M4 的崩溃点测试要按这个口径推算 `calls`，含糊的话会写出随机飘的断言。

**要求：`FaultyDisk` 必须通过 `contract_tests`（无故障注入时行为与 `LocalDisk` 完全一致）**，
否则它测出来的问题可能是它自己引入的。

> 原计划只写了这条要求却没给测试——「要求」没有测试兜着就等于没有。在
> `crates/disk/src/faulty.rs` 里补上（crate 内单测，与 `LocalDisk` 的调用点写法一致）：

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use rstore_common::disk_id::DiskId;

    #[tokio::test]
    async fn passes_shared_contract_suite_when_healthy() {
        // `FaultyDisk` 自身的正确性门禁：无故障注入时它必须与 LocalDisk 行为一致。
        // 否则 M4 里「FaultyDisk 测出来的故障」可能根本是它自己引入的。
        let tmp = tempfile::TempDir::new().unwrap();
        let inner = crate::local::LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        crate::contract_tests::run_all(&FaultyDisk::wrap(inner)).await;
    }
}
```

（`tempfile` 已在 Task 3.1 加进 `[dev-dependencies]`。若 `LocalDisk` 的路径不是
`crate::local::LocalDisk`，按 Task 3.2 的实际布局调整。）

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-disk --features fault-injection --test faulty_disk`
Expected: 全部 PASS

```bash
git add crates/disk/
git commit -m "test(disk): FaultyDisk fault injection harness

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

> **两个 feature 组合都要过**（原计划只给了带 feature 的那条命令）：
> ```bash
> cargo test -p rstore-disk                              # 不带 feature：集成测试编译成空
> cargo test -p rstore-disk --features fault-injection   # 带 feature：全部用例
> cargo clippy --workspace --all-targets --locked -- -D warnings
> ```
> 不带 feature 的那条若红，说明 `tests/faulty_disk.rs` 的 `#![cfg(...)]` 门控没写对。

---

## M4 — 存储引擎核心

> 这是 MVP 的主体。每个 Task 都要求先写测试，且**测试必须使用 `FaultyDisk`**，
> 而不是只用 `LocalDisk`。

### Task 4.1: bitrot 分片写入器

**Files:**
- Create: `crates/store/src/error.rs`（`StoreError`。4.4/4.5/4.7 都要用它，本任务是用得最早的）
- Create: `crates/store/src/writer.rs`
- Modify: `crates/store/src/lib.rs`（加 `pub mod error; pub mod writer;`；现在里面只有一个文档注释）
- Modify: `crates/store/Cargo.toml`（`[dev-dependencies]` 加 `tempfile.workspace = true`）
- Test: 同文件 `#[cfg(test)]`

> **哈希与尺寸函数已经在 `rstore-checksum` 里了**（M2 完成）：`bitrot_hash(&[u8]) -> [u8;32]`、
> `bitrot_size(size, shard_size) -> u64`、`HASH_LEN`，密钥常量 `BITROT_KEY_V1` 也在那儿。
> **直接 `use rstore_checksum::...`，绝不要在 store 里再写一份。** 重写一份意味着
> 两个密钥常量：今天写进去的数据，换了实现之后校验全失败。
> 原计划把「`bitrot_size` 的算术表」当成本任务的测试来写——那几条断言
> （`(1,1024)→33`、`(1025,1024)→1089` …）**已经在 checksum 的 `size_arithmetic` 里钉过了**，
> 抄一遍只是把同一张表钉两次。本任务真正该测的是**写入器的实际落盘尺寸与
> `bitrot_size` 的预言一致**（那是跨 crate 的接缝，单测各自的绿证明不了它）。

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstore_checksum::{bitrot_hash, bitrot_size, HASH_LEN};
    use rstore_common::disk_id::DiskId;
    use rstore_disk::{DiskAPI, LocalDisk};

    use super::*;
    use crate::error::StoreError;

    /// 真实的 `LocalDisk` 而不是内存假盘：本任务要断言的正是「落到磁盘上的字节形状」。
    fn temp_disk() -> (tempfile::TempDir, Arc<dyn DiskAPI>) {
        let tmp = tempfile::TempDir::new().unwrap();
        let d: Arc<dyn DiskAPI> = Arc::new(LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap());
        (tmp, d)
    }

    #[tokio::test]
    async fn writes_interleaved_hash_and_data() {
        // 布局：每个 block 落盘为 `[hash(32B)][data]`（DESIGN §11）。
        let (tmp, disk) = temp_disk();
        let mut w = BitrotShardWriter::new(disk, "part.1".into(), 1024);
        w.push_block(&[7u8; 1024]).unwrap();
        w.push_block(&[9u8; 476]).unwrap();
        assert_eq!(w.payload_len(), 1500);
        w.finish().await.unwrap();

        let raw = std::fs::read(tmp.path().join("part.1")).unwrap();

        // 只断言总长度是不够的：把布局写成 `[data][hash]` 的实现，总长度一模一样。
        // 必须逐块核对摘要与数据各自的位置。
        assert_eq!(raw.len(), 1564);
        assert_eq!(&raw[..HASH_LEN], &bitrot_hash(&[7u8; 1024])[..]);
        assert_eq!(&raw[HASH_LEN..HASH_LEN + 1024], &[7u8; 1024][..]);
        assert_eq!(&raw[1056..1056 + HASH_LEN], &bitrot_hash(&[9u8; 476])[..]);
        assert_eq!(&raw[1056 + HASH_LEN..], &[9u8; 476][..]);
    }

    /// 落盘尺寸必须等于 `rstore_checksum::bitrot_size` 的预言。两者分居两个 crate，
    /// 若各算各的，两边单测都会绿，只有读路径会按错误的偏移去取字节。
    #[tokio::test]
    async fn layout_agrees_with_shared_bitrot_size() {
        for (len, bs) in [
            (0usize, 1024usize),
            (1, 1024),
            (1024, 1024),
            (1025, 1024),
            (5000, 512),
        ] {
            let (tmp, disk) = temp_disk();
            let payload = vec![0xABu8; len];
            let mut w = BitrotShardWriter::new(disk, "part.1".into(), bs);
            for chunk in payload.chunks(bs) {
                w.push_block(chunk).unwrap();
            }
            w.finish().await.unwrap();

            let on_disk = std::fs::metadata(tmp.path().join("part.1")).unwrap().len();
            assert_eq!(on_disk, bitrot_size(len as u64, bs as u64), "len={len} bs={bs}");
        }
    }

    /// 短块只允许出现在**末尾**。若中间混进短块而写入器默许，读侧按
    /// `k * (32 + block_size)` 的固定步长定位就会整体错位；错位读出的字节哈希必然对不上，
    /// 于是被报成 `Corrupt(BitrotMismatch)`——**写入方的 bug 伪装成盘损坏，
    /// 进而触发对健康数据的 heal**。宁可在这里拒绝。
    #[test]
    fn rejects_misuse_that_would_desync_the_reader() {
        let (_tmp, disk) = temp_disk();
        let mut w = BitrotShardWriter::new(disk, "part.1".into(), 1024);
        w.push_block(&[1u8; 100]).unwrap(); // 短块：可以，但必须是最后一块
        assert!(matches!(
            w.push_block(&[2u8; 100]),
            Err(StoreError::ShardLayout(_))
        ));

        let (_tmp, disk) = temp_disk();
        let mut w = BitrotShardWriter::new(disk, "part.1".into(), 1024);
        // 超长块任何时候都不合法；空块也无意义（`bitrot_size` 不会为 0 字节产生块），
        // 多出来的那 32 字节摘要没有对应的数据，读侧会把它当成一个空块。
        assert!(matches!(
            w.push_block(&[3u8; 1025]),
            Err(StoreError::ShardLayout(_))
        ));
        assert!(matches!(w.push_block(&[]), Err(StoreError::ShardLayout(_))));
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store writer`
Expected: 编译失败

- [ ] **Step 3: 实现**

**先写 `crates/store/src/error.rs`：**

```rust
/// store 层的错误。跨盘操作的失败必须能区分「quorum 没凑够」与「盘本身报错」——
/// 前者是本次写失败，后者要按 `DiskError` 的三级分类决定是重试、标记落后还是触发 heal。
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("disk error: {0}")]
    Disk(#[from] DiskError),
    #[error("write quorum not reached: {achieved}/{required}")]
    WriteQuorum { achieved: u8, required: u8 },
    #[error("read quorum not reached: {achieved}/{required}")]
    ReadQuorum { achieved: u8, required: u8 },
    #[error("bad shard layout: {0}")]
    ShardLayout(String),
    #[error("internal error: {0}")]
    Internal(String),
}
```

**再写 `crates/store/src/writer.rs`：**

```rust
pub struct BitrotShardWriter {
    disk: Arc<dyn DiskAPI>,
    rel_path: String,
    block_size: usize,
    buf: Vec<u8>,
    /// 已推入一个短块：此后不允许再推入任何块。
    short_seen: bool,
}

impl BitrotShardWriter {
    pub fn new(disk: Arc<dyn DiskAPI>, rel_path: String, block_size: usize) -> Self;

    /// 追加一个 block：把 `[hash(32B)][data]` 追加进内部缓冲。
    /// `data.len() <= block_size`；短块只能出现在末尾；空块一律拒绝。
    pub fn push_block(&mut self, data: &[u8]) -> Result<(), StoreError>;

    /// 已推入的原始字节数（不含摘要）。读侧构造 `BitrotShardReader` 时要拿它当
    /// `shard_len`，所以这里必须暴露出来而不是让调用方自己累加。
    pub fn payload_len(&self) -> u64;

    /// 一次性把整份分片落盘并 fsync（文件 + 父目录）。
    pub async fn finish(self) -> Result<(), StoreError>;
}
```

> **为什么是「缓冲 + 一次落盘」，而不是逐块追加**：`DiskAPI::write_all` 的实现是
> `fsx::write_all_fsync` → `File::create`，**创建即截断**。逐块调 `write_all` 的话，
> 每次调用都会把前一块抹掉，最后盘上只剩最后一块——而每个 `write_block` 都返回 `Ok`，
> 从调用方看一切正常。`DiskAPI` 目前没有 `append`/`write_at`，本任务也不去加它：
> MVP 的 PUT 本来就把整份对象拿在内存里（`PutArgs { data: Vec<u8> }`），
> 缓冲分片不会比输入本身更占内存。流式分片写入属于 Phase 2，届时给 `DiskAPI`
> 补一个定位写方法即可，本结构体的接口不用变。
>
> 顺带一提，这个形状反而更符合崩溃语义：整份分片一次性写进 staging 路径，
> 再由 rename 提交（DESIGN §12），中途崩溃留下的是一个永远不会被看见的半截文件。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store writer`
Expected: PASS

```bash
git add crates/store/ Cargo.lock
git commit -m "feat(store): StoreError and bitrot shard writer with interleaved layout

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4.2: bitrot 分片读取器

**Files:**
- Create: `crates/store/src/reader.rs`
- Modify: `crates/store/src/lib.rs`（加 `pub mod reader;`）
- Test: 同文件 `#[cfg(test)]`

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstore_checksum::{bitrot_size, HASH_LEN};
    use rstore_common::disk_id::DiskId;
    use rstore_common::error::{CorruptKind, DiskError, TransientKind};
    use rstore_disk::{DiskAPI, LocalDisk};

    use super::*;
    use crate::writer::BitrotShardWriter;

    const BS: usize = 1024;
    const PAYLOAD_LEN: usize = 1500;

    fn temp_disk() -> (tempfile::TempDir, Arc<dyn DiskAPI>) {
        let tmp = tempfile::TempDir::new().unwrap();
        let d: Arc<dyn DiskAPI> = Arc::new(LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap());
        (tmp, d)
    }

    /// 用 Task 4.1 的写入器造一份真实分片——读写的布局约定本来就该由它们彼此对齐。
    async fn write_shard(disk: &Arc<dyn DiskAPI>) {
        let mut w = BitrotShardWriter::new(Arc::clone(disk), "part.1".into(), BS);
        for chunk in vec![0x5Au8; PAYLOAD_LEN].chunks(BS) {
            w.push_block(chunk).unwrap();
        }
        w.finish().await.unwrap();
    }

    fn reader(disk: Arc<dyn DiskAPI>) -> BitrotShardReader {
        BitrotShardReader::new(disk, "part.1".into(), BS, PAYLOAD_LEN as u64)
    }

    #[tokio::test]
    async fn reads_back_what_was_written() {
        let (_tmp, disk) = temp_disk();
        write_shard(&disk).await;
        assert_eq!(reader(disk).read_all().await.unwrap(), vec![0x5Au8; PAYLOAD_LEN]);
    }

    #[tokio::test]
    async fn detects_bitrot() {
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;

        // 破坏**数据**字节（偏移 `HASH_LEN` 起是第一个 block 的数据，不是它的摘要）。
        // 破坏摘要字节测出的是另一条路径：那条路径证明不了「重算的数据哈希」真的在比。
        let path = tmp.path().join("part.1");
        let mut raw = std::fs::read(&path).unwrap();
        raw[HASH_LEN] ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();

        let err = reader(disk).read_all().await.unwrap_err();
        assert!(
            matches!(err, DiskError::Corrupt(CorruptKind::BitrotMismatch)),
            "got {err:?}"
        );
    }

    /// 文件被截断 → `Transient(ShortRead)`，**不是** `Corrupt`。
    /// 截断很可能只是写入尚未完成；报成 `Corrupt` 会把它统计进损坏、进而触发 heal（DESIGN §17）。
    #[tokio::test]
    async fn short_file_is_transient_not_corrupt() {
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;

        let path = tmp.path().join("part.1");
        let raw = std::fs::read(&path).unwrap();
        std::fs::write(&path, &raw[..raw.len() - 10]).unwrap();

        let err = reader(disk).read_all().await.unwrap_err();
        assert!(
            matches!(err, DiskError::Transient(TransientKind::ShortRead)),
            "got {err:?}"
        );
    }

    /// 比预期**长**的文件同样是损坏：多出来的字节没人能解释。
    /// 这条同时钉住「读取器确实会比对文件长度」——不做这个检查的实现会读完
    /// 自己需要的字节就返回 `Ok`，把多余的尾巴静默忽略掉。
    #[tokio::test]
    async fn overlong_file_is_corrupt() {
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;

        let path = tmp.path().join("part.1");
        let mut raw = std::fs::read(&path).unwrap();
        assert_eq!(raw.len() as u64, bitrot_size(PAYLOAD_LEN as u64, BS as u64));
        raw.extend_from_slice(&[0xFF; 10]);
        std::fs::write(&path, &raw).unwrap();

        let err = reader(disk).read_all().await.unwrap_err();
        assert!(
            matches!(err, DiskError::Corrupt(CorruptKind::LengthMismatch)),
            "got {err:?}"
        );
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store reader`
Expected: 编译失败

- [ ] **Step 3: 实现**

```rust
pub struct BitrotShardReader {
    disk: Arc<dyn DiskAPI>,
    rel_path: String,
    block_size: usize,
    /// 该分片上应有的原始字节数（不含摘要）。
    shard_len: u64,
}

impl BitrotShardReader {
    /// `shard_len` 是**必须传**的，不是可以从文件推出来的：
    /// 不传的话，读取器无法区分「文件被截断」与「本来就该这么短」。
    /// 按文件长度自行推导块数的实现，一旦推少了就会返回一段**截短的数据**——
    /// 而每一块的摘要各自都是对的，哈希校验根本拦不住它。静默丢数据是最坏的失败模式。
    /// PUT 侧本来就知道这个数（分片原始长度），让它传下来即可。
    pub fn new(
        disk: Arc<dyn DiskAPI>,
        rel_path: String,
        block_size: usize,
        shard_len: u64,
    ) -> Self;

    /// 读回整份分片并逐块校验，返回 `shard_len` 字节的原始数据。
    pub async fn read_all(&self) -> Result<Vec<u8>, DiskError>;
}
```

`read_all` 的判定顺序：

1. `disk.stat(rel_path)`，`None` → `NotFound`；
2. 期望长度 `expected = bitrot_size(shard_len, block_size)`（从 `rstore-checksum` 取，
   与写入器同源）。`actual < expected` → `Transient(ShortRead)`；
   `actual > expected` → `Corrupt(LengthMismatch)`；
3. 一次性 `read_exact_at` 整份（MVP 下分片本来就是内存里的整块）；
4. 逐块：块数 `n = shard_len.div_ceil(block_size)`，块 `k` 的数据长度是
   `block_size`，**但最后一块**是 `shard_len - (n-1) * block_size`；
   偏移 `k * (HASH_LEN + block_size)`，先 32 字节摘要再数据；
   重算 `bitrot_hash(数据)` 与摘要比对，不符 → `Corrupt(BitrotMismatch)`。

> 读取器只对外暴露 `read_all`。原计划让测试调 `read_block(0)`，但那会引出一个
> 越界索引该归哪一类错误的问题——而「调用方传了越界的下标」既不是损坏也不是瞬时故障，
> 硬塞进 `DiskError` 的任何一个变体都是在污染语义（`Corrupt` 尤其糟：会白白触发 heal）。
> 干脆不暴露这个口子。Task 4.7 若需要区段读，那时再带着明确的错误归类来加。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store reader`
Expected: PASS

```bash
git add crates/store/
git commit -m "feat(store): bitrot shard reader with corruption classification

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4.3: ErasureSet 与盘选择

**Files:**
- Create: `crates/store/src/set.rs`
- Create: `crates/store/src/pool.rs`
- Create: `crates/store/src/testutil.rs`（`#![cfg(test)]` 的模块；M4 所有测试共用）
- Modify: `crates/store/src/lib.rs`（加 `pub mod set; pub mod pool;` 与 `#[cfg(test)] mod testutil;`）
- Modify: `crates/disk/src/faulty.rs`（加 `reset_call_count`；原因见下方 `inject_fault_on`）
- Modify: `crates/store/Cargo.toml`（见下方「依赖」）
- Test: `crates/store/src/set.rs` 的 `#[cfg(test)]`

> **依赖（原计划的 Files 清单完全没提，缺了它 M4 一个测试都编译不过）：**
> ```toml
> [dev-dependencies]
> # tempfile 已由 Task 4.1 加过，不要重复声明（Cargo 会合并同名依赖，
> # 重复写只会让 Cargo.toml 里出现两处同名项）。
> rstore-disk = { workspace = true, features = ["fault-injection"] }
> ```
> 第二条是关键。`FaultyDisk` 在 `rstore-disk` 里是 `#[cfg(any(test, feature = "fault-injection"))]`
> 门控的——`rstore-disk` **自己**跑单测时它才存在。store 编译时 `rstore-disk` 是被依赖的
> crate，"它的 test cfg" 根本不生效，所以必须在 store 的 dev-dependencies 里显式开这个 feature。
> （feature 只在测试构建里被打开；正常构建的 store 不会带进故障注入代码。）

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn default_parity_matches_design_table() {
    assert_eq!(default_parity(1), 0);
    assert_eq!(default_parity(2), 1);
    assert_eq!(default_parity(3), 1);
    assert_eq!(default_parity(4), 2);
    assert_eq!(default_parity(5), 2);
    assert_eq!(default_parity(6), 3);
    assert_eq!(default_parity(7), 3);
    assert_eq!(default_parity(8), 4);
    assert_eq!(default_parity(16), 4);
}

#[test]
fn parity_never_exceeds_half() {
    for n in 2..=16u8 {
        assert!(default_parity(n) * 2 <= n, "n={n}");
    }
}

#[test]
fn write_quorum_bumps_on_symmetric_geometry() {
    assert_eq!(write_quorum(4, 2), 4);   // 4+2: 不对称
    assert_eq!(write_quorum(3, 3), 4);   // 3+3: 对称，+1
    // 注意第一个参数是 **total**，不是 data：4+2 配置的 total 是 6，read_quorum = 6 - 2 = 4。
    // （原计划这里写 `read_quorum(4, 2)`，把 data 当成了 total。）
    assert_eq!(read_quorum(6, 2), 4);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store set`
Expected: 编译失败

- [ ] **Step 3: 实现**

```rust
/// DESIGN §10.1 的默认 parity 表。
pub fn default_parity(total: u8) -> u8 {
    match total {
        0 | 1 => 0,
        2..=3 => 1,
        4..=5 => 2,
        6..=7 => 3,
        _ => 4,
    }
}

pub fn read_quorum(total: u8, parity: u8) -> u8 { total - parity }

pub fn write_quorum(data: u8, parity: u8) -> u8 {
    if data == parity { data + 1 } else { data }
}

pub fn delete_quorum(total: u8) -> u8 { total / 2 + 1 }
```

`ErasureSet` 持 `Vec<Option<Arc<dyn DiskAPI>>>`（`None` 表示掉线的盘）、
`data`/`parity`、`CodecCache`。提供 `pick_slot_for(index)` 与 `available_disks()`。

```rust
/// 一个 erasure set 的盘数 = N = data + parity。
/// **盘数与分片数一一对应：每块盘持有一份分片。**
/// 例如 `total = 6, parity = 2` 即 4+2 配置，需要 6 块盘。
pub struct ErasureSet {
    disks: Vec<Option<Arc<dyn DiskAPI>>>,
    data: u8,      // = total - parity
    parity: u8,    // = total - data
    codec_cache: CodecCache,
}

impl ErasureSet {
    /// 槽位视图，下标即分片下标。`None` = 该盘掉线。Task 4.4/4.6/4.7 都要按槽位遍历。
    pub fn disks(&self) -> &[Option<Arc<dyn DiskAPI>>];

    pub fn data(&self) -> u8 { self.data }
    pub fn parity(&self) -> u8 { self.parity }
    pub fn total(&self) -> u8 { self.data + self.parity }
    pub fn read_quorum(&self) -> u8 { read_quorum(self.total(), self.parity) }
    pub fn write_quorum(&self) -> u8 { write_quorum(self.data, self.parity) }
}
```

**测试夹具放在 `crates/store/src/testutil.rs`：**

```rust
//! M4 测试共用的夹具。`#![cfg(test)]` 门控，不进发布产物。

/// 一个已挂好 `FaultyDisk` 的 erasure set。
///
/// **为什么需要这层包装**：`ErasureSet::disks` 存的是 `Arc<dyn DiskAPI>`，
/// 类型已经擦除，拿不回 `FaultyDisk` 去调 `set_fault`。所以夹具必须自己
/// 留一份强类型句柄。原计划让 `ErasureSet` 直接提供 `inject_fault_on(i, fault)`，
/// 那是做不到的——`ErasureSet` 是发布代码，不该认识只在测试里存在的 `FaultyDisk`。
///
/// `Deref<Target = ErasureSet>` 让 `set.put_object(..)`、`set.disks()`、
/// `commit(&set, ..)`（`&TestSet` 自动 deref 成 `&ErasureSet`）都能照常写。
pub struct TestSet {
    set: ErasureSet,
    faulties: Vec<Arc<FaultyDisk>>,
    /// 持有临时目录，随 `TestSet` drop 一起清理。
    _dir: TempDir,
}

impl std::ops::Deref for TestSet {
    type Target = ErasureSet;
    fn deref(&self) -> &ErasureSet { &self.set }
}

impl TestSet {
    /// 往第 `i` 块盘注入故障。`&self`（`FaultyDisk` 内部用 `Mutex`），
    /// 所以测试里 `let set = ...` 不必声明 `mut`。
    ///
    /// **实现时必须先调 `FaultyDisk::reset_call_count()` 再 `set_fault()`。**
    /// `FaultyDisk` 的调用计数是「自构造以来」的累计值，`set_fault` 与 `clear_fault`
    /// 都不重置它（见 `faulty.rs` 的文档）。不重置的话，
    /// 「已经跑过一次 PUT 之后再注入 `FailAfter { calls: 2 }`」会立刻全部失败——
    /// 因为计数器早就超过 2 了，于是测试得不到它想要的那个中断位置。
    /// 在这里统一成「从注入这一刻起再放行 `calls` 次」，语义才与直觉一致。
    pub fn inject_fault_on(&self, i: usize, fault: Fault);
    /// 撤销第 `i` 块盘的故障，回到正常行为。
    pub fn clear_fault_on(&self, i: usize);

    /// 在**每一块**盘上写出 `rel_path`（只有 `write_all` 会失败才跳过，正常情况全成功）。
    ///
    /// 给 `commit` 造出 staging 目录用的：`commit` 做的是 rename，
    /// **源路径不存在时 rename 会以 `NotFound` 失败**。少了这一步，Task 4.4 里
    /// 每块盘的 rename 都会失败、`achieved` 恒为 0——「绝不在低于 quorum 时报告成功」
    /// 那条最重要的不变量测试就会**空洞地通过**（`Ok` 分支一次都进不去）。
    pub async fn write_probe(&self, rel_path: &str, data: &[u8]);
}

/// 建 `total` 块盘、`parity` 为 `parity` 的 set（`data = total - parity`）。
/// **`set_with_disks(6, 2)` 读作「6 块盘、parity=2、data=4」**——整个 M4 的测试都用这个约定。
pub async fn set_with_disks(total: u8, parity: u8) -> TestSet;
```

> **`set_with_disks` 必须给每块盘一个独立的子目录**：盘 `i` 的根是 `{tmp}/disk{i}`，
> 而不是把 `tmp.path()` 直接交给 6 块盘。同根的话这 6 块「盘」其实是同一个目录——
> `disks()[i].write_all(rel, ..)` 写的是同一个文件，「6 副本、掉 2 块还能读」
> 这些性质就全部退化成同义反复：测试照样全绿，但一块盘都没测到。
> Task 4.8 的 `dirs_on(&set, i, ..)` 会对每个 `i` 返回同样的结果，
> 那种断言看起来在逐盘校验，实际上只校验了一遍。

> **原计划这里有个签名打架**：Task 4.3 写的是 `set_with_disks(6, 2)`（盘数, parity），
> Task 4.4 却写成了 `set_with_disks(6, |_| None).await` / `set_with_disks(n, fault)`
> （盘数, 故障闭包）。同名不同签名，两者不可能同时成立。
> **以 `(total, parity)` 为准**——它更简单，且「先建好再逐块注入故障」的表达力不比闭包差
> （还能中途 `clear_fault_on` 再改）。Task 4.4 的测试相应改成先 `set_with_disks(6, 2)`，
> 再按需 `set.inject_fault_on(i, ...)`。别再加第二个同名函数。

**集合路由的 MVP 形态：** `Pool` 持 `Vec<Arc<ErasureSet>>`。MVP 下 `set_count == 1`
（所有盘同属一个 set），因此路由是恒等映射。

```rust
pub struct Pool {
    sets: Vec<Arc<ErasureSet>>,
}

impl Pool {
    /// `sets` 不得为空：一个没有 set 的池子任何操作都做不了，
    /// 让它在构造期就失败，胜过让每个调用点各自处理 `Vec` 为空。
    pub fn new(sets: Vec<Arc<ErasureSet>>) -> Result<Self, StoreError>;
    pub fn sets(&self) -> &[Arc<ErasureSet>];

    /// 按对象键选 set。MVP 下恒等返回第一个。
    pub fn pick_set(&self, key: &str) -> &ErasureSet {
        // MVP: set_count == 1，路由恒等（`key` 未使用）。
        // 多 set 时的 SipHash 路由见 DESIGN §9.2（Phase 3）。
        let _ = key;
        &self.sets[0]
    }
}
```

`Pool::new` 的空切片拒绝要有测试（`assert!(Pool::new(vec![]).is_err())`）——
「恒定映射」的假设靠一个 `sets[0]` 撑着，`sets` 为空就是 panic。`pick_set` 本身
不需要测试：MVP 下它没有分支。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store set`
Expected: PASS

```bash
git add crates/store/ Cargo.lock
git commit -m "feat(store): erasure set geometry, quorum rules, and test fixture

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4.4: 提交协议

**Files:**
- Create: `crates/store/src/commit.rs`
- Modify: `crates/store/src/lib.rs`（加 `pub mod commit;`）
- Test: 同文件 `#[cfg(test)]`

- [ ] **Step 1: 写失败测试**

```rust
use super::*;
use rstore_disk::faulty::{Fault, FaultKind};

use crate::testutil::set_with_disks;

/// 每块盘都失败时用这个：`FailAfter { calls: 0 }` 表示「一次都不成功，第 1 次起就失败」。
fn always_fail() -> Fault {
    Fault::FailAfter { calls: 0, kind: FaultKind::Transient }
}

#[tokio::test]
async fn commits_when_quorum_reached() {
    let set = set_with_disks(6, 2).await;
    // **必须先造出 staging 目录**：`commit` 做的是 rename，源路径不存在时
    // rename 会以 `NotFound` 失败。不写这一步的话 achieved 恒为 0，
    // 所有测试都会以一种「看起来在测、其实什么都没测」的方式失败或通过。
    set.write_probe("b/o/tx1/meta.xl", b"probe").await;

    let r = commit(&set, "b/o/tx1", "b/o/0000", 4).await;
    let outcome = r.expect("6 块盘全健康，必须达到 quorum=4");
    assert_eq!(outcome.achieved, 6);
    assert_eq!(outcome.renamed.len(), 6);

    // 舞台目录确实被搬走了，而不是复制了一份。
    let d = set.disks()[0].as_ref().unwrap();
    assert!(matches!(d.stat("b/o/tx1").await, Ok(None) | Err(DiskError::NotFound)));
    assert!(matches!(d.stat("b/o/0000").await, Ok(Some(_))));
}

#[tokio::test]
async fn fails_and_reports_when_below_quorum() {
    // 前 3 块盘 rename 必失败，只剩 3 块能成功；write_quorum = 4。
    let set = set_with_disks(6, 2).await;
    set.write_probe("b/o/tx1/meta.xl", b"probe").await;
    for i in 0..3 {
        set.inject_fault_on(i, always_fail());
    }

    let r = commit(&set, "b/o/tx1", "b/o/0000", 4).await;
    assert!(
        matches!(r, Err(StoreError::WriteQuorum { achieved: 3, required: 4 })),
        "got {r:?}"
    );
}

/// 回滚必须真的动手：2 块盘 rename 成功后失败，这 2 个目录不能被留下。
/// （若删除本身也失败，残留由对账处理——所以只断言"尽力而为"的可见结果。）
#[tokio::test]
async fn rollback_removes_already_renamed_dirs() {
    let set = set_with_disks(6, 2).await;
    set.write_probe("b/o/tx1/meta.xl", b"probe").await;
    // 0/1 正常 → rename 成功；其余全部立即失败
    for i in 2..6 {
        set.inject_fault_on(i, always_fail());
    }

    let r = commit(&set, "b/o/tx1", "b/o/0000", 4).await;
    assert!(
        matches!(r, Err(StoreError::WriteQuorum { achieved: 2, .. })),
        "got {r:?}"
    );

    for i in [0usize, 1] {
        let d = set.disks()[i].as_ref().expect("这两块盘应当存在");
        assert!(
            matches!(d.stat("b/o/0000").await, Ok(None) | Err(DiskError::NotFound)),
            "盘 {i} 上残留了回滚不掉的目录"
        );
    }
}

/// **硬承诺（DESIGN §12.2）**：只要返回 Ok，成功盘数就不可能低于 write_quorum。
/// 这是全项目最重要的一条不变量。
/// 6 块盘、每块"成功 / 失败"两种状态 → 用位掩码穷举全部 64 种组合，不做抽样。
///
/// 注意这条测试有两个容易写成「空洞通过」的地方，两个都要盯住：
/// 一是忘了 `write_probe`，于是每块盘的 rename 都因源路径不存在而失败、
/// `Ok` 分支一次都进不去；二是只断言 `Ok` 时的 `achieved`，
/// 就没人发现「其实一次都没成功过」。所以下面同时统计 `ok_count`。
#[tokio::test]
async fn never_reports_success_below_quorum() {
    const QUORUM: u8 = 4;
    let mut ok_count = 0usize;

    for mask in 0u32..64 {
        let set = set_with_disks(6, 2).await;
        set.write_probe("b/o/tx1/meta.xl", b"probe").await;
        for i in 0..6 {
            if mask & (1 << i) != 0 {
                set.inject_fault_on(i, always_fail());
            }
        }

        if let Ok(outcome) = commit(&set, "b/o/tx1", "b/o/0000", QUORUM).await {
            ok_count += 1;
            assert!(
                outcome.achieved >= QUORUM,
                "mask={mask:#07b}: 报了成功，但只达成 {} < {QUORUM}",
                outcome.achieved
            );
        }
    }

    // mask=0（全健康）与 mask 中失败盘数 ≤ 2 的那些都必须成功。
    // 若这里变成 0，说明「Ok 分支」根本没被走到，上面的断言全是空转。
    assert!(ok_count > 0, "没有任何一轮达成 quorum，这条测试没有测到东西");
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store commit`
Expected: 编译失败

- [ ] **Step 3: 实现**

```rust
/// 提交结果。达到 quorum 时返回。
///
/// **没有 `Clone`，不是笔误**：`failures` 里装着 `DiskError`，而 `DiskError`
/// （`rstore-common`）只派生了 `Debug, PartialEq, Eq`。给它补 `Clone` 是一条跨 crate 的
/// 改动，而这里没有任何调用方需要克隆提交结果——需要时再补，不要为了对齐一个
/// 顺手写下的 derive 列表去动公共错误类型。
#[derive(Debug, PartialEq, Eq)]
pub struct CommitOutcome {
    /// 成功 rename 的盘数（= `renamed.len()`）。
    pub achieved: u8,
    /// 成功的盘下标。
    pub renamed: Vec<usize>,
    /// 失败的盘下标与原因。**不要丢**——上层要据此把这些盘标记为落后，
    /// 交给反熵 / heal 补数据。丢掉它们等于永远不知道谁落后了。
    pub failures: Vec<(usize, DiskError)>,
}

/// 达到 quorum 则返回 `Ok(CommitOutcome)`，否则回滚并返回 `Err(WriteQuorum)`。
///
/// 硬承诺（DESIGN §12.2）：**绝不在低于 quorum 时报告成功**。
/// 回滚是 best-effort：失败时的残留由对账流程清理，本函数不保证不留字节。
pub async fn commit(
    set: &ErasureSet,
    staging_rel: &str,
    final_rel: &str,
    write_quorum: u8,
) -> Result<CommitOutcome, StoreError> {
    let mut renamed: Vec<usize> = Vec::new();
    let mut failures: Vec<(usize, DiskError)> = Vec::new();

    // 并行 rename 到所有可用盘（下面注记说明串行/并行）
    for (i, disk) in set.disks().iter().enumerate() {
        match disk {
            // 盘离线是「暂时够不着」，不是「确定性缺失」——归 Transient，
            // 免得被 heal 当成损坏统计进去（DESIGN §17）。
            None => failures.push((i, DiskError::Transient(TransientKind::Io))),
            Some(d) => match d.rename(staging_rel, final_rel).await {
                Ok(()) => renamed.push(i),
                Err(e) => failures.push((i, e)),
            },
        }
    }

    let achieved = renamed.len() as u8;
    if achieved >= write_quorum {
        Ok(CommitOutcome { achieved, renamed, failures })
    } else {
        // 尽力回滚：只清理我们自己 rename 过去的那些
        for i in &renamed {
            let _ = set.disks()[*i].as_ref().unwrap().remove_dir_all(final_rel).await;
        }
        Err(StoreError::WriteQuorum { achieved, required: write_quorum })
    }
}
```

> **`final_rel` 必须是本次写入独有的路径。** 回滚做的是
> `remove_dir_all(final_rel)`——如果两次写入共用同一个 `final_rel`，
> 回滚会把上一次已经提交成功的数据一起删掉。所以覆盖写不是「rename 到同一个名字」，
> 而是「rename 到新的版本目录，成功之后再由 Task 4.8 处理旧版本」。
> 顺带一提：这也意味着 `rename` 的目标目录通常**不该已存在**，正好和
> `std::fs::rename` 对非空目标目录会失败的行为对得上。

> **注意并发写法**：上面是串行伪代码，便于阅读与测试。实现时应换成
> `futures::future::join_all` 并行发起，但**判定逻辑一字不改**——
> 尤其是「先统计成功数、再决定回滚」的顺序，不要写成「遇到第一个失败就提前返回」。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store commit`
Expected: PASS

```bash
git add crates/store/
git commit -m "feat(store): rename commit protocol with best-effort rollback

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4.5: PUT 路径

**Files:**
- Create: `crates/store/src/put.rs`
- Modify: `crates/store/src/lib.rs`（加 `pub mod put;`）
- Modify: `crates/store/Cargo.toml`（`[dependencies]` 加 `md-5.workspace = true`；见下方 etag）
- Modify: `Cargo.toml`（`[workspace.dependencies]` 加 `md-5 = "0.10"`）
- Modify: `crates/meta/src/fileinfo.rs`（加 `encode_body` / `decode_body`，见下方「body 编解码」）
- Modify: `crates/meta/src/lib.rs`（把这两个函数加进再导出列表）
- Test: 同文件 `#[cfg(test)]`

> **前置**：4.1（写入器）、4.2（读取器）、4.3（`TestSet` 夹具）、4.4（`commit`）都必须已落地。
> 而 4.3 的夹具依赖 Task 3.4 的 `FaultyDisk`，所以实际顺序是
> **4.1 → 4.2 → 3.4 → 4.3 → 4.4 → 4.5**。

#### 先补一个缺口：`rstore-meta` 没有公开的 body 编解码入口

`ShallowVersion::body` 的类型是 `OpaqueBody`，而 `OpaqueBody = Vec<u8>`——
**「不透明」是对外的承诺，但总得有人知道它里面是什么。** 现在整个工作区里
唯一一处 `rmp_serde::to_vec(&ObjectBody { .. })` 在 `fileinfo.rs` 的 `#[cfg(test)]` 里，
`crates/meta/src/lib.rs` 只再导出了 `encode_header` / `decode_header`：

```
$ grep -rn "encode_body\|decode_body" crates/meta/src/     # 无输出
```

这意味着**本任务和 Task 4.7 都走不通**：
- PUT 要构造 `ShallowVersion { header, body }`，`body` 得由 `ObjectBody` 编出来——**造不出**；
- GET 要从元数据里取 `ec_dist`（拼接分片顺序）和 `parts` 的长度——**读不回**。

两条路都别走：**不要让 `crates/store` 直接依赖 `rmp-serde` 自己编**。
那等于把「body 是 msgpack」这个线格式事实从 meta 层泄漏到 store 层，
而且 store 还得自己把 `rmp_serde` 的错误映射成 `DiskError::Corrupt(MalformedHeader)`——
同一套映射 meta 已经写过两遍（`encode_header` / `decode_header`），第三遍必然分叉。
更糟的是日后换编码（比如为了省 CPU 换成某二进制格式）要改两个 crate。

正确做法是让**拥有格式的 crate 提供入口**，在 `crates/meta/src/fileinfo.rs` 加：

```rust
/// 把 `ObjectBody` 编成 `ShallowVersion::body` 的线格式。
pub fn encode_body(body: &ObjectBody) -> Result<OpaqueBody, DiskError> {
    rmp_serde::to_vec(body).map_err(|_| DiskError::Corrupt(CorruptKind::MalformedHeader))
}

/// 解析 `ShallowVersion::body`。**读 `ec_dist` / `parts` 的唯一入口。**
pub fn decode_body(bytes: &[u8]) -> Result<ObjectBody, DiskError> {
    rmp_serde::from_slice(bytes).map_err(|_| DiskError::Corrupt(CorruptKind::MalformedHeader))
}
```

并在 `crates/meta/src/lib.rs` 的 `pub use fileinfo::{...}` 里加上这两个名字。
这两个函数各自补一条 roundtrip 单测（含「垃圾字节 → `Corrupt(MalformedHeader)`」），
和 `encode_header` 的测试并排放在 `fileinfo.rs` 的测试模块里。

> 错误映射必须与 `encode_header`（`fileinfo.rs:161`）**逐字一致**：用
> `DiskError::Corrupt(CorruptKind::MalformedHeader)`，不要新造一个 kind。
> DESIGN §17 规定 heal 由「观察到 `Corrupt`」触发，多一个 kind 就多一条没人处理的路径。

#### 对象目录布局（M4 起冻结；GET / DELETE / 对账都按它找路）

```
<bucket>/<key>/<data_dir 的 uuid>/meta.xl        ← Task 4.6 的容器，PUT 一次写入
<bucket>/<key>/<data_dir 的 uuid>/part.1         ← 该盘的**全部** block 串成的单个分片文件
<bucket>/<key>/.staging-<txid>/…                 ← 写入中的暂存目录，提交前对读路径不可见
```

- `part.1` 这个名字在每块盘上都一样，但**内容各不相同**：它装的是「落在本盘上的那一份分片」。
- 单部分对象（multipart 是 Phase 3）永远只有一个 `part.1`；多 block 只是把它写得更长。
- 一个版本一个目录。`data_dir` 同时写进 header 的 `data_dir` 字段，所以**拿到元数据就知道目录名**，
  不需要目录名与元数据之间的二次索引。
- **暂存目录必须以 `.staging-` 开头。** 这不是命名偏好，是正确性前提：发现逻辑（4.7）
  会跳过这个前缀的条目，从而保证「能被投票的目录」一定已经提交过。
  如果没有这个前缀，一次「6 块盘都写完了暂存 meta、还没提交就崩了」的 PUT，
  会让这个半成品目录在 6 块盘上各得一票、轻松越过 `read_quorum`——读到的就是半截数据。
  详细推导见 Task 4.10。
- meta 与 part 都先写进暂存目录，再由 `commit` 一次 rename 提交——
  这样「元数据可见」与「分片可见」是同一个原子事件。
  （4.4 里的 `write_probe` 只是给不写数据的提交测试造现场，PUT 自己会造。）

#### 分片几何（读侧必须能独立复算，否则解不出来）

- `BLOCK_SIZE: usize = 1 << 20`（1 MiB）。对象按它切块：
  `n = max(1, size.div_ceil(BLOCK_SIZE))`。
- 第 `k` 块的明文长度 `L_k`：除最后一块外都是 `BLOCK_SIZE`，最后一块是
  `size - (n-1) * BLOCK_SIZE`。
- 第 `k` 块的**分片长度** `shard_size_k = even_ceil(div_ceil(L_k, data))`，
  `even_ceil(x) = x + (x & 1)`。**必须取偶数**：`Codec::new` 要求 `shard_size`
  为正偶数，而 `1 MiB / 3` 这类除不尽的情形不取整就构造不出编解码器。
- 第 `k` 块拆成 `data` 个分片：分片 `i` 取 `[i * shard_size_k, (i+1) * shard_size_k)`，
  **不足处补零**。拼回时是「`data` 个分片顺序相接，再截断到 `L_k`」——
  也就是说补齐的零只可能出现在最后一个分片的尾部，这一条是读侧截断规则的依据。
- 因此**每块盘的分片明文总长** `shard_len = Σ_k shard_size_k`，
  由 `(size, data, BLOCK_SIZE)` 三者完全决定，读侧可以独立复算。
- **传给 `BitrotShardWriter` / `BitrotShardReader` 的 `block_size` 是「满块的分片长度」**，
  一个对象只算一次：

  ```rust
  /// 用于 bitrot 分块的分片步长。取 `min(size, BLOCK_SIZE)` 那一档，
  /// 于是 `n == 1`（整对象不足一个满块）时它就是这一块自己的分片长度。
  fn shard_step(size: u64, data: u8) -> u64 {
      even_ceil(div_ceil(size.min(BLOCK_SIZE as u64), data as u64))
  }
  ```

  最后一块的 `shard_size_k` 必不大于它（`even_ceil` 单调，而 `L_last ≤ BLOCK_SIZE`），
  所以读取器「除最后一块外长度一律等于 `block_size`」的假设成立。
- 分片在盘上的顺序就是块序：`part.1` = `[hash][block0 本盘分片][hash][block1 本盘分片] …`。

#### 槽位映射

`dist = distribution(&object_key, N)`，其中 `object_key = format!("{bucket}/{key}")`——
**必须把 bucket 一起喂进去**，只用 key 的话同名对象在不同桶里退化成同一套分布。
`dist` 是 `1..=N` 的排列，`dist[k] - 1` 是**第 k 号分片**所在的物理盘下标，
分片编号沿用 `codec.encode` 的输出顺序：`0..data` 是数据分片，`data..N` 是校验分片。
反查：盘 `d` 持有分片 `k` ⟺ `dist[k] == d + 1`。PUT 与 GET 必须用同一套映射，
`distribution` 的输出还要过一遍 `is_valid_distribution`，不通过 → `StoreError::Internal`。

#### etag

`etag = hex(md5(data))`（小写十六进制）。**必须是真的 MD5**，不能是「内容哈希」之类的
自造摘要：S3 客户端（`aws-cli` / `mc` / `rclone`）单部分上传后比对的就是 MD5，自造摘要
会让它们在 PUT 成功之后报校验失败。这条到 M5 才发现的话，返工要重写整个 PUT 路径。

#### 操作契约

```rust
pub const BLOCK_SIZE: usize = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutArgs {
    pub bucket: String,
    pub key: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOut {
    pub size: u64,
    pub etag: String,
    /// 本版本的数据目录名，同时是 header 里的 `data_dir`。
    pub data_dir: Uuid,
    pub version_id: Uuid,
}

impl ErasureSet {
    pub async fn put_object(&self, args: PutArgs) -> Result<PutOut, StoreError>;
}
```

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::testutil::{set_with_disks, TestSet};

    /// 某块盘上 `bucket/key/<data_dir>` 里的文件名（已排序）。
    /// 走 `DiskAPI::list_dir` 而不是直接碰文件系统：夹具里的盘可能被 `FaultyDisk`
    /// 包过，`list_dir` 才是被测试的那条路径。
    async fn list_data_dir(
        set: &TestSet,
        disk_idx: usize,
        key_rel: &str,
        data_dir: &Uuid,
    ) -> Vec<String> {
        let d = set.disks()[disk_idx].as_ref().expect("该盘应当在线");
        d.list_dir(&format!("{key_rel}/{data_dir}")).await.unwrap()
    }

    #[tokio::test]
    async fn put_small_object_inlines_it() {
        let set = set_with_disks(6, 2).await;
        let data = vec![1u8; 1000];
        let out = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "small".into(),
                data: data.clone(),
            })
            .await
            .unwrap();

        assert!(!out.etag.is_empty());
        assert_eq!(out.size, 1000);
        // 内联对象的数据在 meta.xl 里，**不该有分片文件**。只断言 etag/size
        // 是不够的——那样「悄悄走了大对象路径」的实现照样能过。
        for i in 0..6 {
            let files = list_data_dir(&set, i, "b/small", &out.data_dir).await;
            assert_eq!(files, vec!["meta.xl".to_string()], "disk {i}");
        }

        // 「内联了」不等于「内联对了」：把 meta.xl 读回来解一遍。
        // 本任务还不能用 GET（那是 Task 4.7），所以直接走 meta 容器的解码。
        let d = set.disks()[0].as_ref().unwrap();
        let raw = d
            .read_exact_at(
                &format!("b/small/{}/meta.xl", out.data_dir),
                0,
                d.stat(&format!("b/small/{}/meta.xl", out.data_dir))
                    .await
                    .unwrap()
                    .unwrap()
                    .size as usize,
            )
            .await
            .unwrap();
        let meta = rstore_meta::decode(&raw).unwrap();
        // 无版本化时版本键是 "null"（DESIGN §8.4）。
        assert_eq!(meta.inline.get("null"), Some(&data[..]));
    }

    #[tokio::test]
    async fn put_large_object_creates_shards() {
        let set = set_with_disks(6, 2).await;
        let data = vec![7u8; 1_500_000]; // 2 个 block：1 MiB + 451 424
        let out = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "big".into(),
                data: data.clone(),
            })
            .await
            .unwrap();
        assert_eq!(out.size, 1_500_000);

        // 单部分对象：每块盘的数据目录里恰好一个分片文件 part.1。
        // 原计划这里写「不是 6 个！」，是因为当时没定清楚 part.N 的 N 指什么——
        // N 是**部分号**（multipart 的 part），不是盘号；MVP 只有一部分。
        for i in 0..6 {
            let files = list_data_dir(&set, i, "b/big", &out.data_dir).await;
            assert_eq!(
                files,
                vec!["meta.xl".to_string(), "part.1".to_string()],
                "disk {i}"
            );
        }

        // 盘上字节数必须等于读侧能独立复算出来的那个数。写侧算错几何的话，
        // GET 会以 Transient(ShortRead) 或 Corrupt 收场，而那时错误现场已经离原因很远了。
        let step = shard_step(out.size, 4);
        let shard_len = expected_shard_len(out.size, 4);
        let expect_on_disk = rstore_checksum::bitrot_size(shard_len, step);
        for i in 0..6 {
            let d = set.disks()[i].as_ref().unwrap();
            let st = d
                .stat(&format!("b/big/{}/part.1", out.data_dir))
                .await
                .unwrap()
                .expect("part.1 必须存在");
            assert_eq!(st.size, expect_on_disk, "disk {i}");
        }
    }

    /// etag 是给 S3 客户端比对的，必须是真 MD5。这条用已知向量钉死，
    /// 免得后来有人「优化」成自造摘要——那会让客户端在 PUT 成功后报校验失败。
    #[test]
    fn etag_is_lowercase_hex_md5() {
        assert_eq!(etag_of(b"hello"), "5d41402abc4b2a76b9719d911017c592");
        assert_eq!(etag_of(b""), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[tokio::test]
    async fn put_fails_below_write_quorum() {
        let set = set_with_disks(6, 2).await;
        for i in 0..3 {
            set.inject_fault_on(i, Fault::Offline);
        }
        let r = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                data: vec![0u8; 1_000_000],
            })
            .await;
        // `WriteQuorum` 是 struct 变体，`matches!` 里必须带 `{ .. }`（原计划漏了，
        // 那样写根本编译不过）。
        assert!(matches!(r, Err(StoreError::WriteQuorum { .. })), "got {r:?}");

        // 越界守卫：低于 quorum 时**一块盘都不该留下已提交的最终版本目录**。
        //
        // 断言的是 `b/k` 而**不是** `b`。暂存目录是 `b/k/.staging-<txid>`，
        // 只要往某个 key 写过字节，`b` 下就必然有 `k` 这一层——`list_dir("b")`
        // 永远返回 `["k"]`。原计划断言的正是 `b`，那条在「失败不清理」的实现下
        // **不可能成立**（实现时实测：`disk 3 上残留了 ["k"]`）；
        // 要让它成立只能删掉 `b/k`，而覆盖写场景下那会连**已提交的旧版本**一起删掉。
        // 它真正想守的是「没有可见的最终版本目录」，所以把 `.staging-*` 排除掉再看。
        for i in 0..6 {
            let d = set.disks()[i].as_ref().unwrap();
            let entries = match d.list_dir("b/k").await {
                Ok(v) => v,
                Err(rstore_disk::DiskError::NotFound) => Vec::new(),
                Err(e) => panic!("disk {i} list_dir 失败: {e:?}"),
            };
            let committed: Vec<_> = entries
                .iter()
                .filter(|e| !e.starts_with(".staging-"))
                .collect();
            assert!(
                committed.is_empty(),
                "disk {i} 上出现了已提交的最终版本目录: {committed:?}"
            );
        }
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store put`
Expected: 编译失败（`put_object` / `etag_of` / `expected_shard_len` 都还不存在）

- [ ] **Step 3: 实现**

```rust
/// `even_ceil`、`div_ceil`、`expected_shard_len`、`etag_of` 都是本模块的自由函数，
/// 因为 GET 侧（Task 4.7）要复算同一套几何，测试也要用。
pub(crate) fn even_ceil(x: u64) -> u64 { x + (x & 1) }
pub(crate) fn shard_step(size: u64, data: u8) -> u64 { … }        // 见上
pub(crate) fn expected_shard_len(size: u64, data: u8) -> u64 { … } // 见下
pub(crate) fn etag_of(data: &[u8]) -> String { … }                // hex(md5)
```

`expected_shard_len` 的算法就是几何那一段的直接翻译，不另设规则：

```rust
pub(crate) fn expected_shard_len(size: u64, data: u8) -> u64 {
    if size == 0 {
        return 0;
    }
    let n = size.div_ceil(BLOCK_SIZE as u64);
    let full = shard_step(size, data);
    // 除最后一块外全是满块，长度一样。
    let last_l = size - (n - 1) * BLOCK_SIZE as u64;
    (n - 1) * full + even_ceil(last_l.div_ceil(data as u64))
}
```

`put_object` 的流程：

1. `txid = Uuid::new_v4()`、`data_dir = Uuid::new_v4()`；
   `staging = format!("{bucket}/{key}/.staging-{txid}")`、
   `final_rel = format!("{bucket}/{key}/{data_dir}")`。
2. `dist = distribution(&format!("{bucket}/{key}"), N)`；`is_valid_distribution` 不过 → `Internal`。
3. **先数盘**：可用盘（`disks()` 里 `Some` 且 `is_local` 不做额外过滤）数 `< write_quorum`
   → 直接 `WriteQuorum`。连编码都别做——先编码再发现写不下去，等于白烧 CPU。
4. **内联分支**：`rstore_common::consts::should_inline(size, /* versioned_bucket = */ false)`。
   构造 `ObjectMeta`（版本键 `"null"`，`inline["null"] = data`，
   `header.flags` 置 `INLINE_DATA`，body 的 `meta_sys` 写 `keys::INLINE_DATA` 标记），
   `rstore_meta::encode(&meta)` → `disk.write_all("{staging}/meta.xl", &bytes)`。
   **不要再写一份 `should_inline` 的阈值**：`consts` 里已经有一份，两份阈值必然漂移，
   而漂移的后果是「写的时候按大对象、读的时候按内联」这种最难查的错。
5. **大对象分支**：
   a. 循环外为每块盘建一个 `BitrotShardWriter::new(disk, "{staging}/part.1", step as usize)`；
   b. 逐 block：按几何拆 `data` 个分片（补零）→
      `codec_cache.get(data as usize, parity as usize, shard_size_k as usize)` →
      `codec.encode(&shards)` 得 `parity` 个校验分片；
   c. 把 `N` 个分片按 `dist` 派到各自盘，`push_block(本盘那一份)`；
   d. 维护每块盘的 `shard_len` 累加值（= 它那份分片的明文总长），写进 `part` 的
      `PartInfo.size` / `actual_size`。
   e. 每个块的 `shard_size_k` 不同 → `codec_cache` 会缓存多个 `Codec`，这是预期行为。
6. 所有 writer `finish()`，收集成功盘数；`< write_quorum` → `WriteQuorum`。
   **注意这里不要「清理已写的分片」**：失败时留下的暂存目录由对账流程回收
   （DESIGN §12.2），主动清理反而会把 Task 4.10 想观察的崩溃残留抹掉，
   让「残留可被识别」那条不变量测试变成空转。
7. 写 `{staging}/meta.xl`：header 的 `size` / `ec_m` / `ec_n` / `data_dir` / `flags` /
   `version_id`，body 的
   `parts = [PartInfo { number: 1, size: shard_len, actual_size: size, etag, index: None }]`、
   `ec_dist = dist`、`checksum_algo = ChecksumAlgo::Crc32c`、`storage_class = Standard`。
   `ShallowVersion::body` 用 `rstore_meta::encode_body(&body)` 得到——**不要自己调 `rmp_serde`**，
   理由见上文「body 编解码」。
   `header.version_id = Some(version_id)`、`header.ty = VersionType::Object`、
   `header.flags` 需置 `USES_DATA_DIR`（本版本确实用了数据目录）。
   **`header.mod_time` 必须写**（`SystemTime::now()` 的纳秒数）——GET 靠它在一个 key
   存在多个版本目录时选出最新的那个（见 4.7）。全程留 `None` 的话，覆盖写之后
   「哪一份是新的」就没有比较依据了。
8. `commit(&set, &staging, &final_rel, write_quorum)`。
9. 返回 `PutOut { size, etag, data_dir, version_id }`（`version_id` 即 header 的 `version_id`）。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store put`
Expected: PASS

```bash
git add crates/store/ Cargo.toml Cargo.lock
git commit -m "feat(store): PUT path with erasure encoding and inline fast path

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

### Task 4.6: 元数据仲裁

**Files:**
- Create: `crates/store/src/quorum.rs`
- Modify: `crates/store/src/lib.rs`（加 `pub mod quorum;`）
- Modify: `crates/store/Cargo.toml`（`[dependencies]` 加 `tracing.workspace = true`——
  某一票因编码失败作废时要记一笔；`tracing` 已在 `[workspace.dependencies]` 里，
  M6 的 server 也要用，这里只是让 store 也能用）
- Test: 同文件 `#[cfg(test)]`

> **不需要给 store 加 `rmp-serde` 依赖**（原计划的 Files 清单里有这条）。
> Task 4.5 已经把 `rstore_meta::encode_body` / `decode_body` 加了出来——
> 测试用它们造样本，和生产代码走的是同一条路径，比测试自己调 `rmp_serde` 更贴近真实。
> 顺带也就不用把「body 是 msgpack」这个事实引进 store 的依赖图。

#### 身份判定：直接比元数据的**线格式字节**

`resolve_metadata` 要回答的是「这几块盘上的元数据是不是同一份」。判定方式就是
`rstore_meta::encode(meta)` 的结果做 key —— 两份元数据一字不差时，编码出来必然逐字节相同；
只要有一个字段不同，编码就不同。

**不要引入额外摘要**（原计划写「SHA-256 over size/flags/mod_time/…」）：那条路要求先把字段
挑出来再拼一遍，挑漏一个字段就是「两份不同的元数据被当成同一份」，而且要为此新增一个哈希依赖。
容器的 `encode` 本来就是确定性的（固定数组 + 有序 map），字节相等就是最精确的判定。

> **由此得出一条必须写进 `quorum.rs` 文档注释的规则：`ObjectBody.meta_sys` 里不得放
> 逐盘可变的字段。** 身份是整份字节相等，任何一块盘上多出一个字节的差异都会被算成
> 「另一种元数据」，进而把一次本来健康的读打成 `ReadQuorum`。
> Phase 4 的 heal / purge 状态因此**不能**写进 `meta_sys`，要走 sidecar 文件。
> 这条规则有测试守着（下面 `meta_sys_differences_split_the_vote`），不是口头约定。

#### 操作契约

```rust
/// 在 `metas` 上做多数决。`metas` 的长度即盘数 `total`；
/// `None` 表示该盘**没有返回**（既不计票，也不计为失败——它与「返回了一份不匹配的元数据」
/// 是完全不同的两件事，而后者要计进各自的组）。
///
/// `read_quorum = metas.len() - parity`。票数最高的组达不到它就返回 `ReadQuorum`。
/// 注意这里**不存在「多数决就返回第一名」的兜底**：3 票对 2 票对 1 票时第一名只有 3 票，
/// 低于 4 就得以错误收场，不能因为「它最多」就把它扶正——那是把一次不确定的读
/// 伪装成确定的结果。
pub fn resolve_metadata(
    metas: &[Option<ObjectMeta>],
    parity: u8,
) -> Result<ObjectMeta, StoreError>;
```

原计划给了 `resolve_metadata` 与 `resolve_metadata_opt` 两个函数，前者只是后者给每个元素套了
`Some` —— 两个入口、同一套逻辑，调用方还得先想清楚自己有几种缺失。**合并成一个**。

原计划末尾的「用 `u16` 位图做早停优化（`N ≤ 16`，零分配）」**删掉**：这里的输入本来就是
一块内存里的 `&[Option<ObjectMeta>]`（所有盘都已经并行读完并反序列化完了），
「早停」省不掉任何 IO；`N ≤ 16` 的规模下位图与 `Vec` 的差别测不出来。
留一句注释说明为什么不早停，比留一段没人能验证收益的优化代码好。

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    // 这些名字都在 `rstore_meta` 的 crate 根上（lib.rs 有 `pub use fileinfo::{…}`），
    // 不用写 `fileinfo::` 前缀。
    use rstore_meta::{
        ChecksumAlgo, FileVersionHeader, Flags, ObjectBody, ObjectMeta, ShallowVersion,
        StorageClass, VersionType,
    };

    use super::*;
    use crate::error::StoreError;

    /// 造一份确定的元数据。`tag` 只改 `meta_user` 里一个键——
    /// 这是「两个不同版本」的最小可分辨差异。
    fn meta_with_tag(tag: &str) -> ObjectMeta {
        let header = FileVersionHeader {
            size: 1000,
            ec_m: 4,
            ec_n: 6,
            flags: Flags::USES_DATA_DIR,
            ..Default::default()
        };
        let body = ObjectBody {
            id: None,
            parts: Vec::new(),
            ec_dist: vec![1, 2, 3, 4, 5, 6],
            checksum_algo: ChecksumAlgo::Crc32c,
            storage_class: StorageClass::Standard,
            meta_user: [("tag".to_string(), tag.to_string())].into_iter().collect(),
            meta_sys: Default::default(),
        };
        let body_bytes = rstore_meta::encode_body(&body).unwrap();
        ObjectMeta {
            versions: vec![ShallowVersion { header, body: body_bytes }],
            inline: Default::default(),
            meta_ver: 1,
        }
    }

    fn some(metas: Vec<ObjectMeta>) -> Vec<Option<ObjectMeta>> {
        metas.into_iter().map(Some).collect()
    }

    #[test]
    fn identical_metadata_wins_quorum() {
        // total=6, parity=2 → read_quorum=4。4 票 a 达到 quorum。
        let metas = vec![
            meta_with_tag("a"), meta_with_tag("a"), meta_with_tag("a"),
            meta_with_tag("a"), meta_with_tag("b"), meta_with_tag("b"),
        ];
        let r = resolve_metadata(&some(metas), 2).unwrap();
        assert_eq!(r, meta_with_tag("a"));
    }

    #[test]
    fn minority_metadata_cannot_win() {
        // 3 票 < read_quorum 4 → 必须报错，不能「多数决」直接返回少数派。
        let metas = vec![
            meta_with_tag("a"), meta_with_tag("a"), meta_with_tag("a"),
            meta_with_tag("b"), meta_with_tag("c"), meta_with_tag("b"),
        ];
        // struct 变体在 `matches!` 里必须带 `{ .. }`（原计划漏了，那样编译不过）。
        assert!(matches!(
            resolve_metadata(&some(metas), 2),
            Err(StoreError::ReadQuorum { achieved: 3, required: 4 })
        ));
    }

    #[test]
    fn no_quorum_is_an_error() {
        let metas = some(vec![meta_with_tag("a"), meta_with_tag("b"), meta_with_tag("c")]);
        // total=3, parity=1 → read_quorum = 3 - 1 = 2，三组各 1 票，谁都不够。
        assert!(matches!(
            resolve_metadata(&metas, 1),
            Err(StoreError::ReadQuorum { achieved: 1, required: 2 })
        ));
    }

    #[test]
    fn missing_disks_are_not_failures() {
        // 2 块盘没返回、4 块一致 → 成功。`None` 不是失败，也不占票。
        let metas = vec![
            Some(meta_with_tag("a")), None,
            Some(meta_with_tag("a")), None,
            Some(meta_with_tag("a")), Some(meta_with_tag("a")),
        ];
        assert!(resolve_metadata(&metas, 2).is_ok());
    }

    /// 这条守的是「`meta_sys` 不得放逐盘可变字段」这条规则。它断言的**正是**
    /// 「差异会拆票」这个看起来不友好的行为：等到 Phase 4 有人往 `meta_sys` 里塞
    /// heal 状态时，这里会先红一次，逼他去看 `quorum.rs` 顶上那段说明。
    #[test]
    fn meta_sys_differences_split_the_vote() {
        let mut healing = meta_with_tag("a");
        let mut body = rstore_meta::decode_body(&healing.versions[0].body).unwrap();
        body.meta_sys.insert("x-rs-healing".into(), b"true".to_vec());
        healing.versions[0].body = rstore_meta::encode_body(&body).unwrap();

        let metas = vec![
            meta_with_tag("a"), meta_with_tag("a"), meta_with_tag("a"),
            healing,
            meta_with_tag("a"), meta_with_tag("a"),
        ];
        // 5 票对 1 票，仍然达到 quorum=4——单个盘的差异不会**立刻**打垮读。
        assert!(resolve_metadata(&some(metas), 2).is_ok());

        // 但只要差异达到 parity+1 块盘，读就失败。这就是为什么逐盘可变字段不能进 meta_sys。
        let mut all_diff = Vec::new();
        for _ in 0..3 {
            all_diff.push(meta_with_tag("a"));
        }
        for _ in 0..3 {
            let mut m = meta_with_tag("a");
            let mut b = rstore_meta::decode_body(&m.versions[0].body).unwrap();
            b.meta_sys.insert("x-rs-healing".into(), b"true".to_vec());
            m.versions[0].body = rstore_meta::encode_body(&b).unwrap();
            all_diff.push(m);
        }
        assert!(matches!(
            resolve_metadata(&some(all_diff), 2),
            Err(StoreError::ReadQuorum { .. })
        ));
    }

    /// 身份判定的**全部依据**是「容器编码逐字节相等」。这条钉住编码是确定性的：
    /// 同一份内存元数据编两次必须一模一样。若哪天有人给容器编码引入时间戳、
    /// 随机顺序的 map 或指针地址，这里会红——而那时所有读都会开始报 ReadQuorum。
    #[test]
    fn identity_is_the_wire_encoding_and_is_deterministic() {
        let a = meta_with_tag("a");
        let b = meta_with_tag("a");
        assert_eq!(rstore_meta::encode(&a).unwrap(), rstore_meta::encode(&b).unwrap());
        assert_eq!(rstore_meta::encode(&a).unwrap(), rstore_meta::encode(&a).unwrap());
    }

    /// `VersionType::DeleteMarker` 必须能被编码进容器——DELETE（Task 4.8）靠它。
    /// 顺带钉住「删除标记也是一份可投票的元数据」。
    #[test]
    fn delete_marker_metadata_round_trips() {
        let mut m = meta_with_tag("a");
        m.versions[0].header.ty = VersionType::DeleteMarker;
        m.versions[0].header.size = 0;
        let bytes = rstore_meta::encode(&m).unwrap();
        assert_eq!(rstore_meta::decode(&bytes).unwrap(), m);
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store quorum`
Expected: 编译失败

- [ ] **Step 3: 实现**

```rust
pub fn resolve_metadata(
    metas: &[Option<ObjectMeta>],
    parity: u8,
) -> Result<ObjectMeta, StoreError> {
    // 用线格式字节当 key。HashMap<Vec<u8>, (u8, &ObjectMeta)> 即可；
    // 元素个数 ≤ 16，不需要任何优化结构。
    // 编码失败（理论上不会，但 encode 返回 Result）→ 该盘这一票作废，
    // 按「未返回」处理，并在 tracing 里记一笔。
    …
    let total = metas.len() as u8;
    let read_quorum = total.saturating_sub(parity);
    // 票数最高的组若 < read_quorum → ReadQuorum { achieved, required }
}
```

`achieved` 填**获胜组的票数**（不是「返回了元数据的盘数」）：读失败时运维要看到的是
「最强的那份共识有多强」，不是「有几块盘活着」。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store quorum`
Expected: PASS

```bash
git add crates/store/
git commit -m "feat(store): metadata quorum over wire-encoding identity

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

### Task 4.7: GET 路径

**Files:**
- Create: `crates/store/src/get.rs`
- Modify: `crates/store/src/error.rs`（**加 `NotFound` 变体**——GET / DELETE / 对账都要用它）
- Modify: `crates/store/src/lib.rs`（加 `pub mod get;`）
- Test: 同文件 `#[cfg(test)]`

> **`StoreError` 缺 `NotFound`**：4.1 定的五个变体里没有它，而「对象不存在」既不是
> 磁盘错误、也不是 quorum 不足、更不是布局或内部错误。硬塞进任何一个都会让调用方
> 分不清「真的没有」和「读失败」——M5 要把这两者映射成完全不同的 HTTP 状态码
> （404 vs 500）。所以本任务先把变体补上：
>
> ```rust
> #[error("not found")]
> NotFound,
> ```

#### 契约

```rust
/// 闭区间 `[start, end]`。M5 的 S3 层负责把 `bytes=a-b` / `bytes=a-` / `bytes=-n`
/// 三种写法解析并裁剪到这个形状，store 层只认闭区间。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetOut {
    /// **请求范围内**的字节。不是整个对象。
    pub data: Vec<u8>,
    /// 整个对象的原始长度（不是 `data.len()`）——S3 的 `Content-Range` 要它。
    pub size: u64,
    pub etag: String,
    pub data_dir: Uuid,
}

impl ErasureSet {
    /// `range` 为 `None` 时返回整个对象。
    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<GetOut, StoreError>;
}
```

**「发现 + 仲裁 + 选出胜出目录」要抽成一个可复用的函数**，因为 Task 4.8 的 GC 与
Task 4.10 的对账都要问同一个问题「现在哪个目录是权威的」：

```rust
/// `resolve_version` 的结果。**三态，不是一个 `Option`**。
pub(crate) enum Resolved {
    /// 权威版本目录 + 它的元数据。**可能是删除标记**——用 `live()` 判要不要读。
    Version { dir: String, meta: rstore_meta::ObjectMeta },
    /// 所有盘上都没有这个 key 的任何候选目录（从来没写过，或已被对账清空）。
    Absent,
}

impl Resolved {
    /// 有对象可读时返回 `(权威目录名, 元数据)`；`Absent` 与「最新版本是删除标记」
    /// 都返回 `None`。
    pub(crate) fn live(&self) -> Option<(&str, &rstore_meta::ObjectMeta)>;
}

/// 找到 `bucket/key` 当前权威的版本目录及其实元数据。
/// `Ok(Absent)` = 所有盘上都没有这个 key 的任何候选目录；
/// `Err(ReadQuorum)` = 有候选目录但一个都过不了 quorum。
pub(crate) async fn resolve_version(
    set: &ErasureSet,
    bucket: &str,
    key: &str,
) -> Result<Resolved, StoreError>;
```

`get_object` 的第一步就是调它：

```rust
let resolved = resolve_version(self, bucket, key).await?;
let Some((dir, meta)) = resolved.live() else {
    return Err(StoreError::NotFound);
};
```

（借用注意：`live()` 借用 `resolved`，所以必须像上面这样先绑定再 `let-else`；
写成 `resolve_version(…).await?.live()` 是在借一个临时值。）

**为什么必须带上 `dir` 而不是把「是删除标记」直接压成 `NotFound`：** 对账（4.10）
要问的不是「有没有对象」，而是「**哪个目录是权威的、绝对不能删**」。删除标记**就是**
一个权威目录——DELETE 之后那些没拿到标记的盘还留着旧数据目录，正是靠标记目录压住它们
才读不出旧版本。若 `resolve_version` 在这里返回「什么都没有」，对账就会把标记目录也当成
垃圾删掉，旧版本随即在那些盘上复活，而 `read_quorum` 恰好够——「对账不得改变可观测结果」
这条不变量会以「数据复活」的形式被破掉。

「能不能读」这件事仍然**只有一处答案**（`live()`），Task 4.8 的 GC 与 4.10 的对账都走它；
变的只是「哪个目录是权威的」也跟着一起被返回出来了。

原计划写的 `set.get_object("b", "k", None).await.unwrap().read_to_end().await.unwrap()`
暗示了一个流式 reader 类型——**MVP 不做流式**：分片本来就整份读进内存再解码，
再包一层 `AsyncRead` 只是给同一块内存加一层接口，还连带要定义「读到一半发现校验失败」
该怎么办（HTTP 已经 200 发出去了，没法再改状态码）。返回 `Vec<u8>`，
流式留到 M5 之后按需再做。相应地，原计划那条 `get_inlines_short_circuit_disk_reads`
的空壳测试也一起重写（见下）。

#### 怎么找到元数据

PUT 把对象放在 `<bucket>/<key>/<data_dir>/meta.xl`，而 **GET 事先不知道 `data_dir`**——
原计划第 2 步「并行读各盘 meta.xl」没说这个路径从哪来。规则是：

1. 并行对每块可用盘 `list_dir("{bucket}/{key}")`，取**所有盘目录名的并集**作为候选版本目录。
   一块盘返回 `NotFound`（它压根没这个对象）按「空列表」处理，不是失败。
   **跳过所有以 `.staging-` 开头的条目**——那些是写到一半、还没提交的目录。
   不跳的话，一个「6 块盘都写完了暂存 meta、没来得及提交」的现场会在 6 块盘上各得一票、
   直接越过 `read_quorum`，于是 GET 会把半成品当成正式版本读出来。推导见 Task 4.10。
2. 候选为空 → `Ok(Resolved::Absent)`。**不是 `NotFound`**——「有没有权威目录」与
   「这个目录算不算有对象」是两个正交的问题，后者在 `get_object` 里由 `live()` 回答。
3. 对**每个候选目录** `c`：收集 `Vec<Option<ObjectMeta>>`（盘 `d` 上存在 `c/meta.xl` 且能解码
   → `Some`，否则 `None`），跑 `resolve_metadata(&metas, parity)`。能过 quorum 的候选才算「成立」。
4. 成立的候选可能有多个（覆盖写之后旧目录还没被 GC 掉，或 GC 中途崩溃）。取
   **最新版本 `header.mod_time` 最大**的那个；`mod_time` 相同（理论上不会）时取 `version_id` 大的。
   一个都不成立 → `ReadQuorum`。胜出的目录名与元数据一起包进 `Resolved::Version`。

  > 这条正是「崩溃后至少还能读到一个版本」的实现：旧版本只在少数盘上（多数盘已被覆盖），
  > 票数过不了 quorum，自然被淘汰；新版本在多数盘上，胜出。
  > 但也正因为如此，**PUT 必须给 header 写上 `mod_time`**（`SystemTime::now()` 的纳秒数）——
  > 全是 `None` 的话第 4 步就没有比较依据了。

5. 胜出元数据的最新版本 `ty == VersionType::DeleteMarker` → **仍然返回
   `Resolved::Version`**（目录就是那个删除标记目录）。「删除标记不算对象」这件事由
   `live()` 统一判，`resolve_version` 不替调用方下这个结论——见上文「为什么必须带上 `dir`」。

#### 读数据

- **内联分支**：最新版本的 `flags` 含 `INLINE_DATA`（或 body 的 `meta_sys` 有
  `keys::INLINE_DATA`）→ 直接从 `meta.inline.get(<version key>)` 取数据返回，
  **一次都不碰 `part.*`**。版本键：无版本化桶是 `"null"`。

  > 内联对象的 `etag` 只能**现算**：`put.rs` 的内联分支没把 etag 落进 meta
  > （那条路径的 `parts` 是空 `Vec`，`etag_of` 只在返回 `PutOut` 时用过一次）。
  > `etag_of(&data)` 与 PUT 当时算出的值逐字节相同，对客户端不可见；
  > 代价只是每次读内联对象多一遍 MD5。MVP 接受这个代价——
  > 要消掉它得让 4.5 把 etag 写进 body，属于另一处的改动。
  > 分片对象不受影响，直接取 `body.parts[0].etag`。
- **分片分支**：对每块盘读 `<winner_dir>/part.1`，得到该盘的整份分片明文
  （长度应等于 `expected_shard_len(size, data)`）；按 `block_size = shard_step(size, data)`
  切成 `n` 段，第 `k` 段是 `[k*step, min((k+1)*step, shard_len))`。
- 每块盘在块 `k` 上的那一段，是**分片号 `j` 满足 `dist[j] == d + 1`** 的那一份。
- **`part.1` 读取失败（含 `Corrupt(BitrotMismatch)`）不中止整次读取**，只是把该盘的槽位置成
  `None`：
  - 一块坏盘对应一个缺失槽位，`decode` 用校验分片补回来——这正是纠删码存在的意义；
  - 若坏盘多到可用槽位 `< data`，那就是 `ReadQuorum`。
  - 反过来，**绝不要**把「有一块盘报错」直接上抛成整个 GET 失败：那等于一有 bitrot 就丢可用性。
- 可用槽位数（成功读回且过了 bitrot 校验的盘数）`< read_quorum`（= `data`）→ `ReadQuorum`。
  注意 `read_quorum == data == decode` 的最小需求，两者天然一致。
- `codec = codec_cache.get(data, parity, shard_size_k as usize)`，
  `slots` 长度必须是 `N`；`codec.decode(&slots)` 得到 `data` 个数据分片 →
  顺序相接 → **截断到 `L_k`**（补齐的零在最后一个分片尾部）→ 追加到输出。
- 全部块拼完后，输出长度必须等于 `size`——不等就是 `Internal`（这是本函数自己的
  不变量，读到了不一致的长度却照常返回，就是在静默丢数据）。

#### Range 的处理与一个明确的限制

MVP 里 `read_all` 是读取器唯一的入口（4.2 特意不暴露 `read_block`，见那节的说明），
所以 **Range 请求仍然会把整份分片读进来、把所有块解码出来，最后才切出 `[start, end]`**。
也就是说 Range 省的是网络与 S3 层的内存，没省磁盘 IO。

原计划写的「Range 请求：只读取覆盖请求范围的 block」需要给读取器加一个带明确错误归类的
区段接口，**这不是 MVP 必需的**——它只是性能优化。这里明确记为已知限制，
等有真实的读放大数据再说，别为一个没有测量支撑的优化先拆掉 4.2 刚冻结的读取器接口。

`range` 的边界检查：`start > end` 或 `end >= size` → `StoreError::Internal`。
M5 的 S3 层必须先裁剪（`bytes=0-99999` 打在 1000 字节的对象上要返回 206 + `Content-Range: bytes 0-999/1000`，
而不是报错），所以走到这里还越界就是 M5 的 bug，不该被 store 悄悄吸收。

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use rstore_disk::faulty::Fault;

    use super::*;
    use crate::put::PutArgs;
    use crate::testutil::set_with_disks;

    fn put_args(bucket: &str, key: &str, data: Vec<u8>) -> PutArgs {
        PutArgs { bucket: bucket.into(), key: key.into(), data }
    }

    #[tokio::test]
    async fn get_returns_what_was_put() {
        let set = set_with_disks(6, 2).await;
        // 跨 3 个 block，且不是 251 的整数倍 → 末块会被补齐，覆盖补零-截断那段几何。
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        set.put_object(put_args("b", "k", data.clone())).await.unwrap();

        let got = set.get_object("b", "k", None).await.unwrap();
        assert_eq!(got.size, 3_000_000);
        assert_eq!(got.data, data);
    }

    /// 小对象（1000 字节）远低于内联阈值，PUT 时数据进了 meta.xl，盘上**没有** part.1。
    /// 这里在数据目录里塞一个内容完全错误的 `part.1` 诱饵：如果 GET 走了分片路径，
    /// 它要么报错、要么返回垃圾；只要它返回正确的内联数据，就证明它确实没碰 part。
    #[tokio::test]
    async fn get_inlines_short_circuit_disk_reads() {
        let set = set_with_disks(6, 2).await;
        let data = vec![0x5Au8; 1000];
        let out = set.put_object(put_args("b", "small", data.clone())).await.unwrap();

        for i in 0..2 {
            let d = set.disks()[i].as_ref().unwrap();
            d.write_all(&format!("b/small/{}/part.1", out.data_dir), &[0xFFu8; 4096])
                .await
                .unwrap();
        }

        let got = set.get_object("b", "small", None).await.unwrap();
        assert_eq!(got.data, data);
    }

    /// 内联对象也要支持 Range（小对象照样能被 `bytes=` 打）。
    #[tokio::test]
    async fn get_inline_object_with_range() {
        let set = set_with_disks(6, 2).await;
        let data: Vec<u8> = (0..1000u32).map(|i| (i % 7) as u8).collect();
        set.put_object(put_args("b", "small", data.clone())).await.unwrap();

        let got = set
            .get_object("b", "small", Some(ByteRange { start: 100, end: 199 }))
            .await
            .unwrap();
        assert_eq!(got.size, 1000);
        assert_eq!(got.data, data[100..200]);
    }

    #[tokio::test]
    async fn get_with_range_returns_the_right_slice() {
        let set = set_with_disks(6, 2).await;
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        set.put_object(put_args("b", "k", data.clone())).await.unwrap();

        let got = set
            .get_object("b", "k", Some(ByteRange { start: 1000, end: 1999 }))
            .await
            .unwrap();
        assert_eq!(got.size, 3_000_000);
        assert_eq!(got.data, data[1000..2000]);
    }

    #[tokio::test]
    async fn get_survives_two_disk_losses() {
        let set = set_with_disks(6, 2).await;
        set.put_object(put_args("b", "k", vec![3u8; 2_000_000])).await.unwrap();
        set.inject_fault_on(0, Fault::Offline);
        set.inject_fault_on(1, Fault::Offline);

        let got = set.get_object("b", "k", None).await.unwrap();
        assert_eq!(got.data, vec![3u8; 2_000_000]);
    }

    #[tokio::test]
    async fn get_fails_closed_below_read_quorum() {
        let set = set_with_disks(6, 2).await;
        set.put_object(put_args("b", "k", vec![3u8; 2_000_000])).await.unwrap();
        for i in 0..3 {
            set.inject_fault_on(i, Fault::Offline);
        }
        let r = set.get_object("b", "k", None).await;
        // struct 变体必须带 `{ .. }`。
        assert!(
            matches!(r, Err(StoreError::ReadQuorum { .. })),
            "低于 read_quorum 时绝不能返回部分数据，got {r:?}"
        );
    }

    /// 一块盘的 bitrot 不该打垮读——那是纠删码的用武之地。
    /// 这一条与 `get_fails_closed_below_read_quorum` 一起，把「可用性」和
    /// 「绝不给错数据」两侧都钉住。
    #[tokio::test]
    async fn get_reconstructs_around_one_corrupt_shard() {
        let set = set_with_disks(6, 2).await;
        let data: Vec<u8> = (0..1_000_000u32).map(|i| (i % 13) as u8).collect();
        let out = set.put_object(put_args("b", "k", data.clone())).await.unwrap();

        // 直接把 0 号盘上的分片文件内容改坏（这是真·静默损坏：写入者没参与，
        // 大小都没变，只有 bitrot 校验能发现）。
        let d = set.disks()[0].as_ref().unwrap();
        let rel = format!("b/k/{}/part.1", out.data_dir);
        let len = d.stat(&rel).await.unwrap().unwrap().size as usize;
        let mut bytes = d.read_exact_at(&rel, 0, len).await.unwrap();
        bytes[rstore_checksum::HASH_LEN] ^= 0xFF;
        d.write_all(&rel, &bytes).await.unwrap();

        let got = set.get_object("b", "k", None).await.unwrap();
        assert_eq!(got.data, data, "一块盘损坏时必须靠校验分片重建，且结果必须正确");
    }

    #[tokio::test]
    async fn get_missing_object_is_not_found() {
        let set = set_with_disks(6, 2).await;
        let r = set.get_object("b", "nope", None).await;
        assert!(matches!(r, Err(StoreError::NotFound)), "got {r:?}");
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store get`
Expected: 编译失败

- [ ] **Step 3: 实现**

按上面「怎么找到元数据」与「读数据」两节逐步实现。两个容易写错的地方：

- `list_dir` 对**不存在的目录**返回 `Err(NotFound)`，对**空目录**返回 `Ok(vec![])`。
  发现逻辑里这两者都按「这块盘上没有版本目录」处理，别让 `Err(NotFound)` 冒泡成整次 GET 的
  `NotFound`——只有当**所有**盘都找不到候选目录时才是对象不存在。
- 解码时的 `slots` 长度必须是 `N`（`Codec::decode` 会校验），槽位下标是**分片号**，
  不是盘号。把盘号当分片号写进去，正常路径下会以 `UnequalShardLength` 或
  错误的解码结果收场——而后者如果恰好长度对得上，就会安静地返回错数据。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store get`
Expected: PASS

```bash
git add crates/store/
git commit -m "feat(store): GET path with version discovery, inline fast path, fail-closed quorum

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

### Task 4.8: DELETE 与覆盖写

**Files:**
- Create: `crates/store/src/delete.rs`
- Modify: `crates/store/src/put.rs`（覆盖写提交成功后调用本模块的 GC）
- Modify: `crates/store/src/lib.rs`（加 `pub mod delete;`）
- Test: 同文件 `#[cfg(test)]`

#### 两个机制，一句话各说清

**覆盖写** 走的就是 `put_object` 的完整路径——新的 `data_dir`、新的暂存目录、新的提交。
**没有「就地改写」这条路**：那样「元数据可见」与「分片可见」就不再是同一个原子事件，
崩溃后会出现「目录名没变、内容半新半旧」的版本，而那种状态没有任何字段能识别出来。
提交成功之后，才轮到 GC 去收拾旧目录。

**DELETE** 写的是一个**删除标记版本**（`VersionType::DeleteMarker`，`size = 0`，
不带 `USES_DATA_DIR`），走同样的暂存目录 + `commit`，quorum 用 `delete_quorum = N/2 + 1`。
它不是「把文件删掉」，而是「写一个新的、更新的版本，而那个版本表示『没有对象』」。
这么做的直接好处：`get_object` 的 `resolved.live()`（4.7）发现最新版本是删除标记就返回 `None`，
于是**并发读**不会出现「一半盘上新数据已提交、一半盘上旧数据刚被删」这种谁都读不出来的窗口。

#### GC 规则（一条，写死，别再加特例）

> **只在一块盘同时持有「胜出目录」时才删它上面别的目录。**

逐盘执行：列出 `{bucket}/{key}` 下的所有目录；如果其中**包含胜出目录**，就把其余目录
`remove_dir_all`；否则**一个都不动**。

- 为什么要有「同时持有胜出目录」这个前提：覆盖写提交时可能有盘 rename 失败（落后盘）。
  落后盘上只有旧目录，此时删掉旧目录会让这块盘变成**彻底没有这个对象**——
  而它本来还能为读提供一份有效分片。留着的代价只是磁盘占用，删掉的代价是丢失一份冗余。
- GC 是 **best-effort**：删除失败只记日志，不上抛。残留由对账（4.10）兜底。
- GC **幂等**：`remove_dir_all` 对不存在的目录返回 `Ok`（`fsx::remove_dir_all` 已经这么实现了）。
- 顺序不可颠倒：**先让 `commit` 成功，再 GC**。反过来就是在删还没提交的数据。
- GC 删的是胜出目录之外的**所有**条目，含 `.staging-*`。所以它**不是**一个可以随便
  并发调用的清理器：理论上另一个针对同一个 key 的在写 PUT 的暂存目录也会被删掉，
  那个 PUT 随后的 `rename` 会因为源目录不存在而失败（**失败得干净**，不会静默写坏）。
  MVP 不做同 key 并发写的协调；真正安全地清理崩溃残留是 Task 4.10 的
  `reclaim_orphans`（它明确声明不与写入并发运行）。

```rust
impl ErasureSet {
    /// `delete_quorum = N/2 + 1`。
    /// 对不存在的 key 也照样写删除标记并返回 `Ok`——这是 S3 的语义
    /// （DELETE 是幂等的，重复删同一 key 都是 204）。
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StoreError>;
}
```

#### 测试辅助

GC 之后谁该在、谁不该在，要能一眼看出来。加一个测试专用的列举函数（放在 `delete.rs`
的 `#[cfg(test)]` 里，4.10 用不到就不必提升到 `testutil`）：

```rust
/// 某块盘上 `bucket/key` 下还剩下哪些版本目录（已排序）。
async fn dirs_on(set: &TestSet, disk_idx: usize, key_rel: &str) -> Vec<String> {
    let d = set.disks()[disk_idx].as_ref().expect("该盘应当在线");
    match d.list_dir(key_rel).await {
        Ok(v) => v,
        Err(DiskError::NotFound) => Vec::new(),
        Err(e) => panic!("disk {disk_idx} list_dir 失败: {e:?}"),
    }
}
```

- [ ] **Step 1: 写失败测试**

原计划这四条测试**全部只有一个 `/* … */` 注释，没有一行代码**——那不是测试，
是待办事项列表。下面把它们写成真正的断言。

```rust
#[cfg(test)]
mod tests {
    use rstore_common::error::DiskError;
    use rstore_disk::faulty::Fault;

    use super::*;
    use crate::put::PutArgs;
    use crate::testutil::{set_with_disks, TestSet};

    fn put_args(bucket: &str, key: &str, data: Vec<u8>) -> PutArgs {
        PutArgs { bucket: bucket.into(), key: key.into(), data }
    }

    async fn dirs_on(set: &TestSet, disk_idx: usize, key_rel: &str) -> Vec<String> { … }

    #[tokio::test]
    async fn overwrite_replaces_latest() {
        let set = set_with_disks(6, 2).await;
        let a = vec![1u8; 1_500_000];
        let b = vec![2u8; 1_500_000];
        set.put_object(put_args("b", "k", a)).await.unwrap();
        let out_b = set.put_object(put_args("b", "k", b.clone())).await.unwrap();

        assert_eq!(set.get_object("b", "k", None).await.unwrap().data, b);

        // 覆盖写之后，每块盘上**只剩**胜出的那个目录。这条同时钉住 GC 真的跑了，
        // 以及它没把胜出目录自己也一起删掉。
        for i in 0..6 {
            assert_eq!(
                dirs_on(&set, i, "b/k").await,
                vec![out_b.data_dir.to_string()],
                "disk {i}"
            );
        }
    }

    /// GC 的安全边界：**只在一块盘也持有胜出目录时才删它的旧目录**。
    /// 2 块盘在第二次 PUT 时掉线，它们只留下旧目录——那两份旧分片是有效冗余，
    /// 删掉就等于把「6 副本 4+2」降级成「4 副本」。
    #[tokio::test]
    async fn gc_keeps_old_dir_where_the_new_meta_never_landed() {
        let set = set_with_disks(6, 2).await;
        set.put_object(put_args("b", "k", vec![1u8; 2_000_000])).await.unwrap();

        for i in 0..2 {
            set.inject_fault_on(i, Fault::Offline);
        }
        let out_b = set.put_object(put_args("b", "k", vec![2u8; 2_000_000])).await.unwrap();
        for i in 0..2 {
            set.clear_fault_on(i);
        }
        // 覆盖写仍然成功：4 块盘 ≥ write_quorum(4)。
        assert_eq!(set.get_object("b", "k", None).await.unwrap().data, vec![2u8; 2_000_000]);

        for i in 0..2 {
            let dirs = dirs_on(&set, i, "b/k").await;
            assert_eq!(dirs.len(), 1, "disk {i} 应只留旧目录，got {dirs:?}");
            assert_ne!(dirs[0], out_b.data_dir.to_string(), "disk {i} 上不该有胜出目录");
        }
        for i in 2..6 {
            assert_eq!(
                dirs_on(&set, i, "b/k").await,
                vec![out_b.data_dir.to_string()],
                "disk {i}"
            );
        }
    }

    #[tokio::test]
    async fn delete_makes_get_return_not_found() {
        let set = set_with_disks(6, 2).await;
        set.put_object(put_args("b", "k", vec![3u8; 1_000_000])).await.unwrap();
        assert!(set.get_object("b", "k", None).await.is_ok());

        set.delete_object("b", "k").await.unwrap();
        let r = set.get_object("b", "k", None).await;
        assert!(matches!(r, Err(StoreError::NotFound)), "got {r:?}");
    }

    /// 删除标记是「先落地、后回收」：标记还没在多数盘上落地的那些盘，
    /// 它们的分片**必须还在**。这条证明 DELETE 不是一个「先删数据再写标记」的
    /// 危险实现——那样一旦标记写失败，数据就没了。
    #[tokio::test]
    async fn delete_marks_before_gc() {
        let set = set_with_disks(6, 2).await;
        let out = set.put_object(put_args("b", "k", vec![4u8; 1_000_000])).await.unwrap();

        // 让 2 块盘写不了：删除标记只能在 4 块盘上落地，恰好等于 delete_quorum(6) = 4。
        for i in 0..2 {
            set.inject_fault_on(i, Fault::Offline);
        }
        set.delete_object("b", "k").await.unwrap();
        for i in 0..2 {
            set.clear_fault_on(i);
        }

        assert!(matches!(
            set.get_object("b", "k", None).await,
            Err(StoreError::NotFound)
        ));
        // 没拿到删除标记的那 2 块盘上，原始数据目录必须原样留着（它们是有效冗余，
        // 而且此时删掉就真没东西可回收了）。有删除标记的 4 块盘上它才被回收。
        for i in 0..2 {
            let dirs = dirs_on(&set, i, "b/k").await;
            assert_eq!(dirs, vec![out.data_dir.to_string()], "disk {i} 不该回收旧数据");
        }
        for i in 2..6 {
            let dirs = dirs_on(&set, i, "b/k").await;
            assert_eq!(dirs.len(), 1, "disk {i}, got {dirs:?}");
            assert_ne!(dirs[0], out.data_dir.to_string(), "disk {i} 应只剩删除标记目录");
        }
    }

    /// DELETE 幂等：S3 语义下重复删同一个 key 都是成功，删不存在的 key 也是成功。
    #[tokio::test]
    async fn delete_is_idempotent_and_ok_on_missing_key() {
        let set = set_with_disks(6, 2).await;
        set.delete_object("b", "never-existed").await.unwrap();
        assert!(matches!(
            set.get_object("b", "never-existed", None).await,
            Err(StoreError::NotFound)
        ));

        set.put_object(put_args("b", "k", vec![5u8; 1_000_000])).await.unwrap();
        set.delete_object("b", "k").await.unwrap();
        set.delete_object("b", "k").await.unwrap();
        assert!(matches!(
            set.get_object("b", "k", None).await,
            Err(StoreError::NotFound)
        ));
    }

    /// GC 是幂等的：对一个已经被 GC 干净的 key 再跑一次 GC，既要安静地什么都不做，
    /// 也不能把**胜出目录自己**删掉。光断言 `NotFound` 是抓不到后者的
    /// （标记目录被删光以后对象照样是 `NotFound`），所以要直接看目录列表。
    #[tokio::test]
    async fn gc_is_idempotent() {
        let set = set_with_disks(6, 2).await;
        set.put_object(put_args("b", "k", vec![6u8; 1_000_000])).await.unwrap();
        set.delete_object("b", "k").await.unwrap();
        // 再删一次会写一枚新的删除标记并再跑一轮 GC，把上一枚标记目录收掉。
        set.delete_object("b", "k").await.unwrap();
        assert!(matches!(
            set.get_object("b", "k", None).await,
            Err(StoreError::NotFound)
        ));

        // 此时每块盘上只剩那一个删除标记目录：空跑一次 GC 必须原地不动。
        for i in 0..6 {
            let dirs = dirs_on(&set, i, "b/k").await;
            assert_eq!(dirs.len(), 1, "disk {i}, got {dirs:?}");
            gc_superseded(&set, "b", "k", &dirs[0]).await;
            assert_eq!(
                dirs_on(&set, i, "b/k").await,
                dirs,
                "disk {i}: 空跑 GC 改动了目录"
            );
        }
        assert!(matches!(
            set.get_object("b", "k", None).await,
            Err(StoreError::NotFound)
        ));
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store delete`
Expected: 编译失败

- [ ] **Step 3: 实现**

`delete.rs` 里：

```rust
/// GC 掉 `bucket/key` 下除 `winner_dir` 之外的版本目录。
/// **只在一块盘同时持有 `winner_dir` 时才动手**——见上文规则。
pub(crate) async fn gc_superseded(
    set: &ErasureSet,
    bucket: &str,
    key: &str,
    winner_dir: &str,
);
```

#### 先解决 `put.rs` 里三个跨模块拿不到的东西

`delete.rs` 与 `put.rs` 是**兄弟模块**，Rust 的私有项只对「本模块及其后代」可见，
所以下面三个都得放宽到 `pub(crate)`（`put.rs` 本来就在本任务的 Files 清单里）：

| 现有项 | 现状 | 改成 |
| --- | --- | --- |
| `fn now_nanos() -> u64`（`put.rs:86`） | 模块私有 | `pub(crate) fn now_nanos() -> u64` |
| `impl ErasureSet { async fn write_meta_all(&self, staging, bytes) }`（`put.rs:228`） | 私有方法 | `pub(crate) async fn write_meta_all(...)` |
| 删除标记的元数据构造 | **不存在** | 新增 `pub(crate) fn build_delete_meta(total: u8, version_id: Uuid) -> Result<ObjectMeta, StoreError>` |

**不要**想着直接复用 `put.rs:95` 的 `build_meta`：

- 它把 `ty` 硬编码成 `VersionType::Object`、把 `data_dir` 塞成 `Some(data_dir)`，改不动；
- 它的参数已经是 7 个，再加一个 `ty` 就是 8 个，会撞上 `clippy::too_many_arguments`
  （阈值 7，属于 `clippy::all`，而门禁是 `-D warnings`）。为它单开一个删除标记构造器
  比把它拆成参数结构体风险小得多。

`build_delete_meta` 就放在 `build_meta` 旁边，把「`mod_time` 必须写、不然 GET 的版本仲裁
没有比较依据」这条不变量留在同一个文件里，别让它在 `delete.rs` 里重新推一遍：

```rust
/// 删除标记的元数据：**一个版本**，`ty = DeleteMarker`，`size = 0`，
/// `data_dir = None`，`flags` 为空（不置 `USES_DATA_DIR`，它没有数据目录）。
pub(crate) fn build_delete_meta(
    total: u8,
    version_id: Uuid,
) -> Result<ObjectMeta, StoreError> {
    let header = FileVersionHeader {
        version_id: Some(version_id),
        ty: VersionType::DeleteMarker,
        size: 0,
        // 同样必须写：`resolve_version` 靠它把这枚标记判成「最新」。
        mod_time: Some(now_nanos()),
        // 删除标记没有分片。这两个字段不参与任何判断——`Resolved::live()`
        // 在解码 body **之前**就返回 `None` 了，分片分支根本走不到。
        ec_m: 0,
        ec_n: 0,
        flags: Flags::empty(),
        data_dir: None,
    };
    Ok(ObjectMeta {
        versions: vec![ShallowVersion {
            header,
            body: encode_body(&ObjectBody {
                id: None,
                parts: Vec::new(),
                ec_dist: Vec::new(),
                checksum_algo: ChecksumAlgo::Crc32c,
                storage_class: StorageClass::Standard,
                meta_user: BTreeMap::new(),
                meta_sys: BTreeMap::new(),
            })?,
        }],
        inline: InlineData::new(),
        meta_ver: 1,
    })
}
```

（参数里那个 `total` 在实现里暂时用不上——如果你确实一个字段都不用它，就把参数删掉，
别留一个 `_total` 糊过去；关键是别把 `ec_m/ec_n` 填成 `data/total`，
那会让一个没有分片的版本看起来像 4+2。）

`delete_object` 流程：

1. `txid = Uuid::new_v4()`、`marker_dir = Uuid::new_v4()`；
   `staging = format!("{bucket}/{key}/.staging-{txid}")`、
   `final_rel = format!("{bucket}/{key}/{marker_dir}")`。
   （`.staging-` 前缀见 Task 4.5 的布局说明：没有它，未提交的半成品目录会在仲裁里胜出。）
2. `let bytes = encode(&build_delete_meta(total, marker_version_id)?)?;`
   ——构造细节见上文「先解决 `put.rs` 里三个跨模块拿不到的东西」。
   `marker_version_id` 与 `marker_dir` **是两个各自独立的 `Uuid::new_v4()`**，
   不要共用同一个。
3. `self.write_meta_all(&staging, &bytes).await;`（复用 PUT 那条，
   逐盘失败只忽略——最终 quorum 由 `commit` 的 rename 判定）。
4. `commit(set, &staging, &final_rel, delete_quorum(total))`。
5. 失败 → `WriteQuorum { achieved, required: delete_quorum }`（复用变体即可，
   `required` 字段本身就把数字说清楚了；为它单开一个变体只会让 M5 的错误映射多一个分支）。
6. 成功 → `gc_superseded(set, bucket, key, &marker_dir.to_string()).await`。

`put_object`（覆盖写）在 `commit` 成功之后加同一句
`gc_superseded(set, bucket, key, &data_dir.to_string()).await`。

> **`final_rel` 必须是本次写入独有的路径**（4.4 已经强调过）：因为「删除标记」也是一次
> 正常的提交，它同样要 rename 到一个**新**目录。若两次删除共用同一个目录名，
> 第二次的 rename 会因目标已存在而失败（`std::fs::rename` 对非空目录会报错）。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store delete`
Expected: PASS

```bash
git add crates/store/
git commit -m "feat(store): overwrite and delete via tombstone commit with outvote-based GC

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

### Task 4.9: Quorum 边界测试套件

**Files:**
- Create: `crates/store/src/quorum_boundaries.rs`（模块顶部 `#![cfg(test)]`）
- Modify: `crates/store/src/lib.rs`（加 `#[cfg(test)] mod quorum_boundaries;`）

> **改位置：不再放 `crates/store/tests/`。** 集成测试是**独立编译的 crate**，
> 它看不到 store 内部的 `#[cfg(test)] mod testutil`——而本任务要用
> `set_with_disks` / `TestSet::inject_fault_on`。原计划把文件放在 `tests/` 下，
> 那里的代码一行都编译不过。
>
> 想让它留在 `tests/` 只有两条路：把 `testutil` 变成永远公开的发布代码
> （把测试夹具塞进对外 API），或者用 feature 门控（那样 `cargo test --workspace`
> 不带 feature 时整个文件变成空的，测试**静默不跑**——比编译失败更糟）。
> 所以跟 `testutil` 一样放 `src/` 里、用 `#![cfg(test)]` 门控。

> **`[dev-dependencies]` 不用再动**：`rstore-disk = { workspace = true, features = ["fault-injection"] }`
> 在 Task 4.3 已经加过了。原计划这里又列了一遍「Modify Cargo.toml」，
> 照着做会出现第二处同名依赖声明。
>
> **注意**：`cargo test --workspace`**不带** `--features fault-injection` 时，
> `rstore-disk` 的 `faulty` 模块对 store 可见吗？——可见。4.3 把它声明在 store 的
> dev-dependencies 上并开了 feature，而 `cargo test --workspace` 会为 store 的测试构建
> 启用该 feature（dev-dependencies 的 feature 在测试构建里生效）。这也是为什么必须在
> dev-dependencies 里开 feature，而不是靠 `--features` 命令行。

- [ ] **Step 1: 写测试（这是 DESIGN §19.2 的落地）**

```rust
//! DESIGN §19.2 的边界矩阵。与其余测试的分工：这里**只回答**「掉几块盘、
//! 哪种操作、该成功还是该失败」，具体数据是否正确由 4.7/4.8 各自的用例负责。
#![cfg(test)]

use rstore_disk::faulty::Fault;

use crate::error::StoreError;
use crate::put::PutArgs;
use crate::testutil::{set_with_disks, TestSet};
// 本文件不需要 `ByteRange`：矩阵只跑整对象读。别顺手 import 它——
// `cargo clippy --all-targets -- -D warnings` 会把未使用的导入判成失败。

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Read,
    Write,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    Ok,
    ReadQuorum,
    WriteQuorum,
}

/// 对象大小固定 2 MiB：跨 2 个 block，既走真编码路径又不至于让 7 个用例跑太久。
const BODY: usize = 2 * 1024 * 1024;

fn body(seed: u8) -> Vec<u8> {
    let mut v = vec![0u8; BODY];
    for (i, b) in v.iter_mut().enumerate() {
        *b = (i as u8) ^ seed;
    }
    v
}

/// 建 set → 先健康地 PUT 一份 → 注入 `offline` 块掉线 → 执行 `op` → 断言结果类别。
///
/// **Read / Delete 用例必须先有一份成功写入的对象**：否则「读不存在的对象」
/// 会以 `NotFound` 收场，而 `Expect::Ok` 的断言根本到不了——那是一种
/// 「看起来测了，其实测的是别的东西」的假绿。PUT 阶段掉线数必须是 0。
async fn run_case(offline: usize, op: Op, expect: Expect) {
    let set = set_with_disks(6, 2).await;
    let data = body(7);

    if matches!(op, Op::Read | Op::Delete) {
        set.put_object(PutArgs { bucket: "b".into(), key: "k".into(), data: data.clone() })
            .await
            .expect("健康状态下 PUT 必须成功");
    }

    for i in 0..offline {
        set.inject_fault_on(i, Fault::Offline);
    }

    let r = match op {
        Op::Read => set.get_object("b", "k", None).await.map(|out| {
            // **`Ok` 必须是「内容正确」的 `Ok`。** 只断言 `is_ok()` 的话，
            // 一个返回全零缓冲区的实现也能过。
            assert_eq!(out.data, data, "offline={offline} 读回的内容不对");
        }),
        Op::Write => set
            .put_object(PutArgs { bucket: "b".into(), key: "w".into(), data: body(9) })
            .await
            .map(|_| ()),
        Op::Delete => set.delete_object("b", "k").await,
    };

    match (expect, r) {
        (Expect::Ok, Ok(())) => {}
        (Expect::Ok, Err(e)) => panic!("offline={offline} {op:?}: 期望成功，got {e:?}"),
        // 这里**不能**再补一个 `(Expect::Ok, _) => unreachable!()`：
        // `Result` 只有 `Ok`/`Err` 两个变体，上面两条已经覆盖了 `Expect::Ok` 的全部，
        // 那条通配臂会被 `unreachable_patterns` 判成警告，而门禁是 `-D warnings`。
        (Expect::ReadQuorum, Err(StoreError::ReadQuorum { .. })) => {}
        (Expect::WriteQuorum, Err(StoreError::WriteQuorum { .. })) => {}
        (expect, Err(e)) => panic!("offline={offline} {op:?}: 期望 {expect:?}，got {e:?}"),
        (expect, Ok(())) => panic!("offline={offline} {op:?}: 期望 {expect:?}，却成功了"),
    }
}

/// 4+2 配置下的完整边界矩阵。每行是一个独立用例。
#[tokio::test]
async fn matrix_4_plus_2() {
    let cases = [
        // (掉线盘数, 操作, 期望)
        (0, Op::Read, Expect::Ok),
        (2, Op::Read, Expect::Ok),          // read_quorum = 6 - 2 = 4，刚好还能读
        (3, Op::Read, Expect::ReadQuorum),  // 低于 read_quorum
        (0, Op::Write, Expect::Ok),
        (2, Op::Write, Expect::Ok),         // write_quorum = data = 4
        (3, Op::Write, Expect::WriteQuorum),
        (1, Op::Delete, Expect::Ok),          // delete_quorum = 6/2 + 1 = 4，5 可用 ≥ 4
        (3, Op::Delete, Expect::WriteQuorum), // 3 可用 < 4
        (4, Op::Delete, Expect::WriteQuorum), // 2 可用 < 4
    ];
    for (offline, op, expect) in cases {
        run_case(offline, op, expect).await;
    }
}
```

> **原计划这张表里 `Delete` 那三行是错的**（`(1, Ok)`、注释写「delete_quorum = 3」、
> `(4, WriteQuorum)`），上面已经按**定义**重算：`delete_quorum(6) = 6/2 + 1 = 4`。
> 把这个缺陷记在这里不是留痕，是因为它属于一类反复出现的错：
> **照抄手写注释里的常量**，而不是从 `consts` 里的定义重新算。
> 以后要加行（比如 `(0, Op::Delete, …)`）同样按定义算，别信任何注释里的数字。

```rust
/// 少数盘静默损坏：读必须成功，而且结果必须正确。
/// 「少数」的界是 `parity`——损坏盘数 ≤ parity 时纠删码能把数据重建出来。
#[tokio::test]
async fn bitrot_on_minority_still_reads_correctly() {
    let set = set_with_disks(6, 2).await;
    let data = body(3);
    let out = set
        .put_object(PutArgs { bucket: "b".into(), key: "k".into(), data: data.clone() })
        .await
        .unwrap();

    for i in 0..2 {
        corrupt_shard(&set, i, &out.data_dir).await;
    }

    let got = set.get_object("b", "k", None).await.unwrap();
    assert_eq!(got.data, data, "损坏盘数 == parity 时必须靠校验分片重建");
}

/// **DESIGN §2 的 P1**：宁可报错，绝不返回错数据。
/// 损坏盘数 > parity 时，能用于解码的份数已经不够，此时任何「尽力而为」的重建
/// 都会产生一段**看起来正常但没有校验能发现**的字节。
#[tokio::test]
async fn bitrot_on_majority_exposes_corruption_not_wrong_data() {
    let set = set_with_disks(6, 2).await;
    let data = body(5);
    let out = set
        .put_object(PutArgs { bucket: "b".into(), key: "k".into(), data: data.clone() })
        .await
        .unwrap();

    for i in 0..3 {
        corrupt_shard(&set, i, &out.data_dir).await;
    }

    match set.get_object("b", "k", None).await {
        Err(StoreError::ReadQuorum { .. }) => {}
        // 宁可在这里因为「实现了某种超出 MVP 范围的恢复」而红，也不要放过
        // 一个返回了错误字节却报成功的实现。
        Ok(out) => panic!("损坏超过 parity 时返回了 {} 字节数据，未报错", out.data.len()),
        Err(e) => panic!("期望 ReadQuorum，got {e:?}"),
    }
}

/// 直接对盘上的分片文件做读-改-写，制造**真·静默损坏**：文件长度不变、
/// 没有任何 API 报错，只有 bitrot 摘要能发现它。
///
/// 不用 `Fault::CorruptBytes` 的原因：那是在**写入时**变换 payload，
/// 而这里要损坏的是**已经提交在地上**的数据——两者是完全不同的故障场景
/// （前者模拟坏盘，后者模拟 bit rot / 静默错写），后者才是 heal 的触发条件。
async fn corrupt_shard(set: &TestSet, disk_idx: usize, data_dir: &uuid::Uuid) {
    use rstore_checksum::HASH_LEN;

    let d = set.disks()[disk_idx].as_ref().expect("该盘应当在线");
    let rel = format!("b/k/{data_dir}/part.1");
    let len = d.stat(&rel).await.unwrap().expect("分片必须存在").size as usize;
    let mut bytes = d.read_exact_at(&rel, 0, len).await.unwrap();
    bytes[HASH_LEN] ^= 0xFF;
    d.write_all(&rel, &bytes).await.unwrap();
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store quorum_boundaries`
Expected: 编译失败

- [ ] **Step 3: 实现**

本任务**没有产品代码要写**——它只写测试。唯一要动的是 `lib.rs` 里的模块声明。
若某条用例红了，先按下面的顺序怀疑：

1. 是 `run_case` 的用例表算错了（重新按 `read_quorum` / `write_quorum` / `delete_quorum`
   的**定义**手算一遍，不要信注释）；
2. 才是被测代码有 bug。

- [ ] **Step 4: 提交**

Run: `cargo test -p rstore-store quorum_boundaries`
Expected: 全部 PASS

```bash
git add crates/store/
git commit -m "test(store): quorum boundary matrix across failure modes

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

### Task 4.10: 崩溃点状态机测试

**Files:**
- Create: `crates/store/src/reconcile.rs`（产品代码：`scan_orphans` / `reclaim_orphans`；
  测试是文件内的 `#[cfg(test)] mod tests`。**注意模块本身不是 `#![cfg(test)]`**——
  与 4.9 不同，这两个函数是产品代码）
- Modify: `crates/store/src/lib.rs`（加 `pub mod reconcile;`）
- **不需要**改 `put.rs` / `get.rs` / `delete.rs`：本任务要用的两样东西
  （`get::resolve_version`、`delete::gc_superseded`）在 4.7/4.8 就已经是 `pub(crate)`，
  而原计划列出的暂存目录前缀改动也已经在 4.5/4.7 落地（见下文）。

> **不再放 `crates/store/tests/`**，理由同 Task 4.9：集成测试看不到 crate 内的
> `testutil`。用 `#![cfg(test)]` 模块放在 `src/`。
>
> 原计划第 4 步的 `git add crates/store/src/pool.rs` 也是错的——`pool.rs` 在 Task 4.3
> 就建好了，本任务不动它。

#### 先补一个洞：未提交的暂存目录能在仲裁里胜出

原来的布局把暂存目录写成 `<bucket>/<key>/<txid>`，与最终目录 `<bucket>/<key>/<data_dir>`
**只在名字上不同，形状完全一样**。于是这条路径是通的：

> PUT 把 `meta.xl` 写进了 6 块盘的暂存目录，**还没提交**就崩了。
> 重启后 `resolve_version`（4.7）列出 `bucket/key` 下的目录，看到这个 txid 目录在
> **6 块盘**上都存在、内容还一模一样 → **6 票**，轻松越过 `read_quorum = 4`；
> 它的 `mod_time` 又最新 → 胜出。然后去读它的 `part.1`——那是半截的。

也就是说「可见性」不变量会直接破掉，而且破法是「读到一个半成品」而不是报错。

修法是让两者在**发现阶段就不可混淆**：

```
<bucket>/<key>/.staging-<txid>/     ← 暂存目录：以 `.staging-` 开头
<bucket>/<key>/<data_dir>/          ← 已提交的版本目录
```

- `resolve_version` 的发现阶段（4.7）**跳过所有以 `.staging-` 开头的条目**；
- `reclaim_orphans` 把 `.staging-*` 一律删除——按定义它们就是没提交成功的；
- 于是「至少有 `read_quorum` 块盘上存在**且已提交**」这个前提才成立，投票才有意义。

> **对账不得与写入并发运行。** MVP 的实现是「列出 `.staging-*` 就删」，
> 它分不清「上次崩溃留下的」和「此刻正在写的」。这条约束要写进
> `reclaim_orphans` 的文档注释：对账是离线/运维动作，不是随写随跑的 GC。
> 真要并发，得给暂存目录带上 pid/时间戳并按年龄判断——Phase 3 再说。

**这三处已经落地了，本任务不要重复改**（原计划把它们列在 Step 3 里，那是写在
4.5/4.7 之前的话）：

| 位置 | 现状 |
| --- | --- |
| `put.rs` 的 `staging` 拼法 | Task 4.5 起就是 `format!("{key_rel}/.staging-{txid}")` |
| `delete.rs` 的 `staging` 拼法 | Task 4.8 起同上 |
| `get.rs` 发现阶段的前缀过滤 | Task 4.7 起就有 |

（4.4 的 `commit` 只是把人给的路径 `rename` 过去，不关心名字，不用改；
它那几条测试里用的 `"b/o/tx1"` 是纯粹的字面路径，也不用改。）

#### 崩溃怎么模拟，以及为什么不能只用 `FailAfter`

`Fault::FailAfter` 让调用**返回错误**，而真的崩溃是**进程没了**。两者的差别很实在：
返回错误之后代码还有机会跑清理逻辑（4.4 的回滚就是），而真崩溃没有。
所以**不能**去断言「崩溃点之后必然留有残骸」——那是在断言一个实现细节。

能断言的是两条**两种情况下都必须成立**的不变量：

1. **可见性**：对象要么完全可见且内容正确，要么 `NotFound`。
   绝不出现「可读但内容不对」、「读一半报错」、「读到半成品」。
2. **对账不改观测结果**：跑一遍 `reclaim_orphans` 之后，每个对象的可读结果
   （内容或 `NotFound`）必须与跑之前**完全一致**。
   这条就是「垃圾回收不删活数据」的可执行定义——比「每个孤儿都能被回收」强得多：
   后者几乎是同义反复（孤儿按定义就是能删的），前者才真的会抓到 GC 误删。

> 因此下面用**中断调度**而不是「第一阶段/第二阶段」来编号用例：`FailAfter` 计的是
> 「任意 `DiskAPI` 方法的调用次数」（见 `rstore_disk::faulty` 的文档），
> 各盘的调用序列并不对齐，所以「第 k 次调用」落在哪个逻辑阶段本来就是不确定的。
> 与其假装它精确，不如把它当成「在一批任意位置被打断」——而不变量恰好不依赖位置。

#### 契约

```rust
impl ErasureSet {
    /// 列出 `bucket` 下**没有被任何权威元数据引用**的目录（含 `.staging-*`）。
    /// 返回的是相对 `bucket` 的路径（如 `k/.staging-3f2a…`），便于报错时直接看。
    pub async fn scan_orphans(&self, bucket: &str) -> Result<Vec<String>, StoreError>;

    /// 删除孤儿。**绝不删除活数据**：只在一块盘同时持有该 key 的权威目录时才动手
    /// （与 Task 4.8 的 GC 同一条规则，直接复用 `gc_superseded`）。
    /// 不与写入并发运行——见上文。
    pub async fn reclaim_orphans(&self, bucket: &str) -> Result<(), StoreError>;
}
```

- [ ] **Step 1: 写测试**

```rust
#[cfg(test)]
mod tests {
    use rstore_disk::faulty::{Fault, FaultKind};

    use super::*;
    use crate::error::StoreError;
    use crate::put::PutArgs;
    use crate::testutil::{set_with_disks, TestSet};

    /// 对象内容取一段与长度绑定的可辨识字节，读到半截时断言能看出来。
    fn expected() -> Vec<u8> {
        (0..2_000_000u32).map(|i| (i % 251) as u8).collect()
    }

    /// 在一批任意位置制造中断。**不声称每一个都落在某个特定阶段**——
    /// 用 `FailAfter` 模拟的是「调用序列在某处断掉」，而各盘的调用计数本来就不对齐。
    const POINTS: &[usize] = &[0, 1, 2, 3, 5, 8, 13, 21, 34, 55];

    /// 一次中断之后，世界必须满足两条不变量。
    async fn assert_invariants_hold(set: &TestSet, key: &str, tag: &str) {
        let before = set.get_object("b", key, None).await;
        match &before {
            Ok(out) => assert_eq!(out.data, expected(), "{tag}: 读到了内容但内容不对"),
            Err(StoreError::NotFound) => {}
            Err(e) => panic!("{tag}: 既不是可见也不是不存在: {e:?}"),
        }

        set.reclaim_orphans("b").await.unwrap();

        let after = set.get_object("b", key, None).await;
        match (before, after) {
            (Ok(a), Ok(b)) => assert_eq!(a.data, b.data, "{tag}: 对账改变了可读内容"),
            (Err(StoreError::NotFound), Err(StoreError::NotFound)) => {}
            (a, b) => panic!("{tag}: 对账改变了可见性: {a:?} -> {b:?}"),
        }
    }

    #[tokio::test]
    async fn interrupted_put_leaves_a_consistent_world() {
        // 守卫，跟 4.4 的 `ok_count` 是同一个用途：若没有任何一个中断点真的让
        // PUT 成功过，「可见性」的 `Ok` 分支一次都进不去，整轮测试就是空转。
        let mut ok_count = 0usize;

        for &calls in POINTS {
            let set = set_with_disks(6, 2).await;
            for i in 0..6 {
                set.inject_fault_on(
                    i,
                    Fault::FailAfter { calls, kind: FaultKind::Transient },
                );
            }
            // 结果本身不关心（可能就是失败了），关心的是失败之后世界的状态。
            let r = set
                .put_object(PutArgs {
                    bucket: "b".into(),
                    key: "k".into(),
                    data: expected(),
                })
                .await;
            for i in 0..6 {
                set.clear_fault_on(i);
            }
            if r.is_ok() {
                ok_count += 1;
            }

            assert_invariants_hold(&set, "k", &format!("中断点 calls={calls}")).await;
        }

        // `calls = 0` 时一次调用都不放行，PUT 必失败；随着 `calls` 变大总会有几次放行到底。
        // 若这里恒为 0，说明中断点设置得让整轮测试都是空转。
        assert!(ok_count > 0, "没有任何一个中断点让 PUT 成功过，这轮测试没测到东西");
    }

    /// 覆盖写两个版本，在第二次写入（含 GC）的各处中断。
    /// **至少能读到其中一个版本**——一个都不剩就是真丢数据。
    #[tokio::test]
    async fn gc_interruption_never_loses_both_versions() {
        for &calls in POINTS {
            let set = set_with_disks(6, 2).await;
            set.put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                data: vec![1u8; 2_000_000],
            })
            .await
            .unwrap();

            for i in 0..6 {
                set.inject_fault_on(
                    i,
                    Fault::FailAfter { calls, kind: FaultKind::Transient },
                );
            }
            let _ = set
                .put_object(PutArgs {
                    bucket: "b".into(),
                    key: "k".into(),
                    data: vec![2u8; 2_000_000],
                })
                .await;
            for i in 0..6 {
                set.clear_fault_on(i);
            }

            // 第一个版本是**成功提交过**的，所以「两个都读不到」是硬失败。
            match set.get_object("b", "k", None).await {
                Ok(out) => assert!(
                    out.data == vec![1u8; 2_000_000] || out.data == vec![2u8; 2_000_000],
                    "calls={calls}: 读到的内容两个版本都不是"
                ),
                Err(StoreError::NotFound) => {
                    panic!("calls={calls}: 两个版本都丢了")
                }
                Err(e) => panic!("calls={calls}: {e:?}"),
            }

            set.reclaim_orphans("b").await.unwrap();
            assert!(
                set.get_object("b", "k", None).await.is_ok(),
                "calls={calls}: 对账之后对象反而不见了"
            );
        }
    }

    /// 暂存目录绝不能被当成一个版本候选。这条直接盯住 Step 3 修的那个洞：
    /// 把 `meta.xl` 写进 `.staging-*` 之后不提交，GET 必须说「没有这个对象」，
    /// 而不是把半成品读出来。
    #[tokio::test]
    async fn uncommitted_staging_dir_is_invisible_and_reclaimable() {
        let set = set_with_disks(6, 2).await;

        // 手工造一个「写完了 meta、没提交」的现场：6 块盘上都有同一个暂存目录。
        let staging = "b/k/.staging-00000000-0000-0000-0000-000000000001";
        let probe = rstore_meta::encode(&rstore_meta::ObjectMeta {
            versions: vec![rstore_meta::ShallowVersion {
                header: rstore_meta::FileVersionHeader {
                    size: 2_000_000,
                    ec_m: 4,
                    ec_n: 6,
                    ..Default::default()
                },
                body: rstore_meta::encode_body(&rstore_meta::ObjectBody {
                    id: None,
                    parts: Vec::new(),
                    ec_dist: vec![1, 2, 3, 4, 5, 6],
                    checksum_algo: rstore_meta::ChecksumAlgo::Crc32c,
                    storage_class: rstore_meta::StorageClass::Standard,
                    meta_user: Default::default(),
                    meta_sys: Default::default(),
                })
                .unwrap(),
            }],
            inline: Default::default(),
            meta_ver: 1,
        })
        .unwrap();
        for i in 0..6 {
            set.disks()[i]
                .as_ref()
                .unwrap()
                .write_all(&format!("{staging}/meta.xl"), &probe)
                .await
                .unwrap();
        }

        assert!(
            matches!(
                set.get_object("b", "k", None).await,
                Err(StoreError::NotFound)
            ),
            "未提交的暂存目录被当成了版本"
        );

        let orphans = set.scan_orphans("b").await.unwrap();
        assert!(
            orphans.iter().any(|o| o.contains(".staging-")),
            "对账没把暂存目录认成孤儿: {orphans:?}"
        );

        set.reclaim_orphans("b").await.unwrap();
        for i in 0..6 {
            let entries = set.disks()[i]
                .as_ref()
                .unwrap()
                .list_dir("b/k")
                .await
                .unwrap_or_default();
            assert!(entries.is_empty(), "disk {i} 上还有残留: {entries:?}");
        }
    }
}
```

> **要有第四条测试：`reclaim_keeps_the_delete_marker_as_authority`。**
> 上面三条覆盖了 PUT 中断、GC 中断、暂存目录不可见，但**没有一条**盯住
> 「删除标记必须走 `Version` 分支而不是 `Absent` 分支」——而这正是三态契约存在的
> 全部理由。构造：先 PUT，再 DELETE，让删除标记只落到 4 块盘（其余 2 块仍留旧目录）；
> 对账**前后逐盘 `list_dir` 必须完全相同**，且 `get` 仍是 `NotFound`。
> 若误走 `Absent` 的「全删」分支，那 2 块盘的旧目录会被清掉，两侧列表不再相等，
> 断言变红。这一条是 4.10 的核心不变量，不能省。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store reconcile`
Expected: 编译失败

- [ ] **Step 3: 实现**

```rust
pub async fn scan_orphans(&self, bucket: &str) -> Result<Vec<String>, StoreError> {
    // 1. 列出 bucket 下的所有 key（每块盘各列一次，取并集——某块盘可能缺某些 key）。
    // 2. 对每个 key 调 `get::resolve_version`，**三态各有一种处理**：
    //      Version { dir: w, .. } → 该 key 下 `name != w` 的一律是孤儿；
    //                               删除标记目录**也是 w**，所以它自己不会被列成孤儿。
    //      Absent                 → 发现阶段已排除了所有非 `.staging-` 条目，
    //                               所以这个 key 下剩的全是 `.staging-*`，**全部是孤儿**。
    //      Err(ReadQuorum)        → **跳过这个 key**（不列任何孤儿）并记日志。
    //                               有候选目录却选不出权威版本，此刻删什么都不安全。
    // 3. `resolve_version` 另外还会回 `Err(Disk(...))` 之类的硬错误——同样跳过该 key。
}

pub async fn reclaim_orphans(&self, bucket: &str) -> Result<(), StoreError> {
    // 逐盘逐 key，按同一个三态分派：
    //   Version { dir: w, .. } → 只在这块盘**同时持有 w** 时才删该盘上的其余条目
    //                            （直接复用 `delete::gc_superseded`，不要再写一份判定）；
    //                            没拿到 w 的盘一个都不动——它上面那份旧分片仍是有效冗余。
    //   Absent                 → 该 key 下只剩 `.staging-*`，逐盘 `remove_dir_all` 删掉即可
    //                            （没有权威目录要保护，也没有任何东西读得出来）。
    //   Err(ReadQuorum)        → 跳过该 key。
}
```

> **`Absent` 这一支不能顺手推广成「没有 winner 就删光一切」。** 它成立的前提是
> 「发现阶段已把所有非 `.staging-` 条目排除掉了」——即 `Absent` **等价于**该 key 下
> 只剩暂存目录。删除标记的情形**不是** `Absent`：它有权威目录（标记目录），走的是
> `Version` 那一支。把两者混为一谈，就会出现「对账删掉标记目录 → 那些没拿到标记的盘上
> 旧版本复活」，恰好违反本任务要钉住的不变量 2。

> **`ok_count` 那个守卫不能省。** 参考 Task 4.4 的 `never_reports_success_below_quorum`——
> 那里踩过的坑一模一样：缺了守卫，`Ok` 分支一次都没进，测试却是绿的。

- [ ] **Step 4: 提交**

Run: `cargo test -p rstore-store reconcile`
Expected: 全部 PASS

```bash
git add crates/store/
git commit -m "test(store): interruption-point invariants and orphan reconciliation

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

### Task 4.11: 桶操作与对象列举

**Files:**
- Create: `crates/store/src/bucket.rs`、`crates/store/src/list.rs`
- Modify: `crates/store/src/error.rs`（加 `BucketNotEmpty`）
- Modify: `crates/store/src/get.rs`（把「取一个权威版本的 etag」抽成 `pub(crate)` 复用）
- Modify: `crates/store/src/reconcile.rs`（`entries_under` 改 `pub(crate)`，见「对象列举怎么走」）
- Modify: `crates/common/src/consts.rs`（加 `RESERVED_PREFIX`，见下）
- Modify: `crates/store/src/lib.rs`（加 `pub mod bucket;`、`pub mod list;`）

> **`RESERVED_PREFIX` 在本任务定义，不在 5.7。** 遍历要跳过 `.rstore*` 开头的条目，
> 而这个常量是**本任务**第一个需要它的地方（`rstore-store` 的 allowlist 里有
> `rstore-common`，能看见）。原计划把它排在 Task 5.7，那是顺序倒挂：
> `list.rs` 会用到一个还没被定义的常量，实现者只能内联字面量 `".rstore"`，
> 于是同一个前缀在 store 与 s3 两处各写一份——正是这个常量存在的意义所要防的事。
>
> ```rust
> // crates/common/src/consts.rs
> /// 用户可见命名空间里的保留前缀（DESIGN §6.3）：对象 key 的首段、
> /// 以及盘根 / 桶根下的系统目录都不得以它开头。
> /// **store 的目录遍历与 s3 的 key 校验都引用这一个常量，不得内联字面量。**
> pub const RESERVED_PREFIX: &str = ".rstore";
> ```
>
> 谁先需要谁定义：`rstore-common` 是唯一同时被 `rstore-store` 与 `rstore-s3`
> 看见的 crate，放这里两边都能用，也不会给 allowlist 添新边。

> **为什么会有这一节。** M5 的 5.2（`CreateBucket` / `DeleteBucket` / `HeadBucket` /
> `ListBuckets`）与 5.5（`ListObjectsV2`）都假定存储层已经有桶操作与对象列举——
> 但 M4 的 4.1~4.10 从头到尾只实现了**对象级**的 PUT / GET / DELETE。
> 全篇 grep `create_bucket` / `list_objects` 只在 5.7 与 5.5 的描述里各出现一次，
> 没有任何一个 Task 实现它们。不补这一节，5.2 与 5.5 是**写不出来**的：
> 适配器那一层只有 `Arc<dyn ObjectStore>`，不可能自己去遍历盘。

#### 桶怎么表示（MVP 的一条规则）

**桶存在 ⟺ 该桶目录下有 `.rstore.sys/bucket.meta`。** 这跟 DESIGN §6.2 给桶级元数据
留的 `.rstore.sys/` 是同一个位置；MVP 只在里面放一个空标记文件（内容是 `{}`，
不解析、不演进——DESIGN 把 policy / versioning / usage 都划在后续阶段）。

为什么要标记文件而不是「有目录就算有桶」：`delete_bucket` 会把整个桶目录删掉，
之后任何残留的空目录都会让「桶还在不在」这个判断失真。标记文件是显式的。

`DiskAPI` 没有 `mkdir`，但 `write_all` 会创建父目录（LocalDisk 就是这条路），
所以 `create_bucket` 写 `"<bucket>/.rstore.sys/bucket.meta"` 一步就够了。

#### 桶级元数据的 quorum

桶级元数据**没有纠删码**（它不是对象数据），所以不能套用 `read_quorum` / `write_quorum`
那两个针对分片的定义。这里只需要一个语义：**多数派可见**。复用已经有定义的那个数——
`delete_quorum(total) = total / 2 + 1`（严格多数）——并把它当作「桶级操作的门槛」：

- `create_bucket`：逐盘写标记，成功盘数 ≥ 严格多数 → `Ok`；否则
  `Err(StoreError::WriteQuorum { achieved, required: 严格多数 })`。
- `bucket_exists`：标记在 **≥ 严格多数** 的盘上存在 → `true`。
- `delete_bucket`：逐盘 `remove_dir_all("<bucket>")`，成功盘数 ≥ 严格多数 → `Ok`；
  否则 `Err(StoreError::WriteQuorum { achieved, required: 严格多数 })`
  （**复用 `WriteQuorum`，不要为「删桶没删动」新造变体**——5.8 的错误映射表里
  它对应 500，与新建桶失败同一类；测试 `delete_bucket_needs_a_strict_majority`
  断言的就是这个变体）。
  删之前先确认桶存在：桶压根不在 → `Err(StoreError::NotFound)`。
  **但「存在」的判定必须比 `bucket_exists` 弱**：这里是「**任意一块在线盘**上读到标记文件」，
  而不是 `bucket_exists` 的「≥ 严格多数」。两者不能混用——`delete_bucket_needs_a_strict_majority`
  这条测试让 3 块盘掉线，标记只剩 3 份 < 4，若用 `bucket_exists` 判定就会得到
  `NotFound`，而测试断言的是 `WriteQuorum`。语义上也该如此：
  「桶存在但掉线过半」是一次**写失败**（503 + Retry-After，可以重试），
  不是「桶本来就不存在」（404，重试无用）——把后者报给客户端会让它以为数据没了。
  这也是 `ApiError` 里 `NoSuchBucket` 与 `Unavailable` 分开的理由。

用同一个数是为了避免再造一个新常量；写进注释说明它是「桶级元数据的多数派门槛」，
**不是** `delete_quorum` 在语义上被挪用——两者恰好都是「严格多数」而已。

> 这条规则保证的是**单调性**：不会出现「一半盘认为桶在、一半认为不在」，于是连续两次
> `HeadBucket` 得到相反的答案。这是放弃纠删码之后能拿到的最弱但足够的不变量。

#### 契约

```rust
/// LIST 返回的一行。**只包含仍然活着的对象**——删除标记与纯孤儿都不出现。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectEntry {
    pub key: String,
    pub size: u64,
    pub etag: String,
    /// `header.mod_time`；`None` 时按 0 处理（PUT 总会写它，见 4.5）。
    pub mod_time: u64,
}

impl ErasureSet {
    /// 幂等：桶已存在也返回 `Ok`（S3 的 `BucketAlreadyOwnedByYou` 不在 MVP 范围内，
    /// 而 `aws s3 mb` 的重复调用不该让冒烟脚本失败）。
    pub async fn create_bucket(&self, bucket: &str) -> Result<(), StoreError>;

    /// 桶里有**活对象**（即 `list_objects(bucket, None)` 非空）→ `BucketNotEmpty`。
    /// 只有删除标记与孤儿目录的桶算空桶，可以删。
    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), StoreError>;

    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool, StoreError>;

    /// 所有盘的桶名并集（已排序）。**跳过以 `.` 开头的条目**——
    /// 盘根下有 `.rstore.sys/`（DESIGN §6.2），它不是一个桶。
    pub async fn list_buckets(&self) -> Result<Vec<String>, StoreError>;

    /// `bucket` 下所有活对象，按 key 升序。`prefix` 为 `None` 时返回全部。
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ObjectEntry>, StoreError>;
}
```

`StoreError` 加一个变体（其余不动）：

```rust
#[error("bucket not empty")]
BucketNotEmpty,
```

#### 对象列举怎么走（递归 + 复用 `resolve_version`）

`list_dir` 只返回**条目名**、不区分文件与目录（`fsx::list_dir` 就是这么实现的），
所以递归时每个条目要补一次 `stat` 看 `is_dir`。规则：

1. 逐盘从 `<bucket>` 开始递归，收集候选 key，**取所有盘的并集**
   （某块盘可能缺某些 key）。对当前目录 `D`（相对 bucket 的路径为 `p`，根为 `""`）：
   - `list_dir(D)` 包含 `meta.xl` → `D` 是一个**版本目录**，它的父路径 `p` 就是一个 key，
     记入候选并**停止往下递归**；
   - 否则对每个条目 `name`：
     - 跳过 `name.starts_with(".staging-")`（未提交的暂存目录，4.10）；
     - **`p` 为空（bucket 根）时**，再跳过 `name.starts_with(".rstore")`——
       那是系统目录（`.rstore.sys`、将来的 `.rstore.uploads`）。用户 key 的首段
       不可能是这个前缀（DESIGN §6.3 / Task 5.7 保证），所以这个跳过不会藏掉用户数据；
       更深层的段**不跳**（`a/.rstore/x` 是合法 key，跳过它就是「GET 得到、LIST 看不到」
       这种最难查的不一致）；
     - `stat(D/name).is_dir` 为真才递归进去。
2. 对每个候选 key：`resolve_version(set, bucket, key)`，只有 `live()` 是 `Some` 才收录。
   删除标记与「只剩暂存目录」的 key 因此自然消失——**与 GET 用的是同一处判断**，
   不会有「GET 说没有、LIST 说有」。
   **`Err(e)` 必须原样上抛，不能 `if let Ok(..)` 吞掉。** 只剩 2 块在线盘的 set
   照样能列出候选目录，但元数据过不了 `read_quorum`——此时「列不全」与「列对了」
   在返回值上无法区分，唯一安全的做法是报 `ReadQuorum`。吞掉它的表现是
   **静默返回一个残缺列表**，而 `rclone sync` 会拿这个列表去删远端数据。
3. 按 key 升序排序（并集来自多块盘，顺序不保证）。
4. `prefix` 过滤在最后做一次 `key.starts_with(prefix)`。

> **「并集」别再写第二遍。** `reconcile.rs` 里已经有一个 `entries_under(rel)`，
> 做的就是「各在线盘 `list_dir(rel)` 的并集，缺失/读不到按空处理」。把它改成
> `pub(crate)` 后 `list.rs` 直接用——本任务的文件清单里因此要加一行
> `Modify: crates/store/src/reconcile.rs`。在 `list.rs` 里重写一遍的代价是
> 两处「某块盘读不到怎么办」的策略会各自漂移，而对账与列举对这件事的答案必须相同。

**性能**：MVP 是全盘遍历 + 每 key 一次元数据仲裁，`// PERF: 见 DESIGN §1.2 与 §20
Phase 2 — 命名空间索引` 的挂钩注释留在这里（5.5 还要再留一次）。不要试图在 MVP 里
做前缀剪枝：`prefix` 是按 key 的字符串前缀，而 key 的目录切分与它并不对齐
（`prefix = "a/b"` 可能落在 `a/b` 或 `a/bc` 两个目录下），剪枝剪错就是静默丢结果。

#### etag 只能有一处算法

`GetOut` 要 etag，`ObjectEntry` 也要 etag，而内联对象的 etag **没落进 meta**
（4.5 的内联分支 `parts` 是空的，见 Task 4.7 的说明）。所以把「从一个权威版本取出 etag」
抽成 `get.rs` 里的 `pub(crate) fn`，让 GET 与 LIST 共用：

```rust
/// 取 `bucket/key` 权威版本的 etag。
/// 分片对象直接读 `parts[0].etag`；内联对象现算 `etag_of(inline)`。
pub(crate) fn etag_of_meta(meta: &rstore_meta::ObjectMeta) -> Result<String, StoreError>;
```

它内部就是 `get_object` 现在那两段：取 `latest_version(meta)` → `decode_body(latest.body)`
→ 看 `Flags::INLINE_DATA` / `keys::INLINE_DATA` 分流。**一个例外要单独处理**：
`get_object` 分片分支在 `parts` 为空时回落到 `etag_of(&full)`，而 `etag_of_meta`
手上**没有 `full`**（它不该为了算个 etag 去读整份分片）。所以 `parts` 为空且不是内联
→ `Err(StoreError::Internal("version has neither inline data nor parts"))`。
这是元数据自相矛盾的信号，不是正常路径；报错比猜一个 etag 强。

改完之后 `get_object` 的两处 etag 计算都换成调用 `etag_of_meta(meta)`（内联分支
本来就用 `etag_of(&full)`，而 `full` 就是 `inline["null"]`，与 `etag_of_meta` 算的
是同一个值——**换成共用那个，别留两份**）。

**别在 `list.rs` 里再写一份**——两处算法分叉的表现是「HEAD 的 ETag 与 LIST 的不一样」，
而 S3 客户端（`rclone check`）会拿它当校验依据。

> **PUT 的用户元数据（`x-amz-meta-*`）在 MVP 里不落盘**：`ObjectBody::meta_user` 恒为空
> `BTreeMap`。PUT / HEAD / GET 都不会转发它。这是已知限制，不是 bug——
> 要支持得先让 `PutArgs` 带上元数据、`build_meta` 写进 body、GET 再从 `decode_body` 取出来。
> 同理，**分片对象也不返回 `x-amz-meta-mtime` 之类**。M5 不要为此去改 store 层。

- [ ] **Step 1: 写失败测试**

```rust
#[cfg(test)]
mod tests {
    use rstore_disk::faulty::Fault;

    use super::*;
    use crate::error::StoreError;
    use crate::put::PutArgs;
    use crate::testutil::{set_with_disks, TestSet};

    fn put_args(bucket: &str, key: &str, n: usize) -> PutArgs {
        PutArgs { bucket: bucket.into(), key: key.into(), data: vec![7u8; n] }
    }

    #[tokio::test]
    async fn create_then_exists_and_recreate_is_ok() {
        let set = set_with_disks(6, 2).await;
        assert!(!set.bucket_exists("data").await.unwrap());

        set.create_bucket("data").await.unwrap();
        assert!(set.bucket_exists("data").await.unwrap());

        // 幂等：重复建桶不该失败（`aws s3 mb` 会无条件调用它）。
        set.create_bucket("data").await.unwrap();
        assert!(set.bucket_exists("data").await.unwrap());
    }

    #[tokio::test]
    async fn delete_bucket_rejects_non_empty_then_succeeds_when_empty() {
        let set = set_with_disks(6, 2).await;
        set.create_bucket("data").await.unwrap();
        set.put_object(put_args("data", "k", 1_000_000)).await.unwrap();

        let r = set.delete_bucket("data").await;
        assert!(matches!(r, Err(StoreError::BucketNotEmpty)), "got {r:?}");
        assert!(set.bucket_exists("data").await.unwrap(), "拒绝之后桶必须原样在");

        // 删掉对象（写的是删除标记）之后桶就算空了：墓碑与孤儿都不算「非空」。
        set.delete_object("data", "k").await.unwrap();
        set.delete_bucket("data").await.unwrap();
        assert!(!set.bucket_exists("data").await.unwrap());
    }

    #[tokio::test]
    async fn list_objects_skips_delete_markers_and_uncommitted_staging() {
        let set = set_with_disks(6, 2).await;
        set.create_bucket("data").await.unwrap();
        set.put_object(put_args("data", "keep", 1_000_000)).await.unwrap();
        set.put_object(put_args("data", "gone", 1_000_000)).await.unwrap();
        set.delete_object("data", "gone").await.unwrap();

        // 手工造一个「写完 meta、没提交」的现场：它绝不能被 LIST 当成一个对象。
        for i in 0..6 {
            set.disks()[i]
                .as_ref()
                .unwrap()
                .write_all("data/ghost/.staging-00000000-0000-0000-0000-000000000002/meta.xl", b"x")
                .await
                .unwrap();
        }

        let entries = set.list_objects("data", None).await.unwrap();
        let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, vec!["keep"], "got {entries:?}");
        assert_eq!(entries[0].size, 1_000_000);
        // etag 必须是 32 位小写十六进制的 MD5（与 PUT 的 PutOut.etag 同源）。
        assert_eq!(entries[0].etag.len(), 32, "etag={}", entries[0].etag);
        assert!(entries[0].etag.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    /// 前缀过滤 + 嵌套 key。**特别钉住 `a/.rstore/x` 这种深层保留名**：
    /// 它首段是 `a`，是合法 key（5.7 只禁首段），GET 能读到它就说明 LIST 也必须列出来。
    #[tokio::test]
    async fn list_objects_walks_nested_keys_and_filters_prefix() {
        let set = set_with_disks(6, 2).await;
        set.create_bucket("data").await.unwrap();
        for key in ["a", "dir/b", "dir/c/d", "a/.rstore/x"] {
            set.put_object(put_args("data", key, 200_000)).await.unwrap();
        }

        let all: Vec<String> = set
            .list_objects("data", None)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(all, vec!["a", "a/.rstore/x", "dir/b", "dir/c/d"], "必须按 key 升序");

        let dir: Vec<String> = set
            .list_objects("data", Some("dir/"))
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(dir, vec!["dir/b", "dir/c/d"]);
    }

    /// 掉线 4 块盘（只剩 2 块）时列举必须**失败**，而不是返回一个缺了内容的列表：
    /// 只剩 2 份元数据过不了 read_quorum，此时「列不全」与「列错」无法区分。
    #[tokio::test]
    async fn list_objects_fails_rather_than_truncates_below_quorum() {
        let set = set_with_disks(6, 2).await;
        set.create_bucket("data").await.unwrap();
        set.put_object(put_args("data", "k", 1_000_000)).await.unwrap();

        for i in 0..4 {
            set.inject_fault_on(i, Fault::Offline);
        }
        let r = set.list_objects("data", None).await;
        assert!(
            matches!(r, Err(StoreError::ReadQuorum { .. })),
            "低于 quorum 时必须报错，不能返回残缺列表: {r:?}"
        );
    }

    #[tokio::test]
    async fn list_buckets_skips_system_dirs() {
        let set = set_with_disks(6, 2).await;
        set.create_bucket("alpha").await.unwrap();
        set.create_bucket("beta").await.unwrap();

        // 盘根下的盘级系统目录（DESIGN §6.2 的 `<disk>/.rstore.sys/disk_id`）不是桶。
        for i in 0..6 {
            set.disks()[i]
                .as_ref()
                .unwrap()
                .write_all(".rstore.sys/disk_id", b"not-a-bucket")
                .await
                .unwrap();
        }

        let buckets = set.list_buckets().await.unwrap();
        assert_eq!(buckets, vec!["alpha", "beta"]);
    }

    #[tokio::test]
    async fn delete_bucket_needs_a_strict_majority() {
        let set = set_with_disks(6, 2).await;
        set.create_bucket("data").await.unwrap();

        // 严格多数 = 6/2+1 = 4；3 块可用 < 4。
        for i in 0..3 {
            set.inject_fault_on(i, Fault::Offline);
        }
        let r = set.delete_bucket("data").await;
        assert!(
            matches!(r, Err(StoreError::WriteQuorum { .. })),
            "got {r:?}"
        );
    }
}
```

> 最后一条测试里的 `3` 与 `4` 是按**定义**算的（严格多数 = `total / 2 + 1` = 4），
> 不要照抄注释；任务 4.9 已经因为同一类错误返工过一次。

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store bucket`
Expected: 编译失败（`no method named create_bucket`）

- [ ] **Step 3: 实现**

按上面的契约实现。几处要点：

- `create_bucket` / `delete_bucket` / `bucket_exists` 里的「逐盘」用
  `self.disks().iter().flatten()`；掉线的盘与 `None` 槽位都只算「没成功」，
  不像错误一样上抛（除非成功盘数不达标）。
- `delete_bucket` 必须先判空再删：反过来就是「先删了再发现不该删」。
- `list_objects` 的递归深度用 bucket 内 key 的层数封顶没有意义（key 可以任意深），
  但要注意**不要跟着符号链接走**——`stat` 给的是 `fs::metadata`，会跟随链接；
  MVP 的数据目录由本程序自己创建，不产生链接，这条只作为注释记下。
- `list.rs` 与 `bucket.rs` 都 `use crate::get::resolve_version`（已是 `pub(crate)`）。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store bucket`
Expected: PASS

```bash
git add crates/store/
git commit -m "feat(store): bucket operations and object listing

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

## M5 — S3 接入

**任务清单与执行顺序：**

| Task | 内容 | 依赖 |
|---|---|---|
| 5.1 | s3s 骨架与认证 | — |
| 5.2 | 桶操作 | 5.1 |
| 5.3 | 对象读写 | 5.2 |
| 5.4 | Range | 5.3 |
| 5.5 | ListObjectsV2 | 5.3 |
| 5.6 | Multipart 一律 501 | 5.1 |
| 5.7 | 命名校验（保留前缀 + 盘上碰撞的 key 形状） | 5.3 |
| 5.8 | 错误映射 | 5.2 |
| 5.9 | 兼容层与客户端冒烟测试 | **6.3**（本计划唯一一处 M5 依赖 M6） |
| 5.10 | 条件请求（GET / HEAD 子集） | 5.4 |
| 5.11 | 虚拟主机风格寻址 | 5.1 |

> 5.9~5.11 在文档里排在 5.2~5.8 之后**只因为它们是后来补的**，不是执行顺序：
> **5.7 / 5.8 / 5.10 / 5.11 应该在 5.9 之前跑完**——它们的测试都是
> `oneshot` 级别的，不依赖真实进程；而 5.9 要等到 6.3 才有服务可起。
> 反过来，**不要把 5.9 排到最后才做**：它是「客户端真的能用」的唯一证据，
> 而 5.6 的 501、5.7 的 key 规则、5.10 的条件请求都可能被它推翻。

### Task 5.1: s3s 骨架与认证

**Files:**
- **Modify** `crates/api/src/lib.rs`（**已存在**，M0 的骨架只有一行 doc 注释）
- Create: `crates/api/src/error.rs`
- Modify: `crates/api/Cargo.toml`（加 `async-trait`）
- **Modify** `crates/s3/src/lib.rs`（**已存在**的骨架）
- Create: `crates/s3/src/impl_s3.rs`
- Modify: `crates/s3/Cargo.toml`（`s3s` / `async-trait` / `bytes` / `futures`；`http` **必须在
  `[dependencies]`** 里——`S3Response::with_status` 要 `http::StatusCode`，而 s3s 没有
  re-export 它。测试再加 `tower` / `http-body-util` / `s3s-sigv4`。
  **不要加 `serde_json`**：错误响应的 XML 由 s3s 自己按 `S3Error` 生成，
  我们没有一处要手写 JSON）
- Modify: 根 `Cargo.toml` 的 `[workspace.dependencies]`（加 `s3s = "0.17"`，测试要用的
  `tower` 与 `http` 也一并加进去）

> **不需要 `crates/s3/src/auth.rs`。** 原计划要自己写一个 `AuthProvider`，但 s3s 已经
> 提供了正好是 MVP 需要的那一个：`s3s::auth::SimpleAuth::from_single(access_key, secret_key)`
> （单条静态 root 凭证）。自己再包一层只是把 20 行的东西变成 60 行。
>
> **版本**：`s3s` 最新是 **0.17.0**，MSRV `1.96.0`；本仓库 `rust-toolchain.toml` 锁的是
> `1.97.1`，够用（`cargo info s3s` 可复核）。别用 `0.18.0-alpha`。

> **先看护栏脚本的 allowlist**（`scripts/check_layer_deps.py`）：
> ```python
> "rstore-store": {"rstore-common", "rstore-checksum", "rstore-erasure",
>                  "rstore-meta", "rstore-disk"},
> "rstore-api":   {"rstore-common"},
> ```
> 也就是说 **`store` 与 `api` 是兄弟，谁也不能依赖谁**——
> `scripts/tests/test_check_layer_deps.py` 里有一条测试直接把
> `rstore-api -> rstore-store` 钉成 `FORBIDDEN EDGE`。
> 原计划这一步有两处直接违反它：

**（1）`StoreError` 不能由 api 定义。** 原计划写「api 定义 `ObjectStore` trait 与领域错误
`StoreError`」，而 4.1 已经把 `StoreError` 定在 `rstore-store` 里了。api 看不到 store，
所以 api 必须定义**自己的**错误类型，由组合根做一次映射：

```rust
// crates/api/src/error.rs
/// API 层错误。**不是** `rstore_store::StoreError` 的别名——store 与 api 之间没有依赖边，
/// 两者只能各定一份，再由 `rstore-server` 映射。
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("no such key")]
    NoSuchKey,
    #[error("no such bucket")]
    NoSuchBucket,
    #[error("bucket not empty")]
    BucketNotEmpty,
    #[error("invalid bucket name")]
    InvalidBucketName,
    #[error("invalid object name")]
    InvalidObjectName,
    #[error("invalid range")]
    InvalidRange,
    #[error("not implemented")]
    NotImplemented,
    #[error("unavailable")]
    Unavailable,
    #[error("internal: {0}")]
    Internal(String),
}
```

变体是按 **S3 错误码**挑的，不是照抄 `StoreError`：每个变体在 Task 5.8 的映射表里
都有唯一的 `(Code, HTTP status)`，不出现「两个变体映到同一个码」这种要调用方去猜的歧义。
`NoSuchKey` / `NoSuchBucket` 分开而不是合并成 `NotFound`，是因为 S3 的 `HeadBucket`
要的是 `404 NoSuchBucket`、`GetObject` 要的是 `404 NoSuchKey`——合并之后 5.8 还得反推
「这次是哪个操作」，那是把调用点的信息丢在半路。

**Multipart 系列在 MVP 里不实现**（`ObjectStore` trait 不定义 multipart 方法，
`s3s` 侧的 `CreateMultipartUpload` / `UploadPart` / `CompleteMultipartUpload` /
`AbortMultipartUpload` / `ListParts` / `ListMultipartUploads` 一律返回
`501` + `ApiError::NotImplemented`）。理由：DESIGN 把 multipart 划在 Phase 3，
而 Task 4.5 的存储层只写单个 `part.1`（`PutArgs` 里根本没有 part 列表），
两边同时成立是不可能的——要么改 4.5 的 PUT 设计支持多 part 组装，要么推迟 multipart。
**明确推迟 multipart**，`assert_code(ApiError::NotImplemented, "NotImplemented", 501)` 是它的门。
代价写在 Task 5.9：aws-cli 的 `s3 cp` 超过 8 MiB 会自动改走 multipart，因此兼容冒烟脚本
的载荷固定 < 8 MiB；真实用户传大文件会拿到 501。这是 MVP 的已知限制，不是 bug
（与本文件的「**MVP 的已知限制**」表里的前两行是同一条，那里还列了补它要先改什么）。

M5 的 `Task 5.8: 错误映射` 就是把 `ApiError` 映到 S3 错误码；`StoreError → ApiError`
的转换写在组合根（`rstore-server`），它是唯一同时看得见两边的 crate。

**（2）绑定实现不能直接 `impl ObjectStore for ErasureSet`。** 原计划说「绑定实现的位置是
`rstore-server/src/wiring.rs`」——位置对，但**孤儿规则不允许**：`ObjectStore` 是外部 trait、
`ErasureSet` 是外部类型，第三方 crate 里给「外 trait + 外类型」写 impl 编译不过。
正确做法是在组合根里定义一个**本地**适配器类型：

```rust
// crates/server/src/wiring.rs
struct EngineAdapter {
    set: Arc<ErasureSet>,   // rstore-server 允许依赖 store
}

#[async_trait::async_trait]
impl ObjectStore for EngineAdapter {   // trait 外部、类型本地 → 合法
    ...
    // 每个方法把 StoreError 映射成 ApiError（越界/内部错误在这里归位）
}
```

于是约束没变，还更强了：**`rstore-s3` 只持有 `Arc<dyn ObjectStore>`**，
它的 `Cargo.toml` 永远不需要 `rstore-store`（allowlist 里 `rstore-s3` 只有
`rstore-common` 与 `rstore-api`），可以拿 mock `ObjectStore` 单独测试。

构造参数注入：

```rust
#[derive(Clone)]
pub struct RstoreFs {
    /// 不是 `Arc<ECStore>`——s3 看不到引擎类型（allowlist 里没有那条边）。
    /// 于是它可以拿一个 mock `ObjectStore` 单独测试。
    store: Arc<dyn ObjectStore>,
}
```

凭证不走这个结构体：它是 `S3ServiceBuilder` 的一个参数（见 Step 3）。

- [ ] **Step 1: 定义 `rstore-api` 的契约**

原计划**没有这一步**，只有一句「api 定义 `ObjectStore` trait」——trait 的方法集合从头到尾
没有出现过。而 5.2~5.6 全都是「拿 `ObjectStore` 实现 s3s 的 `S3`」，没有契约就没法开工。
下面是完整定义（`crates/api/src/lib.rs` + `crates/api/src/error.rs`）：

```rust
// crates/api/src/lib.rs
//! 存储契约 trait，供上层消费。不得反向依赖任何实现 crate。
pub mod error;

pub use error::ApiError;

use async_trait::async_trait;

/// S3 层能对引擎提出的全部问题。**刻意不含 multipart**——MVP 一律返回 501，
/// 所以 trait 上根本没有对应方法，编译期就堵死了「不小心实现了半个 multipart」。
///
/// 所有方法返回 [`ApiError`] 而**不是** `rstore_store::StoreError`：`rstore-api` 与
/// `rstore-store` 是兄弟，allowlist 里没有这条边（`scripts/tests/test_check_layer_deps.py`
/// 把 `rstore-api -> rstore-store` 钉成 FORBIDDEN EDGE）。`StoreError -> ApiError`
/// 的映射写在组合根 `rstore-server/src/wiring.rs`。
#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    async fn create_bucket(&self, bucket: &str) -> Result<(), ApiError>;
    async fn delete_bucket(&self, bucket: &str) -> Result<(), ApiError>;
    async fn head_bucket(&self, bucket: &str) -> Result<(), ApiError>;
    async fn list_buckets(&self) -> Result<Vec<String>, ApiError>;

    async fn put_object(&self, bucket: &str, key: &str, data: Vec<u8>)
        -> Result<ObjectInfo, ApiError>;
    async fn get_object(&self, bucket: &str, key: &str, range: Option<ByteRange>)
        -> Result<ObjectData, ApiError>;
    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo, ApiError>;
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), ApiError>;

    /// MVP 是**全盘遍历**（见 Task 4.11），返回已按 key 升序。
    async fn list_objects(&self, bucket: &str, prefix: Option<&str>)
        -> Result<Vec<ObjectEntry>, ApiError>;
}

/// 闭区间 `[start, end]`。**不复用 `rstore_store::ByteRange`**（那条边不存在）——
/// 两边各定一份，由组合根转换。把 `bytes=a-b` / `bytes=a-` / `bytes=-n` 解析并裁剪成
/// 闭区间是 S3 层的职责（Task 5.4），到这里必须已经是越界检查过的。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub size: u64,
    pub etag: String,
    /// Unix 纳秒。`rstore_meta` 里是 `Option<u64>`；本层统一成 `u64`
    /// （PUT 总会写它，见 4.5），组合根负责 `unwrap_or(0)`。
    pub mod_time: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectData {
    /// **请求范围内**的字节（`range` 为 `None` 时是整份）。
    pub data: Vec<u8>,
    /// 整个对象的原始长度（`Content-Range` 要它）。
    pub size: u64,
    pub etag: String,
    pub mod_time: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectEntry {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub mod_time: u64,
}
```

**MVP 不做流式**，所以 `data: Vec<u8>`：分片本来就整份读进内存再解码，再包一层
`AsyncRead` 只是给同一块内存加一层接口（理由同 Task 4.7）。配合 8 MiB 的载荷上限
（multipart 未实现，见 5.6），内存占用是有界的。

- [ ] **Step 2: 写集成测试**

原计划这两条测试**只有注释、没有一行代码**，而且「启动服务」在单元测试里意味着绑端口——
那是 flaky 的根源。`S3Service` 是 hyper + tower 的 service，直接用
`tower::ServiceExt::oneshot` 打进去，**零端口、零等待**：

```rust
#[cfg(test)]
mod tests {
    use http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use s3s::auth::SimpleAuth;
    use s3s::service::S3ServiceBuilder;
    use tower::ServiceExt;

    use super::*;

    /// 一个什么都没做的 ObjectStore：本节只测认证，不测业务。
    struct NopStore;
    #[async_trait::async_trait]
    impl ObjectStore for NopStore {
        async fn list_buckets(&self) -> Result<Vec<String>, ApiError> { Ok(Vec::new()) }
        // 其余方法一律 `Err(ApiError::Internal("nop".into()))`。
        // **写出来，别用 `todo!()`**：`unsafe_code = "forbid"` 不拦它，但 panic
        // 会污染测试输出，而且一个 panic 的桩会让「测试绿了」这件事失去意义。
    }

    fn service() -> s3s::service::S3Service {
        let mut b = S3ServiceBuilder::new(RstoreFs { store: Arc::new(NopStore) });
        b.set_auth(SimpleAuth::from_single("testkey", "testsecret"));
        b.build()
    }

    async fn status_of(req: Request<Vec<u8>>) -> StatusCode { /* oneshot + parse */ }

    #[tokio::test]
    async fn rejects_bad_signature() {
        // 错误密钥签出来的 Authorization → 403，且响应体的 <Code> 是 SignatureDoesNotMatch
    }

    #[tokio::test]
    async fn accepts_valid_sigv4() {
        // 用 s3s 自带的 sigv4 工具（`s3s::crypto` / `s3s_sigv4`）签一个 ListBuckets → 200
    }
}
```

**不必自己实现签名算法**：`s3s` 依赖里已经带了 `s3s-sigv4`（`s3s::crypto` 下也有现成的
签名工具）。用被测代码**不同一条路径**的签名实现来生成请求，才是真的在测「服务端的校验」。

- [ ] **Step 3: 实现**

```rust
let mut builder = S3ServiceBuilder::new(RstoreFs { store });
builder.set_auth(SimpleAuth::from_single(&access_key, &secret_key));
let service = builder.build();
```

`access_key` / `secret_key` 由调用方（Task 6.3 的配置加载）传进来，5.1 不读文件。
`RstoreFs` 实现 `s3s::S3`，每个方法把 `ObjectStore` 的结果译成 `S3Response`；
**multipart 的六个方法一律 `Err(s3s::s3_error!(NotImplemented))`**（见 5.6）。

> **5.1 就把全部非 multipart 方法写成「直接委托 + 翻译」，5.2~5.5 只在上面加各自的专门行为。**
> 这两节看起来都写了「实现」，边界是：
>
> - **5.1**：九个方法各自一行委托，`get_object` 传 `range: None`，`list_objects_v2` 忽略
>   `delimiter` / `max_keys` / `continuation_token`。目的是**骨架能跑通、认证能测**——
>   5.1 Step 2 的 `accepts_valid_sigv4` 要求 `ListBuckets` 真的返回 200，所以
>   `NopStore` 的九个桩必须全部被调到。
> - **5.2~5.5**：只加「本组特有」的东西——5.4 的 `resolve_range`、5.5 的分页与
>   `common_prefixes`，以及各自那组测试。
>
> 所以 5.2~5.5 的 Step 2 **不是**从头实现一遍，而是改 5.1 留下的那几处。
> 若把 5.1 缩成「只实现 `list_buckets`」，5.1 的测试就退化成只验一条路径，
> 而其余八个方法的编译错误要等到 5.2 才暴露。

> **5.1 里 `ApiError → S3 错误` 先写成 `impl_s3.rs` 内的私有 `fn to_s3_error`，**
> 并标 `// TODO(Task 5.2): 挪到 errors.rs`。**Task 5.2 负责建 `errors.rs` 并把它搬过去**
> （见 Task 5.2 的 Files）。5.1 不建 `errors.rs` 是为了不在骨架阶段就分出两个文件；
> 但**搬过去这件事必须在 5.2 做完**，别让它以 `TODO` 的形态漂到 5.8。

> `S3ServiceBuilder` 默认带 `AwsNameValidation`（桶名规则）。**5.7 因此不再重复实现
> 桶名校验**——见那里的说明。

- [ ] **Step 4: 提交**

```bash
git add Cargo.toml crates/api/ crates/s3/
git commit -m "feat(s3): s3s service skeleton with single root credential auth

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5.2 ~ 5.6: S3 操作实现

> **先读这一节的前三段，它们把 5.2~5.6 共用的东西讲完。**
> 原计划这五个 Task 挤成一个表格，每格一句「测试要点」，没有一行代码、没有一个
> 文件路径。实现者拿到的是「实现 `PutObject` / `GetObject` / `HeadObject` /
> `DeleteObject`」这种句子——这跟没有规格的区别，只是它看起来像有规格。

#### 一、s3s 0.17 已经替你做完的事（已逐条核实，别再自己造）

| 事实 | 后果 |
|---|---|
| `S3` trait 的**每个方法都有默认实现**，就是 `Err(s3_error!(NotImplemented, "… is not implemented yet"))` | **5.6 是零代码**：只要不覆写那六个 multipart 方法，它们天生返回 501。别再手写六个 `Err(...)` |
| `BucketName` / `ObjectKey` / `Prefix` / `Delimiter` / `Token` / `NextToken` / `StartAfter` / `ContentRange` / `AcceptRanges` / `ObjectVersionId` 都是 `pub type X = String` | 直接当 `String` 用，不必 `into()` 猜类型 |
| **`ETag` 是 `enum ETag { Strong(String), Weak(String) }`，不是 `String` 别名** | `e_tag: Some(ETag::Strong(etag))`。写成 `Some(etag)` 编译不过——这是本表里唯一一个「看着像 String 其实不是」的类型 |
| `Size` / `ContentLength` / `ObjectSize` = `i64`；`MaxKeys` / `KeyCount` = `i32`；`IsTruncated` = `bool` | 注意 `Size` 是 **`i64`**，`ObjectData.size` 是 `u64`，要显式 `as i64` |
| `pub type List<T> = Vec<T>`；`Buckets = List<Bucket>`；`ObjectList = List<Object>` | `contents: Some(vec![Object { … }])` |
| `S3Response<T>` 的 `output` 字段**会被自动序列化成响应头**（`etag` → `ETag`、`content_length` → `Content-Length`、`last_modified` → `Last-Modified`、`e_tag`、`accept_ranges`、`content_range`…） | 只需要填 `output` 的字段；**不要**手工往 `S3Response.headers` 里塞这些头，塞了也是双份 |
| `GetObjectOutput.content_range` 为 `Some` 时，序列化器**自动把状态码设成 206** | 5.4 不需要自己设 status，只要算出 `content_range` 字符串 |
| `GetObjectInput.range` 已经被 s3s 解析成 `Range::Int { first, last: Option<u64> }` / `Range::Suffix { length: u64 }`（`Range::parse` 内部做） | **5.4 不写 `bytes=` 解析器**。s3s 只做到「语法解析」，它不知道对象多大，所以**闭合区间与越界检查仍是我们的活** |
| `StreamingBlob::from_bytes(Bytes)`；`StreamingBlob` 实现 `Stream<Item = Result<Bytes, StdError>>` | 读请求体用 `futures::TryStreamExt::try_collect::<Vec<Bytes>>()` 再 `.concat()`——**`try_concat()` 用不了**，它要求 `Bytes: Extend<u8>`，而 `Bytes` 不满足。造响应体用 `StreamingBlob::from_bytes` |
| `http` 与 `http-body-util` **没有被 s3s 公开 re-export**（`s3s::http` 是私有模块） | 两者都要进 `crates/s3/Cargo.toml` 的 **`[dependencies]`**——不只是 dev-dependencies：`S3Response::with_status` 要 `http::StatusCode`，5.6 的测试要 `http_body_util::Full` |
| `s3s-sigv4` 也**没有**被 s3s 完整 re-export（只公开了 `AmzDate`，`AuthorizationV4` 在 fuzzing cfg 后面） | 测试要自己签名时，把 `s3s-sigv4 = "0.17"` 显式加进 `[dev-dependencies]`。另外 `Payload::empty()` 是 `#[cfg(test)]` 的，用不了——用公开的 `EMPTY_STRING_SHA256_HASH` + `Payload::SingleChunk(..)` |
| `S3ServiceBuilder` 默认容忍 **900 秒**时钟偏移 | 签名测试必须用**当前 UTC 时间**；credential scope 里的日期是 **`YYYYMMDD`（8 位）**，不是完整 ISO8601——写错会被判 `Authorization` malformed |
| 测试请求体**不能**是 `Request<Vec<u8>>` | `Vec<u8>` 不实现 `http_body::Body`，而 `S3Service` 要求 `B: Body<Data = Bytes>`。测试里用 `http_body_util::Full<Bytes>` |
| `Timestamp: From<SystemTime>` | `mod_time`（Unix 纳秒）→ `SystemTime::UNIX_EPOCH + Duration::from_nanos(n)` → `Timestamp::from(..)` |
| `s3s::validation::NameValidation` 只有 `validate_bucket_name`；`S3ServiceBuilder` 默认挂 `AwsNameValidation` | 桶名校验白送；**对象 key 没有校验钩子**，只能自己写（5.7） |

#### 二、测试怎么打：零端口、零磁盘、零引擎

`rstore-s3` 的 allowlist 是 `{rstore-common, rstore-api}`——**它连 `rstore-store` 都看不见**，
所以 M5 的测试**不可能**建一个真的 `ErasureSet`，也不该建（引擎的正确性 M4 已经测完了）。
M5 要测的是**协议翻译**：状态码、响应头、XML 形状、XML 字段。

做法：写一个内存版 `ObjectStore` 假实现，再用 `tower::ServiceExt::oneshot` 把请求直接
打给 `S3Service`（它是 hyper + tower 的 service，**不绑端口、不等待**）。

- [ ] **预备步骤（属于 Task 5.2，不是它 Step 1 之前的独立任务）: 建 `crates/s3/src/mock.rs`**

`#[cfg(test)]` 门控；`lib.rs` 里 `#[cfg(test)] mod mock;`。
它是 5.2~5.6 **全部测试**共用的夹具，先建它。

```rust
//! 内存版 `ObjectStore`，只为 S3 层的协议测试服务。
//!
//! **为什么不用真的 `ErasureSet`**：`rstore-s3` 的 allowlist 里没有 `rstore-store`
//! （见 `scripts/check_layer_deps.py`），依赖边根本不存在。而这也正是对的——
//! 引擎的行为由 M4 的测试负责，这里只该验证「协议翻译」这一层。

use std::collections::BTreeMap;
use std::sync::Mutex;

use rstore_api::{ApiError, ByteRange, ObjectData, ObjectEntry, ObjectInfo, ObjectStore};

#[derive(Default)]
pub struct MockStore {
    /// `(bucket, key) -> (内容, etag, mod_time_nanos)`
    objects: Mutex<BTreeMap<(String, String), (Vec<u8>, String, u64)>>,
    buckets: Mutex<Vec<String>>,
    /// 下一次 `object_miss` 要返回 `NoSuchKey` 而不是 `Unavailable`。
    /// 5.8 的错误映射要靠它区分 404 与 500。
    pub fail_next_with: Mutex<Option<ApiError>>,
}
```

要点，**照做，不要即兴**：

- 锁一律 `std::sync::Mutex`，且**临界区里绝不能出现 `.await`**。`clippy::await_holding_lock`
  在 workspace lints 里是 **`deny`**（不是 warn），跨 await 持锁会直接挡死门禁。
  每个方法里「加锁 → 取值/改值 → 出作用域」写完再 await。
- `fail_next_with` 是个小开关：取一次就清空。**5.8 的错误映射测试需要它**——
  否则「store 返回 `NoSuchKey` 时 S3 层回 404」这条断言没法触发，只能干看着工具函数。
- `put_object` 存进 map 时 etag 自算（`format!("{:x}", md5::compute(&data))` 或直接
  `"deadbeef…"` 之类的固定串——**测试用的是我们自己写死的 etag**，不必真算 MD5；
  但要在测试里断言响应头 `ETag` 等于它，这样才测到「传下去了」）。
- `list_objects` 按 key 升序返回，`prefix` 照常过滤。
- `get_object` 的 `range`：**MockStore 也做裁剪**（切片成 `data[start..=end]`），
  但 `size` 返回**整个对象的长度**——这正是真实契约（见 `ObjectData.size` 的注释）。
  5.4 的 `Content-Range` 断言依赖这一点。

#### 三、5.2 ~ 5.6 的公共骨架

每个 Task 都改 `crates/s3/src/impl_s3.rs`（5.1 已建），并在文件里的 `#[cfg(test)] mod tests`
追加本组的测试。**提交粒度**：一个 Task 一个 commit，信息格式
`feat(s3): implement <operation group>`。

测试的公共工具（也在 `mock.rs` 或测试模块顶部）：

```rust
/// 打一个请求进去，返回 (状态码, 响应头, 响应体字节)。
async fn call(req: Request<Vec<u8>>) -> (StatusCode, HeaderMap, Bytes);

/// 解析错误响应体的 `<Code>`，例如 `"NoSuchKey"`。XML 里就这一个字段要断言。
fn error_code(body: &[u8]) -> String;
```

`call` 用 `service().oneshot(req)`；`ServiceExt::oneshot` 需要 `tower` 与
`http-body-util`（`BodyExt::collect`）——**两者都要加进 `crates/s3/Cargo.toml` 的
`[dev-dependencies]`**，别加进 `[dependencies]`。

---

#### Task 5.2: 桶操作

**Files:** Modify `crates/s3/src/impl_s3.rs`、Create `crates/s3/src/mock.rs`、Create `crates/s3/src/errors.rs`、Modify `crates/s3/src/lib.rs`、Modify `crates/s3/Cargo.toml`

- [ ] **Step 1: 写测试**（`MockStore` 按上面的 Step 1 建好）

```rust
#[tokio::test]
async fn create_bucket_then_head_and_list() {
    // PUT /bucket  → 200；head_bucket 走 ObjectStore::head_bucket → 200 空体
    // GET /        → 200，XML 里 <Buckets> 含 <Name>test-bucket</Name>
}

#[tokio::test]
async fn delete_non_empty_bucket_is_409() {
    // MockStore 里先塞一个对象，再 DELETE /bucket
    // → 409，error_code == "BucketNotEmpty"
}

#[tokio::test]
async fn head_missing_bucket_is_404_nosuchbucket() {
    // → 404，error_code == "NoSuchBucket"
}
```

- [ ] **Step 2: 实现四个方法**

```rust
async fn create_bucket(&self, req: S3Request<CreateBucketInput>)
    -> S3Result<S3Response<CreateBucketOutput>>
{
    self.store.create_bucket(&req.input.bucket).await?;
    // location 留 None：MVP 没有 region 概念，返回 <Location></Location> 即可。
    Ok(S3Response::new(CreateBucketOutput { bucket_arn: None, location: None }))
}
```

`delete_bucket` / `head_bucket` 同形。`head_bucket` 的输出四个字段**全部留 `None`**——
MVP 不实现 region，`aws s3 mb` 与 `mc` 都不依赖它。

`list_buckets`：

```rust
let names = self.store.list_buckets().await?;
let buckets = names.into_iter()
    .map(|name| Bucket { name: Some(name), creation_date: None, ..Default::default() })
    .collect();
Ok(S3Response::new(ListBucketsOutput {
    buckets: Some(buckets), continuation_token: None, owner: None, prefix: None,
}))
```

> `creation_date: None` 是**有意的**：桶的创建时间在设计里根本没存
> （`.rstore.sys/bucket.meta` 的内容就是 `{}`）。补一个假时间戳会骗客户端。

**`ApiError → S3 错误` 的映射在 `crates/s3/src/errors.rs` 里。**
本 Task **把这个文件建出来，并把 Task 5.1 留在 `impl_s3.rs` 里的私有
`fn to_s3_error` 搬过去**（那里标了 `TODO(Task 5.2)`）。搬过去之后只放
`NoSuchBucket` / `BucketNotEmpty` / `Internal` 三条（够 5.2 的测试跑起来）；
5.3~5.6 各自用到哪条就补哪条；**Task 5.8 收尾时把它补齐成完整一张表**，
并加上那份 `assert_code` 矩阵测试。这样安排是为了不让 5.2 的测试干等 5.8——
但 5.8 必须**核对**前面的实现确实用了这张表，而不是各自手搓了几处
`s3_error!(NoSuchBucket)` 散落在 `impl_s3.rs` 里。散落的那种写法会在
「同一个错误码两处实现、改一处漏一处」上翻车。

`errors.rs` 因此要出现在 5.2 的 Files 列表里（Create），
5.8 只是 Modify 它。`assert_code` 测试工具也先在 5.2 建。

- [ ] **Step 3: 三道门禁 + 提交**（`cargo test -p rstore-s3`、`clippy`、`check-layer-deps.sh`）

---

#### Task 5.3: 对象读写

**Files:** Modify `crates/s3/src/impl_s3.rs`

- [ ] **Step 1: 写测试**

```rust
#[tokio::test]
async fn put_then_get_round_trips_bytes() {
    // PUT /b/k  body = b"hello rustorage"  → 200，响应头 ETag 非空
    // GET /b/k  → 200，体 == b"hello rustorage"，Content-Length == 15
}

#[tokio::test]
async fn head_object_has_length_but_no_body() {
    // HEAD /b/k → 200，Content-Length == 15，且响应体**为空**
}

#[tokio::test]
async fn get_missing_key_is_404_nosuchkey() {
    // → 404，error_code == "NoSuchKey"
}

#[tokio::test]
async fn delete_object_is_204_and_idempotent() {
    // DELETE /b/k → 204；再 DELETE 一次仍 204（S3 的 DELETE 幂等）
}
```

- [ ] **Step 2: 实现**

```rust
async fn put_object(&self, req: S3Request<PutObjectInput>)
    -> S3Result<S3Response<PutObjectOutput>>
{
    let data = match req.input.body {
        Some(body) => body.try_concat().await?.to_vec(),
        // 空对象是合法的 PUT（`touch` 一个 0 字节文件）：body 为 None 就是空。
        None => Vec::new(),
    };
    let info = self.store.put_object(&req.input.bucket, &req.input.key, data).await?;
    Ok(S3Response::new(PutObjectOutput {
        e_tag: Some(ETag::Strong(info.etag)), ..Default::default()
    }))
}
```

> `..Default::default()` 在这里**是必须的**：`PutObjectOutput` 有四十多个字段，
> 逐个写 `None` 既长又会在 s3s 升级加字段时编译失败。
> 前提是这些 DTO 都 `#[derive(Default)]`——已核实，是的。

`get_object`（不带 Range 的那部分；Range 在 5.4 加）：

```rust
let out = self.store.get_object(&req.input.bucket, &req.input.key, None).await?;
Ok(S3Response::new(GetObjectOutput {
    body: Some(StreamingBlob::from_bytes(Bytes::from(out.data))),
    content_length: Some(out.size as i64),
    e_tag: Some(ETag::Strong(out.etag)),
    last_modified: Some(timestamp_of(out.mod_time)),
    accept_ranges: Some("bytes".to_string()),
    content_range: None,   // 无 Range → 序列化器不会设 206
    ..Default::default()
}))
```

`head_object` 与 `get_object` 同形，但**输出类型不同**（`HeadObjectOutput`）、
`body` 必须留 `None`，且要调 `self.store.head_object(..)` 而不是 `get_object`——
否则 HEAD 会把整份对象读进内存再丢掉。

`delete_object` → `S3Response::with_status(DeleteObjectOutput::default(), StatusCode::NO_CONTENT)`。

- [ ] **Step 3: 三道门禁 + 提交**

---

#### Task 5.4: Range

**Files:** Modify `crates/s3/src/impl_s3.rs`、Modify `crates/s3/src/errors.rs`

> `errors.rs` 在这个 Task 的列表里，是因为 `ApiError::InvalidRange` 目前**没有**映射行
> （只有 5.2 的桶错误与 5.3 的 `NoSuchKey`），会掉进 `_` 兜底变成
> `500 InternalError "unmapped api error: invalid range"`。所以要加
> `ApiError::InvalidRange => s3s::s3_error!(InvalidRange)`，并在
> `object_operation_errors_map_to_their_s3_codes` 里补一行
> `assert_code(ApiError::InvalidRange, "InvalidRange", 416)`。
> 这是 5.3 改 `errors.rs` 的同一模式：**每个 Task 补自己用到的行**，5.8 收口。

**不写 `bytes=` 解析器**——`req.input.range` 已经是 `Option<Range>`（见本节表）。
这一步只做三件事：把 `Range` 解成闭区间、查对象长度、越界时回 `416`。

- [ ] **Step 1: 写测试**（三种写法各一条）

```rust
#[tokio::test]
async fn range_int_form_returns_206_with_content_range() {
    // 对象 26 字节 "abcdefghijklmnopqrstuvwxyz"，Range: bytes=2-5  → "cdef"
    // 206；Content-Range == "bytes 2-5/26"；Content-Length == 4
}

#[tokio::test]
async fn range_open_ended_form() {
    // bytes=22-  → "wxyz"；Content-Range == "bytes 22-25/26"
}

#[tokio::test]
async fn range_suffix_form() {
    // bytes=-4   → "wxyz"；Content-Range == "bytes 22-25/26"
}

#[tokio::test]
async fn range_beyond_size_is_416() {
    // bytes=100-200  → 416，error_code == "InvalidRange"，
    // 且响应头 Content-Range == "bytes */26"（RFC 9110 §15.5.17 的 SHOULD；
    // 断言它，否则这条头会被当成可选的装饰品在重构里掉掉）
}
```

- [ ] **Step 2: 实现**

```rust
/// 把 s3s 解析好的 `Range` 收敛成真实对象的闭区间。越界 → `ApiError::InvalidRange`。
fn resolve_range(r: Range, size: u64) -> Result<ByteRange, ApiError> {
    match r {
        Range::Int { first, last } => {
            if first >= size { return Err(ApiError::InvalidRange); }
            let end = last.unwrap_or(size - 1).min(size - 1);
            if end < first { return Err(ApiError::InvalidRange); }
            Ok(ByteRange { start: first, end })
        }
        Range::Suffix { length } => {
            if length == 0 || size == 0 { return Err(ApiError::InvalidRange); }
            let start = size.saturating_sub(length);
            Ok(ByteRange { start, end: size - 1 })
        }
    }
}
```

**需要对象长度才能闭合区间**，而 `ObjectStore::get_object` 的返回值里**有** `size`——
但那要先把整份对象读回来才知道。所以顺序是：先 `head_object` 拿 `size`，再 `get_object`
带 range。两次调用是可接受的（MVP 本来就不流式），**别为此去改 trait**。

`content_range` 字符串：`format!("bytes {}-{}/{}", br.start, br.end, out.size)`——
注意分母是**整个对象长度**，不是请求范围的。写错这一处，`rclone` 会认为数据被截断。

> **`Content-Length` 必须改成切片长度，这一处最容易漏。** Task 5.3 写的
> `content_length: Some(out.size as i64)` 在**无 Range 时是对的**（那时 `out.size`
> 恰好等于 body 长度），但带 Range 时它是**整个对象**的长度。s3s 会把这个字段
> 原样写进 `Content-Length` 头，于是客户端会**对着一个 4 字节的 body 等 26 字节**——
> 表现是下载卡住/超时，而不是报错，极难定位。带 Range 的分支要写：
>
> ```rust
> content_length: Some(out.data.len() as i64),   // = br.end - br.start + 1
> ```
>
> `ObjectData` 的分工就是为此设计的：`data` 是**切片**、`size` 是**整份**——
> 一个给 `Content-Length`，另一个给 `Content-Range` 的分母。两者互换都会坏，
> 但只有前者会**挂住**客户端。

> **416 要带 `Content-Range: bytes */<size>`。** RFC 9110 §15.5.17 对 416 是一条
> SHOULD，AWS S3 也照做。`s3s::S3Error` 支持带响应头（`set_headers(HeaderMap)`），
> 而 `hyper::HeaderMap` 就是 `http::HeaderMap`——`http` 已经在 `[dependencies]` 里：
>
> ```rust
> // resolve_range 保持纯函数（返回 Result<ByteRange, ApiError>），
> // 由调用点补头——只有调用点手里有 info.size。
> //
> // 注意 req.input.range 是 Option<Range>，解出来的也是 Option<ByteRange>：
> // 无 Range 的请求必须继续走 `None`（即整份），不能硬凑成 0..size-1——
> // 那样会让 5.3 那条「无 Range → 200 而非 206」的路径悄悄变成 206。
> let resolved = match req.input.range {
>     Some(r) => Some(match resolve_range(r, info.size) {
>         Ok(br) => br,
>         Err(ApiError::InvalidRange) => {
>             let mut err = s3s::s3_error!(InvalidRange);
>             let mut headers = http::HeaderMap::new();
>             headers.insert(
>                 "content-range",
>                 format!("bytes */{}", info.size).parse().expect("ascii"),
>             );
>             err.set_headers(headers);
>             return Err(err);
>         }
>         Err(e) => return Err(to_s3_error(e)),
>     }),
>     None => None,
> };
> let out = self.store.get_object(&bucket, &key, resolved).await?;
> // 有 Range 时 Content-Length 是切片长度，无 Range 时才是整份长度。
> let content_length = match resolved {
>     Some(br) => (br.end - br.start + 1) as i64,
>     None => out.size as i64,
> };
> let content_range = resolved.map(|br| format!("bytes {}-{}/{}", br.start, br.end, out.size));
> ```
>
> `resolved` 先算出来再复用于 `content_length` 与 `content_range`，别在两处各解一遍
> ——那正是「一个分支改了、另一个没改」的来源。
>
> 补一条断言 `Content-Range == "bytes */26"` 的测试。**别把 `resolve_range` 改成
> 返回带头的错误**——那会让一个纯区间计算函数去知道 HTTP 头，也让它没法脱离
> `info.size` 单独测。

- [ ] **Step 3: 三道门禁 + 提交**

---

#### Task 5.5: ListObjectsV2

**Files:** Modify `crates/s3/src/impl_s3.rs`

`ObjectStore::list_objects` 返回**已排序的全量列表**（4.11），
`prefix` / `delimiter` / `max_keys` / `continuation_token` **全部在这一层做**。

- [ ] **Step 1: 写测试**

```rust
// 夹具：b 下有 "a.txt", "dir/x", "dir/y", "dir/sub/z", "z.txt"
#[tokio::test] async fn lists_all_sorted() { /* 5 条，按 key 升序 */ }
#[tokio::test] async fn prefix_filters() { /* prefix="dir/" → 3 条 */ }
#[tokio::test] async fn delimiter_rolls_up_common_prefixes() {
    // delimiter="/" → contents 是 ["a.txt", "z.txt"]，
    // common_prefixes 是 ["dir/"]（**只一条**，不是 dir/x、dir/y、dir/sub 三条）
}
#[tokio::test] async fn prefix_and_delimiter_together_keep_the_prefix() {
    // prefix="dir/" + delimiter="/" → contents 是 ["dir/x", "dir/y"]，
    // common_prefixes 是 ["dir/sub/"]——**前缀在 cp 里保留**。
    // 这条单独存在，是因为它盯的那个 bug 只有「前缀 + 分隔符同时用」时才出现：
    // 若 cp 从 `key[prefix.len()..]` 上直接切（而不是切完再拼回 prefix），
    // 得到的是 "sub/"，于是客户端按 "sub/" 去列举会一条都拿不到。
    // 上一组测试里 prefix 是空串，两种写法结果相同，抓不到这个错。
}
#[tokio::test] async fn max_keys_zero_returns_empty_and_truncated() {
    // max-keys=0 → contents 空、key_count == 0、**is_truncated == true**
    // （桶里还有对象）。这条盯的是「计数检查放在 push 之后」的写法：
    // 那样 0 永远等不到相等，会把全部 5 条都返回出去。
}
#[tokio::test] async fn max_keys_truncates_and_sets_is_truncated() {
    // max-keys=2 → key_count == 2，is_truncated == true，
    // next_continuation_token 非空
}
#[tokio::test] async fn continuation_token_resumes_without_gap_or_dup() {
    // 用上一步的 token 再请求 → 拿到剩下的，且两页**并集等于全集、交集为空**
}
```

> 倒数第二条是这组的核心断言。**「翻页不漏不重」**正是客户端 `rclone sync` 会依赖的性质：
> 漏一条 = 远端文件被当成不存在而**删除**。别只断言「第二页有 N 条」。

- [ ] **Step 2: 实现**

```rust
let entries = self.store.list_objects(&req.input.bucket, req.input.prefix.as_deref()).await?;
let prefix = req.input.prefix.as_deref().unwrap_or("");
let delimiter = req.input.delimiter.as_deref().filter(|d| !d.is_empty());
let max_keys = req.input.max_keys.unwrap_or(1000).max(0) as usize;
// 续传起点：**不解析 token 的内容，就当它是「上一个 key」**——见下方注记。
let start_after = req.input.continuation_token.clone().or(req.input.start_after.clone());
```

遍历 `entries`（已排序），维护 `contents: Vec<Object>` 与
`common_prefixes: BTreeSet<String>`（`BTreeSet` 顺带保证输出有序，且天然去重）：

1. `if let Some(s) = &start_after { if entry.key <= *s { continue; } }`（**字符串比较，不是下标**）
   ——**这一步必须在第 2 步之前**：被游标跳过的条目不该占 `max_keys` 的额度。
   顺序写反的表现是「第二页比第一页短」，而它只在 `start_after` 落在前缀内部时才出现。
2. **先判容量，再处理这一条**（见下面的注记）：
   `if contents.len() + common_prefixes.len() >= max_keys { truncated = true; break; }`
3. 有 delimiter 且 `key[prefix.len()..]` 里含有 delimiter →
   `let cp = key[..prefix.len() + rel_idx + 1].to_string();`
   `common_prefixes.insert(cp);`（**下标是相对 `key` 全串的**：相对切片的 `rel_idx`
   必须加回 `prefix.len()`，否则前缀被吃掉——见上面那条测试）
4. 否则 `contents.push(Object { key: Some(key), size: Some(size as i64),
   e_tag: Some(ETag::Strong(etag)), last_modified: Some(ts),
   storage_class: Some(ObjectStorageClass::from_static("STANDARD")),
   ..Default::default() })`

`key_count = contents.len() + common_prefixes.len()`（S3 的定义就是两者之和）。

> **计数检查必须在每一条之前做，不能在 push 之后。** 写成「push 完检查相等」时，
> `max_keys = 0` 永远等不到 `0 == 0`（那一刻已经 push 过一条，或者根本没进循环体），
> 于是整个桶被返回出去——而调用方明确说了「我只要 0 条」。
> 「先判容量」还有一个好处：`is_truncated` 就是「循环因容量而 `break`」，
> 不需要另外推「后面还有没有东西」。
>
> **`is_truncated` 的语义是「还有没返回完的条目」，不是「结果页是满的」。**
> 恰好整除时它必须是 `false`——写成「满了就是截断」会让客户端多翻一页空页；
> 更糟的是 `rclone sync` 之类的循环实现可能因此不收敛。

> **`next_continuation_token` 是「最后一条**返回过的**条目的 key」，不是
> 「下一条的 key」。** 配 `start_after` 的 `key <= s → skip` 语义，两者必须自洽：
> 取成「下一条」会让下一条被跳掉（漏数据），取成「第一条」则第二页原地重来（死循环）。
> 共同前缀**也能当游标**：`cp = "dir/"` 是 `"dir/x"` 的前缀，所以
> `key <= "dir/"` 恰好跳过整个 `dir/` 下的所有 key，不需要特殊处理。

> **输出的列表字段是 `Option`，一律填 `Some(...)`，哪怕是空的。**
> `contents: Option<ObjectList>`、`common_prefixes: Option<CommonPrefixList>`——
> 空桶时 `Some(vec![])` 会渲染成空的 `<Contents/>`，合法且比省略更容易预测。
> 同时把 `name`（桶名）、`prefix`、`delimiter`、`max_keys`、
> `continuation_token`（**回显输入的那个 token**）都填上——AWS 会回显它们，
> 而有些客户端会拿回显值做校验。

> **continuation token 就用裸 key，不做 base64。** S3 没有规定 token 的内容，只要
> 「传回来能接着走」即可。用 base64 只是让 token 看起来不透明，代价是多一个依赖和一个
> 编解码来回。**这是有意的简化**，写进注释；要改成不透明 token 时，唯一要保证的是
> 「解码出来的 key 仍然是全序里的位置」，而现在是同一个东西，反而不会错。
> （注意：裸 key 会把对象名暴露给客户端——但在一个**已经**要给它看对象名的 API 里，
> 这不算信息泄露。）

> 留下挂钩注释：`// PERF: 见 DESIGN §1.2 与 §20 Phase 2 — 命名空间索引`
> ——MVP 是**全盘遍历**（4.11），不是终态设计。

- [ ] **Step 3: 三道门禁 + 提交**

---

#### Task 5.6: Multipart 一律 501

**Files:** Modify `crates/s3/src/impl_s3.rs`（**只为加测试**）

**没有实现步骤——因为不需要。** s3s 的六个 multipart 方法默认实现就是
`Err(s3_error!(NotImplemented, "… is not implemented yet"))`（已核实），
**不要覆写它们**。

- [ ] **Step 1: 写测试**

```rust
#[tokio::test]
async fn all_six_multipart_ops_are_501_not_implemented() {
    // 对 POST /b/k?uploads、PUT /b/k?partNumber=1&uploadId=x、
    // POST /b/k?uploadId=x、DELETE /b/k?uploadId=x、GET /b/k?uploadId=x、
    // GET /b?uploads 各打一次，逐个断言：
    //   status == 501 且 error_code == "NotImplemented"
}
```

六个都断言，**不要只测一个就 `..` 掉**：`S3` trait 方法众多，日后有人覆写了其中一个
（比如为了别的目的实现了 `UploadPart`），只有单独断言才能发现。

- [ ] **Step 2: 三道门禁 + 提交**

> **为什么 5.6 是「返回 501」而不是「实现 multipart」**：Task 4.5 冻结的存储层
> 每个版本只写一个 `part.1`，`PutArgs { bucket, key, data }` 里没有 part 列表，
> `PartInfo` 的 `number` 恒为 1。要让 `CompleteMultipartUpload` 能把 N 个分片拼成一个对象，
> 必须先改 4.5 与 4.7 的设计（多 part 目录、part 索引、ETag 的 `-n` 格式、`ListParts` 状态）。
> DESIGN 把 multipart 划在 Phase 3，本计划跟它一致：**推迟**。
> 这是有意识的范围决策，不是遗漏——所以 5.6 必须**留一个可执行的测试**把 501 钉住，
> 免得日后有人以为 multipart「已经默默支持了」。

---

### Task 5.7: 命名校验（保留名规则 + 盘上会碰撞的 key 形状）

**Files:** Create `crates/s3/src/validate.rs`；Modify `crates/s3/src/impl_s3.rs`
（`RESERVED_PREFIX` 已由 Task 4.11 定义，本任务只引用）

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn rejects_object_key_with_reserved_first_segment() {
    // DESIGN §6.3：对象 key 的第一段不得以 `.rstore` 开头
    assert!(validate_object_key(".rstore/x").is_err());
    assert!(validate_object_key(".rstore.uploads/abc").is_err());
    assert!(validate_object_key(".rstore.sys").is_err());
    // 只有第一段受限，深层路径允许
    assert!(validate_object_key("a/.rstore/x").is_ok());
    assert!(validate_object_key("normal/key").is_ok());
    assert!(validate_object_key("").is_err());
}

#[test]
fn rejects_keys_that_would_alias_on_disk() {
    // DESIGN §15.4 把「路径中的双斜杠」列为必须覆盖的行为。这条测试钉住
    // **为什么它必须是一条拒绝规则，而不是「读的时候归一化一下就好」**：
    // `crates/disk/src/fsx.rs` 的 `resolve()` 是逐 `Component` 拼接的，
    // `Component::CurDir` 被丢弃、空段被折叠。于是
    //
    //     a//b   a/./b   a/   /a     都会落到与 a/b 或 a 相同的文件上
    //
    // 而 S3 把 `a//b` 与 `a/b` 当成**两个不同的 key**。放行的话，
    // 后写的一个会静默覆盖前一个，LIST 又只列出一个 key——两个 key 各写一次、
    // 读回来一样，中间没有任何报错。P1 是「宁可报错，绝不返回错数据」，
    // 所以这里必须 400，而不是归一化。
    //
    // 归一化在 MVP 里不成立：归一化之后 PUT `a//b` 会写进 `a/b`，
    // 那么 GET `a//b` 会读到别人写在 `a/b` 的东西——仍然是同一个静默别名，
    // 只是换了个方向。真正的修法是**在盘上编码 key**（Phase 2），
    // 那是 `fsx` 的活儿，不是这一层能补的。
    // **s3s 自带的 `normalize_forward_slash_path` 开关也不能替代这条规则**，
    // 理由见 Step 3 后面那段——它只处理空段。
    assert!(validate_object_key("a//b").is_err(), "空段会折成 a/b");
    assert!(validate_object_key("a/./b").is_err(), "`.` 段会被丢弃");
    assert!(validate_object_key("a/").is_err(), "尾随斜杠会折成 a");
    assert!(validate_object_key("/a").is_err(), "前导斜杠会折成 a");
    // `..` 在盘层是 `Fatal(FatalKind::PathEscape)`——不在这里挡，它到 S3 层
    // 就是 500（Fatal 归入 Internal），而客户端拿到的应该是 400。
    assert!(validate_object_key("a/../b").is_err(), "禁止上溯段");
    assert!(validate_object_key("..").is_err());
    // 干净的多段 key 照常通过。
    assert!(validate_object_key("a/b/c").is_ok());
    assert!(validate_object_key("a/..b").is_ok(), "`..b` 只是普通名字");
    assert!(validate_object_key("a/b.").is_ok(), "`b.` 只是普通名字");
}

#[test]
fn reserved_prefix_constant_is_not_empty() {
    // 防止有人在重构中把常量改成空串，让校验静默失效
    assert!(!rstore_common::consts::RESERVED_PREFIX.is_empty());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-s3 validate`
Expected: 编译失败

- [ ] **Step 3: 实现**

```rust
/// 对象 key 校验。返回 `ApiError::InvalidObjectName`（→ 400）。
///
/// 两条独立规则，都由安全/数据正确性而来，不是因为「顺手多校验一下」：
///
/// 1. 第一段不得以 [`RESERVED_PREFIX`] 开头 —— DESIGN §6.3，否则用户能在
///    保留命名空间里创建对象。只限第一段，深层允许出现 `.rstore`。
/// 2. **逐段**不得为空串、`.` 或 `..` —— 见 `rejects_keys_that_would_alias_on_disk`
///    的说明：这三个形状会被 `fsx::resolve` 折叠，让两个不同的 S3 key 落到同一个文件上。
pub fn validate_object_key(key: &str) -> Result<(), ApiError> {
    let mut segments = key.split('/');
    // `key` 为空时 `split` 产出单个空段，被下面的循环拒掉——不必先判空。
    if segments
        .next()
        .is_some_and(|first| first.starts_with(rstore_common::consts::RESERVED_PREFIX))
    {
        return Err(ApiError::InvalidObjectName);
    }
    // 规则 2。**注意这同时覆盖了空 key**（`"".split('/')` 得到一个空段）。
    // `..b` / `b.` 这类只是普通名字，不能误伤——所以是逐段**相等**比较，
    // 不是 `starts_with(".")`。
    if key.split('/').any(|seg| seg.is_empty() || seg == "." || seg == "..") {
        return Err(ApiError::InvalidObjectName);
    }
    Ok(())
}
```

> **为什么要拒绝，而不是归一化。** 归一化（把 `a//b` 改写成 `a/b` 后放行）是把
> 「两个 key」偷偷变成一个 key：PUT `a//b` 写进 `a/b`，之后 GET `a/b` 会读到它，
> 而 PUT `a/b` 又会覆盖它。客户端拿到的每一次响应都是 200，丢的数据却对不上任何一次
> 请求。拒绝会立刻暴露在客户端面前（400 + `InvalidObjectName`），这是可诊断的失败。
> 真正的修法是**在盘上编码 key**（MinIO 就是这么做的：键名进盘前编码），
> 那是 Phase 2 改 `fsx` 的事。
>
> **s3s 其实提供一个归一化开关，我们刻意不开。** `S3Config` 上有一个
> `normalize_forward_slash_path`（默认 `false`，可经
> `S3ServiceBuilder::set_config` 传入），它在建 key 之前调
> `crate::path::normalize_forward_slash`：`//key` → `key`、`a//b` → `a/b`。
> 两点让它**不能**替代本节的规则：
>
> 1. 那个函数只 `filter(|s| !s.is_empty())`——它**只处理空段**，`.`
>    与 `..` 原样留下，所以 `a/./b` 与 `a/../b` 仍然会落到 `fsx::resolve` 上，
>    别名与 500 两个问题一个都没解决；
> 2. 它**保留尾随斜杠**（`a/` → `a/`），而尾随斜杠恰恰也会折成 `a`。
>
> 所以本节的规则无论开关怎么设都必须存在；开着它只是额外把 `//` 也变成静默别名。
> **默认关着**，让这些 key 400 出来。若 Task 5.9 的冒烟脚本真的撞上某个客户端
> 在发 `//`，那时的正确动作是回来看这一条、确认它想表达的 key 到底是什么，
> **而不是先打开这个开关**——打开它只会把「客户端发错了什么」这个信息抹掉。

> **不要重复实现 key 长度上限**：s3s 的 `parse_path_style*` 已经调了
> `crate::path::check_key`（≤ 1024 字节，超出返回 `KeyTooLong`）。
> 本节只补它**没有**的两条规则。

**`RESERVED_PREFIX` 已经在 Task 4.11 定义好了**（在 `crates/common/src/consts.rs`，
`rstore-store` 的目录遍历要用同一个常量）。本任务**直接引用**，不要再定义一遍——
重复定义是编译错误，而更糟的做法是「5.7 另起一个常量名」，那样两个前缀就开始各自演化了。
本任务的文件清单里因此**没有** `crates/common/src/consts.rs`。

> **不要写 `validate_bucket_name`。** 原计划里有一个，连同它的四条断言
> （`.hidden` / `OK-Bucket` / `-leading` 拒绝，`ok-bucket` / `ok.bucket.123` 通过）——
> 但那套规则 **s3s 已经在做了**：`S3ServiceBuilder` 默认挂 `AwsNameValidation`，
> 它调 `s3s::path::check_bucket_name`，检查 3–63 字符、`a-z0-9.-`、首尾必须是字母数字、
> 不含 `..`、不是 IP 字面量、不以 `xn--` 开头。
> 自己再写一份的结果是**两个校验器会分叉**：`192.168.1.1` 这类名字 s3s 会拒、手写的那份会放行，
> 而「到底哪个在生效」取决于这行代码今天挂在哪——这种不一致比少一个校验难查得多。
> 桶名交给 s3s；本任务只补它**没有**的钩子（保留前缀，`NameValidation` trait 只有
> `validate_bucket_name` 一个方法，没有对象 key 的钩子，所以只能在 `impl_s3.rs` 入口做）。

> **常量必须落在 `rstore-common`，不能落在 `rstore-meta`。** 原计划写的是
> `crates/meta/src/keys.rs`，而校验代码在 `crates/s3` 里——但护栏 allowlist
> （`scripts/check_layer_deps.py`）给 `rstore-s3` 的允许集只有
> `{rstore-common, rstore-api}`，`rstore-meta` 是被禁的边。常量放 meta 的话，
> 这行校验一写出来 `scripts/check-layer-deps.sh` 就退出 1。
> `rstore-common` 是唯一同时被 meta 与 s3 看见的 crate，往那里放两边都能用。
> （`crates/meta/src/keys.rs` 里那几个 `RUSTORAGE_KEY_PREFIX` / `INLINE_DATA` 是**线格式**
> 的 header 名，只有 meta 层用，留在原地。）

- [ ] **Step 4: 在请求入口接入**

在 `impl_s3.rs` 的 **`put_object` / `get_object` / `head_object` / `delete_object`**
四个入口**都**调 `validate_object_key`，**建桶不调**（桶名交给 s3s 的
`AwsNameValidation`，见 Step 3 的说明）。

> 原计划说「只在**写入**入口强制，读路径先校验还是查到 404 都可以」。规则 1
> （保留前缀）时这话成立——两种答案客户端都接受。**但规则 2 加进来之后就不成立了**：
> `..` 在盘层是 `Fatal(FatalKind::PathEscape)`，读路径不拦的话它一路变成
> `ApiError::Internal` → **500**。对 `GET /b/../x` 回 500 是在说「服务端坏了」，
> 而事实是这个 key 不合法（400）。两条规则在同一次调用里检查，别拆开——
> 拆开就会出现「写路径按一套规则、读路径按另一套」的分叉。

> **LIST 的 `prefix` 不加这条校验。** DESIGN §15.4 那行写的是「某些客户端/**list**
> 操作会发出 `//`」，而 `ListObjectsV2` 的前缀走的是**查询参数**
> （`?list-type=2&prefix=…`），不是路径段。前缀是过滤器，不落盘，因此没有别名风险；
> 对它做校验只会让「想看 `a//b` 下有什么」这类合法列举变成 400。
> 本节的规则**只作用于对象 key 的四个入口**。

补两个 HTTP 层测试（用 5.1 那套 `tower::ServiceExt::oneshot`，别绑端口）：

- 对 `.rstore.sys/x` 发 PUT，期望 `400` + `InvalidObjectName`（规则 1）；
- 对 `a//b` 发 PUT，同样期望 `400` + `InvalidObjectName`（规则 2）。

第二条不能省：规则 1 的测试对规则 2 完全无感，而规则 2 才是那条会导致**数据静默
互相覆盖**的规则。

- [ ] **Step 5: 提交**

Run: `cargo test -p rstore-s3 validate`
Expected: PASS

```bash
# 注意**没有** `crates/common/src/consts.rs`：`RESERVED_PREFIX` 是 Task 4.11
# 定义的，本任务只引用它。也**不是** `crates/meta/src/keys.rs`——那是原计划
# 修掉之前的位置，本任务一行都不用动它。
git add crates/s3/src/validate.rs crates/s3/src/impl_s3.rs
git commit -m "feat(s3): object key validation with reserved prefix rule

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5.8: 错误映射

**Files:** Modify `crates/s3/src/errors.rs`（**已在 5.2 建好，这里补齐成完整表**）、Modify `crates/s3/src/impl_s3.rs`（把散落的错误构造收敛到一处）

- [ ] **Step 1: 写测试**

```rust
#[test]
fn maps_api_errors_to_s3_codes() {
    // S3 层只看得见 `ApiError`（见 Task 5.1 的分层说明），**不是** `StoreError`——
    // 两者之间没有依赖边。原计划这张表写的 `StoreError::{NoSuchBucket, InvalidPart,
    // DiskFull, SlowDown}` 四个变体**一个都不存在**，照抄编译不过。
    //
    // 断言里带上 HTTP 状态码：只断言 `<Code>` 字符串的话，「NoSuchKey 配了 500」
    // 这种错会漏过去，而 S3 客户端是按状态码分支的。
    assert_code(ApiError::NoSuchKey, "NoSuchKey", 404);
    assert_code(ApiError::NoSuchBucket, "NoSuchBucket", 404);
    assert_code(ApiError::BucketNotEmpty, "BucketNotEmpty", 409);
    assert_code(ApiError::InvalidBucketName, "InvalidBucketName", 400);
    assert_code(ApiError::InvalidObjectName, "InvalidObjectName", 400);
    assert_code(ApiError::InvalidRange, "InvalidRange", 416);
    assert_code(ApiError::NotImplemented, "NotImplemented", 501);
    assert_code(ApiError::Unavailable, "InternalError", 503);
    assert_code(ApiError::Internal("boom".into()), "InternalError", 500);
}
```

- [ ] **Step 2-4: 实现、跑测试、提交**

要求每个 S3 错误响应包含 `Code` / `Message` / `Resource` / `RequestId` 四要素
（DESIGN §15.4）。`assert_code` 的第三个参数就是这个变体对应的 HTTP 状态码。

**`StoreError → ApiError` 的映射不在这个文件里**，它属于组合根
（`rstore-server/src/wiring.rs` 的 `EngineAdapter`），因为只有那里同时看得见两边。
映射表（写在这里，实现时照抄）：

| `StoreError` | `ApiError` | 说明 |
|---|---|---|
| `NotFound` | `NoSuchKey` | Task 4.7 加了变体；`head_bucket` / `delete_bucket` 要映成 `NoSuchBucket`，见下 |
| `BucketNotEmpty` | `BucketNotEmpty` | 409；Task 4.11 加这个变体。这是 `DeleteBucket` 唯一的非 404 失败 |
| `ReadQuorum { .. }` / `WriteQuorum { .. }` | `Unavailable` | 503 + `Retry-After`；这是**暂时**不可用，不是 500 |
| `ShardLayout(_)` | `Internal` | 布局坏了是本实现自己的 bug，必须显式暴露 |
| `Internal(_)` | `Internal` | |
| `Disk(DiskError::NotFound)` | `NoSuchKey` | 单盘缺失通常已被上层吸收成 slot=None，走到这里说明是整体缺失 |
| `Disk(_)` 其余 | `Internal` | |
| `_`（`#[non_exhaustive]` 的兜底） | `Internal` | `StoreError` 是 `#[non_exhaustive]`，必须有兜底分支 |

**`NotFound` 那一行要在适配器里按方法分岔**：`StoreError::NotFound` 本身分不清
「对象不在」与「桶不在」，而 `EngineAdapter` 知道自己在实现哪个方法——
`head_bucket` / `delete_bucket` 把它映成 `NoSuchBucket`（404），
`get_object` / `head_object` / `delete_object` 映成 `NoSuchKey`（404）。
这正是 `ApiError` 把这两个码分开（而不是合并成一个 `NotFound`）的原因；
把分岔放在适配器里而不是在 `StoreError` 上再加一个变体，是因为**只有适配器同时知道
「是哪次调用」**，`StoreError` 加变体反而要把这个信息从调用点一路传下来。

**不引入 `DiskFull` / `SlowDown`**：`StoreError`、`DiskError` 里都没有能区分出它们的变体，
凭空加两个 S3 错误码只会得到「永远返回不到」的死分支。真需要时先在
`rstore_common::error::FatalKind` 里加变体，再回来补这一行——那时它才是有依据的。

```bash
git add crates/s3/
git commit -m "feat(s3): domain error to S3 error code mapping

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5.9: 兼容层与客户端冒烟测试

**Files:**
- Create: `crates/s3-compat/src/lib.rs`（**可以先是空的**，见 Step 3 的注记）
- Create: `tests/compat/aws_cli.sh`、`tests/compat/mc.sh`、`tests/compat/rclone.sh`

> **载荷必须 < 8 MiB。** aws-cli 的 `s3 cp` 对超过 8 MiB 的文件会自动改走
> multipart 上传，而 MVP 对 multipart 返回 501（见 Task 5.6）。脚本里的
> `head -c 1048576` 是刻意的；**不要**为了「测得更充分」把它调大，
> 那会让冒烟脚本以「测到 multipart 的 501」的形式失败，而那个失败不是 compat 层能修的。

> **本任务的实际执行顺序在 Task 6.3 之后。** 三个脚本都打 `http://127.0.0.1:9000`，
> 需要一个**已经跑起来的服务**——而「启动编排 + `--volumes/--port` 命令行」是
> Task 6.3 才做的（`crates/server/src/main.rs` 由它创建）。也就是说这是本计划里
> **唯一一处 M5 依赖 M6 的地方**：先把 6.1~6.3 做完，再回来跑这里的脚本。
> 别在 5.9 里临时写一个一次性 main 来绕过——那就是在 M6 之前先把 M6 做一半。
> 启动命令见 Task 6.4 的验收脚本（`cargo run -p rstore-server -- --volumes … --port 9000`）。

> **凭据要与服务端的默认值一致。** 脚本里硬编码的
> `rustorage` / `rustorage-secret` 必须就是 Task 6.3 默认配置里那一对
> （服务端走 `SimpleAuth::from_single`，见 5.1）。两边不一致的表现是三个脚本
> 齐刷刷 403 `SignatureDoesNotMatch`，而错误信息不会告诉你这是配置对不上。
> 把这对值记在 Task 6.3 的默认配置里，并在这里引用同一个来源。

> **前置检查写进脚本开头**：`command -v aws >/dev/null || { echo "aws CLI 未安装" >&2; exit 1; }`
> （`mc.sh` / `rclone.sh` 同理）。没有这一段时，缺一个客户端会以
> 「command not found」的形式失败，看起来像是服务端的问题。

- [ ] **Step 1: 先跑冒烟脚本，找出真实的不兼容点**

脚本框架（`aws_cli.sh`）：

```bash
#!/usr/bin/env bash
set -euo pipefail
command -v aws >/dev/null || { echo "aws CLI 未安装" >&2; exit 1; }

export AWS_ACCESS_KEY_ID=rustorage
export AWS_SECRET_ACCESS_KEY=rustorage-secret
export AWS_DEFAULT_REGION=us-east-1
EP="http://127.0.0.1:9000"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# `mb` 在桶已存在时会非零退出；我们的 CreateBucket 是幂等的（返回 200），
# 但客户端自己也可能先行报错——吞掉退出码，别让 `set -e` 在这里把脚本带走。
aws --endpoint-url "$EP" s3 mb s3://test-bucket || true

head -c 1048576 /dev/urandom > "$WORK/1m.bin"
aws --endpoint-url "$EP" s3 cp "$WORK/1m.bin" s3://test-bucket/1m.bin
aws --endpoint-url "$EP" s3 cp s3://test-bucket/1m.bin "$WORK/roundtrip.bin"
cmp "$WORK/1m.bin" "$WORK/roundtrip.bin"

aws --endpoint-url "$EP" s3api list-objects-v2 --bucket test-bucket --prefix "" --max-keys 1
aws --endpoint-url "$EP" s3api list-objects-v2 --bucket test-bucket --delimiter "/"
echo "aws-cli smoke: OK"
```

`mc.sh`：

```bash
#!/usr/bin/env bash
set -euo pipefail
command -v mc >/dev/null || { echo "mc 未安装" >&2; exit 1; }
EP="http://127.0.0.1:9000"
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT

mc alias set rs "$EP" rustorage rustorage-secret
mc mb --ignore-existing rs/test-bucket
head -c 1048576 /dev/urandom > "$WORK/1m.bin"
mc cp "$WORK/1m.bin" rs/test-bucket/1m.bin
mc cat rs/test-bucket/1m.bin > "$WORK/roundtrip.bin"
cmp "$WORK/1m.bin" "$WORK/roundtrip.bin"
mc ls rs/test-bucket
mc rm rs/test-bucket/1m.bin
echo "mc smoke: OK"
```

`rclone.sh`：

```bash
#!/usr/bin/env bash
set -euo pipefail
command -v rclone >/dev/null || { echo "rclone 未安装" >&2; exit 1; }
EP="http://127.0.0.1:9000"
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT

# 用环境变量定义 remote，不写 ~/.config/rclone/rclone.conf（那会污染开发机）。
# `provider=Minio` 是为了让 rclone 用 **path-style** 寻址并挑一套
# 对自建端点更宽松的签名细节；`provider=Other` 也能用，但对 AWS 专有行为更敏感。
export RCLONE_CONFIG_RS_TYPE=s3
export RCLONE_CONFIG_RS_PROVIDER=Minio
export RCLONE_CONFIG_RS_ENDPOINT="$EP"
export RCLONE_CONFIG_RS_ACCESS_KEY_ID=rustorage
export RCLONE_CONFIG_RS_SECRET_ACCESS_KEY=rustorage-secret
export RCLONE_CONFIG_RS_FORCE_PATH_STYLE=true

rclone mkdir rs/test-bucket
mkdir -p "$WORK/src"
head -c 1048576 /dev/urandom > "$WORK/src/1m.bin"
rclone copy "$WORK/src" rs/test-bucket/
rclone copy rs/test-bucket/1m.bin "$WORK/roundtrip.bin"
cmp "$WORK/src/1m.bin" "$WORK/roundtrip.bin"
# `check` 会比对大小与 **ETag**——它正是 #3 那个「HEAD 的 ETag 必须与 LIST 的一致」
# 的验收点。ETag 两处算法分叉时，这条会失败而 `cmp` 不会。
rclone check "$WORK/src" rs/test-bucket --one-way
echo "rclone smoke: OK"
```

> **这三个脚本的客户端行为差异本身就是被测对象**，不是可以互相抄的模板：
> `mc cp` 默认 64 MiB 以上才走 multipart、`rclone` 的 `--s3-upload-cutoff` 默认 200 MiB、
> `aws s3 cp` 是 **8 MiB**——只有 aws-cli 那条在 1 MiB 上就已经安全。
> 1 MiB 对三者都在单次 PUT 范围内，这是**刻意选的下限**。
> 若日后有人把载荷调大，先回来核对这三个阈值。

- [ ] **Step 2: 运行脚本，记录失败项**

Run: `bash tests/compat/aws_cli.sh`
Expected: 首次运行**允许失败**——失败项就是 compat 层的需求来源

- [ ] **Step 3: 为每个失败项添加 compat 中间件**

**必须遵守 DESIGN §15.3 的准入规则**：每条中间件带注释

```rust
// compat: aws-cli — PUT 空对象时不发 Content-Length，需归一化为 0 — see tests/compat/aws_cli.sh
```

并且该中间件被移除时，对应的冒烟测试必须失败。**不允许凭猜测添加中间件。**

> **`crates/s3-compat` 的接线缺口，动手前先看这条。** 护栏 allowlist 给它的是
> `{rstore-common}`，而 `rstore-s3` 的允许集是 `{rstore-common, rstore-api}`——
> 也就是说 **`rstore-s3` 看不见 `rstore-s3-compat`，没有任何东西会调用这里的中间件**。
> 这在本任务里是**可以接受的**，因为正确的做法本来就是「先跑脚本、观察到真实失败、
> 再决定要不要加中间件」。所以：
>
> - 如果三个脚本**一次就全过**（很可能是这个结果，因为 5.2~5.8 已经按客户端真实行为写了），
>   那 `crates/s3-compat/src/lib.rs` 就保持空的、只留一句模块文档，
>   **不要**为了「让这个 crate 有点内容」去写没人调用的中间件。这是 YAGNI。
> - 如果确实观察到失败、需要加中间件，**首选在组合根接**：
>   `rstore-server` 的 allowlist 里**同时**有 `rstore-s3` 与 `rstore-s3-compat`，
>   所以它可以在 `crates/server/src/lib.rs` 里把中间件当作 tower 层套在
>   `S3Service` 外面。**这条路不需要改 allowlist**，也符合「绑定与装配只在组合根发生」
>   （DESIGN §5 R4）——本来是首选，只是因为改动位置不在 `rstore-s3` 里，容易被忽略。
> - 只有当中间件必须在 `impl_s3.rs` **内部**才能生效（例如要读 `S3Request` 里
>   s3s 私有的解析中间态）时，才退而改 allowlist：给 `rstore-s3` 加上
>   `rstore-s3-compat`，并同步 `scripts/tests/test_check_layer_deps.py` 里对表结构的断言。
>   `rstore-s3-compat` 只依赖 `rstore-common`，这条边不会引入环。

- [ ] **Step 4: 三个脚本全部通过后提交**

```bash
git add crates/s3-compat/ tests/compat/
git commit -m "feat(s3-compat): client ecosystem compatibility layers driven by smoke tests

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5.10: 条件请求（GET / HEAD 子集）

**Files:**
- Create `crates/s3/src/conditional.rs`
- Modify `crates/s3/src/lib.rs`（加 `pub(crate) mod conditional;`）
- Modify `crates/s3/src/impl_s3.rs`（`get_object` / `head_object` 各插一段）
- Modify `crates/s3/src/mock.rs`（`fake_etag` 与 `MOCK_MOD_TIME_NANOS` 改成 `pub(crate)`）
- Modify `crates/s3/Cargo.toml`（**顺便清掉三个依赖**，见 Step 1 末尾）

> **为什么有这一节。** DESIGN §15.4 的「必须覆盖的行为」一共 7 条，MVP 只覆盖了 4 条。
> 这一节补第 5 条：`If-Match` / `If-None-Match` / `If-Modified-Since` /
> `If-Unmodified-Since`。剩下两条是 Task 5.11（虚拟主机寻址）与 Task 5.7 的
> Step 1 第二条测试（双斜杠）。
>
> **DESIGN 只点名了前三个，我们做四个。** RFC 9110 §13.2.2 的求值顺序是定义在
> 四个头之上的，其中 `If-Unmodified-Since` 是第 2 步、且被明确写成
> 「**仅在 `If-Match` 缺席时**求值」。少实现一个的后果不是「少一个功能」，
> 而是让那条「缺席时才看」的规则**没有对应的代码，因而没有对应的测试**——
> 日后有人重排顺序时，没有任何东西会失败。多写 6 行的代价换一条能钉住顺序的断言。
>
> **范围刻意只到 GET / HEAD。** PUT 的 `If-None-Match: *`（条件创建）会引入
> 「检查与写入之间的竞态」——`ObjectStore` 上没有原子的 conditional-put，
> 在 S3 层先查再写是 TOCTOU。DESIGN 没要求 PUT，**不做**——并已记入本文件末尾的
> 「**MVP 的已知限制**」表（那一行同时说明日后要补时得先给 `ObjectStore` 加什么）。
> 同理不做 `If-Range`（那要和 5.4 的 Range 联动，收益远小于复杂度）。
>
> **s3s 不会替我们做这件事。** 它把这四个头**解析**成了 `GetObjectInput` /
> `HeadObjectInput` 上的字段（已核实四个字段都在），但没有任何默认实现去**求值**。
> 不写这一节，它们就是四个被静静忽略的字段——客户端拿到 200 和全量内容，
> 而它明确要求了「只在没变的时候给我 304」。

- [ ] **Step 1: 写失败测试**

先改 `crates/s3/src/mock.rs` 两处可见性（一行一处）：

```rust
// 5.10 的断言要引用这两个**确切值**，而不是在测试里再抄一遍魔数：
// 抄一遍的结果是「以后改了 mock 的时间戳，条件请求的测试悄悄测的是别的东西」。
pub(crate) const MOCK_MOD_TIME_NANOS: u64 = 1_700_000_000_000_000_000;
pub(crate) fn fake_etag(data: &[u8]) -> String { /* 原样，只改可见性 */ }
```

> **顺手清依赖（本任务做，别再往后拖）。** `crates/s3/Cargo.toml` 的
> `[dependencies]` 里现在有三个**一处都没用到**的项，是 Task 5.1 的遗留：
>
> - `serde_json` —— 5.1 的 Files 里已经写明「不要加」（错误 XML 由 s3s 自己生成）；
> - `thiserror` —— `crates/s3/src/` 下 `grep -rn thiserror` 无命中；
> - `tokio` —— 只被 `#[tokio::test]` 用到（`grep -rn tokio crates/s3/src/` 的命中
>   全在 `mod tests` 里），**应该挪到 `[dev-dependencies]`**。
>
> 留着它们不会让门禁变红（`cargo clippy` 不检查未用依赖），所以会一直漂下去；
> 而 `cargo machete` / 审阅者看到「依赖了 tokio 的库」会以为这是个异步 runtime 库。
> 一行 `git mv` 级别的改动，趁这一节一起做。

`crates/s3/src/conditional.rs` 的单元测试——**这一节的主要覆盖面在这里**，
因为条件求值是纯函数，不需要经过 HTTP：

```rust
#[cfg(test)]
mod tests {
    // **逐项列出，不写 `use super::*`**：本模块顶部有
    // `use std::time::{Duration, SystemTime, UNIX_EPOCH};` 与
    // `use s3s::dto::{ETag, ETagCondition, Timestamp};`，glob 会不会把父模块的
    // 私有 `use` 也带进来是容易记混的一条规则。测试要让读的人不必推这一层。
    use rstore_api::ObjectInfo;
    use s3s::dto::{ETag, ETagCondition, Timestamp};

    use super::{evaluate, Conditions, Verdict};

    /// 与 `mock.rs` 的 `MOCK_MOD_TIME_NANOS` 对齐：1_700_000_000s = 2023-11-14T22:13:20Z
    fn info(etag: &str, mod_time: u64) -> ObjectInfo {
        ObjectInfo { size: 10, etag: etag.to_string(), mod_time }
    }
    const T: u64 = crate::mock::MOCK_MOD_TIME_NANOS;
    const T_ETAG: &str = "00ff00ff00ff00ff";

    fn cond<'a>(
        if_match: Option<&'a ETagCondition>,
        if_none_match: Option<&'a ETagCondition>,
        if_modified_since: Option<&'a Timestamp>,
        if_unmodified_since: Option<&'a Timestamp>,
    ) -> Conditions<'a> { Conditions { if_match, if_none_match, if_modified_since, if_unmodified_since } }

    fn etag(s: &str) -> ETagCondition { ETagCondition::ETag(ETag::Strong(s.to_string())) }

    /// 一个 HTTP-date 会用到的时刻。参数是 **Unix 纳秒**，与 mock 的 `mod_time` 同一单位，
    /// 便于写 `ts(T - 1_000_000_000)`（= 早一秒）。
    fn ts(nanos: u64) -> Timestamp {
        Timestamp::from(std::time::UNIX_EPOCH + std::time::Duration::from_nanos(nanos))
    }

    #[test]
    fn no_conditions_proceeds() {
        assert!(matches!(evaluate(cond(None, None, None, None), &info(T_ETAG, T)), Verdict::Proceed));
    }

    #[test]
    fn if_match_uses_strong_comparison() {
        let i = info(T_ETAG, T);
        // 值相同 → 通过
        assert!(matches!(evaluate(cond(Some(&etag(T_ETAG)), None, None, None), &i), Verdict::Proceed));
        // 值不同 → 412
        assert!(matches!(
            evaluate(cond(Some(&etag("deadbeef")), None, None, None), &i),
            Verdict::PreconditionFailed
        ));
        // 弱 ETag 参与 If-Match **永远不匹配**（RFC 9110 §8.8.3 的强比较）
        let weak = ETagCondition::ETag(ETag::Weak(T_ETAG.to_string()));
        assert!(matches!(
            evaluate(cond(Some(&weak), None, None, None), &i),
            Verdict::PreconditionFailed
        ));
        // `*` 表示「只要存在」，走到这里对象一定存在
        assert!(matches!(
            evaluate(cond(Some(&ETagCondition::Any), None, None, None), &i),
            Verdict::Proceed
        ));
    }

    #[test]
    fn if_none_match_matches_on_value_ignoring_weakness() {
        let i = info(T_ETAG, T);
        // 值相同 → 304
        assert!(matches!(
            evaluate(cond(None, Some(&etag(T_ETAG)), None, None), &i),
            Verdict::NotModified
        ));
        // 弱 ETag 在 If-None-Match 里**照样匹配**（弱比较）
        let weak = ETagCondition::ETag(ETag::Weak(T_ETAG.to_string()));
        assert!(matches!(
            evaluate(cond(None, Some(&weak), None, None), &i),
            Verdict::NotModified
        ));
        // 值不同 → 继续
        assert!(matches!(
            evaluate(cond(None, Some(&etag("deadbeef")), None, None), &i),
            Verdict::Proceed
        ));
    }

    #[test]
    fn if_none_match_any_means_only_if_absent() {
        // `*` 在 If-None-Match 里是「只要不存在」——对象存在，所以恒 304。
        // 这是 `aws s3 sync` 之类客户端「跳过已存在对象」的写法。
        assert!(matches!(
            evaluate(cond(None, Some(&ETagCondition::Any), None, None), &info(T_ETAG, T)),
            Verdict::NotModified
        ));
    }

    #[test]
    fn modified_since_compares_at_second_granularity() {
        let i = info(T_ETAG, T);
        let t = ts(T);
        // 与 Last-Modified **同一秒** → 不可再早 → 304（`<=`，不是 `<`）。
        // 这里用 `<` 的话，「取回之后立刻再带 If-Modified-Since 重发」会永远拿 200，
        // 缓存的语义就废了。
        assert!(matches!(
            evaluate(cond(None, None, Some(&t), None), &i),
            Verdict::NotModified
        ));
        // 客户端手里的副本更旧（早一秒）→ 已经变了 → 200
        assert!(matches!(
            evaluate(cond(None, None, Some(&ts(T - 1_000_000_000)), None), &i),
            Verdict::Proceed
        ));
        // 亚秒差不能翻转结论：对象在 T 之后 0.9 秒修改，HTTP-date 只能表达整秒，
        // 所以它仍算「同一秒」→ 304。**这条钉住的是「比较前先截断到秒」**，
        // 不截断的话这里会返回 200，而客户端会把没变的对象当成变了。
        assert!(matches!(
            evaluate(cond(None, None, Some(&t), None), &info(T_ETAG, T + 900_000_000)),
            Verdict::NotModified
        ));
    }

    #[test]
    fn unmodified_since_is_412_when_modified_later() {
        let i = info(T_ETAG, T);
        // 对象在该时刻之后被改过 → 412（这是「乐观锁」：我读到的是旧的，别覆盖）
        assert!(matches!(
            evaluate(cond(None, None, None, Some(&ts(T - 1_000_000_000))), &i),
            Verdict::PreconditionFailed
        ));
        // 没改过 → 通过
        assert!(matches!(
            evaluate(cond(None, None, None, Some(&ts(T))), &i),
            Verdict::Proceed
        ));
    }

    #[test]
    fn if_match_takes_precedence_over_unmodified_since() {
        // RFC 9110 §13.2.2 第 2 步：**只有 If-Match 缺席时**才看 If-Unmodified-Since。
        // 两者都发且互相矛盾时，应报告更早那一步的失败（412），而不是让
        // If-Unmodified-Since 把 If-Match 已经判过的结论再翻一遍。
        let i = info(T_ETAG, T);
        let old = ts(T - 1_000_000_000);
        let ok_tag = etag(T_ETAG);
        assert!(
            matches!(
                evaluate(cond(Some(&ok_tag), None, None, Some(&old)), &i),
                Verdict::Proceed
            ),
            "If-Match 命中时应短路，不再看 If-Unmodified-Since"
        );
    }

    #[test]
    fn if_none_match_takes_precedence_over_modified_since() {
        // 第 4 步同理：**只有 If-None-Match 缺席时**才看 If-Modified-Since。
        // 这条测试的形状是「两个都发、结论不同、断言取前者」——
        // 顺序写反的实现在这里会返回 `Proceed`（200）而不是 `NotModified`（304）。
        //
        // **`If-Modified-Since` 必须取一个比 `mod_time` 更早的时刻**，这样它单独求值
        // 才给 `Proceed`。取「未来」的时刻是这条测试最容易踩的坑：那时刻也满足
        // `mod_time <= it`，于是两种顺序**都**返回 304，断言恒真、什么都没测到。
        let i = info(T_ETAG, T);
        let earlier = ts(T - 1_000_000_000);
        assert!(
            matches!(
                evaluate(cond(None, Some(&etag(T_ETAG)), Some(&earlier), None), &i),
                Verdict::NotModified
            ),
            "If-None-Match 命中时应短路，不再看 If-Modified-Since"
        );
    }
}
```

`impl_s3.rs` 的 HTTP 层测试——**只补两条**，证明「四个头真的被 s3s 解析进了
`input`，而且真的接到了 `evaluate` 上」，不在这里重复上面的矩阵：

```rust
#[tokio::test]
async fn get_with_if_none_match_hit_is_304_and_has_no_body() {
    // PUT /b/k → 记下响应头里的 ETag
    // 再 GET /b/k 带 `If-None-Match: <那个 ETag>` → **304**，且响应体为空
    // （额外断言响应头里仍有 ETag —— RFC 9110 §15.4.5 要求 304 带验证器，
    //  少了它客户端的缓存条目会失效）
}

#[tokio::test]
async fn get_with_if_match_mismatch_is_412() {
    // GET /b/k 带 `If-Match: "deadbeef"` → **412**，error_code == "PreconditionFailed"
}

#[tokio::test]
async fn conditional_header_does_not_turn_404_into_412() {
    // GET /b/missing 带 `If-Match: *` → **404 NoSuchKey**，**不是** 412。
    // 这一条钉住「先取对象，取不到就直接 404」。**先求条件再取对象**的实现
    // 在这里会给出 412，而 `If-Match: *` 的意思是「只要存在就给我」——
    // 「不存在」这件事本身就该走 404，客户端正是靠 404 来区分「没有」和「被别人改过」。
}
```

给 `impl_s3.rs` 的测试模块加一个能带自定义头的请求构造器（现有的 `request()`
只设 `host`）：

```rust
/// 与 `request` 同形，但接受额外请求头——条件请求的测试需要它们。
fn request_with(
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
) -> http::Request<TestBody> {
    let mut b = http::Request::builder().method(method).uri(path).header("host", HOST);
    for (k, v) in headers {
        b = b.header(*k, *v);
    }
    b.body(Full::new(Bytes::new())).expect("build request")
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-s3 conditional`
Expected: 编译失败（`conditional` 模块不存在）

- [ ] **Step 3: 实现**

`crates/s3/src/conditional.rs` ——**整个模块就这些**：

```rust
//! HTTP 条件请求（DESIGN §15.4）。**只做 GET / HEAD 的子集**。
//!
//! 求值顺序照 RFC 9110 §13.2.2，不是随便排的：先 `If-Match`（不满足 → 412），
//! 再 `If-Unmodified-Since`（**仅在 `If-Match` 缺席时**），再 `If-None-Match`
//! （命中 → GET/HEAD 是 304），最后 `If-Modified-Since`（**仅在 `If-None-Match`
//! 缺席时**）。两处「缺席时才看」是这一节最容易写漏的地方，漏掉的表现是
//! 「两个头都发的客户端偶尔拿到 200 而它期望 304」。
//!
//! **本模块不处理「对象不存在」**：调用方先 `head_object`，拿到 `NoSuchKey`
//! 就直接 404，根本不进这里。`If-Match: *` 的语义是「只要存在」，而「不存在」
//! 由 404 表达——把它变成 412 会让客户端分不清「没有」与「被别人改过」。

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rstore_api::ObjectInfo;
use s3s::dto::{ETag, ETagCondition, Timestamp};

/// 求值结果。三个阶段与 RFC 的三种响应一一对应。
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Verdict {
    /// 条件全部满足（或缺席），按正常流程返回 200 / 206。
    Proceed,
    /// 412 PreconditionFailed。
    PreconditionFailed,
    /// 304 NotModified，**无 body**。
    NotModified,
}

/// 四个条件头的求值输入。拿走引用即可——`evaluate` 只读。
pub(crate) struct Conditions<'a> {
    pub if_match: Option<&'a ETagCondition>,
    pub if_none_match: Option<&'a ETagCondition>,
    pub if_modified_since: Option<&'a Timestamp>,
    pub if_unmodified_since: Option<&'a Timestamp>,
}

pub(crate) fn evaluate(c: Conditions<'_>, info: &ObjectInfo) -> Verdict {
    // 我们存的 etag 是**裸**十六进制串（`fake_etag` / 引擎的 md5 都不带引号），
    // `ETag::Strong` 里装的也是裸值——`ETag::parse_http_header` 已经把
    // `"x"` 和 `W/"x"` 的引号与 `W/` 剥掉了。所以直接比，**不要在这里手工剥引号**。
    let current = ETag::Strong(info.etag.clone());

    // 第 1 步：If-Match，**强比较**。
    if let Some(cond) = c.if_match {
        let ok = match cond {
            // 走到这里对象一定存在（不存在在调用点就 404 了），所以 `*` 恒真。
            ETagCondition::Any => true,
            ETagCondition::ETag(e) => e.strong_cmp(&current),
        };
        if !ok {
            return Verdict::PreconditionFailed;
        }
    }

    // 第 2 步：If-Unmodified-Since，**只有 If-Match 缺席时**才求值。
    if c.if_match.is_none() {
        if let Some(limit) = c.if_unmodified_since {
            if truncated(info.mod_time) > *limit {
                return Verdict::PreconditionFailed;
            }
        }
    }

    // 第 3 步：If-None-Match，**弱比较**（值相等即命中，不管强弱标记）。
    if let Some(cond) = c.if_none_match {
        let matched = match cond {
            // `If-None-Match: *` = 「只要不存在」→ 对象存在 ⇒ 命中 ⇒ 304。
            ETagCondition::Any => true,
            ETagCondition::ETag(e) => e.weak_cmp(&current),
        };
        if matched {
            return Verdict::NotModified;
        }
    }

    // 第 4 步：If-Modified-Since，**只有 If-None-Match 缺席时**才求值。
    if c.if_none_match.is_none() {
        if let Some(since) = c.if_modified_since {
            // `<=`：与 Last-Modified 同一秒也算「没变」。用 `<` 的话，
            // 「取回后立刻带 If-Modified-Since 重发」永远拿 200，缓存语义就废了。
            if truncated(info.mod_time) <= *since {
                return Verdict::NotModified;
            }
        }
    }

    Verdict::Proceed
}

/// 把 Unix 纳秒截断到**秒**。
///
/// HTTP-date 只精确到秒，所以亚秒差**不能**参与比较：对象在 `Last-Modified`
/// 之后 0.9 秒被改过时，客户端手里的 HTTP-date 与新的 Last-Modified 是同一天同一秒，
/// 它无从表达这个差异。不截断的实现会把 `If-Modified-Since` 判成「变了」，
/// 于是同一个客户端每隔一秒重试都拿 200——而这些其实没变。
///
/// 用 `Timestamp::from(SystemTime)` 构造而不是依赖 `time::OffsetDateTime`：
/// `s3s` 没有 re-export `time`，为一次比较往依赖里加一个 crate 不值得。
fn truncated(mod_time_nanos: u64) -> Timestamp {
    Timestamp::from(UNIX_EPOCH + Duration::from_secs(mod_time_nanos / 1_000_000_000))
}

/// 对象的 `Last-Modified`，**秒粒度**。304 响应要带上它（RFC 9110 §15.4.5）。
pub(crate) fn last_modified(info: &ObjectInfo) -> Timestamp {
    truncated(info.mod_time)
}
```

`impl_s3.rs` 里 `head_object` / `get_object` 的接入。**顺序是关键**
（这是 Task 5.4 已经定好的「先 head 拿 size 再 get」那条流，条件请求正好
复用同一个 `head_object` 调用，不额外多一次往返）：

```rust
// ---- get_object ----
// 文件顶部加：use crate::conditional::{self, Verdict};
let input = req.input;
// 1. 先取元数据（5.4 为了 resolve_range 本来就要这一步）。
let info = self.store.head_object(&input.bucket, &input.key).await?;  // Err → 404
// 2. 条件请求。放在 range 之前：304 不该去解析 Range，
//    412 更不该——那两份工作都是白做的。
match conditional::evaluate(conditional::Conditions {
    if_match: input.if_match.as_ref(),
    if_none_match: input.if_none_match.as_ref(),
    if_modified_since: input.if_modified_since.as_ref(),
    if_unmodified_since: input.if_unmodified_since.as_ref(),
}, &info) {
    Verdict::PreconditionFailed => return Err(s3s::s3_error!(PreconditionFailed)),
    Verdict::NotModified => {
        // 304 **不带 body**。`content_length` 也留 None：s3s 会把
        // `content_length` 无条件写进响应头，而带 Content-Length 却没有 body
        // 容易被客户端当成截断。ETag 与 Last-Modified 必须带
        // （RFC 9110 §15.4.5：304 要携带能更新缓存条目的验证器）。
        return Ok(S3Response::with_status(
            GetObjectOutput {
                e_tag: Some(ETag::Strong(info.etag.clone())),
                last_modified: Some(conditional::last_modified(&info)),
                ..Default::default()
            },
            StatusCode::NOT_MODIFIED,
        ));
    }
    Verdict::Proceed => {}
}
// 3. 以下照 5.4 的流程走：resolve_range(&input.range, info.size)，再 get_object。

// ---- head_object ----
// 同形，只是没有 Range 那一步；304 分支构造 `HeadObjectOutput
// { e_tag, last_modified, ..Default::default() }` 后同样 `with_status(NOT_MODIFIED)`。
```

> **不要试图把条件求值塞进 `ObjectStore::get_object`。** 那要给它加一个
> 「条件」参数，于是 `rstore-api` 就得知道 `ETagCondition`（s3s 的类型），
> 而 `rstore-api` 的依赖集只有 `{rstore-common}`——这是分层被破坏的第一道口子。
> 条件请求是**协议层**的语义（RFC 9110），不是存储语义，它属于 `rstore-s3`。

- [ ] **Step 4: 三道门禁 + 提交**

Run: `cargo test -p rstore-s3` 与 `bash scripts/check-layer-deps.sh`
Expected: 全绿；护栏退出 0（本节没有引入任何跨 crate 依赖）

```bash
git add crates/s3/
git commit -m "feat(s3): conditional requests for GET and HEAD

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5.11: 虚拟主机风格寻址

**Files:**
- Modify `crates/s3/src/lib.rs`（加 `build_service`，把 5.1 只在测试里写过的装配提取成公开函数）
- Modify `crates/s3/src/impl_s3.rs`（测试：多加一个 `set_host` 的用例）

> **为什么有这一节。** DESIGN §15.4 的第 6 条：客户端会用
> `Host: bucket.example.com` 而不是 `Host: example.com` + `Path: /bucket/key` 来寻址。
> 不做的话，`aws --endpoint-url https://s3.example.com` 这类配置下一个桶都访问不到
> （它会拿 `bucket.example.com` 当桶名发出去，而我们按 path-style 解析，
> 桶名成了 `bucket.example.com`，于是 404）。
>
> **默认关闭。** `--base-domain` 不给时**保持现在的纯 path-style 行为**——
> 这是向后兼容的默认值，也是 `tests/compat/*.sh` 与 `tests/acceptance.sh` 用的模式
> （它们全部走 path-style：`mc` 配 `FORCE_PATH_STYLE=true`、rclone 配
> `RCLONE_CONFIG_RS_FORCE_PATH_STYLE=true`）。所以**打开这个开关不能改变
> 现有四个脚本的任何行为**，这也是为什么它必须由参数门控而不是无条件开启。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn virtual_host_style_host_header_selects_bucket() {
    // 用 `build_service(store, KEY, SECRET, Some("example.com"))` 建 service。
    // 请求 `Host: test-bucket.example.com`、路径 `/obj`：
    //   PUT  → 200，且 store 里落在 **"test-bucket"** 桶下（不是 "test-bucket.example.com"）
    //   GET  → 200，内容与 PUT 的一致
    // **不需要 DNS、不需要端口**：s3s 只看 `Host` 头，`oneshot` 直接打进去。
}

#[tokio::test]
async fn path_style_still_works_when_base_domain_is_set() {
    // 同一个 service（base_domain = Some("example.com")），
    // 但请求 `Host: example.com` + 路径 `/test-bucket/obj` → 仍然 200。
    // `parse_host_header` 对 `host_part == base_part` 返回**不带 bucket** 的
    // `VirtualHost`，于是回落到 path-style；这条钉住「开了虚拟主机没有把
    // path-style 关掉」。
}

#[tokio::test]
async fn host_outside_base_domain_falls_back_to_cname_style() {
    // **这条记录的是一个反直觉的行为，不是我们想要的功能。**
    // base_domain = Some("example.com") 时，`Host: 127.0.0.1:9000` 既不等于
    // base、也不是它的子域 → `SingleDomain` 的 CNAME 回退把**整个 host**
    // 当成桶名（`127.0.0.1`）→ 404 NoSuchBucket。
    // 断言的就是这个 404 + `NoSuchBucket`——把它钉成「已知行为」而不是意外。
    // 它正是 Task 6.3 里 `--base-domain` 默认值必须留空的原因（见那里的说明）。
    // 若哪天 CNAME 回退被关掉，这条会失败，那时应当**同时**回来核对
    // `--base-domain` 的默认值论证是否还成立。
}

#[tokio::test]
async fn no_base_domain_means_path_style_only() {
    // `build_service(.., None)` 的 service 收到 `Host: test-bucket.example.com`
    // **完全不看 Host 头**——s3s 侧是 `if let (Some(host_header), Some(s3_host))`，
    // `s3_host` 为 `None` 时整段跳过，Host 头被丢弃（`ops/mod.rs` 的
    // `parse_request_host`）。
    // 断言 `GET /`（path-style 的 ListBuckets）→ 200 **且正文含
    // `ListAllMyBucketsResult`**：只断言 200 区分不出「当成了 ListBuckets」
    // 与「当成了名为 test-bucket 的桶上的一次列举」——后者会是 404。
    // 这条是「默认行为没变」的回归测试（见本节的「默认关闭」说明）。
}

#[test]
fn invalid_base_domain_is_rejected_at_construction() {
    // `build_service(.., Some("not a domain"))` → **Err**，且错误信息里含该字符串。
    // 启动期就失败，不要拖到第一个请求：那时错误会变成一个 400/500，
    // 而运维看到的是「服务起来了但客户端全挂」，没有任何线索指向 --base-domain。
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-s3 host`
Expected: 编译失败（`build_service` 不存在）

- [ ] **Step 3: 实现**

`crates/s3/src/lib.rs`——把 5.1 只写在测试里的装配提取成一个公开函数。
**`build_service` 是本 crate 唯一的装配入口**，6.3 的启动流程调它：

```rust
use std::sync::Arc;

use rstore_api::ObjectStore;
use s3s::auth::SimpleAuth;
use s3s::host::SingleDomain;
use s3s::service::{S3Service, S3ServiceBuilder};

pub use impl_s3::RstoreFs;

/// 装配 S3 service。
///
/// `base_domain` 为 `None` 时**只支持 path-style**（s3s 的默认 host 解析）；
/// 为 `Some(d)` 时开启虚拟主机风格：`Host: <bucket>.<d>` 会被解析成
/// `bucket = <bucket>`。
///
/// **返回 `Result` 是因为 `d` 可能不是合法域名**，那属于启动期配置错误——
/// 应该在进程启动时明确报错退出，而不是等到第一个请求变成一个费解的 400。
/// 所以这里用一个简单的 `String` 承载配置错误，不复用 `ApiError`
/// （它是**请求期**的错误类型；用它会让「启动失败」和「请求失败」在同一处混起来）。
pub fn build_service(
    store: Arc<dyn ObjectStore>,
    access_key: &str,
    secret_key: &str,
    base_domain: Option<&str>,
) -> Result<S3Service, String> {
    let mut builder = S3ServiceBuilder::new(RstoreFs { store });
    builder.set_auth(SimpleAuth::from_single(access_key, secret_key));
    if let Some(domain) = base_domain {
        // `SingleDomain` 默认带 CNAME 回退（域外的 host 被当成桶名）。
        // **保留默认**：关掉它（`with_cname_fallback(false)`）会让「用别的域名
        // 指进来」的部署方式失效，而我们没有理由禁止它。
        let host = SingleDomain::new(domain)
            .map_err(|e| format!("invalid --base-domain {domain:?}: {e}"))?;
        builder.set_host(host);
    }
    // 不设 host 时走 s3s 的默认（纯 path-style）——**不要**显式设 PathStyle，
    // 那会把 s3s 换默认实现时的新行为挡在外面，而我们的默认行为就是它的默认行为。
    Ok(builder.build())
}

#[cfg(test)]
mod mock;

pub(crate) mod conditional;
pub(crate) mod errors;
pub mod impl_s3;
```

> **`RstoreFs { store }` 的字段是 `pub`**（5.1 就是这么定的），所以测试里
> 直接用它构造也行；但生产路径必须走 `build_service`，否则 `set_host`
> 这一行在 6.3 里会被漏掉——而漏掉的表现是「虚拟主机模式静默不生效」，
> 没有任何报错。

- [ ] **Step 4: 三道门禁 + 提交**

```bash
git add crates/s3/
git commit -m "feat(s3): optional virtual-host style addressing via base domain

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

> **6.3 的启动流程要跟着改**：`--base-domain` 参数（见 Task 6.3 的启动契约表）
> 传进 `build_service(.., config.base_domain.as_deref())`，
> 返回的 `Err` 直接让进程以非零码退出并打印那条信息。
> 这一处**在 6.3 落地之前，虚拟主机模式是「实现好了但没人打开」**——
> 那没关系，本节的四个测试覆盖的是能力本身。

---

## M6 — 运维面与验收

> **本里程碑里 6.1 / 6.2 / 6.3 的 `#[tokio::test]` 函数体原本是空的**（里面只有一行
> 描述要测什么的注释）——那不是测试，是待办列表。**已经补上了每条要断言什么**，
> 实现时要先让它们红起来（TDD），再写实现。之所以补的是「断言什么」而不是完整代码：
> 这三节的 API 面由 M5 的 `rstore-server` 骨架决定，此刻写死的签名很可能是错的。
> 每一条至少要断言一件**可观察**的事：
>
> - 6.1：`/ready` 的状态码与 `Retry-After` 头；`/health` 在 `Booting` 阶段也是 200；
>   `mark_stage` 回退时**不改变** stage（要断言返回值或重新读一次，不能只调用了事）。
> - 6.2：开关关闭时计数**不变**（读两次，比对）；`/metrics` 的响应体里必须
>   **真的出现**那几个指标名（断言字符串包含，不是断言 `is_ok()`）。
> - 6.3：`format.json` 不一致时**返回 `Err`**，且错误信息里**包含出问题的盘路径**
>   （断言 `contains`，因为「启动失败」这件事本身不指明是哪块盘就没法运维）；
>   已有数据的盘 + 空白盘必须拒绝；关闭时在飞请求跑完才退出。
>
> 判断标准就一句：**把这行断言删掉，测试是不是照样绿？** 是的话它就等于没写。

### Task 6.1: Readiness 与健康端点

**Files:** Create `crates/server/src/readiness.rs`

> **这三个端点与 S3 API 共用同一个端口。** 6.4 的验收脚本轮询的是
> `$ENDPOINT/ready`，而 `$ENDPOINT` 就是 `http://127.0.0.1:9000`——S3 的端口。
> 原计划没说这一条，实现者很可能另起一个管理端口（比如 9001），
> 于是验收脚本会一直轮询到超时。**不要另起端口**：MVP 只需在路由表里
> 在给 s3s 之前先匹配 `/health`、`/ready`、`/metrics` 三个路径。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn returns_503_before_storage_ready() {
    // 进程已监听但尚未完成存储初始化 → /ready 返回 503 且带 Retry-After: 5
}

#[tokio::test]
async fn returns_200_after_storage_ready() {
    // `mark_stage(SystemStage::StorageReady)` 之后再请求 /ready → **200**
    // 且**没有** Retry-After 头（在 Booting 阶段它是 503 + Retry-After: 5，
    // 那条断言由上一个测试负责；这里断言的是「状态翻转了」，两次都读一次状态码）
}

#[tokio::test]
async fn stage_is_monotonic() {
    // mark_stage 不允许回退：先到 FullReady，再 mark_stage(StorageReady)
    // → 返回值表明被拒，且**重新读 stage 仍是 FullReady**。
    // 只调用了事等于没测（原计划这里连注释都没有）
}

#[tokio::test]
async fn health_is_independent_of_readiness() {
    // /health 在 Booting 阶段也返回 200（存活探针不依赖存储）
}
```

- [ ] **Step 2-4: 实现、跑测试、提交**

```rust
pub enum SystemStage { Booting = 0, StorageReady = 1, FullReady = 2 }
```

`/health` 为存活探针（进程活着即 200）；`/ready` 受 stage 控制，未就绪返回
`503` + `Retry-After: 5`。

```bash
git add crates/server/src/readiness.rs
git commit -m "feat(server): staged readiness with health/ready endpoints

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 6.2: Prometheus 指标

**Files:** Create `crates/server/src/metrics.rs`

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn metrics_disabled_is_noop() {
    // 开关关闭时 record_* 不改变任何计数。
    // **读两次再比对**：只写「record_* 之后没 panic」是不碰计数的空断言。
    // 做法：关掉开关，读一次计数快照 → 调几个 record_* → 再读一次 → 两次相等。
}

#[tokio::test]
async fn exposes_prometheus_text_format() {
    // GET /metrics → 响应体里**真的包含**这几个指标名（用 contains 断言字符串，
    // 不是断言 `is_ok()`）：put_duration_seconds、get_duration_seconds、
    // erasure_quorum_failures_total、bitrot_mismatch_total、disk_errors_total。
    // 藏在正文里的那种「返回了 200 但正文是空表」的 bug，只有 contains 能抓到。
}
```

> `/metrics` 与 `/health` `/ready` 一样**挂在 S3 那个端口上**（见 Task 6.1 的说明）。

- [ ] **Step 2-4: 实现、跑测试、提交**

按 DESIGN §18.2，指标名常量集中定义，热路径用 `LazyLock` 缓存 handle。
至少暴露：`put_duration_seconds{stage}`、`get_duration_seconds{stage}`、
`erasure_quorum_failures_total{op}`、`bitrot_mismatch_total`、`disk_errors_total{kind}`。

> **去掉原计划里的 `buffer_pool_acquire_total{class}`。** DESIGN §13.3 的四级缓冲池
> 在 MVP 里**没有任何任务实现它**（M1~M5 全篇没有缓冲池），照抄这个指标名只会得到一个
> 永远为 0、没人能解释的序列——运维看到「acquire 一直是 0」第一反应是「是不是坏了」，
> 而不是「哦这个功能还没做」。等 Phase 2 真把缓冲池做出来时再一起加指标。
> 这也正好是 Task 5.8 里删掉 `DiskFull` / `SlowDown` 的同一条理由：
> **不为不存在的代码注册指标/错误码。**

```bash
git add crates/server/src/metrics.rs
git commit -m "feat(server): prometheus metrics endpoint

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 6.3: 启动与关闭编排

**Files:** Create `crates/server/src/startup.rs`、`crates/server/src/config.rs`、`crates/server/src/main.rs`

> 原计划把文件名写成 `config_load.rs`，但这里**不读配置文件**——MVP 的配置全部来自命令行
> 参数（见下面的「启动契约」）。名字跟着职责走，叫 `config.rs`。

#### 启动契约（原计划完全没有这一段）

**6.4 的验收脚本与 5.9 的三个冒烟脚本，全部依赖下面这些参数名与默认值。**
任一处在实现时改了名，验收脚本就会以「unrecognized option」失败——而那是 M6 的最后一步。

| 参数 | 必需 | 默认值 | 说明 |
|---|---|---|---|
| `--volumes <path>...` | 是 | 无 | **接受一个或多个路径**（`nargs(1..)`）。6.4 的脚本展开成 6 个独立参数传入 |
| `--port <u16>` | 否 | `9000` | 只监听 `127.0.0.1`（MVP 不做 TLS，也不该对外） |
| `--parity <u8>` | 否 | `default_parity(volumes.len())`（4.3） | 6 块盘时默认是 **3**（3+3），而验收脚本要 4+2 → 必须显式传 `--parity 2` |
| `--access-key <str>` | 否 | **`rustorage`** | 加进 `SimpleAuth::from_single`（5.1） |
| `--secret-key <str>` | 否 | **`rustorage-secret`** | 同上 |
| `--base-domain <str>` | 否 | **不给（= 纯 path-style）** | 传给 `build_service(.., base_domain)`（Task 5.11）开启虚拟主机寻址 |
| `--metrics` | 否 | 关闭 | 打开 `/metrics`（6.2） |

> **`--base-domain` 的默认值是「不给」，这条必须保持。** `tests/compat/*.sh` 与
> `tests/acceptance.sh` 全部走 path-style（`mc` 与 `rclone` 都显式配了
> `FORCE_PATH_STYLE=true`）。给它一个非空默认值之后，客户端发来的
> `Host: 127.0.0.1:9000` 既不等于是 base domain、也不是它的子域，于是走
> `SingleDomain` 的 **CNAME 回退**——**桶名变成 `127.0.0.1`**，四个脚本里
> 每一个请求都 404 `NoSuchBucket`，而错误信息里不会出现「base-domain」这个词。
> （注意机制不是「域名校验失败」：`strip_port_suffix` 会把端口剥掉，
> `is_valid_domain` 也接受 `127.0.0.1:9000`。所以别想着靠
> `with_cname_fallback(false)` 去救一个错误的默认值——那只是把另一个行为改掉。）
>
> `build_service` 返回的 `Err`（域名不合法）**必须让进程以非零码退出并打印那条信息**，
> 不要 `unwrap()`：Task 5.11 的 `invalid_base_domain_is_rejected_at_construction`
> 就是为了让这个错误在启动期可见。

> **那两个默认凭据是 5.9 三个冒烟脚本硬编码的同一对值。** 两边不一致的表现是三个脚本
> 齐刷刷 `403 SignatureDoesNotMatch`，而错误信息不会告诉你是配置对不上。
> 改这里的默认值 = 改 `tests/compat/*.sh`，两处必须同时动。

```bash
# 6.4 的脚本实际会这么调（注意 --volumes 展开成 6 个参数、--parity 显式给 2）：
cargo run -p rstore-server -- \
    --volumes /tmp/rs/d1 /tmp/rs/d2 /tmp/rs/d3 /tmp/rs/d4 /tmp/rs/d5 /tmp/rs/d6 \
    --parity 2 --port 9000
```

**不要为 MVP 引入配置文件、环境变量覆盖、TOML/YAML 解析。** 六个参数够用，
而多一层配置来源就多一处「到底哪个在生效」的排查成本。

#### 启动顺序

解析参数 → 打开各盘（`LocalDisk::open`）→ 逐盘读 `format.json` →
`rstore_meta::format::select_authoritative` → 构造 `ErasureSet` →
**`rstore_s3::build_service(store, key, secret, base_domain)`**（Task 5.11，
组合根在这里才是唯一同时看得见 `rstore-s3` 与 `rstore-store` 的地方）→
`mark_stage(StorageReady)` → 起 HTTP 服务 → `mark_stage(FullReady)`。
所有长生命周期任务绑定 `CancellationToken`。

> `build_service` 返回 `Err` 时直接打印并 `exit(1)`，别 `unwrap()`——
> 那条 `Err` 是「`--base-domain` 不是合法域名」，是**运维输入错误**，
> 需要那句信息才修得了。`unwrap()` 只会打印一个 panic backtrace，
> 而 backtrace 里不会出现那个参数名。

**格式校验必须复用 `crates/meta/src/format.rs` 里已有的两个函数，不要重写一份**：

- `select_authoritative(&[FormatV1]) -> Result<FormatV1, FormatError>` —— 已经实现了
  「按 `shared_identity()` 分组计票、不一致时报错」。注意它**拿到的是已经读出来的
  `FormatV1` 列表**：读盘失败（`NotFound`）的盘**不进这个列表**，走下面那条路径。
- `should_initialize(&[DiskError]) -> bool` —— 已经实现了「**仅当所有盘都返回
  `NotFound`**（即一块盘都读不到 `format.json`）时才允许初始化」这条闸门。
  它正是 `refuses_to_reformat_reachable_disks` 要测的东西：一盘有数据、一盘空白时，
  错误列表里不全都是 `NotFound`，于是返回 `false`，启动必须**拒绝**。

这两个函数在 Task 2.x 就写好了并有测试，6.3 只是调用者。

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn refuses_to_start_on_inconsistent_formats() {
    // 两盘 format.json 的 shared_identity 不一致 → 启动返回 Err，
    // **且错误信息里包含出问题的那块盘的路径**（断言 contains，
    // 因为「启动失败」这件事本身不指明是哪块盘就没法运维）
}

#[tokio::test]
async fn refuses_to_reformat_reachable_disks() {
    // 一盘写好 format.json、一盘是空目录 → 启动返回 Err，
    // 且**不得**把那个空目录初始化成新盘（断言空目录里仍然没有 format.json）
}

#[tokio::test]
async fn shutdown_cleanly_stops_accepting_then_drains() {
    // 可观察的两件事，缺一不可：
    // (a) 关闭发起后，**新**连接被拒（不是 503，是不再 accept）；
    // (b) 关闭发起**之前**已经接住的在飞请求，跑完并返回 200。
    // 做法：起一个会 sleep 200ms 的 handler，先发一个请求、不 await，
    // 再调 shutdown()，最后断言那个请求拿到 200，且随后新请求失败。
    // **只断言「shutdown() 返回 Ok」等于没测**——那不碰请求生命周期。
}
```

- [ ] **Step 2-4: 实现、跑测试、提交**

```bash
git add crates/server/src/startup.rs crates/server/src/config.rs crates/server/src/main.rs
git commit -m "feat(server): startup/shutdown orchestration with format validation

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 6.4: 端到端验收

**Files:** Create `tests/acceptance.sh`

- [ ] **Step 1: 写验收脚本**

```bash
#!/usr/bin/env bash
set -euo pipefail
ENDPOINT=http://127.0.0.1:9000
ROOT=/tmp/rs
WORK=$(mktemp -d)          # 载荷与比对结果放服务端数据目录**之外**

# 本脚本自己直接调 `aws`（第 2 步起），所以**必须**在这里给凭据：
# `tests/compat/*.sh` 里的 export 是子进程，出了那个脚本就没了。
# 少了这三行，在有 ~/.aws/credentials 的开发机上能跑、在干净的 CI 上必然
# 「Unable to locate credentials」，而这跟服务端毫无关系。
# 这三个值必须与 Task 6.3 的默认参数（--access-key / --secret-key）一致。
export AWS_ACCESS_KEY_ID=rustorage
export AWS_SECRET_ACCESS_KEY=rustorage-secret
export AWS_DEFAULT_REGION=us-east-1
AWS="aws --endpoint-url $ENDPOINT"

# 启动 6 盘 4+2 实例（4 数据分片 + 2 校验分片 = 6 块盘）。
# **`--parity 2` 必须显式给**：6 块盘的默认 parity 是 3（见 Task 4.3 的 default_parity），
# 那样 read_quorum = 6-3 = 3，下面「掉 2 块」还剩 4 块，离边界很远，测不到那条边界。
# 给 2 之后 read_quorum = 6-2 = 4，掉 2 块恰好**只剩 4 块**——一步不多、一步不少，
# 这是纠删码最有价值的那个用例；而下面引用的「4+2」注释也才对得上。
mkdir -p $ROOT/{d1,d2,d3,d4,d5,d6}
cargo run -p rstore-server -- --volumes $ROOT/d{1,2,3,4,5,6} --parity 2 --port 9000 &
SERVER_PID=$!
# 无论从哪一条 `set -e` 退出，都别把服务留在后台占着 9000：
trap 'kill $SERVER_PID 2>/dev/null || true' EXIT

# 不要 `sleep 3`：慢机器上会假失败，快机器上白等。轮询 /ready 直到 200。
# (`curl … && break` 里 curl 不是 `&&` 列表中的最后一条，所以失败不会触发 errexit。)
for _ in $(seq 1 60); do
    curl -fsS -o /dev/null $ENDPOINT/ready && break
    sleep 0.5
done
curl -fsS -o /dev/null $ENDPOINT/ready || {
    echo "server did not become ready" >&2; exit 1
}

# 1. 客户端冒烟（三个脚本各自的 endpoint / 凭据 / 建桶约定见 tests/compat/）
bash tests/compat/aws_cli.sh
bash tests/compat/mc.sh
bash tests/compat/rclone.sh

# 2. 放一份**可逐字节比对**的载荷，作为容错验收的基准。
#    必须 < 8 MiB：MVP 不支持 multipart，aws-cli 超阈值会自动改走分片上传。
head -c 3000000 /dev/urandom > $WORK/payload.bin
#    建桶：已存在时 `mb` 会失败，所以吞掉它的退出码，别让 `set -e` 在这里把脚本带走。
$AWS s3 mb s3://accept 2>/dev/null || true
$AWS s3 cp $WORK/payload.bin s3://accept/big.bin

# 3. 容错：停掉两块盘 —— **用 `mv` 把盘目录挪走，不要用 `chmod 000`**。
#    本项目的开发与验收环境是 Windows（Git Bash），`chmod 000` 在那里是空操作：
#    脚本会一路绿灯，却一块盘都没停掉，于是这条容错验收等于没测。
#    `mv` 在两个平台都真的让路径消失，`LocalDisk` 会得到 NotFound/IO 错误，
#    正是「盘掉线」要模拟的东西。
mv $ROOT/d5 $ROOT/d5.off
mv $ROOT/d6 $ROOT/d6.off

#    4+2 掉 2 块，read_quorum = 4，读**必须**成功且**内容逐字节相同**。
#    原计划这一步只有一行「→ 读仍成功」的注释、没有任何命令——那等于什么都没测：
#    分片读错、解码错位、返回截断的数据，这条注释全都发现不了。
$AWS s3 cp s3://accept/big.bin $WORK/degraded.bin
cmp $WORK/payload.bin $WORK/degraded.bin

# 4. 恢复，再读一次确认恢复没把数据改坏（同一条比对，但走的是另一条盘路径）。
mv $ROOT/d5.off $ROOT/d5
mv $ROOT/d6.off $ROOT/d6
$AWS s3 cp s3://accept/big.bin $WORK/healed.bin
cmp $WORK/payload.bin $WORK/healed.bin

echo "ACCEPTANCE: OK"
```

> **前置检查也要写**：`command -v aws >/dev/null || { echo "aws CLI 未安装" >&2; exit 1; }`
> 放在 `set -euo pipefail` 之后。没有这一段时，缺 aws 会以「command not found」失败，
> 看起来像是服务端的问题。

> **两条比对（第 3、4 步）是这份脚本里唯一真正有诊断价值的部分**，别把它们退化成
> `curl -f` 或者 `aws … >/dev/null`。`cmp` 失败会带出首个不同字节的偏移，
> 而「读成功但内容不对」恰恰是纠删码实现最典型的坏法（P1：宁可报错，绝不返回错数据）。
> 载荷用 `/dev/urandom` 而不是全零：全零的字节里，分片错位与补零 bug 都看不出来。

- [ ] **Step 2: 运行，直到全部通过**

Run: `bash tests/acceptance.sh`
Expected: 输出 `ACCEPTANCE: OK`

- [ ] **Step 3: 提交**

```bash
git add tests/acceptance.sh
git commit -m "test: end-to-end acceptance script

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## 完成检查清单

MVP 交付时必须全部为真：

- [ ] `cargo build --workspace` 无 warning
- [ ] `cargo test --workspace` 全绿
- [ ] `cargo clippy --workspace --all-targets --locked -- -D warnings` 通过
- [ ] `bash scripts/check-layer-deps.sh` 退出码 0
- [ ] `python scripts/tests/test_check_layer_deps.py` 全绿
      （用 `python` 不用 `python3`：本机 Windows 上 `python3` 可能是 Store/MSIX 别名，
      `python3 --version` 正常但 `python3 -c 'print(1)'` 会 Permission denied(126)。
      `scripts/check-layer-deps.sh` 已按「跑得出 1」探测，这里是同一件事。）
- [ ] CI 在 `main` 与 PR 上跑通（护栏、lint、test 三步都不是跳过状态）
- [ ] `bash tests/acceptance.sh` 输出 `ACCEPTANCE: OK`
- [ ] 4+2 配置下：掉 2 盘可读、掉 2 盘可写、掉 3 盘读返回 `ReadQuorum` 而非错误数据
- [ ] `FaultyDisk` 注入静默字节损坏时，读路径能检出 `BitrotMismatch`
- [ ] 崩溃点测试覆盖 DESIGN §12.3 的全部窗口
- [ ] `rstore-s3-compat` 中每个中间件都有对应的冒烟测试，且注释指明来源客户端
- [ ] DESIGN §1.2 的非目标清单中，没有任何一项被意外实现（范围不蔓延）
- [ ] 六个 multipart 操作各自返回 `501 NotImplemented`（MVP 明确推迟，见 Task 5.6）
- [ ] 桶操作可用：建桶幂等、非空桶删返回 409、`ListBuckets` 不把系统目录当桶
      （Task 4.11）
- [ ] `ListObjectsV2` 不列出删除标记、不列出未提交的 `.staging-*` 目录，
      且掉盘低于 quorum 时报错而不是返回残缺列表（Task 4.11 / 5.5）

---

## MVP 的已知限制

**这里是「刻意不做」的清单，不是待办列表。** 每一条都是范围决策，都指明了它由哪个
Task 负责、以及日后要补时该动哪里。最后那次整体复审拿这份清单逐条核对：里面的每一条
都应该**在代码里有对应的拒绝/报错路径**，而不是「看起来忘了做」。

| 限制 | 表现 | 所属 Task | 日后要补时 |
|---|---|---|---|
| **不支持 multipart** | 六个 multipart 操作一律 `501 NotImplemented`。aws-cli 的 `s3 cp` 对 > 8 MiB 的文件会自动改走 multipart，因此真实用户传大文件会拿到 501 | 5.6 | 先改 4.5/4.7 的存储层（多 part 目录、part 索引、ETag 的 `-n` 格式），再实现六个操作 |
| **载荷上限约 8 MiB** | 同上一条的推论：交付给客户端的大对象只能靠 < 8 MiB 的单次 PUT | 5.6 / 5.9 | 同 5.6 |
| **条件请求只覆盖 GET / HEAD** | `PUT` 带 `If-None-Match: *`（条件创建）**不求值**，会被当成普通 PUT。`If-Range` 也不支持 | 5.10 | 需要先给 `ObjectStore` 加原子的 conditional-put——在 S3 层「先查再写」是 TOCTOU，不能这么补 |
| **含空段 / `.` / `..` 的对象 key 被拒（400）** | 与 AWS 的行为**不同**：AWS 把 `a//b` 与 `a/b` 当两个 key，我们直接 400 `InvalidObjectName` | 5.7 | 在盘上编码 key（改 `fsx`），而不是打开 s3s 的 `normalize_forward_slash_path`——理由见 Task 5.7 |
| **虚拟主机寻址默认关闭** | 默认纯 path-style；要按 `Host: bucket.example.com` 寻址必须显式传 `--base-domain` | 5.11 | 无。这是刻意的门控，见 Task 6.3 的启动契约表 |
| **LIST 是全盘遍历** | 大数据集上很慢；没有索引、没有分页下推（分页只在 S3 层做） | 4.11 / 5.5 | Phase 2 的索引；接口已留挂钩位 |
| **无并发锁** | DESIGN §16.1 的按 `(bucket, key)` 分片 `RwLock` **在 M1~M5 全篇没有任何任务实现它**（`crates/store/src/` 下 `grep -rn "RwLock\|Mutex"` 只命中 `testutil.rs` 的一句注释）。PUT/GET 并发目前由文件系统语义兜底：`.staging-*` + rename 提交保证了「看不到半成品」，但**不保证同一 key 上两个并发 PUT 的先后** | — | Phase 2。连同「heal 与写共用同一把锁」那条约束一起推迟——那条约束在 heal 存在之前没有意义 |
| **无背压** | DESIGN §16.3 的信号量 + 有界降级通道 + `{Primary, Degraded, Unbounded, Rejected}` 准入**同样在计划里没有任何任务**（`背压` / `Semaphore` 在 M1~M5 零命中）。表现是突发大并发下内存与磁盘队列无界增长 | — | Phase 2。**这一条是计划对 DESIGN 的静默遗漏**，不是本节新增的范围决策——之所以写在这里，是为了让最终复审看得见它 |
| **单节点** | 无多节点、无 heal 的调度者。DESIGN §17 说 heal 由「观测到 `Corrupt`」触发：目前 `Corrupt` 会被分类、记录、参与 quorum 判定，但**没有自动修复流程** | — | Phase 3 |

> **加新限制时，必须同时确认代码里有对应的拒绝路径。** 比如「不支持 multipart」
> 之所以能写进这张表，是因为 Task 5.6 有一个测试断言六个操作各自返回 501；
> 而如果某条限制只是「文档里说了但代码里没拦」，那就是漏实现，不是限制。

---

## 风险与注意事项

| 风险 | 应对 |
|---|---|
| `reed-solomon-simd` 的 API 与预期不符 | Task 1.3 已把库调用完全封在门面内；若签名不同，只改 `encode`/`decode` 内部，测试不变 |
| 各平台 `read_exact_at` 差异（Windows 无 `FileExt::read_exact_at`） | `fsx.rs` 用 seek + read 的通用实现；Linux/macOS 再加 `#[cfg]` 优化路径 |
| 单节点下 rename 的持久性依赖文件系统 | `sync_file_and_parent` 必须先 fsync 文件再 fsync 父目录，Task 3.2 有专门测试 |
| LIST 全盘扫描在大数据集上很慢 | MVP 明确接受，接口留挂钩位；不要在这一阶段引入索引（YAGNI） |
| 崩溃点测试难以稳定复现 | 用 `FaultyDisk::FailAfter` 精确控制，而非依赖真实 kill；不确定的路径不要写进测试 |
| **默认凭据三处不一致**（Task 6.3 的 `--access-key`/`--secret-key` 默认值、`tests/compat/*.sh`、`tests/acceptance.sh`） | 三处写的是同一个 `rustorage` / `rustorage-secret`，但**没有任何东西能强制它们一致**——编译器看不见 shell 脚本。不一致的表现是三个脚本齐刷刷 403 `SignatureDoesNotMatch`。改动任一处时三处同改；6.4 的验收脚本是全链路唯一会同时用到它们的地方 |
| **`--parity` 默认值与验收脚本的期望不一致** | `default_parity(6) = 3`（有测试钉住），而 6.4 要的是 4+2。所以 6.4 的脚本**必须显式**传 `--parity 2`；漏掉的话「掉 2 块」离边界还很远，那条最关键的容错验收会退化成一次普通读 |
| **Task 5.9 的冒烟脚本依赖 Task 6.3 的启动编排** | 本计划里唯一一处 M5 依赖 M6。5.9 的 Step 2 只有在 6.3 落地后才能跑；这不是可以「先欠着」的排序，别在 5.9 里临时写一次性 main 绕过 |
| **`--base-domain` 一旦有非空默认值，四个 shell 脚本全红** | Task 5.11 的能力由 `--base-domain` 门控，默认「不给」= 纯 path-style。**失败机制是 `SingleDomain` 的 CNAME 回退，不是域名校验失败**（端口会被 `strip_port_suffix` 剥掉，`is_valid_domain` 也接受带端口的 host）：给了 `--base-domain example.com` 之后，脚本发来的 `Host: 127.0.0.1:9000` 既不等于是 base、也不是它的子域，于是走 CNAME 回退，**桶名变成 `127.0.0.1`**，全部请求 404 `NoSuchBucket`——而报错里不会出现「base-domain」这个词。改这个默认值 = 同时改四个脚本的寻址模式 |
| **`validate_object_key` 的两条规则必须同时生效**（Task 5.7） | 规则 1（保留前缀）漏了 → 用户在 `.rstore*` 里写数据；规则 2（空段 / `.` / `..`）漏了 → 两个不同的 S3 key 落到同一个文件上**静默互相覆盖**。规则 2 还必须接在**读路径**上：只接写路径的话 `GET /b/../x` 会从盘层的 `Fatal(PathEscape)` 变成 500，而不是 400 |
| **条件请求的求值顺序不能重排**（Task 5.10） | RFC 9110 §13.2.2 的两处「缺席时才看」（`If-Match` 挡住 `If-Unmodified-Since`、`If-None-Match` 挡住 `If-Modified-Since`）漏掉任何一处，表现都是「两个头都发的客户端偶尔拿到 200 而它期望 304」——这种 bug 在单头测试里完全看不见，所以 5.10 的矩阵里专门有两条「两个都发、结论不同」的用例 |

---

*本计划对应 `docs/DESIGN.md` v1。计划与设计冲突时，先修正设计文档再改计划。*
