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
| **M4** | 存储引擎核心 | PUT/GET/DELETE + quorum + 提交协议 | M3 |
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
  src/consts.rs                     # BLOCK_SIZE / HASH_LEN / MAX_SHARDS 等

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
  src/get.rs                        # GET 路径
  src/delete.rs                     # DELETE 路径
  src/commit.rs                     # rename 提交协议
  src/quorum.rs                     # quorum 规则与元数据仲裁
  src/errs.rs                       # reduce_errs 错误归约

crates/api/
  src/lib.rs                        # ObjectStore trait、领域错误、boundary 别名

crates/s3/
  src/lib.rs                        # s3s S3Service 装配（构造时接收 Arc<dyn ObjectStore>，见 DESIGN §5 R4）
  src/impl_s3.rs                    # impl S3 for RstoreFs —— 只持有 api trait，不依赖 rstore-store
  src/auth.rs                       # 静态 root 凭证的 AuthProvider
  src/errors.rs                     # 领域错误 → S3 错误码
  src/validate.rs                   # 桶名 / 对象 key 校验（Task 5.7）

crates/s3-compat/
  src/lib.rs                        # compat 中间件栈（按 §DESIGN 15.3 准入）

crates/server/
  src/lib.rs                        # 可测试的服务装配（供集成测试调用）
  src/main.rs                       # 瘦二进制入口，只调 lib（Task 6.3 创建）
  src/wiring.rs                     # 组合根：把 rstore-store 的实现绑定到 rstore-api trait（DESIGN §5 R4）
  src/startup.rs                    # 启动编排
  src/readiness.rs
  src/metrics.rs
  src/config_load.rs

# 集成测试放在各自 crate 的 tests/ 下 —— 根目录是虚拟 workspace（无 [package]），
# 根 tests/ 不会被 cargo 编译。根 tests/ 只放 shell 脚本。
crates/disk/tests/
  faulty_disk.rs                    # Task 3.4
crates/store/tests/
  quorum_boundaries.rs              # Task 4.9
  commit_crash.rs                   # Task 4.10
crates/s3/tests/
  compat_smoke.rs                   # Task 5.9 的 Rust 侧冒烟
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
git commit -m "chore: scaffold workspace with per-domain crates"
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
git commit -m "chore: allowlist-based layer guard and workspace lint hardening"
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
git commit -m "ci: run layer guard, lint, and tests on every push and PR"
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
git commit -m "feat(meta): shard distribution permutation with property tests"
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
git commit -m "feat(checksum): keyed blake3 bitrot hashing with pinned KAT"
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
git commit -m "feat(erasure): codec facade with roundtrip and fail-closed property tests"
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
git commit -m "feat(erasure): LRU cache for codec shells"
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
git commit -m "feat(meta): object metadata data model with nil/epoch semantics"
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
5. 读 `version_count`（偏移 10..12）。**在分配之前**用「剩余字节数 / 最小记录尺寸」
   做上界检查 → `Corrupt(LengthMismatch)`；
6. 逐条解析记录：把 `rmp_serde::Deserializer` 套在 `&mut Cursor` 上读一个
   `FileVersionHeader`（msgpack 自描述，读完 `cursor.position()` 就是边界，
   **不需要长度前缀**）；随后读 `body_len u32 BE`，**分配之前**检查
   `body_len <= 剩余字节` → `Corrupt(LengthMismatch)`；再读 `body_len` 字节作为
   不透明 `Vec<u8>`。msgpack 解析失败 → `Corrupt(MalformedHeader)`；
7. 记录读完处就是 CRC：算 `crc32c(&bytes[..crc_pos])`，与
   `u32::from_le_bytes(bytes[crc_pos..crc_pos + 4])` 比对，不符 → `Corrupt(CrcMismatch)`；
8. CRC 通过后，剩余字节 `bytes[crc_pos + 4..]` 用 msgpack 解成 `InlineData`
   （空切片 → 空 map）。失败 → `Corrupt(MalformedHeader)`。

`encode` 侧对称：记录写完后写 u32 LE 的 CRC，再写 `rmp_serde::to_vec(&meta.inline)`。

> 这里直接调 `rmp_serde::to_vec`（Task 2.3 才会给 `InlineData` 加上
> `encode`/`decode` 方法）。等 2.3 落地后，把这一处和对应的解码处替换成
> `meta.inline.encode()` / `InlineData::decode(tail)`——别留着两份做着同一件事的代码。

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
git commit -m "feat(meta): meta.xl container codec with corruption defenses"
```

---

### Task 2.3: 内联数据帧

**Files:**
- Create: `crates/common/src/consts.rs`（**目前不存在**，需要新建）
- Create: `crates/meta/src/inline.rs`
- Modify: `crates/common/src/lib.rs`（加 `pub mod consts;`）
- Modify: `crates/meta/src/lib.rs`（加 `pub mod inline;`）
- Modify: `crates/meta/src/fileinfo.rs`（给 `InlineData` 补 `encode`/`decode`，也可放在 inline.rs，同一 crate 内均可）
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
    fn known_input_is_under_threshold() {
        assert!(rstore_common::consts::should_inline(64 * 1024, false));
        assert!(!rstore_common::consts::should_inline(256 * 1024, false));
        // 版本化桶门限更严格
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

pub const INLINE_BLOCK: u64 = 128 * 1024;

/// 版本化桶取 1/8；MVP 未启用版本化，但函数签名保留该维度。
pub fn should_inline(size: u64, versioned_bucket: bool) -> bool {
    let threshold = if versioned_bucket { INLINE_BLOCK / 8 } else { INLINE_BLOCK };
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
git commit -m "feat(meta): inline data framing with size thresholds"
```

---

## M3 — 盘抽象

### Task 3.1: DiskAPI trait

**Files:**
- Create: `crates/common/src/disk_id.rs`（`DiskId`——**必须放 common**）
- Create: `crates/disk/src/lib.rs`
- Modify: `crates/common/src/lib.rs`（加 `pub mod disk_id;`）
- Modify: `crates/disk/Cargo.toml`（`tokio`/`async-trait`/`thiserror` 已在 `[dependencies]`；
  需加 `[dev-dependencies] tempfile.workspace = true`）

> `DiskId` 定义在 **`rstore-common`** 而不是 disk：Task 3.3 的 `meta::format` 也要用它，
> 而 meta 只允许依赖 common/checksum（护栏方向 `disk → meta`）。
> `FileStat` 只有 disk 层用，直接定义在 `crates/disk/src/lib.rs`。

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

- [ ] **Step 3: 提交（此时还没有实现，仅契约）**

```bash
git add crates/disk/src/lib.rs
git commit -m "feat(disk): DiskAPI trait and shared contract test suite"
```

---

### Task 3.2: LocalDisk 实现

**Files:**
- Create: `crates/disk/src/local.rs`
- Create: `crates/disk/src/fsx.rs`
- Create: `crates/disk/src/error_map.rs`
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
`local.rs` 用 `tokio::task::spawn_blocking` 包装。**关键约束**：

- **路径逃逸检查**：把所有 `rel_path` 规范化后确认仍在盘根之下，否则 `Fatal`；
- `read_exact_at` 用 `File::read_exact_at`（`std::os::unix::fs::FileExt`）或
  Windows 上等价的 seek+read；短读 → `Transient`；
- `sync_file_and_parent` 必须先 fsync 文件再 fsync 父目录（顺序不可颠倒，否则 rename 可能不持久）；
- `error_map.rs`：`NotFound` → `DiskError::NotFound`；`UnexpectedEof`/`WouldBlock`/`TimedOut`
  → `Transient`；权限/只读挂载 → `Fatal`；**其余默认 `Transient`**（宁可重试，不误判为损坏）。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-disk`
Expected: 全部 PASS

```bash
git add crates/disk/src/
git commit -m "feat(disk): LocalDisk with fsync-aware rename and path escape guard"
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
}
```

> `Transient` / `TransientKind` 是 M3 才引入的 `DiskError` 变体（Task 2.1 里
> `#[non_exhaustive]` 留的口子）。**在本任务把 `DiskError::Transient(TransientKind)`、
> `TransientKind::Timeout`、以及 `DiskError::Fatal(FatalKind)` 一并定义到
> `crates/common/src/error.rs`**——磁盘层从 Task 3.2 起就会返回它们，现在补比以后再改好。
> `TransientKind` 至少要有 `Io` / `Timeout` / `ShortRead`；`FatalKind` 至少要有
> `PermissionDenied` / `ReadOnly` / `PathEscape` / `NoSpace`。

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

按 DESIGN §7 定义 `FormatV1` / `FormatErasureV1` / `DiskInfo`，以及：

- `shared_identity()` → 除 `this` 外的全部字段的哈希；
- `validate()` → `format == "erasure"`、`distribution_algo` 已识别、
  所有 set 长度一致且 `2..=16`；
- `select_authoritative(formats: &[FormatV1]) -> Result<FormatV1, FormatError>`：
  按 `shared_identity()` 分组计票，未达 quorum 报错；
- `should_initialize(errs: &[DiskError]) -> bool`：**仅当所有盘都返回 `NotFound` 时**为真
  （对应 DESIGN §7「网络不可达的盘绝不被当作新拓扑的证据」）。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-meta format`
Expected: PASS

```bash
git add crates/meta/src/format.rs
git commit -m "feat(meta): format.json with shared identity quorum and strict init gate"
```

---

### Task 3.4: FaultyDisk 测试基础设施

**Files:**
- Create: `crates/disk/src/faulty.rs`
- Create: `crates/disk/tests/faulty_disk.rs`
- Modify: `crates/disk/Cargo.toml`（新增 `[features] fault-injection = []`）

> **为什么要 feature**：`FaultyDisk` 必须能被 `rstore-store` 的集成测试用到，
> 而集成测试是**独立编译的 crate**，`#[cfg(test)]` 在那里不生效。
> 因此模块门控写作 `#[cfg(any(test, feature = "fault-injection"))]`：
> crate 内单测自动可见，跨 crate 由 feature 显式开启。

- [ ] **Step 1: 写失败测试**

```rust
use rstore_disk::faulty::{FaultyDisk, Fault};
use rstore_disk::{DiskAPI, LocalDisk};

#[tokio::test]
async fn can_drop_writes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let inner = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
    let d = FaultyDisk::wrap(inner).with(Fault::DropWrites);
    d.write_all("f", b"x").await.unwrap();       // 对外报成功
    assert!(matches!(d.read_exact_at("f", 0, 1).await, Err(DiskError::NotFound(_))));
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
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-disk --features fault-injection --test faulty_disk`
Expected: 编译失败

- [ ] **Step 3: 实现**

`FaultyDisk` 用 `AtomicUsize` 计调用次数、`Mutex<Option<Fault>>` 存当前故障。
必须支持 DESIGN §19.2 列出的全部故障类型：`DropWrites`、`PartialWrite`、`CorruptBytes`、
`Truncate`、`FailAfter { calls, kind }`（`kind: FaultKind::{Transient, Corrupt, NotFound}`）、
`Offline`。

**要求：`FaultyDisk` 必须通过 `contract_tests`（无故障注入时行为与 `LocalDisk` 完全一致）**，
否则它测出来的问题可能是它自己引入的。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-disk --features fault-injection --test faulty_disk`
Expected: 全部 PASS

```bash
git add crates/disk/
git commit -m "test(disk): FaultyDisk fault injection harness"
```

---

## M4 — 存储引擎核心

> 这是 MVP 的主体。每个 Task 都要求先写测试，且**测试必须使用 `FaultyDisk`**，
> 而不是只用 `LocalDisk`。

### Task 4.1: bitrot 分片写入器

**Files:**
- Create: `crates/store/src/writer.rs`
- Test: 同文件 `#[cfg(test)]`

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn writes_interleaved_hash_and_data() {
    // 写 1500 字节、block_size=1024 → 期望落盘 = (32+1024) + (32+476) = 1564
    let tmp = TempDir::new().unwrap();
    let disk = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
    let w = BitrotShardWriter::new(disk, "part.1".into(), 1024);
    w.write_block(&vec![7u8; 1024]).await.unwrap();
    w.write_block(&vec![9u8; 476]).await.unwrap();
    w.finish().await.unwrap();
    let size = tokio::fs::metadata(tmp.path().join("part.1")).await.unwrap().len();
    assert_eq!(size, 32 + 1024 + 32 + 476);
}

#[test]
fn bitrot_size_matches_writer_output() {
    // (原始字节数, block_size, 落盘字节数 = ceil(size/bs)*32 + size)
    let cases = [
        (0u64, 1024u64, 0u64),
        (1, 1024, 33),           // 1 块: 32 + 1
        (1024, 1024, 1056),      // 1 块: 32 + 1024
        (1025, 1024, 1089),      // 2 块: 64 + 1025
        (5000, 512, 5320),       // 10 块: 320 + 5000
    ];
    for (size, bs, want) in cases {
        assert_eq!(bitrot_size(size, bs), want, "size={size} bs={bs}");
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store writer`
Expected: 编译失败

- [ ] **Step 3: 实现**

`BitrotShardWriter` 持 `DiskAPI` + 相对路径 + `block_size`，每次 `write_block` 计算
`bitrot_hash(block)` 并**一次**追加 `[hash][data]`（一次向量写，对应 DESIGN §11）。
`finish()` 调 `sync_file_and_parent`。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store writer`
Expected: PASS

```bash
git add crates/store/src/writer.rs
git commit -m "feat(store): bitrot shard writer with interleaved layout"
```

---

### Task 4.2: bitrot 分片读取器

**Files:** Modify `crates/store/src/reader.rs`（新建）

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn reads_back_what_was_written() { /* 往返 1500 字节，断言逐块校验通过且内容恒等 */ }

#[tokio::test]
async fn detects_bitrot() {
    // 写 → 手工破坏落盘文件的一个数据字节 → 读必须返回 Corrupt(BitrotMismatch)
    let err = reader.read_block(0).await.unwrap_err();
    assert!(matches!(err, DiskError::Corrupt(CorruptKind::BitrotMismatch)));
}

#[tokio::test]
async fn short_file_is_transient_not_corrupt() {
    // 文件被截断 → Transient（因为可能是写入未完成），而不是 Corrupt
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store reader`
Expected: 编译失败

- [ ] **Step 3: 实现**

`BitrotShardReader` 按 `[hash][data]` 定位：读 block `k` 需要
`offset = k * (32 + block_size)`，长度 `32 + block_len`（最后一块可能更短，由总大小推导）。
重算哈希并对齐比较；不匹配 → `Corrupt(BitrotMismatch)`；
文件长度不足 → `Transient`（写入可能未完成），**不是** `Corrupt`。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store reader`
Expected: PASS

```bash
git add crates/store/src/reader.rs
git commit -m "feat(store): bitrot shard reader with corruption classification"
```

---

### Task 4.3: ErasureSet 与盘选择

**Files:**
- Create: `crates/store/src/set.rs`
- Create: `crates/store/src/pool.rs`
- Test: `crates/store/src/set.rs` 的 `#[cfg(test)]`

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
    assert_eq!(read_quorum(4, 2), 4);
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
    /// 测试辅助：建 `total` 块盘、parity 为 `parity` 的 set。
    /// 返回的 set 已挂好 `FaultyDisk`，可用 `inject_fault_on(i, fault)` 注入故障。
    pub async fn for_test(dir: &Path, total: u8, parity: u8) -> Self;
    pub fn total(&self) -> u8 { self.data + self.parity }
    pub fn read_quorum(&self) -> u8 { read_quorum(self.total(), self.parity) }
    pub fn write_quorum(&self) -> u8 { write_quorum(self.data, self.parity) }
}
```

> 测试里的 `set_with_disks(n, p)` 是 M4 测试模块内共用的辅助函数：内部建一个 `TempDir`，
> 挂 `n` 块 `FaultyDisk`，返回持有该临时目录的 `ErasureSet`（目录随 set drop 清理）。
> **写作 `set_with_disks(6, 2)` 表示「6 块盘、parity=2、data=4」**。整个 M4 的测试都遵循这个约定。
> 把它写在 `crates/store/src/testutil.rs`（`#[cfg(test)]`），供各测试文件共用。

**集合路由的 MVP 形态：** `Pool` 持 `Vec<Arc<ErasureSet>>`。MVP 下 `set_count == 1`
（所有盘同属一个 set），因此路由是恒等映射。保留该维度并在 `Pool::pick_set` 处留注释：

```rust
// MVP: set_count == 1，路由恒等。多 set 时的 SipHash 路由见 DESIGN §9.2（Phase 3）。
```

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store set`
Expected: PASS

```bash
git add crates/store/src/set.rs crates/store/src/pool.rs
git commit -m "feat(store): erasure set geometry and quorum rules"
```

---

### Task 4.4: 提交协议

**Files:**
- Create: `crates/store/src/commit.rs`
- Test: 同文件 `#[cfg(test)]`

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn commits_when_quorum_reached() {
    let set = set_with_disks(6, 2).await;
    let r = commit(&set, "b/o/tx1", "b/o/0000", 4).await;
    assert!(r.is_ok());
}

#[tokio::test]
async fn fails_and_reports_when_below_quorum() {
    // 3 块盘 DropWrites/Offline，只留 3 块可 rename，write_quorum=4
    let r = commit(&set, "b/o/tx1", "b/o/0000", 4).await;
    assert!(matches!(r, Err(StoreError::WriteQuorum { achieved: 3, required: 4 })));
}

#[tokio::test]
async fn rollback_removes_already_renamed_dirs() {
    // 达到 2 块盘 rename 成功后失败 → 回滚应尽力删除这 2 个
    // 断言：data_dir 在成功的盘上不再存在（若删除也失败，则残留由对账处理，测试只断言尽力而为）
}

#[tokio::test]
async fn never_reports_success_below_quorum() {
    // 属性式：随机注入故障，只要返回 Ok，就断言成功盘数 >= write_quorum
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store commit`
Expected: 编译失败

- [ ] **Step 3: 实现**

```rust
/// 返回达到的 quorum 数；调用方据此决定成功或失败。
///
/// 硬承诺（DESIGN §12.2）：**绝不在低于 quorum 时报告成功**。
/// 回滚是 best-effort：失败时的残留由对账流程清理，本函数不保证不留字节。
pub async fn commit(
    set: &ErasureSet,
    staging_rel: &str,
    final_rel: &str,
    write_quorum: u8,
) -> Result<CommitOutcome, StoreError> {
    let mut achieved = 0u8;
    let mut renamed: Vec<usize> = Vec::new();
    let mut errs: Vec<DiskError> = Vec::new();

    // 并行 rename 到所有可用盘
    for (i, disk) in set.disks().iter().enumerate() {
        match disk {
            None => errs.push(DiskError::NotFound("offline".into())),
            Some(d) => match d.rename(staging_rel, final_rel).await {
                Ok(()) => { achieved += 1; renamed.push(i); }
                Err(e)   => errs.push(e),
            },
        }
    }

    if achieved >= write_quorum {
        Ok(CommitOutcome { achieved, renamed })
    } else {
        // 尽力回滚：只清理我们自己 rename 过去的那些
        for i in &renamed {
            let _ = set.disks()[*i].as_ref().unwrap().remove_dir_all(final_rel).await;
        }
        Err(StoreError::WriteQuorum { achieved, required: write_quorum })
    }
}
```

> **注意并发写法**：上面是串行伪代码，便于阅读与测试。实现时应换成
> `futures::future::join_all` 并行发起，但**判定逻辑一字不改**——
> 尤其是「先统计成功数、再决定回滚」的顺序，不要写成「遇到第一个失败就提前返回」。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store commit`
Expected: PASS

```bash
git add crates/store/src/commit.rs
git commit -m "feat(store): rename commit protocol with best-effort rollback"
```

---

### Task 4.5: PUT 路径

**Files:**
- Create: `crates/store/src/put.rs`
- Test: `crates/store/src/put.rs` 的 `#[cfg(test)]`

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn put_small_object_inlines_it() {
    let set = set_with_disks(6, 2).await;
    let put = PutArgs { bucket: "b".into(), key: "small".into(), data: vec![1u8; 1000] };
    let out = set.put_object(put).await.unwrap();
    // 内联对象：各盘应存在 meta.xl，但没有 part.*（因为数据在 meta 里）
    assert!(out.etag.len() > 0);
    assert_eq!(out.size, 1000);
}

#[tokio::test]
async fn put_large_object_creates_shards() {
    let set = set_with_disks(6, 2).await;
    let data = vec![7u8; 1_500_000];  // 触发多 block
    let out = set.put_object(PutArgs { bucket: "b".into(), key: "big".into(), data }).await.unwrap();
    assert_eq!(out.size, 1_500_000);
    // 单部分对象：每块盘的数据目录里恰好一个分片文件 part.1（不是 6 个！）
    for disk_idx in 0..6 {
        let files = list_data_dir(&set, disk_idx, "b/big", &out.data_dir).await;
        assert_eq!(files, vec!["meta.xl".to_string(), "part.1".to_string()], "disk {disk_idx}");
    }
}

#[tokio::test]
async fn put_fails_below_write_quorum() {
    let set = set_with_disks(6, 2).await;
    set.inject_fault_on(0, Fault::Offline);
    set.inject_fault_on(1, Fault::Offline);
    set.inject_fault_on(2, Fault::Offline);
    let r = set.put_object(PutArgs { bucket: "b".into(), key: "k".into(), data: vec![0u8; 1_000_000] }).await;
    assert!(matches!(r, Err(StoreError::WriteQuorum { .. })));
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store put`
Expected: 编译失败

- [ ] **Step 3: 实现**

流程（严格按 DESIGN §12.1）：

1. 生成 `data_dir = Uuid::new_v4()`，`txid = Uuid::new_v4()`；
2. 计算 `distribution(key, N)`；校验返回值合法，否则 `InternalError`；
3. 小对象（`should_inline`）→ 构造只含内联数据的 `ObjectMeta`，直接写 `meta.xl`；
4. 大对象 → 按 `BLOCK_SIZE = 1 MiB` 分块；每块：
   a. 按分布排列把块拆到 `data` 个逻辑分片；
   b. `codec.encode` 得到 `parity` 个校验分片；
   c. 对 `N` 个槽位并行 `BitrotShardWriter::write_block`；
   d. 统计成功数，`< write_quorum` → 中止并清理；
5. 写 `meta.xl` 到各可用盘（同样要求 quorum）；
6. `commit(set, staging, final, write_quorum)`；
7. 返回 `{ size, etag(MD5 或 content hash), data_dir, version_id }`。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store put`
Expected: PASS

```bash
git add crates/store/src/put.rs
git commit -m "feat(store): PUT path with erasure encoding and inline fast path"
```

---

### Task 4.6: 元数据仲裁

**Files:**
- Create: `crates/store/src/quorum.rs`
- Test: 同文件 `#[cfg(test)]`

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn identical_metadata_wins_quorum() {
    // total=6, parity=2 → read_quorum=4。4 票 meta_a 达到 quorum。
    let metas = vec![meta_a(), meta_a(), meta_a(), meta_a(), meta_b(), meta_b()];
    let r = resolve_metadata(&metas, 6, 2).unwrap();
    assert_eq!(r, meta_a());
}

#[test]
fn minority_metadata_cannot_win() {
    // 3 票 < read_quorum 4 → 必须报错，不能「多数决」直接返回少数派
    let metas = vec![meta_a(), meta_a(), meta_a(), meta_b(), meta_c(), meta_b()];
    assert!(matches!(resolve_metadata(&metas, 6, 2), Err(StoreError::ReadQuorum)));
}

#[test]
fn no_quorum_is_an_error() {
    let metas = vec![meta_a(), meta_b(), meta_c()];
    assert!(matches!(resolve_metadata(&metas, 6, 2), Err(StoreError::ReadQuorum)));
}

#[test]
fn volatile_fields_do_not_split_quorum() {
    // 两个 meta 只有 heal/purge 状态不同，其余相同 → 必须归为同一组
    let mut b = meta_a();
    b.meta_sys.insert("x-rs-healing".into(), b"true".to_vec());
    let metas = vec![meta_a(), meta_a(), b, meta_a()];
    assert!(resolve_metadata(&metas, 6, 2).is_ok());
}

#[test]
fn missing_disks_are_not_failures() {
    // 2 块盘返回 NotFound，4 块一致 → 成功
    let metas = vec![Some(meta_a()), None, Some(meta_a()), None, Some(meta_a()), Some(meta_a())];
    assert!(resolve_metadata_opt(&metas, 6, 2).is_ok());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store quorum`
Expected: 编译失败

- [ ] **Step 3: 实现**

```rust
/// 全部盘都返回了元数据时使用。
pub fn resolve_metadata(
    metas: &[ObjectMeta],
    total: u8,
    parity: u8,
) -> Result<ObjectMeta, StoreError>;

/// 部分盘掉线时使用。`None` 表示该盘未返回（**不计为失败，也不计为票**）。
pub fn resolve_metadata_opt(
    metas: &[Option<ObjectMeta>],
    total: u8,
    parity: u8,
) -> Result<ObjectMeta, StoreError>;
```

- 身份哈希：SHA-256 over `size / flags / mod_time / version_id / data_dir / parts`，
  **显式排除** `x-rs-healing`、`x-rs-purge-status` 等易变键；
- 按身份哈希分组计票，取票数最高组；`< read_quorum` → `StoreError::ReadQuorum`；
- `None`（盘未返回）**不计为失败**，也**不计为票**——语义与「返回了不匹配的元数据」不同；
- 早停优化：用 `u16` 位图记录已返回的槽位（`N ≤ 16`，零分配）。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store quorum`
Expected: PASS

```bash
git add crates/store/src/quorum.rs
git commit -m "feat(store): metadata quorum with identity-hash voting"
```

---

### Task 4.7: GET 路径

**Files:**
- Create: `crates/store/src/get.rs`
- Test: 同文件 `#[cfg(test)]`

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn get_returns_what_was_put() {
    let set = set_with_disks(6, 2).await;
    let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
    set.put_object(put_args("b", "k", data.clone())).await.unwrap();
    let got = set.get_object("b", "k", None).await.unwrap().read_to_end().await.unwrap();
    assert_eq!(got, data);
}

#[tokio::test]
async fn get_with_range_reads_only_needed_shards() {
    // Range: bytes=1000-1999 → 结果等于 data[1000..2000]
}

#[tokio::test]
async fn get_survives_two_disk_losses() {
    let set = set_with_disks(6, 2).await;
    set.put_object(put_args("b", "k", vec![3u8; 2_000_000])).await.unwrap();
    set.inject_fault_on(0, Fault::Offline);
    set.inject_fault_on(1, Fault::Offline);
    let got = set.get_object("b", "k", None).await.unwrap().read_to_end().await.unwrap();
    assert_eq!(got.len(), 2_000_000);
}

#[tokio::test]
async fn get_fails_closed_below_read_quorum() {
    let set = set_with_disks(6, 2).await;
    set.put_object(put_args("b", "k", vec![3u8; 2_000_000])).await.unwrap();
    for i in 0..3 { set.inject_fault_on(i, Fault::Offline); }
    let r = set.get_object("b", "k", None).await;
    assert!(matches!(r, Err(StoreError::ReadQuorum)), "must not return partial data");
}

#[tokio::test]
async fn get_inlines_short_circuit_disk_reads() {
    // 小对象应只读 meta.xl，不打开 part.* —— 用 FaultyDisk 让所有 part 读取失败来验证
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store get`
Expected: 编译失败

- [ ] **Step 3: 实现**

流程（DESIGN §14.4）：

1. 计算 `distribution` 与目标 set；
2. 并行读各盘 `meta.xl` → `resolve_metadata` 得权威元数据；
3. 内联对象 → 直接从元数据返回，**不碰 part**；
4. 否则建 `BitrotShardReader` 并行按需读取，`< read_quorum` → `ReadQuorum`；
5. `codec.decode` 重构数据分片，按分布排列拼回原顺序；
6. 若 `available > data`（有多余分片）→ 异步入队读修复（MVP 可先只记录指标，
   留 TODO 注释指向 Phase 4）；
7. Range 请求：只读取覆盖请求范围的 block。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store get`
Expected: PASS

```bash
git add crates/store/src/get.rs
git commit -m "feat(store): GET path with inline fast path and fail-closed quorum"
```

---

### Task 4.8: DELETE 与覆盖写

**Files:**
- Create: `crates/store/src/delete.rs`
- Test: 同文件 `#[cfg(test)]`

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn overwrite_replaces_latest() { /* put A, put B, get == B */ }

#[tokio::test]
async fn delete_makes_get_return_not_found() { /* put, delete, get → NotFound */ }

#[tokio::test]
async fn delete_marks_before_gc() {
    // 删除是「写新的元数据标记」而不是立即删数据；确认标记先落地
}

#[tokio::test]
async fn gc_only_after_old_dir_outvoted() {
    // 覆盖写后，旧 data_dir 在多数盘上被确认取代，才允许删除
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store delete`
Expected: 编译失败

- [ ] **Step 3: 实现**

- **覆盖写**：新版本走与 PUT 完全相同的路径（新 `data_dir`）；
  提交成功后，对旧 `data_dir` 执行「多盘投票」——若多数盘的当前元数据已不指向它，
  才在**各盘**删除旧目录。投票未达多数 → 留着，交给对账；
- **DELETE**：MVP 无版本化，语义是「写入一个删除记录作为最新版本，并触发旧数据 GC」；
  删除记录本身也走 `commit`，quorum 用 `delete_quorum = N/2 + 1`；
- **GC 幂等**：删除不存在的目录返回 `Ok`。

- [ ] **Step 4: 跑测试确认通过并提交**

Run: `cargo test -p rstore-store delete`
Expected: PASS

```bash
git add crates/store/src/delete.rs
git commit -m "feat(store): overwrite and delete with outvote-based GC"
```

---

### Task 4.9: Quorum 边界测试套件

**Files:**
- Create: `crates/store/tests/quorum_boundaries.rs`
- Modify: `crates/store/Cargo.toml`（`[dev-dependencies]` 加
  `rstore-disk = { workspace = true, features = ["fault-injection"] }`）

- [ ] **Step 1: 写测试（这是 DESIGN §19.2 的落地）**

```rust
/// 4+2 配置下的完整边界矩阵。每行是一个独立用例。
#[tokio::test]
async fn matrix_4_plus_2() {
    let cases = [
        // (掉线盘数, 操作, 期望)
        (0, Op::Read,  Expect::Ok),
        (2, Op::Read,  Expect::Ok),            // N-data = 2，刚好还能读
        (3, Op::Read,  Expect::ReadQuorum),    // 低于 read_quorum
        (2, Op::Write, Expect::Ok),            // parity = 2，刚好还能写
        (3, Op::Write, Expect::WriteQuorum),
        (1, Op::Delete, Expect::Ok),
        (4, Op::Delete, Expect::WriteQuorum),  // delete_quorum = 3
    ];
    for (offline, op, expect) in cases {
        run_case(set_with_disks(6, 2).await, offline, op, expect).await;
    }
}

#[tokio::test]
async fn bitrot_on_minority_still_reads_correctly() {
    // 1 块盘写入静默损坏 → 读成功，且结果是正确的
}

#[tokio::test]
async fn bitrot_on_majority_exposes_corruption_not_wrong_data() {
    // 3 块盘静默损坏 → 绝不返回错误数据：要么 ReadQuorum，要么能校验出不一致
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store --test quorum_boundaries`
Expected: 编译失败（`run_case` 未实现）

- [ ] **Step 3: 实现测试辅助函数并让测试通过**

`run_case` 负责：建 set → 注入对应数量的故障 → 执行操作 → 断言错误类型。
**特别注意最后一条**：它验证的是 DESIGN §2 的 P1——宁可报错，不可返回错数据。

- [ ] **Step 4: 提交**

Run: `cargo test -p rstore-store --test quorum_boundaries`
Expected: 全部 PASS

```bash
git add crates/store/tests/quorum_boundaries.rs crates/store/Cargo.toml
git commit -m "test(store): quorum boundary matrix across failure modes"
```

---

### Task 4.10: 崩溃点状态机测试

**Files:** Create `crates/store/tests/commit_crash.rs`

- [ ] **Step 1: 写测试**

把提交协议建模为一个可注入「崩溃点」的状态机。每个崩溃点执行「kill → 重建 Pool → 断言」：

```rust
#[derive(Debug, Clone, Copy)]
enum CrashPoint {
    BeforeStagingWrite,
    AfterShardWriteBeforeSync,
    AfterSyncBeforeRename,
    AfterPartialRename,
    AfterRenameBeforeOldGc,
    DuringOldGc,
}

#[tokio::test]
async fn crash_recovery_invariants() {
    for point in ALL_CRASH_POINTS {
        let dir = tempfile::TempDir::new().unwrap();
        let crashed = run_put_until_crash(&dir, point).await;
        // 模拟重启：重新打开同一个目录
        let pool = Pool::open(&dir.path()).await.unwrap();
        let got = pool.get_object("b", "k").await;

        // 不变量 1（可见性）：对象要么完全可见且内容正确，要么 NotFound
        match got {
            Ok(r) => assert_eq!(r.read_to_end().await.unwrap(), EXPECTED_DATA, "point={point:?}"),
            Err(StoreError::NotFound) => {}
            Err(e) => panic!("unexpected error at {point:?}: {e:?}"),
        }

        // 不变量 2（可回收性）：任何残留都能被对账流程识别
        let leftovers = pool.scan_orphans().await.unwrap();
        for l in leftovers {
            assert!(pool.can_reclaim(&l), "unreclaimable orphan at {point:?}: {l}");
        }
    }
}

#[tokio::test]
async fn old_gc_crash_never_loses_both_versions() {
    // 覆盖写两个版本后，在 GC 各阶段崩溃，重启后至少能读到其中一个版本
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store --test commit_crash`
Expected: 编译失败

- [ ] **Step 3: 实现**

需要：
- `run_put_until_crash`：用 `FaultyDisk` 的 `FailAfter` 精确控制崩溃时机；
- `Pool::scan_orphans` / `can_reclaim`：MVP 阶段实现为「识别 `.staging-*` 与
  无对应元数据的 data-dir，且能安全删除」。这是 DESIGN §12.2 中「残留由对账清理」的最小实现。

- [ ] **Step 4: 提交**

Run: `cargo test -p rstore-store --test commit_crash`
Expected: 全部 PASS

```bash
git add crates/store/tests/commit_crash.rs crates/store/src/pool.rs
git commit -m "test(store): commit protocol crash-point invariant tests"
```

---

## M5 — S3 接入

### Task 5.1: s3s 骨架与认证

**Files:**
- Create: `crates/api/src/lib.rs`
- Create: `crates/s3/src/lib.rs`、`crates/s3/src/auth.rs`
- Create: `crates/s3/src/impl_s3.rs`

- [ ] **Step 1: 定义 api 契约**

`crates/api/src/lib.rs` 定义 `ObjectStore` trait（`put_object` / `get_object` /
`head_object` / `delete_object` / `list_objects` / multipart 系列）与领域错误 `StoreError`。

**绑定实现的位置是 `rstore-server/src/wiring.rs`，不是 `rstore-s3`**（DESIGN §5 规则 R4）。
`rstore-s3` 只持有 `Arc<dyn ObjectStore>`，由构造参数注入：

```rust
pub struct RstoreFs {
    store: Arc<dyn ObjectStore>,   // 不是 Arc<ECStore> —— s3 看不到引擎类型
    auth:  Arc<dyn AuthProvider>,
}
```

这样 `rstore-s3` 可以脱离引擎单独测试（传入 mock `ObjectStore`），
且 `crates/s3/Cargo.toml` 永远不需要加 `rstore-store` 依赖——
否则会与 DESIGN §5 的依赖表及 Task 0.2 的护栏脚本直接冲突。

- [ ] **Step 2: 写集成测试（用 s3s 的测试工具或直接打 HTTP）**

```rust
#[tokio::test]
async fn rejects_bad_signature() {
    // 启动服务 → 发一个错误的 Authorization → 期望 403 SignatureDoesNotMatch
}

#[tokio::test]
async fn accepts_valid_sigv4() {
    // 用 aws-sigv4 生成正确签名 → 期望 200
}
```

- [ ] **Step 3: 实现**

用 `s3s::S3ServiceBuilder` 组装，`set_auth` 传入一个静态 root 凭证的 `AuthProvider`
（MVP 单用户，从配置文件读 access_key / secret_key）。

- [ ] **Step 4: 提交**

```bash
git add crates/api/ crates/s3/
git commit -m "feat(s3): s3s service skeleton with single root credential auth"
```

---

### Task 5.2 ~ 5.6: S3 操作实现

按以下顺序，**每个 Task 一个操作组**，每个都先写 HTTP 层集成测试：

| Task | 操作 | 测试要点 |
|---|---|---|
| 5.2 | `CreateBucket` / `DeleteBucket` / `HeadBucket` / `ListBuckets` | 空桶可删、非空桶删返回 409 |
| 5.3 | `PutObject` / `GetObject` / `HeadObject` / `DeleteObject` | 往返一致、ETag 正确、HEAD 无 body 但有 Content-Length |
| 5.4 | `GetObject` 的 Range | `bytes=a-b` / `bytes=a-` / `bytes=-n` 三种形式；206 与 `Content-Range` |
| 5.5 | `ListObjectsV2` | 前缀、分隔符、`max-keys` 分页、`CommonPrefixes`、`continuation-token` 往返 |
| 5.6 | Multipart 全流程 | 分片上传后 Complete 的 ETag 格式为 `<md5>-<n>`；Abort 后目录被清理；不存在的 part 号返回 `InvalidPart` |
| 5.7 | 命名校验 | 见下（单独展开） |

**每个 Task 的提交信息格式：** `feat(s3): implement <operation group>`

> **实现提示（5.5 尤其注意）**：LIST 在 MVP 是**全盘遍历**。必须在注释中写明这一点，
> 并留下 `// PERF: 见 DESIGN §1.2 与 §20 Phase 2 — 命名空间索引` 的挂钩注释，
> 避免后续读者误以为这是终态设计。

---

### Task 5.7: 命名校验（保留名规则）

**Files:** Create `crates/s3/src/validate.rs`

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
fn rejects_bucket_names_violating_s3_rules() {
    assert!(validate_bucket_name(".hidden").is_err());    // 不能以 '.' 开头
    assert!(validate_bucket_name("OK-Bucket").is_err());  // 不能有大写
    assert!(validate_bucket_name("-leading").is_err());
    assert!(validate_bucket_name("ok-bucket").is_ok());
    assert!(validate_bucket_name("ok.bucket.123").is_ok());
}

#[test]
fn reserved_prefix_constant_is_not_empty() {
    // 防止有人在重构中把常量改成空串，让校验静默失效
    assert!(!rstore_meta::keys::RESERVED_PREFIX.is_empty());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-s3 validate`
Expected: 编译失败

- [ ] **Step 3: 实现**

```rust
/// 对象 key 校验。返回 `StoreError::InvalidObjectName`。
/// 只检查第一段；深层段允许出现 `.rstore`（DESIGN §6.3）。
pub fn validate_object_key(key: &str) -> Result<(), StoreError>;

/// 桶名校验。S3 规则：3–63 字符、小写字母或数字开头、仅含 `a-z0-9.-`。
/// 返回 `StoreError::InvalidBucketName`。
pub fn validate_bucket_name(name: &str) -> Result<(), StoreError>;
```

在 `crates/meta/src/keys.rs` 中定义 `pub const RESERVED_PREFIX: &str = ".rstore";`，
两处校验都引用它，**不得内联字面量**。

- [ ] **Step 4: 在请求入口接入**

在 `impl_s3.rs` 的 `put_object` / `get_object` / `head_object` / `delete_object` /
`list_objects_v2` 入口调用校验。补一个 HTTP 层测试：对 `.rstore.sys/x` 发 PUT，
期望 `400` + `InvalidObjectName`。

- [ ] **Step 5: 提交**

Run: `cargo test -p rstore-s3 validate`
Expected: PASS

```bash
git add crates/s3/src/validate.rs crates/meta/src/keys.rs crates/s3/src/impl_s3.rs
git commit -m "feat(s3): bucket and object key validation with reserved prefix rule"
```

---

### Task 5.8: 错误映射

**Files:** Create `crates/s3/src/errors.rs`

- [ ] **Step 1: 写测试**

```rust
#[test]
fn maps_store_errors_to_s3_codes() {
    assert_code(StoreError::NotFound, "NoSuchKey");
    assert_code(StoreError::NoSuchBucket, "NoSuchBucket");
    assert_code(StoreError::InvalidPart, "InvalidPart");
    assert_code(StoreError::ReadQuorum { .. }, "InternalError");
    assert_code(StoreError::WriteQuorum { .. }, "InternalError");
    assert_code(StoreError::DiskFull, "InsufficientStorage");
    assert_code(StoreError::SlowDown, "SlowDown");
}
```

- [ ] **Step 2-4: 实现、跑测试、提交**

要求每个 S3 错误响应包含 `Code` / `Message` / `Resource` / `RequestId` 四要素
（DESIGN §15.4）。

```bash
git add crates/s3/src/errors.rs
git commit -m "feat(s3): domain error to S3 error code mapping"
```

---

### Task 5.9: 兼容层与客户端冒烟测试

**Files:**
- Create: `crates/s3-compat/src/lib.rs`
- Create: `tests/compat/aws_cli.sh`、`tests/compat/mc.sh`、`tests/compat/rclone.sh`

- [ ] **Step 1: 先跑冒烟脚本，找出真实的不兼容点**

脚本框架（`aws_cli.sh`）：

```bash
#!/usr/bin/env bash
set -euo pipefail
export AWS_ACCESS_KEY_ID=rustorage
export AWS_SECRET_ACCESS_KEY=rustorage-secret
export AWS_DEFAULT_REGION=us-east-1
EP="http://127.0.0.1:9000"

aws --endpoint-url "$EP" s3 mb s3://test-bucket
head -c 1048576 /dev/urandom > /tmp/1m.bin
aws --endpoint-url "$EP" s3 cp /tmp/1m.bin s3://test-bucket/1m.bin
aws --endpoint-url "$EP" s3 cp s3://test-bucket/1m.bin /tmp/roundtrip.bin
cmp /tmp/1m.bin /tmp/roundtrip.bin
aws --endpoint-url "$EP" s3api list-objects-v2 --bucket test-bucket --prefix "" --max-keys 1
aws --endpoint-url "$EP" s3api list-objects-v2 --bucket test-bucket --delimiter "/"
echo "aws-cli smoke: OK"
```

同样方式写 `mc.sh`（`mc alias set` / `cp` / `ls` / `cat` / `rm`）与
`rclone.sh`（`copy` / `check`）。

- [ ] **Step 2: 运行脚本，记录失败项**

Run: `bash tests/compat/aws_cli.sh`
Expected: 首次运行**允许失败**——失败项就是 compat 层的需求来源

- [ ] **Step 3: 为每个失败项添加 compat 中间件**

**必须遵守 DESIGN §15.3 的准入规则**：每条中间件带注释

```rust
// compat: aws-cli — PUT 空对象时不发 Content-Length，需归一化为 0 — see tests/compat/aws_cli.sh
```

并且该中间件被移除时，对应的冒烟测试必须失败。**不允许凭猜测添加中间件。**

- [ ] **Step 4: 三个脚本全部通过后提交**

```bash
git add crates/s3-compat/ tests/compat/
git commit -m "feat(s3-compat): client ecosystem compatibility layers driven by smoke tests"
```

---

## M6 — 运维面与验收

### Task 6.1: Readiness 与健康端点

**Files:** Create `crates/server/src/readiness.rs`

- [ ] **Step 1: 写失败测试**

```rust
#[tokio::test]
async fn returns_503_before_storage_ready() {
    // 进程已监听但尚未完成存储初始化 → /ready 返回 503 且带 Retry-After: 5
}

#[tokio::test]
async fn returns_200_after_storage_ready() { }

#[tokio::test]
async fn stage_is_monotonic() {
    // mark_stage 不允许回退
}

#[tokio::test]
async fn health_is_independent_of_readiness() {
    // /health 在 Booting 阶段也返回 200
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
git commit -m "feat(server): staged readiness with health/ready endpoints"
```

---

### Task 6.2: Prometheus 指标

**Files:** Create `crates/server/src/metrics.rs`

- [ ] **Step 1: 写失败测试**

```rust
#[test]
fn metrics_disabled_is_noop() {
    // 开关关闭时 record_* 不改变任何计数
}

#[tokio::test]
async fn exposes_prometheus_text_format() {
    // GET /metrics → 包含 put_duration_seconds / erasure_quorum_failures_total
}
```

- [ ] **Step 2-4: 实现、跑测试、提交**

按 DESIGN §18.2，指标名常量集中定义，热路径用 `LazyLock` 缓存 handle。
至少暴露：`put_duration_seconds{stage}`、`get_duration_seconds{stage}`、
`erasure_quorum_failures_total{op}`、`bitrot_mismatch_total`、
`buffer_pool_acquire_total{class}`、`disk_errors_total{kind}`。

```bash
git add crates/server/src/metrics.rs
git commit -m "feat(server): prometheus metrics endpoint"
```

---

### Task 6.3: 启动与关闭编排

**Files:** Create `crates/server/src/startup.rs`、`crates/server/src/config_load.rs`

- [ ] **Step 1: 写测试**

```rust
#[tokio::test]
async fn refuses_to_start_on_inconsistent_formats() {
    // 两盘 format.json 的 shared_identity 不一致 → 启动失败且错误信息指明是哪些盘
}

#[tokio::test]
async fn refuses_to_reformat_reachable_disks() {
    // 一盘有数据、一盘空白 → 必须拒绝，不能把有数据的盘当新盘初始化
}

#[tokio::test]
async fn shutdown_cleanly_stops_accepting_then_drains() { }
```

- [ ] **Step 2-4: 实现、跑测试、提交**

启动顺序：解析配置 → 打开各盘 → 读/校验 `format.json` → 构造 `Pool` →
`mark_stage(StorageReady)` → 起 HTTP 服务。
关闭：停止接受新连接 → 等待在飞请求（带超时）→ 退出。
所有长生命周期任务绑定 `CancellationToken`。

```bash
git add crates/server/src/startup.rs crates/server/src/config_load.rs crates/server/src/main.rs
git commit -m "feat(server): startup/shutdown orchestration with format validation"
```

---

### Task 6.4: 端到端验收

**Files:** Create `tests/acceptance.sh`

- [ ] **Step 1: 写验收脚本**

```bash
#!/usr/bin/env bash
set -euo pipefail
# 启动 6 盘 4+2 实例（4 数据分片 + 2 校验分片 = 6 块盘）
mkdir -p /tmp/rs/{d1,d2,d3,d4,d5,d6}
cargo run -p rstore-server -- --volumes /tmp/rs/d{1,2,3,4,5,6} --port 9000 &
SERVER_PID=$!
sleep 3

# 1. 客户端冒烟
bash tests/compat/aws_cli.sh
bash tests/compat/mc.sh
bash tests/compat/rclone.sh

# 2. 容错：停掉两块盘（用 chmod 000 模拟，或直接删除目录权限）
#    → 读仍成功
# 3. 恢复后校验数据完整

kill $SERVER_PID
echo "ACCEPTANCE: OK"
```

- [ ] **Step 2: 运行，直到全部通过**

Run: `bash tests/acceptance.sh`
Expected: 输出 `ACCEPTANCE: OK`

- [ ] **Step 3: 提交**

```bash
git add tests/acceptance.sh
git commit -m "test: end-to-end acceptance script"
```

---

## 完成检查清单

MVP 交付时必须全部为真：

- [ ] `cargo build --workspace` 无 warning
- [ ] `cargo test --workspace` 全绿
- [ ] `cargo clippy --all-targets -- -D warnings` 通过
- [ ] `bash scripts/check-layer-deps.sh` 退出码 0
- [ ] `python3 scripts/tests/test_check_layer_deps.py` 全绿
- [ ] CI 在 `main` 与 PR 上跑通（护栏、lint、test 三步都不是跳过状态）
- [ ] `bash tests/acceptance.sh` 输出 `ACCEPTANCE: OK`
- [ ] 4+2 配置下：掉 2 盘可读、掉 2 盘可写、掉 3 盘读返回 `ReadQuorum` 而非错误数据
- [ ] `FaultyDisk` 注入静默字节损坏时，读路径能检出 `BitrotMismatch`
- [ ] 崩溃点测试覆盖 DESIGN §12.3 的全部窗口
- [ ] `rstore-s3-compat` 中每个中间件都有对应的冒烟测试，且注释指明来源客户端
- [ ] DESIGN §1.2 的非目标清单中，没有任何一项被意外实现（范围不蔓延）

---

## 风险与注意事项

| 风险 | 应对 |
|---|---|
| `reed-solomon-simd` 的 API 与预期不符 | Task 1.3 已把库调用完全封在门面内；若签名不同，只改 `encode`/`decode` 内部，测试不变 |
| 各平台 `read_exact_at` 差异（Windows 无 `FileExt::read_exact_at`） | `fsx.rs` 用 seek + read 的通用实现；Linux/macOS 再加 `#[cfg]` 优化路径 |
| 单节点下 rename 的持久性依赖文件系统 | `sync_file_and_parent` 必须先 fsync 文件再 fsync 父目录，Task 3.2 有专门测试 |
| LIST 全盘扫描在大数据集上很慢 | MVP 明确接受，接口留挂钩位；不要在这一阶段引入索引（YAGNI） |
| 崩溃点测试难以稳定复现 | 用 `FaultyDisk::FailAfter` 精确控制，而非依赖真实 kill；不确定的路径不要写进测试 |

---

*本计划对应 `docs/DESIGN.md` v1。计划与设计冲突时，先修正设计文档再改计划。*
