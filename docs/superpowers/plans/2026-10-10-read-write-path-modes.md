# 读写路径新旧模式共存 + 性能对比 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 Rustorage 的读写路径加上四个**可拆除**的加速机制（范围读、元数据缓存、有界列举、写入缓冲复用），保留今天的实现作为「旧模式」，产出可复现的新旧对比。

**Architecture:** 开关是一个 `IoModes` 结构体，挂在 `ErasureSet` 上。`ErasureSet::new(disks, parity)` **就是**旧模式（它委托给 `with_modes(.., IoModes::default())`），所以现存调用点一个字节都不用改。每个机制**恰好一个决策点**，量完即整体删除。

**Tech Stack:** Rust 2021 · tokio · clap · `async-trait`（dev）· `tempfile`（dev）。**不引 criterion**——基准是 `harness = false` 的手写 `fn main`。

**设计文档：** [`docs/superpowers/specs/2026-10-10-read-write-path-modes-design.md`](../specs/2026-10-10-read-write-path-modes-design.md)。计划与设计冲突时以设计为准，设计里说不清的以本计划为准并回头补设计。

**参考材料：** [`docs/read-write-path-design.md`](../../read-write-path-design.md)（从 RustFS 提取的读写架构原理）。

---

## 全局约束（每个任务都适用）

1. **旧模式 = 今天的代码。** 任何既有函数在模式关掉时的行为必须逐字节不变。既有测试**一行都不许改**（除了本计划明确要求改的）。
2. **盘上布局一个字节不变。** 新旧模式共用同一批数据文件。
3. **不新增对外 S3 API。** `rstore-api::ObjectStore` 只加一个**带默认实现**的方法。
4. **commit 用中文**，结尾带 `Co-Authored-By: Claude Code <noreply@anthropic.com>`。
5. 每个任务结束都要跑：
   ```bash
   cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
   ```
   三条全绿才提交。

---

## 文件结构

| 文件 | 动作 | 职责 |
|---|---|---|
| `crates/common/src/modes.rs` | 新增 | `IoModes`：四个具名 bool + `old`/`new` 解析 |
| `crates/common/src/lib.rs` | 改 | `pub mod modes;` |
| `crates/store/src/set.rs` | 改 | `ErasureSet` 加 `modes` / `resolve_cache` 字段，`with_modes` |
| `crates/store/src/reader.rs` | 改 | `read_range`（+ 抽出 `checked_stat` / `read_blocks`） |
| `crates/store/src/get.rs` | 改 | `read_shards` 认块区间；`resolve_version_cached` |
| `crates/store/src/resolve_cache.rs` | 新增 | 按 `(bucket,key)` 分片缓存的版本解析结果 |
| `crates/store/src/list.rs` | 改 | 有序增量遍历 `candidate_keys_ordered`；`list_objects_from` |
| `crates/store/src/put.rs` | 改 | `read_block_into` + `ShardScratch`；`put_object` 薄包装 |
| `crates/store/src/delete.rs` | 改 | `delete_object` 薄包装 |
| `crates/store/src/lib.rs` | 改 | 注册 `resolve_cache` 与 `io_modes_equiv` |
| `crates/store/src/io_modes_equiv.rs` | 新增 | 差分等价测试（§7.2 硬门槛 1） |
| `crates/store/src/testutil.rs` | 改 | `set_with_modes` / `set_with_recording_disks_modes` |
| `crates/api/src/lib.rs` | 改 | `ObjectStore::list_objects_from`（带默认实现） |
| `crates/server/src/wiring.rs` | 改 | 实现 `list_objects_from`；构造带模式的 set |
| `crates/server/src/config.rs` | 改 | `--io-mode` + `modes()` |
| `crates/server/src/startup.rs` | 改 | 两处构造点传模式 |
| `crates/s3/src/impl_s3.rs` | 改 | `list_objects_v2` 改成分批取 |
| `crates/store/benches/io_modes.rs` | 新增 | 基准（`harness = false`） |
| `crates/store/Cargo.toml` | 改 | `[[bench]]` |
| `docs/io-modes-bench.md` | 新增 | 跑出来的对比结果 |
| `docs/DESIGN.md` | 改 | §11 追加范围读的 bitrot 语义 |

---

## Task 1: `IoModes` 骨架 + 接入 `ErasureSet` + CLI

**Files:**
- Create: `crates/common/src/modes.rs`
- Modify: `crates/common/src/lib.rs`
- Modify: `crates/store/src/set.rs:47-77`
- Modify: `crates/store/src/testutil.rs:86-107`
- Modify: `crates/server/src/config.rs`
- Modify: `crates/server/src/startup.rs:253,312`

- [ ] **Step 1: 写 `IoModes` 及其失败测试**

创建 `crates/common/src/modes.rs`：

```rust
//! 读写路径的模式开关（**临时**）。
//!
//! **存在的唯一目的**：让「旧路径」（今天的实现）与「新路径」（四个加速机制）
//! 同时存在于一个二进制里，从而能在同一份数据集、同一台机器上直接对比测量。
//!
//! **全部字段为 `false` 时，行为与引入本模块之前逐字节相同**——
//! `ErasureSet::new` 的语义没有变，它只是委托给了 `with_modes(.., IoModes::default())`。
//!
//! 基准跑完即整体删除本模块，各机制各自成为唯一实现。见
//! `docs/superpowers/specs/2026-10-10-read-write-path-modes-design.md` §10。

/// 四个机制各自独立，互不耦合。
///
/// 用四个具名 `bool` 而不是 bitflags：为了**可 grep、可逐个删**。
/// 加一个 `bitflags` 依赖换来的只是更短的代码，而这份代码只活到基准跑完。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoModes {
    /// 范围读：只读命中的块区间，不再把整份分片读进内存。
    pub ranged_shard_read: bool,
    /// 元数据缓存：`get_object` / `head_object` 复用最近解析出的版本。
    pub metadata_cache: bool,
    /// 有界列举：LIST 增量遍历，够数即停，不再全量物化。
    pub bounded_listing: bool,
    /// 写入缓冲复用：单次 PUT 内跨块复用分片缓冲。
    pub pooled_write_buffers: bool,
}

impl IoModes {
    /// 四个机制全开。
    pub const ALL: Self = Self {
        ranged_shard_read: true,
        metadata_cache: true,
        bounded_listing: true,
        pooled_write_buffers: true,
    };

    /// 从 CLI 的 `old` / `new` 解析；其他值返回 `None`（由调用方报错）。
    ///
    /// **刻意不暴露逐位开关**：CLI 面上只留两档，避免产生「半新半旧」的运维组合。
    /// 逐位组合只在测试里用（阶梯式归因需要 `+A` → `+A,C` → `+A,C,D`）。
    pub fn from_io_mode(s: &str) -> Option<Self> {
        match s {
            "old" => Some(Self::default()),
            "new" => Some(Self::ALL),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 「旧」必须恰好等于 `Default`（全关），「新」必须四项全开。
    /// 这条钉住 CLI 两档与结构体默认值之间的对应关系——漂了就会出现
    /// 「指定 `old` 却拿到了部分新机制」这种最难查的对比失真。
    #[test]
    fn old_is_all_off_and_new_is_all_on() {
        // 走 `from_io_mode` 拿值而不是直接读 `IoModes::ALL` / `default()`：
        // 一来顺带把解析路径测了，二来对常量表达式直接断言会被 clippy 的
        // `assertions_on_constants` 拒掉（它求值后发现恒真）。
        let off = IoModes::from_io_mode("old").expect("old 是合法取值");
        let on = IoModes::from_io_mode("new").expect("new 是合法取值");

        assert_eq!(off, IoModes::default());
        assert_eq!(on, IoModes::ALL);

        // 逐字段再断一遍：整体比较已经覆盖「全关/全开」，但逐项写出来，
        // 失败时能直接指出是哪一个开关漂了。
        assert!(!off.ranged_shard_read);
        assert!(!off.metadata_cache);
        assert!(!off.bounded_listing);
        assert!(!off.pooled_write_buffers);

        assert!(on.ranged_shard_read);
        assert!(on.metadata_cache);
        assert!(on.bounded_listing);
        assert!(on.pooled_write_buffers);
    }

    /// 未知取值必须返回 `None` 而不是悄悄退化成某一档：静默的默认值会让
    /// 「我明明指定了 new」变成一次无效的测量。
    #[test]
    fn unknown_mode_is_none() {
        assert_eq!(IoModes::from_io_mode("half"), None);
        assert_eq!(IoModes::from_io_mode(""), None);
        assert_eq!(IoModes::from_io_mode("NEW"), None);
    }
}
```

- [ ] **Step 2: 注册模块并跑测试**

在 `crates/common/src/lib.rs` 的 `pub mod error;` 之后加一行：

```rust
pub mod modes;
```

Run: `cargo test -p rstore-common modes`
Expected: 2 passed。

- [ ] **Step 3: 给 `ErasureSet` 加模式**

在 `crates/store/src/set.rs` 顶部的 `use rstore_disk::DiskAPI;` 之前加：

```rust
use rstore_common::modes::IoModes;
```

把 `ErasureSet` 结构体（第 47-52 行）改成：

```rust
pub struct ErasureSet {
    disks: Vec<Option<Arc<dyn DiskAPI>>>,
    data: u8,   // = total - parity
    parity: u8, // = total - data
    codec_cache: CodecCache,
    /// 读写路径的模式开关。基准专用，量完即删（设计文档 §10）。
    modes: IoModes,
}
```

把 `pub fn new(..)`（第 59 行，连同它上方的文档注释）整段替换成：

```rust
    /// **旧路径**：四个机制全关。
    ///
    /// 签名与语义都与引入模式开关之前完全一致，因此所有现存调用点（含全部既有
    /// 测试）一个字节都不用改。**「旧模式」的定义就是这个构造函数。**
    pub fn new(disks: Vec<Option<Arc<dyn DiskAPI>>>, parity: u8) -> Result<Self, StoreError> {
        Self::with_modes(disks, parity, IoModes::default())
    }

    /// 指定模式构造。`disks.len()` 即 `total = data + parity`；
    /// `parity` 必须 `< total`，否则没有数据分片，任何编码都无意义。
    ///
    /// `Vec` 里的 `None` 表示该槽位的盘已经掉线（这类槽位仍占据一个分片下标）。
    ///
    /// **构造期的校验一条都不因为模式而放松**：这两个判据是几何正确性的前提。
    pub fn with_modes(
        disks: Vec<Option<Arc<dyn DiskAPI>>>,
        parity: u8,
        modes: IoModes,
    ) -> Result<Self, StoreError> {
        let len = disks.len();
        // `total` 以 u8 参与几何运算；超过 255 块盘无法表示，直接在构造期拒绝，
        // 胜过把截断后的盘数带到读取/编码路径里再出问题。
        let total = u8::try_from(len).map_err(|_| {
            StoreError::Internal(format!("erasure set has {len} disks, exceeds u8::MAX"))
        })?;
        if parity >= total {
            return Err(StoreError::Internal(format!(
                "parity {parity} must be < total {total}: no data shards left"
            )));
        }
        Ok(Self {
            disks,
            data: total - parity,
            parity,
            codec_cache: CodecCache::new(CODEC_CACHE_CAPACITY),
            modes,
        })
    }

    /// 本 set 的模式开关。
    pub fn modes(&self) -> IoModes {
        self.modes
    }
```

- [ ] **Step 4: 在 `set.rs` 的测试模块里加一条**

在 `crates/store/src/set.rs` 的 `mod tests` 里，`rejects_empty_set` 之后加：

```rust
    /// `new` 就是「全关」，`with_modes` 原样保留模式，且**几何校验在两处都生效**。
    /// 最后两条尤其重要：新路径不该因为「只是多了个开关」就少掉一道几何闸。
    #[test]
    fn new_is_old_mode_and_with_modes_keeps_geometry_checks() {
        let set = ErasureSet::new(vec![None; 6], 2).unwrap();
        assert_eq!(set.modes(), IoModes::default());

        let set = ErasureSet::with_modes(vec![None; 6], 2, IoModes::ALL).unwrap();
        assert_eq!(set.modes(), IoModes::ALL);
        assert_eq!(set.data(), 4, "模式不该影响几何");
        assert_eq!(set.read_quorum(), 4);

        assert!(ErasureSet::with_modes(vec![None; 2], 2, IoModes::ALL).is_err());
        assert!(ErasureSet::with_modes(vec![], 0, IoModes::ALL).is_err());
    }
```

- [ ] **Step 5: 加夹具 `set_with_modes`**

在 `crates/store/src/testutil.rs` 顶部把 `use crate::set::ErasureSet;` 之前加：

```rust
use rstore_common::modes::IoModes;
```

把 `set_with_disks`（第 86 行，连同它上方的文档注释）整段替换成：

```rust
/// [`set_with_modes`] 的旧模式特化。**`set_with_disks(6, 2)` 读作「6 块盘、parity=2、
/// data=4、四个机制全关」**——整个 M4 的测试都用这个约定。
///
/// 它就是「旧模式」在夹具层的定义：`set_with_disks` 与
/// `set_with_modes(.., IoModes::default())` 必须永远等价，由
/// `set_with_disks_is_old_mode` 钉住。
pub async fn set_with_disks(total: u8, parity: u8) -> TestSet {
    set_with_modes(total, parity, IoModes::default()).await
}

/// 建 `total` 块盘、`parity` 为 `parity` 的 set，并指定读写路径模式。
///
/// 盘 `i` 的根是独立子目录 `{tmp}/disk{i}`：同根的话 6 块「盘」其实是同一个目录，
/// 「6 副本、掉 2 块还能读」这些性质会退化成同义反复，测试全绿却什么都没测到。
pub async fn set_with_modes(total: u8, parity: u8, modes: IoModes) -> TestSet {
    let dir = tempfile::TempDir::new().expect("create tempdir");

    let mut disks: Vec<Option<Arc<dyn DiskAPI>>> = Vec::with_capacity(total as usize);
    let mut faulties: Vec<Arc<FaultyDisk>> = Vec::with_capacity(total as usize);
    for i in 0..total as usize {
        let root = dir.path().join(format!("disk{i}"));
        let inner = LocalDisk::open(&root, DiskId::new_v4()).expect("open local disk");
        let faulty = Arc::new(FaultyDisk::wrap(inner));
        faulties.push(Arc::clone(&faulty));
        // 具体类型 `Arc<FaultyDisk>` 在此处 unsize 成 `Arc<dyn DiskAPI>` 存入槽位。
        let erased: Arc<dyn DiskAPI> = faulty;
        disks.push(Some(erased));
    }

    let set = ErasureSet::with_modes(disks, parity, modes).expect("valid erasure set geometry");
    TestSet {
        set,
        faulties,
        _dir: dir,
    }
}
```

- [ ] **Step 6: 在 `testutil.rs` 的测试模块里加一条**

在 `crates/store/src/testutil.rs` 的 `mod tests` 末尾加：

```rust
    /// `set_with_disks` 必须恰好是「旧模式」。谁要是让默认夹具悄悄带上新模式，
    /// 整个 M4 的单测都会在测另一条路径，而所有断言照样是绿的。
    #[tokio::test]
    async fn set_with_disks_is_old_mode() {
        let set = set_with_disks(2, 0).await;
        assert_eq!(set.modes(), IoModes::default());

        let set = set_with_modes(2, 0, IoModes::ALL).await;
        assert_eq!(set.modes(), IoModes::ALL);
        assert_eq!(set.total(), 2, "模式不该影响几何");
    }
```

- [ ] **Step 7: 跑 store 的测试，确认旧路径没动**

Run: `cargo test -p rstore-store`
Expected: 全绿。**尤其** `set.rs` 里那 9 条既有几何测试一条不红——它们证明 `new` 的语义没变。

- [ ] **Step 8: 加 `--io-mode`**

在 `crates/server/src/config.rs` 顶部 `use rstore_store::set::default_parity;` 之后加：

```rust
use rstore_common::modes::IoModes;
```

在 `Config` 结构体里，`iam_dir` 字段之后加：

```rust
    /// 读写路径模式：`old` = 今天的实现；`new` = 四个加速机制全开。
    ///
    /// **默认 `old`**：新路径还没跑过完整接受度验证，不该在无人察觉时成为默认。
    /// 这是性能对比用的开关，基准做完后整体删除（见设计文档 §10）。
    ///
    /// **只暴露两档**，不做逐位开关：CLI 面上出现「半新半旧」的组合只会让
    /// 每一次测量都需要额外解释自己开的是哪几个机制。
    #[arg(long, value_name = "MODE", default_value = "old", value_parser = ["old", "new"])]
    pub(crate) io_mode: String,
```

在 `impl Config` 里，`iam_dir()` 方法之后加：

```rust
    /// 解析成 [`IoModes`]。
    ///
    /// clap 的 `value_parser` 已经把取值限死在 `old` / `new`，所以这个 `expect`
    /// 是**可达性断言**而不是错误处理：真走到 `None` 说明 `arg` 属性被改坏了。
    pub(crate) fn modes(&self) -> IoModes {
        IoModes::from_io_mode(&self.io_mode).expect("clap value_parser 已把取值限死为 old|new")
    }
```

- [ ] **Step 9: 修既有测试的构造字面量并加新测试**

`config.rs` 的 `mod tests` 里，`fn cfg(..)` 的字面量在 `console: false,` 之后加一行：

```rust
            io_mode: "old".into(),
```

**`startup.rs` 的测试里还有第二个 `Config` 字面量**（`fn make_config`，约第 405 行）——
加了必填字段之后它也会编译不过。同样在 `iam_dir: None,` 之后加一行
`io_mode: "old".into(),`。**别漏**：这条只有真跑 `cargo clippy --all-targets`
才会暴露（`lib` 构建看不见 `#[cfg(test)]`）。

在 `mod tests` 末尾加：

```rust
    /// 不指定时必须是 `old`（全关）——这条钉住「默认行为与今天一致」。
    #[test]
    fn io_mode_defaults_to_old_and_new_switches_everything_on() {
        let mut c = cfg(vec![PathBuf::from("/d1")], None);
        assert_eq!(c.modes(), IoModes::default());

        c.io_mode = "new".into();
        assert_eq!(c.modes(), IoModes::ALL);
    }
```

- [ ] **Step 10: 两处构造点传模式**

`crates/server/src/startup.rs` 第 253 行，把：

```rust
        let set = Arc::new(
            ErasureSet::new(disks, cfg.parity())
                .map_err(|e| anyhow!("构造 erasure set 失败: {e}"))?,
        );
```

改成：

```rust
        let set = Arc::new(
            ErasureSet::with_modes(disks, cfg.parity(), cfg.modes())
                .map_err(|e| anyhow!("构造 erasure set 失败: {e}"))?,
        );
```

第 312 行，把：

```rust
    let set = Arc::new(
        ErasureSet::new(disks, cfg.parity()).map_err(|e| anyhow!("构造 erasure set 失败: {e}"))?,
    );
```

改成：

```rust
    let set = Arc::new(
        ErasureSet::with_modes(disks, cfg.parity(), cfg.modes())
            .map_err(|e| anyhow!("构造 erasure set 失败: {e}"))?,
    );
```

- [ ] **Step 11: 全量校验**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```

Expected: 全绿。手工再确认一次 CLI：

```bash
cargo run -p rstore-server -- --help | grep -A2 io-mode
```

Expected: 看到 `--io-mode <MODE>` 与默认值 `old`。

- [ ] **Step 12: 提交**

```bash
git add crates/common/src/modes.rs crates/common/src/lib.rs \
        crates/store/src/set.rs crates/store/src/testutil.rs \
        crates/server/src/config.rs crates/server/src/startup.rs
git commit -m "feat(store): IoModes 开关骨架，旧路径的定义就是 ErasureSet::new

四个机制（范围读/元数据缓存/有界列举/写入缓冲复用）各自一个具名 bool，
挂在 ErasureSet 上。new() 委托给 with_modes(.., default())，所以
「旧模式」与改动前的行为之间不存在第二处差异，现存调用点一个字节不动。

CLI 只暴露 old|new 两档，默认 old。

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Task 2: `BitrotShardReader::read_range`

**Files:**
- Modify: `crates/store/src/reader.rs:46-98`

- [ ] **Step 1: 写失败测试**

在 `crates/store/src/reader.rs` 的 `mod tests` 里，`missing_file_is_not_found` 之后加：

```rust
    // ---- read_range：范围读 ----
    // BS = 1024、PAYLOAD_LEN = 1500 → 分片有 2 块（第 0 块 1024 字节，末块 476 字节）。

    /// 范围读必须与整份读的对应切片**逐字节相同**——这是范围读唯一能拿来对齐的参照物。
    /// 穷举所有 `(first, last)` 组合，把单块、跨块、含末块三种情形都走到。
    #[tokio::test]
    async fn read_range_matches_the_slice_of_read_all() {
        let (_tmp, disk) = temp_disk();
        write_shard(&disk).await;
        let full = reader(Arc::clone(&disk)).read_all().await.unwrap();
        assert_eq!(full.len(), PAYLOAD_LEN);

        let n = PAYLOAD_LEN.div_ceil(BS);
        assert_eq!(n, 2, "本测试的分片几何依赖它是 2 块");
        for first in 0..n {
            for last in first..n {
                let got = reader(Arc::clone(&disk))
                    .read_range(first, last)
                    .await
                    .unwrap();
                let lo = first * BS;
                let hi = ((last + 1) * BS).min(PAYLOAD_LEN);
                assert_eq!(got, full[lo..hi], "range {first}..={last}");
            }
        }
    }

    /// **范围外的位腐不该被范围读发现**——这是设计文档 §8 声明出来的语义变化。
    ///
    /// 同一次损坏下 `read_all` 必须失败，否则这条测试什么都没钉住：它要证的不是
    /// 「范围读很宽松」，而是「范围读校验的范围正比于它读到的范围」。
    #[tokio::test]
    async fn read_range_ignores_bitrot_outside_the_range() {
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;

        // 第 1 块的数据字节：跳过第 0 块的 [摘要(32) + 数据(1024)]，再跳过第 1 块自己的摘要。
        let at = (HASH_LEN + BS) + HASH_LEN;
        let path = tmp.path().join("part.1");
        let mut raw = std::fs::read(&path).unwrap();
        raw[at] ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();

        let r = reader(Arc::clone(&disk)).read_range(0, 0).await;
        assert!(r.is_ok(), "范围外的位腐不该被范围读发现，got {r:?}");

        let full = reader(Arc::clone(&disk)).read_all().await;
        assert!(
            matches!(full, Err(DiskError::Corrupt(CorruptKind::BitrotMismatch))),
            "整份读必须失败，否则这条测试是空的，got {full:?}"
        );
    }

    /// 范围**内**的位腐必须被抓到：范围读省的是校验**范围**，不是校验本身。
    #[tokio::test]
    async fn read_range_detects_bitrot_inside_the_range() {
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;

        let path = tmp.path().join("part.1");
        let mut raw = std::fs::read(&path).unwrap();
        raw[HASH_LEN] ^= 0xFF; // 第 0 块的数据字节
        std::fs::write(&path, &raw).unwrap();

        let r = reader(Arc::clone(&disk)).read_range(0, 0).await;
        assert!(
            matches!(r, Err(DiskError::Corrupt(CorruptKind::BitrotMismatch))),
            "got {r:?}"
        );
    }

    /// **整份文件的长度检查在范围读上一条都不能少**：只读几个块，也必须先确认
    /// 文件总长恰好等于 `bitrot_size`。少了它，一个被截断的文件会在范围读里
    /// 静默返回一段合法-looking 的数据。
    #[tokio::test]
    async fn read_range_keeps_the_whole_file_length_check() {
        // 截断 → Transient(ShortRead)，**不是** Corrupt。
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;
        let path = tmp.path().join("part.1");
        let raw = std::fs::read(&path).unwrap();
        std::fs::write(&path, &raw[..raw.len() - 10]).unwrap();
        let r = reader(Arc::clone(&disk)).read_range(0, 0).await;
        assert!(
            matches!(r, Err(DiskError::Transient(TransientKind::ShortRead))),
            "got {r:?}"
        );

        // 超长 → Corrupt(LengthMismatch)。
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;
        let path = tmp.path().join("part.1");
        let mut raw = std::fs::read(&path).unwrap();
        raw.extend_from_slice(&[0xFF; 10]);
        std::fs::write(&path, &raw).unwrap();
        let r = reader(Arc::clone(&disk)).read_range(0, 0).await;
        assert!(
            matches!(r, Err(DiskError::Corrupt(CorruptKind::LengthMismatch))),
            "got {r:?}"
        );

        // 不存在 → NotFound（参与 quorum 时计为「缺失」而非「失败」）。
        let (_tmp, disk) = temp_disk();
        let r = reader(disk).read_range(0, 0).await;
        assert!(matches!(r, Err(DiskError::NotFound)), "got {r:?}");
    }

    /// **空分片上的范围读是契约违规，不是空结果**：0 块的分片没有任何合法区间，
    /// 按 `read_range` 的文档契约用 `assert` 爆掉。
    ///
    /// 这条把**契约本身**钉住：谁把那个 `assert` 换成 `return Ok(Vec::new())`，
    /// 越界就会变成一段安静返回的空数据，而调用方分不清它和「真的读到 0 字节」。
    /// 引擎不会走到这一格——`read_shards` 在 `size == 0` 时提前返回，
    /// 所以那里 `n >= 1`，区间恒有解。
    #[tokio::test]
    #[should_panic(expected = "exceeds")]
    async fn read_range_on_an_empty_shard_is_a_contract_violation() {
        let (_tmp, disk) = temp_disk();
        let w = BitrotShardWriter::new(Arc::clone(&disk), "part.1".into(), BS);
        w.finish().await.unwrap(); // 空 payload：落成 0 字节文件

        // 注意这里必须用 `shard_len = 0` 的读取器，**不是**上面测试用的那个
        // `PAYLOAD_LEN` 版本：文件是 0 字节，而 `PAYLOAD_LEN` 的期望长度是
        // `bitrot_size(1500, 1024) = 1564`，长度检查会先报 `ShortRead` 把它拦下，
        // 根本走不到块区间那一步。
        assert_eq!(
            BitrotShardReader::new(Arc::clone(&disk), "part.1".into(), BS, 0)
                .read_all()
                .await
                .unwrap(),
            Vec::<u8>::new(),
            "空分片的整份读仍然必须是空"
        );

        // 0 块的分片，任何区间都不合法。
        let _ = BitrotShardReader::new(disk, "part.1".into(), BS, 0)
            .read_range(0, 0)
            .await;
    }

    /// 反向的契约：`first > last` 同样必须爆掉，而不是安静地什么都不读。
    #[tokio::test]
    #[should_panic(expected = "reversed")]
    async fn read_range_with_a_reversed_interval_is_a_contract_violation() {
        let (_tmp, disk) = temp_disk();
        write_shard(&disk).await;
        let _ = reader(disk).read_range(1, 0).await;
    }
```

- [ ] **Step 2: 跑测试确认编译不过**

Run: `cargo test -p rstore-store reader::`
Expected: 编译错误 `no method named 'read_range'`。

- [ ] **Step 3: 实现**

把 `crates/store/src/reader.rs` 里 `impl BitrotShardReader` 的 `read_all`（第 46-98 行，含它上面那段文档注释）整段替换成：

```rust
    /// 该分片按 `block_size` 切出的块数。0 表示空分片（`shard_len == 0`）。
    fn block_count(&self) -> usize {
        self.shard_len.div_ceil(self.block_size as u64) as usize
    }

    /// 第 `k` 块的明文长度。**只有最后一块可能短**，其余都是满 `block_size`。
    fn block_data_len(&self, k: usize) -> usize {
        if k + 1 == self.block_count() {
            (self.shard_len - k as u64 * self.block_size as u64) as usize
        } else {
            self.block_size
        }
    }

    /// `stat` + **整份文件**的长度检查。`read_all` 与 `read_range` 的共同前置。
    ///
    /// 判定顺序是刻意的，不可调换：
    /// 1. 先 `stat`：不存在 → `NotFound`（参与 quorum 时计为「缺失」而非失败）。
    /// 2. 再比对**文件总长**。短 → `Transient(ShortRead)`（很可能只是写入未完成，
    ///    报成 `Corrupt` 会把它统计进损坏、进而触发 heal）；长 → `Corrupt(LengthMismatch)`
    ///    （多出来的字节没人能解释）。
    ///
    /// **范围读也照样走这一整套**：它收窄的只是 `read_exact_at` 的区间，不是
    /// 「文件总长必须恰好等于 [`bitrot_size`]」这条判据。少了它，一个被截断的文件
    /// 会在只读前几个块的场景下静默返回一段合法-looking 的数据。
    async fn checked_stat(&self) -> Result<u64, DiskError> {
        let stat = self.disk.stat(&self.rel_path).await?;
        let Some(stat) = stat else {
            return Err(DiskError::NotFound);
        };

        // 期望长度与写入器同源：两边都走 `bitrot_size`，不会各自漂移。
        let expected = bitrot_size(self.shard_len, self.block_size as u64);
        if stat.size < expected {
            return Err(DiskError::Transient(TransientKind::ShortRead));
        }
        if stat.size > expected {
            return Err(DiskError::Corrupt(CorruptKind::LengthMismatch));
        }
        Ok(expected)
    }

    /// 读并校验**半开区间** `[first, last)` 内的块，返回它们拼起来的明文。
    ///
    /// 只 `read_exact_at` 这些块实际占用的那段字节：其余块既不入内存，也不重算摘要。
    /// `first == last`（空区间，含空分片）返回空 `Vec`，**一次 IO 都不发**。
    async fn read_blocks(&self, first: usize, last: usize) -> Result<Vec<u8>, DiskError> {
        assert!(first <= last, "block range {first}..{last} is reversed");
        let n = self.block_count();
        assert!(last <= n, "block range {first}..{last} exceeds {n} blocks");
        if first == last {
            return Ok(Vec::new());
        }

        let stride = HASH_LEN + self.block_size;
        let start = first * stride;
        // 末块的结束偏移要按它**实际**的长度算：一律用满 `stride` 会越过文件尾。
        // 这是范围读与整份读在偏移上唯一一处差别，也是唯一容易写错的地方。
        let end = last * stride - self.block_size + self.block_data_len(last - 1);
        let raw = self
            .disk
            .read_exact_at(&self.rel_path, start as u64, end - start)
            .await?;

        // 偏移一律相对 **本次读到的这段** 计算，与整份读的相对偏移由 `first` 平移。
        let mut out = Vec::with_capacity(end - start);
        for k in first..last {
            let off = (k - first) * stride;
            let digest = &raw[off..off + HASH_LEN];
            let data_len = self.block_data_len(k);
            let data = &raw[off + HASH_LEN..off + HASH_LEN + data_len];
            // 比对**重算的数据哈希**与落盘摘要。破坏数据字节必被这里抓出。
            if bitrot_hash(data)[..] != digest[..] {
                return Err(DiskError::Corrupt(CorruptKind::BitrotMismatch));
            }
            out.extend_from_slice(data);
        }
        Ok(out)
    }

    /// 读回整份分片并逐块校验，返回 `shard_len` 字节的原始数据。
    ///
    /// 就是 `read_blocks(0, n)`——长度检查与逐块校验都在那条共用路径上。
    /// `shard_len == 0` 时 `n == 0`，直接落回空 `Vec`，不 panic。
    pub async fn read_all(&self) -> Result<Vec<u8>, DiskError> {
        self.checked_stat().await?;
        let n = self.block_count();
        self.read_blocks(0, n).await
    }

    /// 只读第 `first_block..=last_block` 块（**含两端**）并逐块校验，返回这些块的明文。
    ///
    /// **跳过的块不做 bitrot 校验**——位腐检测的成本正比于真正读到的范围
    /// （见设计文档 §8）。这是刻意的语义变化，不是遗漏：完整读会因为块 7 的损坏
    /// 丢掉整块盘的分片，读块 1 的范围读不会。所以范围读的可用性是**单向变好**的。
    ///
    /// 调用方必须保证 `first_block <= last_block` 且 `last_block < block_count()`。
    /// 越界是程序 bug，用 `assert` 在最早处爆掉，胜过返回一段偏移错位的数据。
    pub async fn read_range(
        &self,
        first_block: usize,
        last_block: usize,
    ) -> Result<Vec<u8>, DiskError> {
        assert!(
            first_block <= last_block,
            "block range {first_block}..={last_block} is reversed"
        );
        self.checked_stat().await?;
        self.read_blocks(first_block, last_block + 1).await
    }
```

- [ ] **Step 4: 跑全部 reader 测试**

Run: `cargo test -p rstore-store reader::`
Expected: 既有 6 条 + 新增 6 条全绿。

**特别确认既有那 6 条一条不红**——`read_all` 是重写过实现的老函数，那 6 条是它行为不变的唯一证据。

- [ ] **Step 5: 跑全量**

Run: `cargo clippy --workspace --all-targets -- -D warnings && cargo test -p rstore-store`
Expected: 全绿。

- [ ] **Step 6: 提交**

```bash
git add crates/store/src/reader.rs
git commit -m "feat(store): BitrotShardReader::read_range，只读命中的块区间

抽出 checked_stat / read_blocks 两条共用路径：read_all 成为
read_blocks(0, n)，read_range 成为 read_blocks(first, last+1)。
整份文件的长度检查留在共用路径上一条不少——收窄的只是 read_exact_at
的区间，不是「文件总长必须等于 bitrot_size」这条判据。

跳过的块不重算摘要，这是设计文档 §8 声明出来的语义变化。

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Task 3: `ranged_shard_read` 接线到 `get.rs`

**Files:**
- Modify: `crates/store/src/get.rs:253`（`read_shards`，含上方文档注释）、`:384`（`get_object`）

- [ ] **Step 1: 写失败测试**

在 `crates/store/src/get.rs` 的 `mod tests` 里，`get_with_range_returns_the_right_slice` 之后加：

```rust
    /// 开了范围读之后，**结果必须与全量读逐字节相同**。
    /// 区间刻意覆盖三种块边界：首块内、跨块边界、末块单字节、整份。
    #[tokio::test]
    async fn ranged_get_matches_full_get() {
        // 3_000_000 = 2 个满块 + 一个 942_592 字节的末块（不是 251 的整数倍）。
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        let ranges = [
            (0u64, 0u64),
            (0, 99),
            (crate::put::BLOCK_SIZE as u64 - 1, crate::put::BLOCK_SIZE as u64 + 1),
            (1_000_000, 2_000_000),
            (2_999_999, 2_999_999),
        ];

        for (start, end) in ranges {
            let plain = set_with_modes(6, 2, IoModes::default()).await;
            plain
                .put_object(put_args("b", "k", data.clone()))
                .await
                .unwrap();

            let ranged = set_with_modes(6, 2, IoModes::ALL).await;
            ranged
                .put_object(put_args("b", "k", data.clone()))
                .await
                .unwrap();

            let expect = &data[start as usize..=end as usize];
            let a = plain
                .get_object("b", "k", Some(ByteRange { start, end }))
                .await
                .unwrap();
            let b = ranged
                .get_object("b", "k", Some(ByteRange { start, end }))
                .await
                .unwrap();
            assert_eq!(a.data, expect, "旧模式 {start}-{end}");
            assert_eq!(b.data, expect, "新模式 {start}-{end}");
            assert_eq!(a.size, b.size);
            assert_eq!(a.etag, b.etag);
        }
    }

    /// 新模式下的 **HEAD 与整读**也必须一个字不变——本次只动范围读那条分支。
    #[tokio::test]
    async fn new_mode_leaves_full_get_and_head_alone() {
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        let set = set_with_modes(6, 2, IoModes::ALL).await;
        set.put_object(put_args("b", "k", data.clone()))
            .await
            .unwrap();

        let got = set.get_object("b", "k", None).await.unwrap();
        assert_eq!(got.data, data);
        assert_eq!(got.size, 3_000_000);

        let head = set.head_object("b", "k").await.unwrap();
        assert_eq!(head.size, got.size);
        assert_eq!(head.etag, got.etag);
    }
```

改 `get.rs` 测试模块的 import，把：

```rust
    use crate::testutil::{body, set_with_disks};
```

换成：

```rust
    use rstore_common::modes::IoModes;

    use crate::testutil::{body, set_with_disks, set_with_modes};
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p rstore-store get::ranged_get_matches_full_get`
Expected: FAIL——新模式此时与旧模式相同，`ranged_get_matches_full_get` 其实会**通过**（因为 `read_range` 还没接线）。**所以这一步改用另一条证据**：

Run: `cargo test -p rstore-store get::new_mode_leaves_full_get_and_head_alone`
Expected: PASS。这两条现在都是绿的——它们钉的是**接线之后**不许改坏的东西。真正的失败要等逻辑写完之后才可能被抓住，接受这一点：本任务的测试是**回归护栏**，前置绿灯是正常的。

- [ ] **Step 3: 改 `read_shards` 认块区间**

把 `crates/store/src/get.rs` 里 `read_shards` **从它上方的文档注释起、到函数收尾的 `}` 为止**
（第 249-381 行那一段）整段替换成：

```rust
/// 分片分支：读回每块盘的 `part.1`，解码重建 `blocks` 指定的块区间。
///
/// `blocks = None` 表示**全部块**——即今天的旧路径，一字不变。
/// `blocks = Some((first, last))`（闭区间）表示只要这几块，此时每块盘也只读这几块。
/// **模式判断在调用方**（`get_object`），本函数只认区间。
///
/// **一块盘的读取失败（含 `Corrupt(BitrotMismatch)`）绝不中止整次读取**，
/// 只是把该盘的槽位置成 `None`，交给纠删码用校验分片补回来——这正是纠删码存在的意义。
/// 只有可用槽位 `< read_quorum` 才是 `ReadQuorum`。
async fn read_shards(
    set: &ErasureSet,
    key_rel: &str,
    winner_dir: &str,
    header: &FileVersionHeader,
    body: &ObjectBody,
    blocks: Option<(usize, usize)>,
) -> Result<Vec<u8>, StoreError> {
    let size = header.size;
    if size == 0 {
        return Ok(Vec::new());
    }
    let data = header.ec_m;
    let parity = header.ec_n.saturating_sub(header.ec_m);
    let total = header.ec_n as usize;
    if data == 0 || parity == 0 || total != set.total() as usize {
        return Err(StoreError::ShardLayout(format!(
            "shard geometry ({data}+{parity}={total}) does not match set total {}",
            set.total()
        )));
    }
    // `ec_dist` 必须是一份合法排列：它不是的话，分片号 ↔ 盘号的映射就是错的，
    // 而错的映射在长度恰好对得上时会**安静地返回错数据**。
    if body.ec_dist.len() != total
        || !rstore_meta::distribution::is_valid_distribution(&body.ec_dist)
    {
        return Err(StoreError::ShardLayout(format!(
            "invalid ec_dist {:?} for total {total}",
            body.ec_dist
        )));
    }

    let step = shard_step(size, data) as usize;
    let shard_len = expected_shard_len(size, data);
    // `size > 0` 已由上面的提前返回保证，所以 `n >= 1`——`n - 1` 不会下溢。
    let n = size.div_ceil(BLOCK_SIZE as u64) as usize;
    let part_rel = format!("{key_rel}/{winner_dir}/part.1");

    // 闭区间的两端。`None` 就是「第 0 块到末块」——与今天完全一致。
    let (first_block, last_block) = blocks.unwrap_or((0, n - 1));
    if first_block > last_block || last_block >= n {
        return Err(StoreError::Internal(format!(
            "block range {first_block}..={last_block} is invalid for {n} blocks"
        )));
    }

    // 盘 `d` 持分片 `j` ⟺ `ec_dist[j] == d + 1`。反过来建表：盘号 → 分片号。
    let mut shard_of_disk = vec![usize::MAX; total];
    for (j, &d1) in body.ec_dist.iter().enumerate() {
        shard_of_disk[(d1 - 1) as usize] = j;
    }

    // 每块盘读一次；失败只记 None，不中止整次读取。
    // `blocks = None` → 整份分片（旧路径）；`Some` → 只读命中的块。
    let mut payloads: Vec<Option<Vec<u8>>> = Vec::with_capacity(total);
    for slot in set.disks() {
        let payload = match slot {
            Some(disk) => {
                let r = BitrotShardReader::new(Arc::clone(disk), part_rel.clone(), step, shard_len);
                match blocks {
                    None => r.read_all().await,
                    Some((fb, lb)) => r.read_range(fb, lb).await,
                }
                .ok()
            }
            None => None,
        };
        payloads.push(payload);
    }
    let available = payloads.iter().filter(|p| p.is_some()).count() as u8;
    let read_quorum = set.read_quorum();
    if available < read_quorum {
        return Err(StoreError::ReadQuorum {
            achieved: available,
            required: read_quorum,
        });
    }

    // 本函数自己的不变量：读到了不一致的长度却照常返回，就是在静默丢数据。
    // 全量读时期望 `size`；区间读时期望该区间覆盖的对象字节数。
    let expect_len = (((last_block + 1) as u64) * BLOCK_SIZE as u64)
        .min(size)
        - first_block as u64 * BLOCK_SIZE as u64;
    // 盘读到的那段是从第 `first_block` 块的头开始的，所以切片下标要减掉这个基准。
    // 全量读时它是 0，即今天的行为。
    let base = first_block * step;

    let mut out: Vec<u8> = Vec::with_capacity(expect_len as usize);
    for k in first_block..=last_block {
        // 块下标现在是 `usize`（`n` 也是），所以偏移一律在 `usize` 里算；
        // `shard_len` 是 `u64`（`expected_shard_len` 的返回类型），这里收窄一次。
        let lo = k * step;
        let hi = ((k + 1) * step).min(shard_len as usize);
        let shard_size_k = hi - lo;
        if shard_size_k == 0 {
            return Err(StoreError::Internal(format!(
                "empty shard slice at block {k} (size {size}, step {step})"
            )));
        }

        // **槽位下标是分片号，不是盘号**：盘 `d` 的这段分片落在槽位 `shard_of_disk[d]`。
        // 把盘号当分片号写进去，正常路径下会以 `UnequalShardLength` 或错误的解码结果收场——
        // 而后者如果恰好长度对得上，就会安静地返回错数据。
        let mut slots: Vec<Option<Vec<u8>>> = vec![None; total];
        for (d, payload) in payloads.iter().enumerate() {
            if let Some(payload) = payload {
                let j = shard_of_disk[d];
                if j != usize::MAX {
                    let rel_lo = lo - base;
                    slots[j] = Some(payload[rel_lo..rel_lo + shard_size_k].to_vec());
                }
            }
        }

        let codec = set
            .codec_cache()
            .get(data as usize, parity as usize, shard_size_k)
            .map_err(|e| {
                StoreError::Internal(format!(
                    "codec geometry ({data}, {parity}, {shard_size_k}): {e}"
                ))
            })?;
        let data_shards = codec
            .decode(&slots)
            .map_err(|e| StoreError::Internal(format!("erasure decode: {e}")))?;

        // 补齐的零只在最后一个数据分片的尾部：顺序相接后截断到**本块真实长度**。
        let block_len = (size - k as u64 * BLOCK_SIZE as u64).min(BLOCK_SIZE as u64) as usize;
        let mut block = Vec::with_capacity(data_shards.len() * shard_size_k);
        for shard in &data_shards {
            block.extend_from_slice(shard);
        }
        if block.len() < block_len {
            return Err(StoreError::Internal(format!(
                "decoded block {k} is {} bytes, shorter than expected {block_len}",
                block.len()
            )));
        }
        out.extend_from_slice(&block[..block_len]);
    }

    if out.len() as u64 != expect_len {
        return Err(StoreError::Internal(format!(
            "reassembled {} bytes but blocks {first_block}..={last_block} should hold {expect_len}",
            out.len()
        )));
    }
    Ok(out)
}
```

- [ ] **Step 4: 加裁剪函数**

在 `crates/store/src/get.rs` 的 `apply_range` 之后加：

```rust
/// 区间读产物的裁剪：`buf` 是从第 `first_block` 块的头开始的一段，
/// 按 `Range` 把两端修掉。
///
/// 与 [`apply_range`] 的区别只有基准偏移：后者的 `buf` 从对象头开始。
/// 两者一样，**走到这里还越界就是 M5 的 bug**，不静默吸收。
fn slice_blocks(
    buf: Vec<u8>,
    size: u64,
    range: Option<ByteRange>,
    first_block: usize,
) -> Result<Vec<u8>, StoreError> {
    let Some(ByteRange { start, end }) = range else {
        return Err(StoreError::Internal(
            "slice_blocks called without a range".into(),
        ));
    };
    if start > end || end >= size {
        return Err(StoreError::Internal(format!(
            "range {start}-{end} out of bounds for size {size}"
        )));
    }
    // `first_block = start / BLOCK_SIZE`（由调用方保证），所以 `start >= base` 恒成立。
    let base = first_block as u64 * BLOCK_SIZE as u64;
    let lo = (start - base) as usize;
    let hi = (end - base) as usize + 1;
    if hi > buf.len() {
        return Err(StoreError::Internal(format!(
            "range {start}-{end} exceeds materialized block span {}",
            buf.len()
        )));
    }
    Ok(buf[lo..hi].to_vec())
}
```

- [ ] **Step 5: 接线到 `get_object`**

把 `get.rs` 里 `get_object` **从上方的文档注释起，到 `Ok(GetOut {` 之前为止**（第 382-455 行那一段）替换成：

```rust
    /// `range` 为 `None` 时返回整个对象。
    ///
    /// **旧模式（`IoModes::default()`）不做流式**：Range 仍会把整份分片读进来、
    /// 把所有块解码出来，最后才切出 `[start, end]`（省的是网络与 S3 层的内存，
    /// 没省磁盘 IO）——这段描述依然准确，只是有了新路径可选。
    ///
    /// **新模式（`ranged_shard_read`）**：把 Range 折算成块区间，每块盘只读这几块。
    /// 省下的是磁盘 IO 与内存，代价是**被跳过的块不再做 bitrot 校验**（设计文档 §8）。
    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<GetOut, StoreError> {
        // `live()` 借用 `resolved`，所以必须先绑定再 let-else；不能写成
        // `resolve_version(…).await?.live()`——那是借一个临时值。
        let resolved = resolve_version(self, bucket, key).await?;
        let Some((winner_dir, meta)) = resolved.live() else {
            return Err(StoreError::NotFound);
        };
        let key_rel = format!("{bucket}/{key}");
        let latest = latest_version(meta).ok_or_else(|| {
            StoreError::Internal(format!("winner metadata for {key_rel} has no versions"))
        })?;
        let header = latest.header.clone();
        let size = header.size;

        // body 无论哪条分支都要解析：它既承载 `ec_dist` / `parts`，也是
        // 「flags 没标内联但 meta_sys 标了」那条兜底判据的来源。
        let body = decode_body(&latest.body)?;

        // 内联分支：**一次都不碰 `part.*`**。版本键：无版本化桶是 `"null"`。
        // 内联对象本来就全在内存里，范围读对它没有可省的东西，两条模式走同一条路。
        let is_inline = header.flags.contains(Flags::INLINE_DATA)
            || body.meta_sys.contains_key(keys::INLINE_DATA);
        if is_inline {
            let full = meta
                .inline
                .get("null")
                .ok_or_else(|| {
                    StoreError::Internal(format!(
                        "version marked inline but holds no inline data for {key_rel}"
                    ))
                })?
                .to_vec();
            let data_dir = header.data_dir.ok_or_else(|| {
                StoreError::Internal(format!("inline version of {key_rel} has no data_dir"))
            })?;
            // etag 与 LIST 共用同一处算法（见 `etag_of_meta`）。
            let etag = etag_of_meta(meta)?;
            let data = apply_range(full, size, range)?;
            return Ok(GetOut {
                data,
                size,
                etag,
                data_dir,
                mod_time: header.mod_time.unwrap_or(0),
            });
        }

        // 分片分支。
        //
        // **模式判断只此一处**：开了范围读就把 Range 折算成块区间，关着就传 `None`
        // （= 今天的全量读）。折算只用已经算出来的 `BLOCK_SIZE`，不需要新的偏移计算。
        let blocks = range.and_then(|r| {
            if !self.modes().ranged_shard_read {
                return None;
            }
            let n = size.div_ceil(BLOCK_SIZE as u64);
            let first = r.start / BLOCK_SIZE as u64;
            let last = (r.end / BLOCK_SIZE as u64).min(n - 1);
            Some((first as usize, last as usize))
        });
        let buf = read_shards(self, &key_rel, winner_dir, &header, &body, blocks).await?;
        // etag 与 LIST 共用同一处算法（见 `etag_of_meta`）；`parts` 为空时它报 `Internal`。
        let etag = etag_of_meta(meta)?;
        let data_dir = header
            .data_dir
            .or(body.id)
            .ok_or_else(|| StoreError::Internal(format!("version of {key_rel} has no data_dir")))?;
        // 两条分支的裁剪基准不同：全量读从对象头开始，区间读从块边界开始。
        let data = match blocks {
            None => apply_range(buf, size, range)?,
            Some((first, _)) => slice_blocks(buf, size, range, first)?,
        };
```

（函数余下的 `Ok(GetOut { .. })` 部分不动。）

- [ ] **Step 6: 跑测试**

Run: `cargo test -p rstore-store get::`
Expected: 全绿，包括既有的 10 条与新增 2 条。

- [ ] **Step 7: 跑全量**

```bash
cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
```

Expected: 全绿。

- [ ] **Step 8: 提交**

```bash
git add crates/store/src/get.rs
git commit -m "feat(store): ranged_shard_read，范围读只读命中的块

read_shards 多一个 blocks 参数：None = 全部块（旧路径，一字不变），
Some((first,last)) = 只读这几块。模式判断只落在 get_object 一处。
新增 slice_blocks 负责区间读产物的两端裁剪，与 apply_range 的区别
只有基准偏移。

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Task 4: `metadata_cache`

**Files:**
- Create: `crates/store/src/resolve_cache.rs`
- Modify: `crates/store/src/lib.rs`
- Modify: `crates/store/src/set.rs`（`resolve_cache` 字段）
- Modify: `crates/store/src/get.rs`（`resolve_version_cached`）
- Modify: `crates/store/src/put.rs`（`put_object` 薄包装）
- Modify: `crates/store/src/delete.rs`（`delete_object` 薄包装）

- [ ] **Step 1: 写缓存本体**

创建 `crates/store/src/resolve_cache.rs`：

```rust
//! `resolve_version` 的结果缓存（`metadata_cache` 机制）。
//!
//! **为什么值得缓存**：[`resolve_version`](crate::get::resolve_version) 要对
//! **每块盘**做一次 `list_dir`（发现候选版本目录）再逐盘读 `meta.xl` 仲裁。
//! 一个只做 HEAD 的客户端因此把整条元数据路径走了 N 遍，而它问的问题在同一秒里
//! 往往是同一个。
//!
//! **怎么保证不读到过期的**：每个分片一个**单调递增**的计数器。`put_object` /
//! `delete_object` 提交成功后把该 key 所在分片的计数 +1；缓存条目记下自己写入时的
//! 计数值，命中要求两者相等。于是：
//!
//! - 写成功之后，该分片的所有旧条目立刻失效（宁可多解析一次，不可给旧值）；
//! - 读者「取计数 → 解析 → 存条目」这中间若发生了写入，存储时会发现手上的计数
//!   已经不等于当前值，于是**放弃写入**而不是塞一份旧的进去；
//! - 计数器**只增不减**，所以即使条目被淘汰后重建，也不会出现
//!   「旧读者与新建条目恰好同号」这种 ABA。
//!
//! **刻意做粗**：一次写入让整个分片的条目一起失效，而不是只失效那一个 key。
//! 这是基准专用代码，粗粒度换来的是「正确性一眼可验」——精确到 key 的代际表
//! 本身也要有界，两套结构保持同步的收益不抵它的复杂度。见设计文档 §9。
//!
//! **谁不走这里**：写路径（`delete.rs`）、对账（`reconcile.rs`）与列举
//! （`list.rs`）。它们要的是盘上的**真相**，不是「最近为真」。见设计文档 §4.2。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::get::Resolved;

/// 分片数。按 key 哈希打散、不搞全局锁（§16.1 的意图），取 16 个定值——
/// 这是单节点、开关式的缓存，不需要跟着 CPU 核数走。
const SHARDS: usize = 16;

/// 每个分片最多驻留多少条目。超了**整片清空**：淘汰只会造成 miss、不会造成错值，
/// 所以这里不需要真正的 LRU 语义。
const SHARD_CAPACITY: usize = 1024;

/// 缓存键：`(bucket, key)`。
type CacheKey = (String, String);
/// 缓存值：`(写入时的分片计数, 解析结果)`。
type CacheEntry = (u64, Arc<Resolved>);

struct Shard {
    /// 单调递增。**只增不减**——见模块文档里的 ABA 论证。
    generation: AtomicU64,
    entries: Mutex<HashMap<CacheKey, CacheEntry>>,
}

/// 按 `(bucket, key)` 缓存的版本解析结果。
pub(crate) struct ResolveCache {
    shards: Vec<Shard>,
}

impl ResolveCache {
    pub(crate) fn new() -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| Shard {
                    generation: AtomicU64::new(0),
                    entries: Mutex::new(HashMap::new()),
                })
                .collect(),
        }
    }

    fn shard(&self, bucket: &str, key: &str) -> &Shard {
        // 默认哈希器足够：这里要的是「打散」，不是抗碰撞（碰撞只影响命中率）。
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        bucket.hash(&mut h);
        key.hash(&mut h);
        &self.shards[(h.finish() as usize) % SHARDS]
    }

    /// 取当前计数。**必须在解析之前取**，解析完成后连同结果一起交给 [`Self::store`]。
    pub(crate) fn generation(&self, bucket: &str, key: &str) -> u64 {
        self.shard(bucket, key).generation.load(Ordering::SeqCst)
    }

    /// 命中则返回条目；计数对不上、或压根没有条目，都返回 `None`。
    pub(crate) fn get(&self, bucket: &str, key: &str, gen: u64) -> Option<Arc<Resolved>> {
        let shard = self.shard(bucket, key);
        let entries = shard.entries.lock().expect("resolve cache poisoned");
        let (stored_gen, value) = entries.get(&(bucket.to_string(), key.to_string()))?;
        (*stored_gen == gen).then(|| Arc::clone(value))
    }

    /// 存入解析结果。**只在计数没变过时才写**：变了说明解析期间发生过写入，
    /// 这份结果已经是旧的，塞进去就等于制造一个错值。
    pub(crate) fn store(&self, bucket: &str, key: &str, gen: u64, value: Arc<Resolved>) {
        let shard = self.shard(bucket, key);
        if shard.generation.load(Ordering::SeqCst) != gen {
            return;
        }
        let mut entries = shard.entries.lock().expect("resolve cache poisoned");
        if entries.len() >= SHARD_CAPACITY {
            entries.clear();
        }
        entries.insert((bucket.to_string(), key.to_string()), (gen, value));
    }

    /// 一次写入**成功之后**调用：让这个 key 所在分片的所有条目失效。
    pub(crate) fn invalidate(&self, bucket: &str, key: &str) {
        self.shard(bucket, key)
            .generation
            .fetch_add(1, Ordering::SeqCst);
    }
}
```

**（执行时补记）** `Shard::entries` 的类型若直接写成
`Mutex<HashMap<(String, String), (u64, Arc<Resolved>)>>`，`clippy` 的
`type_complexity` 会以 `-D warnings` 直接报错。上面已改为 `CacheKey` / `CacheEntry`
两个别名——不是风格问题，是这一关过不去。

- [ ] **Step 2: 注册模块并给 `ErasureSet` 加字段**

`crates/store/src/lib.rs`：在 `pub mod reconcile;` 之后加：

```rust
mod resolve_cache;
```

（**不是 `pub mod`**：这个类型只在 crate 内部用，`ErasureSet` 的对应字段也是私有的。）

`crates/store/src/set.rs`：顶部加：

```rust
use crate::resolve_cache::ResolveCache;
```

`ErasureSet` 结构体在 `modes` 字段之后加：

```rust
    /// 版本解析缓存。**只在 `metadata_cache` 打开时才存在**——
    /// 关掉模式时它是 `None`，`resolve_version_cached` 于是走原路径，一次都不碰缓存。
    resolve_cache: Option<ResolveCache>,
```

`with_modes` 的 `Ok(Self { .. })` 里，`modes,` 之前加：

```rust
            resolve_cache: modes.metadata_cache.then(ResolveCache::new),
```

在 `pub fn modes(&self)` 之后加：

```rust
    /// 版本解析缓存。`None` = 模式关着（或没开 `metadata_cache`）。
    pub(crate) fn resolve_cache(&self) -> Option<&ResolveCache> {
        self.resolve_cache.as_ref()
    }
```

- [ ] **Step 3: 加 `resolve_version_cached`**

在 `crates/store/src/get.rs` 里，`resolve_version` 函数之后加：

```rust
/// [`resolve_version`] 的缓存版，**只给 `get_object` / `head_object` 用**。
///
/// 模式关着的时候它一次都不碰缓存，直接走原路径——所以调用方不必自己判断模式，
/// **决策点就在这个 `let Some(..) else` 上，只有一处**。
///
/// **错误不进缓存**：`ReadQuorum` 是暂时状态，把它记下来只会让一次抖动变成持续失败。
/// `Absent` **进缓存**——「这个 key 不存在」正是 HEAD 密集负载里最有价值的一类命中，
/// 而它被写入改变时由 `invalidate` 兜住。
pub(crate) async fn resolve_version_cached(
    set: &ErasureSet,
    bucket: &str,
    key: &str,
) -> Result<Arc<Resolved>, StoreError> {
    let Some(cache) = set.resolve_cache() else {
        return Ok(Arc::new(resolve_version(set, bucket, key).await?));
    };
    let gen = cache.generation(bucket, key);
    if let Some(hit) = cache.get(bucket, key, gen) {
        return Ok(hit);
    }
    let fresh = Arc::new(resolve_version(set, bucket, key).await?);
    cache.store(bucket, key, gen, Arc::clone(&fresh));
    Ok(fresh)
}
```

**不需要**给 `get.rs` 加任何 import：`self.modes()` 是 `ErasureSet` 上的方法，
`.ranged_shard_read` 是字段访问，`IoModes` 这个类型名在 `get.rs` 里从头到尾没出现过。
（加了会立刻变成 unused import，而 `-D warnings` 会挡住。）

- [ ] **Step 4: 让 `get_object` / `head_object` 走缓存版**

`get_object` 里那一行：

```rust
        let resolved = resolve_version(self, bucket, key).await?;
```

改成：

```rust
        let resolved = resolve_version_cached(self, bucket, key).await?;
```

`head_object` 里那一行：

```rust
        let resolved = resolve_version(self, bucket, key).await?;
```

改成：

```rust
        let resolved = resolve_version_cached(self, bucket, key).await?;
```

`get_object` 里 `resolved.live()` 返回的 `meta` 现在来自 `Arc<Resolved>` 的解引用，
后续代码（`meta.inline.get(...)`、`etag_of_meta(meta)`）**不用改**——`Arc<T>` 的
`Deref` 让它们照常绑定到 `&ObjectMeta`。

- [ ] **Step 5: 写入成功后失效**

`crates/store/src/put.rs`：把第 207 行 `pub async fn put_object` 的**文档注释与签名行**
（连同它上方的文档注释）替换成：

```rust
    /// 写入一个对象并提交。达到 `write_quorum` 才算成功。
    ///
    /// **流式**：请求体逐块读入、逐块编码落盘，峰值内存与对象大小无关（上界
    /// `BLOCK_SIZE` + 一份分片）。
    ///
    /// 本函数是 [`Self::put_object_inner`] 的薄包装，只多一件事：**提交成功后
    /// 让元数据缓存里这个 key 作废**。放在唯一的成功出口上，两个返回分支
    /// （内联 / 分片）与将来新增的分支都不会漏掉它。
    pub async fn put_object(&self, args: PutArgs) -> Result<PutOut, StoreError> {
        let bucket = args.bucket.clone();
        let key = args.key.clone();
        let out = self.put_object_inner(args).await?;
        // **只在成功时失效**：失败（含低于 quorum）没有改变任何权威版本，
        // 此时清掉条目只会白白少一次命中。
        if let Some(cache) = self.resolve_cache() {
            cache.invalidate(&bucket, &key);
        }
        Ok(out)
    }

    /// 真正的写入逻辑。语义与签名都与引入缓存之前一致。
    async fn put_object_inner(&self, args: PutArgs) -> Result<PutOut, StoreError> {
```

（函数体从 `let PutArgs {` 开始到结尾一个字符都不动，只是缩进层级不变——它原本就是
`impl` 内的 4 空格缩进方法体。）

`crates/store/src/delete.rs`：把 `pub async fn delete_object` 的文档注释与签名行替换成：

```rust
    /// 写一枚删除标记并提交；`delete_quorum = N/2 + 1`。
    ///
    /// 对不存在的 key 也照样写标记并返回 `Ok`——这是 S3 的语义
    /// （DELETE 幂等，重复删同一 key、删一个从没存在过的 key 都成功）。
    ///
    /// 与 `put_object` 同构的薄包装：提交成功后让元数据缓存里这个 key 作废。
    /// 少了这一步，删除之后 HEAD 还会从缓存里读到那个已被标记删掉的对象。
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StoreError> {
        self.delete_object_inner(bucket, key).await?;
        if let Some(cache) = self.resolve_cache() {
            cache.invalidate(bucket, key);
        }
        Ok(())
    }

    /// 真正的删除逻辑。语义与签名都与引入缓存之前一致。
    async fn delete_object_inner(&self, bucket: &str, key: &str) -> Result<(), StoreError> {
```

原函数体的最后是一个 `Ok(())`，保留不动。

- [ ] **Step 6: 写行为测试**

在 `crates/store/src/get.rs` 的 `mod tests` 末尾加：

```rust
    /// 缓存必须**在覆盖写之后立刻失效**：读到旧版本就是错数据。
    #[tokio::test]
    async fn metadata_cache_sees_a_new_version_after_put() {
        let set = set_with_modes(6, 2, IoModes::ALL).await;
        set.put_object(put_args("b", "k", vec![1u8; 1_500_000]))
            .await
            .unwrap();
        // 先读一次把条目填上。
        assert_eq!(set.head_object("b", "k").await.unwrap().size, 1_500_000);

        set.put_object(put_args("b", "k", vec![2u8; 700_000]))
            .await
            .unwrap();
        assert_eq!(
            set.head_object("b", "k").await.unwrap().size,
            700_000,
            "覆盖写之后缓存必须失效"
        );
        assert_eq!(set.get_object("b", "k", None).await.unwrap().data, vec![2u8; 700_000]);
    }

    /// 删除之后必须立刻读到 `NotFound`，而不是缓存里的旧对象。
    #[tokio::test]
    async fn metadata_cache_sees_a_delete_immediately() {
        let set = set_with_modes(6, 2, IoModes::ALL).await;
        set.put_object(put_args("b", "k", vec![1u8; 1_500_000]))
            .await
            .unwrap();
        assert_eq!(set.head_object("b", "k").await.unwrap().size, 1_500_000);

        set.delete_object("b", "k").await.unwrap();
        let r = set.head_object("b", "k").await;
        assert!(matches!(r, Err(StoreError::NotFound)), "删除之后缓存必须失效，got {r:?}");
    }

    /// **不存在的 key 也要缓存**（HEAD 密集负载里那是最有价值的一类命中），
    /// 而且 PUT 之后必须立刻可见——否则缓存会把「不存在」永久钉住。
    #[tokio::test]
    async fn metadata_cache_caches_absent_and_uncaches_on_put() {
        let set = set_with_modes(6, 2, IoModes::ALL).await;
        assert!(matches!(
            set.head_object("b", "k").await,
            Err(StoreError::NotFound)
        ));

        set.put_object(put_args("b", "k", vec![1u8; 1_500_000]))
            .await
            .unwrap();
        assert_eq!(set.head_object("b", "k").await.unwrap().size, 1_500_000);
    }

    /// 读失败（`ReadQuorum`）**不进缓存**：一次抖动不该变成持续失败。
    /// 恢复一块盘之后必须立刻能读到。
    #[tokio::test]
    async fn metadata_cache_does_not_cache_errors() {
        let set = set_with_modes(6, 2, IoModes::ALL).await;
        set.put_object(put_args("b", "k", vec![1u8; 1_500_000]))
            .await
            .unwrap();

        for i in 0..4 {
            set.inject_fault_on(i, rstore_disk::faulty::Fault::Offline);
        }
        assert!(matches!(
            set.head_object("b", "k").await,
            Err(StoreError::ReadQuorum { .. })
        ));

        for i in 0..4 {
            set.clear_fault_on(i);
        }
        assert_eq!(
            set.head_object("b", "k").await.unwrap().size,
            1_500_000,
            "错误不该被缓存，恢复之后必须立刻可读"
        );
    }
```

- [ ] **Step 7: 跑测试**

Run: `cargo test -p rstore-store`
Expected: 全绿。既有测试一条不红——**这是缓存没有改变默认行为的最强证据**（默认 `IoModes::default()` 下缓存根本不存在）。

- [ ] **Step 8: 全量校验并提交**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add crates/store/src/resolve_cache.rs crates/store/src/lib.rs \
        crates/store/src/set.rs crates/store/src/get.rs \
        crates/store/src/put.rs crates/store/src/delete.rs
git commit -m "feat(store): metadata_cache，缓存版本解析结果

按 (bucket,key) 分片缓存 Resolved，每片一个单调计数器：put/delete
提交成功后 +1，条目按计数校验命中，只增不减所以没有 ABA。写入时若
发现计数已变则放弃存条目，宁可少一次命中也不塞旧值。

put_object / delete_object 各自拆成薄包装 + inner，失效点只落在
唯一的成功出口上。错误不进缓存，Absent 进。

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Task 5: `bounded_listing`

**Files:**
- Modify: `crates/store/src/list.rs`
- Modify: `crates/api/src/lib.rs`
- Modify: `crates/server/src/wiring.rs`
- Modify: `crates/s3/src/impl_s3.rs:315-410`

- [ ] **Step 1: 加有序增量遍历**

在 `crates/store/src/list.rs` 的 `impl ErasureSet` 里，`candidate_keys` 之后加：

```rust
    /// 按 key 升序、**可提前停**的有界遍历。返回 `(候选 key, 是否还有更多)`。
    ///
    /// **为什么不能用「先全收进 BTreeSet 再 sort」**（[`Self::candidate_keys`] 的做法）：
    /// 那样无法提前停，10 万个 key 的桶要全走完才能返回第一页。这里改成有序 DFS，
    /// 收够 `want` 个候选、或越过 `after` 所在的子树时立刻收手。
    ///
    /// **为什么必须在键目录这一层就把 key 定下来**：盘上布局是
    /// `<bucket>/<key>/<uuid>/meta.xl`，即一个 key 对应一个**目录**，版本在它的
    /// 子目录里。于是同一个目录 `p` 下会同时出现两种子目录：`p` 自己的版本目录
    /// （形状是 uuid）和更深的 key `p/a` 的目录 `a`。**uuid 的字典序与键结构毫无关系**，
    /// 所以「先下探、再从版本目录反推父路径」的实现会输出乱序的 key
    /// （`p/a` 可能排在 `p` 前面）。正确做法是在目录 `p` 这一层先判定
    /// 「`p` 自己是不是一个 key」并**先输出它**，然后再下探非版本目录。
    /// 见设计文档 §4.3。
    ///
    /// `after` 是**排除式游标**：一个 key（或公共前缀 `dir/`），不是下标。
    /// 整棵子树若整体 `<= after` 会被整块剪掉，所以翻页不会退化成「每页从头走一遍」。
    async fn candidate_keys_ordered(
        &self,
        bucket: &str,
        after: Option<&str>,
        want: usize,
    ) -> (Vec<String>, bool) {
        let mut keys: Vec<String> = Vec::new();
        let mut more = false;
        // 显式栈（async 递归要装箱）。子目录**逆序**压栈，弹出顺序即为升序。
        let mut stack: Vec<String> = vec![String::new()];

        while let Some(dir_rel) = stack.pop() {
            // 整棵子树都在游标之前 → 整块剪掉，一次 `list_dir` 都不发。
            // 子树里的 key 要么等于 `dir_rel`，要么以 `dir_rel/` 开头；两者都
            // `<= after` 当且仅当 `dir_rel/` <= after。
            if let Some(a) = after {
                if !dir_rel.is_empty() && format!("{dir_rel}/").as_str() <= a {
                    continue;
                }
            }

            // `entries_under` 返回的是 BTreeSet 派生出的升序 `Vec`，直接可用。
            let entries = self.entries_under(&join_rel(bucket, &dir_rel)).await;
            let mut child_dirs: Vec<String> = Vec::new();
            let mut is_key = false;
            for name in entries {
                if name.starts_with(".staging-") {
                    continue;
                }
                // 用户 key 的首段不可能是保留前缀（DESIGN §6.3 / Task 5.7），
                // 所以这只跳过系统目录，不会藏掉用户数据。
                if dir_rel.is_empty() && name.starts_with(RESERVED_PREFIX) {
                    continue;
                }
                let child_rel = join_rel(&dir_rel, &name);
                // 游标剪枝：整棵子树 `<= after` 时连一次 `stat` / `list_dir` 都不必发。
                // 条目已升序，所以第一个被剪掉的子树之后全是——直接 `break`。
                if let Some(a) = after {
                    if format!("{child_rel}/").as_str() <= a {
                        break;
                    }
                }
                if !self.any_disk_is_dir(&join_rel(bucket, &child_rel)).await {
                    continue;
                }
                // 子目录里直接躺着 `meta.xl` → 它是**版本目录**，说明父目录
                // `dir_rel` 本身就是一个 key；版本目录不再往下走。
                let sub = self.entries_under(&join_rel(bucket, &child_rel)).await;
                if sub.iter().any(|e| e == "meta.xl") {
                    is_key = true;
                } else {
                    child_dirs.push(child_rel);
                }
            }

            // **先输出自己，再下探**——`p` 必须排在 `p/a` 之前。
            if is_key && !dir_rel.is_empty() && after.is_none_or(|a| dir_rel.as_str() > a) {
                if keys.len() >= want {
                    // 已经够一页，而且**确实**还有下一个候选 → 报 `more` 并立刻收手。
                    // 这里多走一步是为了把 `more` 定准：谎报 `true` 会让调用方再发一次
                    // 请求，而在「整棵子树都要重走」的老实现上那是一次全量遍历。
                    more = true;
                    break;
                }
                keys.push(dir_rel.clone());
            }

            for child in child_dirs.into_iter().rev() {
                stack.push(child);
            }
        }

        // 走到这里说明遍历自然结束 = 后面没有了（`more` 仍是初值 `false`）。
        (keys, more)
    }
```

- [ ] **Step 2: 加 `list_objects_from`，并把 `list_objects` 改成它的特化**

把 `list.rs` 里第 55 行的 `pub async fn list_objects`（连同它上方的文档注释与它下面的
`PERF:` 注释）整段替换成：

```rust
    /// `bucket` 下所有活对象，按 key 升序。`prefix` 为 `None` 时返回全部。
    ///
    /// 冒烟/对账类调用方（`rclone sync`）会拿这个列表去删远端数据，所以**列不全绝
    /// 不静默**：`resolve_version` 的 `Err(ReadQuorum)` 原样上抛，而不是返回一个
    /// 残缺列表。
    ///
    /// 实现上它就是 [`Self::list_objects_from`] 的「从头开始、不限量」那一档，
    /// **因此旧模式的行为与本次改动前逐字节相同**。
    ///
    // PERF: 见 DESIGN §1.2 与 §20 Phase 2 — 命名空间索引。MVP 是全盘遍历 +
    // 每 key 一次元数据仲裁；不做前缀剪枝（`prefix` 按 key 字符串前缀，而 key 的
    // 目录切分与它并不对齐，剪错就是静默丢结果）。
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ObjectEntry>, StoreError> {
        let (entries, _more) = self
            .list_objects_from(bucket, prefix, None, usize::MAX)
            .await?;
        Ok(entries)
    }

    /// 从 `after`（**不含**）之后开始，按 key 升序返回至多 `want` 个活对象，
    /// 并报告是否还有更多。`prefix` 与 [`Self::list_objects`] 同义。
    ///
    /// **`want` 是候选 key 数的上界，不是返回条目数的上界**：候选是否「活着」由
    /// `resolve_version` 判（与 `list_objects` 用的是同一处判断），被删掉的 key
    /// 会从结果里消失。所以一页可能比 `want` 短，而 `more` 仍为 `true`——
    /// 调用方据此继续取下一批。
    ///
    /// **模式判断只此一处**：`bounded_listing` 关着就走旧的全量遍历
    /// （[`Self::candidate_keys`]，今天的实现），开着才走有序增量遍历。
    /// 两条实现都在，这正是新旧对比的基础。
    pub async fn list_objects_from(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        after: Option<&str>,
        want: usize,
    ) -> Result<(Vec<ObjectEntry>, bool), StoreError> {
        let (candidates, more) = if self.modes().bounded_listing {
            self.candidate_keys_ordered(bucket, after, want).await
        } else {
            // 旧路径：**全量遍历**，再在内存里做游标与限量。
            // 不换成新遍历，否则对比就失去了参照物。
            let all = self.candidate_keys(bucket).await;
            let mut out: Vec<String> = Vec::new();
            let mut more = false;
            for k in all {
                if let Some(a) = after {
                    if k.as_str() <= a {
                        continue;
                    }
                }
                if out.len() >= want {
                    more = true;
                    break;
                }
                out.push(k);
            }
            (out, more)
        };

        let mut out: Vec<ObjectEntry> = Vec::with_capacity(candidates.len());
        for key in candidates {
            let resolved = resolve_version(self, bucket, &key).await?;
            // 删除标记与 `Absent` 都走这里消失——与 GET 用的是同一处判断。
            // `Err(e)` 已由上面的 `?` 上抛，绝不 `if let Ok(..)` 吞掉。
            let Some((_dir, meta)) = resolved.live() else {
                continue;
            };
            let latest = latest_version(meta).ok_or_else(|| {
                StoreError::Internal(format!(
                    "authoritative metadata for {bucket}/{key} has no versions"
                ))
            })?;
            let etag = etag_of_meta(meta)?;
            out.push(ObjectEntry {
                key,
                size: latest.header.size,
                etag,
                mod_time: latest.header.mod_time.unwrap_or(0),
            });
        }
        // 前缀过滤在最后做一次（MVP 不做前缀剪枝，见 `list_objects` 的 PERF 注释）。
        // **必须在返回前做**：S3 层会按 `prefix.len()` 切片 key，不匹配的条目会让它 panic。
        if let Some(p) = prefix {
            out.retain(|e| e.key.starts_with(p));
        }
        Ok((out, more))
    }
```

- [ ] **Step 3: 写 store 层的分页测试**

在 `crates/store/src/list.rs` 的 `mod tests` 末尾加：

```rust
    /// 有序增量遍历与旧的全量遍历必须给出**同一串 key、同一个顺序**，
    /// 包括那个会暴露 UUID 陷阱的形状：同一个目录下既有 key 自己的版本目录
    /// （形状是 uuid），又有更深的 key 的目录，**且后者的名字排在 uuid 之前**。
    ///
    /// `-x` 的首字节 0x2d 小于任何 uuid 的首字节（十六进制 0x30..0x66），
    /// 所以「先下探、再从版本目录反推父路径」的实现会把 `p/-x` 排在 `p` 前面。
    #[tokio::test]
    async fn ordered_traversal_matches_full_traversal_including_the_uuid_trap() {
        let keys = ["a", "p", "p/-x", "p/z", "dir/b"];

        let old = set_with_modes(6, 2, IoModes::default()).await;
        let new = set_with_modes(6, 2, IoModes::ALL).await;
        for set in [&old, &new] {
            set.create_bucket("data").await.unwrap();
            for key in keys {
                set.put_object(put_args("data", key, 200_000)).await.unwrap();
            }
        }

        let full: Vec<String> = old
            .list_objects("data", None)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(full, vec!["a", "dir/b", "p", "p/-x", "p/z"]);

        // 一页一个，逐页拼回来——这条把「提前停 + 游标续传 + 子树剪枝」整条路走满。
        let mut paged: Vec<String> = Vec::new();
        let mut cursor: Option<String> = None;
        let mut guard = 0;
        loop {
            guard += 1;
            assert!(guard < 100, "分页没有终止——游标没有前进");
            let (page, more) = new
                .list_objects_from("data", None, cursor.as_deref(), 1)
                .await
                .unwrap();
            if page.is_empty() {
                assert!(!more, "空页不能报 more，那会让调用方空转");
                break;
            }
            cursor = Some(page.last().unwrap().key.clone());
            paged.extend(page.into_iter().map(|e| e.key));
            if !more {
                break;
            }
        }
        assert_eq!(paged, full, "增量遍历必须与全量遍历给出同一串 key、同一个顺序");
    }

    /// **旧模式走 `list_objects_from` 也必须给出正确结果**——它在模式关着时退化成
    /// 「全量遍历 + 内存过滤」，这条钉住那条退化路径没写错。
    #[tokio::test]
    async fn list_objects_from_works_in_old_mode_too() {
        let set = set_with_modes(6, 2, IoModes::default()).await;
        set.create_bucket("data").await.unwrap();
        for key in ["a", "b", "c"] {
            set.put_object(put_args("data", key, 200_000)).await.unwrap();
        }

        let (page, more) = set
            .list_objects_from("data", None, Some("a"), 1)
            .await
            .unwrap();
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].key, "b");
        assert!(more, "后面还有 c");

        let (page, more) = set
            .list_objects_from("data", None, Some("c"), 10)
            .await
            .unwrap();
        assert!(page.is_empty());
        assert!(!more);
    }

    /// 前缀过滤在两条模式下都必须生效——S3 层会按 `prefix.len()` 切片 key，
    /// 放过去一个不匹配的条目会让它 panic。
    #[tokio::test]
    async fn list_objects_from_filters_prefix_in_both_modes() {
        for modes in [IoModes::default(), IoModes::ALL] {
            let set = set_with_modes(6, 2, modes).await;
            set.create_bucket("data").await.unwrap();
            for key in ["a", "dir/b", "dir/c"] {
                set.put_object(put_args("data", key, 200_000)).await.unwrap();
            }
            let (page, _more) = set
                .list_objects_from("data", Some("dir/"), None, 10)
                .await
                .unwrap();
            let got: Vec<&str> = page.iter().map(|e| e.key.as_str()).collect();
            assert_eq!(got, vec!["dir/b", "dir/c"], "modes={modes:?}");
        }
    }
```

改 `list.rs` 测试模块的 import，把：

```rust
    use crate::testutil::{body, set_with_disks};
```

换成：

```rust
    use rstore_common::modes::IoModes;

    use crate::testutil::{body, set_with_disks, set_with_modes};
```

（`set_with_disks` 仍被既有测试用到，保留。）

- [ ] **Step 4: 跑 store 的 list 测试**

Run: `cargo test -p rstore-store list::`
Expected: 既有 3 条 + 新增 3 条全绿。

- [ ] **Step 5: 给 `ObjectStore` 加带默认实现的方法**

在 `crates/api/src/lib.rs` 的 trait 里，`list_objects` 之后加：

```rust
    /// 有界列举（`bounded_listing` 机制用）：从 `after`（**不含**）之后按 key 升序
    /// 取至多 `want` 个条目，返回 `(entries, more)`。
    ///
    /// **默认实现退化成「一次全量 + 客户端过滤」——这正是旧行为**，所以没实现它的
    /// `ObjectStore`（mock / nop / 将来的其他实现）不必改动，也不会因为少了这个方法
    /// 而编译不过。`Wiring` 覆盖它以走引擎的增量遍历。
    ///
    /// `want` 是条目数的上界；`more = true` 表示后面还有。**实现必须保证
    /// `entries.is_empty()` ⇒ `more == false`**，否则调用方会空转。
    async fn list_objects_from(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        after: Option<&str>,
        want: usize,
    ) -> Result<(Vec<ObjectEntry>, bool), ApiError> {
        let all = self.list_objects(bucket, prefix).await?;
        let mut out: Vec<ObjectEntry> = Vec::new();
        let mut more = false;
        for e in all {
            if let Some(a) = after {
                if e.key.as_str() <= a {
                    continue;
                }
            }
            if out.len() >= want {
                more = true;
                break;
            }
            out.push(e);
        }
        Ok((out, more))
    }
```

- [ ] **Step 6: `Wiring` 覆盖它**

在 `crates/server/src/wiring.rs` 的 `impl ObjectStore for Wiring` 里，`list_objects` 之后加：

```rust
    /// 转发给引擎的增量遍历。模式关着时引擎自己会退回全量（见 `list_objects_from`
    /// 在 `crates/store/src/list.rs` 里的分派），所以这一层不需要认识模式。
    async fn list_objects_from(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        after: Option<&str>,
        want: usize,
    ) -> Result<(Vec<ObjectEntry>, bool), ApiError> {
        match self
            .set
            .list_objects_from(bucket, prefix, after, want)
            .await
        {
            Ok((entries, more)) => Ok((
                entries
                    .into_iter()
                    .map(|e| ObjectEntry {
                        key: e.key,
                        size: e.size,
                        etag: e.etag,
                        mod_time: e.mod_time,
                    })
                    .collect(),
                more,
            )),
            Err(e) => {
                self.note_err(&e, "list");
                Err(map_object_err(e))
            }
        }
    }
```

- [ ] **Step 7: 把 S3 的 `list_objects_v2` 改成分批取**

这块改 `crates/s3/src/impl_s3.rs` 的 **两个**区间，中间那段（第 330-342 行的
`prefix` / `delimiter` / `max_keys` / `start_after` 声明）**原样保留**。

**区间一：第 322-328 行**。把

```rust
        // PERF: 见 DESIGN §1.2 与 §20 Phase 2 — 命名空间索引。
        // MVP 靠 `list_objects` 的全盘遍历（4.11）+ 本层过滤/分页，不是终态设计。
        let entries = self
            .store
            .list_objects(&req.input.bucket, req.input.prefix.as_deref())
            .await
            .map_err(to_s3_error)?;
```

替换成：

```rust
        // PERF: 见 DESIGN §1.2 与 §20 Phase 2 — 命名空间索引。
        // MVP 靠引擎的列举（4.11）+ 本层过滤/分页，不是终态设计。
        //
        // 候选是**分批**向引擎要的，不是一次要全量：`bounded_listing` 打开时
        // 引擎的增量遍历会在够数时提前收手，于是「10 万个 key 的桶只要 1000 条」
        // 只走一小段。模式关着时引擎退回全量遍历，第一批就带回全部、`more = false`，
        // 于是下面的循环只转一圈——**与今天的开销相同**。
```

**区间二：第 344-385 行**（从 `let mut contents: Vec<Object> = Vec::new();`
到 `for entry in entries { ... }` 那个循环的收尾 `}`）。整段替换成：

```rust
        let mut contents: Vec<Object> = Vec::new();
        // `BTreeSet` 顺带保证共同前缀有序且天然去重（同一目录只出一次）。
        let mut common_prefixes: BTreeSet<String> = BTreeSet::new();
        let mut truncated = false;
        // 「最后一条**返回过的**条目」的 key，作为下一页的游标。共同前缀也能当游标：
        // `cp = "dir/"` 是 `"dir/x"` 的前缀，`key <= "dir/"` 恰好跳过整个 dir/ 子树。
        let mut last_key: Option<String> = None;
        // **取批游标**：与 `last_key` 不同——`last_key` 是给客户端的续传 token，
        // 这个是本层内部翻批用的，一次响应里可能前进很多次。
        let mut cursor: Option<String> = start_after.clone();
        // 一批至少覆盖一整页，免得为了凑满一页反复取批。
        let batch_want = max_keys.max(1);

        'batches: loop {
            let (batch, more) = self
                .store
                .list_objects_from(
                    &req.input.bucket,
                    req.input.prefix.as_deref(),
                    cursor.as_deref(),
                    batch_want,
                )
                .await
                .map_err(to_s3_error)?;

            for entry in &batch {
                // 游标必须最先应用：被跳过的条目不该占 max_keys 的额度，否则第二页会比
                // 第一页短（且只在游标落在前缀内部时暴露）。字符串比较，不是下标。
                if let Some(s) = &start_after {
                    if entry.key <= *s {
                        continue;
                    }
                }
                // **先判容量，再处理本条**：max_keys=0 时在这里就 `break`，不会漏进循环体；
                // 于是 `is_truncated` 恰好等于「循环因容量而中断」，不必事后猜还有没有剩余。
                if contents.len() + common_prefixes.len() >= max_keys {
                    truncated = true;
                    break 'batches;
                }
                let rel = &entry.key[prefix.len()..];
                match delimiter.and_then(|d| rel.find(d)) {
                    Some(rel_idx) => {
                        // 下标是相对 `key` 全串的：相对切片的 rel_idx 必须加回 prefix.len()，
                        // 否则前缀被吃掉，客户端按 "sub/" 列举一条都拿不到。
                        let cp = entry.key[..prefix.len() + rel_idx + 1].to_string();
                        common_prefixes.insert(cp.clone());
                        last_key = Some(cp);
                    }
                    None => {
                        contents.push(Object {
                            key: Some(entry.key.clone()),
                            size: Some(entry.size as i64),
                            e_tag: Some(ETag::Strong(entry.etag.clone())),
                            last_modified: Some(timestamp_of(entry.mod_time)),
                            storage_class: Some(ObjectStorageClass::from_static("STANDARD")),
                            ..Default::default()
                        });
                        last_key = Some(entry.key.clone());
                    }
                }
            }

            // 本批处理完了，后面没有更多 → 这一页就是最终结果，`truncated` 保持 false。
            if !more {
                break 'batches;
            }
            // 还有更多：从本批最后一个候选之后继续。
            // 引擎的契约保证 `batch` 非空（空批必然 `more == false`），所以这里取不到
            // 最后一个就等于上游违约——**必须停下**，否则就是一个死循环。
            match batch.last() {
                Some(e) => cursor = Some(e.key.clone()),
                None => break 'batches,
            }
        }
```

> **两处必须加 `.clone()`**：循环头从 `for entry in entries`（按值消费）改成了
> `for entry in &batch`（按引用，因为 `batch` 后面还要用 `batch.last()` 推进游标）。
> 于是 `entry.key` 与 `entry.etag` 都成了借用，原来直接 move 的两处
> （`key: Some(entry.key)`、`e_tag: Some(ETag::Strong(entry.etag))`）必须改成
> `.clone()`。漏掉会直接编译不过，不会静默出错。

- [ ] **Step 8: 加 S3 层的分页回归测试**

在 `crates/s3/src/impl_s3.rs` 的 `mod tests` 里，第 1361 行 `request_with` 之后加两个
XML 解析小工具和一条测试。（该模块**没有** `list_objects_v2` 的既有用例，所以这里
是全新的一条；`mock_service` / `call_on` / `request` / `OBJ_BODY` 都是模块里已有的。）

```rust
    /// 从 ListObjectsV2 的 XML 里按出现顺序取出所有 `<Key>..</Key>`。
    ///
    /// 手撕而不是引 XML 库：这里只需要「有几个、什么顺序」，而多一个依赖要过
    /// allowlist 与 license 检查，不划算。
    fn keys_in(body: &[u8]) -> Vec<String> {
        let xml = std::str::from_utf8(body).expect("list body is utf-8 xml");
        let mut out = Vec::new();
        let mut rest = xml;
        while let Some(i) = rest.find("<Key>") {
            let after = &rest[i + "<Key>".len()..];
            let end = after.find("</Key>").expect("well-formed <Key>");
            out.push(after[..end].to_string());
            rest = &after[end..];
        }
        out
    }

    /// 取 `<NextContinuationToken>..</NextContinuationToken>`；没有（= 最后一页）返回 `None`。
    fn next_token(body: &[u8]) -> Option<String> {
        let xml = std::str::from_utf8(body).expect("list body is utf-8 xml");
        let open = "<NextContinuationToken>";
        let i = xml.find(open)? + open.len();
        let end = xml[i..].find("</NextContinuationToken>")?;
        Some(xml[i..i + end].to_string())
    }

    /// **逐页取要拼出与一次全量完全相同的 key 串**——`max-keys=2` 对 5 个 key，
    /// 必然要走完 `list_objects_v2` 里那个 `'batches` 循环的三圈。
    ///
    /// 这条盯的就是那个循环：它把「一次拿全」换成了「多次拿一小段」，游标前进、
    /// 容量判定、`is_truncated` / `next_continuation_token` 的任何一个写错都会在这里显形。
    ///
    /// **用 `MockStore` 是够的**：它没覆盖 `ObjectStore::list_objects_from`，
    /// 于是走 trait 的默认实现（一次全量 + 按 `after`/`want` 过滤），
    /// 那正是「旧模式」的形状，S3 层照样会收到 `more = true` 并被迫翻批。
    /// 引擎那一侧的**提前停**由 `crates/store/src/list.rs` 的单测负责，
    /// 两件事各测各的，不要在这里混着测。
    #[tokio::test]
    async fn list_v2_paging_reassembles_the_whole_bucket() {
        let store = Arc::new(MockStore::default());
        for i in 0..5 {
            let (status, _, body) = call_on(
                mock_service(store.clone()),
                request("PUT", &format!("/test-bucket/k{i}"), OBJ_BODY),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "body: {}", String::from_utf8_lossy(&body));
        }

        let mut seen: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        let mut pages = 0usize;
        loop {
            pages += 1;
            assert!(pages <= 10, "分页没有终止——游标没有前进");
            let path = match &token {
                Some(t) => format!("/test-bucket?list-type=2&max-keys=2&continuation-token={t}"),
                None => "/test-bucket?list-type=2&max-keys=2".to_string(),
            };
            let (status, _, body) = call_on(mock_service(store.clone()), request("GET", &path, b"")).await;
            assert_eq!(status, StatusCode::OK, "body: {}", String::from_utf8_lossy(&body));

            seen.extend(keys_in(&body));
            match next_token(&body) {
                Some(t) => token = Some(t),
                None => break,
            }
        }

        assert_eq!(
            seen,
            vec!["k0", "k1", "k2", "k3", "k4"],
            "分页拼接必须与全量列举逐条相同、同序"
        );
        assert_eq!(pages, 3, "5 个 key、每页 2 个 → 恰好三页（2+2+1）");
    }
```

- [ ] **Step 8b: 跑这条测试**

Run: `cargo test -p rstore-s3 list_v2_paging`
Expected: PASS。

**若 `pages` 不是 3**，说明 `next_token` 的解析或游标传递有问题——先查
`http::Uri` 里查询串的取法，再查 `next_continuation_token` 的生成条件。

- [ ] **Step 9: 全量校验并提交**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add crates/store/src/list.rs crates/api/src/lib.rs \
        crates/server/src/wiring.rs crates/s3/src/impl_s3.rs
git commit -m "feat(store): bounded_listing，有序增量遍历 + 分批列举

新增 candidate_keys_ordered：在键目录这一层就判定「这个目录自己是不是
一个 key」并先输出、再下探非版本目录，因此输出天然有序；配合排除式
游标与子树剪枝，够 want 个候选即停。旧的全量遍历一字不动，两条实现
并存——这正是新旧对比的基础。

ObjectStore 加带默认实现的 list_objects_from（默认退化成旧行为，
mock/nop 不必改），Wiring 转发给引擎，list_objects_v2 改成按批取。

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Task 6: `pooled_write_buffers`

**Files:**
- Modify: `crates/store/src/put.rs:103`（`read_block`）、`:368`（`write_shards_stream`）
- Modify: `crates/store/src/testutil.rs`（`set_with_recording_disks_modes`）

- [ ] **Step 1: 把读块拆成「读进已有缓冲」**

把 `crates/store/src/put.rs` 里 `read_block`（第 103 行，连同它上方的文档注释）整段替换成：

```rust
/// 读「至多一个块」进**调用方提供的** `buf`，返回读到的字节数。
///
/// 与 [`read_block`] 是同一件事，区别只在于缓冲由调用方提供、于是可以跨块复用。
/// `truncate` 之后再 `resize` 会把新增的尾部清零——与 `vec![0u8; BLOCK_SIZE]`
/// 的那次清零代价相同（都不省 memset），省下的是**分配**本身。
async fn read_block_into(
    body: &mut (dyn AsyncRead + Unpin + Send),
    buf: &mut Vec<u8>,
) -> Result<usize, StoreError> {
    buf.clear();
    buf.resize(BLOCK_SIZE, 0);
    let mut filled = 0usize;
    while filled < BLOCK_SIZE {
        let n = body
            .read(&mut buf[filled..])
            .await
            .map_err(|e| StoreError::Internal(format!("read request body: {e}")))?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    buf.truncate(filled);
    Ok(filled)
}

/// 读「至多一个块」：读满 `BLOCK_SIZE` 或到 EOF 为止，两者取先到者。
///
/// 返回长度 `< BLOCK_SIZE` 就说明已经读到流末尾——调用方据此判定「没有更多了」。
/// **这是整条写路径唯一的缓冲点**，上界就是 `BLOCK_SIZE`。
///
/// 不用 `AsyncReadExt::take().read_to_end()`：`take` 消费接收者，而我们需要
/// 在下一轮继续读同一个流，改回来要写 `(&mut *body).take(..)` 这类借用体操。
/// 逐次 `read` 到填满更直白，也与 `fsx::read_exact_at` 的循环同一种写法。
async fn read_block(body: &mut (dyn AsyncRead + Unpin + Send)) -> Result<Vec<u8>, StoreError> {
    let mut buf = Vec::new();
    read_block_into(body, &mut buf).await?;
    Ok(buf)
}
```

- [ ] **Step 2: 加 `ShardScratch`**

在 `read_block` 之后加：

```rust
/// 单次 PUT 内跨块复用的暂存缓冲（`pooled_write_buffers` 机制）。
///
/// **为什么是局部而不是全局池**：真实需求只有「一次 PUT 里别每块重新分配」。
/// DESIGN §13.3 的四级池要 `Semaphore` + `ManuallyDrop` + `try_lock` 一整套，
/// 而它解决的是跨请求、跨尺寸的复用——不是这次的对比目标。见设计文档 §5。
///
/// **它不减少任何一次 IO**，所以它的证据形态是**分配次数**，不是 IO 计数。
/// 只看 IO 计数的对比会显示 0 差异，那不是 bug，那是它本来的样子。
struct ShardScratch {
    /// 当前块的读缓冲，容量在满载时为 `BLOCK_SIZE`。
    block: Vec<u8>,
    /// `data_shards` 份数据分片缓冲，跨块复用。
    shards: Vec<Vec<u8>>,
}
```

- [ ] **Step 2b: 加分配计数器（L4 唯一的证据形态）**

**为什么需要一个计数器**：这个机制**不减少任何一次 IO**，所以 IO 计数在它身上必然
显示 0 差异。设计文档 §6 把「分配次数」定为它的证据形态——没有这个计数器，
Task 8 的 L4 两行就没有任何可断言的东西。

**为什么不是全局分配器探针**：那要一个 `GlobalAlloc` 实现，而它必须 `unsafe`，
workspace lint 是 `unsafe_code = "forbid"`。

**这里量的是什么（必须说准，别夸大）**：它数的是**我们自己的代码显式增长分片缓冲的
次数**，不是分配器真实的分配次数。判据是 `Vec::capacity() < 需要的长度`——
`clear()` 不释放容量、`resize(n, 0)` 在 `capacity >= n` 时复用，所以这个判据恰好
就是「这次 `resize` 会不会向分配器要内存」。它对 `Vec` 成立且是确定性的。

在 `crates/store/src/set.rs` 顶部加：

```rust
use std::sync::atomic::{AtomicU64, Ordering};
```

`ErasureSet` 结构体在 `modes` 字段之后加：

```rust
    /// 分片缓冲的**显式增长次数**，累计值。**只为基准存在**——
    /// `pooled_write_buffers` 不减少任何一次 IO，只有这个计数能显示它的作用。
    /// 随模式开关一起删除（设计文档 §10）。
    scratch_allocs: AtomicU64,
```

`with_modes` 的 `Ok(Self { .. })` 里加：

```rust
            scratch_allocs: AtomicU64::new(0),
```

在 `pub fn modes(&self)` 之后加：

```rust
    /// 取走累计的分配次数并归零。
    ///
    /// **`pub` 而不是 `pub(crate)`**：读它的是 `benches/io_modes.rs`，那是独立编译的
    /// crate，看不到 `pub(crate)` 的东西。这个可见性只活到基准跑完。
    pub fn take_scratch_allocs(&self) -> u64 {
        self.scratch_allocs.swap(0, Ordering::Relaxed)
    }
```

- [ ] **Step 3: 改写 `write_shards_stream` 的缓冲部分**

把 `crates/store/src/put.rs` 里 `write_shards_stream`（第 368 行）的**函数体**
（从 `let data_shards = self.data();` 到 `Ok((shard_len, size, format!("{:x}", md5.finalize())))`）
替换成：

```rust
        let data_shards = self.data();
        let parity = self.parity();
        let step = step as usize;
        let part_rel = format!("{staging}/part.1");
        // 缓冲复用只在开关打开时启用。**关着时走上下一字不改的老路径**：
        // 每块一个新 `Vec`，`block` 也每轮重新分配。
        let pooled = self.modes().pooled_write_buffers;

        // 循环外为每块盘建一个写入器；掉线的槽位没有写入器。
        let mut writers: Vec<Option<BitrotShardWriter>> = self
            .disks()
            .iter()
            .map(|slot| {
                slot.as_ref()
                    .map(|d| BitrotShardWriter::new(Arc::clone(d), part_rel.clone(), step))
            })
            .collect();

        let mut md5 = Md5::new();
        let mut size: u64 = 0;
        // `first` 是调用方已经读出来的第一个块——它天然就是这份暂存缓冲的起点。
        let mut scratch = ShardScratch {
            block: first,
            shards: Vec::new(),
        };

        loop {
            // 空块只可能在 EOF 时出现（且此时前面的判断已经 break），
            // 留着这道闸是为了不把「空块」喂进 `push_block`——它会当成布局错误拒绝。
            if scratch.block.is_empty() {
                break;
            }

            md5.update(&scratch.block);
            let shard_size_k =
                even_ceil((scratch.block.len() as u64).div_ceil(u64::from(data_shards))) as usize;

            // 关掉复用时就地把上一轮的分片缓冲全部丢掉，让下一轮重新分配——
            // 与引入本机制之前一字不差。
            if !pooled {
                scratch.shards = Vec::new();
            }
            scratch.shards.resize_with(data_shards as usize, Vec::new);
            // 数据分片：按 `shard_size_k` 切，不足处补零。补零只可能落在最后一个
            // 分片的尾部，这正是读侧「顺序相接再截断到 L_k」的依据。
            let mut grew = 0u64;
            for (i, shard) in scratch.shards.iter_mut().enumerate() {
                // 分配计数（Step 2b）：只有「这一轮 `resize` 真的会向分配器要内存」
                // 才计数。`clear()` 不释放容量，容量够时 `resize` 就在原地写，
                // 所以这个判据恰好就是「要不要新内存」。
                // **只数分片数据缓冲**，外层 `Vec<Vec<u8>>`（24 字节/项）不计——
                // 量的对象是分片数据本身。
                if shard.capacity() < shard_size_k {
                    grew += 1;
                }
                shard.clear();
                shard.resize(shard_size_k, 0);
                let s = i * shard_size_k;
                if s < scratch.block.len() {
                    let e = (s + shard_size_k).min(scratch.block.len());
                    shard[..e - s].copy_from_slice(&scratch.block[s..e]);
                }
            }
            self.scratch_allocs.fetch_add(grew, Ordering::Relaxed);

            let codec = self
                .codec_cache()
                .get(data_shards as usize, parity as usize, shard_size_k)
                .map_err(|e| {
                    StoreError::Internal(format!(
                        "codec geometry ({data_shards}, {parity}, {shard_size_k}): {e}"
                    ))
                })?;
            let parity_shards = codec
                .encode(&scratch.shards)
                .map_err(|e| StoreError::Internal(format!("erasure encode: {e}")))?;

            // 分片编号沿用 `encode` 的输出顺序：`0..data` 数据分片，`data..N` 校验分片。
            // 数据分片借自 `scratch.shards`、校验分片借自 `parity_shards`——两者拼成
            // 同一串编号，但**不合成一个新的 `Vec`**（合成就把复用又还回去了）。
            let total_shards = data_shards as usize + parity_shards.len();
            for kk in 0..total_shards {
                let shard: &[u8] = if kk < data_shards as usize {
                    &scratch.shards[kk]
                } else {
                    &parity_shards[kk - data_shards as usize]
                };

                // 分片 `kk` 落在盘 `dist[kk] - 1`。每块盘每块恰好收到一份（`dist` 是排列）。
                let physical = usize::from(dist[kk] - 1);
                // 两种错误必须分开处理，**不能**一个 `?` 了事：
                // - `ShardLayout` 是程序 bug（几何算错、块序颠倒），整次写入中止；
                // - 盘的 IO 失败等价于「这块盘拿不到票」，交给下面的 `write_quorum` 判定。
                // 逐块落盘之后后者第一次有了提前暴露的机会（从前 `push_block` 根本不碰盘，
                // 所有 IO 错误都堆在 `finish` 里由选票兜底）。若在这里提前返回，
                // 一块掉线的盘就会让**整个 PUT** 失败，而它本该只是少一票。
                // 所以丢掉这块盘的写入器，后续块不再往它写。
                let failed = match writers[physical].as_mut() {
                    Some(w) => match w.push_block(shard).await {
                        Ok(()) => false,
                        Err(e @ StoreError::ShardLayout(_)) => return Err(e),
                        Err(_) => true,
                    },
                    None => false,
                };
                if failed {
                    writers[physical] = None;
                }
            }

            size += scratch.block.len() as u64;
            // 短块就是最后一块——这里 break，绝不再去读流，否则会多等一次 IO。
            if scratch.block.len() < BLOCK_SIZE {
                break;
            }
            if pooled {
                // 复用同一块缓冲：`read_block_into` 会先 clear + resize，旧内容不残留。
                read_block_into(body, &mut scratch.block).await?;
            } else {
                scratch.block = read_block(body).await?;
            }
        }

        let mut achieved = 0u8;
        for w in writers.into_iter().flatten() {
            if w.finish().await.is_ok() {
                achieved += 1;
            }
        }
        if achieved < write_quorum {
            return Err(StoreError::WriteQuorum {
                achieved,
                required: write_quorum,
            });
        }

        let shard_len = expected_shard_len(size, data_shards);
        Ok((shard_len, size, format!("{:x}", md5.finalize())))
```

`put.rs` 顶部加 `Ordering` 的导入（`use std::sync::atomic::Ordering;`）——
`fetch_add` 的第二个参数要用它。检查是否已有 `use std::sync::...` 的同类导入，
有就并进那一组，别留两条。

**这个计数的算术（L4 断言的依据）**：`data_shards = 4`、`L4_SIZE = 16 MiB`、
`BLOCK_SIZE = 1 MiB` → 16 个块。

| 模式 | 每块的分片缓冲 | 合计 |
|---|---|---|
| 旧（`pooled = false`） | `scratch.shards` 每轮清空 → 4 份新 `Vec` 容量 0 → 4 次增长 | 16 × 4 = **64** |
| 新（`pooled = true`） | 首轮 4 份从容量 0 涨到 `shard_size_k`；之后容量够，0 次 | **4** |

16 倍。最后一块短于 `BLOCK_SIZE` 时 `shard_size_k` 变小，**不会**触发增长，
所以新模式的 4 是精确值、不是「不超过」。

- [ ] **Step 4: 加录制版夹具**

在 `crates/store/src/testutil.rs` 里，把 `set_with_recording_disks` 连同它上面的
文档注释（第 **189-223** 行）整段拆成薄包装 + 带模式的版本：

```rust
/// [`set_with_recording_disks_modes`] 的旧模式特化。
pub async fn set_with_recording_disks(total: u8, parity: u8) -> (TestSet, WriteLog) {
    set_with_recording_disks_modes(total, parity, IoModes::default()).await
}

/// [`set_with_modes`] 的录制版：每块盘的最内层是 `RecordingDisk`，
/// 所有盘的写入长度汇总到同一个 `WriteLog`，并指定读写路径模式。返回 `(set, log)`。
///
/// 包装顺序是 `LocalDisk` → `RecordingDisk` → `FaultyDisk`。**这个顺序不能反**：
/// `RecordingDisk` 记的是「最终落到盘上的那些写入」，若把它套在 `FaultyDisk`
/// 外面，`Fault::DropWrites` 之类「假装成功」的故障就不会出现在日志里。
pub async fn set_with_recording_disks_modes(
    total: u8,
    parity: u8,
    modes: IoModes,
) -> (TestSet, WriteLog) {
    let dir = tempfile::TempDir::new().expect("create tempdir");
    let log = WriteLog::default();

    let mut disks: Vec<Option<Arc<dyn DiskAPI>>> = Vec::with_capacity(total as usize);
    let mut faulties: Vec<Arc<FaultyDisk>> = Vec::with_capacity(total as usize);
    for i in 0..total as usize {
        let root = dir.path().join(format!("disk{i}"));
        let inner: Arc<dyn DiskAPI> =
            Arc::new(LocalDisk::open(&root, DiskId::new_v4()).expect("open local disk"));
        // `FaultyDisk::wrap` 取 `impl DiskAPI + 'static`（按值），所以先把
        // `RecordingDisk` 建出来、按值传进去，之后再 Arc/unsize。
        let recorded = RecordingDisk::new(inner, log.clone());
        let faulty = Arc::new(FaultyDisk::wrap(recorded));
        faulties.push(Arc::clone(&faulty));
        let erased: Arc<dyn DiskAPI> = faulty;
        disks.push(Some(erased));
    }

    let set = ErasureSet::with_modes(disks, parity, modes).expect("valid erasure set geometry");
    (
        TestSet {
            set,
            faulties,
            _dir: dir,
        },
        log,
    )
}
```

- [ ] **Step 5: 写测试**

在 `crates/store/src/put.rs` 的 `mod tests` 里，找一个既能拿到 `TestSet` 又能拿到
`WriteLog` 的既有测试作为位置参照，在它之后加：

```rust
    /// 缓冲复用**不该改变任何可观察行为**。三组断言，一组比一组强：
    /// 1. 内容/大小/etag 相同；
    /// 2. `RecordingDisk` 记下的**落盘写入长度序列逐项相同**；
    /// 3. 读回来仍然逐字节正确（复用缓冲如果残留旧内容，会在这里显形）。
    #[tokio::test]
    async fn pooled_write_buffers_is_observably_identical() {
        // 3_000_000 = 2 个满块 + 一个短末块：末块是唯一会改变 `shard_size_k` 的地方，
        // 也是「复用缓冲」最容易出错的块（长度变了，缓冲要 resize 而不是沿用）。
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();

        let (plain, log_plain) = set_with_recording_disks_modes(6, 2, IoModes::default()).await;
        let a = plain
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                body: body(data.clone()),
                etag: None,
            })
            .await
            .unwrap();

        let (pooled, log_pooled) =
            set_with_recording_disks_modes(6, 2, IoModes::ALL).await;
        let b = pooled
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                body: body(data.clone()),
                etag: None,
            })
            .await
            .unwrap();

        assert_eq!(a.size, b.size);
        assert_eq!(a.etag, b.etag, "etag 是内容的 MD5，相同内容必须给出相同 etag");

        assert_eq!(
            log_plain.sizes(),
            log_pooled.sizes(),
            "落盘的写入长度序列必须逐项相同——缓冲复用不该改变写入的形状"
        );

        assert_eq!(plain.get_object("b", "k", None).await.unwrap().data, data);
        assert_eq!(pooled.get_object("b", "k", None).await.unwrap().data, data);
    }

    /// L4 的**证据本身**：分配计数必须真的降下来。
    ///
    /// 这条断言的意义在于——IO 计数在两条模式下**必然相同**（这个机制不动 IO），
    /// 所以上面两条测试拿不到任何区分度。只有分配计数能证明机制在工作。
    ///
    /// 算术：`(6, 2)` → `data_shards = 4`；3 个块（2 满 + 1 短）。
    /// 旧模式每块 4 份新缓冲 = 12；新模式首块 4 份、之后 0 = 4。末块 `shard_size_k`
    /// 变小不会触发增长，所以 4 是精确值。
    #[tokio::test]
    async fn pooled_write_buffers_cuts_shard_buffer_allocations() {
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();

        let (plain, _log_plain) = set_with_recording_disks_modes(6, 2, IoModes::default()).await;
        plain
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                body: body(data.clone()),
                etag: None,
            })
            .await
            .unwrap();

        let (pooled, _log_pooled) = set_with_recording_disks_modes(6, 2, IoModes::ALL).await;
        pooled
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                body: body(data.clone()),
                etag: None,
            })
            .await
            .unwrap();

        assert_eq!(plain.take_scratch_allocs(), 12, "旧模式：3 块 × 4 份新缓冲");
        assert_eq!(pooled.take_scratch_allocs(), 4, "新模式：首块 4 份，之后复用");
        // `take_` 的语义是「取走并归零」——再取一次必须是 0，否则基准里的累计值
        // 会把两次测量混在一起。
        assert_eq!(pooled.take_scratch_allocs(), 0);
    }

    /// 单块对象（小于一个块）没有第二块可复用——**这是这个机制的阴性对照**。
    /// 两条模式的落盘写入序列必须完全相同。
    #[tokio::test]
    async fn pooled_write_buffers_has_no_effect_on_a_single_block_object() {
        let (plain, log_plain) = set_with_recording_disks_modes(6, 2, IoModes::default()).await;
        let (pooled, log_pooled) = set_with_recording_disks_modes(6, 2, IoModes::ALL).await;

        for set in [&plain, &pooled] {
            set.put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                body: body(vec![0x5Au8; 64 * 1024]),
                etag: None,
            })
            .await
            .unwrap();
        }
        assert_eq!(log_plain.sizes(), log_pooled.sizes());
    }
```

改 `crates/store/src/put.rs` 第 485 行起的 `mod tests` 的 import，把第 491 行：

```rust
    use crate::testutil::{body, set_with_disks, set_with_recording_disks, TestSet};
```

换成（`IoModes` 加在**测试模块内**，不是文件顶部——文件顶部加会在非 test 构建里
变成未使用 import）：

```rust
    use rstore_common::modes::IoModes;

    use crate::testutil::{
        body, set_with_disks, set_with_modes, set_with_recording_disks,
        set_with_recording_disks_modes, TestSet,
    };
```

- [ ] **Step 6: 跑测试**

Run: `cargo test -p rstore-store put::`
Expected: 既有测试全绿 + 新增 2 条绿。

- [ ] **Step 7: 全量校验并提交**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add crates/store/src/set.rs crates/store/src/put.rs crates/store/src/testutil.rs
git commit -m "feat(store): pooled_write_buffers，单次 PUT 内复用分片缓冲

read_block 拆出 read_block_into（读进已有缓冲），write_shards_stream
用本地 ShardScratch 跨块复用块缓冲与 data_shards 份分片缓冲。校验分片
不再合成进一个新 Vec，直接借 parity_shards——合成就把复用还回去了。

刻意不做全局分层池（DESIGN §13.3）：真实需求只是「一次 PUT 内别重复
allocate」，而那套池要 Semaphore + ManuallyDrop + try_lock 一整套。

证据形态是分配次数而不是 IO 计数——它不减少任何一次 IO，所以 IO 计数
在它身上必然 0 差异。为此在 ErasureSet 上加了 scratch_allocs 计数器
（随模式开关一起删除），判据是 shard.capacity() < shard_size_k：这恰好
就是「本轮 resize 会不会向分配器要内存」。不做全局分配器探针，那要
unsafe，而 workspace lint 是 unsafe_code = forbid。

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Task 7: 差分等价测试套件

**Files:**
- Create: `crates/store/src/io_modes_equiv.rs`
- Modify: `crates/store/src/lib.rs`

- [ ] **Step 1: 写测试文件**

创建 `crates/store/src/io_modes_equiv.rs`：

```rust
//! 新旧读写路径的**差分等价**测试（设计文档 §7.2 的硬门槛 1）。
//!
//! 三组断言，**第三组是关键**：
//!
//! - (i) 健康、**同类工作负载**：两种模式输出逐字节相同；
//! - (ii) 损坏落在读取区间**内**：两种模式产生**相同的错误变体**；
//! - (iii) 损坏落在读取区间**外**：新模式的可用性更好——把注入压到恰好让旧模式
//!   掉到 quorum 以下时，旧模式必须失败、新模式必须成功。
//!
//! 第三组不能用「新旧应当一致」去写——它们**本来就该不一致**，那正是这次改动的
//! 意义（设计文档 §8）。把它写成一条有名字的测试，比让它在前两组的模糊断言里
//! 擦边过去要诚实得多。
//!
//! **本模块必须是 `src/` 内的 `#[cfg(test)]`**：`testutil` 只在 crate 内可见
//! （`lib.rs` 里是 `#[cfg(test)] mod testutil;`），`benches/` 与 `tests/` 都看不到它。

use rstore_common::modes::IoModes;
use rstore_disk::faulty::Fault;
use uuid::Uuid;

use crate::error::StoreError;
use crate::get::ByteRange;
use crate::put::{shard_step, PutArgs, BLOCK_SIZE};
use crate::testutil::{body, set_with_modes, TestSet};


/// 「旧模式」在这个文件里一律写作 `IoModes::default()`，与 Task 1 里
/// `ErasureSet::new` 的定义保持同一个来源——**不要再另立一个 `OLD` 常量**，
/// 那样两处会各自漂移，而这个文件存在的全部意义就是量的准确。
fn put(bucket: &str, key: &str, data: Vec<u8>) -> PutArgs {
    PutArgs {
        bucket: bucket.into(),
        key: key.into(),
        body: body(data),
        etag: None,
    }
}

// ---- (i) 健康：同类工作负载逐字节相同 ----

/// 四个机制全开与全关，在**同类工作负载**下必须给出逐字节相同的结果。
///
/// 覆盖：分片对象整读 / 范围读（四种块边界）/ HEAD / LIST / 覆盖写后再读 / 删除。
/// **不比较 `mod_time`**：两个 set 是不同的临时目录，PUT 发生在不同的时刻，
/// 那个字段本来就不该相等。比它等于在比时钟。
#[tokio::test]
async fn new_and_old_agree_on_healthy_workloads() {
    // 3_000_000 = 2 个满块 + 一个短末块；不是 251 的整数倍 → 覆盖补零-截断那段几何。
    let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();

    let old = set_with_modes(6, 2, IoModes::default()).await;
    let new = set_with_modes(6, 2, IoModes::ALL).await;
    for set in [&old, &new] {
        set.put_object(put("b", "k", data.clone())).await.unwrap();
    }

    // 整读。
    let a = old.get_object("b", "k", None).await.unwrap();
    let b = new.get_object("b", "k", None).await.unwrap();
    assert_eq!(a.data, data);
    assert_eq!(b.data, data, "新模式的整读必须与旧模式逐字节相同");
    assert_eq!(a.size, b.size);
    assert_eq!(a.etag, b.etag, "etag 只能有一处算法");

    // 范围读：首块内、跨块边界、末块末字节、中间单字节。
    for (start, end) in [
        (0u64, 99u64),
        (BLOCK_SIZE as u64 - 1, BLOCK_SIZE as u64 + 1),
        (data.len() as u64 - 1, data.len() as u64 - 1),
        (1_000_000, 1_000_000),
    ] {
        let r = ByteRange { start, end };
        let a = old.get_object("b", "k", Some(r)).await.unwrap();
        let b = new.get_object("b", "k", Some(r)).await.unwrap();
        assert_eq!(a.data, data[start as usize..=end as usize], "旧模式 {start}-{end}");
        assert_eq!(b.data, a.data, "新模式 {start}-{end}");
    }

    // HEAD。
    let a = old.head_object("b", "k").await.unwrap();
    let b = new.head_object("b", "k").await.unwrap();
    assert_eq!((a.size, a.etag), (b.size, b.etag));

    // LIST。
    let a: Vec<String> = old
        .list_objects("b", None)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.key)
        .collect();
    let b: Vec<String> = new
        .list_objects("b", None)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.key)
        .collect();
    assert_eq!(a, b);

    // 覆盖写：两条路径都必须立刻看到新版本（缓存失效 + 增量列举的边界）。
    for set in [&old, &new] {
        set.put_object(put("b", "k", vec![7u8; 700_000])).await.unwrap();
    }
    let a = old.head_object("b", "k").await.unwrap();
    let b = new.head_object("b", "k").await.unwrap();
    assert_eq!((a.size, a.etag), (b.size, b.etag), "覆盖写之后都必须看到新版本");

    // 删除。
    for set in [&old, &new] {
        set.delete_object("b", "k").await.unwrap();
    }
    assert!(matches!(old.head_object("b", "k").await, Err(StoreError::NotFound)));
    assert!(matches!(new.head_object("b", "k").await, Err(StoreError::NotFound)));
}

// ---- (ii) 损坏落在读取区间内：两种模式产生相同的错误 ----

/// 3 盘 1 校验：`data = 2`、`read_quorum = 2`。
///
/// 0 号盘上**第 0 块**的数据字节改坏，1 号盘掉线 → 只剩 2 号盘可用 < quorum。
/// 区间读会读到第 0 块、整份读也会读到第 0 块，所以**两种模式都必须失败**，
/// 而且失败得一模一样：丢的是同一块盘、报的是同一个 `ReadQuorum`。
#[tokio::test]
async fn corruption_inside_the_range_fails_identically_in_both_modes() {
    let data = vec![0x5Au8; 2_000_000];

    for (label, modes) in [("old", IoModes::default()), ("new", IoModes::ALL)] {
        let set = set_with_modes(3, 1, modes).await;
        let out = set
            .put_object(put("b", "k", data.clone()))
            .await
            .unwrap();

        corrupt_on_disk(&set, 0, &out.data_dir, 0).await;
        set.inject_fault_on(1, Fault::Offline);

        let r = set
            .get_object("b", "k", Some(ByteRange { start: 0, end: 99 }))
            .await;
        assert!(
            matches!(r, Err(StoreError::ReadQuorum { .. })),
            "{label}: 区间内的损坏必须让两种模式报同一个 ReadQuorum，got {r:?}"
        );
    }
}

// ---- (iii) 损坏落在读取区间外：声明出来的语义变化 ----

/// **这是设计文档 §8 声明出来的差异，必须显式断言。**
///
/// 几何：3 盘 1 校验（`data = 2`、`read_quorum = 2`），对象恰好 2 个块。
/// 0 号盘上**第 1 块**（区间外）改坏。
///
/// 1. **弱断言**：只毁这一块盘时，两种模式都还剩 2 块可用，**都成功且结果相同**——
///    差异此时还没被逼出来；
/// 2. **强断言**：再让 1 号盘掉线，旧模式掉到 1 块可用（< 2）必须 `ReadQuorum` 失败，
///    而新模式**根本没读第 1 块**，照样成功；
/// 3. **反向对照**：新模式在**整读**下与旧模式一样失败——差异只来自「少读了几块」，
///    不是来自「新模式更宽松地接受损坏」。
#[tokio::test]
async fn corruption_outside_the_range_is_the_declared_semantic_change() {
    let size = 2 * BLOCK_SIZE as u64;
    let data: Vec<u8> = vec![0x5Au8; size as usize];
    let range = Some(ByteRange { start: 0, end: 99 });

    let old = set_with_modes(3, 1, IoModes::default()).await;
    let new = set_with_modes(3, 1, IoModes::ALL).await;

    let mut dirs: Vec<Uuid> = Vec::new();
    for set in [&old, &new] {
        let out = set.put_object(put("b", "k", data.clone())).await.unwrap();
        dirs.push(out.data_dir);
    }
    // 两边都把 0 号盘的**第 1 块**（区间外）改坏。
    corrupt_on_disk(&old, 0, &dirs[0], 1).await;
    corrupt_on_disk(&new, 0, &dirs[1], 1).await;

    // (1) 弱断言：只有一块盘坏、且坏在区间外 → 两边都成功，结果相同。
    assert_eq!(old.get_object("b", "k", range).await.unwrap().data, data[..100]);
    assert_eq!(new.get_object("b", "k", range).await.unwrap().data, data[..100]);

    // (2) 强断言：再掉一块盘，把旧模式压到 quorum 以下。
    for set in [&old, &new] {
        set.inject_fault_on(1, Fault::Offline);
    }
    match old.get_object("b", "k", range).await {
        Err(StoreError::ReadQuorum { .. }) => {}
        other => panic!(
            "旧模式必须报 ReadQuorum：区间外的位腐让它丢掉 0 号盘，可用盘掉到 1 < 2，got {other:?}"
        ),
    }
    assert_eq!(
        new.get_object("b", "k", range)
            .await
            .expect("新模式没读第 1 块，不该因为它的损坏而失败")
            .data,
        data[..100],
        "成功时给的必须是**正确**的前 100 字节"
    );

    // (3) 反向对照：新模式整读时同样必须失败。
    match new.get_object("b", "k", None).await {
        Err(StoreError::ReadQuorum { .. }) => {}
        other => panic!("新模式整读会读到第 1 块，必须同样失败，got {other:?}"),
    }
}

// ---- 夹具 ----

/// 把第 `disk_idx` 块盘上 `part.1` 里第 `block` 块的**第一个数据字节**改坏。
///
/// 改的是数据字节而不是摘要字节：摘要被改只说明校验在比，数据被改才说明
/// 「重算的数据哈希」真的在比（`reader.rs` 的 `detects_bitrot` 有同样的注释）。
///
/// 偏移靠 `shard_step` 复算，不写死数字——写死的话几何一改，这里就悄悄改错了位置，
/// 测试还会**绿**（改到别处去了）。
async fn corrupt_on_disk(set: &TestSet, disk_idx: usize, data_dir: &Uuid, block: usize) {
    let size = 2 * BLOCK_SIZE as u64;
    let data_shards = set.data();
    let stride = rstore_checksum::HASH_LEN + shard_step(size, data_shards) as usize;
    let at = block * stride + rstore_checksum::HASH_LEN;

    let d = set.disks()[disk_idx].as_ref().expect("该盘应当在线");
    let rel = format!("b/k/{data_dir}/part.1");
    let len = d.stat(&rel).await.unwrap().unwrap().size as usize;
    let mut bytes = d.read_exact_at(&rel, 0, len).await.unwrap();
    assert!(at < len, "要改的偏移 {at} 超出分片长度 {len}");
    bytes[at] ^= 0xFF;
    d.write_all(&rel, &bytes).await.unwrap();
}
```

- [ ] **Step 2: 注册模块**

`crates/store/src/lib.rs`：在 `#[cfg(test)] mod quorum_boundaries;` 之后加：

```rust
#[cfg(test)]
mod io_modes_equiv;
```

- [ ] **Step 3: 跑等价测试**

Run: `cargo test -p rstore-store io_modes_equiv`
Expected: 3 passed。

**如果 `corruption_outside_the_range_is_the_declared_semantic_change` 的第 (2) 段红了**，
说明 `ranged_shard_read` 没有真的收窄读取范围（很可能 `blocks` 被算成了 `None`）。
回到 Task 3 Step 5 检查 `blocks` 的折算。

- [ ] **Step 4: 全量校验并提交**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add crates/store/src/io_modes_equiv.rs crates/store/src/lib.rs
git commit -m "test(store): 新旧读写路径的差分等价套件

三组断言：健康的同类工作负载逐字节相同；损坏落在读取区间内时两种
模式报同一个错误变体；损坏落在区间外时新模式的可用性更好——把注入
压到恰好让旧模式掉到 quorum 以下那一档显式断言，并反问一句「新模式
整读时是否同样失败」，确保差异来自少读了几块而不是更宽松地接受损坏。

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Task 8: 基准（`harness = false`，无 criterion）

**Files:**
- Create: `crates/store/benches/io_modes.rs`
- Modify: `crates/store/Cargo.toml`

- [ ] **Step 1: 建基准目标**

`crates/store/Cargo.toml`：在 `[dev-dependencies]` 之后加：

```toml
# `harness = false`：这是一个手写的 `fn main`，不引 criterion。
# 设计文档 §7.3 定的是「以确定性计数为主、墙钟为辅」——计数由本文件自己的
# `IoLog` 给出，criterion 的统计功利用不上，而它会拖一整套依赖进来。
[[bench]]
name = "io_modes"
harness = false
```

- [ ] **Step 2: 写基准**

创建 `crates/store/benches/io_modes.rs`：

```rust
//! 读写路径新旧模式的对比基准。
//!
//! 运行：`cargo bench -p rstore-store --bench io_modes`
//!
//! **不能复用 `crate::testutil`**：`lib.rs` 里是 `#[cfg(test)] mod testutil;`，
//! 而 bench 目标是**独立编译的 crate**，以普通依赖身份链接 `rstore-store`——
//! 那个模块在这里根本不存在。这与「等价测试必须待在 `src/` 内」是同一条理由的
//! 两个方向。可以直接复用的只有 `DiskAPI` / `LocalDisk` / `Fault`。
//!
//! **计数运行与计时运行是两批**：`IoLog` 会在每个热路径调用上做一次原子自增，
//! 所以它必然改变时序。这里两者是同一次运行里分段测的（先计数、后重置计时），
//! 报告里必须把这一点写清楚。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use rstore_common::disk_id::DiskId;
use rstore_common::error::DiskError;
use rstore_common::modes::IoModes;
use rstore_disk::{DiskAPI, FileStat, LocalDisk};
use rstore_store::get::ByteRange;
use rstore_store::put::PutArgs;
use rstore_store::set::ErasureSet;

// ---- 工作负载规模 ----
// **报告里的每个数字都必须与这里的取值一起写**，否则复现不了。

/// 只读 100 字节的那个对象有多大。
const L1_SIZE: usize = 256 * 1024 * 1024;
/// HEAD 密集负载的请求数。
const L2_HEADS: usize = 10_000;
/// HEAD 阴性对照里不同 key 的个数（每多一个 key 都要一次 PUT，所以比主测小）。
const L2_DISTINCT: usize = 1_000;
/// 列举负载的桶里有多少 key。
const L3_KEYS: usize = 10_000;
/// 列举主测要凑满一页多少条。
const L3_PAGE: usize = 1_000;
/// 写负载的对象大小与轮数。
const L4_SIZE: usize = 16 * 1024 * 1024;
const L4_ITERS: usize = 8;
/// 每个 `(模式, 工作负载)` 重复几轮，取最小值压掉噪声。
const REPEAT: usize = 3;

/// 记录 IO 调用次数与字节数。包在 `LocalDisk` 外面。
#[derive(Default)]
struct IoLog {
    reads: AtomicU64,
    read_bytes: AtomicU64,
    stats: AtomicU64,
    lists: AtomicU64,
}

/// 一组计数快照：`(read 次数, read 字节, stat 次数, list_dir 次数)`。
#[derive(Clone, Copy, Default)]
struct Counts(u64, u64, u64, u64);

impl IoLog {
    fn snapshot(&self) -> Counts {
        Counts(
            self.reads.load(Ordering::Relaxed),
            self.read_bytes.load(Ordering::Relaxed),
            self.stats.load(Ordering::Relaxed),
            self.lists.load(Ordering::Relaxed),
        )
    }

    /// 归零。在**每个工作负载开跑前**调用，于是快照之间的差就是这次工作负载的开销。
    fn reset(&self) {
        for a in [&self.reads, &self.read_bytes, &self.stats, &self.lists] {
            a.store(0, Ordering::Relaxed);
        }
    }
}

impl Counts {
    fn sub(self, base: Counts) -> Counts {
        Counts(
            self.0 - base.0,
            self.1 - base.1,
            self.2 - base.2,
            self.3 - base.3,
        )
    }
}

/// 计数盘。**只计数、不改行为**：每个方法都原样转发给内层。
struct CountingDisk {
    inner: LocalDisk,
    log: Arc<IoLog>,
}

#[async_trait::async_trait]
impl DiskAPI for CountingDisk {
    async fn write_all(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError> {
        self.inner.write_all(rel_path, data).await
    }
    async fn append(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError> {
        self.inner.append(rel_path, data).await
    }
    async fn read_exact_at(
        &self,
        rel_path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, DiskError> {
        self.log.reads.fetch_add(1, Ordering::Relaxed);
        self.log.read_bytes.fetch_add(len as u64, Ordering::Relaxed);
        self.inner.read_exact_at(rel_path, offset, len).await
    }
    async fn rename(&self, from_rel: &str, to_rel: &str) -> Result<(), DiskError> {
        self.inner.rename(from_rel, to_rel).await
    }
    async fn remove_dir_all(&self, rel_path: &str) -> Result<(), DiskError> {
        self.inner.remove_dir_all(rel_path).await
    }
    async fn list_dir(&self, rel_path: &str) -> Result<Vec<String>, DiskError> {
        self.log.lists.fetch_add(1, Ordering::Relaxed);
        self.inner.list_dir(rel_path).await
    }
    async fn stat(&self, rel_path: &str) -> Result<Option<FileStat>, DiskError> {
        self.log.stats.fetch_add(1, Ordering::Relaxed);
        self.inner.stat(rel_path).await
    }
    async fn sync_file_and_parent(&self, rel_path: &str) -> Result<(), DiskError> {
        self.inner.sync_file_and_parent(rel_path).await
    }
    fn disk_id(&self) -> &DiskId {
        self.inner.disk_id()
    }
    fn is_local(&self) -> bool {
        self.inner.is_local()
    }
}

/// 一块 set 加它的计数器与临时目录（`TempDir` 一 drop，盘上的数据就没了）。
struct Fixture {
    set: Arc<ErasureSet>,
    log: Arc<IoLog>,
    _dir: tempfile::TempDir,
}

fn build(total: u8, parity: u8, modes: IoModes) -> Fixture {
    let dir = tempfile::TempDir::new().expect("create tempdir");
    let log = Arc::new(IoLog::default());
    let mut disks: Vec<Option<Arc<dyn DiskAPI>>> = Vec::with_capacity(total as usize);
    for i in 0..total as usize {
        let root = dir.path().join(format!("disk{i}"));
        let inner = LocalDisk::open(&root, DiskId::new_v4()).expect("open local disk");
        let d: Arc<dyn DiskAPI> = Arc::new(CountingDisk {
            inner,
            log: Arc::clone(&log),
        });
        disks.push(Some(d));
    }
    let set = Arc::new(ErasureSet::with_modes(disks, parity, modes).expect("valid geometry"));
    Fixture {
        set,
        log,
        _dir: dir,
    }
}

fn put_args(bucket: &str, key: &str, data: &[u8]) -> PutArgs {
    PutArgs {
        bucket: bucket.into(),
        key: key.into(),
        body: Box::new(std::io::Cursor::new(data.to_vec())),
        etag: None,
    }
}

/// 一次工作负载的测量结果。
#[derive(Clone, Copy)]
struct Measured {
    counts: Counts,
    /// 分片缓冲的**显式增长次数**（`ErasureSet::take_scratch_allocs`）。
    /// 只有 L4 会用到它——其余三个机制的 IO 计数就够，而 L4 的 IO 计数必然相同。
    allocs: u64,
    micros: u128,
}

/// 跑 `REPEAT` 轮取**最小墙钟**，并把对应的计数差记下来。
///
/// 取最小而不是平均：墙钟只会被噪声推**高**，不会被推低，所以最小值最接近真值。
///
/// **三轮的计数必须完全相同，这里直接断言。** 计数是本基准的主证据，而它唯一的
/// 失效方式就是「有东西没被重置」——那会让数字看起来平滑而实际上是脏的。
/// `debug_assert` 不够：`cargo bench` 默认就是 release，得用真断言。
/// 分配计数同样逐轮必须相同，一起断言。
/// `body` 收一个**轮次下标**：写负载必须靠它把每轮的 key 区分开。不区分的话第 2 轮
/// 变成覆盖写，`gc_superseded` 会多走一条删旧版本的路，计数于是与第 1 轮不同——
/// 上面那条「计数必须逐轮相同」的断言会立刻炸，而那是**对的**：那意味着每轮在测
/// 不同的东西。读负载用不到这个参数，写 `|_round|`。
async fn measure<F, Fut>(fx: &Fixture, mut body: F) -> Measured
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let mut best: Option<Measured> = None;
    for round in 0..REPEAT {
        fx.log.reset();
        // 分配计数器也要归零：夹具准备阶段（`seeded`）写了几万个对象，
        // 那些增长次数全都留在计数器里，不冲掉就会混进本轮读数。
        fx.set.take_scratch_allocs();
        let base = fx.log.snapshot();
        let t0 = Instant::now();
        body(round).await;
        let micros = t0.elapsed().as_micros();
        let counts = fx.log.snapshot().sub(base);
        let allocs = fx.set.take_scratch_allocs();

        match best {
            Some(prev) => {
                assert_eq!(
                    (counts.0, counts.1, counts.2, counts.3, allocs),
                    (
                        prev.counts.0,
                        prev.counts.1,
                        prev.counts.2,
                        prev.counts.3,
                        prev.allocs
                    ),
                    "第 {round} 轮的计数与第 0 轮不同（{:?}/{allocs} vs {:?}/{}）——\
                     计数是确定性的，不同就说明有东西没被重置，这个数字不能用",
                    (counts.0, counts.1, counts.2, counts.3),
                    (prev.counts.0, prev.counts.1, prev.counts.2, prev.counts.3),
                    prev.allocs,
                );
                if micros < prev.micros {
                    best = Some(Measured {
                        counts,
                        allocs,
                        micros,
                    });
                }
            }
            None => {
                best = Some(Measured {
                    counts,
                    allocs,
                    micros,
                })
            }
        }
    }
    best.expect("REPEAT 至少是 1")
}

// ---- 工作负载 ----
//
// **闭包一律写成 `|round| async move { .. }`，并且只捕获 `Copy` 的东西。**
// 这是 `FnMut() -> Fut` 这个形状的硬要求：async 块不可能借用闭包自己的环境
// （那个环境在每次调用时都会被重新借出），所以凡是 future 要用到的值，都必须在
// 闭包**外面**就已经是一个引用（`&T` / `Option<ByteRange>` 这类 `Copy` 值），
// 然后由 `async move` 把那份引用**拷贝**进去。写成 `|| async { .. &data .. }`
// 会因为「借用了闭包自身的字段」而编译不过。

/// L1 主测：256 MiB 对象的 `GET bytes=0-99`。
async fn l1_ranged(fx: &Fixture) -> Measured {
    let range = Some(ByteRange { start: 0, end: 99 });
    measure(fx, |_round| async move {
        fx.set.get_object("b", "big", range).await.unwrap();
    })
    .await
}

/// L1 阴性对照：同一个对象的**完整** GET。范围读对它没有可省的东西。
async fn l1_full(fx: &Fixture) -> Measured {
    measure(fx, |_round| async move {
        fx.set.get_object("b", "big", None).await.unwrap();
    })
    .await
}

/// L2 主测：同一个 key 上跑 `L2_HEADS` 次 HEAD。
async fn l2_same_key(fx: &Fixture) -> Measured {
    measure(fx, |_round| async move {
        for _ in 0..L2_HEADS {
            fx.set.head_object("b", "hot").await.unwrap();
        }
    })
    .await
}

/// L2 阴性对照：`L2_DISTINCT` 个**不同** key 各 HEAD 一次。
/// 缓存必然次次失效，两种模式应当**一样慢**。
async fn l2_distinct(fx: &Fixture) -> Measured {
    measure(fx, |_round| async move {
        for i in 0..L2_DISTINCT {
            fx.set.head_object("b", &format!("k{i:06}")).await.unwrap();
        }
    })
    .await
}

/// L3 主测：从 `L3_KEYS` 个 key 的桶里凑满一页 `L3_PAGE` 条。
async fn l3_page(fx: &Fixture) -> Measured {
    measure(fx, |_round| async move {
        let mut seen = 0usize;
        let mut cursor: Option<String> = None;
        while seen < L3_PAGE {
            let (page, more) = fx
                .set
                .list_objects_from("b", None, cursor.as_deref(), L3_PAGE)
                .await
                .unwrap();
            if page.is_empty() {
                assert!(!more, "空页不能报 more");
                break;
            }
            seen += page.len();
            cursor = Some(page.last().unwrap().key.clone());
            if !more {
                break;
            }
        }
        assert_eq!(seen, L3_PAGE, "桶里应当至少有 {L3_PAGE} 个 key");
    })
    .await
}

/// 同一个桶里**除了** L3 那批 key 还有别的东西（`big`、`hot`、L2 的 `kNNNNNN`），
/// 所以种子数据的**总**条目数是 `L3_KEYS + OTHER_KEYS`。
const OTHER_KEYS: usize = 2 + L2_DISTINCT;

/// L3 阴性对照：`max-keys` 大到必须扫全，两种模式应当**一样慢**。
async fn l3_all(fx: &Fixture) -> Measured {
    measure(fx, |_round| async move {
        let all = fx.set.list_objects("b", None).await.unwrap();
        assert_eq!(
            all.len(),
            L3_KEYS + OTHER_KEYS,
            "种子数据全在那儿，一个都不该少"
        );
    })
    .await
}

/// L4 主测：`L4_ITERS` 次 16 MiB 的 PUT。
///
/// **它的主证据只能是分配次数**——`pooled_write_buffers` 不减少任何一次 IO，
/// 所以它那两行的 reads/stats/lists 差异接近 0 是**预期**，不是没生效。
/// 分配次数由 `ErasureSet::take_scratch_allocs` 给出（`main` 里有精确断言）：
/// `(6, 2)` → `data_shards = 4`，16 MiB = 16 个满块 →
/// 旧模式 `8 × 16 × 4 = 512`，新模式 `8 × 4 = 32`，16 倍。
async fn l4_put_16m(fx: &Fixture) -> Measured {
    let buf = vec![0x5Au8; L4_SIZE];
    let data: &[u8] = &buf;
    measure(fx, |round| async move {
        for i in 0..L4_ITERS {
            // key 里带轮次：不然第 2 轮变成覆盖写，`gc_superseded` 会多走一条
            // 删旧版本的路，计数与第 1 轮不同，上面那条断言会（正确地）炸掉。
            fx.set
                .put_object(put_args("b", &format!("w{round}-{i:04}"), data))
                .await
                .unwrap();
        }
    })
    .await
}

/// L4 阴性对照：单块（64 KiB）对象，没有第二块可复用。
///
/// 分配次数也必须**逐项相同**（8 次 PUT × 1 块 × 4 份 = 32）：只有一次分片机会时，
/// 「复用」和「重新分配」是同一件事。这条对照证明主测那 16 倍来自跨块复用，
/// 而不是来自某种全局的分配路径改变。
async fn l4_put_small(fx: &Fixture) -> Measured {
    let buf = vec![0x5Au8; 64 * 1024];
    let data: &[u8] = &buf;
    measure(fx, |round| async move {
        for i in 0..L4_ITERS {
            fx.set
                .put_object(put_args("b", &format!("s{round}-{i:04}"), data))
                .await
                .unwrap();
        }
    })
    .await
}

// ---- 装配与打印 ----

/// 建一个已经填好数据的 set。
async fn seeded(modes: IoModes) -> Fixture {
    let fx = build(6, 2, modes);
    fx.set.create_bucket("b").await.unwrap();

    // L1 的对象。
    fx.set
        .put_object(put_args("b", "big", &vec![0x11u8; L1_SIZE]))
        .await
        .unwrap();
    // L2 主测的 hot key。
    fx.set
        .put_object(put_args("b", "hot", &vec![0x22u8; 200_000]))
        .await
        .unwrap();
    // L2 对照的若干 key。
    for i in 0..L2_DISTINCT {
        fx.set
            .put_object(put_args("b", &format!("k{i:06}"), &vec![0x33u8; 4_096]))
            .await
            .unwrap();
    }
    // L3 的桶（与 L2 的 key 同桶，`l3_all` 断言用 `L3_KEYS`）。
    for i in 0..L3_KEYS {
        fx.set
            .put_object(put_args("b", &format!("p{i:06}"), &vec![0x44u8; 4_096]))
            .await
            .unwrap();
    }
    fx
}

fn print_row(name: &str, old: Measured, new: Measured) {
    let fmt = |m: Measured| {
        format!(
            "{:>10}  {:>14}  {:>10}  {:>9}  {:>8}  {:>11}",
            m.counts.0,
            m.counts.1,
            m.counts.2,
            m.counts.3,
            m.allocs,
            format!("{}us", m.micros)
        )
    };
    println!("\n=== {name} ===");
    println!(
        "{:>6}  {:>10}  {:>14}  {:>10}  {:>9}  {:>8}  {:>11}",
        "mode", "reads", "read_bytes", "stats", "lists", "allocs", "wall"
    );
    println!("{:>6}  {}", "old", fmt(old));
    println!("{:>6}  {}", "new", fmt(new));
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    println!("READ/WRITE PATH IO MODES — 计数与墙钟");
    println!("规模：L1_SIZE={L1_SIZE} L2_HEADS={L2_HEADS} L2_DISTINCT={L2_DISTINCT}");
    println!("      L3_KEYS={L3_KEYS} L3_PAGE={L3_PAGE} L4_SIZE={L4_SIZE} L4_ITERS={L4_ITERS}");
    println!("      REPEAT={REPEAT}（取最小墙钟）");

    let old = seeded(IoModes::default()).await;
    let new = seeded(IoModes::ALL).await;

    // **顺序有约束，不能随手调**：`l3_all` 断言的是种子数据的**精确**条目数，
    // 所以 L4 那两批 PUT（会往同一个桶里加 key）必须排在 L3 之后。
    // 三个只读负载之间没有顺序依赖，但写负载一律最后跑，这个不变量好守。
    print_row("L1 GET bytes=0-99（主测）", l1_ranged(&old).await, l1_ranged(&new).await);
    print_row(
        "L1 完整 GET（阴性对照：范围读无可省）",
        l1_full(&old).await,
        l1_full(&new).await,
    );

    print_row(
        "L2 同一 key 10k 次 HEAD（主测）",
        l2_same_key(&old).await,
        l2_same_key(&new).await,
    );
    print_row(
        "L2 1k 个不同 key 各 HEAD 一次（阴性对照：缓存必然失效）",
        l2_distinct(&old).await,
        l2_distinct(&new).await,
    );

    print_row(
        "L3 从 10k key 里凑满 1000 条（主测）",
        l3_page(&old).await,
        l3_page(&new).await,
    );
    print_row(
        "L3 列全桶（阴性对照：必须扫全）",
        l3_all(&old).await,
        l3_all(&new).await,
    );

    // L4：先取数，**断言之后再打印**——数字错了就该在这里炸掉，而不是印出来让人
    // 自己看出不对。这是本次基准唯一一条不靠「两行对照」的证据，值得硬断言。
    let l4 = (l4_put_16m(&old).await, l4_put_16m(&new).await);
    assert_eq!(l4.0.allocs, 512, "旧模式：8 次 PUT × 16 块 × 4 份缓冲");
    assert_eq!(l4.1.allocs, 32, "新模式：8 次 PUT × 首块 4 份，之后复用");
    print_row("L4 PUT 16 MiB × N（主测）", l4.0, l4.1);

    let ctrl = (l4_put_small(&old).await, l4_put_small(&new).await);
    assert_eq!(
        ctrl.0.allocs, ctrl.1.allocs,
        "阴性对照：单块对象没有第二块可复用，分配次数必须完全相同"
    );
    assert_eq!(ctrl.0.allocs, 32, "8 次 PUT × 1 块 × 4 份缓冲");
    print_row(
        "L4 PUT 64 KiB × N（阴性对照：单块，无复用机会）",
        ctrl.0,
        ctrl.1,
    );

    println!(
        "\n注意：计数与墙钟来自**同一次**运行，`IoLog` 的原子自增会影响时序。\n\
         读法：计数是主证据（确定性、可复现），墙钟是佐证。\n\
         reads/read_bytes/stats/lists 四个机制各自对应的那一列应当差异巨大，\n\
         其余三列（含 L4 的全部 IO 列）差异接近 0 是**预期**，不是没生效。\n\
         L4 的证据在 `allocs` 列：旧 512 / 新 32（16 MiB × 8 次），单块对照两行相同。\n\
         它量的是**我们自己的代码显式增长分片缓冲的次数**，不是分配器的真实分配次数——\n\
         全局分配器探针要 `unsafe`，而 workspace lint 是 `unsafe_code = \"forbid\"`。"
    );
}
```

- [ ] **Step 3: 确认能编译并跑通**

Run: `cargo bench -p rstore-store --bench io_modes --no-run`
Expected: 编译通过。

Run: `cargo bench -p rstore-store --bench io_modes`
Expected: 打印 8 行结果。**看这几条**：

- `L1 GET bytes=0-99`：new 的 `read_bytes` 应当远小于 old（old ≈ 每盘 256/4 = 64 MiB × 6 盘 ≈ 384 MiB；new ≈ 几百字节 × 6 盘）；
- `L2 同一 key 10k 次 HEAD`：new 的 `lists` 应当远小于 old（old ≈ 10k × 6 × 2；new ≈ 几次）；
- `L2 1k 个不同 key`：两行应当**几乎一样**（阴性对照）；
- `L3 列全桶`：两行应当**几乎一样**（阴性对照）；
- `L4 PUT 16 MiB`：`allocs` 列必须是 **512 / 32**（这条不靠眼看，`main` 里已断言）；
- `L4 PUT 64 KiB`：两行的 `allocs` 必须**相同**（各 32，也已断言）。

任何一条对照位的差异显著非零，**先修测量再谈结论**——那说明数据集串了、或计数没重置。
L4 那两条断言失败时同理：先怀疑计数器没在轮次开始时归零，再怀疑复用逻辑。

- [ ] **Step 4: 提交**

```bash
git add crates/store/Cargo.toml crates/store/benches/io_modes.rs
git commit -m "bench(store): 读写路径新旧模式对比基准

harness = false 的手写 main，不引 criterion——计数由本地 IoLog 给出，
criterion 的统计功利用不上而它要拖一整套依赖进来。

四个工作负载各配一个阴性对照：L1 范围读 vs 完整读、L2 同 key HEAD
vs 不同 key HEAD、L3 凑一页 vs 列全桶、L4 16MiB PUT vs 单块 PUT。
没有对照位，「新模式更快」就只是数字而不是证据。

bench 目标是独立编译的 crate，看不到 #[cfg(test)] 的 testutil，
所以计数盘在这里重写了一份。

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## Task 9: 跑基准、写对比结果、同步文档

**Files:**
- Create: `docs/io-modes-bench.md`
- Modify: `docs/DESIGN.md:502-514`（§11）

- [ ] **Step 1: 跑基准，把输出原样记下来**

Run: `cargo bench -p rstore-store --bench io_modes 2>&1 | tee /tmp/io-modes-bench.txt`

- [ ] **Step 2: 写结果文档**

创建 `docs/io-modes-bench.md`，**把上一步的真实输出填进去**（不要手抄成好看的形状——
原样贴，附上机器与日期）。结构：

```markdown
# 读写路径新旧模式对比

> 数据来自 `cargo bench -p rstore-store --bench io_modes` 的一次运行。
> **规模参数与机器必须一起记**，否则复现不了。

**日期：** 2026-10-10
**机器：** <在这一行填 `uname -a` 或 Windows 版本 + CPU 型号>
**规模：** L1_SIZE=… L2_HEADS=… L2_DISTINCT=… L3_KEYS=… L3_PAGE=… L4_SIZE=… L4_ITERS=… REPEAT=…

## 原始输出

<原样贴 `cargo bench` 的输出>

## 怎么读

**计数是主证据，墙钟是佐证。** 「旧模式每块盘读 256 MiB，新模式读 1 KiB」是构造性
的事实，换台机器仍然成立；「快了 3.4 倍」会随机器、缓存、后台进程漂移。

**阴性对照位的差异必须接近 0。** 它们不证明新机制有多快，它们证明这次测量没有
在量别的东西。对照位若显著非零，先修测量再谈结论。

**L4 的主证据只能是分配次数**：`pooled_write_buffers` 不减少任何一次 IO，
所以它那两行的 reads/stats/lists 差异接近 0 是**预期**，不是没生效。
它的证据在 `allocs` 列：16 MiB × 8 次是 **512 → 32**（16 倍），
单块对照（64 KiB）两行**相同**（各 32）——后者才是前面那个 16 倍确实来自
跨块复用的证据。它数的是我们自己的代码显式增长分片缓冲的次数，
不是分配器的真实分配次数（全局分配器探针要 `unsafe`，workspace lint 禁止）。

## 结论

<逐条写：哪个机制在哪个负载上带来多少差、对照位是否干净、有哪些数字出乎预期>

## 已知局限

- 计数与墙钟来自同一次运行，`IoLog` 的原子自增会影响时序；
- `crates/disk/src/fsx.rs::sync_dir` 在 **Windows 上是 no-op**，所以本次**没有**
  测 fsync / 组提交——那在这台机器上是一条平直的假线（设计文档 §1.2）；
- 内存上界没有被直接测量（`unsafe_code = "forbid"` 排除了全局分配器探针）；
  `allocs` 是「显式增长缓冲的次数」这个**行为**的计数，不是字节数，
  也不能推出 RSS 的变化。
```

- [ ] **Step 3: 更新 DESIGN §11**

在 `docs/DESIGN.md` 的 §11「完整性：bitrot 校验」末尾（`> **与 RustFS 的差异**` 那段引用块之后）加：

```markdown
- **读时校验的范围正比于读到的范围**：范围读（`GET bytes=a-b`）只重算它真正读到的
  那几个块的摘要，被跳过的块不校验。因此「完整 GET 能发现块 7 的损坏、读块 1 的范围
  GET 不能发现」是**预期行为**。代价换来的可用性提升是单向的：一块盘只在**实际需要
  的那几个块**损坏时才被判为缺块，所以同一份数据下范围读丢的分片只会比整对象读更少。
```

- [ ] **Step 4: 确认设计文档里那处与代码不符的说法已改**

`docs/superpowers/specs/2026-10-10-read-write-path-modes-design.md` 的 §4.2 已经改成
「缓存自带一套按 key 哈希分片的锁」（DESIGN §16.1 那张 `RwLock` 表在代码里并不存在）。
确认它在 git 里已经提交：

```bash
git log --oneline -1 -- docs/superpowers/specs/2026-10-10-read-write-path-modes-design.md
git status --short docs/superpowers/specs/
```

若显示未提交，补一次提交。

- [ ] **Step 5: 全量校验并提交**

```bash
cargo fmt --all --check && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace
git add docs/io-modes-bench.md docs/DESIGN.md docs/superpowers/specs/
git commit -m "docs: 读写路径新旧模式的对比结果 + DESIGN §11 同步

记录 cargo bench 的原始输出与规模参数，并写清读法：计数是主证据、
墙钟是佐证，阴性对照位必须接近 0，L4 的主证据在 allocs 列
（512 → 32，单块对照两行相同）。

DESIGN §11 补一句「读时校验的范围正比于读到的范围」——这条语义变化
是设计出来的，不是缺陷。

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## 收尾（不在本计划的提交范围里）

按设计文档 §10，基准跑完之后的删除是**下一步的独立工作**，本计划只到「量出来」为止：

1. 把获胜路径合并成唯一路径；
2. 删 `crates/common/src/modes.rs`、`ErasureSet` 的 `modes` / `resolve_cache` /
   `scratch_allocs` 字段与 `take_scratch_allocs()`、`with_modes`、`--io-mode`、
   `config.rs` 的 `io_mode`、`startup.rs` 的传参、`docs/io-modes-bench.md`；
3. 等价测试改写成针对新行为的**回归测试**（不再测「新旧一致」）；
4. 基准折叠成单模式，但**保留阴性对照**；
5. 验收：全仓 grep 不到 `IoModes` / `io_mode` / `io-mode`。

---

## 自审记录

**规格覆盖：** §3 `IoModes`→Task 1；§3.2 接入→Task 1；§3.3 CLI→Task 1；§4.1 范围读→Task 2+3；
§4.2 元数据缓存→Task 4；§4.3 有界列举→Task 5；§5 写侧→Task 6；§6 插桩→Task 8；
§7.1 工作负载与对照→Task 8；§7.2 两个硬门槛→Task 7 + Task 8 Step 3；§7.3 证据形态→Task 9；
§8 语义变化→Task 3 + Task 9 Step 3；§9 YAGNI→各任务的「不做」注释；§10 删除计划→收尾节；
§11 DESIGN 增量→Task 9 Step 3；§12 文件清单→本计划全部覆盖。

**与设计的一处偏离（已回填设计）：** 设计 §4.2 原本说元数据缓存「复用 DESIGN §16.1 的
分片 `RwLock` 表」。**代码里没有那张表**（`crates/store/src/` 里没有任何
`RwLock`/`Mutex`/`Semaphore`），§16.1 至今只是设计预留。设计文档已改成「缓存自带一套
按 key 哈希分片的锁」，本计划 Task 4 按改后的说法实现。

**类型一致性：** `IoModes` 四个字段名全程一致；`set_with_modes` / `with_modes` /
`modes()` / `resolve_cache()` / `candidate_keys_ordered` / `list_objects_from` /
`read_range` / `read_blocks` / `checked_stat` / `slice_blocks` / `read_block_into` /
`ShardScratch` / `set_with_recording_disks_modes` 在各任务间的签名与调用一致。
每个被引用到的既有符号都对着真代码核过：`crate::put::shard_step` 是 `pub(crate)`、
`ErasureSet::data()` 是 `pub`、`rstore_checksum::HASH_LEN` 是 `pub`、
`WriteLog::sizes()` 是 `pub`、`reader.rs` 测试夹具的
`temp_disk` / `write_shard` / `reader` 三个辅助函数都已存在且签名对得上。

**自审改掉的四处**（都是写完才发现的真问题，不是措辞）：

1. **基准里 8 个工作负载的闭包形状本来是编译不过的。** 写成
   `|| async { .. &data .. }` 会让 async 块借用闭包自己的环境，而 `measure` 要的是
   `FnMut() -> Fut`——那个借用活不过一次调用。改成 `|round| async move { .. }` 且
   只捕获 `Copy` 的值（`&Fixture`、`&[u8]`、`Option<ByteRange>`），并在代码上方
   把这条约束写成了注释，免得后来者「顺手」改回去。
2. **`measure` 原本只是「声称」三轮计数相同，没有断言。** 计数是这个基准的主证据，
   而它唯一的失效方式是「有东西没被重置」——那会让数字看起来平滑而实际是脏的。
   补了真断言（`cargo bench` 是 release，`debug_assert` 无效），并为此给闭包加了
   轮次参数：写负载每轮必须换 key，否则第 2 轮变成覆盖写、`gc_superseded` 会多走
   一条删旧版本的路，计数必然与第 1 轮不同。
3. **基准的工作负载顺序有隐含约束**：`l3_all` 断言种子数据的精确条目数，所以会往同一个
   桶里加 key 的 L4 必须排在 L3 之后。原来把 L4 放最前，会直接炸掉 `l3_all`。
   已调整顺序并把这条约束写进注释。
4. **`read_range` 在空分片上的契约本来是含糊的。** 原计划的测试写着「返回空 `Vec`」，
   而按 `read_blocks` 的越界 `assert`，它会 panic——两者只能有一个对。选择**保留
   assert**（越界是程序 bug，安静返回空数据会让调用方分不清它和「真读到 0 字节」），
   并把测试改成 `#[should_panic]` 钉住这个契约，另加一条 `first > last` 的反向契约。

**一处刻意保留的 TDD 让步：** Task 3 的两条测试在实现之前就是绿的——它们断言的是
「接线之后不许改坏的东西」（范围读结果与全量读逐字节相同），而接线前两者本来就相同。
真正的失败信号在 Task 7 的第三组断言（损坏落在区间外时新旧模式**必须不同**），
它依赖 Task 3 的实现，所以「先红后绿」发生在那里。这条写在 Task 3 Step 2 里，
不假装它是先红后绿的。
