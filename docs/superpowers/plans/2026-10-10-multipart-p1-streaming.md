# P1 流式写路径 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 PUT 写路径从「整份对象缓冲在内存里」改成流式，使峰值内存与对象大小脱钩。

**Architecture:** 三层各改一处——磁盘层补一个追加原语（`DiskAPI::append`），分片写入器
改成逐块追加（`BitrotShardWriter::push_block` 因此变成 `async`），store 层的
`put_object` 收 `AsyncRead` 而不是 `Vec<u8>`。改造后**行为必须逐字节不变**，由既有的
210 个测试证明。

**Tech Stack:** Rust 2021、tokio（`AsyncRead`）、async-trait、s3s 0.17、Reed–Solomon 纠删编码。

---

## 这个计划只覆盖 P1

设计文档 `docs/superpowers/specs/2026-10-10-multipart-design.md` 分了 P1–P4 四个阶段。
**本文只做 P1（流式写路径）**，理由写在设计文档 §10 与 §11：P1 动的是
quorum / commit / bitrot 所在的那条路，必须**行为零变化**地落地、由既有 210 个测试证明，
然后 P2 才在它上面盖楼。

P2（Create / UploadPart / Complete / Abort 与限制校验）、P3（ListParts /
ListMultipartUploads / UploadPartCopy / CopyObject）、P4（文档同步）**另起一份计划**。
那份计划会更准——它可以直接读到 P1 建成的 `PutArgs` 真实签名。

## 与设计文档的三处偏离（都是被实现细节推翻的）

| 设计文档 | 本文 | 为什么 |
|---|---|---|
| §3.1a：加 `create_writer` / `DiskWriter` 句柄，`finish()` 持有 fsync | 改为无状态的 `DiskAPI::append(rel_path, data)` | `LocalDisk` 的每个方法都是「`spawn_blocking` 跑一个无状态闭包」（`local.rs:50-59`）。要让句柄跨调用存活，得把 `File` 在每次 `spawn_blocking` 前后 take/put 出来，多出一条 `expect` 的 panic 路径，而它买到的只是「少几次 open」——本地盘上微秒级。`append` 保住了设计文档要的东西（`RemoteDisk` 将来把每次调用映射成一个 RPC 分片），实现却短得多 |
| §3.1c：`PutStreamArgs { body, size: Option<u64>, etag }` | 沿用既有名字 `PutArgs`；`size` 字段**不要** | 窥探一个块（≤ `BLOCK_SIZE`）就足以定尺寸，`size` 提示是多余的优化（YAGNI）。`etag` 保留。另外 body 的约束从 `+ Send + Sync` 收到 `+ Send`：trait 方法的返回值要 `Send`，参数只要 `Send` 就够了，`Sync` 是白加的限制 |
| §3.2：写一个「分配计数探针」断言峰值分配 | 改为 `RecordingDisk`：记录每次 `write_all` / `append` 的 payload 长度，断言最大值 ≤ `BLOCK_SIZE + 32` | **「分配计数探针」在本仓库做不出来**：它需要一个 `GlobalAlloc` 实现，而 `GlobalAlloc` 必须 `unsafe`，workspace lint 是 `unsafe_code = "forbid"`。记录写入长度是同一件事的可实现版本——整份缓冲会表现为「一次巨大的 `write_all`」，照样被抓 |

这三处**要先回写进设计文档**（Task 5 的最后一步），按项目「不允许文档漂移」的规矩。

## 文件结构

| 文件 | 职责 | 本计划对它做什么 |
|---|---|---|
| `crates/disk/src/fsx.rs` | 阻塞版文件系统原语 | 加 `append_all` |
| `crates/disk/src/lib.rs` | `DiskAPI` 契约 + 共享契约测试 | 加 `append` 方法；契约测试加追加语义 |
| `crates/disk/src/local.rs` | 本地盘实现 | 实现 `append` |
| `crates/disk/src/faulty.rs` | 故障注入盘 | 实现 `append`（沿用既有的 payload 变换） |
| `crates/store/src/writer.rs` | bitrot 分片写入器 | `push_block` 改 async、逐块落盘 |
| `crates/store/src/put.rs` | PUT 路径 | `PutArgs` 收流；`put_object` 流式改造 |
| `crates/store/src/reader.rs` | 读路径 | 只改测试里的 `push_block` 调用点 |
| `crates/store/src/testutil.rs` | store 测试夹具 | 加 `RecordingDisk` |
| `crates/api/src/lib.rs` | 存储契约 trait | `put_object` 换签名（`PutRequest`） |
| `crates/api/Cargo.toml` | — | 加 `tokio` |
| `crates/server/src/wiring.rs` | 组合根 | 适配新签名 |
| `crates/s3/src/mock.rs` | 内存版 `ObjectStore` | 适配新签名（读干 body） |
| `crates/s3/src/impl_s3.rs` | S3 协议层 | PUT 把 `StreamingBlob` 转成 `AsyncRead`；`NopStore` 适配 |
| `crates/s3/Cargo.toml` | — | `tokio` 从 dev-dependencies 挪到 dependencies；加 `tokio-util` |

**命名约定**：本计划一律用「块」（block，`BLOCK_SIZE` = 1 MiB 的编码块）指
`write_shards` 循环里的单元，与 S3 的「分片 / part」区分开——后者是 P2 的事。

---

### Task 1: 磁盘层追加原语

**Files:**
- Modify: `crates/disk/src/fsx.rs`（在 `write_all_fsync` 之后插入）
- Modify: `crates/disk/src/lib.rs:37`（`DiskAPI` 加方法）、`crates/disk/src/lib.rs:57-136`（契约测试）
- Modify: `crates/disk/src/local.rs:63-68` 附近
- Modify: `crates/disk/src/faulty.rs:140-166` 附近

- [ ] **Step 1: 写失败的契约测试**

先加断言再加实现——契约测试跑在 `LocalDisk` 与 `FaultyDisk` 两边，一处写、两处证。

在 `crates/disk/src/lib.rs` 的 `contract_tests::run_all` 里，把「sync_file_and_parent 幂等」
那段（当前 123–129 行）**之前**插入：

```rust
        // 追加语义：这正是 `write_all` 做不到、而流式分片落盘依赖的那一点。
        // 三次调用必须等价于「一次写完 abcdef」。
        let app = "__contract__/append";
        disk.append(app, b"ab").await.expect("first append");
        disk.append(app, b"cd").await.expect("second append");
        disk.append(app, b"ef").await.expect("third append");
        let got = disk
            .read_exact_at(app, 0, 6)
            .await
            .expect("read back appended content");
        assert_eq!(
            got.as_slice(),
            b"abcdef",
            "append must concatenate; truncating would leave only the last chunk"
        );

        // 与 `write_all` 的差别要在同一个测试里钉住：同一路径上再 write_all 一次，
        // 必须把 6 字节截断回 3 字节。没有这条，一个「append 其实就是 write_all
        // 的别名」的实现也能通过上面那三行（因为 write_all 会 truncate 到 bcdef... 不对，
        // 会截断到 2 字节，所以其实上面就会红——但**明确**钉住两者差异更值）。
        disk.write_all(app, b"XYZ").await.expect("write_all truncates");
        let got = disk
            .read_exact_at(app, 0, 3)
            .await
            .expect("read back truncated content");
        assert_eq!(got.as_slice(), b"XYZ", "write_all must truncate");
```

- [ ] **Step 2: 跑测试确认它失败**

```bash
cargo test -p rstore-disk passes_shared_contract_suite
```

Expected: 编译失败，`no method named 'append' found for type parameter 'D'`。
（`FaultyDisk` 的契约测试同样会红——它跑的是同一个 `run_all`。）

- [ ] **Step 3: 加 `fsx::append_all`**

在 `crates/disk/src/fsx.rs` 的 `write_all_fsync`（当前结束于 114 行）之后插入：

```rust
/// 追加写文件（自动创建父目录），**不 fsync**。
///
/// 与 [`write_all_fsync`] 的两点差别都是刻意的：
///
/// 1. `OpenOptions::append(true)` 而不是 `File::create`——后者**创建即截断**，
///    逐块追加会把前一块抹掉，而且每次调用都返回 `Ok`，调用方看不出任何异常。
/// 2. **不 fsync**：`append` 会被调用成百上千次（每次一个块），逐次 fsync 等于
///    把整个写路径钉在磁盘转速上。耐久性由调用方在最后一块之后用
///    [`sync_file_and_parent`] 统一负责。
pub fn append_all(root: &Path, rel: &str, data: &[u8]) -> Result<(), DiskError> {
    let path = resolve(root, rel)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(map_io)?;
    }
    let mut file = fs::OpenOptions::new()
        .append(true)
        .create(true)
        .open(&path)
        .map_err(map_io)?;
    file.write_all(data).map_err(map_io)?;
    Ok(())
}
```

在 `fsx.rs` 的 `mod tests` 里加一条：

```rust
    #[test]
    fn append_concatenates_while_write_all_truncates() {
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        append_all(root, "p", b"aa").unwrap();
        append_all(root, "p", b"bb").unwrap();
        assert_eq!(std::fs::read(root.join("p")).unwrap(), b"aabb");
        write_all_fsync(root, "p", b"cc").unwrap();
        assert_eq!(
            std::fs::read(root.join("p")).unwrap(),
            b"cc",
            "write_all 必须仍然是截断语义"
        );
    }
```

`fsx.rs` 的测试模块目前没有 `tempfile` 导入——`tempfile` 已经是 `rstore-disk` 的
dev-dependency（见 `crates/disk/Cargo.toml`），直接用 `tempfile::tempdir()` 即可。

- [ ] **Step 4: 在 `DiskAPI` 上声明 `append`**

在 `crates/disk/src/lib.rs` 的 `write_all` 声明（37 行）之后插入：

```rust
    /// 追加写。逐块落盘的分片写入器用它（见 `rstore_store::writer`）。
    ///
    /// **必须与 [`Self::write_all`] 区分开**：`write_all` 创建即截断，对同一路径
    /// 反复调用只会留下最后一次；`append` 则等价于「顺序写完整个文件」。
    /// **不 fsync**：调用方在最后一块之后用 [`Self::sync_file_and_parent`] 收尾。
    async fn append(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError>;
```

- [ ] **Step 5: 实现 `LocalDisk::append`**

在 `crates/disk/src/local.rs` 的 `write_all`（63–68 行）之后插入：

```rust
    async fn append(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError> {
        let root = Arc::clone(&self.root);
        let rel = rel_path.to_owned();
        let data = data.to_vec();
        run_blocking(move || fsx::append_all(&root, &rel, &data)).await
    }
```

- [ ] **Step 6: 实现 `FaultyDisk::append`**

`FaultyDisk` 的 payload 变换必须在**两条写路径上一致**——否则「用 `append` 的实现」
会绕过 `Truncate` / `PartialWrite` / `CorruptBytes` 这三种故障，故障注入测试就失去意义了。
在 `crates/disk/src/faulty.rs` 的 `write_all`（140–166 行）之后插入：

```rust
    async fn append(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError> {
        self.begin_call()?;

        // 与 `write_all` 相同的 payload 变换。分叉会让走 `append` 的实现
        // 悄悄绕过 Truncate / PartialWrite / CorruptBytes。
        match self.current_fault() {
            Some(Fault::DropWrites) => Ok(()),
            Some(Fault::PartialWrite) => {
                let half = data.len() / 2;
                self.inner.append(rel_path, &data[..half]).await
            }
            Some(Fault::Truncate { len }) => {
                let n = len.min(data.len());
                self.inner.append(rel_path, &data[..n]).await
            }
            Some(Fault::CorruptBytes { at, mask }) => {
                let mut buf = data.to_vec();
                if let Some(byte) = buf.get_mut(at) {
                    *byte ^= mask;
                }
                self.inner.append(rel_path, &buf).await
            }
            _ => self.inner.append(rel_path, data).await,
        }
    }
```

- [ ] **Step 7: 跑测试**

```bash
cargo test -p rstore-disk
```

Expected: 全绿。两条契约测试（`LocalDisk` 与 `FaultyDisk` 各一条
`passes_shared_contract_suite*`）都必须通过新加的四条 `assert_eq!`。

- [ ] **Step 8: 提交**

```bash
git add crates/disk/
git commit -m "feat(disk): 追加原语 append，并与 write_all 的截断语义在同一处钉住

流式分片落盘需要「顺序追加」而不是「创建即截断」。契约测试把两者的差别
放在同一个用例里断言（append 三次 = abcdef；再 write_all 一次截回 XYZ），
免得日后有人把 append 实现成 write_all 的别名。

FaultyDisk 的 payload 变换（Truncate/PartialWrite/CorruptBytes）在两条写
路径上保持一致——分叉会让走 append 的实现绕过故障注入。"
```

---

### Task 2: `BitrotShardWriter` 逐块落盘

**Files:**
- Modify: `crates/store/src/writer.rs`（整份重写 `push_block` / `finish`）
- Modify: `crates/store/src/put.rs:351`（调用点）
- Modify: `crates/store/src/reader.rs:126`（测试调用点）

**这一步之后写路径的内存上界就是 `BLOCK_SIZE`，但 `write_shards` 仍然从
`Vec<u8>` 取数据——Task 3 才让它收流。** 两步分开是为了让「写入器改对了没」有独立判据。

- [ ] **Step 1: 改掉文件头的解释性注释**

`crates/store/src/writer.rs:16-19` 现在写着「必须是缓冲 + 一次落盘，因为 DiskAPI 没有
append」。这条前提**已经作废**（Task 1 加了 `append`），留着就是漂移。替换为：

```rust
/// 把一连串 block 组装成一份 bitrot 分片，**逐块追加**落盘。
///
/// 块间布局不变，仍是 `[hash(32B)][data]`（DESIGN §11）。变的是落盘方式：
/// 每块 `append` 一次，而不是攒在 `Vec` 里一次写完——于是峰值内存从
/// 「整份分片」降到「一个块」。
///
/// **为什么是 `append` 而不是反复 `write_all`**：`write_all` 的底层是
/// `File::create`（创建即截断），每块调一次会把前一块抹掉，且每次调用都返回
/// `Ok`，调用方看不出异常。见 `rstore_disk::DiskAPI::append` 的文档。
///
/// 崩溃时留下的是 `.staging-*` 里的半截文件——staging 目录本来就被发现逻辑
/// 跳过、由对账回收（DESIGN §12.3），所以**不引入新的崩溃点语义**。
```

同时把结构体里的 `buf: Vec<u8>` 字段删掉（连同它的注释）。

- [ ] **Step 2: 写失败的测试：落盘必须是渐进的**

在 `writer.rs` 的 `mod tests` 里加：

```rust
    /// 逐块落盘的核心断言：推入一个块之后，盘上就该有它——不必等 `finish`。
    ///
    /// 没有这条，「把整份分片攒在内存里」的实现照样能通过其余所有测试
    /// （布局、长度、_size 一致性全都对），而那正是本次要拆掉的那个放大器。
    #[tokio::test]
    async fn each_block_reaches_disk_before_finish() {
        let (tmp, disk) = temp_disk();
        let path = tmp.path().join("part.1");

        let mut w = BitrotShardWriter::new(Arc::clone(&disk), "part.1".into(), 1024);
        assert!(
            !path.exists(),
            "构造写入器本身不该创建文件：没有数据就没有分片"
        );

        w.push_block(&[1u8; 1024]).await.unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            1024 + 32,
            "第一个块必须在 push_block 返回时就已在盘上"
        );
        assert!(
            w.payload_len() == 1024,
            "payload_len 仍是已推入的原始字节数，与落盘进度无关"
        );

        w.push_block(&[2u8; 1024]).await.unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            2 * (1024 + 32),
            "第二个块追加在第一个之后"
        );

        w.finish().await.unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 2 * (1024 + 32));
    }
```

- [ ] **Step 3: 跑测试确认它失败**

```bash
cargo test -p rstore-store each_block_reaches_disk_before_finish
```

Expected: 编译失败——`push_block` 现在返回的 future 没被 await 的写法会有类型错误，
且 `push_block(&[1u8; 1024]).await` 上会报「`()` is not a future」（因为当前
`push_block` 是同步的）。改完 Step 4 之后才会红在断言上。

- [ ] **Step 4: 改实现**

把 `crates/store/src/writer.rs` 的 `push_block`（50–77 行）与 `finish`（85–93 行）
替换为：

```rust
    /// 追加一个 block：把 `[hash(32B)][data]` 追加到分片文件末尾。
    /// `data.len() <= block_size`；短块只能出现在末尾；空块一律拒绝。
    ///
    /// 校验**全部在落盘之前**做完：中途返回错误时盘上已有前几块，但那是暂存目录里的
    /// 半截分片，由对账回收；**绝不能**在校验通过前写入，否则「一个坏块」会变成
    /// 「一份内容不可信的分片」。
    pub async fn push_block(&mut self, data: &[u8]) -> Result<(), StoreError> {
        if data.is_empty() {
            return Err(StoreError::ShardLayout(
                "empty block: zero-byte blocks have no writer/reader representation".into(),
            ));
        }
        if data.len() > self.block_size {
            return Err(StoreError::ShardLayout(format!(
                "block of {} bytes exceeds block_size {}",
                data.len(),
                self.block_size
            )));
        }
        if self.short_seen {
            return Err(StoreError::ShardLayout(
                "block pushed after a short block; a short block may only be the last one".into(),
            ));
        }

        // 摘要与数据一起追加：两次 `append` 会让读侧在中间看到半条记录，
        // 而崩溃恰好发生在那时就会留下一个「长度合法、内容错位」的分片。
        let mut framed = Vec::with_capacity(32 + data.len());
        framed.extend_from_slice(&bitrot_hash(data));
        framed.extend_from_slice(data);
        self.disk.append(&self.rel_path, &framed).await?;

        self.payload_len += data.len() as u64;
        // 短块只能收尾：置位后任何后续 push_block 都会被上面拦下。
        if data.len() < self.block_size {
            self.short_seen = true;
        }
        Ok(())
    }
```

```rust
    /// 收尾：必要时落一个空文件，再 fsync 文件与父目录。
    /// 逐块 `append` 都不 fsync，耐久性在这里一次性兑现；
    /// 顺序不可颠倒（文件 → 父目录），否则崩溃后文件内容在盘上、目录里却没有它。
    pub async fn finish(self) -> Result<(), StoreError> {
        // 一个块都没推过时，盘上还不存在这个文件——逐块 `append` 只有在真的写了
        // 字节时才会创建它。但**空分片在读侧是合法的**：`bitrot_size(0, bs) == 0`，
        // 而读取器是靠 `stat` 判存在性的，文件缺失会被报成 `NotFound`
        // （参与 quorum 时计为「缺失」，是另一个语义）。
        // 缓冲版的实现靠 `write_all` 的「创建即截断」顺手落下了这个 0 字节文件，
        // 改成逐块追加后这个副作用没了，必须显式补回来。
        // 这里用 `write_all` 而不是 `append(&[])`：前者会把已存在的路径清空，
        // 与「这份分片就是空的」严格一致；`append` 遇到残留内容会留下垃圾。
        if self.payload_len == 0 {
            self.disk.write_all(&self.rel_path, &[]).await?;
        }
        self.disk.sync_file_and_parent(&self.rel_path).await?;
        Ok(())
    }
```

> **为什么 `finish` 不能只是 fsync（实测踩过）**：`writer::layout_agrees_with_shared_bitrot_size`
> 的第一个用例是 `(0, 1024)`，`reader::empty_shard_reads_to_empty` 也直接构造空写入器——
> 两条都会以 `Disk(NotFound)` 失败。凡「空 payload 落在盘上必须有文件」的契约都靠这一步兜住。

`bitrot_hash` 的返回值是 `[u8; 32]`（`rstore_checksum::HASH_LEN`），
`32 + data.len()` 里的字面量 32 直接换成 `rstore_checksum::HASH_LEN`：

```rust
        let mut framed = Vec::with_capacity(rstore_checksum::HASH_LEN + data.len());
```

并在 `use` 块里把 `use rstore_checksum::bitrot_hash;` 保持不变（`HASH_LEN` 用全路径引用）。

- [ ] **Step 5: 更新 `writer.rs` 里既有的三个测试**

三处 `w.push_block(...)` 都要 `.await`：

- `writes_interleaved_hash_and_data`（118–132 行）：`w.push_block(&[7u8; 1024]).unwrap();`
  → `w.push_block(&[7u8; 1024]).await.unwrap();`，第二行同理。
- `layout_agrees_with_shared_bitrot_size`（146–152 行）：`chunk` 是
  `payload.chunks(bs)` 借出来的 `&[u8]`，`w.push_block(chunk).unwrap();`
  → `w.push_block(chunk).await.unwrap();`
- `rejects_misuse_that_would_desync_the_reader`（167–186 行）：这个测试**是同步的**
  （`#[test]`），而 `push_block` 现在要 await。把它改成 `#[tokio::test]` /
  `async fn`，四处 `w.push_block(...)` 加 `.await`。
  **错误路径的断言必须仍然成立**——四条 `matches!(..., Err(StoreError::ShardLayout(_)))`
  一条都不许放松。

- [ ] **Step 6: 更新 `put.rs:351` 与 `reader.rs:126` 的调用点**

`crates/store/src/put.rs:351`：

```rust
                    w.push_block(shard)?;
```
→
```rust
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
```

> **为什么 `?` 是错的（实测踩过）**：`put::tests::put_fails_below_write_quorum`、
> `quorum_boundaries::matrix_4_plus_2`、`delete::tests::gc_keeps_old_dir_where_the_new_meta_never_landed`
> 三条都会以 `Disk(Transient(Io))` 失败——它们注入的是离线盘，期望的是
> `WriteQuorum` 或「降级成功」，不是把第一次 IO 错误当成整次 PUT 的结局。
> 注意 `let failed = ...` 用 `match` 而不是 `if let Some(w) = ... else` 是**必需的**：
> 得先把可变借用在 match 表达式结束时放掉，下一行才能整体写 `writers[physical] = None`。

`crates/store/src/reader.rs:126`（测试夹具里的 `w.push_block(chunk).unwrap();`）：

```rust
            w.push_block(chunk).await.unwrap();
```

- [ ] **Step 7: 跑测试**

```bash
cargo test -p rstore-store
```

Expected: 全绿，包括 `put.rs` 的三条既有 PUT 测试
（`put_small_object_inlines_it` / `put_large_object_creates_shards` /
`put_fails_below_write_quorum`）——它们断言的**字节布局**必须一字不变。

- [ ] **Step 8: 提交**

```bash
git add crates/store/src/writer.rs crates/store/src/put.rs crates/store/src/reader.rs
git commit -m "perf(store): 分片写入器逐块落盘，峰值内存降到单个块

push_block 因此变成 async（要 await append），三处调用点同步更新。
新增 each_block_reaches_disk_before_finish 钉住「渐进落盘」——没有它，
一个把整份分片攒在内存里的实现能通过其余所有测试。

文件头那条『必须是缓冲 + 一次落盘，因为 DiskAPI 没有 append』的前提已被
上一提交推翻，一并改掉。"
```

---

### Task 3: `PutArgs` 收流

**Files:**
- Modify: `crates/store/src/put.rs`（`PutArgs`、`put_object`、`write_shards`）
- Modify: `crates/store/src/put.rs` 的 `mod tests`（4 处 `PutArgs` 构造）

- [ ] **Step 1: 写失败的测试**

在 `crates/store/src/put.rs` 的 `mod tests` 里加一个夹具与两条测试：

```rust
    /// 内存 body。**不要在这里定义**——放到 `crate::testutil`（Step 7 有说明），
    /// 七个测试模块都要用它。这里只需 `use crate::testutil::{body, set_with_disks, TestSet};`。

    /// 流式入口必须与原入口逐字节等价：同样的字节进去，同样的 etag / size / 盘上布局。
    #[tokio::test]
    async fn streaming_put_matches_buffered_put() {
        let set = set_with_disks(6, 2).await;
        let data: Vec<u8> = (0..1_500_000u32).map(|i| (i % 251) as u8).collect();
        let out = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "streamed".into(),
                body: body(data.clone()),
                etag: None,
            })
            .await
            .unwrap();

        assert_eq!(out.size, 1_500_000);
        // etag 必须还是真 MD5——边读边算的实现在这里最容易退化成「半个对象的 MD5」。
        assert_eq!(out.etag, etag_of(&data));

        // 盘上分片长度与读侧独立复算的那个数一致（与 put_large_object_creates_shards 同款断言）。
        let step = shard_step(out.size, 4);
        let expect_on_disk =
            rstore_checksum::bitrot_size(expected_shard_len(out.size, 4), step);
        for i in 0..6 {
            let d = set.disks()[i].as_ref().unwrap();
            let st = d
                .stat(&format!("b/streamed/{}/part.1", out.data_dir))
                .await
                .unwrap()
                .expect("part.1 必须存在");
            assert_eq!(st.size, expect_on_disk, "disk {i}");
        }
    }

    /// 调用方给了 etag 就必须用它，而不是算出来的 MD5。P2 的 multipart Complete
    /// 靠这条把合成的 `-N` 形式写进对象。
    #[tokio::test]
    async fn supplied_etag_wins_over_computed_md5() {
        let set = set_with_disks(6, 2).await;
        let data = vec![3u8; 1_500_000];
        let out = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "given".into(),
                body: body(data.clone()),
                etag: Some("d41d8cd98f00b204e9800998ecf8427e-2".into()),
            })
            .await
            .unwrap();
        assert_eq!(out.etag, "d41d8cd98f00b204e9800998ecf8427e-2");
        assert_ne!(out.etag, etag_of(&data), "必须不是算出来的那个");
    }
```

- [ ] **Step 2: 跑测试确认它失败**

```bash
cargo test -p rstore-store streaming_put_matches_buffered_put
```

Expected: 编译失败——`PutArgs` 没有 `body` / `etag` 字段。

- [ ] **Step 3: 改 `PutArgs`**

`crates/store/src/put.rs:37-42` 替换为：

```rust
/// 一次写入的输入。
///
/// **不复用 `Debug` / `Clone` / `PartialEq`**：`body` 是流，这三者都派不出来
/// （改造前它持有 `Vec<u8>`，所以曾经可以）。
pub struct PutArgs {
    pub bucket: String,
    pub key: String,
    /// 请求体。**读一次就没了**——消费方不该假设它能重放。
    pub body: Box<dyn AsyncRead + Unpin + Send>,
    /// `Some` = 直接用这个 etag（P2 的 multipart Complete 传合成值）；
    /// `None` = 边读边算整份内容的 MD5。
    pub etag: Option<String>,
}
```

在文件顶部（`use` 区）加：

```rust
use tokio::io::{AsyncRead, AsyncReadExt};
```

- [ ] **Step 4: 加「读一个块」的辅助函数**

在 `etag_of`（81–84 行）之后插入：

```rust
/// 读「至多一个块」：读满 `BLOCK_SIZE` 或到 EOF 为止，两者取先到者。
///
/// 返回长度 `< BLOCK_SIZE` 就说明已经读到流末尾——调用方据此判定「没有更多了」。
/// **这是整条写路径唯一的缓冲点**，上界就是 `BLOCK_SIZE`。
///
/// 不用 `AsyncReadExt::take().read_to_end()`：`take` 消费接收者，而我们需要
/// 在下一轮继续读同一个流，改回来要写 `(&mut *body).take(..)` 这类借用体操。
/// 逐次 `read` 到填满更直白，也与 `fsx::read_exact_at` 的循环同一种写法。
async fn read_block(body: &mut (dyn AsyncRead + Unpin + Send)) -> Result<Vec<u8>, StoreError> {
    let mut buf = vec![0u8; BLOCK_SIZE];
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
    Ok(buf)
}
```

- [ ] **Step 5: 改 `put_object` 的前半段**

`crates/store/src/put.rs:170-207` 替换为：

```rust
    /// 写入一个对象并提交。达到 `write_quorum` 才算成功。
    ///
    /// **流式**：请求体逐块读入、逐块编码落盘，峰值内存与对象大小无关（上界
    /// `BLOCK_SIZE` + 一份分片）。
    pub async fn put_object(&self, args: PutArgs) -> Result<PutOut, StoreError> {
        let PutArgs {
            bucket,
            key,
            mut body,
            etag,
        } = args;
        let total = self.total();
        let data_shards = self.data();
        let write_quorum = self.write_quorum();

        let txid = Uuid::new_v4();
        let data_dir = Uuid::new_v4();
        let version_id = Uuid::new_v4();
        let key_rel = format!("{bucket}/{key}");
        let staging = format!("{key_rel}/.staging-{txid}");
        let final_rel = format!("{key_rel}/{data_dir}");

        // 槽位映射：`dist[k] - 1` 是第 k 号分片所在的物理盘下标。**必须把 bucket
        // 一起喂进去**，只用 key 的话同名对象在不同桶里会退化成同一套分布。
        let dist = distribution(&key_rel, total).map_err(|e| {
            StoreError::Internal(format!("distribution for {key_rel} with n={total}: {e}"))
        })?;
        // 排列不自洽说明分布函数坏了，别把未经验证的排列带到写路径上。
        if !is_valid_distribution(&dist) {
            return Err(StoreError::Internal(format!(
                "distribution for {key_rel} is not a permutation: {dist:?}"
            )));
        }

        // 先数盘：可用盘不足 quorum 直接拒绝——先编码再发现写不下去等于白烧 CPU。
        let available = self.available_disks() as u8;
        if available < write_quorum {
            return Err(StoreError::WriteQuorum {
                achieved: available,
                required: write_quorum,
            });
        }

        // 先读一个块。**这一步同时定尺寸**：读不满一个满块就说明整个对象到此为止，
        // 于是 `known_len` 是精确值；读满了则说明对象至少一个块，后续长度不影响
        // `shard_step`（它只看 `size.min(BLOCK_SIZE)`，见该函数的文档）。
        let first = read_block(&mut *body).await?;
        let known_len = (first.len() < BLOCK_SIZE).then_some(first.len() as u64);

        if known_len.is_some_and(|s| should_inline(s, /* versioned_bucket = */ false)) {
            // 内联分支：数据进 meta.xl，不产生 part.*。阈值只此一份来源，
            // 绝不在本文件里再写一个常量。
            let size = first.len() as u64;
            let etag = etag.unwrap_or_else(|| etag_of(&first));
            let mut inline = InlineData::new();
            inline.insert("null", first);
            let mut flags = Flags::empty();
            flags.insert(Flags::INLINE_DATA);
            let body_meta = ObjectBody {
                id: Some(data_dir),
                parts: Vec::new(),
                ec_dist: Vec::new(),
                checksum_algo: ChecksumAlgo::Crc32c,
                storage_class: StorageClass::Standard,
                meta_user: BTreeMap::new(),
                meta_sys: [(keys::INLINE_DATA.to_string(), Vec::new())]
                    .into_iter()
                    .collect(),
            };
            let mut meta =
                build_meta(size, data_shards, total, data_dir, version_id, flags, body_meta)?;
            meta.inline = inline;
            let bytes = encode(&meta)?;
            self.write_meta_all(&staging, &bytes).await;

            commit(self, &staging, &final_rel, write_quorum).await?;
            gc_superseded(self, &bucket, &key, &data_dir.to_string()).await;

            return Ok(PutOut {
                size,
                etag,
                data_dir,
                version_id,
            });
        }

        // 分片分支。
        //
        // `shard_step` 只需要 `size.min(BLOCK_SIZE)`：已知总长时传它；未知时说明
        // 对象至少有一个满块，`min` 的结果恒为 `BLOCK_SIZE`，传哪个够大的值都一样。
        let step = shard_step(known_len.unwrap_or(BLOCK_SIZE as u64), data_shards);

        let (shard_len, size, etag) = self
            .write_shards_stream(&mut *body, first, &dist, &staging, write_quorum, step, etag)
            .await?;

        let mut flags = Flags::empty();
        // 本版本确实用了数据目录（目录名就是 data_dir）。
        flags.insert(Flags::USES_DATA_DIR);
        let body_meta = ObjectBody {
            id: Some(data_dir),
            parts: vec![PartInfo {
                number: 1,
                size: shard_len,
                actual_size: size,
                etag: etag.clone(),
                index: None,
            }],
            ec_dist: dist.clone(),
            checksum_algo: ChecksumAlgo::Crc32c,
            storage_class: StorageClass::Standard,
            meta_user: BTreeMap::new(),
            meta_sys: BTreeMap::new(),
        };
        let meta = build_meta(size, data_shards, total, data_dir, version_id, flags, body_meta)?;
        let bytes = encode(&meta)?;
        self.write_meta_all(&staging, &bytes).await;

        // 提交：rename 达到 quorum 才算成功；低于时由 `commit` best-effort 回滚
        // 它自己 rename 过去的那些目录。
        commit(self, &staging, &final_rel, write_quorum).await?;

        // **先提交、后 GC，顺序不可颠倒**：反过来就是在删还没提交的数据。
        // 这是覆盖写的收尾——本版本（`data_dir`）已胜出，旧目录才是待回收的。
        // best-effort：GC 自身不上抛（见 `gc_superseded`）。
        gc_superseded(self, &bucket, &key, &data_dir.to_string()).await;

        Ok(PutOut {
            size,
            etag,
            data_dir,
            version_id,
        })
    }
```

- [ ] **Step 6: 把 `write_shards` 换成 `write_shards_stream`**

`crates/store/src/put.rs:283-369`（整个 `write_shards`）替换为：

```rust
    /// 分片写入：从 `body` 逐块读、逐块编码、逐块追加到各盘的 `part.1`。
    ///
    /// 返回 `(每块盘上的分片字节数, 对象总长, etag)`。分片字节数是读侧构造读取器
    /// 时要的 `shard_len`，由 `expected_shard_len` 用**实际读到的总长**复算——
    /// 与读侧共用同一个函数，两边各算各的必然漂移。
    ///
    /// `first` 是调用方已经读出来的第一个块（可能已到 EOF，即一个短块）。
    ///
    /// 低于 quorum 直接返回错误，**且不清理已写的分片**：残留由对账流程回收
    /// （DESIGN §12.2）。主动清理反而会把崩溃残留抹掉，让对账的可回收性无处可测。
    async fn write_shards_stream(
        &self,
        body: &mut (dyn AsyncRead + Unpin + Send),
        first: Vec<u8>,
        dist: &[u8],
        staging: &str,
        write_quorum: u8,
        step: u64,
        etag: Option<String>,
    ) -> Result<(u64, u64, String), StoreError> {
        let data_shards = self.data();
        let parity = self.parity();
        let step = step as usize;
        let part_rel = format!("{staging}/part.1");

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
        let mut block = first;

        loop {
            // 空块只可能在 EOF 时出现（且此时前面的判断已经 break），
            // 留着这道闸是为了不把「空块」喂进 `push_block`——它会当成布局错误拒绝。
            if block.is_empty() {
                break;
            }

            md5.update(&block);
            let shard_size_k =
                even_ceil((block.len() as u64).div_ceil(u64::from(data_shards))) as usize;

            // 数据分片：按 `shard_size_k` 切，不足处补零。补零只可能落在最后一个
            // 分片的尾部，这正是读侧「顺序相接再截断到 L_k」的依据。
            let mut shards: Vec<Vec<u8>> = (0..data_shards as usize)
                .map(|i| {
                    let s = i * shard_size_k;
                    let mut shard = vec![0u8; shard_size_k];
                    if s < block.len() {
                        let e = (s + shard_size_k).min(block.len());
                        shard[..e - s].copy_from_slice(&block[s..e]);
                    }
                    shard
                })
                .collect();

            let codec = self
                .codec_cache()
                .get(data_shards as usize, parity as usize, shard_size_k)
                .map_err(|e| {
                    StoreError::Internal(format!(
                        "codec geometry ({data_shards}, {parity}, {shard_size_k}): {e}"
                    ))
                })?;
            let parity_shards = codec
                .encode(&shards)
                .map_err(|e| StoreError::Internal(format!("erasure encode: {e}")))?;
            // 分片编号沿用 `encode` 的输出顺序：`0..data` 数据分片，`data..N` 校验分片。
            shards.extend(parity_shards);

            // 分片 `kk` 落在盘 `dist[kk] - 1`。每块盘每块恰好收到一份（`dist` 是排列）。
            for (kk, shard) in shards.iter().enumerate() {
                let physical = usize::from(dist[kk] - 1);
                // 与 Task 2 Step 6 同一条规则：布局错误中止，盘 IO 失败只丢这一票。
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

            size += block.len() as u64;
            // 短块就是最后一块——这里 break，绝不再去读流，否则会多等一次 IO。
            if block.len() < BLOCK_SIZE {
                break;
            }
            block = read_block(body).await?;
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

        // 调用方给了 etag 就用它（P2 的 multipart Complete）；否则用边读边算的 MD5。
        let etag = etag.unwrap_or_else(|| format!("{:x}", md5.finalize()));
        let shard_len = expected_shard_len(size, data_shards);
        Ok((shard_len, size, etag))
    }
```

`md5` 的导入：`put.rs` 的 `etag_of`（81–84 行）内部已有
`use md5::{Digest, Md5};`，那是函数内局部导入。把 `Md5::new()` / `md5.update` /
`md5.finalize()` 用在同一文件里，需要把导入提到文件顶部 `use` 区：

```rust
use md5::{Digest, Md5};
```

并把 `etag_of` 里的那行局部 `use` 删掉。`md-5` 已是 `rstore-store` 的依赖
（`crates/store/Cargo.toml`）。

**`etag` 不是这个方法的参数**（原计划把它列进来了，实测两者都错）：

- **clippy 对方法会把 `&self` 计进 `too_many_arguments`**（默认阈值 7），所以
  **方法**的名额只有 6 个，而自由函数才是 7 个。原计划拿 `build_meta`（自由函数、
  7 个参数、无 allow）当参照，那是错的——照抄会直接 `-D warnings` 失败。
- 更顺的修法不是加 `#[allow]`，而是**把 `etag` 挪出签名**：写入器的职责是
  「写下去 + 算摘要」，「用算出来的还是调用方给的」是策略。内联分支本来就是这个分派
  （`etag.unwrap_or_else(|| etag_of(&first))`），挪出去两支就对称了。
  于是返回 `(shard_len, size, md5)`，调用方 `let etag = etag.unwrap_or(md5);`。
- 改完参数是 `body` / `first` / `dist` / `staging` / `write_quorum` / `step` 六个，
  加 `&self` 恰好 7，不报警。

- [ ] **Step 7: 更新**全 crate**的 `PutArgs` 构造点**

**原计划只数了 `put.rs` 里的 4 处，那是错的**——`PutArgs` 是全 crate 的测试夹具，
`cargo test -p rstore-store` 会一次性暴露其余 12 处。清单：

| 位置 | 形态 | 改法 |
|---|---|---|
| `put.rs` 的 3 处直构造 | `data: …` | `body: body(…), etag: None` |
| `bucket.rs:134` / `list.rs:153` | `fn put_args(bucket, key, n)` 辅助函数 | 函数体内改一次 |
| `delete.rs:119` / `get.rs:480` | `fn put_args(bucket, key, data)` 辅助函数 | 函数体内改一次 |
| `quorum_boundaries.rs` 3 处直构造 | `data: payload(n)` / `data: data.clone()` | 见下 |
| `reconcile.rs` 4 处直构造 | 同上 | 同上 |

`quorum_boundaries.rs` 里**已有一个本地 `fn body(seed: u8) -> Vec<u8>`**（造 payload 用），
与本节新加的共享 helper 撞名。把它改名为 `payload`——它本来就是造 payload 的，
新名字更贴切，改完 5 处调用点即可，然后从 `testutil` 导入 `body`。
`crate::testutil::body(body(..))` 那种写法不要留。

本节新加的 `fn body(data: Vec<u8>) -> Box<dyn AsyncRead + Unpin + Send>` **不放在
`put.rs` 的测试模块里，而是放进 `crates/store/src/testutil.rs`**（`pub(crate)`）：
上面那张表里已经有七个模块要用它，各自抄一份就是七份复制品。
`testutil` 本来就是 `#[cfg(test)]` 门控的共用夹具（`lib.rs:18`）。

以下针对 `put.rs` 本身：四处：`put_small_object_inlines_it`（399–403 行）、`put_large_object_creates_shards`
（441–445 行）、`put_fails_below_write_quorum`（493–497 行）、以及 Step 1 新加的两条
（它们已经用了新形态）。前三处把 `data: data.clone()` / `data: vec![0u8; 1_000_000]`
换成 `body: body(data.clone())` / `body: body(vec![0u8; 1_000_000])`，并补
`etag: None`。

`put_large_object_creates_shards` 里的 `data` 变量此后不再被读取
（`vec![7u8; 1_500_000]` 只用于构造 body），会有 unused 警告——把它直接内联进
`body(...)`，不要 `let data = ...` 再 `body(data.clone())`：

```rust
        let out = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "big".into(),
                body: body(vec![7u8; 1_500_000]), // 2 个 block：1 MiB + 451 424
                etag: None,
            })
            .await
            .unwrap();
```

`put_small_object_inlines_it` 里 `data` 后面还要用来比对内联内容
（`assert_eq!(meta.inline.get("null"), Some(&data[..]));`），所以它仍要保留，
传 `body(data.clone())`。

- [ ] **Step 8: 跑测试**

```bash
cargo test -p rstore-store
```

Expected: 全绿。特别确认 `put_small_object_inlines_it` 与 `put_large_object_creates_shards`
的**盘上布局断言**（`vec!["meta.xl"]` / `vec!["meta.xl", "part.1"]`、以及
`bitrot_size` 那个字节数）一字未改地通过。

- [ ] **Step 9: 提交**

```bash
git add crates/store/src/put.rs
git commit -m "perf(store): PUT 收 AsyncRead，逐块读-编码-落盘

PutArgs.data: Vec<u8> 换成 body: Box<dyn AsyncRead + Unpin + Send>，
并新增 etag: Option<String>（P2 的 multipart Complete 要传合成值）。

尺寸不再需要调用方给：先读一个块，读不满就说明整个对象在此，读满则
shard_step 与总长无关（它只看 size.min(BLOCK_SIZE)）。于是 PutArgs 上
不需要 size 字段。

峰值内存从 3.5x 对象大小降到 BLOCK_SIZE + 一份分片。"
```

---

### Task 4: 契约 trait 换签名，打通 HTTP 层

**Files:**
- Modify: `crates/api/src/lib.rs:22-27`、`crates/api/Cargo.toml`
- Modify: `crates/server/src/wiring.rs:120-150`
- Modify: `crates/s3/src/mock.rs:111-125`
- Modify: `crates/s3/src/impl_s3.rs:111-143`（PUT）、`crates/s3/src/impl_s3.rs:516-523`（`NopStore`）
- Modify: `crates/s3/Cargo.toml`

- [ ] **Step 1: `rstore-api` 加 tokio 并改 trait**

`crates/api/Cargo.toml` 的 `[dependencies]` 加一行：

```toml
tokio.workspace = true
```

`crates/api/src/lib.rs` 的 `put_object` 声明（22–27 行）替换为：

```rust
    /// 写入一个对象。`req.body` 是请求体流，**读一次就没了**。
    async fn put_object(&self, req: PutRequest) -> Result<ObjectInfo, ApiError>;
```

在 `lib.rs` 顶部（`use async_trait::async_trait;` 之后）加：

```rust
use tokio::io::AsyncRead;
```

在 `ObjectInfo` 定义（54 行）之前插入：

```rust
/// 一次 PUT 的输入。**用参数结构体而不是四个位置参数**：`put_object` 的调用点
/// （S3 层与组合根各一处）读起来更好认，且以后加字段不必再改签名。
///
/// `body` 只要求 `Send`：trait 方法的**返回值**（那个 future）必须 `Send`，
/// 参数随 future 一起被捕获，因此参数也只要 `Send`。`Sync` 是白加的限制，
/// 会让 `StreamReader` 这类适配器白白卡住。
pub struct PutRequest {
    pub bucket: String,
    pub key: String,
    pub body: Box<dyn AsyncRead + Unpin + Send>,
    /// `Some` = 用给定的 etag（P2 的 multipart Complete）；`None` = 按内容算 MD5。
    pub etag: Option<String>,
}
```

同时把 `lib.rs:9-10` 那句「**刻意不含 multipart**——MVP 一律返回 501，所以 trait 上
根本没有对应方法」改掉——它在这份设计下**仍然成立**（P1 不加任何 multipart 方法），
但要把「P2 会加」写进去，免得读者以为这次已经加了：

```rust
/// S3 层能对引擎提出的全部问题。
///
/// **仍然没有 multipart**——分片上传是 P2/P3 的事，届时这里会新增方法。
/// 本阶段只把 `put_object` 从「收 `Vec<u8>`」改成「收流」，为 P2 的
/// Complete 铺路（它要把所有 part 流式读出来重编码成整体）。
```

- [ ] **Step 2: 适配组合根 `wiring.rs`**

`crates/server/src/wiring.rs:120-150` 替换为：

```rust
    async fn put_object(&self, req: PutRequest) -> Result<ObjectInfo, ApiError> {
        let started = self.metrics.is_enabled().then(std::time::Instant::now);
        let out = self
            .set
            .put_object(PutArgs {
                bucket: req.bucket,
                key: req.key,
                body: req.body,
                etag: req.etag,
            })
            .await;
        if let Some(t0) = started {
            self.metrics.record_put(t0.elapsed());
        }
        match out {
            Ok(v) => Ok(ObjectInfo {
                size: v.size,
                etag: v.etag,
                // `PutOut` 不带 `mod_time`，而 S3 的 PUT 响应只消费 etag，这里填 0。
                mod_time: 0,
            }),
            Err(e) => {
                self.note_err(&e, "put");
                Err(map_object_err(e))
            }
        }
    }
```

把文件顶部 `use rstore_api::{...}` 里的 `ObjectStore` 之外补上 `PutRequest`。

- [ ] **Step 3: 适配 `MockStore`**

`crates/s3/src/mock.rs:111-125` 替换为：

```rust
    async fn put_object(&self, req: PutRequest) -> Result<ObjectInfo, ApiError> {
        if let Some(err) = self.take_failure() {
            return Err(err);
        }
        // 协议层测试只关心状态码与响应头，所以这里把流读干即可——
        // 内存上界是 P1 在 `rstore-store` 里证明的，不是这里的职责。
        // `read_to_end` 需要 `tokio` 作为**正式依赖**（见 Cargo.toml 的说明）。
        let mut data = Vec::new();
        req.body
            .read_to_end(&mut data)
            .await
            .map_err(|e| ApiError::Internal(format!("mock: read body: {e}")))?;
        let etag = req.etag.unwrap_or_else(|| fake_etag(&data));
        let size = data.len() as u64;
```

**其余部分（`fake_etag` 之后的落库逻辑）保持原样**——`data` 与 `etag` 两个变量名不变，
所以后续代码不用动。`mock.rs` 顶部加：

```rust
use tokio::io::AsyncReadExt;
```

并把 `use rstore_api::{...}` 补上 `PutRequest`。

- [ ] **Step 4: 适配 `NopStore` 与 PUT 处理器**

`crates/s3/src/impl_s3.rs:516-523` 的 `NopStore::put_object` 替换为：

```rust
        async fn put_object(&self, _req: PutRequest) -> Result<ObjectInfo, ApiError> {
            Err(ApiError::Internal("nop".into()))
        }
```

`crates/s3/src/impl_s3.rs:111-143` 的 `put_object` 替换为：

```rust
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        // 写入入口：两条命名规则都在这里挡（DESIGN §6.3 + 盘上别名）。
        validate_object_key(&req.input.key).map_err(to_s3_error)?;

        // 请求体**不再收全**：`StreamingBlob` 是一个字节流，直接转成 `AsyncRead`
        // 交给存储层逐块消费。空 body 是合法的 PUT（`touch` 一个 0 字节文件），
        // `tokio::io::empty()` 就是那个「立刻 EOF」的流。
        let body: Box<dyn AsyncRead + Unpin + Send> = match req.input.body {
            Some(blob) => Box::new(StreamReader::new(
                // `StreamingBlob` 的 Item 是 `Result<Bytes, Box<dyn Error + Send + Sync>>`，
                // 而 `StreamReader` 要求 `Result<_, io::Error>`。这层映射只是把错误
                // 换个盒子——真正的错误语义在上面的 `read_block` 里统一成
                // `StoreError::Internal`，不会在这里丢信息。
                blob.map(|r| r.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))),
            )),
            None => Box::new(tokio::io::empty()),
        };

        let info = self
            .store
            .put_object(PutRequest {
                bucket: req.input.bucket,
                key: req.input.key,
                body,
                etag: None,
            })
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(info.etag)),
            ..Default::default()
        }))
    }
```

`impl_s3.rs` 顶部加：

```rust
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;
```

并把 `use rstore_api::{...}` 补上 `PutRequest`。**删掉**现在只服务于旧实现的导入：
`bytes::Bytes` 与 `futures::TryStreamExt`（`try_collect` 用）——若它们在本文件别处
还有用，保留；跑 `cargo build -p rstore-s3` 后按 `unused import` 警告逐个处理。
`futures::StreamExt`（`blob.map(..)` 用）**要留着**，但**只能放在函数内**，见下。

**实测踩到的四处（原计划没说或说反了）：**

1. **`bytes::Bytes` 不能删。** 它在 `impl_s3.rs:215` 的 GET 响应体
   （`StreamingBlob::from_bytes(Bytes::from(out.data))`）里还在用，删掉直接编译不过。
   只有 `futures::TryStreamExt` 是真该删的。
2. **`use futures::StreamExt as _;` 必须写进 `put_object` 函数体内，不能放模块级。**
   测试模块（`use super::*`）里 `http_body_util::BodyExt` 也有个 `collect`，
   两个 trait 同时可见 → `error[E0034]: multiple applicable items in scope`
   （`impl_s3.rs:569`）。原先模块级用的是 `TryStreamExt`，它没有 `collect`，
   所以从前不冲突——换成 `StreamExt` 就撞上了。
3. **`MockStore::put_object` 要 `mut req`**：`req.body.read_to_end(&mut data)` 要可变借。
   原计划给的是 `req: PutRequest`，漏了 `mut`。
4. **测试里有 5 处 `store.put_object("test-bucket", key, data)`**（`827` / `1054` /
   `1058` / `1195` / `1398` 行附近）。加一个测试模块级的辅助函数比逐处展开好读：

   ```rust
   fn put_req(bucket: &str, key: &str, data: Vec<u8>) -> PutRequest {
       PutRequest {
           bucket: bucket.to_owned(),
           key: key.to_owned(),
           body: Box::new(std::io::Cursor::new(data)),
           etag: None,
       }
   }
   ```

   然后把调用点写成 `.put_object(put_req("test-bucket", key, key.as_bytes().to_vec()))`。

另外 `clippy::io_other_error`（workspace 门禁是 `-D warnings`）要求写成
`std::io::Error::other(e)`，而不是 `Error::new(ErrorKind::Other, e)`。

- [ ] **Step 5: 改 `crates/s3/Cargo.toml`**

两处改动：

```toml
[dependencies]
# ...（其余不动）
# `mock.rs` 在 `src/` 里，要 `AsyncReadExt::read_to_end` 读干请求体——
# 于是 tokio 从 dev-dependency 升为**正式依赖**。
tokio.workspace = true
# `StreamReader` 把 `StreamingBlob` 转成 `AsyncRead`。**必须显式写 `io`**：
# workspace 里声明的是 `features = ["rt"]`，而 tokio-util 0.7 的 `default = []`，
# 不写这一条 `tokio_util::io` 整个模块都不存在。
tokio-util = { workspace = true, features = ["io"] }
```

`[dev-dependencies]` 里原有的 `tokio.workspace = true`（以及那句「只在
`#[tokio::test]` 里用到」的注释）**删掉**——dev-dependency 会自动继承正式依赖，
重复声明没有意义，而那句注释现在也不对了。

- [ ] **Step 6: 构建并跑 s3 层测试**

```bash
cargo build --workspace
cargo test -p rstore-s3
```

Expected: 编译通过；`rstore-s3` 全部测试绿。**特别确认**
`all_six_multipart_ops_are_501_not_implemented`（`impl_s3.rs:1420`）仍然通过——
P1 不改任何 multipart 行为，它必须原样通过。

- [ ] **Step 7: 提交**

```bash
git add crates/api crates/server crates/s3
git commit -m "refactor(api): put_object 收 PutRequest 里的 AsyncRead

S3 层不再把请求体 try_collect+concat 成一个 Vec<u8>（那是 2x 峰值的那一半），
改为 StreamingBlob -> StreamReader -> AsyncRead 直接转交存储层。

连带：rstore-api 加 tokio；rstore-s3 的 tokio 从 dev-dependency 升为正式
依赖（mock.rs 要 read_to_end）、加 tokio-util（io feature）。
三个 ObjectStore 实现（Wiring / MockStore / NopStore）同步适配。"
```

---

### Task 5: 内存上界的回归测试、全量门禁、文档回写

**Files:**
- Modify: `crates/store/src/testutil.rs`（加 `RecordingDisk`）
- Modify: `crates/store/src/put.rs` 的 `mod tests`
- Modify: `docs/superpowers/specs/2026-10-10-multipart-design.md`（回写三处偏离）
- Modify: `crates/store/src/put.rs:452` 的注释（设计文档 §12 记录的那处）

- [ ] **Step 1: 写 `RecordingDisk` 与 `WriteLog`**

在 `crates/store/src/testutil.rs` 的 `set_with_disks` 之后（`#[cfg(test)] mod tests` 之前）
插入：

```rust
/// 写入长度日志。`Arc` 包一层是为了让 `RecordingDisk` 与测试各持一份。
#[derive(Clone, Default)]
pub struct WriteLog(Arc<std::sync::Mutex<Vec<usize>>>);

impl WriteLog {
    /// 所有写入 payload 的长度，`write_all` 与 `append` 合并、按时序。
    pub fn sizes(&self) -> Vec<usize> {
        self.0.lock().expect("write log poisoned").clone()
    }

    fn record(&self, n: usize) {
        self.0.lock().expect("write log poisoned").push(n);
    }
}

/// 记录**每次写入 payload 长度**的盘，用于断言写路径的内存上界。
///
/// **为什么不是「分配计数探针」**：那需要一个 `GlobalAlloc` 实现，而它必须
/// `unsafe`，workspace lint 是 `unsafe_code = "forbid"`。记录写入长度是同一件事的
/// 可实现版本：整份缓冲会表现为「一次巨大的写入」，照样能抓住。
///
/// **`write_all` 与 `append` 都要记**——只看 `append` 的话，一个退回
/// 「缓冲整份分片再 `write_all`」的实现会在这一层完全隐形（那正是要防的回归）。
///
/// **它包在最内层**（`LocalDisk` 之外、`FaultyDisk` 之内）：故障注入与写入计数
/// 于是观测的是同一批调用，两者不会互相遮蔽。见 [`set_with_recording_disks`]。
pub struct RecordingDisk {
    inner: Arc<dyn DiskAPI>,
    log: WriteLog,
}

impl RecordingDisk {
    pub fn new(inner: Arc<dyn DiskAPI>, log: WriteLog) -> Self {
        Self { inner, log }
    }
}

#[async_trait::async_trait]
impl DiskAPI for RecordingDisk {
    async fn write_all(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError> {
        self.log.record(data.len());
        self.inner.write_all(rel_path, data).await
    }

    async fn append(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError> {
        self.log.record(data.len());
        self.inner.append(rel_path, data).await
    }

    async fn read_exact_at(
        &self,
        rel_path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, DiskError> {
        self.inner.read_exact_at(rel_path, offset, len).await
    }
    async fn rename(&self, from_rel: &str, to_rel: &str) -> Result<(), DiskError> {
        self.inner.rename(from_rel, to_rel).await
    }
    async fn remove_dir_all(&self, rel_path: &str) -> Result<(), DiskError> {
        self.inner.remove_dir_all(rel_path).await
    }
    async fn list_dir(&self, rel_path: &str) -> Result<Vec<String>, DiskError> {
        self.inner.list_dir(rel_path).await
    }
    async fn stat(&self, rel_path: &str) -> Result<Option<FileStat>, DiskError> {
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

/// [`set_with_disks`] 的变体：每块盘的最内层是 `RecordingDisk`，
/// 所有盘的写入长度汇总到同一个 `WriteLog`。返回 `(set, log)`。
///
/// 包装顺序是 `LocalDisk` → `RecordingDisk` → `FaultyDisk`。**这个顺序不能反**：
/// `RecordingDisk` 记的是「最终落到盘上的那些写入」，若把它套在 `FaultyDisk`
/// 外面，`Fault::DropWrites` 之类「假装成功」的故障就不会出现在日志里。
pub async fn set_with_recording_disks(total: u8, parity: u8) -> (TestSet, WriteLog) {
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

    let set = ErasureSet::new(disks, parity).expect("valid erasure set geometry");
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

顶部 `use` 区改成：

```rust
use std::sync::Arc;

use rstore_common::disk_id::DiskId;
use rstore_common::error::DiskError;
use rstore_disk::{DiskAPI, Fault, FaultyDisk, FileStat, LocalDisk};
```

（`DiskError` 与 `FileStat` 是新增的；其余三行原本就有。）

**锁的纪律**：`WriteLog::record` 里的 `Mutex` guard 在函数返回时就释放，
`await` 在它之后——workspace 把 `clippy::await_holding_lock` 设成 `deny`，
跨 `await` 持锁会直接挡死门禁。


- [ ] **Step 2: 写回归测试**

在 `crates/store/src/put.rs` 的 `mod tests` 里加：

```rust
    /// **本任务的核心断言**：写路径从不一次性写出超过一个块。
    ///
    /// 这钉的是设计文档 §1.2 那个 3.5x 放大器。退回任何一种整份缓冲都会在这里红：
    /// - 退回 `BitrotShardWriter` 攒整份分片 -> `write_all` 收到整份分片（64 MiB 对象
    ///   的 4 数据分片是 16 MiB），远大于 `BLOCK_SIZE + 32`；
    /// - 退回 S3 层的 `try_collect().concat()` -> 不会出现在本层，由
    ///   `crates/s3` 的测试与 acceptance 脚本覆盖。
    #[tokio::test]
    async fn write_path_never_emits_more_than_one_block() {
        // 16 MiB = 16 个块，4+2 布局。够大到让「整份缓冲」与「逐块」差出数量级：
        // 整份缓冲时每块盘一次写出的分片是 4 MiB，逐块时是 256 KiB。
        const SIZE: usize = 16 * 1024 * 1024;

        let (set, log) = set_with_recording_disks(6, 2).await;
        let out = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "bounded".into(),
                body: body(vec![0x5Au8; SIZE]),
                etag: None,
            })
            .await
            .unwrap();
        assert_eq!(out.size as usize, SIZE);

        let sizes = log.sizes();
        assert!(!sizes.is_empty(), "盘上一个字节都没写？");
        let max = *sizes.iter().max().unwrap();
        assert!(
            max <= BLOCK_SIZE + rstore_checksum::HASH_LEN,
            "单次写入 {max} 字节，超过一个块（{BLOCK_SIZE} + {}）——写路径又在整份缓冲了",
            rstore_checksum::HASH_LEN
        );
        // 顺带确认它确实是**多次**写入，而不是运气好一次写完刚好没超。
        // 16 个块 × 6 块盘 = 96 次 append（外加 6 次 meta.xl）。
        assert!(
            sizes.len() > 16,
            "16 MiB / 1 MiB 应该远多于 16 次写入，实际 {}",
            sizes.len()
        );
    }
```

`mod tests` 的 `use` 区补 `set_with_recording_disks`（若既有
`use crate::testutil::{set_with_disks, TestSet};` 就并进去）。


- [ ] **Step 3: 跑测试确认它通过**

```bash
cargo test -p rstore-store write_path_never_emits_more_than_one_block
```

Expected: PASS。

- [ ] **Step 4: 反过来验证这条测试真的会红（**不可跳过**）**

一条从没红过的回归测试不算回归测试。临时把
`crates/store/src/writer.rs` 的 `push_block` 改回「攒进 `buf`、`finish` 时
一次 `write_all`」（即 Task 2 之前的样子），跑同一条测试：

```bash
cargo test -p rstore-store write_path_never_emits_more_than_one_block
```

Expected: **FAIL**，报「单次写入 N 字节……写路径又在整份缓冲了」，且 N ≫ 1 MiB。
确认之后**把改动还原**，再跑一次确认变绿。把这次的红色输出抄进提交信息——
它是这条测试有效的唯一证据。

- [ ] **Step 5: 回写设计文档的三处偏离**

编辑 `docs/superpowers/specs/2026-10-10-multipart-design.md`：

- §3.1a 的 `create_writer` / `DiskWriter` 代码块与说明，改成 `DiskAPI::append`
  的实际形态，并写明「为什么不用句柄」（`spawn_blocking` 无状态闭包，见本文档
  顶部偏离表提的那条理由）。
- §3.1c 的 `PutStreamArgs` 改成实际的 `PutArgs`（去掉 `size` 字段，body 约束收到
  `Send`），并说明尺寸是靠「先读一个块」定下来的。
- §3.2 的「分配计数探针」改成 `RecordingDisk` 的记录写入长度，并写明
  **`unsafe_code = "forbid"` 让 `GlobalAlloc` 方案在原仓库里根本做不出来**。

**同时**改 `crates/store/src/put.rs:452` 那条注释：

```rust
        // 单部分对象：每块盘的数据目录里恰好一个分片文件 part.1。
        // N 是**部分号**（multipart 的 part），不是盘号；MVP 只有一部分。
```

改为：

```rust
        // 单部分对象：每块盘的数据目录里恰好一个分片文件 part.1。
        // N 是**部分号**（multipart 的 part），不是盘号。P1 阶段恒为 1；
        // P2 的 multipart 走「Complete 时重编码成整体」（设计文档 §2 决策一），
        // 那时盘上**仍是一个** part.1，S3 的 part 号只体现在 etag 的 `-N` 后缀里——
        // 也就是说这个 N 与 S3 的 part 是一一对应的，只是永远等于 1。
```

- [ ] **Step 6: 跑完整门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
bash scripts/check-layer-deps.sh
```

Expected: 四条全绿。

- `fmt`：**很可能要改**（本计划手写的代码块没按 rustfmt 的换行排过）。
  直接跑 `cargo fmt --all` 再重跑 `--check`。
- `clippy`：本计划**不预期有任何告警**，也不加任何 `#[allow]`。
  若有告警，**按告警改实现，不要拿 allow 盖过去**。
- `test`：基线是 210 条（`grep -c '#\[test\]\|#\[tokio::test\]' crates/*/src/*.rs` 合计）。
  本次新增 **5** 条：`fsx::append_concatenates_while_write_all_truncates`、
  `writer::each_block_reaches_disk_before_finish`、
  `put::streaming_put_matches_buffered_put`、`put::supplied_etag_wins_over_computed_md5`、
  `put::write_path_never_emits_more_than_one_block`。
  既有 210 条**一条都不许变红**。
- `check-layer-deps.sh`：`rstore-api` 新增的 `tokio` 是外部依赖，不涉及
  crate 之间的 allowlist；这条门禁预期无变化，但它**必须跑**——签名从
  `Vec<u8>` 换成 `Box<dyn AsyncRead>` 有可能诱发「直接引用 `rstore-store` 类型」
  这种偷懒写法，那样就会红。

- [ ] **Step 7: 端到端验收（证明 HTTP 层没被改坏）**

```bash
bash tests/acceptance.sh
```

Expected: 全绿。这条脚本是**唯一**覆盖「真实 HTTP 请求 → s3s → 存储层 → 盘」全链路
的测试，P1 改了这条链上的每一层的签名，必须由它收尾。

跑之前先确认 9000 端口没被占：

```bash
taskkill //F //IM rstore-server.exe 2>/dev/null || true
```

（`tests/acceptance.sh` 自带构建与 `/ready` 轮询，不需要手工起服务。）

- [ ] **Step 8: 提交**

```bash
git add crates/store/src/testutil.rs crates/store/src/put.rs \
        docs/superpowers/specs/2026-10-10-multipart-design.md
git commit -m "test(store): 钉住写路径的单块写入上界，并回写设计文档

新增 RecordingDisk：记录每次 write_all / append 的 payload 长度。
设计文档 §3.2 原定的『分配计数探针』在本仓库做不出来——GlobalAlloc
必须 unsafe，而 workspace lint 是 unsafe_code = forbid；记录写入长度
是同一件事的可实现版本。

write_path_never_emits_more_than_one_block 已按 Step 4 反向验证过：
把 writer.rs 退回缓冲实现会红，且报出的单次写入长度远大于一个块。

设计文档 §3.1a（create_writer -> append）与 §3.1c（PutStreamArgs -> PutArgs）
两处偏离一并回写，put.rs 那条『MVP 只有一部分』的注释同步改写。"
```

- [ ] **Step 9: 收尾**

P1 到此完成。它的完成判据是设计文档 §10 里那一条：

> 既有 210 个测试全绿（**行为不变**）；1 GiB 写入峰值内存 < 64 MiB

前半条由 Step 6 的 `cargo test --workspace` 证明；后半条由 Step 2 的
`write_path_never_emits_more_than_one_block` 以「单次写入 ≤ 一个块」的形式证明
（16 MiB 的用例足以分辨数量级，不必真写 1 GiB——那会让测试慢到没人跑）。

**接下来写 P2/P3/P4 的计划。** 它们的 `PutArgs` 形态此时已经落定，可以照着真实
签名写，不用再猜。

---

## 自审记录

**规格覆盖**（对照设计文档）：

| 设计文档条目 | 本文对应 |
|---|---|
| §3.1a 磁盘原语 | Task 1（形态改为 `append`，偏离已记录） |
| §3.1b 写入器增量落盘 | Task 2 |
| §3.1c `put_object` 收流 | Task 3 + Task 4 |
| §3.2 完成判据（内存上界） | Task 5 Step 2/4（手段改为 `RecordingDisk`，偏离已记录） |
| §9.2 故障注入（Complete 前后的窗口） | **不在 P1**——那些窗口属于 Complete，是 P2 的事。P1 只保证 `FaultyDisk` 的 payload 变换在两条写路径上一致（Task 1 Step 6） |
| §9.4 改 `all_six_multipart_ops_are_501_not_implemented` | **不在 P1**——P1 不改 multipart 行为，那条测试必须原样通过（Task 4 Step 6） |
| §10 P1 里程碑 | 本文整体 |
| §10 P2/P3/P4 | 另起计划 |
| §12 文档偏离 | Task 5 Step 5 只覆盖与 P1 有关的三条；README / MVP.md 那几条属于 P4 |

**占位符扫描**：无。Task 5 Step 1 的 `set_with_recording_disks` 一度是照现有夹具
名字猜的，写计划时已读 `crates/store/src/testutil.rs` 落实成真代码（包装顺序
`LocalDisk` → `RecordingDisk` → `FaultyDisk`，由 `set_with_disks` 的实际结构决定）。

**类型一致性**：`PutArgs { bucket, key, body, etag }` 在 Task 3 定义，在 Task 4 的
`wiring.rs` 与 Task 5 的测试里被构造，字段名一致。
`PutRequest { bucket, key, body, etag }` 在 Task 4 定义并被 `mock.rs` / `NopStore`
使用，字段名一致。`write_shards_stream` 的返回 `(u64, u64, String)` 在 Task 3 的
`put_object` 里按 `(shard_len, size, etag)` 解构，顺序一致。
`DiskAPI::append(&self, rel_path: &str, data: &[u8])` 在 Task 1 定义，在
`RecordingDisk`（Task 5）与 `FaultyDisk`（Task 1）里同签名实现。

**已核实的门禁细节**：`Cargo.toml` 的 `[workspace.lints.clippy]` 只有
`all` 与 `await_holding_lock = "deny"`，**没有**改 `too-many-arguments-threshold`，
所以默认阈值 7 生效，`write_shards_stream` 的 7 个参数不触发告警。本计划因此
**不需要任何 `#[allow]`**——这一点写计划时先猜错、后核实改正了。
