# 读写路径新旧模式共存 + 性能对比 设计

> 状态：**设计已定，未实现。**
> 本文是 `docs/DESIGN.md` 的增量：它不推翻既有契约，只对读写路径做**可拆除**的加速，
> 并留下一套可复现的新旧对比。
> 与 `docs/DESIGN.md` 冲突时以本文为准（本文更新）；与代码冲突时以代码为准。
> 参考材料：[`docs/read-write-path-design.md`](../read-write-path-design.md)（从 RustFS 提取的读写架构原理）。

---

## 1. 目标与非目标

### 1.1 目标

当前读路径有三处已知的浪费，写路径有一处：

| 现象 | 位置 |
|---|---|
| 范围读会把**整份分片**读进内存并逐块校验，最后才切出 `[start, end]` | `crates/store/src/get.rs::read_shards` |
| 每次 HEAD / GET 都要在**每块盘**上 `list_dir` 一遍版本目录来找元数据 | `crates/store/src/get.rs::resolve_version` |
| LIST 先把整个桶**全量**物化，再应用 marker / delimiter / `max-keys` | `crates/store/src/list.rs` → `crates/s3/src/impl_s3.rs::list_objects_v2` |
| 写路径每个块重新分配一份分片缓冲 | `crates/store/src/put.rs::write_shards_stream` |

本次要做的是：**把四个机制各自做成一个运行时开关**，旧路径（= 今天的代码）原样保留，
然后用**同一份数据集、同一台机器**量出新旧差距。

### 1.2 非目标（这一版明确不做）

| 不做 | 理由 |
|---|---|
| 流式 GET（`AsyncRead` 分块吐给 HTTP） | 会改 `ObjectStore` trait 的返回类型，牵动 S3 层与兼容层；与「量出读写路径的差」不是同一个任务。DESIGN §10.5 已把它列为独立方向 |
| 全局分层缓冲池（DESIGN §13.3 的 SMALL/MEDIUM/LARGE/XLARGE） | 需要 `Semaphore` + `ManuallyDrop` + `try_lock` 一整套；本次的真实需求只是「单次 PUT 内别重复 allocate」，见 §5 |
| 组提交 / `xl.meta.bkp` / 减少 fsync 次数 | `crates/disk/src/fsx.rs::sync_dir` 在 **Windows 上是 no-op**，基于 fsync 的对比在本开发机上是一条平直假线，量不出东西 |
| 命名空间索引（DESIGN §20 Phase 2） | 会改盘上布局，旧模式就无从对照了 |
| 读修复入队 / `reduce_errs`（DESIGN §10.3 / §10.5） | 属于纠错而非加速，与本次的对比目标正交 |
| 准入控制 / 背压（DESIGN §16.3） | 同上；没有它不会让本次的对比失真 |

**四条硬边界：**

1. 盘上布局一个字节都不变。新旧模式共用同一批数据文件，所以可以拿同一份数据集直接对跑。
2. 不新增对外 API。开关只走 CLI，S3 数据面契约不动。
3. 每个机制**恰好一个决策点**。删除时就是删一个分支，不是拆一坨抽象。
4. 开关是**临时的**。见 §10 的删除计划——量完即删。

---

## 2. 共存机制的选择

三种做法：

| 方案 | 形态 | 代价 | 结论 |
|---|---|---|---|
| **A. 运行时开关** | `IoModes { 四个 bool }` 挂在 `ErasureSet` 上 | 每个机制一个 `if`；全局可 grep；两种模式在同一进程内可切换 | **采用** |
| B. trait 策略对象 | `Arc<dyn ReadPath>` / `Arc<dyn WritePath>` | 要新造 trait、改 `ObjectStore` 的装配；删除时是删一整层抽象，不是删一段逻辑 | 否决 |
| C. 编译期 feature | `--features new-read-path` | **两种模式没法在同一个二进制里对跑** | 否决 |

选 A 的核心理由只有一句：**这个开关的全部价值，在于让「新旧两种模式只差那几行」成为一件可构造的事实。**

B 和 C 都会引入属于它们自己的结构——trait 的分派开销、feature 组合下的条件编译——量出来的就不再纯粹是读写路径的差。
A 的分支预测开销是常数且可忽略，而且因为它足够廉价，才能放心地让**旧模式成为默认**（§3.3）。

> **为什么开关挂在 `ErasureSet` 而不是 `ObjectStore` / `wiring` 层**
> 四个机制全都实现在 `crates/store` 内部（`get.rs` / `list.rs` / `put.rs`）。
> 挂在 `ObjectStore` 上等于让 `rstore-api` 认识一个它不需要知道的概念，装配层还要多一层穿透。

---

## 3. `IoModes` 与接入点

### 3.1 结构

新文件 `crates/common/src/modes.rs`（`rstore-common` 目前只有 `consts` / `disk_id` / `error`，`modes` 是第四个模块，独立成块便于整体删除）：

```rust
/// 读写路径的模式开关。四个机制各自独立，互不耦合。
///
/// 存在的唯一目的，是让新旧两条路径同时存在于一个二进制里被对比测量。
/// 基准做完后本模块整体删除，各机制各自成为唯一实现（见设计文档 §10）。
///
/// 全部 `false` = 今天的旧路径。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoModes {
    /// 范围读：只读命中的块区间，不再整份分片读进来。
    pub ranged_shard_read: bool,
    /// 元数据缓存：`get_object` / `head_object` 复用最近解析出的版本。
    pub metadata_cache: bool,
    /// 有界列举：LIST 增量遍历并在够数时提前停，不再全量物化。
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

    /// 从 CLI 的 `old` / `new` 解析。
    pub fn from_io_mode(s: &str) -> Option<Self> {
        match s {
            "old" => Some(Self::default()),
            "new" => Some(Self::ALL),
            _ => None,
        }
    }
}
```

用四个具名 bool 而非 bitflags：为了**可 grep、可逐个删**。加一个 `bitflags` 依赖换来的只是更短的代码，
而这份代码的寿命只有一次基准测试那么长。

> **为什么不做 `--io-mode` 之外的细粒度开关**
> 四个 bool 已经可以在测试里任意组合（阶梯式归因需要 `+A` → `+A,C` → `+A,C,D`），
> 但**不暴露给 CLI**。CLI 面上只有 `old` / `new` 两档，避免产生「半新半旧」的运维组合。

### 3.2 接入 `ErasureSet`

`crates/store/src/set.rs`：

```rust
pub struct ErasureSet {
    disks: Vec<Option<Arc<dyn DiskAPI>>>,
    data: u8,
    parity: u8,
    codec_cache: CodecCache,
    modes: IoModes,          // 新增
}

impl ErasureSet {
    /// 旧路径。**语义与签名都不变**——全部现存调用点（含全部既有测试）一个字节都不用动。
    pub fn new(disks: Vec<Option<Arc<dyn DiskAPI>>>, parity: u8) -> Result<Self, StoreError> {
        Self::with_modes(disks, parity, IoModes::default())
    }

    /// 指定模式构造。**构造期的校验一条不少**：`total > u8::MAX` 拒绝、
    /// `parity >= total` 拒绝——这两条是几何正确性的前提，与模式无关。
    pub fn with_modes(
        disks: Vec<Option<Arc<dyn DiskAPI>>>,
        parity: u8,
        modes: IoModes,
    ) -> Result<Self, StoreError> {
        // 函数体 = 原 `new` 的全部内容，加上 `modes` 字段的赋值。
        // 不新增、不删除任何校验。
    }

    pub fn modes(&self) -> IoModes { self.modes }
}
```

**旧模式的定义就是今天的构造函数。** 这不是修辞：`new` 委托给 `with_modes(.., IoModes::default())`，
所以「旧模式」与「改动前的行为」之间不存在第二处差异。

### 3.3 CLI

`crates/server/src/config.rs` 的 clap `Config` 新增：

```rust
/// 读写路径模式：old = 今天的实现；new = 四个加速机制全开。用于性能对比。
#[arg(long, value_name = "MODE", default_value = "old", value_parser = ["old", "new"])]
io_mode: String,
```

`crates/server/src/wiring.rs` 里把 `IoModes::from_io_mode(&cfg.io_mode)` 传给 `ErasureSet::with_modes`。

**默认 `old`。** 新路径还没跑过完整接受度验证，不该在无人察觉的情况下成为默认。
等基准跑完、删除计划执行之后（§10），新路径自然成为唯一路径，这个标志随之消失。

---

## 4. 读侧：三个机制

### 4.1 `ranged_shard_read` — 范围读只读命中的块

**今天的行为**（`crates/store/src/get.rs::read_shards`，第 253 行起）：
对每块盘 `BitrotShardReader::new(..).read_all()` 把**整份分片**读进内存并逐块重算 bitrot，
攒出 `payloads: Vec<Option<Vec<u8>>>`，解码全部块，最后才切出 `[start, end]`。
`get_object` 的文档注释（第 382 行）对此是诚实的：「省的是网络与 S3 层的内存，没省磁盘 IO」。

**新路径**：`BitrotShardReader` 新增

```rust
/// 只读第 `first_block..=last_block` 块（含两端）。块内按 `[hash(32B)][data]` 交错，
/// 块 k 的区间是 `k*(HASH_LEN + step) .. (k+1)*(HASH_LEN + step)`，只校验读到的块。
pub async fn read_range(&self, first_block: usize, last_block: usize)
    -> Result<Vec<u8>, StoreError>

/// 保留，成为 `read_range(0, block_count - 1)` 的特例。
pub async fn read_all(&self) -> Result<Vec<u8>, StoreError>
```

**关键约束：整文件长度检查留在共享路径上，只把 `read_exact_at` 的区间收窄。**

也就是说新路径**依然 `stat`**，并且
`stat.size < expected → Transient(ShortRead)`、`stat.size > expected → Corrupt(LengthMismatch)`
这两条判定原样保留。变的只有两件事：不再把整份分片搬进内存、不再重算被跳过块的 bitrot。

> **为什么这条约束不能松**
> `reader.rs` 的文档注释已经解释过：`shard_len` 必须由调用方显式传入，**不能从文件长度反推**。
> 反过来看，一旦新路径跳过长度检查，截断的文件就会静默地返回一段合法-looking 的数据。
> 长度检查是廉价的一次 `stat`，没有理由省。

**区间是怎么来的**：`read_shards` 里每个块的 `lo = k * step` / `hi = min((k + 1) * step, shard_len)`
**已经算好了**（那是解码循环的输入）。新路径只是把已经算出来的东西再用一次：
`[start, end]` → `first_block = start / step`、`last_block = end / step`。
不需要新的偏移计算。

**降级读取**：新路径同样按「缺块」处理，`read_quorum = N - parity` 的门槛**一个字不变**。

注意选盘规则：`read_shards` 目前是**对每一块盘都读一次**（`for slot in set.disks()`），
没有「只读 `data` 块盘」的最小读优化——所以新旧模式读的是**同一组盘**，
差别只在每块盘上读了哪些块。

因此可用性方向是**单向变好**：一块盘只有在**实际需要的那几个块**损坏时才会被判为缺块。
整对象读会因为块 7 的位腐而丢掉整块盘的分片；读块 1 的范围读不会。
这条是 §8 声明的语义变化，也正是等价值测试必须分场景写的原因（§7.2）。

### 4.2 `metadata_cache` — 缓存解析出的版本

**今天的行为**（`crates/store/src/get.rs::resolve_version`，第 146 行起）：
对**每块盘**执行 `list_dir("{bucket}/{key}")`，跳过 `.staging-` 前缀，
由此发现候选版本目录，再读元数据投票。每个 HEAD / GET 都要付这份代价。

**新路径**：新增 `resolve_version_cached`：

- 表的每一格是 `(generation: u64, resolved: Resolved)`，按 `(bucket, key)` 索引；
- `put_object` / `delete_object` 在它们**本来就持有**的 §16.1 对象写锁下
  把该键的 `generation` 递增一格。**递增即失效**——读侧比对 generation，
  对不上就当作未命中重新解析，不需要单独一条淘汰路径；
- 复用 DESIGN §16.1 的分片 `RwLock` 表（按 key 哈希分片，不新造一套锁表），
  读侧取读锁、写侧取写锁，与既有锁纪律一致。

> **为什么用代际而不是「写时删表项」**
> 写路径已经持有那把写锁，递增一个 `u64` 比删表项更便宜，也不会因为
> 「先删表项、再写盘、写失败」而留下一个本该失效却被当作命中的表项。
> 代际是单调的，所以任何**在飞**的慢读者最多是重解析一次，不会读到过期值。

**调用点**：只有 `get_object` 与 `head_object` 走缓存版。

> **为什么这三处故意不接缓存**
> - `crates/store/src/delete.rs`：写路径要的是**真相**，不是「最近为真」。删除决策建立在陈旧版本上会删错对象。
> - `crates/store/src/reconcile.rs`：对账的全部意义就是绕过缓存去看盘上实际有什么。给它一个缓存等于让它失去存在理由。
> - `crates/store/src/list.rs`：LIST 本来就是遍历，缓存不解决问题（它的瓶颈是遍历本身，见 §4.3）。
>
> 这三处是**刻意的**，不是遗漏。改动评审时如果看到有人顺手给它们加上缓存，那是个 bug。

### 4.3 `bounded_listing` — 有界增量列举

**今天的行为**：
`crates/store/src/list.rs::list_objects` → `candidate_keys`（第 98 行）用显式栈 DFS 把整个桶收集进
`BTreeSet<String>`（每个条目还要 N 次 `any_disk_is_dir`，第 133 行），再对**每个** key 调 `resolve_version`。
然后 `crates/s3/src/impl_s3.rs::list_objects_v2`（第 324 行）把**整个列表物化完**，才应用
cursor / `max_keys` / `delimiter`。

**新路径**：`candidate_keys` 改为有界增量有序遍历，返回 `(entries, more)`；
S3 层保留 marker / delimiter / max-keys 语义，靠折叠循环反复调用。
旧模式一次返回全部、`more = false`，忽略 marker 与 limit——行为与今天逐字节一致。

#### ⚠️ 这里有一个真陷阱：UUID 版本目录破坏了「有序路径 = 有序键」

盘上布局是 `<bucket>/<key>/<data-dir-uuid>/meta.xl`。于是：

- 键 `p` 落在目录 `p` 下，实际内容在 `p/<uuid>/meta.xl`；
- 键 `p/a` 也落在目录 `p` 下，实际内容在 `p/a/<uuid>/meta.xl`。

要输出有序的键，就得在遍历 `p` 时**同时**判断「`p` 本身是个对象」还是「`p` 是个前缀」。
但 `p` 的子目录里既有 `<uuid>`（属于 `p`）也有 `a`（属于 `p/a`），
而 **UUID 的字典序与键结构毫无关系**——按目录名排序得不到键序。

**错误做法**：下探到 `<uuid>` 再反推父路径。这既得不到有序输出，
也无法区分「`p` 是对象」与「`p` 是前缀」（两者在盘上都表现为目录 `p`）。

**正确做法**：**在键目录层级就决定并输出**。对 `p` 做一层前瞻——
看它的子目录里有没有哪个直接含 `meta.xl`：

- 有 → `p` 是一个对象，输出键 `p`，**且不再下探**（不能再把 `p/` 当公共前缀吐一遍）；
- 没有 → `p` 是前缀，输出 `p/` 作为公共前缀，继续下探它的子目录（跳过 `<uuid>` 形状的条目）。

代价是每层多一次前瞻 `stat`，但换来的是**有序输出 + 正确的对象/前缀二选一**，
而且可以把「跳过的 UUID 目录」与「继续下探的键目录」区分开。

> **旧实现是靠什么绕过去的**
> 它用 `BTreeSet` 收集全部再整体 sort——所以它**天然正确但天然全量**。
> 这恰好是「为了测量而保留旧路径」的一个好理由：新路径的正确性可以拿旧路径当参照物来验。

**S3 层的连续性**：`crates/s3/src/impl_s3.rs:335-342` 目前把 `continuation_token` 当作裸的 last-key
（无状态、不做 base64）。本次**不改这个契约**——`bounded_listing` 只改存储层一次拿多少，
不改 token 的编码方式。第 362 行的 `truncated` 判定顺序（在应用条目**之前**检查）也保持不变。

---

## 5. 写侧：`pooled_write_buffers`

`crates/store/src/put.rs::write_shards_stream`（第 368 行）目前每个块重新分配
`vec![0u8; shard_size_k]` × `data_shards`（第 412 行）。

**新路径**：一个局部 `ShardScratch` 跨块复用：

```rust
/// 单次 PUT 内的分片暂存区。跨块复用，避免每块重新分配。
///
/// **为什么是局部而不是全局池**：这里的真实需求只有「单次 PUT 内别重复 allocate」。
/// 全局分层池（DESIGN §13.3）需要 Semaphore + ManuallyDrop + try_lock 一整套，
/// 而它要解决的问题（跨请求、跨尺寸的缓冲复用）不是本次的对比目标。
struct ShardScratch {
    block: Vec<u8>,              // = read_block 的 BLOCK_SIZE 缓冲
    shards: Vec<Vec<u8>>,        // data_shards 份，按几何分配一次
    allocs: u64,                 // 插桩计数，见 §6
}
```

复用点不止分片缓冲：`read_block`（第 103 行）的 `vec![0u8; BLOCK_SIZE]` 也在同一个 scratch 里。
两个都是「一次 PUT 内形状固定、内容每块重写」的缓冲，天然适合复用。

> **这个机制的对照必须是「分配计数器」，不能拿 IO 计数糊弄**
> 见 §7 的硬门槛 1。它**不会**减少任何一次 `read_exact_at` / `stat` / `list_dir`，
> 所以如果只看 IO 计数，它必然显示 0 差异——那不是 bug，那是它本来的样子。
> 它的证据形态是「16 MiB PUT 的分配次数」，与其余三个机制不同。

**可选步骤二**：`rstore-erasure::encode_into`（把编码输出直接写进已有缓冲，而不是返回新 `Vec`）。
**由计数器决定要不要做**，本文不预先承诺。若 `ShardScratch` 已经吃掉了绝大部分分配，就不做。

---

## 6. 插桩：`IoLog`

在 **bench 目标内部**自定义 `IoLog`，形态照抄 `crates/store/src/testutil.rs` 的 `RecordingDisk`
（一个实现 `DiskAPI` 的包装类型）：

```rust
/// 记录 IO 调用次数与字节数。包在 `LocalDisk` 外面。
struct IoLog {
    reads: AtomicU64,        // read_exact_at 调用次数
    read_bytes: AtomicU64,   // read_exact_at 字节数
    stats: AtomicU64,        // stat 次数
    lists: AtomicU64,        // list_dir 次数
}
```

再加 `ShardScratch.allocs` 记录分配次数。

> **bench 必须自己重写一份，不能复用 `testutil`**
> `crates/store/src/lib.rs` 里是 `#[cfg(test)] mod testutil;`，而 bench 目标
> （`benches/*.rs`）是**独立编译的 crate**，以普通依赖身份链接 `rstore-store`——
> `#[cfg(test)]` 的模块在那里根本不存在。这和 §7.2 让等价性测试必须待在 `src/` 里
> 是**同一条理由的两个方向**。
>
> 可以原样复用的是 `DiskAPI`（公开 trait）、`LocalDisk`、以及
> `rstore_disk::faulty::Faulty`（dev-dep 已开 `fault-injection` feature）。
> 需要自己写的只有那个包装类型本身，约 60 行。

**为什么插桩不进主代码**：`DiskAPI` 是公开 trait，`async-trait` 已经是 `rstore-store` 的 dev-dependency，
在 bench 里包一层就够了。往生产代码里塞计数器，等于让一次性的基准测试永久污染热路径。

**为什么不做全局分配器探针**：那需要一个 `GlobalAlloc` 实现，而它必然 `unsafe`，
workspace lint 是 `unsafe_code = "forbid"`。`testutil.rs` 的文档注释已经记过这一条，不必再试一次。

> **计数来自插桩运行，计时来自不插桩运行。**
> 这是两批不同的运行，结果不能混在一张表里。插桩会改变时序（尤其是 `AtomicU64` 在热循环里），
> 所以计时必须跑干净的二进制。

---

## 7. 基准工作负载与两个硬门槛

### 7.1 工作负载矩阵

| 编号 | 主测 | 阴性对照（预期差异 ≈ 0） |
|---|---|---|
| L1 | 256 MiB 对象 `GET bytes=0-99` | 同一对象的完整 GET |
| L2 | 同一 key 跑 10k 次 HEAD | 10k 个**不同** key 各 HEAD 一次 |
| L3 | 100k-key 桶 `ListObjectsV2 max-keys=1000` | `max-keys=100000` |
| L4 | `PUT` 16 MiB × N | `PUT` 小于一个块的 64 KiB 对象（单块，无复用机会） |

**每一行都必须有对照位。** 没有对照位，「新模式更快」就只是数字，不是证据——
它无法排除「这台机器今天就是快」或者「你对比的两件事本来就不是一回事」。

> **对照位与 §7.2 的等价性是两件事，别混。**
> 对照位是**基准里故意换掉负载形状**的一栏，用来证明差异确实来自那个机制
> （所以它必然与主测不是同一个工作负载）。
> 等价性测试恰恰相反：它**必须**是同一种工作负载。
> 前者问「这个机制带来了多少差」，后者问「这个机制有没有改坏东西」。

对应关系：

- L1 ↔ `ranged_shard_read`（对照位是为了证明差异来自范围，不是来自「今天磁盘快」）；
- L2 ↔ `metadata_cache`（对照位是关键：不同 key 缓存必然失效，两者应当**一样慢**）；
- L3 ↔ `bounded_listing`（`max-keys=100000` 时新路径被迫扫全，两者应当**一样慢**）；
- L4 ↔ `pooled_write_buffers`（对照位是单块对象：没有第二块可复用，新旧应当**一样**。
  注意它的主证据是**分配计数**而不是 IO 计数——这个机制不减少任何一次 IO，见 §7.2）。

### 7.2 两个硬门槛

**硬门槛 1：新旧差分等价（`#[cfg(test)]` 模块内）**

分三组断言，**第三组是关键**：

| 组 | 场景 | 断言 |
|---|---|---|
| (i) 健康 | 两种模式、**同类工作负载**（范围读对范围读，完整读对完整读） | 输出**逐字节相同** |
| (ii) 损坏在读取范围内 | `FaultyDisk` 注入位腐，损坏块**落在本次读的区间里** | 两种模式产生**相同的错误变体** |
| (iii) 损坏在读取范围外 | 位腐只落在被跳过的块上 | 新模式的可用盘数 **≥** 旧模式，且压过 quorum 时旧模式失败、新模式成功 |

第 (iii) 组不能用「新旧应当一致」去写——它们**本来就该不一致**，那正是这次改动的意义。
把它写成一个明确的、有名字的测试，比让它在 (i) 组的模糊断言里擦边过去要诚实得多。

**第 (iii) 组的断言形式要小心。** 「新模式成功、旧模式失败」不会自动成立：
在 `N=6, data=4, parity=2`、`read_quorum = 4` 的几何下，只让一块盘在有范围外的位腐，
旧模式丢掉一块盘后仍有 5 块可用，**照样成功**。所以正确的断言是两段：

1. 同样的注入下，新模式的可用盘数 ≥ 旧模式（一般情形，弱断言）；
2. 把注入的盘数调到**恰好让旧模式掉到 quorum 以下**（此处 N=6 时是 3 块），
   再断言旧模式 `Err(ReadQuorum)` 而新模式成功（分界情形，强断言）。

只写第 2 段会变成「依赖具体几何的魔法数字」，只写第 1 段又太弱抓不住回归——两段都要。

`Offline` 故障没有「范围」概念（整块盘都不可用），所以新旧应当一致，归入第 (ii) 组。

> **为什么必须是 `src/` 内的 `#[cfg(test)]` 模块**
> `crates/store/src/lib.rs` 里是 `#[cfg(test)] mod testutil;`，所以 `testutil` 对
> `tests/` 下的集成测试和 bench 目标**不可见**；而且 `TestSet.set` 是私有字段。
> 等价性测试要么作为 `src/` 内的 `#[cfg(test)]` 模块存在，要么只用公开 API 自己搭夹具。

**硬门槛 2：每个工作负载都有阴性对照**

见 §7.1。对照位的差异如果显著非零，说明测量本身有问题（插桩泄漏、缓存串扰、
数据集不干净），必须先修测量再谈结论。

### 7.3 证据形态

**以确定性计数为主，墙钟时间为辅。**

「旧模式每块盘读 256 MiB，新模式读 1 MiB」比「快了 3.4 倍」更能站住：
前者是构造性的事实，换台机器、换个负载仍然是它；后者会随机器、缓存、
后台进程漂移。墙钟时间仍然要报，但它是佐证，不是主证据。

`pooled_write_buffers` 是唯一例外——它没有 IO 计数可报，它的主证据就是分配计数。

---

## 8. 声明出来的语义变化

范围读**不再校验被跳过块的位腐**。位腐检测的成本正比于真正读到的范围。

因此以下行为是**预期内的**，不是 bug：

- 完整 GET 能发现块 7 的损坏；读块 1 的范围 GET **不能**发现块 7 的损坏。
- 反过来，可用性是**单向变好**的：一块盘只在**实际需要的那几个块**损坏时才被判为缺块，
  所以同一份数据下，范围读丢的分片只会比整对象读更少、不会更多。

这条变化记入 `docs/DESIGN.md` §11（完整性：bitrot 校验），
并由 §7.2 的第 (iii) 组测试**显式断言**——它是一条被声明出来的行为差异，
不是一个在等价性测试里含糊过去的边角。

**等价性测试只比较同类工作负载**，正是因为这个差异是设计意图而非缺陷——
拿范围读的输出去比对完整读的输出，比的是两件本来就该不一样的事。

---

## 9. 已知不做的（YAGNI）

| 不做 | 理由 |
|---|---|
| `--io-mode` 暴露四个独立 bool | CLI 面上只留 `old` / `new`，避免产生「半新半旧」的运维组合（§3.1） |
| 让 `delete.rs` / `reconcile.rs` / `list.rs` 也用元数据缓存 | 见 §4.2——它们要的是真相，不是「最近为真」 |
| 改 `continuation_token` 的编码方式 | 与本次目标正交；`bounded_listing` 只改一次拿多少 |
| 全局缓冲池 | 见 §5——需求不匹配，代价不匹配 |
| fsync / 组提交优化 | 在 Windows 上 `sync_dir` 是 no-op，量不出来（§1.2） |
| 读修复 / 准入控制 | 属于纠错与背压，与加速正交（§1.2） |

---

## 10. 删除计划

基准跑完之后：

1. **合并**：把获胜路径合并成唯一路径——把 `IoModes` 的每个 `if` 都换成走新分支。
2. **删除**：`crates/common/src/modes.rs`、`ErasureSet` 的 `modes` 字段与 `with_modes`、
   `--io-mode` 标志、`config.rs` 里的 `io_mode` 字段、`wiring.rs` 里的传参。
3. **保留并改写**：等价性测试保留下来，改写成针对新行为的**回归测试**
   （它测的是「新路径做对了」，不再是「新旧一致」）。
4. **保留并折叠**：基准保留，但折叠成单一模式（对照位保留——阴性对照即使在新代码里也有价值）。
5. **旧路径的代码留在 git 历史里**，不在源码里留注释掉的残骸。

删除的验收标准：全仓 grep 不到 `IoModes` / `io_mode` / `io-mode`，
`cargo test --workspace` 与 `cargo clippy --workspace --all-targets -- -D warnings` 全绿。

---

## 11. 对 `docs/DESIGN.md` 的增量

| DESIGN 条目 | 本次之后 |
|---|---|
| §11 完整性 | 追加「范围读的位腐校验范围正比于读到的范围」（§8） |
| §10.5 并行 I/O | 不变；流式 GET 仍不在范围内 |
| §13.3 缓冲池 | 不变；本次做的是局部 `ShardScratch`，**不是**四级池 |
| §16.1 锁 | 元数据缓存复用同一张分片 `RwLock` 表，不新造锁 |
| §16.3 背压 | 不变（明确不做） |
| §20 路线图 | 命名空间索引仍是独立方向；本次的 `bounded_listing` 是它的**前置验证**，不是它的实现 |

---

## 12. 要改的文件

| 文件 | 动作 |
|---|---|
| `crates/common/src/modes.rs` | **新增**：`IoModes` |
| `crates/common/src/lib.rs` | `pub mod modes;` |
| `crates/store/src/set.rs` | 加 `modes` 字段、`with_modes`、`modes()`；`new` 委托 |
| `crates/store/src/reader.rs` | 新增 `BitrotShardReader::read_range` |
| `crates/store/src/get.rs` | `read_shards` 分支；新增 `resolve_version_cached`；`get_object` / `head_object` 接线 |
| `crates/store/src/list.rs` | 新增有界增量遍历；`candidate_keys` 分派 |
| `crates/store/src/put.rs` | `ShardScratch`；`write_shards_stream` / `read_block` 接线 |
| `crates/server/src/config.rs` | `--io-mode` |
| `crates/server/src/wiring.rs` | 传 `IoModes` |
| `crates/store/src/lib.rs` | 等价性测试模块的登记（`#[cfg(test)]`） |
| `crates/store/benches/` + `crates/store/Cargo.toml` | **新增** criterion bench + `[[bench]]` + dev-dep |
| `docs/DESIGN.md` | §11 追加一句 |
| `docs/read-write-path-design.md` | **提交**（目前 git 未跟踪，本文引用了它） |
