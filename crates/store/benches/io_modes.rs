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
//
// **规模是从「跑得完」倒推的，不是从「好看」倒推的**：种数据要写
// `L2_DISTINCT + L3_KEYS + 2` 个对象 × 2 个 set，每次 PUT 都要在 6 块盘上
// fsync——实测约 33 ms/次，这个数决定了整套基准的墙钟下限。所以 key 数是
// 数量级够用就好，不是越大越好。

/// 只读 100 字节的那个对象有多大。
const L1_SIZE: usize = 64 * 1024 * 1024;
/// HEAD 密集负载的请求数。**不种数据，纯请求量**，所以可以比 key 数大得多。
const L2_HEADS: usize = 2_000;
/// HEAD 阴性对照里不同 key 的个数（每多一个 key 都要一次 PUT，所以比主测小）。
const L2_DISTINCT: usize = 100;
/// 列举负载的桶里有多少 key。
const L3_KEYS: usize = 600;
/// 列举主测要凑满一页多少条。
const L3_PAGE: usize = 100;
/// 写负载的对象大小与轮数。
const L4_SIZE: usize = 16 * 1024 * 1024;
const L4_ITERS: usize = 4;
/// 每个 `(模式, 工作负载)` 重复几轮，取最小值压掉噪声。
const REPEAT: usize = 2;

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
///
/// **先跑一轮热身并丢掉**（热身用轮次下标 `REPEAT`，与计数的 `0..REPEAT` 不重叠）。
/// 不加这一步，第一次调用与后续调用本来就不同：`metadata_cache` 的**第一次**必然
/// 未命中——它要多做一轮 `list_dir` + `read_meta`，第二次起直接命中。这不是脏
/// 计数，是「冷启动 vs 稳态」，而断言分不出这两者。热身之后量的是**稳态**，
/// 这正是缓存类机制该被量的形态；报告里必须写明这一点。
async fn measure<F, Fut>(fx: &Fixture, mut body: F) -> Measured
where
    F: FnMut(usize) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    body(REPEAT).await;

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
                // **`read_bytes` 不参与这条断言**：它不总是确定的。`meta.xl` 的长度
                // 在每次 PUT 之间本来就不同——两个 UUID 走 `HeaderWire` 的 `[u8; 16]`
                // 编码，`rmp_serde` 对 ≥ 0x80 的字节要多花一个 `uint8` 前缀，长度随
                // 随机字节在 19~35 字节之间浮动。所以「读不同对象的元数据」这类负载
                // （L2 的阴性对照）逐轮会差几百字节，那是格式性质，不是脏计数。
                // 调用次数（reads/stats/lists）与 allocs 才是确定性的那部分。
                assert_eq!(
                    (counts.0, counts.2, counts.3, allocs),
                    (prev.counts.0, prev.counts.2, prev.counts.3, prev.allocs),
                    "第 {round} 轮的调用计数与第 0 轮不同（{:?}/{allocs} vs {:?}/{}）——\
                     计数是确定性的，不同就说明有东西没被重置，这个数字不能用",
                    (counts.0, counts.2, counts.3),
                    (prev.counts.0, prev.counts.2, prev.counts.3),
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
///
/// **key 里必须带轮次下标**（`d{round}-{i:06}`）。不带的话热身那一轮就把这些 key
/// 全灌进缓存了，计数的每一轮都是**命中**——新模式会显示 0 次 IO，而这条对照要
/// 证明的恰恰是「不同 key 时新模式并不更快」。带上轮次下标，每轮、包括热身那轮，
/// 看的都是没见过的 key，缓存必然未命中，对照才成立。种子数据因此是
/// `(REPEAT + 1) × L2_DISTINCT` 个 key，不是 `L2_DISTINCT` 个。
async fn l2_distinct(fx: &Fixture) -> Measured {
    measure(fx, |round| async move {
        for i in 0..L2_DISTINCT {
            fx.set
                .head_object("b", &format!("d{round}-{i:06}"))
                .await
                .unwrap();
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

/// 同一个桶里**除了** L3 那批 key 还有别的东西（`big`、`hot`、L2 的 `d*`），
/// 所以种子数据的**总**条目数是 `L3_KEYS + OTHER_KEYS`。
///
/// L2 那批 key 是 `(REPEAT + 1) × L2_DISTINCT` 个：每一轮（含热身）都要看一组
/// 没见过的 key，见 [`l2_distinct`]。
const OTHER_KEYS: usize = 2 + (REPEAT + 1) * L2_DISTINCT;

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
/// 旧模式 `L4_ITERS × 16 × 4`，新模式 `L4_ITERS × 4`，16 倍。
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

/// L4 阴性对照：单块对象，没有第二块可复用。
///
/// 分配次数也必须**逐项相同**（`L4_ITERS` 次 PUT × 1 块 × 4 份）：只有一次分片机会时，
/// 「复用」和「重新分配」是同一件事。这条对照证明主测那 16 倍来自跨块复用，
/// 而不是来自某种全局的分配路径改变。
///
/// **512 KiB 是刻意选的**，不是「小于一个块就行」：≤ 128 KiB 的对象走内联分支
/// （`should_inline`），数据进 `meta.xl`、根本不产生分片文件，两边 `allocs` 都是 0
/// ——那也是「逐项相同」，但空洞。要一个**真的**单块对象，得落在
/// `(128 KiB, BLOCK_SIZE]` 这段区间里。
async fn l4_put_small(fx: &Fixture) -> Measured {
    let buf = vec![0x5Au8; 512 * 1024];
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
    // L2 对照的若干 key：每一轮（含热身那轮）一组，各自独立。
    for round in 0..=REPEAT {
        for i in 0..L2_DISTINCT {
            fx.set
                .put_object(put_args(
                    "b",
                    &format!("d{round}-{i:06}"),
                    &vec![0x33u8; 4_096],
                ))
                .await
                .unwrap();
        }
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
            m.counts.0, m.counts.1, m.counts.2, m.counts.3, m.allocs, m.micros
        )
    };
    println!("\n=== {name} ===");
    println!(
        "{:>6}  {:>10}  {:>14}  {:>10}  {:>9}  {:>8}  {:>11}",
        "mode", "reads", "read_bytes", "stats", "lists", "allocs", "wall_us"
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
    print_row(
        "L1 GET bytes=0-99（主测）",
        l1_ranged(&old).await,
        l1_ranged(&new).await,
    );
    print_row(
        "L1 完整 GET（阴性对照：范围读无可省）",
        l1_full(&old).await,
        l1_full(&new).await,
    );

    print_row(
        &format!("L2 同一 key {L2_HEADS} 次 HEAD（主测）"),
        l2_same_key(&old).await,
        l2_same_key(&new).await,
    );
    print_row(
        &format!("L2 {L2_DISTINCT} 个不同 key 各 HEAD 一次（阴性对照：缓存必然失效）"),
        l2_distinct(&old).await,
        l2_distinct(&new).await,
    );

    print_row(
        &format!("L3 从 {L3_KEYS} 个 key 里凑满 {L3_PAGE} 条（主测）"),
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
    assert_eq!(
        l4.0.allocs,
        4 * 16 * 4,
        "旧模式：L4_ITERS 次 PUT × 16 块 × 4 份缓冲"
    );
    assert_eq!(
        l4.1.allocs,
        4 * 4,
        "新模式：L4_ITERS 次 PUT × 首块 4 份，之后复用"
    );
    print_row("L4 PUT 16 MiB × N（主测）", l4.0, l4.1);

    let ctrl = (l4_put_small(&old).await, l4_put_small(&new).await);
    assert_eq!(
        ctrl.0.allocs, ctrl.1.allocs,
        "阴性对照：单块对象没有第二块可复用，分配次数必须完全相同"
    );
    assert_eq!(ctrl.0.allocs, 4 * 4, "L4_ITERS 次 PUT × 1 块 × 4 份缓冲");
    print_row(
        "L4 PUT 512 KiB × N（阴性对照：单块，无复用机会）",
        ctrl.0,
        ctrl.1,
    );

    println!(
        "\n注意：计数与墙钟来自**同一次**运行，`IoLog` 的原子自增会影响时序。\n\
         读法：计数是主证据（确定性、可复现），墙钟是佐证。\n\
         reads/read_bytes/stats/lists 四个机制各自对应的那一列应当差异巨大，\n\
         其余三列（含 L4 的全部 IO 列）差异接近 0 是**预期**，不是没生效。\n\
         L4 的证据在 `allocs` 列：旧 L4_ITERS×16×4 / 新 L4_ITERS×4，单块对照两行相同。\n\
         它量的是**我们自己的代码显式增长分片缓冲的次数**，不是分配器的真实分配次数——\n\
         全局分配器探针要 `unsafe`，而 workspace lint 是 `unsafe_code = \"forbid\"`。"
    );
}
