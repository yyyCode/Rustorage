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
use rstore_disk::Fault;
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
        assert_eq!(
            a.data,
            data[start as usize..=end as usize],
            "旧模式 {start}-{end}"
        );
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
        set.put_object(put("b", "k", vec![7u8; 700_000]))
            .await
            .unwrap();
    }
    let a = old.head_object("b", "k").await.unwrap();
    let b = new.head_object("b", "k").await.unwrap();
    assert_eq!(
        (a.size, a.etag),
        (b.size, b.etag),
        "覆盖写之后都必须看到新版本"
    );

    // 删除。
    for set in [&old, &new] {
        set.delete_object("b", "k").await.unwrap();
    }
    assert!(matches!(
        old.head_object("b", "k").await,
        Err(StoreError::NotFound)
    ));
    assert!(matches!(
        new.head_object("b", "k").await,
        Err(StoreError::NotFound)
    ));
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
        let out = set.put_object(put("b", "k", data.clone())).await.unwrap();

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
    assert_eq!(
        old.get_object("b", "k", range).await.unwrap().data,
        data[..100]
    );
    assert_eq!(
        new.get_object("b", "k", range).await.unwrap().data,
        data[..100]
    );

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
    // `shard_step` 只看 `size.min(BLOCK_SIZE)`，而本文件两处调用点的对象都 ≥ 一个
    // 满块，所以 `min` 的结果恒为 `BLOCK_SIZE`——用它复算的步长与真实写入一致。
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
