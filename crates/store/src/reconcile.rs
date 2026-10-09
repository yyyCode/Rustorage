//! 崩溃点对账（Task 4.10）：扫描 `bucket` 下没有被任何权威元数据引用的目录并回收。
//!
//! 这一节存在的理由只有两条**两种情况下都必须成立**的不变量：
//!
//! 1. **可见性**：对象要么完全可见且内容正确，要么 `NotFound`。绝不出现「可读但内容
//!    不对」「读一半报错」「读到半成品」。
//! 2. **对账不改观测结果**：跑一遍 `reclaim_orphans` 之后，每个对象的可读结果（内容
//!    或 `NotFound`）必须与跑之前完全一致。这就是「垃圾回收不删活数据」的可执行定义。
//!
//! 因此对 [`crate::get::resolve_version`] 的结果必须**三态分派**，任何一态都不能省：
//!
//! - `Version { dir }`（**包括删除标记**——它也有权威目录）→ 只有 `dir` 是权威的，
//!   其余条目（含 `.staging-*`）是孤儿；
//! - `Absent` → 发现阶段已排除所有非 `.staging-` 条目，所以这个 key 下剩的全是
//!   暂存目录，**全部是孤儿**；
//! - `Err(..)`（`ReadQuorum` 或硬错误）→ **跳过这个 key**：有候选目录却选不出权威
//!   版本，此刻删什么都不安全。
//!
//! **`Absent` 与「删除标记」是两回事，别混为一谈。** 删除标记不是 `Absent`：它有权威
//! 目录（标记目录），走的是 `Version` 那一支。把两者合并成「没有可读对象就清理」，
//! 就会删掉标记目录，让那些没拿到标记的盘上的旧版本复活，直接违反不变量 2。

use std::collections::BTreeSet;

use crate::delete::gc_superseded;
use crate::error::StoreError;
use crate::get::{resolve_version, Resolved};
use crate::set::ErasureSet;

impl ErasureSet {
    /// 列出 `bucket` 下**没有被任何权威元数据引用**的目录（含 `.staging-*`）。
    ///
    /// 返回的是相对 `bucket` 的路径（如 `k/.staging-3f2a…`），便于报错时直接看。
    /// 单个 key 判定不出权威版本（`ReadQuorum` 或硬错误）时跳过该 key——有候选目录却
    /// 选不出权威版本，此刻列任何孤儿都不安全。
    ///
    /// **不与写入并发运行**：本函数按定义会把 `.staging-*` 视为未提交的残留，但它分不清
    /// 「上次崩溃留下的」与「此刻正在写的」。对账是离线/运维动作，不是随写随跑的 GC。
    pub async fn scan_orphans(&self, bucket: &str) -> Result<Vec<String>, StoreError> {
        let mut orphans: Vec<String> = Vec::new();
        for key in self.entries_under(bucket).await {
            let key_rel = format!("{bucket}/{key}");
            match resolve_version(self, bucket, &key).await {
                // 权威目录是 `winner`；其余条目（含 `.staging-*`）全是孤儿。
                // 删除标记目录**就是** `winner`，所以它自己不会被列成孤儿。
                Ok(Resolved::Version { dir: winner, .. }) => {
                    for entry in self.entries_under(&key_rel).await {
                        if entry != winner {
                            orphans.push(format!("{key}/{entry}"));
                        }
                    }
                }
                // 发现阶段已排除所有非 `.staging-` 条目：剩下的全是暂存目录，全是孤儿。
                Ok(Resolved::Absent) => {
                    for entry in self.entries_under(&key_rel).await {
                        orphans.push(format!("{key}/{entry}"));
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        bucket,
                        key = %key,
                        error = %e,
                        "对账跳过无法判定权威版本的 key"
                    );
                }
            }
        }
        orphans.sort();
        Ok(orphans)
    }

    /// 删除孤儿。**绝不删除活数据**：只在一块盘同时持有该 key 的权威目录时才动手
    /// （与 Task 4.8 的 GC 同一条规则，直接复用 [`gc_superseded`]）。
    ///
    /// 按 [`scan_orphans`](Self::scan_orphans) 的同一个三态分派：
    ///
    /// - `Version { dir }` → `gc_superseded`：只在一块盘**同时持有 `dir`** 时才删该盘上
    ///   的其余条目；没拿到 `dir` 的盘一个都不动——它上面那份旧分片仍是有效冗余。
    /// - `Absent` → 该 key 下只剩 `.staging-*`，逐盘 `remove_dir_all` 即可（没有权威
    ///   目录要保护，也没有任何东西读得出来）。
    /// - `Err(..)` → 跳过这个 key。
    ///
    /// **best-effort**：单个 key / 单块盘的失败只记日志，不上抛。
    ///
    /// **不与写入并发运行**——见 [`scan_orphans`](Self::scan_orphans)：
    /// 实现是「列出 `.staging-*` 就删」，分不清「上次崩溃留下的」与「此刻正在写的」。
    /// 真要并发，得给暂存目录带上 pid/时间戳并按年龄判断（Phase 3）。
    pub async fn reclaim_orphans(&self, bucket: &str) -> Result<(), StoreError> {
        for key in self.entries_under(bucket).await {
            let key_rel = format!("{bucket}/{key}");
            match resolve_version(self, bucket, &key).await {
                // 权威目录存在：复用 4.8 的 GC 规则，不再写第二份判定。
                Ok(Resolved::Version { dir: winner, .. }) => {
                    gc_superseded(self, bucket, &key, &winner).await;
                }
                // 只剩暂存目录：没有权威目录要保护，逐盘清空该 key。
                Ok(Resolved::Absent) => {
                    for (i, slot) in self.disks().iter().enumerate() {
                        let Some(disk) = slot else { continue };
                        // 掉线 / 目录不存在（NotFound）都按「这块盘没有可回收的东西」处理。
                        let entries = match disk.list_dir(&key_rel).await {
                            Ok(v) => v,
                            Err(_) => continue,
                        };
                        for entry in entries {
                            let victim = format!("{key_rel}/{entry}");
                            if let Err(e) = disk.remove_dir_all(&victim).await {
                                tracing::warn!(
                                    disk = i,
                                    path = %victim,
                                    error = %e,
                                    "对账未能回收暂存目录"
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        bucket,
                        key = %key,
                        error = %e,
                        "对账跳过无法判定权威版本的 key"
                    );
                }
            }
        }
        Ok(())
    }

    /// 所有盘上 `rel` 目录下的条目名**并集**（已去重、升序）。
    ///
    /// 某块盘缺这个目录（`NotFound`）或读不到（其它错误）都按「这块盘没贡献条目」处理：
    /// 对账是 best-effort，够不着某块盘不该让整次扫描失败。key 列表与 key 下的目录
    /// 列表都走这一条——各盘的持有集合本来就不必相同（落后盘、部分提交都会造成差异）。
    async fn entries_under(&self, rel: &str) -> Vec<String> {
        let mut union: BTreeSet<String> = BTreeSet::new();
        for slot in self.disks().iter().flatten() {
            if let Ok(entries) = slot.list_dir(rel).await {
                union.extend(entries);
            }
        }
        union.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use rstore_disk::faulty::{Fault, FaultKind};

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

    /// 每块盘上 `key_rel` 下的条目（缺失目录按空列表处理）。用来逐盘比较对账前后。
    async fn dirs_all(set: &TestSet, key_rel: &str) -> Vec<Vec<String>> {
        let mut out = Vec::new();
        for slot in set.disks() {
            match slot {
                Some(d) => out.push(d.list_dir(key_rel).await.unwrap_or_default()),
                None => out.push(Vec::new()),
            }
        }
        out
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
                    Fault::FailAfter {
                        calls,
                        kind: FaultKind::Transient,
                    },
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
        assert!(
            ok_count > 0,
            "没有任何一个中断点让 PUT 成功过，这轮测试没测到东西"
        );
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
                    Fault::FailAfter {
                        calls,
                        kind: FaultKind::Transient,
                    },
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

    /// `Absent` 与「删除标记」必须走**不同**的分支：删除标记**有**权威目录，
    /// 对账绝不能把它删掉。若对账把标记目录当垃圾清掉，持有标记的盘就少掉那个目录，
    /// 那些没拿到标记的盘上的旧版本随即成为候选——正是「对账改变可观测结果」的起点。
    /// 这条把三态分派里最容易退化成两态的那一处单独钉住。
    #[tokio::test]
    async fn reclaim_keeps_the_delete_marker_as_authority() {
        let set = set_with_disks(6, 2).await;
        set.put_object(PutArgs {
            bucket: "b".into(),
            key: "k".into(),
            data: vec![9u8; 1_000_000],
        })
        .await
        .unwrap();

        // 2 块盘写不了 → 删除标记只落在 4 块盘（= delete_quorum(6)）；那 2 块盘上
        // 仍留着旧数据目录（有效冗余，但必须被标记压住）。
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

        // 对账前每块盘上 `b/k` 的条目——持有标记的盘上应只剩标记目录，
        // 没拿到标记的盘上仍是旧数据目录。
        let before = dirs_all(&set, "b/k").await;
        set.reclaim_orphans("b").await.unwrap();
        let after = dirs_all(&set, "b/k").await;

        assert_eq!(
            after, before,
            "对账改动了删除标记现场：标记目录被当成孤儿清掉了"
        );
        assert!(
            matches!(
                set.get_object("b", "k", None).await,
                Err(StoreError::NotFound)
            ),
            "对账之后删除标记失效，旧版本复活了"
        );
    }
}
