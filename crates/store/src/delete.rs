//! DELETE 与覆盖写的 GC（Task 4.8）。
//!
//! 两个机制：
//!
//! - **覆盖写**走的就是 `put_object` 的完整路径（新的 `data_dir`、新的暂存目录、新的提交），
//!   提交成功后由 [`gc_superseded`] 收拾旧目录。**没有就地改写**这条路。
//! - **DELETE** 写的是一枚**删除标记版本**（`VersionType::DeleteMarker`，`size = 0`，
//!   不带 `USES_DATA_DIR`），走同样的暂存目录 + [`commit`]，quorum 用
//!   `delete_quorum = N/2 + 1`。它不是「把文件删掉」，而是「写一个更新的、
//!   表示『没有对象』的版本」。于是并发读不会出现「一半盘新数据已提交、一半盘旧数据
//!   刚被删」这种谁都读不出来的窗口——`get_object` 发现最新版本是删除标记就返回
//!   `NotFound`。

use uuid::Uuid;

use rstore_meta::encode;

use crate::commit::commit;
use crate::error::StoreError;
use crate::put::build_delete_meta;
use crate::set::{delete_quorum, ErasureSet};

/// GC 掉 `bucket/key` 下除 `winner_dir` 之外的版本目录。
///
/// **规则只有一条，写死，别再加特例——只在一块盘同时持有 `winner_dir` 时才动手。**
/// 逐盘执行：列出 `bucket/key` 下的所有条目；若其中包含 `winner_dir`，就把其余条目
/// `remove_dir_all`；否则**一个都不动**。
///
/// 为什么要有「同时持有胜出目录」这个前提：覆盖写提交时可能有盘 rename 失败（落后盘）。
/// 落后盘上只有旧目录，此时删掉旧目录会让这块盘变成**彻底没有这个对象**——而它本来
/// 还能为读提供一份有效分片。留着的代价只是磁盘占用，删掉的代价是丢失一份冗余。
///
/// **best-effort 且幂等**：删除失败只记日志，不上抛（残留由对账 Task 4.10 兜底）；
/// `remove_dir_all` 对不存在的路径返回 `Ok`，所以空跑一次不会改变任何东西。
///
/// **顺序不可颠倒：调用方必须先让 `commit` 成功，再调用本函数**——反过来就是在删
/// 还没提交的数据。它删的是胜出目录之外的**所有**条目（含 `.staging-*`），因此不是
/// 一个可以随便并发调用的清理器；真正安全地清理崩溃残留是 Task 4.10 的 `reclaim_orphans`。
pub(crate) async fn gc_superseded(set: &ErasureSet, bucket: &str, key: &str, winner_dir: &str) {
    let key_rel = format!("{bucket}/{key}");
    for (i, slot) in set.disks().iter().enumerate() {
        let Some(disk) = slot else { continue };

        // 盘掉线（Transient）或目录不存在（NotFound）都按「这块盘没有可回收的东西」处理，
        // 不冒泡也不记 error：GC 是收尾动作，够不着某块盘不该影响任何调用方。
        let entries = match disk.list_dir(&key_rel).await {
            Ok(v) => v,
            Err(_) => continue,
        };

        // **前提：这块盘同时持有胜出目录。** 没有它就一个都不动（见函数文档）。
        if !entries.iter().any(|e| e == winner_dir) {
            continue;
        }

        for entry in entries {
            if entry == winner_dir {
                continue;
            }
            let victim = format!("{key_rel}/{entry}");
            // 删除失败只记日志不上抛：残留交给对账（Task 4.10）回收。
            if let Err(e) = disk.remove_dir_all(&victim).await {
                tracing::warn!(
                    disk = i,
                    path = %victim,
                    error = %e,
                    "GC 未能回收被取代的版本目录，留给对账处理"
                );
            }
        }
    }
}

impl ErasureSet {
    /// 写一枚删除标记并提交；`delete_quorum = N/2 + 1`。
    ///
    /// 对不存在的 key 也照样写标记并返回 `Ok`——这是 S3 的语义
    /// （DELETE 幂等，重复删同一 key、删一个从没存在过的 key 都成功）。
    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), StoreError> {
        let total = self.total();
        let txid = Uuid::new_v4();
        // `marker_dir`（目录名）与 `marker_version_id`（header 里的 version_id）必须是
        // 两个各自独立的 uuid：它们承担不同的身份，共用同一个只是在制造隐式耦合。
        let marker_dir = Uuid::new_v4();
        let marker_version_id = Uuid::new_v4();

        // `.staging-` 前缀见 Task 4.5 的布局说明：没有它，未提交的半成品目录会在
        // 版本仲裁里胜出。`final_rel` 每次写入都不同（新的 `marker_dir`）——两次删除
        // 共用同一目录名时，第二次 rename 会因目标已存在而失败。
        let staging = format!("{bucket}/{key}/.staging-{txid}");
        let final_rel = format!("{bucket}/{key}/{marker_dir}");

        let bytes = encode(&build_delete_meta(marker_version_id)?)?;
        // 逐盘写暂存 meta，失败只忽略；最终 quorum 由 commit 的 rename 判定。
        self.write_meta_all(&staging, &bytes).await;

        // 先让标记在多数盘上落地，**再**回收旧数据。低于 quorum 时 commit 会回滚
        // 自己 rename 过去的目录并以 `WriteQuorum { required: delete_quorum }` 收场——
        // 复用同一个变体，`required` 字段本身就把数字说清楚了。
        commit(self, &staging, &final_rel, delete_quorum(total)).await?;

        // 标记已提交：现在它才是权威目录，其余目录（含旧数据目录与上一枚标记）才可回收。
        gc_superseded(self, bucket, key, &marker_dir.to_string()).await;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstore_common::error::DiskError;
    use rstore_disk::faulty::Fault;

    use super::*;
    use crate::error::StoreError;
    use crate::put::PutArgs;
    use crate::testutil::{body, set_with_disks, TestSet};

    fn put_args(bucket: &str, key: &str, data: Vec<u8>) -> PutArgs {
        PutArgs {
            bucket: bucket.into(),
            key: key.into(),
            body: body(data),
            etag: None,
        }
    }

    /// 某块盘上 `bucket/key` 下还剩下哪些版本目录（已排序）。
    /// 缺失目录按空列表处理；其余错误必须 panic——那说明夹具本身坏了，
    /// 静默当成空列表会让「GC 把东西删光了」这类断言空洞地通过。
    async fn dirs_on(set: &TestSet, disk_idx: usize, key_rel: &str) -> Vec<String> {
        let d = set.disks()[disk_idx].as_ref().expect("该盘应当在线");
        match d.list_dir(key_rel).await {
            Ok(v) => v,
            Err(DiskError::NotFound) => Vec::new(),
            Err(e) => panic!("disk {disk_idx} list_dir 失败: {e:?}"),
        }
    }

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
        set.put_object(put_args("b", "k", vec![1u8; 2_000_000]))
            .await
            .unwrap();

        for i in 0..2 {
            set.inject_fault_on(i, Fault::Offline);
        }
        let out_b = set
            .put_object(put_args("b", "k", vec![2u8; 2_000_000]))
            .await
            .unwrap();
        for i in 0..2 {
            set.clear_fault_on(i);
        }
        // 覆盖写仍然成功：4 块盘 ≥ write_quorum(4)。
        assert_eq!(
            set.get_object("b", "k", None).await.unwrap().data,
            vec![2u8; 2_000_000]
        );

        for i in 0..2 {
            let dirs = dirs_on(&set, i, "b/k").await;
            assert_eq!(dirs.len(), 1, "disk {i} 应只留旧目录，got {dirs:?}");
            assert_ne!(
                dirs[0],
                out_b.data_dir.to_string(),
                "disk {i} 上不该有胜出目录"
            );
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
        set.put_object(put_args("b", "k", vec![3u8; 1_000_000]))
            .await
            .unwrap();
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
        let out = set
            .put_object(put_args("b", "k", vec![4u8; 1_000_000]))
            .await
            .unwrap();

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
            assert_eq!(
                dirs,
                vec![out.data_dir.to_string()],
                "disk {i} 不该回收旧数据"
            );
        }
        for i in 2..6 {
            let dirs = dirs_on(&set, i, "b/k").await;
            assert_eq!(dirs.len(), 1, "disk {i}, got {dirs:?}");
            assert_ne!(
                dirs[0],
                out.data_dir.to_string(),
                "disk {i} 应只剩删除标记目录"
            );
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

        set.put_object(put_args("b", "k", vec![5u8; 1_000_000]))
            .await
            .unwrap();
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
        set.put_object(put_args("b", "k", vec![6u8; 1_000_000]))
            .await
            .unwrap();
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
