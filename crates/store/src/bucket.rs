//! 桶操作（Task 4.11）：create / delete / exists / list。
//!
//! **桶存在 ⟺ 该桶目录下有 `.rstore.sys/bucket.meta`。** 标记文件是显式的：
//! 光凭「有目录」判断的话，`delete_bucket` 删掉整个桶目录后任何残留的空目录都会
//! 让「桶还在不在」失真。标记内容是 `{}`（不解析、不演进）。
//!
//! **桶级元数据的 quorum：** 桶级元数据没有纠删码，不能套用 `read_quorum` /
//! `write_quorum`（那是针对分片几何的定义）。这里需要的语义只有「多数派可见」，
//! 于是复用已有定义的 `delete_quorum(total) = total / 2 + 1`（严格多数）作为
//! 「桶级操作的门槛」。它**不是** `delete_quorum` 在语义上被挪用——两者恰好都是
//! 「严格多数」而已。这条门槛保证的是单调性：不会一半盘认为桶在、一半认为不在。

use std::collections::BTreeSet;

use crate::error::StoreError;
use crate::set::{delete_quorum, ErasureSet};

/// 桶标记文件相对盘根的路径。桶存在 ⟺ 它在（≥ 严格多数）的盘上存在。
fn bucket_meta_rel(bucket: &str) -> String {
    format!("{bucket}/.rstore.sys/bucket.meta")
}

impl ErasureSet {
    /// 建桶。幂等：桶已存在也返回 `Ok`（S3 的 `BucketAlreadyOwnedByYou` 不在 MVP 范围，
    /// 而 `aws s3 mb` 的重复调用不该让冒烟脚本失败）。
    ///
    /// 逐盘写标记（`write_all` 会自动建父目录），成功盘数 ≥ 严格多数 → `Ok`。
    pub async fn create_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        let required = delete_quorum(self.total());
        let rel = bucket_meta_rel(bucket);
        let mut achieved = 0u8;
        // 掉线的盘与 `None` 槽位都只算「没成功」，不像错误一样上抛。
        for disk in self.disks().iter().flatten() {
            if disk.write_all(&rel, b"{}").await.is_ok() {
                achieved += 1;
            }
        }
        if achieved < required {
            return Err(StoreError::WriteQuorum { achieved, required });
        }
        Ok(())
    }

    /// 桶是否存在：标记在 **≥ 严格多数** 的盘上存在 → `true`。
    ///
    /// 只数「能读到且是文件」的盘；读不到 / 掉线 / 根本没有都只算「这块盘没有」。
    pub async fn bucket_exists(&self, bucket: &str) -> Result<bool, StoreError> {
        let required = delete_quorum(self.total());
        let rel = bucket_meta_rel(bucket);
        let mut present = 0u8;
        for disk in self.disks().iter().flatten() {
            if let Ok(Some(st)) = disk.stat(&rel).await {
                if !st.is_dir {
                    present += 1;
                }
            }
        }
        Ok(present >= required)
    }

    /// 删桶。桶里有**活对象**（即 `list_objects(bucket, None)` 非空）→ `BucketNotEmpty`。
    /// 只有删除标记与孤儿目录的桶算空桶，可以删。
    ///
    /// 顺序：先确认桶存在（**弱判定**：只要有任何一块在线盘报告标记存在就算在），
    /// 再判空，最后逐盘 `remove_dir_all`。成功盘数 < 严格多数 →
    /// `WriteQuorum`（**复用同一个变体**，不为「删桶没删动」新造变体：5.8 的错误映射
    /// 表里它对应 500，与新建桶失败同一类）。
    ///
    /// 注意存在性用的是**弱判定**而非 [`bucket_exists`](Self::bucket_exists) 的严格多数：
    /// 删桶时只要还有盘持有标记，就说明桶确实存在过，此时「多数盘掉线」要报
    /// `WriteQuorum`（写不动）而不是 `NotFound`（本来就没有）。用严格多数会把
    /// 「存在但掉线过半」误报成「不存在」，正是测试
    /// `delete_bucket_needs_a_strict_majority` 钉住的那条。
    pub async fn delete_bucket(&self, bucket: &str) -> Result<(), StoreError> {
        // 桶压根不在（没有任何一块在线盘报告标记存在）→ NotFound。
        let rel = bucket_meta_rel(bucket);
        let mut present_any = false;
        for disk in self.disks().iter().flatten() {
            if let Ok(Some(st)) = disk.stat(&rel).await {
                if !st.is_dir {
                    present_any = true;
                    break;
                }
            }
        }
        if !present_any {
            return Err(StoreError::NotFound);
        }

        // 先判空再删：反过来就是「先删了再发现不该删」。
        if !self.list_objects(bucket, None).await?.is_empty() {
            return Err(StoreError::BucketNotEmpty);
        }

        let required = delete_quorum(self.total());
        let mut achieved = 0u8;
        for disk in self.disks().iter().flatten() {
            // `remove_dir_all` 幂等（路径不存在也 `Ok`），失败只算「这块盘没删成」。
            if disk.remove_dir_all(bucket).await.is_ok() {
                achieved += 1;
            }
        }
        if achieved < required {
            return Err(StoreError::WriteQuorum { achieved, required });
        }
        Ok(())
    }

    /// 所有盘的桶名并集（已排序）。**跳过以 `.` 开头的条目**——
    /// 盘根下有 `.rstore.sys/`（DESIGN §6.2），它不是一个桶。
    pub async fn list_buckets(&self) -> Result<Vec<String>, StoreError> {
        let mut union: BTreeSet<String> = BTreeSet::new();
        for disk in self.disks().iter().flatten() {
            if let Ok(entries) = disk.list_dir("").await {
                for name in entries {
                    if !name.starts_with('.') {
                        union.insert(name);
                    }
                }
            }
        }
        Ok(union.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use rstore_disk::faulty::Fault;

    use super::*;
    use crate::put::PutArgs;
    use crate::testutil::set_with_disks;

    fn put_args(bucket: &str, key: &str, n: usize) -> PutArgs {
        PutArgs {
            bucket: bucket.into(),
            key: key.into(),
            data: vec![7u8; n],
        }
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
        set.put_object(put_args("data", "k", 1_000_000))
            .await
            .unwrap();

        let r = set.delete_bucket("data").await;
        assert!(matches!(r, Err(StoreError::BucketNotEmpty)), "got {r:?}");
        assert!(
            set.bucket_exists("data").await.unwrap(),
            "拒绝之后桶必须原样在"
        );

        // 删掉对象（写的是删除标记）之后桶就算空了：墓碑与孤儿都不算「非空」。
        set.delete_object("data", "k").await.unwrap();
        set.delete_bucket("data").await.unwrap();
        assert!(!set.bucket_exists("data").await.unwrap());
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
