//! 对象列举（Task 4.11）：递归遍历桶目录 + 复用 `resolve_version` 过滤掉删除标记与未提交暂存。
//!
//! 列举**必须与 GET 用同一处判断**「这个 key 算不算有对象」——否则会出现
//! 「GET 说没有、LIST 说有」这种最难查的不一致。因此候选 key 交给
//! [`crate::get::resolve_version`]，只有 `live()` 是 `Some` 才收录。
//!
//! `list_dir` 只返回条目名、不区分文件与目录，所以递归时每个条目要补一次 `stat`
//! 看 `is_dir`。候选目录发现复用 [`crate::reconcile`] 的 `entries_under`
//! （各在线盘 `list_dir` 的并集），**不重写第二遍**「某块盘读不到怎么办」。

use std::collections::BTreeSet;

use rstore_common::consts::RESERVED_PREFIX;

use crate::error::StoreError;
use crate::get::{etag_of_meta, latest_version, resolve_version};
use crate::set::ErasureSet;

/// LIST 返回的一行。**只包含仍然活着的对象**——删除标记与纯孤儿都不出现。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectEntry {
    pub key: String,
    pub size: u64,
    pub etag: String,
    /// `header.mod_time`；`None` 时按 0 处理（PUT 总会写它，见 4.5）。
    pub mod_time: u64,
}

/// `a` / `b` 拼成相对路径；`a` 为空时就是 `b`。
fn join_rel(a: &str, b: &str) -> String {
    if a.is_empty() {
        b.to_string()
    } else if b.is_empty() {
        a.to_string()
    } else {
        format!("{a}/{b}")
    }
}

/// 相对路径的父路径（`a/b/c` → `a/b`）；没有父（顶层单段）时返回 `None`。
fn parent_path(rel: &str) -> Option<String> {
    rel.rsplit_once('/').map(|(p, _)| p.to_string())
}

impl ErasureSet {
    /// `bucket` 下所有活对象，按 key 升序。`prefix` 为 `None` 时返回全部。
    ///
    /// 冒烟/对账类调用方（`rclone sync`）会拿这个列表去删远端数据，所以**列不全绝
    /// 不静默**：`resolve_version` 的 `Err(ReadQuorum)` 原样上抛，而不是返回一个
    /// 残缺列表。
    ///
    // PERF: 见 DESIGN §1.2 与 §20 Phase 2 — 命名空间索引。MVP 是全盘遍历 +
    // 每 key 一次元数据仲裁；不做前缀剪枝（`prefix` 按 key 字符串前缀，而 key 的
    // 目录切分与它并不对齐，剪错就是静默丢结果）。
    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ObjectEntry>, StoreError> {
        let mut out: Vec<ObjectEntry> = Vec::new();
        // 候选来自 BTreeSet（已升序），逐个判定权威版本后按同序 push，天然升序。
        for key in self.candidate_keys(bucket).await {
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
        // 前缀过滤在最后做一次（MVP 不做前缀剪枝，见上面的 PERF 注释）。
        if let Some(p) = prefix {
            out.retain(|e| e.key.starts_with(p));
        }
        Ok(out)
    }

    /// `bucket` 下所有**候选** key（相对 bucket 的路径），升序。
    ///
    /// 递归规则见 Task 4.11 规格：
    /// - 某目录的 `list_dir`（各在线盘并集）含 `meta.xl` → 它是版本目录，其父路径
    ///   就是一个 key，记入并**停止往下递归**；
    /// - 否则对每个条目：跳过 `.staging-*`（未提交暂存）；`p` 为空（bucket 根）时
    ///   再跳过 `.rstore*`（系统目录，更深层的段不跳）；`stat` 是目录才递归。
    ///
    /// 用显式栈做深度优先，避免 async 递归（递归 async fn 需要装箱）。
    async fn candidate_keys(&self, bucket: &str) -> Vec<String> {
        let mut keys: BTreeSet<String> = BTreeSet::new();
        let mut stack: Vec<String> = vec![String::new()];
        while let Some(dir_rel) = stack.pop() {
            let entries = self.entries_under(&join_rel(bucket, &dir_rel)).await;
            if entries.iter().any(|e| e == "meta.xl") {
                if let Some(key) = parent_path(&dir_rel) {
                    keys.insert(key);
                }
                continue;
            }
            let at_root = dir_rel.is_empty();
            for name in entries {
                if name.starts_with(".staging-") {
                    continue;
                }
                // 用户 key 的首段不可能是保留前缀（DESIGN §6.3 / Task 5.7），
                // 所以这只跳过系统目录，不会藏掉用户数据。
                if at_root && name.starts_with(RESERVED_PREFIX) {
                    continue;
                }
                let child_rel = join_rel(&dir_rel, &name);
                if self.any_disk_is_dir(&join_rel(bucket, &child_rel)).await {
                    stack.push(child_rel);
                }
            }
        }
        keys.into_iter().collect()
    }

    /// 是否**有任何一块在线盘**报告 `rel` 是一个目录。
    ///
    /// 递归只需要「能不能进得去」；某块盘缺这个目录 / 掉线都不该阻断遍历——真正
    /// 的完整性由每个候选 key 的 `resolve_version` 把关。`stat` 走 `fs::metadata`
    /// 会跟随符号链接，但 MVP 的数据目录由本程序自己创建、不产生链接。
    async fn any_disk_is_dir(&self, rel: &str) -> bool {
        for disk in self.disks().iter().flatten() {
            if let Ok(Some(st)) = disk.stat(rel).await {
                if st.is_dir {
                    return true;
                }
            }
        }
        false
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
    async fn list_objects_skips_delete_markers_and_uncommitted_staging() {
        let set = set_with_disks(6, 2).await;
        set.create_bucket("data").await.unwrap();
        set.put_object(put_args("data", "keep", 1_000_000))
            .await
            .unwrap();
        set.put_object(put_args("data", "gone", 1_000_000))
            .await
            .unwrap();
        set.delete_object("data", "gone").await.unwrap();

        // 手工造一个「写完 meta、没提交」的现场：它绝不能被 LIST 当成一个对象。
        for i in 0..6 {
            set.disks()[i]
                .as_ref()
                .unwrap()
                .write_all(
                    "data/ghost/.staging-00000000-0000-0000-0000-000000000002/meta.xl",
                    b"x",
                )
                .await
                .unwrap();
        }

        let entries = set.list_objects("data", None).await.unwrap();
        let keys: Vec<&str> = entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, vec!["keep"], "got {entries:?}");
        assert_eq!(entries[0].size, 1_000_000);
        // etag 必须是 32 位小写十六进制的 MD5（与 PUT 的 PutOut.etag 同源）。
        assert_eq!(entries[0].etag.len(), 32, "etag={}", entries[0].etag);
        assert!(entries[0]
            .etag
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
    }

    /// 前缀过滤 + 嵌套 key。**特别钉住 `a/.rstore/x` 这种深层保留名**：
    /// 它首段是 `a`，是合法 key（5.7 只禁首段），GET 能读到它就说明 LIST 也必须列出来。
    #[tokio::test]
    async fn list_objects_walks_nested_keys_and_filters_prefix() {
        let set = set_with_disks(6, 2).await;
        set.create_bucket("data").await.unwrap();
        for key in ["a", "dir/b", "dir/c/d", "a/.rstore/x"] {
            set.put_object(put_args("data", key, 200_000))
                .await
                .unwrap();
        }

        let all: Vec<String> = set
            .list_objects("data", None)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(
            all,
            vec!["a", "a/.rstore/x", "dir/b", "dir/c/d"],
            "必须按 key 升序"
        );

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
        set.put_object(put_args("data", "k", 1_000_000))
            .await
            .unwrap();

        for i in 0..4 {
            set.inject_fault_on(i, Fault::Offline);
        }
        let r = set.list_objects("data", None).await;
        assert!(
            matches!(r, Err(StoreError::ReadQuorum { .. })),
            "低于 quorum 时必须报错，不能返回残缺列表: {r:?}"
        );
    }
}
