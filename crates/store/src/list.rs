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

/// `dir_rel` 这棵子树里的**所有**候选 key 是否都 `<= after`——是则可以整块剪掉。
///
/// 子树里的 key 只有两种形状：`dir_rel` 本身，或 `dir_rel/<...>`。两者都以
/// `dir_rel` 开头，紧随其后的字节要么不存在，要么是 `/`（0x2f）——都小于
/// `U+10FFFF` 的首字节 0xF4。所以 `dir_rel + '\u{10FFFF}'` 是这棵子树的一个
/// **严格上界**，拿它跟游标比是保守的：只有确实整棵都在游标之前才剪。
///
/// **两个看似可行、其实都是错的做法**，写在这里免得后来者「顺手优化」回去：
///
/// - 「`dir_rel <= after` 就剪」：`"dir" < "dir/b"`，但 `"dir/c"` 也排在
///   `"dir/b"` 之后——按目录路径比会把它一起剪掉。
/// - 「拿下一个兄弟目录名当上界」：`'/'`(0x2f) 比 `'-'`(0x2d)、`'.'`(0x2e) 都大，
///   于是 `"p-x" < "p/a"`；兄弟名不构成上界。
fn subtree_at_or_before(dir_rel: &str, after: &str) -> bool {
    let mut bound = String::with_capacity(dir_rel.len() + 4);
    bound.push_str(dir_rel);
    bound.push('\u{10FFFF}');
    bound.as_str() <= after
}

impl ErasureSet {
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
    /// **契约：`entries` 为空时 `more` 必然为 `false`**，否则调用方会空转。
    /// `want == 0` 是这条契约的退化边界，直接按空页处理：调用方（S3 层）用
    /// `max_keys.max(1)` 保证不会走到这里，两条模式在这个边界上也保持一致。
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
        if want == 0 {
            return Ok((Vec::new(), false));
        }

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

    /// 按 key 升序、**可提前停**的有界遍历。返回 `(候选 key, 是否还有更多)`。
    ///
    /// **为什么不能用「先全收进 `BTreeSet` 再 sort」**（[`Self::candidate_keys`] 的做法）：
    /// 那样无法提前停，10 万个 key 的桶要全走完才能返回第一页。这里改成有序 DFS，
    /// 收够 `want` 个候选之后立刻收手。
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
    /// **提前停与 `more` 的准确性**：发现第 `want + 1` 个候选时才报 `more = true`。
    /// 多走这一步是为了把 `more` 定准——谎报 `true` 会让调用方再发一次请求，
    /// 而在「整棵子树都要重走」的老实现上那是一次全量遍历。`want` 是**候选**数的
    /// 上界，不是返回条目数的上界（候选是否活着由 `resolve_version` 判）。
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
            let at_root = dir_rel.is_empty();
            // 整棵子树都在游标之前 → 整块剪掉，一次 `list_dir` 都不发。
            if let Some(a) = after {
                if !at_root && subtree_at_or_before(&dir_rel, a) {
                    continue;
                }
            }

            // `entries_under` 返回的是 BTreeSet 派生出的升序 `Vec`，直接可用。
            let entries = self.entries_under(&join_rel(bucket, &dir_rel)).await;
            let mut is_key = false;
            let mut child_dirs: Vec<String> = Vec::new();
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
                if !self.any_disk_is_dir(&join_rel(bucket, &child_rel)).await {
                    continue;
                }
                // 子目录里直接躺着 `meta.xl` → 它是**版本目录**，说明父目录
                // `dir_rel` 本身就是一个 key；版本目录不再往下走。
                let sub = self.entries_under(&join_rel(bucket, &child_rel)).await;
                if sub.iter().any(|e| e == "meta.xl") {
                    is_key = true;
                } else if after.is_none_or(|a| !subtree_at_or_before(&child_rel, a)) {
                    // 整棵子树都在游标之前就不压栈——剪枝发生在这一层，所以下面的
                    // `is_key` 判定不受影响：它只依赖「子目录里有没有 meta.xl」。
                    child_dirs.push(child_rel);
                }
            }

            // **先输出自己，再下探**——`p` 必须排在 `p/a` 之前。
            if is_key && !at_root && after.is_none_or(|a| dir_rel.as_str() > a) {
                if keys.len() >= want {
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
    use rstore_common::modes::IoModes;
    use rstore_disk::faulty::Fault;

    use super::*;
    use crate::put::PutArgs;
    use crate::testutil::{body, set_with_disks, set_with_modes};

    fn put_args(bucket: &str, key: &str, n: usize) -> PutArgs {
        PutArgs {
            bucket: bucket.into(),
            key: key.into(),
            body: body(vec![7u8; n]),
            etag: None,
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
                set.put_object(put_args("data", key, 200_000))
                    .await
                    .unwrap();
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
        assert_eq!(
            paged, full,
            "增量遍历必须与全量遍历给出同一串 key、同一个顺序"
        );
    }

    /// **旧模式走 `list_objects_from` 也必须给出正确结果**——它在模式关着时退化成
    /// 「全量遍历 + 内存过滤」，这条钉住那条退化路径没写错。
    #[tokio::test]
    async fn list_objects_from_works_in_old_mode_too() {
        let set = set_with_modes(6, 2, IoModes::default()).await;
        set.create_bucket("data").await.unwrap();
        for key in ["a", "b", "c"] {
            set.put_object(put_args("data", key, 200_000))
                .await
                .unwrap();
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
                set.put_object(put_args("data", key, 200_000))
                    .await
                    .unwrap();
            }
            let (page, _more) = set
                .list_objects_from("data", Some("dir/"), None, 10)
                .await
                .unwrap();
            let got: Vec<&str> = page.iter().map(|e| e.key.as_str()).collect();
            assert_eq!(got, vec!["dir/b", "dir/c"], "modes={modes:?}");
        }
    }
}
