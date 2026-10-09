//! 元数据仲裁：把多块盘各自返回的 [`ObjectMeta`] 做多数决。
//!
//! ## 身份判定：直接比元数据的线格式字节
//!
//! [`resolve_metadata`] 要回答的是「这几块盘上的元数据是不是同一份」。判定方式就是
//! `rstore_meta::encode(meta)` 的结果做 key —— 容器编码本来就是确定性的（固定数组 +
//! 有序 map），两份元数据一字不差时编码必然逐字节相同；只要有一个字段不同，编码就不同。
//! 这条路上没有额外的摘要（不需要先把字段挑出来再拼一遍），也就不会因为「挑漏了一个
//! 字段」而把两份不同的元数据误判成同一份。
//!
//! ## 规则：`ObjectBody.meta_sys` 里不得放逐盘可变的字段
//!
//! 身份是**整份字节相等**，因此任何一块盘上多出一个字节的差异都会被算成「另一种元数据」，
//! 进而把一次本来健康的读打成 [`crate::error::StoreError::ReadQuorum`]。
//! Phase 4 的 heal / purge 状态因此**不能**写进 `meta_sys`，要走 sidecar 文件。
//! 这条规则有测试守着（`tests::meta_sys_differences_split_the_vote`），不是口头约定。

use std::collections::HashMap;

use rstore_meta::ObjectMeta;

use crate::error::StoreError;

/// 在 `metas` 上做多数决。`metas` 的长度即盘数 `total`；
/// `None` 表示该盘**没有返回**（既不计票，也不计为失败——它与「返回了一份不匹配的元数据」
/// 是完全不同的两件事，而后者要计进各自的组）。
///
/// `read_quorum = metas.len() - parity`。票数最高的组达不到它就返回 `ReadQuorum`。
/// 注意这里**不存在「多数决就返回第一名」的兜底**：3 票对 2 票对 1 票时第一名只有 3 票，
/// 低于 4 就得以错误收场，不能因为「它最多」就把它扶正——那是把一次不确定的读
/// 伪装成确定的结果。
pub fn resolve_metadata(
    metas: &[Option<ObjectMeta>],
    parity: u8,
) -> Result<ObjectMeta, StoreError> {
    // 输入本来就是一块内存里的 `&[Option<ObjectMeta>]`（所有盘都已经并行读完并反序列化完
    // 了），做逐盘早停省不掉任何 IO；盘数 ≤ 16 的规模下，位图之类省下的那点分配也测不
    // 出来。所以这里就用最直白的 `HashMap<Vec<u8>, …>`，不做早停优化。
    //
    // key = 容器编码（线格式字节），value = (该组的票数, 该组的代表元数据)。
    let mut groups: HashMap<Vec<u8>, (u8, &ObjectMeta)> = HashMap::new();
    for meta in metas.iter().flatten() {
        let bytes = match rstore_meta::encode(meta) {
            Ok(bytes) => bytes,
            Err(err) => {
                // 编码失败 → 这一票作废，按「未返回」处理（不计票，也不计进任何「不匹配」
                // 的组），并记一笔。
                //
                // 为什么不按「不匹配」处理：编码失败说明的是**本进程无法给出该盘元数据的
                // 线格式表示**，是我们这一侧的能力问题，而不是盘上那份元数据与别人不同。
                // 若把它当成一个「不匹配」的组，就等于凭空造出一个只含这一盘的假少数派：
                // 它既不代表盘上真实内容，还会稀释真正多数派的票数，可能把一次健康的读
                // 推向 ReadQuorum。按「未返回」处理则退回到「这盘没参与投票」，与 `None`
                // 同义——盘数里扣掉它，但不假装它投了一张反对票。
                tracing::warn!(error = %err, "metadata encode failed; discarding this disk's vote");
                continue;
            }
        };
        match groups.get_mut(&bytes) {
            Some((count, _)) => *count += 1,
            None => {
                groups.insert(bytes, (1, meta));
            }
        }
    }

    let total = metas.len() as u8;
    let read_quorum = total.saturating_sub(parity);

    let best = groups
        .values()
        .map(|(count, meta)| (*count, *meta))
        .max_by_key(|(count, _)| *count);

    match best {
        // 票数最高的组达到 read_quorum 才算赢。
        Some((count, meta)) if count >= read_quorum => Ok(meta.clone()),
        // `achieved` 填**获胜组的票数**（不是「返回了元数据的盘数」）：读失败时运维要看
        // 到的是「最强的那份共识有多强」，不是「有几块盘活着」。
        Some((count, _)) => Err(StoreError::ReadQuorum {
            achieved: count,
            required: read_quorum,
        }),
        // 没有任何盘返回可用的元数据。
        None => Err(StoreError::ReadQuorum {
            achieved: 0,
            required: read_quorum,
        }),
    }
}

#[cfg(test)]
mod tests {
    // 这些名字都在 `rstore_meta` 的 crate 根上（lib.rs 有 `pub use fileinfo::{…}`），
    // 不用写 `fileinfo::` 前缀。
    use rstore_meta::{
        ChecksumAlgo, FileVersionHeader, Flags, ObjectBody, ObjectMeta, ShallowVersion,
        StorageClass, VersionType,
    };

    use super::*;
    use crate::error::StoreError;

    /// 造一份确定的元数据。`tag` 只改 `meta_user` 里一个键——
    /// 这是「两个不同版本」的最小可分辨差异。
    fn meta_with_tag(tag: &str) -> ObjectMeta {
        let header = FileVersionHeader {
            size: 1000,
            ec_m: 4,
            ec_n: 6,
            flags: Flags::USES_DATA_DIR,
            ..Default::default()
        };
        let body = ObjectBody {
            id: None,
            parts: Vec::new(),
            ec_dist: vec![1, 2, 3, 4, 5, 6],
            checksum_algo: ChecksumAlgo::Crc32c,
            storage_class: StorageClass::Standard,
            meta_user: [("tag".to_string(), tag.to_string())].into_iter().collect(),
            meta_sys: Default::default(),
        };
        let body_bytes = rstore_meta::encode_body(&body).unwrap();
        ObjectMeta {
            versions: vec![ShallowVersion {
                header,
                body: body_bytes,
            }],
            inline: Default::default(),
            meta_ver: 1,
        }
    }

    fn some(metas: Vec<ObjectMeta>) -> Vec<Option<ObjectMeta>> {
        metas.into_iter().map(Some).collect()
    }

    #[test]
    fn identical_metadata_wins_quorum() {
        // total=6, parity=2 → read_quorum=4。4 票 a 达到 quorum。
        let metas = vec![
            meta_with_tag("a"),
            meta_with_tag("a"),
            meta_with_tag("a"),
            meta_with_tag("a"),
            meta_with_tag("b"),
            meta_with_tag("b"),
        ];
        let r = resolve_metadata(&some(metas), 2).unwrap();
        assert_eq!(r, meta_with_tag("a"));
    }

    #[test]
    fn minority_metadata_cannot_win() {
        // 3 票 < read_quorum 4 → 必须报错，不能「多数决」直接返回少数派。
        let metas = vec![
            meta_with_tag("a"),
            meta_with_tag("a"),
            meta_with_tag("a"),
            meta_with_tag("b"),
            meta_with_tag("c"),
            meta_with_tag("b"),
        ];
        // struct 变体在 `matches!` 里必须带 `{ .. }`（原计划漏了，那样编译不过）。
        assert!(matches!(
            resolve_metadata(&some(metas), 2),
            Err(StoreError::ReadQuorum {
                achieved: 3,
                required: 4
            })
        ));
    }

    #[test]
    fn no_quorum_is_an_error() {
        let metas = some(vec![
            meta_with_tag("a"),
            meta_with_tag("b"),
            meta_with_tag("c"),
        ]);
        // total=3, parity=1 → read_quorum = 3 - 1 = 2，三组各 1 票，谁都不够。
        assert!(matches!(
            resolve_metadata(&metas, 1),
            Err(StoreError::ReadQuorum {
                achieved: 1,
                required: 2
            })
        ));
    }

    #[test]
    fn missing_disks_are_not_failures() {
        // 2 块盘没返回、4 块一致 → 成功。`None` 不是失败，也不占票。
        let metas = vec![
            Some(meta_with_tag("a")),
            None,
            Some(meta_with_tag("a")),
            None,
            Some(meta_with_tag("a")),
            Some(meta_with_tag("a")),
        ];
        assert!(resolve_metadata(&metas, 2).is_ok());
    }

    /// 这条守的是「`meta_sys` 不得放逐盘可变字段」这条规则。它断言的**正是**
    /// 「差异会拆票」这个看起来不友好的行为：等到 Phase 4 有人往 `meta_sys` 里塞
    /// heal 状态时，这里会先红一次，逼他去看 `quorum.rs` 顶上那段说明。
    #[test]
    fn meta_sys_differences_split_the_vote() {
        let mut healing = meta_with_tag("a");
        let mut body = rstore_meta::decode_body(&healing.versions[0].body).unwrap();
        body.meta_sys
            .insert("x-rs-healing".into(), b"true".to_vec());
        healing.versions[0].body = rstore_meta::encode_body(&body).unwrap();

        let metas = vec![
            meta_with_tag("a"),
            meta_with_tag("a"),
            meta_with_tag("a"),
            healing,
            meta_with_tag("a"),
            meta_with_tag("a"),
        ];
        // 5 票对 1 票，仍然达到 quorum=4——单个盘的差异不会**立刻**打垮读。
        assert!(resolve_metadata(&some(metas), 2).is_ok());

        // 但只要差异达到 parity+1 块盘，读就失败。这就是为什么逐盘可变字段不能进 meta_sys。
        let mut all_diff = Vec::new();
        for _ in 0..3 {
            all_diff.push(meta_with_tag("a"));
        }
        for _ in 0..3 {
            let mut m = meta_with_tag("a");
            let mut b = rstore_meta::decode_body(&m.versions[0].body).unwrap();
            b.meta_sys.insert("x-rs-healing".into(), b"true".to_vec());
            m.versions[0].body = rstore_meta::encode_body(&b).unwrap();
            all_diff.push(m);
        }
        assert!(matches!(
            resolve_metadata(&some(all_diff), 2),
            Err(StoreError::ReadQuorum { .. })
        ));
    }

    /// 身份判定的**全部依据**是「容器编码逐字节相等」。这条钉住编码是确定性的：
    /// 同一份内存元数据编两次必须一模一样。若哪天有人给容器编码引入时间戳、
    /// 随机顺序的 map 或指针地址，这里会红——而那时所有读都会开始报 ReadQuorum。
    #[test]
    fn identity_is_the_wire_encoding_and_is_deterministic() {
        let a = meta_with_tag("a");
        let b = meta_with_tag("a");
        assert_eq!(
            rstore_meta::encode(&a).unwrap(),
            rstore_meta::encode(&b).unwrap()
        );
        assert_eq!(
            rstore_meta::encode(&a).unwrap(),
            rstore_meta::encode(&a).unwrap()
        );
    }

    /// `VersionType::DeleteMarker` 必须能被编码进容器——DELETE（Task 4.8）靠它。
    /// 顺带钉住「删除标记也是一份可投票的元数据」。
    #[test]
    fn delete_marker_metadata_round_trips() {
        let mut m = meta_with_tag("a");
        m.versions[0].header.ty = VersionType::DeleteMarker;
        m.versions[0].header.size = 0;
        let bytes = rstore_meta::encode(&m).unwrap();
        assert_eq!(rstore_meta::decode(&bytes).unwrap(), m);
    }
}
