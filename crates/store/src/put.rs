//! PUT 路径（Task 4.5）：把一个对象写进 erasure set，提交前对读路径不可见。
//!
//! 目录布局（M4 起冻结，GET / DELETE / 对账都按它找路）：
//!
//! ```text
//! <bucket>/<key>/<data_dir 的 uuid>/meta.xl        ← 容器，PUT 一次写入
//! <bucket>/<key>/<data_dir 的 uuid>/part.1         ← 该盘全部 block 串成的单个分片文件
//! <bucket>/<key>/.staging-<txid>/…                 ← 写入中的暂存目录
//! ```
//!
//! **暂存目录必须以 `.staging-` 开头**：发现逻辑（Task 4.7）会跳过这个前缀，
//! 保证「能被投票的目录」一定已经提交过。meta 与 part 都先写进暂存目录，
//! 再由 `commit` 一次 rename 提交——「元数据可见」与「分片可见」是同一个原子事件。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rstore_common::consts::should_inline;
use rstore_meta::distribution::{distribution, is_valid_distribution};
use rstore_meta::keys;
use rstore_meta::{
    encode, encode_body, ChecksumAlgo, FileVersionHeader, Flags, InlineData, ObjectBody,
    ObjectMeta, PartInfo, ShallowVersion, StorageClass, VersionType,
};
use uuid::Uuid;

use crate::commit::commit;
use crate::delete::gc_superseded;
use crate::error::StoreError;
use crate::set::ErasureSet;
use crate::writer::BitrotShardWriter;

/// 对象分块大小：1 MiB。写入器与读取器的 `block_size` 由几何算出（见 [`shard_step`]）。
pub const BLOCK_SIZE: usize = 1 << 20;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutArgs {
    pub bucket: String,
    pub key: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutOut {
    pub size: u64,
    pub etag: String,
    /// 本版本的数据目录名，同时是 header 里的 `data_dir`。
    pub data_dir: Uuid,
    pub version_id: Uuid,
}

/// `even_ceil(x) = x + (x & 1)`：向上取到偶数。`Codec::new` 要求 `shard_size`
/// 为正偶数，而 `1 MiB / 3` 这类除不尽的情形不取整就构造不出编解码器。
pub(crate) fn even_ceil(x: u64) -> u64 {
    x + (x & 1)
}

/// 用于 bitrot 分块的分片步长。取 `min(size, BLOCK_SIZE)` 那一档，
/// 于是 `n == 1`（整对象不足一个满块）时它就是这一块自己的分片长度。
///
/// 读侧（Task 4.7）必须能独立复算同一个值，否则按固定步长定位就会整体错位。
pub(crate) fn shard_step(size: u64, data: u8) -> u64 {
    even_ceil(size.min(BLOCK_SIZE as u64).div_ceil(u64::from(data)))
}

/// 每块盘上那份分片的明文总长：除最后一块外全是满块（长度 `shard_step`），
/// 最后一块单独算。由 `(size, data, BLOCK_SIZE)` 三者完全决定。
pub(crate) fn expected_shard_len(size: u64, data: u8) -> u64 {
    if size == 0 {
        return 0;
    }
    let n = size.div_ceil(BLOCK_SIZE as u64);
    let full = shard_step(size, data);
    let last_l = size - (n - 1) * BLOCK_SIZE as u64;
    (n - 1) * full + even_ceil(last_l.div_ceil(u64::from(data)))
}

/// S3 的 etag：真 MD5 的小写十六进制。**不能是自造摘要**——`aws-cli` / `mc` /
/// `rclone` 单部分上传后比对的就是 MD5，换成别的会让它们在 PUT 成功后报校验失败。
pub(crate) fn etag_of(data: &[u8]) -> String {
    use md5::{Digest, Md5};
    format!("{:x}", Md5::digest(data))
}

/// 当前 unix 纳秒。GET 靠它在一个 key 存在多个版本目录时选出最新的那个（Task 4.7）。
/// `pub(crate)`：Task 4.8 的删除标记构造器与写入路径共用同一个时钟。
pub(crate) fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

/// 造一个版本的容器元数据。两处分支（内联 / 分片）共用 header 的骨架，
/// 只有 flags、body 与尺寸字段不同。
fn build_meta(
    size: u64,
    data_shards: u8,
    total: u8,
    data_dir: Uuid,
    version_id: Uuid,
    flags: Flags,
    body: ObjectBody,
) -> Result<ObjectMeta, StoreError> {
    let header = FileVersionHeader {
        version_id: Some(version_id),
        ty: VersionType::Object,
        size,
        // **必须写**：GET 的版本仲裁没有它就无从比较新旧。
        mod_time: Some(now_nanos()),
        ec_m: data_shards,
        ec_n: total,
        flags,
        data_dir: Some(data_dir),
    };
    Ok(ObjectMeta {
        versions: vec![ShallowVersion {
            header,
            body: encode_body(&body)?,
        }],
        inline: InlineData::new(),
        meta_ver: 1,
    })
}

/// 删除标记的元数据：**一个版本**，`ty = DeleteMarker`，`size = 0`，
/// `data_dir = None`，`flags` 为空（不置 `USES_DATA_DIR`，它没有数据目录）。
///
/// 放在 `build_meta` 旁边，是为了让「`mod_time` 必须写、否则 GET 的版本仲裁
/// 没有比较依据」这条不变量留在同一个文件里，不在 `delete.rs` 里重新推一遍。
///
/// 不能复用 `build_meta`：它把 `ty` 硬编码成 `Object`、把 `data_dir` 塞成
/// `Some(..)`，且参数已到 7 个（再加一个就撞 `clippy::too_many_arguments`）。
pub(crate) fn build_delete_meta(version_id: Uuid) -> Result<ObjectMeta, StoreError> {
    let header = FileVersionHeader {
        version_id: Some(version_id),
        ty: VersionType::DeleteMarker,
        size: 0,
        // 同样必须写：`resolve_version` 靠它把这枚标记判成「最新」。
        mod_time: Some(now_nanos()),
        // 删除标记没有分片。这两个字段不参与任何判断——`Resolved::live()`
        // 在解码 body **之前**就返回 `None` 了，分片分支根本走不到。
        // 尤其别填成 `data/total`：那会让一个没有分片的版本看起来像 4+2。
        ec_m: 0,
        ec_n: 0,
        flags: Flags::empty(),
        data_dir: None,
    };
    Ok(ObjectMeta {
        versions: vec![ShallowVersion {
            header,
            body: encode_body(&ObjectBody {
                id: None,
                parts: Vec::new(),
                ec_dist: Vec::new(),
                checksum_algo: ChecksumAlgo::Crc32c,
                storage_class: StorageClass::Standard,
                meta_user: BTreeMap::new(),
                meta_sys: BTreeMap::new(),
            })?,
        }],
        inline: InlineData::new(),
        meta_ver: 1,
    })
}

impl ErasureSet {
    /// 写入一个对象并提交。达到 `write_quorum` 才算成功。
    pub async fn put_object(&self, args: PutArgs) -> Result<PutOut, StoreError> {
        let PutArgs { bucket, key, data } = args;
        let size = data.len() as u64;
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

        let etag = etag_of(&data);

        if should_inline(size, /* versioned_bucket = */ false) {
            // 内联分支：数据进 meta.xl，不产生 part.*。阈值只此一份来源，
            // 绝不在本文件里再写一个常量。
            let mut inline = InlineData::new();
            inline.insert("null", data.clone());
            let mut flags = Flags::empty();
            flags.insert(Flags::INLINE_DATA);
            let body = ObjectBody {
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
            let mut meta = build_meta(size, data_shards, total, data_dir, version_id, flags, body)?;
            meta.inline = inline;
            let bytes = encode(&meta)?;
            self.write_meta_all(&staging, &bytes).await;
        } else {
            self.write_shards(&data, size, &dist, &staging, write_quorum)
                .await?;

            let shard_len = expected_shard_len(size, data_shards);
            let mut flags = Flags::empty();
            // 本版本确实用了数据目录（目录名就是 data_dir）。
            flags.insert(Flags::USES_DATA_DIR);
            let body = ObjectBody {
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
            let meta = build_meta(size, data_shards, total, data_dir, version_id, flags, body)?;
            let bytes = encode(&meta)?;
            self.write_meta_all(&staging, &bytes).await;
        }

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

    /// 把 meta.xl 写进每块可用盘的暂存目录。逐盘失败只忽略：最终的 quorum
    /// 由 `commit` 的 rename 判定，写不进去的盘自然拿不到票。
    pub(crate) async fn write_meta_all(&self, staging: &str, bytes: &[u8]) {
        let rel = format!("{staging}/meta.xl");
        for disk in self.disks().iter().flatten() {
            let _ = disk.write_all(&rel, bytes).await;
        }
    }

    /// 大对象分支：按几何把对象切成 `data` 个分片、编码出 `parity` 个校验分片，
    /// 按 `dist` 派到各盘的单个 `part.1` 写入器，最后统一 `finish`。
    ///
    /// 低于 quorum 直接返回错误，**且不清理已写的分片**：残留由对账流程回收
    /// （DESIGN §12.2）。主动清理反而会把崩溃残留抹掉，让对账的可回收性无处可测。
    async fn write_shards(
        &self,
        data: &[u8],
        size: u64,
        dist: &[u8],
        staging: &str,
        write_quorum: u8,
    ) -> Result<(), StoreError> {
        let data_shards = self.data();
        let parity = self.parity();
        let step = shard_step(size, data_shards) as usize;
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

        let n = size.div_ceil(BLOCK_SIZE as u64);
        for k in 0..n {
            let start = (k * BLOCK_SIZE as u64) as usize;
            let end = ((k + 1) * BLOCK_SIZE as u64).min(size) as usize;
            let block = &data[start..end];
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
                if let Some(w) = writers[physical].as_mut() {
                    w.push_block(shard)?;
                }
            }
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
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rstore_disk::Fault;
    use uuid::Uuid;

    use super::*;
    use crate::error::StoreError;
    use crate::testutil::{set_with_disks, TestSet};

    /// 某块盘上 `bucket/key/<data_dir>` 里的文件名（已排序）。
    /// 走 `DiskAPI::list_dir` 而不是直接碰文件系统：夹具里的盘可能被 `FaultyDisk`
    /// 包过，`list_dir` 才是被测试的那条路径。
    async fn list_data_dir(
        set: &TestSet,
        disk_idx: usize,
        key_rel: &str,
        data_dir: &Uuid,
    ) -> Vec<String> {
        let d = set.disks()[disk_idx].as_ref().expect("该盘应当在线");
        d.list_dir(&format!("{key_rel}/{data_dir}")).await.unwrap()
    }

    #[tokio::test]
    async fn put_small_object_inlines_it() {
        let set = set_with_disks(6, 2).await;
        let data = vec![1u8; 1000];
        let out = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "small".into(),
                data: data.clone(),
            })
            .await
            .unwrap();

        assert!(!out.etag.is_empty());
        assert_eq!(out.size, 1000);
        // 内联对象的数据在 meta.xl 里，**不该有分片文件**。只断言 etag/size
        // 是不够的——那样「悄悄走了大对象路径」的实现照样能过。
        for i in 0..6 {
            let files = list_data_dir(&set, i, "b/small", &out.data_dir).await;
            assert_eq!(files, vec!["meta.xl".to_string()], "disk {i}");
        }

        // 「内联了」不等于「内联对了」：把 meta.xl 读回来解一遍。
        // 本任务还不能用 GET（那是 Task 4.7），所以直接走 meta 容器的解码。
        let d = set.disks()[0].as_ref().unwrap();
        let raw = d
            .read_exact_at(
                &format!("b/small/{}/meta.xl", out.data_dir),
                0,
                d.stat(&format!("b/small/{}/meta.xl", out.data_dir))
                    .await
                    .unwrap()
                    .unwrap()
                    .size as usize,
            )
            .await
            .unwrap();
        let meta = rstore_meta::decode(&raw).unwrap();
        // 无版本化时版本键是 "null"（DESIGN §8.4）。
        assert_eq!(meta.inline.get("null"), Some(&data[..]));
    }

    #[tokio::test]
    async fn put_large_object_creates_shards() {
        let set = set_with_disks(6, 2).await;
        let data = vec![7u8; 1_500_000]; // 2 个 block：1 MiB + 451 424
        let out = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "big".into(),
                data: data.clone(),
            })
            .await
            .unwrap();
        assert_eq!(out.size, 1_500_000);

        // 单部分对象：每块盘的数据目录里恰好一个分片文件 part.1。
        // 原计划这里写「不是 6 个！」，是因为当时没定清楚 part.N 的 N 指什么——
        // N 是**部分号**（multipart 的 part），不是盘号；MVP 只有一部分。
        for i in 0..6 {
            let files = list_data_dir(&set, i, "b/big", &out.data_dir).await;
            assert_eq!(
                files,
                vec!["meta.xl".to_string(), "part.1".to_string()],
                "disk {i}"
            );
        }

        // 盘上字节数必须等于读侧能独立复算出来的那个数。写侧算错几何的话，
        // GET 会以 Transient(ShortRead) 或 Corrupt 收场，而那时错误现场已经离原因很远了。
        let step = shard_step(out.size, 4);
        let shard_len = expected_shard_len(out.size, 4);
        let expect_on_disk = rstore_checksum::bitrot_size(shard_len, step);
        for i in 0..6 {
            let d = set.disks()[i].as_ref().unwrap();
            let st = d
                .stat(&format!("b/big/{}/part.1", out.data_dir))
                .await
                .unwrap()
                .expect("part.1 必须存在");
            assert_eq!(st.size, expect_on_disk, "disk {i}");
        }
    }

    /// etag 是给 S3 客户端比对的，必须是真 MD5。这条用已知向量钉死，
    /// 免得后来有人「优化」成自造摘要——那会让客户端在 PUT 成功后报校验失败。
    #[test]
    fn etag_is_lowercase_hex_md5() {
        assert_eq!(etag_of(b"hello"), "5d41402abc4b2a76b9719d911017c592");
        assert_eq!(etag_of(b""), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[tokio::test]
    async fn put_fails_below_write_quorum() {
        let set = set_with_disks(6, 2).await;
        for i in 0..3 {
            set.inject_fault_on(i, Fault::Offline);
        }
        let r = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                data: vec![0u8; 1_000_000],
            })
            .await;
        // `WriteQuorum` 是 struct 变体，`matches!` 里必须带 `{ .. }`（原计划漏了，
        // 那样写根本编译不过）。
        assert!(
            matches!(r, Err(StoreError::WriteQuorum { .. })),
            "got {r:?}"
        );

        // 越界守卫：低于 quorum 时**一块盘都不该留下可见的最终目录**，即
        // `b/k/<data_dir>`。规格原文这里查的是 `list_dir("b")` 并断言为空——
        // 但那必然包含 key 目录 `k`（暂存目录就挂在它下面），任何在 bucket 下写
        // 过东西的实现都过不了；而且它与 Step 6 / DESIGN §12.2「失败可留残留、
        // 由对账回收」直接冲突（本实现确实是刻意不清理的）。
        // 因此把断言收敛到真正的意图：`b/k` 下除了 `.staging-*` 暂存目录之外，
        // 不许出现已提交的最终版本目录。
        for i in 0..6 {
            let d = set.disks()[i].as_ref().unwrap();
            let entries = if d.stat("b/k").await.ok().flatten().is_some() {
                d.list_dir("b/k").await.unwrap_or_default()
            } else {
                Vec::new()
            };
            assert!(
                entries.iter().all(|e| e.starts_with(".staging-")),
                "disk {i} 上留下了可见的最终目录: {entries:?}"
            );
        }
    }
}
