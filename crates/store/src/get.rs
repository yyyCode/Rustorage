//! GET 路径（Task 4.7）：版本发现 → 元数据仲裁 → 内联 / 分片读取 → Range 裁剪。

use std::collections::BTreeSet;
use std::sync::Arc;

use rstore_disk::DiskAPI;
use rstore_meta::keys;
use rstore_meta::{
    decode_body, FileVersionHeader, Flags, ObjectBody, ObjectMeta, ShallowVersion, VersionType,
};
use uuid::Uuid;

use crate::error::StoreError;
use crate::put::{etag_of, expected_shard_len, shard_step, BLOCK_SIZE};
use crate::reader::BitrotShardReader;
use crate::set::ErasureSet;

/// 闭区间 `[start, end]`。M5 的 S3 层负责把 `bytes=a-b` / `bytes=a-` / `bytes=-n`
/// 三种写法解析并裁剪到这个形状，store 层只认闭区间。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GetOut {
    /// **请求范围内**的字节。不是整个对象。
    pub data: Vec<u8>,
    /// 整个对象的原始长度（不是 `data.len()`）——S3 的 `Content-Range` 要它。
    pub size: u64,
    pub etag: String,
    pub data_dir: Uuid,
    /// 最新版本的 `mod_time`（Unix 纳秒）；缺省按 0。S3 的 `Last-Modified` 要它。
    pub mod_time: u64,
}

/// HEAD 的输出：只走元数据、一个分片都不碰。S3 的 HEAD（`mc stat` / `rclone check`）
/// 不该为拿 size/etag/mod_time 付一次全量读。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeadOut {
    pub size: u64,
    pub etag: String,
    pub mod_time: u64,
}

/// 一个 `ObjectMeta` 里「最新」的那个版本：先比 `mod_time`，相同再比 `version_id`。
/// PUT 每个版本都会写 `mod_time`（纳秒），版本仲裁完全依赖它。
///
/// `pub(crate)`：`Resolved::live()` 用它判「最新版本是不是删除标记」，LIST（Task 4.11）
/// 也要从权威元数据里取最新版本的 `size` / `mod_time`——「哪个版本最新」只能有一处答案。
pub(crate) fn latest_version(meta: &ObjectMeta) -> Option<&ShallowVersion> {
    meta.versions
        .iter()
        .max_by_key(|v| (v.header.mod_time.unwrap_or(0), v.header.version_id))
}

/// 取一个权威版本（`meta`）的 etag。
///
/// **etag 只能有一处算法**：GET（`get_object`）与 LIST（`ObjectEntry`）都走这里，
/// 否则「HEAD 的 ETag 与 LIST 的不一样」会让 `rclone check` 这类客户端报校验失败。
///
/// 分片对象直接读 `parts[0].etag`；内联对象现算 `etag_of(inline)`——内联对象的 etag
/// **没落进 meta**（Task 4.5 的内联分支 `parts` 为空），只能从 `meta.inline` 现算。
///
/// `parts` 为空且不是内联 → `Internal`：元数据自相矛盾，报错比猜一个 etag 强。
/// （这不影响 GET 的正常路径：内联对象走内联分支，分片对象必有 `parts`。）
pub(crate) fn etag_of_meta(meta: &ObjectMeta) -> Result<String, StoreError> {
    let latest = latest_version(meta)
        .ok_or_else(|| StoreError::Internal("metadata has no versions".into()))?;
    let body = decode_body(&latest.body)?;
    let is_inline = latest.header.flags.contains(Flags::INLINE_DATA)
        || body.meta_sys.contains_key(keys::INLINE_DATA);
    if is_inline {
        let full = meta.inline.get("null").ok_or_else(|| {
            StoreError::Internal("version marked inline but holds no inline data".into())
        })?;
        Ok(etag_of(full))
    } else {
        match body.parts.first() {
            Some(p) => Ok(p.etag.clone()),
            None => Err(StoreError::Internal(
                "version has neither inline data nor parts".into(),
            )),
        }
    }
}

/// 读一块盘上的 `meta.xl`：不存在、读失败或解码失败都返回 `None`。
///
/// 这三种情况在仲裁里同义（该盘对这个候选目录投「未返回」，既不计票也不计为反对票）。
/// 返回 `Option` 而不是 `Result`，是为了让调用方不可能把「某块盘读不到」误当成
/// 「整次 GET 失败」——只有所有候选都过不了 quorum 才是 `ReadQuorum`。
async fn read_meta(disk: &dyn DiskAPI, rel: &str) -> Option<ObjectMeta> {
    let st = match disk.stat(rel).await {
        Ok(Some(st)) if !st.is_dir => st,
        _ => return None,
    };
    let bytes = match disk.read_exact_at(rel, 0, st.size as usize).await {
        Ok(b) => b,
        Err(_) => return None,
    };
    rstore_meta::decode(&bytes).ok()
}

/// `resolve_version` 的结果。**三态，不是一个 `Option`**。
///
/// 之所以要把「权威目录名」带出来而不是把「最新版本是删除标记」直接压成 `NotFound`：
/// 对账（4.10）要问的不是「有没有对象」，而是「**哪个目录是权威的、绝对不能删**」。
/// 删除标记**就是**一个权威目录——DELETE 之后没拿到标记的盘还留着旧数据目录，
/// 正是靠标记目录压住它们才读不出旧版本。若这里返回「什么都没有」，对账就会把标记
/// 目录也当垃圾删掉，旧版本随即在那些盘上复活。详见 `docs/MVP.md` Task 4.7。
pub(crate) enum Resolved {
    /// 权威版本目录 + 它的元数据。**可能是删除标记**——用 [`Resolved::live`] 判要不要读。
    Version { dir: String, meta: ObjectMeta },
    /// 所有盘上都没有这个 key 的任何候选目录（从来没写过，或已被对账清空）。
    Absent,
}

impl Resolved {
    /// 有对象可读时返回 `(权威目录名, 元数据)`；`Absent` 与「最新版本是删除标记」
    /// 都返回 `None`。**「能不能读」只有这一处答案**：4.8 的 GC 与 4.10 的对账都走它。
    pub(crate) fn live(&self) -> Option<(&str, &ObjectMeta)> {
        match self {
            Resolved::Version { dir, meta } => {
                let latest = latest_version(meta)?;
                if latest.header.ty == VersionType::DeleteMarker {
                    None
                } else {
                    Some((dir.as_str(), meta))
                }
            }
            Resolved::Absent => None,
        }
    }
}

/// 找到 `bucket/key` 当前权威的版本目录及其实元数据。
///
/// `Ok(Absent)` = 所有盘上都没有这个 key 的任何候选目录；
/// `Err(ReadQuorum)` = 有候选目录但一个都过不了 quorum。
///
/// **发现/仲裁/选出胜出目录都收在这里**：4.8 的 GC 与 4.10 的对账要问同一个问题
/// 「现在哪个目录是权威的」。注意这里**不**把删除标记压成 `Absent`——那是 `live()`
/// 的事（见 [`Resolved`]）。
pub(crate) async fn resolve_version(
    set: &ErasureSet,
    bucket: &str,
    key: &str,
) -> Result<Resolved, StoreError> {
    let key_rel = format!("{bucket}/{key}");
    let total = set.total() as usize;

    // 1. 发现候选版本目录：各在线盘 `list_dir("{bucket}/{key}")` 的目录名并集。
    //    `NotFound`（这块盘压根没有这个对象）按空列表处理；其余错误也只是
    //    「这块盘没贡献候选」，不冒泡成整次 GET 的失败。
    //    **跳过 `.staging-` 前缀**：那些是写到一半、还没提交的目录；不跳的话
    //    一个「6 块盘都写完暂存 meta、没来得及提交」的现场会拿到 6 票并胜出，
    //    于是把半成品当成正式版本读出来。
    let mut candidates: BTreeSet<String> = BTreeSet::new();
    for slot in set.disks() {
        let Some(disk) = slot else { continue };
        if let Ok(entries) = disk.list_dir(&key_rel).await {
            for entry in entries {
                if !entry.starts_with(".staging-") {
                    candidates.insert(entry);
                }
            }
        }
    }
    if candidates.is_empty() {
        // 「有没有权威目录」与「这个目录算不算有对象」是两个正交的问题：
        // 这里只回答前者；后者由 `live()` 统一判。
        return Ok(Resolved::Absent);
    }

    // 3. 对每个候选目录做元数据仲裁：能过 quorum 的才算「成立」。
    let parity = set.parity();
    let mut established: Vec<(String, ObjectMeta)> = Vec::new();
    // 一个都不成立时，报告「最强的那份共识有多强」，而不是「有几块盘活着」。
    let mut best_achieved: Option<(u8, u8)> = None;
    for cand in candidates {
        let mut metas: Vec<Option<ObjectMeta>> = Vec::with_capacity(total);
        for slot in set.disks() {
            let meta = match slot {
                Some(disk) => read_meta(disk.as_ref(), &format!("{key_rel}/{cand}/meta.xl")).await,
                None => None,
            };
            metas.push(meta);
        }
        match crate::quorum::resolve_metadata(&metas, parity) {
            Ok(meta) if !meta.versions.is_empty() => established.push((cand, meta)),
            // 空版本的元数据无法判定新旧，不纳入候选（也不当成失败）。
            Ok(_) => {}
            Err(StoreError::ReadQuorum { achieved, required }) => {
                if best_achieved.is_none_or(|(a, _)| achieved > a) {
                    best_achieved = Some((achieved, required));
                }
            }
            Err(_) => {}
        }
    }

    // 4. 成立的候选可能有多个（覆盖写之后旧目录还没被 GC 掉，或 GC 中途崩溃）。
    //    取**最新版本 `mod_time` 最大**的那个；相同（理论上不会）时取 `version_id` 大的。
    //    一个都不成立 → `ReadQuorum`。
    let winner = established.into_iter().max_by_key(|(_, meta)| {
        latest_version(meta)
            .map(|v| (v.header.mod_time.unwrap_or(0), v.header.version_id))
            .unwrap_or((0, None))
    });
    let Some((winner_dir, winner_meta)) = winner else {
        let (achieved, required) = best_achieved.unwrap_or((0, set.read_quorum()));
        return Err(StoreError::ReadQuorum { achieved, required });
    };

    // 5. 胜出元数据可能是删除标记——**不在这里改变返回值**，仍把那个目录作为权威
    //    目录返回，由 `live()` 统一判「算不算有对象」。
    Ok(Resolved::Version {
        dir: winner_dir,
        meta: winner_meta,
    })
}

/// [`resolve_version`] 的缓存版，**只给 `get_object` / `head_object` 用**。
///
/// 模式关着的时候它一次都不碰缓存，直接走原路径——所以调用方不必自己判断模式，
/// **决策点就在这个 `let Some(..) else` 上，只有一处**。
///
/// **错误不进缓存**：`ReadQuorum` 是暂时状态，把它记下来只会让一次抖动变成持续失败。
/// `Absent` **进缓存**——「这个 key 不存在」正是 HEAD 密集负载里最有价值的一类命中，
/// 而它被写入改变时由 `invalidate` 兜住。
pub(crate) async fn resolve_version_cached(
    set: &ErasureSet,
    bucket: &str,
    key: &str,
) -> Result<Arc<Resolved>, StoreError> {
    let Some(cache) = set.resolve_cache() else {
        return Ok(Arc::new(resolve_version(set, bucket, key).await?));
    };
    let gen = cache.generation(bucket, key);
    if let Some(hit) = cache.get(bucket, key, gen) {
        return Ok(hit);
    }
    let fresh = Arc::new(resolve_version(set, bucket, key).await?);
    cache.store(bucket, key, gen, Arc::clone(&fresh));
    Ok(fresh)
}

/// Range 的边界检查与裁剪。**走到这里还越界就是 M5 的 bug**，不该被 store 悄悄吸收。
fn apply_range(full: Vec<u8>, size: u64, range: Option<ByteRange>) -> Result<Vec<u8>, StoreError> {
    match range {
        None => Ok(full),
        Some(ByteRange { start, end }) => {
            if start > end || end >= size {
                return Err(StoreError::Internal(format!(
                    "range {start}-{end} out of bounds for size {size}"
                )));
            }
            let lo = start as usize;
            let hi = end as usize + 1;
            if hi > full.len() {
                return Err(StoreError::Internal(format!(
                    "range {start}-{end} exceeds materialized length {}",
                    full.len()
                )));
            }
            Ok(full[lo..hi].to_vec())
        }
    }
}

/// 区间读产物的裁剪：`buf` 是从第 `first_block` 块的头开始的一段，
/// 按 `Range` 把两端修掉。
///
/// 与 [`apply_range`] 的区别只有基准偏移：后者的 `buf` 从对象头开始。
/// 两者一样，**走到这里还越界就是 M5 的 bug**，不静默吸收。
fn slice_blocks(
    buf: Vec<u8>,
    size: u64,
    range: Option<ByteRange>,
    first_block: usize,
) -> Result<Vec<u8>, StoreError> {
    let Some(ByteRange { start, end }) = range else {
        return Err(StoreError::Internal(
            "slice_blocks called without a range".into(),
        ));
    };
    if start > end || end >= size {
        return Err(StoreError::Internal(format!(
            "range {start}-{end} out of bounds for size {size}"
        )));
    }
    // `first_block = start / BLOCK_SIZE`（由调用方保证），所以 `start >= base` 恒成立。
    let base = first_block as u64 * BLOCK_SIZE as u64;
    let lo = (start - base) as usize;
    let hi = (end - base) as usize + 1;
    if hi > buf.len() {
        return Err(StoreError::Internal(format!(
            "range {start}-{end} exceeds materialized block span {}",
            buf.len()
        )));
    }
    Ok(buf[lo..hi].to_vec())
}

/// 分片分支：读回每块盘的 `part.1`，解码重建 `blocks` 指定的块区间。
///
/// `blocks = None` 表示**全部块**——即今天的旧路径，一字不变。
/// `blocks = Some((first, last))`（闭区间）表示只要这几块，此时每块盘也只读这几块。
/// **模式判断在调用方**（`get_object`），本函数只认区间。
///
/// **一块盘的读取失败（含 `Corrupt(BitrotMismatch)`）绝不中止整次读取**，
/// 只是把该盘的槽位置成 `None`，交给纠删码用校验分片补回来——这正是纠删码存在的意义。
/// 只有可用槽位 `< read_quorum` 才是 `ReadQuorum`。
async fn read_shards(
    set: &ErasureSet,
    key_rel: &str,
    winner_dir: &str,
    header: &FileVersionHeader,
    body: &ObjectBody,
    blocks: Option<(usize, usize)>,
) -> Result<Vec<u8>, StoreError> {
    let size = header.size;
    if size == 0 {
        return Ok(Vec::new());
    }
    let data = header.ec_m;
    let parity = header.ec_n.saturating_sub(header.ec_m);
    let total = header.ec_n as usize;
    if data == 0 || parity == 0 || total != set.total() as usize {
        return Err(StoreError::ShardLayout(format!(
            "shard geometry ({data}+{parity}={total}) does not match set total {}",
            set.total()
        )));
    }
    // `ec_dist` 必须是一份合法排列：它不是的话，分片号 ↔ 盘号的映射就是错的，
    // 而错的映射在长度恰好对得上时会**安静地返回错数据**。
    if body.ec_dist.len() != total
        || !rstore_meta::distribution::is_valid_distribution(&body.ec_dist)
    {
        return Err(StoreError::ShardLayout(format!(
            "invalid ec_dist {:?} for total {total}",
            body.ec_dist
        )));
    }

    let step = shard_step(size, data) as usize;
    let shard_len = expected_shard_len(size, data);
    // `size > 0` 已由上面的提前返回保证，所以 `n >= 1`——`n - 1` 不会下溢。
    let n = size.div_ceil(BLOCK_SIZE as u64) as usize;
    let part_rel = format!("{key_rel}/{winner_dir}/part.1");

    // 闭区间的两端。`None` 就是「第 0 块到末块」——与今天完全一致。
    let (first_block, last_block) = blocks.unwrap_or((0, n - 1));
    if first_block > last_block || last_block >= n {
        return Err(StoreError::Internal(format!(
            "block range {first_block}..={last_block} is invalid for {n} blocks"
        )));
    }

    // 盘 `d` 持分片 `j` ⟺ `ec_dist[j] == d + 1`。反过来建表：盘号 → 分片号。
    let mut shard_of_disk = vec![usize::MAX; total];
    for (j, &d1) in body.ec_dist.iter().enumerate() {
        shard_of_disk[(d1 - 1) as usize] = j;
    }

    // 每块盘读一次；失败只记 None，不中止整次读取。
    // `blocks = None` → 整份分片（旧路径）；`Some` → 只读命中的块。
    let mut payloads: Vec<Option<Vec<u8>>> = Vec::with_capacity(total);
    for slot in set.disks() {
        let payload = match slot {
            Some(disk) => {
                let r = BitrotShardReader::new(Arc::clone(disk), part_rel.clone(), step, shard_len);
                match blocks {
                    None => r.read_all().await,
                    Some((fb, lb)) => r.read_range(fb, lb).await,
                }
                .ok()
            }
            None => None,
        };
        payloads.push(payload);
    }
    let available = payloads.iter().filter(|p| p.is_some()).count() as u8;
    let read_quorum = set.read_quorum();
    if available < read_quorum {
        return Err(StoreError::ReadQuorum {
            achieved: available,
            required: read_quorum,
        });
    }

    // 本函数自己的不变量：读到了不一致的长度却照常返回，就是在静默丢数据。
    // 全量读时期望 `size`；区间读时期望该区间覆盖的对象字节数。
    let expect_len = (((last_block + 1) as u64) * BLOCK_SIZE as u64).min(size)
        - first_block as u64 * BLOCK_SIZE as u64;
    // 盘读到的那段是从第 `first_block` 块的头开始的，所以切片下标要减掉这个基准。
    // 全量读时它是 0，即今天的行为。
    let base = first_block * step;

    let mut out: Vec<u8> = Vec::with_capacity(expect_len as usize);
    for k in first_block..=last_block {
        // 块下标现在是 `usize`（`n` 也是），所以偏移一律在 `usize` 里算；
        // `shard_len` 是 `u64`（`expected_shard_len` 的返回类型），这里收窄一次。
        let lo = k * step;
        let hi = ((k + 1) * step).min(shard_len as usize);
        let shard_size_k = hi - lo;
        if shard_size_k == 0 {
            return Err(StoreError::Internal(format!(
                "empty shard slice at block {k} (size {size}, step {step})"
            )));
        }

        // **槽位下标是分片号，不是盘号**：盘 `d` 的这段分片落在槽位 `shard_of_disk[d]`。
        // 把盘号当分片号写进去，正常路径下会以 `UnequalShardLength` 或错误的解码结果收场——
        // 而后者如果恰好长度对得上，就会安静地返回错数据。
        let mut slots: Vec<Option<Vec<u8>>> = vec![None; total];
        for (d, payload) in payloads.iter().enumerate() {
            if let Some(payload) = payload {
                let j = shard_of_disk[d];
                if j != usize::MAX {
                    let rel_lo = lo - base;
                    slots[j] = Some(payload[rel_lo..rel_lo + shard_size_k].to_vec());
                }
            }
        }

        let codec = set
            .codec_cache()
            .get(data as usize, parity as usize, shard_size_k)
            .map_err(|e| {
                StoreError::Internal(format!(
                    "codec geometry ({data}, {parity}, {shard_size_k}): {e}"
                ))
            })?;
        let data_shards = codec
            .decode(&slots)
            .map_err(|e| StoreError::Internal(format!("erasure decode: {e}")))?;

        // 补齐的零只在最后一个数据分片的尾部：顺序相接后截断到**本块真实长度**。
        let block_len = (size - k as u64 * BLOCK_SIZE as u64).min(BLOCK_SIZE as u64) as usize;
        let mut block = Vec::with_capacity(data_shards.len() * shard_size_k);
        for shard in &data_shards {
            block.extend_from_slice(shard);
        }
        if block.len() < block_len {
            return Err(StoreError::Internal(format!(
                "decoded block {k} is {} bytes, shorter than expected {block_len}",
                block.len()
            )));
        }
        out.extend_from_slice(&block[..block_len]);
    }

    if out.len() as u64 != expect_len {
        return Err(StoreError::Internal(format!(
            "reassembled {} bytes but blocks {first_block}..={last_block} should hold {expect_len}",
            out.len()
        )));
    }
    Ok(out)
}

impl ErasureSet {
    /// `range` 为 `None` 时返回整个对象。
    ///
    /// **旧模式（`IoModes::default()`）不做流式**：Range 仍会把整份分片读进来、
    /// 把所有块解码出来，最后才切出 `[start, end]`（省的是网络与 S3 层的内存，
    /// 没省磁盘 IO）——这段描述依然准确，只是有了新路径可选。
    ///
    /// **新模式（`ranged_shard_read`）**：把 Range 折算成块区间，每块盘只读这几块。
    /// 省下的是磁盘 IO 与内存，代价是**被跳过的块不再做 bitrot 校验**（设计文档 §8）。
    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<GetOut, StoreError> {
        // `live()` 借用 `resolved`，所以必须先绑定再 let-else；不能写成
        // `resolve_version(…).await?.live()`——那是借一个临时值。
        let resolved = resolve_version_cached(self, bucket, key).await?;
        let Some((winner_dir, meta)) = resolved.live() else {
            return Err(StoreError::NotFound);
        };
        let key_rel = format!("{bucket}/{key}");
        let latest = latest_version(meta).ok_or_else(|| {
            StoreError::Internal(format!("winner metadata for {key_rel} has no versions"))
        })?;
        let header = latest.header.clone();
        let size = header.size;

        // body 无论哪条分支都要解析：它既承载 `ec_dist` / `parts`，也是
        // 「flags 没标内联但 meta_sys 标了」那条兜底判据的来源。
        let body = decode_body(&latest.body)?;

        // 内联分支：**一次都不碰 `part.*`**。版本键：无版本化桶是 `"null"`。
        let is_inline = header.flags.contains(Flags::INLINE_DATA)
            || body.meta_sys.contains_key(keys::INLINE_DATA);
        if is_inline {
            let full = meta
                .inline
                .get("null")
                .ok_or_else(|| {
                    StoreError::Internal(format!(
                        "version marked inline but holds no inline data for {key_rel}"
                    ))
                })?
                .to_vec();
            let data_dir = header.data_dir.ok_or_else(|| {
                StoreError::Internal(format!("inline version of {key_rel} has no data_dir"))
            })?;
            // etag 与 LIST 共用同一处算法（见 `etag_of_meta`）。
            let etag = etag_of_meta(meta)?;
            let data = apply_range(full, size, range)?;
            return Ok(GetOut {
                data,
                size,
                etag,
                data_dir,
                mod_time: header.mod_time.unwrap_or(0),
            });
        }

        // 分片分支。
        //
        // **模式判断只此一处**：开了范围读就把 Range 折算成块区间，关着就传 `None`
        // （= 今天的全量读）。折算只用已经算出来的 `BLOCK_SIZE`，不需要新的偏移计算。
        let blocks = range.and_then(|r| {
            if !self.modes().ranged_shard_read {
                return None;
            }
            let n = size.div_ceil(BLOCK_SIZE as u64);
            let first = r.start / BLOCK_SIZE as u64;
            let last = (r.end / BLOCK_SIZE as u64).min(n - 1);
            Some((first as usize, last as usize))
        });
        let buf = read_shards(self, &key_rel, winner_dir, &header, &body, blocks).await?;
        // etag 与 LIST 共用同一处算法（见 `etag_of_meta`）；`parts` 为空时它报 `Internal`。
        let etag = etag_of_meta(meta)?;
        let data_dir = header
            .data_dir
            .or(body.id)
            .ok_or_else(|| StoreError::Internal(format!("version of {key_rel} has no data_dir")))?;
        // 两条分支的裁剪基准不同：全量读从对象头开始，区间读从块边界开始。
        let data = match blocks {
            None => apply_range(buf, size, range)?,
            Some((first, _)) => slice_blocks(buf, size, range, first)?,
        };
        Ok(GetOut {
            data,
            size,
            etag,
            data_dir,
            mod_time: header.mod_time.unwrap_or(0),
        })
    }

    /// HEAD：只走元数据、不碰任何 `part.*` 分片，返回 size/etag/mod_time。
    ///
    /// 与 GET 用同一处版本发现（`resolve_version`）与同一处 etag 算法
    /// （`etag_of_meta`），保证 HEAD 与 LIST/GET 的 ETag 不分叉。
    pub async fn head_object(&self, bucket: &str, key: &str) -> Result<HeadOut, StoreError> {
        let resolved = resolve_version_cached(self, bucket, key).await?;
        let Some((_dir, meta)) = resolved.live() else {
            return Err(StoreError::NotFound); // 不存在，或最新版本是删除标记
        };
        let latest = latest_version(meta)
            .ok_or_else(|| StoreError::Internal(format!("{bucket}/{key} has no versions")))?;
        Ok(HeadOut {
            size: latest.header.size,
            etag: etag_of_meta(meta)?,
            mod_time: latest.header.mod_time.unwrap_or(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use rstore_disk::faulty::Fault;

    use rstore_common::modes::IoModes;

    use super::*;
    use crate::put::PutArgs;
    use crate::testutil::{body, set_with_disks, set_with_modes};

    fn put_args(bucket: &str, key: &str, data: Vec<u8>) -> PutArgs {
        PutArgs {
            bucket: bucket.into(),
            key: key.into(),
            body: body(data),
            etag: None,
        }
    }

    #[tokio::test]
    async fn get_returns_what_was_put() {
        let set = set_with_disks(6, 2).await;
        // 跨 3 个 block，且不是 251 的整数倍 → 末块会被补齐，覆盖补零-截断那段几何。
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        set.put_object(put_args("b", "k", data.clone()))
            .await
            .unwrap();

        let got = set.get_object("b", "k", None).await.unwrap();
        assert_eq!(got.size, 3_000_000);
        assert_eq!(got.data, data);
    }

    /// 小对象（1000 字节）远低于内联阈值，PUT 时数据进了 meta.xl，盘上**没有** part.1。
    /// 这里在数据目录里塞一个内容完全错误的 `part.1` 诱饵：如果 GET 走了分片路径，
    /// 它要么报错、要么返回垃圾；只要它返回正确的内联数据，就证明它确实没碰 part。
    #[tokio::test]
    async fn get_inlines_short_circuit_disk_reads() {
        let set = set_with_disks(6, 2).await;
        let data = vec![0x5Au8; 1000];
        let out = set
            .put_object(put_args("b", "small", data.clone()))
            .await
            .unwrap();

        for i in 0..2 {
            let d = set.disks()[i].as_ref().unwrap();
            d.write_all(&format!("b/small/{}/part.1", out.data_dir), &[0xFFu8; 4096])
                .await
                .unwrap();
        }

        let got = set.get_object("b", "small", None).await.unwrap();
        assert_eq!(got.data, data);
    }

    /// 内联对象也要支持 Range（小对象照样能被 `bytes=` 打）。
    #[tokio::test]
    async fn get_inline_object_with_range() {
        let set = set_with_disks(6, 2).await;
        let data: Vec<u8> = (0..1000u32).map(|i| (i % 7) as u8).collect();
        set.put_object(put_args("b", "small", data.clone()))
            .await
            .unwrap();

        let got = set
            .get_object(
                "b",
                "small",
                Some(ByteRange {
                    start: 100,
                    end: 199,
                }),
            )
            .await
            .unwrap();
        assert_eq!(got.size, 1000);
        assert_eq!(got.data, data[100..200]);
    }

    #[tokio::test]
    async fn get_with_range_returns_the_right_slice() {
        let set = set_with_disks(6, 2).await;
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        set.put_object(put_args("b", "k", data.clone()))
            .await
            .unwrap();

        let got = set
            .get_object(
                "b",
                "k",
                Some(ByteRange {
                    start: 1000,
                    end: 1999,
                }),
            )
            .await
            .unwrap();
        assert_eq!(got.size, 3_000_000);
        assert_eq!(got.data, data[1000..2000]);
    }

    /// 开了范围读之后，**结果必须与全量读逐字节相同**。
    /// 区间刻意覆盖三种块边界：首块内、跨块边界、末块单字节、整份。
    #[tokio::test]
    async fn ranged_get_matches_full_get() {
        // 3_000_000 = 2 个满块 + 一个 942_592 字节的末块（不是 251 的整数倍）。
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        let ranges = [
            (0u64, 0u64),
            (0, 99),
            (
                crate::put::BLOCK_SIZE as u64 - 1,
                crate::put::BLOCK_SIZE as u64 + 1,
            ),
            (1_000_000, 2_000_000),
            (2_999_999, 2_999_999),
        ];

        for (start, end) in ranges {
            let plain = set_with_modes(6, 2, IoModes::default()).await;
            plain
                .put_object(put_args("b", "k", data.clone()))
                .await
                .unwrap();

            let ranged = set_with_modes(6, 2, IoModes::ALL).await;
            ranged
                .put_object(put_args("b", "k", data.clone()))
                .await
                .unwrap();

            let expect = &data[start as usize..=end as usize];
            let a = plain
                .get_object("b", "k", Some(ByteRange { start, end }))
                .await
                .unwrap();
            let b = ranged
                .get_object("b", "k", Some(ByteRange { start, end }))
                .await
                .unwrap();
            assert_eq!(a.data, expect, "旧模式 {start}-{end}");
            assert_eq!(b.data, expect, "新模式 {start}-{end}");
            assert_eq!(a.size, b.size);
            assert_eq!(a.etag, b.etag);
        }
    }

    /// 新模式下的 **HEAD 与整读**也必须一个字不变——本次只动范围读那条分支。
    #[tokio::test]
    async fn new_mode_leaves_full_get_and_head_alone() {
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();
        let set = set_with_modes(6, 2, IoModes::ALL).await;
        set.put_object(put_args("b", "k", data.clone()))
            .await
            .unwrap();

        let got = set.get_object("b", "k", None).await.unwrap();
        assert_eq!(got.data, data);
        assert_eq!(got.size, 3_000_000);

        let head = set.head_object("b", "k").await.unwrap();
        assert_eq!(head.size, got.size);
        assert_eq!(head.etag, got.etag);
    }

    #[tokio::test]
    async fn get_survives_two_disk_losses() {
        let set = set_with_disks(6, 2).await;
        set.put_object(put_args("b", "k", vec![3u8; 2_000_000]))
            .await
            .unwrap();
        set.inject_fault_on(0, Fault::Offline);
        set.inject_fault_on(1, Fault::Offline);

        let got = set.get_object("b", "k", None).await.unwrap();
        assert_eq!(got.data, vec![3u8; 2_000_000]);
    }

    #[tokio::test]
    async fn get_fails_closed_below_read_quorum() {
        let set = set_with_disks(6, 2).await;
        set.put_object(put_args("b", "k", vec![3u8; 2_000_000]))
            .await
            .unwrap();
        for i in 0..3 {
            set.inject_fault_on(i, Fault::Offline);
        }
        let r = set.get_object("b", "k", None).await;
        // struct 变体必须带 `{ .. }`。
        assert!(
            matches!(r, Err(StoreError::ReadQuorum { .. })),
            "低于 read_quorum 时绝不能返回部分数据，got {r:?}"
        );
    }

    /// 一块盘的 bitrot 不该打垮读——那是纠删码的用武之地。
    /// 这一条与 `get_fails_closed_below_read_quorum` 一起，把「可用性」和
    /// 「绝不给错数据」两侧都钉住。
    #[tokio::test]
    async fn get_reconstructs_around_one_corrupt_shard() {
        let set = set_with_disks(6, 2).await;
        let data: Vec<u8> = (0..1_000_000u32).map(|i| (i % 13) as u8).collect();
        let out = set
            .put_object(put_args("b", "k", data.clone()))
            .await
            .unwrap();

        // 直接把 0 号盘上的分片文件内容改坏（这是真·静默损坏：写入者没参与，
        // 大小都没变，只有 bitrot 校验能发现）。
        let d = set.disks()[0].as_ref().unwrap();
        let rel = format!("b/k/{}/part.1", out.data_dir);
        let len = d.stat(&rel).await.unwrap().unwrap().size as usize;
        let mut bytes = d.read_exact_at(&rel, 0, len).await.unwrap();
        bytes[rstore_checksum::HASH_LEN] ^= 0xFF;
        d.write_all(&rel, &bytes).await.unwrap();

        let got = set.get_object("b", "k", None).await.unwrap();
        assert_eq!(
            got.data, data,
            "一块盘损坏时必须靠校验分片重建，且结果必须正确"
        );
    }

    #[tokio::test]
    async fn get_missing_object_is_not_found() {
        let set = set_with_disks(6, 2).await;
        let r = set.get_object("b", "nope", None).await;
        assert!(matches!(r, Err(StoreError::NotFound)), "got {r:?}");
    }

    /// HEAD 只走元数据：size/etag/mod_time 与 GET 一致，且不存在的 key 回 `NotFound`。
    #[tokio::test]
    async fn head_matches_get_metadata_without_touching_shards() {
        let set = set_with_disks(6, 2).await;
        let data = vec![9u8; 1_500_000];
        set.put_object(put_args("b", "k", data.clone()))
            .await
            .unwrap();

        let got = set.get_object("b", "k", None).await.unwrap();
        let head = set.head_object("b", "k").await.unwrap();
        assert_eq!(head.size, got.size);
        assert_eq!(head.etag, got.etag);
        assert_eq!(head.mod_time, got.mod_time);

        assert!(matches!(
            set.head_object("b", "nope").await,
            Err(StoreError::NotFound)
        ));
    }

    /// 缓存必须**在覆盖写之后立刻失效**：读到旧版本就是错数据。
    #[tokio::test]
    async fn metadata_cache_sees_a_new_version_after_put() {
        let set = set_with_modes(6, 2, IoModes::ALL).await;
        set.put_object(put_args("b", "k", vec![1u8; 1_500_000]))
            .await
            .unwrap();
        // 先读一次把条目填上。
        assert_eq!(set.head_object("b", "k").await.unwrap().size, 1_500_000);

        set.put_object(put_args("b", "k", vec![2u8; 700_000]))
            .await
            .unwrap();
        assert_eq!(
            set.head_object("b", "k").await.unwrap().size,
            700_000,
            "覆盖写之后缓存必须失效"
        );
        assert_eq!(
            set.get_object("b", "k", None).await.unwrap().data,
            vec![2u8; 700_000]
        );
    }

    /// 删除之后必须立刻读到 `NotFound`，而不是缓存里的旧对象。
    #[tokio::test]
    async fn metadata_cache_sees_a_delete_immediately() {
        let set = set_with_modes(6, 2, IoModes::ALL).await;
        set.put_object(put_args("b", "k", vec![1u8; 1_500_000]))
            .await
            .unwrap();
        assert_eq!(set.head_object("b", "k").await.unwrap().size, 1_500_000);

        set.delete_object("b", "k").await.unwrap();
        let r = set.head_object("b", "k").await;
        assert!(
            matches!(r, Err(StoreError::NotFound)),
            "删除之后缓存必须失效，got {r:?}"
        );
    }

    /// **不存在的 key 也要缓存**（HEAD 密集负载里那是最有价值的一类命中），
    /// 而且 PUT 之后必须立刻可见——否则缓存会把「不存在」永久钉住。
    #[tokio::test]
    async fn metadata_cache_caches_absent_and_uncaches_on_put() {
        let set = set_with_modes(6, 2, IoModes::ALL).await;
        assert!(matches!(
            set.head_object("b", "k").await,
            Err(StoreError::NotFound)
        ));

        set.put_object(put_args("b", "k", vec![1u8; 1_500_000]))
            .await
            .unwrap();
        assert_eq!(set.head_object("b", "k").await.unwrap().size, 1_500_000);
    }

    /// 读失败（`ReadQuorum`）**不进缓存**：一次抖动不该变成持续失败。
    /// 恢复一块盘之后必须立刻能读到。
    #[tokio::test]
    async fn metadata_cache_does_not_cache_errors() {
        let set = set_with_modes(6, 2, IoModes::ALL).await;
        set.put_object(put_args("b", "k", vec![1u8; 1_500_000]))
            .await
            .unwrap();

        for i in 0..4 {
            set.inject_fault_on(i, rstore_disk::faulty::Fault::Offline);
        }
        assert!(matches!(
            set.head_object("b", "k").await,
            Err(StoreError::ReadQuorum { .. })
        ));

        for i in 0..4 {
            set.clear_fault_on(i);
        }
        assert_eq!(
            set.head_object("b", "k").await.unwrap().size,
            1_500_000,
            "错误不该被缓存，恢复之后必须立刻可读"
        );
    }
}
