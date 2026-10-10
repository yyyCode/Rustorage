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
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rstore_common::consts::should_inline;
use rstore_meta::distribution::{distribution, is_valid_distribution};
use rstore_meta::keys;
use rstore_meta::{
    encode, encode_body, ChecksumAlgo, FileVersionHeader, Flags, InlineData, ObjectBody,
    ObjectMeta, PartInfo, ShallowVersion, StorageClass, VersionType,
};
use tokio::io::{AsyncRead, AsyncReadExt};
use uuid::Uuid;

use md5::{Digest, Md5};

use crate::commit::commit;
use crate::delete::gc_superseded;
use crate::error::StoreError;
use crate::set::ErasureSet;
use crate::writer::BitrotShardWriter;

/// 对象分块大小：1 MiB。写入器与读取器的 `block_size` 由几何算出（见 [`shard_step`]）。
pub const BLOCK_SIZE: usize = 1 << 20;

/// 一次写入的输入。
///
/// **不复用 `Debug` / `Clone` / `PartialEq`**：`body` 是流，这三者都派不出来
/// （改造前它持有 `Vec<u8>`，所以曾经可以）。
pub struct PutArgs {
    pub bucket: String,
    pub key: String,
    /// 请求体。**读一次就没了**——消费方不该假设它能重放。
    pub body: Box<dyn AsyncRead + Unpin + Send>,
    /// `Some` = 直接用这个 etag（P2 的 multipart Complete 传合成值）；
    /// `None` = 边读边算整份内容的 MD5。
    pub etag: Option<String>,
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
    format!("{:x}", Md5::digest(data))
}

/// 读「至多一个块」进**调用方提供的** `buf`，返回读到的字节数。
///
/// 与 [`read_block`] 是同一件事，区别只在于缓冲由调用方提供、于是可以跨块复用。
/// `truncate` 之后再 `resize` 会把新增的尾部清零——与 `vec![0u8; BLOCK_SIZE]`
/// 的那次清零代价相同（都不省 memset），省下的是**分配**本身。
async fn read_block_into(
    body: &mut (dyn AsyncRead + Unpin + Send),
    buf: &mut Vec<u8>,
) -> Result<usize, StoreError> {
    buf.clear();
    buf.resize(BLOCK_SIZE, 0);
    let mut filled = 0usize;
    while filled < BLOCK_SIZE {
        let n = body
            .read(&mut buf[filled..])
            .await
            .map_err(|e| StoreError::Internal(format!("read request body: {e}")))?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    buf.truncate(filled);
    Ok(filled)
}

/// 读「至多一个块」：读满 `BLOCK_SIZE` 或到 EOF 为止，两者取先到者。
///
/// 返回长度 `< BLOCK_SIZE` 就说明已经读到流末尾——调用方据此判定「没有更多了」。
/// **这是整条写路径唯一的缓冲点**，上界就是 `BLOCK_SIZE`。
///
/// 不用 `AsyncReadExt::take().read_to_end()`：`take` 消费接收者，而我们需要
/// 在下一轮继续读同一个流，改回来要写 `(&mut *body).take(..)` 这类借用体操。
/// 逐次 `read` 到填满更直白，也与 `fsx::read_exact_at` 的循环同一种写法。
async fn read_block(body: &mut (dyn AsyncRead + Unpin + Send)) -> Result<Vec<u8>, StoreError> {
    let mut buf = Vec::new();
    read_block_into(body, &mut buf).await?;
    Ok(buf)
}

/// 单次 PUT 内跨块复用的暂存缓冲（`pooled_write_buffers` 机制）。
///
/// **为什么是局部而不是全局池**：真实需求只有「一次 PUT 里别每块重新分配」。
/// DESIGN §13.3 的四级池要 `Semaphore` + `ManuallyDrop` + `try_lock` 一整套，
/// 而它解决的是跨请求、跨尺寸的复用——不是这次的对比目标。见设计文档 §5。
///
/// **它不减少任何一次 IO**，所以它的证据形态是**分配次数**，不是 IO 计数。
/// 只看 IO 计数的对比会显示 0 差异，那不是 bug，那是它本来的样子。
struct ShardScratch {
    /// 当前块的读缓冲，容量在满载时为 `BLOCK_SIZE`。
    block: Vec<u8>,
    /// `data_shards` 份数据分片缓冲，跨块复用。
    shards: Vec<Vec<u8>>,
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
    ///
    /// **流式**：请求体逐块读入、逐块编码落盘，峰值内存与对象大小无关（上界
    /// `BLOCK_SIZE` + 一份分片）。
    ///
    /// 本函数是 [`Self::put_object_inner`] 的薄包装，只多一件事：**提交成功后
    /// 让元数据缓存里这个 key 作废**。放在唯一的成功出口上，两个返回分支
    /// （内联 / 分片）与将来新增的分支都不会漏掉它。
    pub async fn put_object(&self, args: PutArgs) -> Result<PutOut, StoreError> {
        let bucket = args.bucket.clone();
        let key = args.key.clone();
        let out = self.put_object_inner(args).await?;
        // **只在成功时失效**：失败（含低于 quorum）没有改变任何权威版本，
        // 此时清掉条目只会白白少一次命中。
        if let Some(cache) = self.resolve_cache() {
            cache.invalidate(&bucket, &key);
        }
        Ok(out)
    }

    /// 真正的写入逻辑。语义与签名都与引入缓存之前一致。
    async fn put_object_inner(&self, args: PutArgs) -> Result<PutOut, StoreError> {
        let PutArgs {
            bucket,
            key,
            mut body,
            etag,
        } = args;
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

        // 先读一个块。**这一步同时定尺寸**：读不满一个满块就说明整个对象到此为止，
        // 于是 `known_len` 是精确值；读满了则说明对象至少一个块，后续长度不影响
        // `shard_step`（它只看 `size.min(BLOCK_SIZE)`，见该函数的文档）。
        let first = read_block(&mut *body).await?;
        let known_len = (first.len() < BLOCK_SIZE).then_some(first.len() as u64);

        if known_len.is_some_and(|s| should_inline(s, /* versioned_bucket = */ false)) {
            // 内联分支：数据进 meta.xl，不产生 part.*。阈值只此一份来源，
            // 绝不在本文件里再写一个常量。
            let size = first.len() as u64;
            let etag = etag.unwrap_or_else(|| etag_of(&first));
            let mut inline = InlineData::new();
            inline.insert("null", first);
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

            // 提交：rename 达到 quorum 才算成功；低于时由 `commit` best-effort 回滚
            // 它自己 rename 过去的那些目录。
            commit(self, &staging, &final_rel, write_quorum).await?;

            // **先提交、后 GC，顺序不可颠倒**：反过来就是在删还没提交的数据。
            gc_superseded(self, &bucket, &key, &data_dir.to_string()).await;

            return Ok(PutOut {
                size,
                etag,
                data_dir,
                version_id,
            });
        }

        // 分片分支。
        //
        // `shard_step` 只需要 `size.min(BLOCK_SIZE)`：已知总长时传它；未知时说明
        // 对象至少有一个满块，`min` 的结果恒为 `BLOCK_SIZE`，传哪个够大的值都一样。
        let step = shard_step(known_len.unwrap_or(BLOCK_SIZE as u64), data_shards);

        let (shard_len, size, md5) = self
            .write_shards_stream(&mut *body, first, &dist, &staging, write_quorum, step)
            .await?;
        // 调用方给了 etag 就用它（P2 的 multipart Complete 传的是合成的 `-N` 形式）；
        // 否则用边读边算出来的真 MD5。策略在这里拍板，写入器只管算。
        let etag = etag.unwrap_or(md5);

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

    /// 分片写入：从 `body` 逐块读、逐块编码、逐块追加到各盘的 `part.1`。
    ///
    /// 返回 `(每块盘上的分片字节数, 对象总长, 内容的 MD5)`。分片字节数是读侧构造
    /// 读取器时要的 `shard_len`，由 `expected_shard_len` 用**实际读到的总长**复算——
    /// 与读侧共用同一个函数，两边各算各的必然漂移。
    ///
    /// **这里只算 MD5，不决定最终 etag**：写入器的职责是「写下去 + 算摘要」，
    /// 「用算出来的还是用调用方给的」是策略，归调用方（内联分支也是这么分派的），
    /// 两支因此对称。顺带把参数压到 6 个——clippy 对**方法**会把 `&self` 计进
    /// `too_many_arguments`（阈值 7），所以方法的名额只有 6 个。
    ///
    /// `first` 是调用方已经读出来的第一个块（可能已到 EOF，即一个短块）。
    ///
    /// 低于 quorum 直接返回错误，**且不清理已写的分片**：残留由对账流程回收
    /// （DESIGN §12.2）。主动清理反而会把崩溃残留抹掉，让对账的可回收性无处可测。
    async fn write_shards_stream(
        &self,
        body: &mut (dyn AsyncRead + Unpin + Send),
        first: Vec<u8>,
        dist: &[u8],
        staging: &str,
        write_quorum: u8,
        step: u64,
    ) -> Result<(u64, u64, String), StoreError> {
        let data_shards = self.data();
        let parity = self.parity();
        let step = step as usize;
        let part_rel = format!("{staging}/part.1");
        // 缓冲复用只在开关打开时启用。**关着时走上下一字不改的老路径**：
        // 每块一个新 `Vec`，`block` 也每轮重新分配。
        let pooled = self.modes().pooled_write_buffers;

        // 循环外为每块盘建一个写入器；掉线的槽位没有写入器。
        let mut writers: Vec<Option<BitrotShardWriter>> = self
            .disks()
            .iter()
            .map(|slot| {
                slot.as_ref()
                    .map(|d| BitrotShardWriter::new(Arc::clone(d), part_rel.clone(), step))
            })
            .collect();

        let mut md5 = Md5::new();
        let mut size: u64 = 0;
        // `first` 是调用方已经读出来的第一个块——它天然就是这份暂存缓冲的起点。
        let mut scratch = ShardScratch {
            block: first,
            shards: Vec::new(),
        };

        loop {
            // 空块只可能在 EOF 时出现（且此时前面的判断已经 break），
            // 留着这道闸是为了不把「空块」喂进 `push_block`——它会当成布局错误拒绝。
            if scratch.block.is_empty() {
                break;
            }

            md5.update(&scratch.block);
            let shard_size_k =
                even_ceil((scratch.block.len() as u64).div_ceil(u64::from(data_shards))) as usize;

            // 关掉复用时就地把上一轮的分片缓冲全部丢掉，让下一轮重新分配——
            // 与引入本机制之前一字不差。
            if !pooled {
                scratch.shards = Vec::new();
            }
            scratch.shards.resize_with(data_shards as usize, Vec::new);
            // 数据分片：按 `shard_size_k` 切，不足处补零。补零只可能落在最后一个
            // 分片的尾部，这正是读侧「顺序相接再截断到 L_k」的依据。
            let mut grew = 0u64;
            for (i, shard) in scratch.shards.iter_mut().enumerate() {
                // 分配计数（Step 2b）：只有「这一轮 `resize` 真的会向分配器要内存」
                // 才计数。`clear()` 不释放容量，容量够时 `resize` 就在原地写，
                // 所以这个判据恰好就是「要不要新内存」。
                // **只数分片数据缓冲**，外层 `Vec<Vec<u8>>`（24 字节/项）不计——
                // 量的对象是分片数据本身。
                if shard.capacity() < shard_size_k {
                    grew += 1;
                }
                shard.clear();
                shard.resize(shard_size_k, 0);
                let s = i * shard_size_k;
                if s < scratch.block.len() {
                    let e = (s + shard_size_k).min(scratch.block.len());
                    shard[..e - s].copy_from_slice(&scratch.block[s..e]);
                }
            }
            self.scratch_allocs.fetch_add(grew, Ordering::Relaxed);

            let codec = self
                .codec_cache()
                .get(data_shards as usize, parity as usize, shard_size_k)
                .map_err(|e| {
                    StoreError::Internal(format!(
                        "codec geometry ({data_shards}, {parity}, {shard_size_k}): {e}"
                    ))
                })?;
            let parity_shards = codec
                .encode(&scratch.shards)
                .map_err(|e| StoreError::Internal(format!("erasure encode: {e}")))?;

            // 分片编号沿用 `encode` 的输出顺序：`0..data` 数据分片，`data..N` 校验分片。
            // 数据分片借自 `scratch.shards`、校验分片借自 `parity_shards`——两者拼成
            // 同一串编号，但**不合成一个新的 `Vec`**（合成就把复用又还回去了）。
            let total_shards = data_shards as usize + parity_shards.len();
            for kk in 0..total_shards {
                let shard: &[u8] = if kk < data_shards as usize {
                    &scratch.shards[kk]
                } else {
                    &parity_shards[kk - data_shards as usize]
                };

                // 分片 `kk` 落在盘 `dist[kk] - 1`。每块盘每块恰好收到一份（`dist` 是排列）。
                let physical = usize::from(dist[kk] - 1);
                // 两种错误必须分开处理，**不能**一个 `?` 了事：
                // - `ShardLayout` 是程序 bug（几何算错、块序颠倒），整次写入中止；
                // - 盘的 IO 失败等价于「这块盘拿不到票」，交给下面的 `write_quorum` 判定。
                // 逐块落盘之后后者第一次有了提前暴露的机会（从前 `push_block` 根本不碰盘，
                // 所有 IO 错误都堆在 `finish` 里由选票兜底）。若在这里提前返回，
                // 一块掉线的盘就会让**整个 PUT** 失败，而它本该只是少一票。
                // 所以丢掉这块盘的写入器，后续块不再往它写。
                let failed = match writers[physical].as_mut() {
                    Some(w) => match w.push_block(shard).await {
                        Ok(()) => false,
                        Err(e @ StoreError::ShardLayout(_)) => return Err(e),
                        Err(_) => true,
                    },
                    None => false,
                };
                if failed {
                    writers[physical] = None;
                }
            }

            size += scratch.block.len() as u64;
            // 短块就是最后一块——这里 break，绝不再去读流，否则会多等一次 IO。
            if scratch.block.len() < BLOCK_SIZE {
                break;
            }
            if pooled {
                // 复用同一块缓冲：`read_block_into` 会先 clear + resize，旧内容不残留。
                read_block_into(body, &mut scratch.block).await?;
            } else {
                scratch.block = read_block(body).await?;
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

        let shard_len = expected_shard_len(size, data_shards);
        Ok((shard_len, size, format!("{:x}", md5.finalize())))
    }
}

#[cfg(test)]
mod tests {
    use rstore_disk::Fault;
    use uuid::Uuid;

    use super::*;
    use crate::error::StoreError;
    use crate::testutil::{
        body, set_with_disks, set_with_recording_disks, set_with_recording_disks_modes, TestSet,
    };
    use rstore_common::modes::IoModes;

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
                body: body(data.clone()),
                etag: None,
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
        let out = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "big".into(),
                body: body(vec![7u8; 1_500_000]), // 2 个 block：1 MiB + 451 424
                etag: None,
            })
            .await
            .unwrap();
        assert_eq!(out.size, 1_500_000);

        // 单部分对象：每块盘的数据目录里恰好一个分片文件 part.1。
        // 原计划这里写「不是 6 个！」，是因为当时没定清楚 part.N 的 N 指什么——
        // N 是**部分号**（multipart 的 part），不是盘号。P1 阶段恒为 1；
        // P2 的 multipart 走「Complete 时重编码成整体」（设计文档 §2 决策一），
        // 那时盘上**仍是一个** part.1，S3 的 part 号只体现在 etag 的 `-N` 后缀里——
        // 也就是说这个 N 与 S3 的 part 是一一对应的，只是永远等于 1。
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

    /// **本任务的核心断言**：写路径从不一次性写出超过一个块。
    ///
    /// 这钉的是设计文档 §1.2 那个 3.5x 放大器。退回任何一种整份缓冲都会在这里红：
    /// - 退回 `BitrotShardWriter` 攒整份分片 -> `write_all` 收到整份分片（16 MiB 对象
    ///   的 4 数据分片是 4 MiB），远大于 `BLOCK_SIZE + 32`；
    /// - 退回 S3 层的 `try_collect().concat()` -> 不会出现在本层，由
    ///   `crates/s3` 的测试与 acceptance 脚本覆盖。
    #[tokio::test]
    async fn write_path_never_emits_more_than_one_block() {
        // 16 MiB = 16 个块，4+2 布局。够大到让「整份缓冲」与「逐块」差出数量级：
        // 整份缓冲时每块盘一次写出的分片是 4 MiB，逐块时是 256 KiB。
        const SIZE: usize = 16 * 1024 * 1024;

        let (set, log) = set_with_recording_disks(6, 2).await;
        let out = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "bounded".into(),
                body: body(vec![0x5Au8; SIZE]),
                etag: None,
            })
            .await
            .unwrap();
        assert_eq!(out.size as usize, SIZE);

        let sizes = log.sizes();
        assert!(!sizes.is_empty(), "盘上一个字节都没写？");
        let max = *sizes.iter().max().unwrap();
        assert!(
            max <= BLOCK_SIZE + rstore_checksum::HASH_LEN,
            "单次写入 {max} 字节，超过一个块（{BLOCK_SIZE} + {}）——写路径又在整份缓冲了",
            rstore_checksum::HASH_LEN
        );
        // 顺带确认它确实是**多次**写入，而不是运气好一次写完刚好没超。
        // 16 个块 × 6 块盘 = 96 次 append（外加若干次 meta.xl）。
        assert!(
            sizes.len() > 16,
            "16 MiB / 1 MiB 应该远多于 16 次写入，实际 {}",
            sizes.len()
        );
    }

    /// 缓冲复用**不该改变任何可观察行为**。三组断言，一组比一组强：
    /// 1. 内容/大小/etag 相同；
    /// 2. `RecordingDisk` 记下的**分片写入长度序列逐项相同**；
    /// 3. 读回来仍然逐字节正确（复用缓冲如果残留旧内容，会在这里显形）。
    ///
    /// **比的是 `part.1` 而不是整份日志**：`meta.xl` 的长度在两次 PUT 之间本来就
    /// 会差几个字节（两个 UUID 走 `HeaderWire` 的 `[u8; 16]` 编码，`rmp_serde` 对
    /// ≥ 0x80 的字节要多花一个 `uint8` 前缀），拿它做跨模式的逐项比对必然间歇性
    /// 失败。分片写入才是这个机制动到的东西，也正是它该被钉住的部分。详见
    /// [`crate::testutil::WriteLog::sizes_of`]。
    #[tokio::test]
    async fn pooled_write_buffers_is_observably_identical() {
        // 3_000_000 = 2 个满块 + 一个短末块：末块是唯一会改变 `shard_size_k` 的地方，
        // 也是「复用缓冲」最容易出错的块（长度变了，缓冲要 resize 而不是沿用）。
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();

        let (plain, log_plain) = set_with_recording_disks_modes(6, 2, IoModes::default()).await;
        let a = plain
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                body: body(data.clone()),
                etag: None,
            })
            .await
            .unwrap();

        let (pooled, log_pooled) = set_with_recording_disks_modes(6, 2, IoModes::ALL).await;
        let b = pooled
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                body: body(data.clone()),
                etag: None,
            })
            .await
            .unwrap();

        assert_eq!(a.size, b.size);
        assert_eq!(
            a.etag, b.etag,
            "etag 是内容的 MD5，相同内容必须给出相同 etag"
        );

        let shards_plain = log_plain.sizes_of("part.1");
        let shards_pooled = log_pooled.sizes_of("part.1");
        // 空向量之间的相等是**空洞的**，先钉住它确实记到了东西：3 个块 × 6 块盘。
        assert_eq!(shards_plain.len(), 18, "3 个块 × 6 块盘");
        assert_eq!(
            shards_plain, shards_pooled,
            "分片的写入长度序列必须逐项相同——缓冲复用不该改变写入的形状"
        );

        assert_eq!(plain.get_object("b", "k", None).await.unwrap().data, data);
        assert_eq!(pooled.get_object("b", "k", None).await.unwrap().data, data);
    }

    /// L4 的**证据本身**：分配计数必须真的降下来。
    ///
    /// 这条断言的意义在于——IO 计数在两条模式下**必然相同**（这个机制不动 IO），
    /// 所以上面两条测试拿不到任何区分度。只有分配计数能证明机制在工作。
    ///
    /// 算术：`(6, 2)` → `data_shards = 4`；3 个块（2 满 + 1 短）。
    /// 旧模式每块 4 份新缓冲 = 12；新模式首块 4 份、之后 0 = 4。末块 `shard_size_k`
    /// 变小不会触发增长，所以 4 是精确值。
    #[tokio::test]
    async fn pooled_write_buffers_cuts_shard_buffer_allocations() {
        let data: Vec<u8> = (0..3_000_000u32).map(|i| (i % 251) as u8).collect();

        let (plain, _log_plain) = set_with_recording_disks_modes(6, 2, IoModes::default()).await;
        plain
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                body: body(data.clone()),
                etag: None,
            })
            .await
            .unwrap();

        let (pooled, _log_pooled) = set_with_recording_disks_modes(6, 2, IoModes::ALL).await;
        pooled
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                body: body(data.clone()),
                etag: None,
            })
            .await
            .unwrap();

        assert_eq!(plain.take_scratch_allocs(), 12, "旧模式：3 块 × 4 份新缓冲");
        assert_eq!(
            pooled.take_scratch_allocs(),
            4,
            "新模式：首块 4 份，之后复用"
        );
        // `take_` 的语义是「取走并归零」——再取一次必须是 0，否则基准里的累计值
        // 会把两次测量混在一起。
        assert_eq!(pooled.take_scratch_allocs(), 0);
    }

    /// 单块对象没有第二块可复用——**这是这个机制的阴性对照**。
    /// 两条模式的分片写入序列必须完全相同。
    ///
    /// **取 512 KiB 而不是「小于一个块就行」**：≤ 128 KiB 的对象走内联分支
    /// （`should_inline`），数据进 `meta.xl`、**根本不产生 `part.1`**——那样比的是
    /// 两个空向量，阴性对照会空洞地通过。512 KiB 高于 128 KiB 的内联阈值、
    /// 又低于 `BLOCK_SIZE`（1 MiB），恰好落进「一个块、不内联」那唯一一段区间。
    #[tokio::test]
    async fn pooled_write_buffers_has_no_effect_on_a_single_block_object() {
        let (plain, log_plain) = set_with_recording_disks_modes(6, 2, IoModes::default()).await;
        let (pooled, log_pooled) = set_with_recording_disks_modes(6, 2, IoModes::ALL).await;

        for set in [&plain, &pooled] {
            set.put_object(PutArgs {
                bucket: "b".into(),
                key: "k".into(),
                body: body(vec![0x5Au8; 512 * 1024]),
                etag: None,
            })
            .await
            .unwrap();
        }
        let shards_plain = log_plain.sizes_of("part.1");
        let shards_pooled = log_pooled.sizes_of("part.1");
        // 同样先确认真记到了东西：1 个块 × 6 块盘。
        assert_eq!(shards_plain.len(), 6, "1 个块 × 6 块盘");
        assert_eq!(shards_plain, shards_pooled);
    }

    /// 流式入口必须与原入口逐字节等价：同样的字节进去，同样的 etag / size / 盘上布局。
    #[tokio::test]
    async fn streaming_put_matches_buffered_put() {
        let set = set_with_disks(6, 2).await;
        let data: Vec<u8> = (0..1_500_000u32).map(|i| (i % 251) as u8).collect();
        let out = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "streamed".into(),
                body: body(data.clone()),
                etag: None,
            })
            .await
            .unwrap();

        assert_eq!(out.size, 1_500_000);
        // etag 必须还是真 MD5——边读边算的实现在这里最容易退化成「半个对象的 MD5」。
        assert_eq!(out.etag, etag_of(&data));

        // 盘上分片长度与读侧独立复算的那个数一致（与 put_large_object_creates_shards 同款断言）。
        let step = shard_step(out.size, 4);
        let expect_on_disk = rstore_checksum::bitrot_size(expected_shard_len(out.size, 4), step);
        for i in 0..6 {
            let d = set.disks()[i].as_ref().unwrap();
            let st = d
                .stat(&format!("b/streamed/{}/part.1", out.data_dir))
                .await
                .unwrap()
                .expect("part.1 必须存在");
            assert_eq!(st.size, expect_on_disk, "disk {i}");
        }
    }

    /// 调用方给了 etag 就必须用它，而不是算出来的 MD5。P2 的 multipart Complete
    /// 靠这条把合成的 `-N` 形式写进对象。
    #[tokio::test]
    async fn supplied_etag_wins_over_computed_md5() {
        let set = set_with_disks(6, 2).await;
        let data = vec![3u8; 1_500_000];
        let out = set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "given".into(),
                body: body(data.clone()),
                etag: Some("d41d8cd98f00b204e9800998ecf8427e-2".into()),
            })
            .await
            .unwrap();
        assert_eq!(out.etag, "d41d8cd98f00b204e9800998ecf8427e-2");
        assert_ne!(out.etag, etag_of(&data), "必须不是算出来的那个");
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
                body: body(vec![0u8; 1_000_000]),
                etag: None,
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
