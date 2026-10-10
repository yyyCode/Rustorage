//! bitrot 分片读取器（DESIGN §11）。与 `writer` 共用同一套交错布局约定。
//!
//! 布局为逐块交错 `[hash(32B)][data]`。哈希与尺寸计算**一律复用 `rstore_checksum`**
//! 的 `bitrot_hash` / `bitrot_size`：在 store 里另写一份就意味着两套 key 常量，
//! 写侧与读侧各算各的，单测两边都绿、只有真实数据校验全失败。

use std::sync::Arc;

use rstore_checksum::{bitrot_hash, bitrot_size, HASH_LEN};
use rstore_common::error::{CorruptKind, DiskError, TransientKind};
use rstore_disk::DiskAPI;

/// 读回一份 bitrot 分片，并逐块重算摘要校验。
pub struct BitrotShardReader {
    disk: Arc<dyn DiskAPI>,
    rel_path: String,
    block_size: usize,
    /// 该分片上应有的原始字节数（不含摘要）。
    shard_len: u64,
}

impl BitrotShardReader {
    /// `block_size` 为 0 直接 panic：与写入器对齐。放在构造期而不是留到 `read_all`
    /// 里让 `div_ceil` 除零，是为了让静态配置错误在最早、最贴近调用点的位置暴露。
    ///
    /// `shard_len` 是**必须传**的，不是可以从文件推出来的：
    /// 不传的话，读取器无法区分「文件被截断」与「本来就该这么短」。
    /// 按文件长度自行推导块数的实现，一旦推少了就会返回一段**截短的数据**——
    /// 而每一块的摘要各自都是对的，哈希校验根本拦不住它。静默丢数据是最坏的失败模式。
    /// PUT 侧本来就知道这个数（分片原始长度），让它传下来即可。
    pub fn new(
        disk: Arc<dyn DiskAPI>,
        rel_path: String,
        block_size: usize,
        shard_len: u64,
    ) -> Self {
        assert!(block_size > 0, "block_size must be non-zero");
        Self {
            disk,
            rel_path,
            block_size,
            shard_len,
        }
    }

    /// 该分片按 `block_size` 切出的块数。0 表示空分片（`shard_len == 0`）。
    fn block_count(&self) -> usize {
        self.shard_len.div_ceil(self.block_size as u64) as usize
    }

    /// 第 `k` 块的明文长度。**只有最后一块可能短**，其余都是满 `block_size`。
    fn block_data_len(&self, k: usize) -> usize {
        if k + 1 == self.block_count() {
            (self.shard_len - k as u64 * self.block_size as u64) as usize
        } else {
            self.block_size
        }
    }

    /// `stat` + **整份文件**的长度检查。`read_all` 与 `read_range` 的共同前置。
    ///
    /// 判定顺序是刻意的，不可调换：
    /// 1. 先 `stat`：不存在 → `NotFound`（参与 quorum 时计为「缺失」而非失败）。
    /// 2. 再比对**文件总长**。短 → `Transient(ShortRead)`（很可能只是写入未完成，
    ///    报成 `Corrupt` 会把它统计进损坏、进而触发 heal）；长 → `Corrupt(LengthMismatch)`
    ///    （多出来的字节没人能解释）。
    ///
    /// **范围读也照样走这一整套**：它收窄的只是 `read_exact_at` 的区间，不是
    /// 「文件总长必须恰好等于 [`bitrot_size`]」这条判据。少了它，一个被截断的文件
    /// 会在只读前几个块的场景下静默返回一段合法-looking 的数据。
    async fn checked_stat(&self) -> Result<u64, DiskError> {
        let stat = self.disk.stat(&self.rel_path).await?;
        let Some(stat) = stat else {
            return Err(DiskError::NotFound);
        };

        // 期望长度与写入器同源：两边都走 `bitrot_size`，不会各自漂移。
        let expected = bitrot_size(self.shard_len, self.block_size as u64);
        if stat.size < expected {
            return Err(DiskError::Transient(TransientKind::ShortRead));
        }
        if stat.size > expected {
            return Err(DiskError::Corrupt(CorruptKind::LengthMismatch));
        }
        Ok(expected)
    }

    /// 读并校验**半开区间** `[first, last)` 内的块，返回它们拼起来的明文。
    ///
    /// 只 `read_exact_at` 这些块实际占用的那段字节：其余块既不入内存，也不重算摘要。
    /// `first == last`（空区间，含空分片）返回空 `Vec`，**一次 IO 都不发**。
    async fn read_blocks(&self, first: usize, last: usize) -> Result<Vec<u8>, DiskError> {
        assert!(first <= last, "block range {first}..{last} is reversed");
        let n = self.block_count();
        assert!(last <= n, "block range {first}..{last} exceeds {n} blocks");
        if first == last {
            return Ok(Vec::new());
        }

        let stride = HASH_LEN + self.block_size;
        let start = first * stride;
        // 末块的结束偏移要按它**实际**的长度算：一律用满 `stride` 会越过文件尾。
        // 这是范围读与整份读在偏移上唯一一处差别，也是唯一容易写错的地方。
        let end = last * stride - self.block_size + self.block_data_len(last - 1);
        let raw = self
            .disk
            .read_exact_at(&self.rel_path, start as u64, end - start)
            .await?;

        // 偏移一律相对 **本次读到的这段** 计算，与整份读的相对偏移由 `first` 平移。
        let mut out = Vec::with_capacity(end - start);
        for k in first..last {
            let off = (k - first) * stride;
            let digest = &raw[off..off + HASH_LEN];
            let data_len = self.block_data_len(k);
            let data = &raw[off + HASH_LEN..off + HASH_LEN + data_len];
            // 比对**重算的数据哈希**与落盘摘要。破坏数据字节必被这里抓出。
            if bitrot_hash(data)[..] != digest[..] {
                return Err(DiskError::Corrupt(CorruptKind::BitrotMismatch));
            }
            out.extend_from_slice(data);
        }
        Ok(out)
    }

    /// 读回整份分片并逐块校验，返回 `shard_len` 字节的原始数据。
    ///
    /// 就是 `read_blocks(0, n)`——长度检查与逐块校验都在那条共用路径上。
    /// `shard_len == 0` 时 `n == 0`，直接落回空 `Vec`，不 panic。
    pub async fn read_all(&self) -> Result<Vec<u8>, DiskError> {
        self.checked_stat().await?;
        let n = self.block_count();
        self.read_blocks(0, n).await
    }

    /// 只读第 `first_block..=last_block` 块（**含两端**）并逐块校验，返回这些块的明文。
    ///
    /// **跳过的块不做 bitrot 校验**——位腐检测的成本正比于真正读到的范围
    /// （见设计文档 §8）。这是刻意的语义变化，不是遗漏：完整读会因为块 7 的损坏
    /// 丢掉整块盘的分片，读块 1 的范围读不会。所以范围读的可用性是**单向变好**的。
    ///
    /// 调用方必须保证 `first_block <= last_block` 且 `last_block < block_count()`。
    /// 越界是程序 bug，用 `assert` 在最早处爆掉，胜过返回一段偏移错位的数据。
    pub async fn read_range(
        &self,
        first_block: usize,
        last_block: usize,
    ) -> Result<Vec<u8>, DiskError> {
        assert!(
            first_block <= last_block,
            "block range {first_block}..={last_block} is reversed"
        );
        self.checked_stat().await?;
        self.read_blocks(first_block, last_block + 1).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstore_checksum::{bitrot_size, HASH_LEN};
    use rstore_common::disk_id::DiskId;
    use rstore_common::error::{CorruptKind, DiskError, TransientKind};
    use rstore_disk::{DiskAPI, LocalDisk};

    use super::*;
    use crate::writer::BitrotShardWriter;

    const BS: usize = 1024;
    const PAYLOAD_LEN: usize = 1500;

    fn temp_disk() -> (tempfile::TempDir, Arc<dyn DiskAPI>) {
        let tmp = tempfile::TempDir::new().unwrap();
        let d: Arc<dyn DiskAPI> = Arc::new(LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap());
        (tmp, d)
    }

    /// 用 Task 4.1 的写入器造一份真实分片——读写的布局约定本来就该由它们彼此对齐。
    async fn write_shard(disk: &Arc<dyn DiskAPI>) {
        let mut w = BitrotShardWriter::new(Arc::clone(disk), "part.1".into(), BS);
        for chunk in vec![0x5Au8; PAYLOAD_LEN].chunks(BS) {
            w.push_block(chunk).await.unwrap();
        }
        w.finish().await.unwrap();
    }

    fn reader(disk: Arc<dyn DiskAPI>) -> BitrotShardReader {
        BitrotShardReader::new(disk, "part.1".into(), BS, PAYLOAD_LEN as u64)
    }

    #[tokio::test]
    async fn reads_back_what_was_written() {
        let (_tmp, disk) = temp_disk();
        write_shard(&disk).await;
        assert_eq!(
            reader(disk).read_all().await.unwrap(),
            vec![0x5Au8; PAYLOAD_LEN]
        );
    }

    #[tokio::test]
    async fn detects_bitrot() {
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;

        // 破坏**数据**字节（偏移 `HASH_LEN` 起是第一个 block 的数据，不是它的摘要）。
        // 破坏摘要字节测出的是另一条路径：那条路径证明不了「重算的数据哈希」真的在比。
        let path = tmp.path().join("part.1");
        let mut raw = std::fs::read(&path).unwrap();
        raw[HASH_LEN] ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();

        let err = reader(disk).read_all().await.unwrap_err();
        assert!(
            matches!(err, DiskError::Corrupt(CorruptKind::BitrotMismatch)),
            "got {err:?}"
        );
    }

    /// 文件被截断 → `Transient(ShortRead)`，**不是** `Corrupt`。
    /// 截断很可能只是写入尚未完成；报成 `Corrupt` 会把它统计进损坏、进而触发 heal（DESIGN §17）。
    #[tokio::test]
    async fn short_file_is_transient_not_corrupt() {
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;

        let path = tmp.path().join("part.1");
        let raw = std::fs::read(&path).unwrap();
        std::fs::write(&path, &raw[..raw.len() - 10]).unwrap();

        let err = reader(disk).read_all().await.unwrap_err();
        assert!(
            matches!(err, DiskError::Transient(TransientKind::ShortRead)),
            "got {err:?}"
        );
    }

    /// 比预期**长**的文件同样是损坏：多出来的字节没人能解释。
    /// 这条同时钉住「读取器确实会比对文件长度」——不做这个检查的实现会读完
    /// 自己需要的字节就返回 `Ok`，把多余的尾巴静默忽略掉。
    #[tokio::test]
    async fn overlong_file_is_corrupt() {
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;

        let path = tmp.path().join("part.1");
        let mut raw = std::fs::read(&path).unwrap();
        assert_eq!(raw.len() as u64, bitrot_size(PAYLOAD_LEN as u64, BS as u64));
        raw.extend_from_slice(&[0xFF; 10]);
        std::fs::write(&path, &raw).unwrap();

        let err = reader(disk).read_all().await.unwrap_err();
        assert!(
            matches!(err, DiskError::Corrupt(CorruptKind::LengthMismatch)),
            "got {err:?}"
        );
    }

    /// 空分片：块数为 0、期望长度 0，应返回空 `Vec` 而不是 panic。
    /// 这里的 0 是**合法的 `shard_len`**，与「文件被截断」截然不同——正因如此才必须由
    /// 调用方传入长度：读取器无法从空文件本身分辨这两者。
    #[tokio::test]
    async fn empty_shard_reads_to_empty() {
        let (_tmp, disk) = temp_disk();
        let w = BitrotShardWriter::new(Arc::clone(&disk), "part.1".into(), BS);
        w.finish().await.unwrap(); // 空 payload：落成 0 字节文件

        let out = BitrotShardReader::new(disk, "part.1".into(), BS, 0)
            .read_all()
            .await
            .unwrap();
        assert_eq!(out, Vec::<u8>::new());
    }

    /// 文件不存在 → `NotFound`：参与 quorum 计数时计为「缺失」，不计为「失败」。
    #[tokio::test]
    async fn missing_file_is_not_found() {
        let (_tmp, disk) = temp_disk();
        let err = reader(disk).read_all().await.unwrap_err();
        assert!(matches!(err, DiskError::NotFound), "got {err:?}");
    }

    // ---- read_range：范围读 ----
    // BS = 1024、PAYLOAD_LEN = 1500 → 分片有 2 块（第 0 块 1024 字节，末块 476 字节）。

    /// 范围读必须与整份读的对应切片**逐字节相同**——这是范围读唯一能拿来对齐的参照物。
    /// 穷举所有 `(first, last)` 组合，把单块、跨块、含末块三种情形都走到。
    #[tokio::test]
    async fn read_range_matches_the_slice_of_read_all() {
        let (_tmp, disk) = temp_disk();
        write_shard(&disk).await;
        let full = reader(Arc::clone(&disk)).read_all().await.unwrap();
        assert_eq!(full.len(), PAYLOAD_LEN);

        let n = PAYLOAD_LEN.div_ceil(BS);
        assert_eq!(n, 2, "本测试的分片几何依赖它是 2 块");
        for first in 0..n {
            for last in first..n {
                let got = reader(Arc::clone(&disk))
                    .read_range(first, last)
                    .await
                    .unwrap();
                let lo = first * BS;
                let hi = ((last + 1) * BS).min(PAYLOAD_LEN);
                assert_eq!(got, full[lo..hi], "range {first}..={last}");
            }
        }
    }

    /// **范围外的位腐不该被范围读发现**——这是设计文档 §8 声明出来的语义变化。
    ///
    /// 同一次损坏下 `read_all` 必须失败，否则这条测试什么都没钉住：它要证的不是
    /// 「范围读很宽松」，而是「范围读校验的范围正比于它读到的范围」。
    #[tokio::test]
    async fn read_range_ignores_bitrot_outside_the_range() {
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;

        // 第 1 块的数据字节：跳过第 0 块的 [摘要(32) + 数据(1024)]，再跳过第 1 块自己的摘要。
        let at = (HASH_LEN + BS) + HASH_LEN;
        let path = tmp.path().join("part.1");
        let mut raw = std::fs::read(&path).unwrap();
        raw[at] ^= 0xFF;
        std::fs::write(&path, &raw).unwrap();

        let r = reader(Arc::clone(&disk)).read_range(0, 0).await;
        assert!(r.is_ok(), "范围外的位腐不该被范围读发现，got {r:?}");

        let full = reader(Arc::clone(&disk)).read_all().await;
        assert!(
            matches!(full, Err(DiskError::Corrupt(CorruptKind::BitrotMismatch))),
            "整份读必须失败，否则这条测试是空的，got {full:?}"
        );
    }

    /// 范围**内**的位腐必须被抓到：范围读省的是校验**范围**，不是校验本身。
    #[tokio::test]
    async fn read_range_detects_bitrot_inside_the_range() {
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;

        let path = tmp.path().join("part.1");
        let mut raw = std::fs::read(&path).unwrap();
        raw[HASH_LEN] ^= 0xFF; // 第 0 块的数据字节
        std::fs::write(&path, &raw).unwrap();

        let r = reader(Arc::clone(&disk)).read_range(0, 0).await;
        assert!(
            matches!(r, Err(DiskError::Corrupt(CorruptKind::BitrotMismatch))),
            "got {r:?}"
        );
    }

    /// **整份文件的长度检查在范围读上一条都不能少**：只读几个块，也必须先确认
    /// 文件总长恰好等于 `bitrot_size`。少了它，一个被截断的文件会在范围读里
    /// 静默返回一段合法-looking 的数据。
    #[tokio::test]
    async fn read_range_keeps_the_whole_file_length_check() {
        // 截断 → Transient(ShortRead)，**不是** Corrupt。
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;
        let path = tmp.path().join("part.1");
        let raw = std::fs::read(&path).unwrap();
        std::fs::write(&path, &raw[..raw.len() - 10]).unwrap();
        let r = reader(Arc::clone(&disk)).read_range(0, 0).await;
        assert!(
            matches!(r, Err(DiskError::Transient(TransientKind::ShortRead))),
            "got {r:?}"
        );

        // 超长 → Corrupt(LengthMismatch)。
        let (tmp, disk) = temp_disk();
        write_shard(&disk).await;
        let path = tmp.path().join("part.1");
        let mut raw = std::fs::read(&path).unwrap();
        raw.extend_from_slice(&[0xFF; 10]);
        std::fs::write(&path, &raw).unwrap();
        let r = reader(Arc::clone(&disk)).read_range(0, 0).await;
        assert!(
            matches!(r, Err(DiskError::Corrupt(CorruptKind::LengthMismatch))),
            "got {r:?}"
        );

        // 不存在 → NotFound（参与 quorum 时计为「缺失」而非「失败」）。
        let (_tmp, disk) = temp_disk();
        let r = reader(disk).read_range(0, 0).await;
        assert!(matches!(r, Err(DiskError::NotFound)), "got {r:?}");
    }

    /// **空分片上的范围读是契约违规，不是空结果**：0 块的分片没有任何合法区间，
    /// 按 `read_range` 的文档契约用 `assert` 爆掉。
    ///
    /// 这条把**契约本身**钉住：谁把那个 `assert` 换成 `return Ok(Vec::new())`，
    /// 越界就会变成一段安静返回的空数据，而调用方分不清它和「真的读到 0 字节」。
    /// 引擎不会走到这一格——`read_shards` 在 `size == 0` 时提前返回，
    /// 所以那里 `n >= 1`，区间恒有解。
    #[tokio::test]
    #[should_panic(expected = "exceeds")]
    async fn read_range_on_an_empty_shard_is_a_contract_violation() {
        let (_tmp, disk) = temp_disk();
        let w = BitrotShardWriter::new(Arc::clone(&disk), "part.1".into(), BS);
        w.finish().await.unwrap(); // 空 payload：落成 0 字节文件

        // 注意这里必须用 `shard_len = 0` 的读取器，**不是**下面测试用的那个
        // `PAYLOAD_LEN` 版本：文件是 0 字节，而 `PAYLOAD_LEN` 的期望长度是
        // `bitrot_size(1500, 1024) = 1564`，长度检查会先报 `ShortRead` 把它拦下，
        // 根本走不到块区间那一步。
        assert_eq!(
            BitrotShardReader::new(Arc::clone(&disk), "part.1".into(), BS, 0)
                .read_all()
                .await
                .unwrap(),
            Vec::<u8>::new(),
            "空分片的整份读仍然必须是空"
        );

        // 0 块的分片，任何区间都不合法。
        let _ = BitrotShardReader::new(disk, "part.1".into(), BS, 0)
            .read_range(0, 0)
            .await;
    }

    /// 反向的契约：`first > last` 同样必须爆掉，而不是安静地什么都不读。
    #[tokio::test]
    #[should_panic(expected = "reversed")]
    async fn read_range_with_a_reversed_interval_is_a_contract_violation() {
        let (_tmp, disk) = temp_disk();
        write_shard(&disk).await;
        let _ = reader(disk).read_range(1, 0).await;
    }
}
