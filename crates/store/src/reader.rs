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

    /// 读回整份分片并逐块校验，返回 `shard_len` 字节的原始数据。
    ///
    /// 判定顺序是刻意的，不可调换：
    /// 1. 先 `stat`：不存在 → `NotFound`（参与 quorum 时计为「缺失」而非失败）。
    /// 2. 再比对**文件总长**。短 → `Transient(ShortRead)`（很可能只是写入未完成，
    ///    报成 `Corrupt` 会把它统计进损坏、进而触发 heal）；长 → `Corrupt(LengthMismatch)`
    ///    （多出来的字节没人能解释）。少了这一步，读取器只会读完自己需要的字节就返回 `Ok`，
    ///    把多余的尾巴静默忽略——于是「文件被追加了垃圾」这种损坏永远发现不了。
    /// 3. 一次性读整份（MVP 下分片本来就是内存里的整块）。
    /// 4. 逐块重算摘要比对，不符 → `Corrupt(BitrotMismatch)`。
    pub async fn read_all(&self) -> Result<Vec<u8>, DiskError> {
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

        let raw = self
            .disk
            .read_exact_at(&self.rel_path, 0, expected as usize)
            .await?;

        // `shard_len == 0` 时块数为 0、`expected == 0`：这里直接落回空 Vec，不 panic。
        let n = self.shard_len.div_ceil(self.block_size as u64);
        let mut out = Vec::with_capacity(self.shard_len as usize);
        for k in 0..n {
            // 偏移用固定步长 `block_size + HASH_LEN`：读侧不依赖任何块内元数据，
            // 块边界完全由 (shard_len, block_size, k) 决定。
            let off = (k as usize) * (HASH_LEN + self.block_size);
            let digest = &raw[off..off + HASH_LEN];
            // 只有最后一块可能是短块；前面的块一定满 block_size。
            let data_len = if k + 1 == n {
                (self.shard_len - k * self.block_size as u64) as usize
            } else {
                self.block_size
            };
            let data = &raw[off + HASH_LEN..off + HASH_LEN + data_len];
            // 比对**重算的数据哈希**与落盘摘要。破坏数据字节必被这里抓出。
            if bitrot_hash(data)[..] != digest[..] {
                return Err(DiskError::Corrupt(CorruptKind::BitrotMismatch));
            }
            out.extend_from_slice(data);
        }
        Ok(out)
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
            w.push_block(chunk).unwrap();
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
}
