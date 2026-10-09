//! bitrot 分片写入器（DESIGN §11）。
//!
//! 落盘布局为逐块交错 `[hash(32B)][data]`。哈希与尺寸计算**一律复用
//! `rstore_checksum`**（`bitrot_hash` / `bitrot_size`）：在 store 里另写一份意味着
//! 两套 key 常量，今天写进去的数据换了实现就校验全失败。

use std::sync::Arc;

use rstore_checksum::bitrot_hash;
use rstore_disk::DiskAPI;

use crate::error::StoreError;

/// 把一连串 block 组装成一份 bitrot 分片并一次性落盘。
///
/// **必须是「缓冲 + 一次落盘」，而不是逐块追加。** `DiskAPI::write_all` 的底层是
/// `File::create`（创建即截断）：若每块都调一次 `write_all`，每次都会把前一块抹掉，
/// 盘上最后只剩最后一块——而每次调用都返回 `Ok`，调用方看不出任何异常。
/// `DiskAPI` 目前没有 `append` / `write_at`，因此写入器自己缓冲整份分片。
pub struct BitrotShardWriter {
    disk: Arc<dyn DiskAPI>,
    rel_path: String,
    block_size: usize,
    /// 交错布局的完整字节（摘要 + 数据），`finish` 时一次写出。
    buf: Vec<u8>,
    /// 已推入一个短块：此后不允许再推入任何块。
    short_seen: bool,
    /// 已推入的原始字节数（不含摘要）。`buf.len()` 无法反推出它——里面还混着摘要。
    payload_len: u64,
}

impl BitrotShardWriter {
    /// `block_size` 为 0 直接 panic：它是调用方的静态配置错误，任何一次 `push_block`
    /// 都无法补救（且 `bitrot_size` 的 `div_ceil` 同样会因除零 panic），
    /// 与其返回一个只剩 panic 用法的 `Result`，不如在构造期就 fail fast。
    pub fn new(disk: Arc<dyn DiskAPI>, rel_path: String, block_size: usize) -> Self {
        assert!(block_size > 0, "block_size must be non-zero");
        Self {
            disk,
            rel_path,
            block_size,
            buf: Vec::new(),
            short_seen: false,
            payload_len: 0,
        }
    }

    /// 追加一个 block：把 `[hash(32B)][data]` 追加进内部缓冲。
    /// `data.len() <= block_size`；短块只能出现在末尾；空块一律拒绝。
    pub fn push_block(&mut self, data: &[u8]) -> Result<(), StoreError> {
        if data.is_empty() {
            return Err(StoreError::ShardLayout(
                "empty block: zero-byte blocks have no writer/reader representation".into(),
            ));
        }
        if data.len() > self.block_size {
            return Err(StoreError::ShardLayout(format!(
                "block of {} bytes exceeds block_size {}",
                data.len(),
                self.block_size
            )));
        }
        if self.short_seen {
            return Err(StoreError::ShardLayout(
                "block pushed after a short block; a short block may only be the last one".into(),
            ));
        }

        self.buf.extend_from_slice(&bitrot_hash(data));
        self.buf.extend_from_slice(data);
        self.payload_len += data.len() as u64;
        // 短块只能收尾：置位后任何后续 push_block 都会被上面拦下。
        if data.len() < self.block_size {
            self.short_seen = true;
        }
        Ok(())
    }

    /// 已推入的原始字节数（不含摘要）。读侧构造读取器时要拿它当 `shard_len`，
    /// 所以这里必须暴露出来而不是让调用方自己累加。
    pub fn payload_len(&self) -> u64 {
        self.payload_len
    }

    /// 一次性把整份分片落盘并 fsync（文件 + 父目录）。
    ///
    /// `write_all` 已 fsync 文件本身；父目录项需要再 sync 一次才耐久，否则崩溃后
    /// 文件内容在盘上、目录里却没有它。
    pub async fn finish(self) -> Result<(), StoreError> {
        self.disk.write_all(&self.rel_path, &self.buf).await?;
        self.disk.sync_file_and_parent(&self.rel_path).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rstore_checksum::{bitrot_hash, bitrot_size, HASH_LEN};
    use rstore_common::disk_id::DiskId;
    use rstore_disk::{DiskAPI, LocalDisk};

    use super::*;
    use crate::error::StoreError;

    /// 真实的 `LocalDisk` 而不是内存假盘：本任务要断言的正是「落到磁盘上的字节形状」。
    fn temp_disk() -> (tempfile::TempDir, Arc<dyn DiskAPI>) {
        let tmp = tempfile::TempDir::new().unwrap();
        let d: Arc<dyn DiskAPI> = Arc::new(LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap());
        (tmp, d)
    }

    #[tokio::test]
    async fn writes_interleaved_hash_and_data() {
        // 布局：每个 block 落盘为 `[hash(32B)][data]`（DESIGN §11）。
        let (tmp, disk) = temp_disk();
        let mut w = BitrotShardWriter::new(disk, "part.1".into(), 1024);
        w.push_block(&[7u8; 1024]).unwrap();
        w.push_block(&[9u8; 476]).unwrap();
        assert_eq!(w.payload_len(), 1500);
        w.finish().await.unwrap();

        let raw = std::fs::read(tmp.path().join("part.1")).unwrap();

        // 只断言总长度是不够的：把布局写成 `[data][hash]` 的实现，总长度一模一样。
        // 必须逐块核对摘要与数据各自的位置。
        assert_eq!(raw.len(), 1564);
        assert_eq!(&raw[..HASH_LEN], &bitrot_hash(&[7u8; 1024])[..]);
        assert_eq!(&raw[HASH_LEN..HASH_LEN + 1024], &[7u8; 1024][..]);
        assert_eq!(&raw[1056..1056 + HASH_LEN], &bitrot_hash(&[9u8; 476])[..]);
        assert_eq!(&raw[1056 + HASH_LEN..], &[9u8; 476][..]);
    }

    /// 落盘尺寸必须等于 `rstore_checksum::bitrot_size` 的预言。两者分居两个 crate，
    /// 若各算各的，两边单测都会绿，只有读路径会按错误的偏移去取字节。
    #[tokio::test]
    async fn layout_agrees_with_shared_bitrot_size() {
        for (len, bs) in [
            (0usize, 1024usize),
            (1, 1024),
            (1024, 1024),
            (1025, 1024),
            (5000, 512),
        ] {
            let (tmp, disk) = temp_disk();
            let payload = vec![0xABu8; len];
            let mut w = BitrotShardWriter::new(disk, "part.1".into(), bs);
            for chunk in payload.chunks(bs) {
                w.push_block(chunk).unwrap();
            }
            w.finish().await.unwrap();

            let on_disk = std::fs::metadata(tmp.path().join("part.1")).unwrap().len();
            assert_eq!(
                on_disk,
                bitrot_size(len as u64, bs as u64),
                "len={len} bs={bs}"
            );
        }
    }

    /// 短块只允许出现在**末尾**。若中间混进短块而写入器默许，读侧按
    /// `k * (32 + block_size)` 的固定步长定位就会整体错位；错位读出的字节哈希必然对不上，
    /// 于是被报成 `Corrupt(BitrotMismatch)`——**写入方的 bug 伪装成盘损坏，
    /// 进而触发对健康数据的 heal**。宁可在这里拒绝。
    #[test]
    fn rejects_misuse_that_would_desync_the_reader() {
        let (_tmp, disk) = temp_disk();
        let mut w = BitrotShardWriter::new(disk, "part.1".into(), 1024);
        w.push_block(&[1u8; 100]).unwrap(); // 短块：可以，但必须是最后一块
        assert!(matches!(
            w.push_block(&[2u8; 100]),
            Err(StoreError::ShardLayout(_))
        ));

        let (_tmp, disk) = temp_disk();
        let mut w = BitrotShardWriter::new(disk, "part.1".into(), 1024);
        // 超长块任何时候都不合法；空块也无意义（`bitrot_size` 不会为 0 字节产生块），
        // 多出来的那 32 字节摘要没有对应的数据，读侧会把它当成一个空块。
        assert!(matches!(
            w.push_block(&[3u8; 1025]),
            Err(StoreError::ShardLayout(_))
        ));
        assert!(matches!(w.push_block(&[]), Err(StoreError::ShardLayout(_))));
    }
}
