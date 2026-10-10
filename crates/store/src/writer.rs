//! bitrot 分片写入器（DESIGN §11）。
//!
//! 落盘布局为逐块交错 `[hash(32B)][data]`。哈希与尺寸计算**一律复用
//! `rstore_checksum`**（`bitrot_hash` / `bitrot_size`）：在 store 里另写一份意味着
//! 两套 key 常量，今天写进去的数据换了实现就校验全失败。

use std::sync::Arc;

use rstore_checksum::{bitrot_hash, HASH_LEN};
use rstore_disk::DiskAPI;

use crate::error::StoreError;

/// 把一连串 block 组装成一份 bitrot 分片，**逐块追加**落盘。
///
/// 块间布局不变（仍是 `[hash(32B)][data]`），变的是落盘方式：每块 `append` 一次，
/// 而不是攒在 `Vec` 里一次写完——于是峰值内存从「整份分片」降到「一个块」。
/// 这个放大器曾被 S3 层的 8 MiB 单次 PUT 上限掩盖着，解除多限后就开到对象大小。
///
/// **为什么是 `append` 而不是反复 `write_all`**：`write_all` 的底层是
/// `File::create`（创建即截断），每块调一次会把前一块抹掉，且每次调用都返回
/// `Ok`，调用方看不出异常。见 `rstore_disk::DiskAPI::append` 的文档。
///
/// 崩溃时留下的是 `.staging-*` 里的半截文件——staging 目录本来就被发现逻辑跳过、
/// 由对账回收（DESIGN §12.3），所以**不引入新的崩溃点语义**。
pub struct BitrotShardWriter {
    disk: Arc<dyn DiskAPI>,
    rel_path: String,
    block_size: usize,
    /// 已推入一个短块：此后不允许再推入任何块。
    short_seen: bool,
    /// 已推入的原始字节数（不含摘要）。盘上文件的长度反推不出它——里面还混着摘要。
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
            short_seen: false,
            payload_len: 0,
        }
    }

    /// 追加一个 block：把 `[hash(32B)][data]` 追加到分片文件末尾。
    /// `data.len() <= block_size`；短块只能出现在末尾；空块一律拒绝。
    ///
    /// 校验**全部在落盘之前**做完：中途返回错误时盘上已有前几块，但那是暂存目录里的
    /// 半截分片、由对账回收；**绝不能**在校验通过前写入，否则「一个坏块」会变成
    /// 「一份内容不可信的分片」。
    pub async fn push_block(&mut self, data: &[u8]) -> Result<(), StoreError> {
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

        // 摘要与数据**一起**追加：分两次 `append` 会让读侧在中间看到半条记录，
        // 崩溃恰好发生在那时就会留下一个「长度合法、内容错位」的分片。
        let mut framed = Vec::with_capacity(HASH_LEN + data.len());
        framed.extend_from_slice(&bitrot_hash(data));
        framed.extend_from_slice(data);
        self.disk.append(&self.rel_path, &framed).await?;

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

    /// 收尾：必要时落一个空文件，再 fsync 文件与父目录。
    /// 逐块 `append` 都不 fsync，耐久性在这里一次性兑现；
    /// 顺序不可颠倒（文件 → 父目录），否则崩溃后文件内容在盘上、目录里却没有它。
    pub async fn finish(self) -> Result<(), StoreError> {
        // 一个块都没推过时，盘上还不存在这个文件——逐块 `append` 只有在真的写了
        // 字节时才会创建它。但**空分片在读侧是合法的**：`bitrot_size(0, bs) == 0`，
        // 而读取器是靠 `stat` 判存在性的，文件缺失会被报成 `NotFound`
        // （参与 quorum 时计为「缺失」，是另一个语义）。
        // 缓冲版的实现靠 `write_all` 的「创建即截断」顺手落下了这个 0 字节文件，
        // 改成逐块追加后这个副作用没了，必须显式补回来。
        // 这里用 `write_all` 而不是 `append(&[])`：前者会把已存在的路径清空，
        // 与「这份分片就是空的」严格一致；`append` 遇到残留内容会留下垃圾。
        if self.payload_len == 0 {
            self.disk.write_all(&self.rel_path, &[]).await?;
        }
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
        w.push_block(&[7u8; 1024]).await.unwrap();
        w.push_block(&[9u8; 476]).await.unwrap();
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
                w.push_block(chunk).await.unwrap();
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
    /// 逐块落盘的核心断言：推入一个块之后，盘上就该有它——不必等 `finish`。
    ///
    /// 没有这条，「把整份分片攒在内存里」的实现照样能通过其余所有测试
    /// （布局、长度、`bitrot_size` 一致性全都对），而那正是本次要拆掉的放大器。
    #[tokio::test]
    async fn each_block_reaches_disk_before_finish() {
        let (tmp, disk) = temp_disk();
        let path = tmp.path().join("part.1");

        let mut w = BitrotShardWriter::new(Arc::clone(&disk), "part.1".into(), 1024);
        assert!(
            !path.exists(),
            "构造写入器本身不该创建文件：没有数据就没有分片"
        );

        w.push_block(&[1u8; 1024]).await.unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            1024 + HASH_LEN as u64,
            "第一个块必须在 push_block 返回时就已在盘上"
        );
        assert_eq!(w.payload_len(), 1024, "payload_len 只算原始字节，不含摘要");

        w.push_block(&[2u8; 1024]).await.unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            2 * (1024 + HASH_LEN as u64),
            "第二个块追加在第一个之后，而不是把它覆盖掉"
        );

        w.finish().await.unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            2 * (1024 + HASH_LEN as u64)
        );
    }

    #[tokio::test]
    async fn rejects_misuse_that_would_desync_the_reader() {
        let (_tmp, disk) = temp_disk();
        let mut w = BitrotShardWriter::new(disk, "part.1".into(), 1024);
        w.push_block(&[1u8; 100]).await.unwrap(); // 短块：可以，但必须是最后一块
        assert!(matches!(
            w.push_block(&[2u8; 100]).await,
            Err(StoreError::ShardLayout(_))
        ));

        let (_tmp, disk) = temp_disk();
        let mut w = BitrotShardWriter::new(disk, "part.1".into(), 1024);
        // 超长块任何时候都不合法；空块也无意义（`bitrot_size` 不会为 0 字节产生块），
        // 多出来的那 32 字节摘要没有对应的数据，读侧会把它当成一个空块。
        assert!(matches!(
            w.push_block(&[3u8; 1025]).await,
            Err(StoreError::ShardLayout(_))
        ));
        assert!(matches!(
            w.push_block(&[]).await,
            Err(StoreError::ShardLayout(_))
        ));
    }
}
