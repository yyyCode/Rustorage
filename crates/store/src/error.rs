//! store 层的错误类型。

use rstore_common::error::DiskError;

/// store 层的错误。跨盘操作的失败必须能区分「quorum 没凑够」与「盘本身报错」——
/// 前者是本次写失败，后者要按 `DiskError` 的三级分类决定是重试、标记落后还是触发 heal。
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("disk error: {0}")]
    Disk(#[from] DiskError),
    #[error("write quorum not reached: {achieved}/{required}")]
    WriteQuorum { achieved: u8, required: u8 },
    #[error("read quorum not reached: {achieved}/{required}")]
    ReadQuorum { achieved: u8, required: u8 },
    #[error("bad shard layout: {0}")]
    ShardLayout(String),
    #[error("internal error: {0}")]
    Internal(String),
}
