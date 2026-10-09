//! 错误模型。

/// 管道（pipeline）相关错误。随实现推进逐步扩充。
#[derive(Debug, thiserror::Error)]
pub enum PipeError {
    /// 分片数不在合法范围 `2..=16` 内。
    #[error("invalid shard count: {0} (valid range is 2..=16)")]
    InvalidShardCount(u8),
}
