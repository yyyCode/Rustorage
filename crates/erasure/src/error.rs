//! 纠删码门面的错误类型。

/// 构造 [`Codec`](crate::Codec) 时的几何校验错误。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ErasureConstructionError {
    #[error("invalid geometry: data={data} parity={parity} (need 2<=data+parity<=16, parity>=1)")]
    InvalidGeometry { data: usize, parity: usize },
    #[error("shard_size must be > 0")]
    ZeroShardSize,
}

/// 编解码时发生的错误。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ErasureError {
    #[error("expected {expected} shards, got {got}")]
    WrongShardCount { expected: usize, got: usize },
    #[error("shards are not equal length")]
    UnequalShardLength,
    #[error("only {available} shards available, need {needed}")]
    TooFewShards { available: usize, needed: usize },
    #[error("codec backend error: {0}")]
    Backend(String),
}
