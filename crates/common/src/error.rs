//! 错误模型。

/// 管道（pipeline）相关错误。随实现推进逐步扩充。
#[derive(Debug, thiserror::Error)]
pub enum PipeError {
    /// 分片数不在合法范围 `2..=16` 内。
    #[error("invalid shard count: {0} (valid range is 2..=16)")]
    InvalidShardCount(u8),
}

/// 确定性损坏的种类。重试无意义，应触发 repair（DESIGN §17）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum CorruptKind {
    #[error("bad magic")]
    BadMagic,
    #[error("unsupported format version")]
    UnsupportedVersion,
    #[error("length mismatch")]
    LengthMismatch,
    #[error("malformed header")]
    MalformedHeader,
    #[error("crc mismatch")]
    CrcMismatch,
    #[error("bitrot checksum mismatch")]
    BitrotMismatch,
    #[error("invalid shard distribution")]
    InvalidDistribution,
}

/// 瞬时故障：重试有意义，**不计入损坏统计**（DESIGN §17）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TransientKind {
    #[error("io error")]
    Io,
    #[error("timeout")]
    Timeout,
    #[error("short read")]
    ShortRead,
}

/// 致命故障：需要人工介入，重试无意义。
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FatalKind {
    #[error("permission denied")]
    PermissionDenied,
    #[error("read-only disk")]
    ReadOnly,
    #[error("path escapes disk root")]
    PathEscape,
    #[error("no space left")]
    NoSpace,
}

/// 磁盘/存储层错误。`#[non_exhaustive]`：`Transient` / `Fatal` 两个变体
/// 在 M3 引入，届时属于非破坏性变更（DESIGN §17）。
///
/// 定义在 `common` 而不是 `disk`：meta 层也要表达「确定性损坏」，
/// 而依赖方向是 `disk → meta`，meta 不能反向依赖 disk。
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DiskError {
    /// 对象/分片不存在。参与 quorum 计数时计为「缺失」，不计为「失败」。
    #[error("not found")]
    NotFound,
    /// 确定性损坏。重试无意义，应触发 repair。
    #[error("corrupt: {0}")]
    Corrupt(CorruptKind),
    /// 瞬时故障：重试有意义，不计入损坏统计。
    #[error("transient: {0}")]
    Transient(TransientKind),
    /// 致命故障：需要人工介入，重试无意义。
    #[error("fatal: {0}")]
    Fatal(FatalKind),
}
