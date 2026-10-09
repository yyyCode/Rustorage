//! API 层错误。
//!
//! **不是** [`rstore_common`] 或 `rstore_store::StoreError` 的别名——`rstore-api` 与
//! `rstore-store` 是兄弟依赖，allowlist 里没有 `api -> store` 这条边
//! （`scripts/tests/test_check_layer_deps.py` 把它钉成 FORBIDDEN EDGE）。
//! 两者只能各定一份错误类型，再由组合根 `rstore-server` 做一次映射。

/// API 层错误。
///
/// 变体按 **S3 错误码**挑的，不是照抄 `StoreError`：每个变体在 Task 5.8 的映射表里
/// 都有唯一的 `(Code, HTTP status)`。`NoSuchKey` / `NoSuchBucket` 刻意分开而不是
/// 合并成 `NotFound`——S3 的 `HeadBucket` 要的是 `404 NoSuchBucket`、`GetObject`
/// 要的是 `404 NoSuchKey`，合并之后 5.8 还得反推「这次是哪个操作」。
#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("no such key")]
    NoSuchKey,
    #[error("no such bucket")]
    NoSuchBucket,
    #[error("bucket not empty")]
    BucketNotEmpty,
    #[error("invalid bucket name")]
    InvalidBucketName,
    #[error("invalid object name")]
    InvalidObjectName,
    #[error("invalid range")]
    InvalidRange,
    #[error("not implemented")]
    NotImplemented,
    #[error("unavailable")]
    Unavailable,
    #[error("internal: {0}")]
    Internal(String),
}
