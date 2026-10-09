//! `ApiError` → S3 错误的映射表。
//!
//! 这张表是**唯一**的映射点：`impl_s3.rs` 里不允许再散落手搓
//! `s3_error!(NoSuchBucket)` 之类的映射，否则「同一个错误码两处实现、改一处漏一处」
//! 上会翻车（Task 5.8 会核对前面的实现确实走了这张表）。
//!
//! 本 Task（5.2）只填了桶操作会用到的三条；5.3~5.6 各自补自己用到的行，
//! Task 5.8 补齐成完整表并加上覆盖全部变体的 `assert_code` 矩阵测试。

use rstore_api::ApiError;
use s3s::S3Error;

/// `ApiError` → S3 错误。
///
/// 刻意写成「一行一个变体」的形状，方便 5.3~5.6 逐行插入。`_` 兜底把尚未映射的
/// 变体一律变成 `500 InternalError`——它不会静默变成别的错误码，Task 5.8 的
/// `assert_code` 矩阵会把漏映射抓出来。
pub(crate) fn to_s3_error(err: ApiError) -> S3Error {
    match err {
        ApiError::NoSuchBucket => s3s::s3_error!(NoSuchBucket),
        // 对象读写（5.3）：GET / HEAD 一个不存在的 key → 404 NoSuchKey。
        ApiError::NoSuchKey => s3s::s3_error!(NoSuchKey),
        ApiError::BucketNotEmpty => s3s::s3_error!(BucketNotEmpty),
        ApiError::Internal(msg) => s3s::s3_error!(InternalError, "internal error: {msg}"),
        // 5.3~5.6 按需在此前插入各自用到的变体；5.8 收敛成完整表。
        other => s3s::s3_error!(InternalError, "unmapped api error: {other}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 断言 `ApiError` 映射到给定的 S3 错误码与 HTTP 状态码。
    ///
    /// Task 5.8 会用它扩成覆盖全部九个变体的矩阵；5.2 先用它钉住本任务用到的三条。
    fn assert_code(err: ApiError, code: &str, status: u16) {
        let mapped = to_s3_error(err);
        assert_eq!(mapped.code().as_str(), code);
        assert_eq!(mapped.status_code().map(|s| s.as_u16()), Some(status));
    }

    #[test]
    fn bucket_operation_errors_map_to_their_s3_codes() {
        assert_code(ApiError::NoSuchBucket, "NoSuchBucket", 404);
        assert_code(ApiError::BucketNotEmpty, "BucketNotEmpty", 409);
        assert_code(ApiError::Internal("boom".into()), "InternalError", 500);
    }

    #[test]
    fn object_operation_errors_map_to_their_s3_codes() {
        assert_code(ApiError::NoSuchKey, "NoSuchKey", 404);
    }
}
