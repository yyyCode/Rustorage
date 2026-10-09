//! `ApiError` → S3 错误的映射表。
//!
//! 这张表是**唯一**的映射点：`impl_s3.rs` 里不允许再散落手搓
//! `s3_error!(NoSuchBucket)` 之类的映射，否则「同一个错误码两处实现、改一处漏一处」
//! 上会翻车。唯一的例外是那种「映射本身要带调用点信息」的错误（416 的
//! `Content-Range` 需要对象长度），那种只能在调用点补头。
//!
//! Task 5.8 把本表补齐成覆盖全部九个 `ApiError` 变体的完整表。

use rstore_api::ApiError;
use s3s::S3Error;

/// `ApiError` → S3 错误。
///
/// 一行一个变体，每个变体的 `(Code, HTTP status)` 唯一。`InternalError` 那两行
/// 保持 500 语义；`Unavailable` 例外——它是**暂时**故障（quorum 不足），必须回可
/// 重试的 503，绝不能混进「服务端有 bug」的 500（见该行注释）。
pub(crate) fn to_s3_error(err: ApiError) -> S3Error {
    match err {
        ApiError::NoSuchBucket => s3s::s3_error!(NoSuchBucket),
        ApiError::NoSuchKey => s3s::s3_error!(NoSuchKey),
        ApiError::BucketNotEmpty => s3s::s3_error!(BucketNotEmpty),
        ApiError::InvalidBucketName => s3s::s3_error!(InvalidBucketName),
        // 不合法的对象 key → 400。对外码必须是标准的 `InvalidArgument`——s3s 的码表里
        // **没有** `InvalidObjectName`，`s3_error!($code)` 会展开成 `S3ErrorCode::$code`
        // （一条路径，不是字符串），写它会编译不过。内部变体名保持不变。
        ApiError::InvalidObjectName => s3s::s3_error!(InvalidArgument),
        ApiError::InvalidRange => s3s::s3_error!(InvalidRange),
        ApiError::NotImplemented => s3s::s3_error!(NotImplemented),
        // quorum 不足是**暂时**的，必须回可重试的 503 `ServiceUnavailable` 并挂
        // `Retry-After`。不能回 `InternalError`：那个码在 s3s 里固定 500，语义是
        // 「服务端有 bug」，会让客户端放弃重试。
        ApiError::Unavailable => {
            let mut err = s3s::s3_error!(ServiceUnavailable);
            let mut headers = http::HeaderMap::new();
            headers.insert("retry-after", "1".parse().expect("ascii"));
            err.set_headers(headers);
            err
        }
        ApiError::Internal(msg) => s3s::s3_error!(InternalError, "internal error: {msg}"),
        // **不写兜底 `_ =>` 分支**：九个变体已全部显式覆盖，多一个兜底只会让
        // 「将来新增变体却忘了在这里加行」**静默**落进 500，而那时 `-D warnings`
        // 下的 `unreachable_patterns` 还得靠 `#[allow]` 压掉——等于亲手关掉了
        // 唯一能抓住这种遗漏的机制。不加兜底时，新增一个 `ApiError` 变体
        // 会直接**编译不过**，这才是护栏。（`ApiError` 没有 `#[non_exhaustive]`，
        // 所以穷举匹配是合法的。）
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 断言 `ApiError` 映射到给定的 S3 错误码与 HTTP 状态码。
    ///
    /// **必须带上状态码**：只断言 `<Code>` 字符串的话，「NoSuchKey 配了 500」这种错会
    /// 漏过去，而 S3 客户端是按状态码分支的（404 触发重建、503 触发重试）。
    fn assert_code(err: ApiError, code: &str, status: u16) {
        let mapped = to_s3_error(err);
        assert_eq!(mapped.code().as_str(), code);
        assert_eq!(mapped.status_code().map(|s| s.as_u16()), Some(status));
    }

    /// 覆盖全部九个 `ApiError` 变体的矩阵：每个变体的 `(Code, HTTP status)` 唯一，
    /// 任何一行被改错都会在这里红。
    #[test]
    fn maps_api_errors_to_s3_codes() {
        assert_code(ApiError::NoSuchKey, "NoSuchKey", 404);
        assert_code(ApiError::NoSuchBucket, "NoSuchBucket", 404);
        assert_code(ApiError::BucketNotEmpty, "BucketNotEmpty", 409);
        assert_code(ApiError::InvalidBucketName, "InvalidBucketName", 400);
        assert_code(ApiError::InvalidObjectName, "InvalidArgument", 400);
        assert_code(ApiError::InvalidRange, "InvalidRange", 416);
        assert_code(ApiError::NotImplemented, "NotImplemented", 501);
        assert_code(ApiError::Unavailable, "ServiceUnavailable", 503);
        assert_code(ApiError::Internal("boom".into()), "InternalError", 500);
    }

    #[test]
    fn unavailable_carries_retry_after_header() {
        // quorum 不足是暂时的，客户端要靠 `Retry-After` 决定何时重试；漏了这个头
        // 只能靠客户端自己退避，等于把服务端的已知信息丢掉。
        let mapped = to_s3_error(ApiError::Unavailable);
        let headers = mapped.headers().expect("Unavailable 应挂响应头");
        let retry_after = headers.get("retry-after").expect("缺 retry-after");
        assert_eq!(retry_after.to_str().expect("ASCII"), "1");
    }
}
