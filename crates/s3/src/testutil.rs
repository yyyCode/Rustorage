//! `rstore-s3` 各测试模块共用的夹具。`#[cfg(test)]` 门控（见 `lib.rs`）。
//!
//! 从 `impl_s3.rs` 的 `mod tests` 搬出来，因为 `iam.rs` 的权限矩阵测试
//! 也要用同一套签名辅助——那些辅助函数是「用被测代码**之外**的一条路径
//! 生成请求」的关键，复制第二份就等于放弃了对校验的测试。

use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, StatusCode};
use http_body_util::{BodyExt, Full};
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use s3s_sigv4::{AmzDate, Payload};
use tower::ServiceExt;

use rstore_api::{
    ApiError, ByteRange, ObjectData, ObjectEntry, ObjectInfo, ObjectStore, PutRequest,
};

use crate::impl_s3::RstoreFs;

pub(crate) const ACCESS_KEY: &str = "testkey";
pub(crate) const SECRET_KEY: &str = "testsecret";
pub(crate) const REGION: &str = "us-east-1";
pub(crate) const HOST: &str = "s3.example.com";

/// 一个什么都没做的 `ObjectStore`：认证/授权测试只用它，不测业务。
///
/// 桩方法**一律写出来**，不用 `todo!()` / `unimplemented!()`：panic 的桩会污染
/// 测试输出，而且一个 panic 的桩会让「测试绿了」这件事失去意义。
pub(crate) struct NopStore;

#[async_trait::async_trait]
impl ObjectStore for NopStore {
    async fn create_bucket(&self, _bucket: &str) -> Result<(), ApiError> {
        Err(ApiError::Internal("nop".into()))
    }
    async fn delete_bucket(&self, _bucket: &str) -> Result<(), ApiError> {
        Err(ApiError::Internal("nop".into()))
    }
    async fn head_bucket(&self, _bucket: &str) -> Result<(), ApiError> {
        Err(ApiError::Internal("nop".into()))
    }
    async fn list_buckets(&self) -> Result<Vec<String>, ApiError> {
        Ok(Vec::new())
    }
    async fn put_object(&self, _req: PutRequest) -> Result<ObjectInfo, ApiError> {
        Err(ApiError::Internal("nop".into()))
    }
    async fn get_object(
        &self,
        _bucket: &str,
        _key: &str,
        _range: Option<ByteRange>,
    ) -> Result<ObjectData, ApiError> {
        Err(ApiError::Internal("nop".into()))
    }
    async fn head_object(&self, _bucket: &str, _key: &str) -> Result<ObjectInfo, ApiError> {
        Err(ApiError::Internal("nop".into()))
    }
    async fn delete_object(&self, _bucket: &str, _key: &str) -> Result<(), ApiError> {
        Err(ApiError::Internal("nop".into()))
    }
    async fn list_objects(
        &self,
        _bucket: &str,
        _prefix: Option<&str>,
    ) -> Result<Vec<ObjectEntry>, ApiError> {
        Err(ApiError::Internal("nop".into()))
    }
}

/// 认证测试用的 service：`NopStore` + 单对凭据。
///
/// **本任务（搬运）保持 `SimpleAuth` 不变**——`SimpleAuth` 与
/// `s3s::access::default_check` 的组合就是改动前的既有行为。
/// 换成 IAM 适配器是下一个任务的事，那时 `service()` 会一起改。
pub(crate) fn service() -> s3s::service::S3Service {
    let mut b = S3ServiceBuilder::new(RstoreFs {
        store: Arc::new(NopStore),
    });
    b.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
    b.build()
}

pub(crate) type TestBody = Full<Bytes>;

/// 打一个请求进**指定的** service，返回 (状态码, 响应头, 响应体字节)。零端口、零等待。
pub(crate) async fn call_on(
    service: s3s::service::S3Service,
    req: http::Request<TestBody>,
) -> (StatusCode, HeaderMap, Bytes) {
    let resp = service.oneshot(req).await.expect("service call failed");
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body failed")
        .to_bytes();
    (status, headers, bytes)
}

/// 打一个请求进默认的认证 service。
pub(crate) async fn call(req: http::Request<TestBody>) -> (StatusCode, HeaderMap, Bytes) {
    call_on(service(), req).await
}

/// 构造一个（通常不签名的）请求。`path` 用 origin-form，如 `/test-bucket`。
pub(crate) fn request(method: &str, path: &str, body: &[u8]) -> http::Request<TestBody> {
    http::Request::builder()
        .method(method)
        .uri(path)
        .header("host", HOST)
        .body(Full::new(Bytes::copy_from_slice(body)))
        .expect("build request")
}

/// 解析错误响应体的 `<Code>`，例如 `"SignatureDoesNotMatch"`。
pub(crate) fn error_code(body: &[u8]) -> String {
    let xml = std::str::from_utf8(body).expect("error body is utf-8 xml");
    let start = xml.find("<Code>").expect("no <Code> in error body") + "<Code>".len();
    let end = start
        + xml[start..]
            .find("</Code>")
            .expect("no </Code> in error body");
    xml[start..end].to_string()
}

/// 用 `s3s` 自带的 SigV4 工具签一个请求，`access_key` 与 `secret` 都显式给出。
///
/// 签名实现来自 `s3s-sigv4`（`s3s` 的直接依赖），**不是**自己另写一份：
/// 用与被测代码不同一条路径的签名实现来生成请求，才是真的在测「服务端的校验」。
///
/// `host` 会同时进 URI 的 authority、`Host` 头和被签的 `SignedHeaders`——
/// 三者必须一致，否则服务端校验时按声明的顺序读到的值对不上签名。
///
/// `access_key` 化成参数是为了权限矩阵测试能签成**非 root 用户**；
/// 它同时出现在 `Credential=` 里，所以服务端才会按那个身份去查密钥。
pub(crate) fn signed_request_as(
    access_key: &str,
    secret: &str,
    method: &str,
    path: &str,
    host: &str,
    body: &[u8],
) -> http::Request<TestBody> {
    let date = jiff::Timestamp::now()
        .strftime("%Y%m%dT%H%M%SZ")
        .to_string();
    let amz_date = AmzDate::parse(&date).expect("valid amz date");
    let payload_hash = sha256_hex(body);

    // 顺序必须与 `SignedHeaders` 完全一致：s3s 校验时按声明的顺序读头、不做排序。
    let signed_headers = [
        ("host", host),
        ("x-amz-content-sha256", payload_hash.as_str()),
        ("x-amz-date", date.as_str()),
    ];
    let query: &[(String, String)] = &[];
    let canonical = s3s_sigv4::create_canonical_request(
        method,
        path,
        query,
        signed_headers,
        Payload::SingleChunk(&payload_hash),
    );
    let string_to_sign = s3s_sigv4::create_string_to_sign(&canonical, &amz_date, REGION, "s3");
    let signature =
        s3s_sigv4::calculate_signature(&string_to_sign, secret, &amz_date, REGION, "s3");

    // Credential 范围里的日期是 `YYYYMMDD`，不是完整的 ISO8601 时间戳。
    let scope_date = &date[..8];
    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={access_key}/{scope_date}/{REGION}/s3/aws4_request, \
         SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
    );

    http::Request::builder()
        .method(method)
        .uri(format!("http://{host}{path}"))
        .header("host", host)
        .header("x-amz-content-sha256", &payload_hash)
        .header("x-amz-date", &date)
        .header("authorization", authorization)
        .body(Full::new(Bytes::copy_from_slice(body)))
        .expect("build request")
}

/// 用正式密钥签一个请求。
pub(crate) fn signed_request(
    method: &str,
    path: &str,
    host: &str,
    body: &[u8],
) -> http::Request<TestBody> {
    signed_request_as(ACCESS_KEY, SECRET_KEY, method, path, host, body)
}

/// 认证测试要故意用**错误的** secret 签名，所以保留这个可传 secret 的薄封装。
pub(crate) fn signed_list_buckets(secret: &str) -> http::Request<TestBody> {
    signed_request_as(ACCESS_KEY, secret, "GET", "/", HOST, b"")
}

/// 请求体的 `x-amz-content-sha256`。空体直接用 `s3s-sigv4` 提供的常量。
pub(crate) fn sha256_hex(body: &[u8]) -> String {
    if body.is_empty() {
        return s3s_sigv4::EMPTY_STRING_SHA256_HASH.to_string();
    }
    use sha2::Digest;
    let digest = sha2::Sha256::digest(body);
    // 手写十六进制：`digest 0.11` 的输出类型是否实现 `LowerHex` 不确定。
    digest.iter().fold(String::with_capacity(64), |mut s, b| {
        use std::fmt::Write;
        let _ = write!(s, "{:02x}", *b);
        s
    })
}
