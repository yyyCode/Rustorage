//! `impl S3 for RstoreFs`。
//!
//! `RstoreFs` **只持有 `Arc<dyn ObjectStore>`**——它的 `Cargo.toml` 永远不需要
//! `rstore-store`（allowlist 里 `rstore-s3` 只有 `rstore-common` 与 `rstore-api`），
//! 于是它可以拿一个 mock `ObjectStore` 单独测试。

use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use futures::TryStreamExt as _;
use http::StatusCode;
use rstore_api::{ApiError, ObjectStore};
use s3s::dto::*;
use s3s::{S3Error, S3Request, S3Response, S3Result, S3};

/// S3 门面。构造参数是 trait 对象，不是引擎类型——s3 层看不到 `ErasureSet`。
///
/// 凭证不走这个结构体：它是 `S3ServiceBuilder` 的一个参数
/// （`SimpleAuth::from_single`，见 `Task 5.1` Step 3）。
#[derive(Clone)]
pub struct RstoreFs {
    /// 不是 `Arc<ECStore>`——s3 看不到引擎类型（allowlist 里没有那条边）。
    pub store: Arc<dyn ObjectStore>,
}

#[async_trait::async_trait]
impl S3 for RstoreFs {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        self.store
            .create_bucket(&req.input.bucket)
            .await
            .map_err(to_s3_error)?;
        // location 留 None：MVP 没有 region 概念，返回 <Location></Location> 即可。
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.store
            .delete_bucket(&req.input.bucket)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.store
            .head_bucket(&req.input.bucket)
            .await
            .map_err(to_s3_error)?;
        // 四个字段全部留 None——MVP 不实现 region，`aws s3 mb` 与 `mc` 都不依赖它。
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let names = self.store.list_buckets().await.map_err(to_s3_error)?;
        let buckets = names
            .into_iter()
            // `creation_date: None` 是有意的：桶的创建时间在设计里根本没存
            // （`.rstore.sys/bucket.meta` 的内容就是 `{}`），补假时间戳会骗客户端。
            .map(|name| Bucket {
                name: Some(name),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            ..Default::default()
        }))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        let data = match req.input.body {
            // `Bytes` 不实现 `Extend<u8>`，`try_concat()` 在这里用不了；
            // 先收集成 `Vec<Bytes>` 再拼接。
            Some(body) => body
                .try_collect::<Vec<Bytes>>()
                .await
                .map_err(|e| s3s::s3_error!(InternalError, "failed to read request body: {e}"))?
                .concat()
                .to_vec(),
            // 空对象是合法的 PUT（`touch` 一个 0 字节文件）：body 为 None 就是空。
            None => Vec::new(),
        };
        let info = self
            .store
            .put_object(&req.input.bucket, &req.input.key, data)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(info.etag)),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        // Range 解析/裁剪是 Task 5.4 的职责，这里先整份返回。
        let out = self
            .store
            .get_object(&req.input.bucket, &req.input.key, None)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(GetObjectOutput {
            body: Some(StreamingBlob::from_bytes(Bytes::from(out.data))),
            content_length: Some(out.size as i64),
            e_tag: Some(ETag::Strong(out.etag)),
            last_modified: Some(timestamp_of(out.mod_time)),
            accept_ranges: Some("bytes".to_string()),
            // content_range 留 None：序列化器只在它是 Some 时才设 206。
            ..Default::default()
        }))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        // 与 get_object 同形，但调 `head_object`——否则 HEAD 会把整份对象读进内存再丢掉。
        let info = self
            .store
            .head_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(HeadObjectOutput {
            content_length: Some(info.size as i64),
            e_tag: Some(ETag::Strong(info.etag)),
            last_modified: Some(timestamp_of(info.mod_time)),
            accept_ranges: Some("bytes".to_string()),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.store
            .delete_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::with_status(
            DeleteObjectOutput::default(),
            StatusCode::NO_CONTENT,
        ))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let entries = self
            .store
            .list_objects(&req.input.bucket, req.input.prefix.as_deref())
            .await
            .map_err(to_s3_error)?;
        // delimiter / max-keys / continuation-token 的翻页是 Task 5.5 的职责。
        let contents = entries
            .into_iter()
            .map(|e| Object {
                key: Some(e.key),
                size: Some(e.size as i64),
                e_tag: Some(ETag::Strong(e.etag)),
                last_modified: Some(timestamp_of(e.mod_time)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(req.input.bucket),
            prefix: req.input.prefix,
            contents: Some(contents),
            ..Default::default()
        }))
    }
}

/// `mod_time`（Unix 纳秒）→ `Timestamp`。
fn timestamp_of(mod_time_nanos: u64) -> Timestamp {
    let system_time = UNIX_EPOCH + Duration::from_nanos(mod_time_nanos);
    Timestamp::from(system_time)
}

/// `ApiError` → S3 错误。
///
/// TODO(Task 5.2/5.8): 这段目前是 `impl_s3.rs` 里的私有函数；`errors.rs` 建好后
/// 应整张表挪过去，并由 5.8 的 `assert_code` 矩阵把它钉死。
fn to_s3_error(err: ApiError) -> S3Error {
    match err {
        ApiError::NoSuchKey => s3s::s3_error!(NoSuchKey),
        ApiError::NoSuchBucket => s3s::s3_error!(NoSuchBucket),
        ApiError::BucketNotEmpty => s3s::s3_error!(BucketNotEmpty),
        ApiError::InvalidBucketName => s3s::s3_error!(InvalidBucketName),
        ApiError::InvalidObjectName => s3s::s3_error!(InvalidArgument, "{}", err),
        ApiError::InvalidRange => s3s::s3_error!(InvalidRange),
        ApiError::NotImplemented => s3s::s3_error!(NotImplemented),
        ApiError::Unavailable => s3s::s3_error!(ServiceUnavailable),
        ApiError::Internal(msg) => s3s::s3_error!(InternalError, "internal error: {msg}"),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use http::{HeaderMap, StatusCode};
    use http_body_util::{BodyExt, Full};
    use s3s::auth::SimpleAuth;
    use s3s::service::S3ServiceBuilder;
    use s3s_sigv4::{AmzDate, Payload};
    use tower::ServiceExt;

    use rstore_api::{ApiError, ByteRange, ObjectData, ObjectEntry, ObjectInfo, ObjectStore};

    use super::*;

    const ACCESS_KEY: &str = "testkey";
    const SECRET_KEY: &str = "testsecret";
    const REGION: &str = "us-east-1";
    const HOST: &str = "s3.example.com";

    /// 一个什么都没做的 `ObjectStore`：本节只测认证，不测业务。
    ///
    /// 桩方法**一律写出来**，不用 `todo!()` / `unimplemented!()`：panic 的桩会污染
    /// 测试输出，而且一个 panic 的桩会让「测试绿了」这件事失去意义。
    struct NopStore;

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
        async fn put_object(
            &self,
            _bucket: &str,
            _key: &str,
            _data: Vec<u8>,
        ) -> Result<ObjectInfo, ApiError> {
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

    fn service() -> s3s::service::S3Service {
        let mut b = S3ServiceBuilder::new(RstoreFs {
            store: Arc::new(NopStore),
        });
        b.set_auth(SimpleAuth::from_single(ACCESS_KEY, SECRET_KEY));
        b.build()
    }

    type TestBody = Full<Bytes>;

    /// 打一个请求进去，返回 (状态码, 响应头, 响应体字节)。零端口、零等待。
    async fn call(req: http::Request<TestBody>) -> (StatusCode, HeaderMap, Bytes) {
        let resp = service().oneshot(req).await.expect("service call failed");
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

    /// 解析错误响应体的 `<Code>`，例如 `"SignatureDoesNotMatch"`。
    fn error_code(body: &[u8]) -> String {
        let xml = std::str::from_utf8(body).expect("error body is utf-8 xml");
        let start = xml.find("<Code>").expect("no <Code> in error body") + "<Code>".len();
        let end = start
            + xml[start..]
                .find("</Code>")
                .expect("no </Code> in error body");
        xml[start..end].to_string()
    }

    /// 用 `s3s` 自带的 SigV4 工具签一个 ListBuckets（`GET /`）请求。
    ///
    /// 签名实现来自 `s3s-sigv4`（`s3s` 的直接依赖），**不是**自己另写一份：
    /// 用与被测代码不同一条路径的签名实现来生成请求，才是真的在测「服务端的校验」。
    fn signed_list_buckets(secret: &str) -> http::Request<TestBody> {
        let date = jiff::Timestamp::now()
            .strftime("%Y%m%dT%H%M%SZ")
            .to_string();
        let amz_date = AmzDate::parse(&date).expect("valid amz date");
        let payload_hash = s3s_sigv4::EMPTY_STRING_SHA256_HASH;

        // 顺序必须与 `SignedHeaders` 完全一致：s3s 校验时按声明的顺序读头、不做排序。
        let signed_headers = [
            ("host", HOST),
            ("x-amz-content-sha256", payload_hash),
            ("x-amz-date", date.as_str()),
        ];
        let query: &[(String, String)] = &[];
        let canonical = s3s_sigv4::create_canonical_request(
            "GET",
            "/",
            query,
            signed_headers,
            Payload::SingleChunk(payload_hash),
        );
        let string_to_sign = s3s_sigv4::create_string_to_sign(&canonical, &amz_date, REGION, "s3");
        let signature =
            s3s_sigv4::calculate_signature(&string_to_sign, secret, &amz_date, REGION, "s3");

        // Credential 范围里的日期是 `YYYYMMDD`，不是完整的 ISO8601 时间戳。
        let scope_date = &date[..8];
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={ACCESS_KEY}/{scope_date}/{REGION}/s3/aws4_request, \
             SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature={signature}"
        );

        http::Request::builder()
            .method("GET")
            .uri("http://s3.example.com/")
            .header("host", HOST)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", &date)
            .header("authorization", authorization)
            .body(Full::new(Bytes::new()))
            .expect("build request")
    }

    #[tokio::test]
    async fn rejects_bad_signature() {
        let (status, _headers, body) = call(signed_list_buckets("wrong-secret")).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(error_code(&body), "SignatureDoesNotMatch");
    }

    #[tokio::test]
    async fn accepts_valid_sigv4() {
        let (status, _headers, body) = call(signed_list_buckets(SECRET_KEY)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        let xml = std::str::from_utf8(&body).expect("xml body");
        assert!(xml.contains("ListAllMyBucketsResult"), "xml: {xml}");
    }
}
