//! `impl S3 for RstoreFs`。
//!
//! `RstoreFs` **只持有 `Arc<dyn ObjectStore>`**——它的 `Cargo.toml` 永远不需要
//! `rstore-store`（allowlist 里 `rstore-s3` 只有 `rstore-common` 与 `rstore-api`），
//! 于是它可以拿一个 mock `ObjectStore` 单独测试。

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use futures::TryStreamExt as _;
use http::StatusCode;
use rstore_api::{ApiError, ByteRange, ObjectStore};
use s3s::dto::*;
use s3s::{S3Request, S3Response, S3Result, S3};

use crate::errors::to_s3_error;

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
        // 闭合 Range 需要对象长度，先 head 拿 size 再带 range get——MVP 不流式，
        // 两次调用可接受，不必为此改 `ObjectStore` trait。
        let info = self
            .store
            .head_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(to_s3_error)?;
        let resolved = match req.input.range {
            Some(r) => Some(match resolve_range(r, info.size) {
                Ok(br) => br,
                Err(ApiError::InvalidRange) => {
                    // 416 的头只有调用点能补（它手里有 info.size）；`resolve_range`
                    // 保持纯函数，不掺 HTTP 头。
                    let mut err = s3s::s3_error!(InvalidRange);
                    let mut headers = http::HeaderMap::new();
                    headers.insert(
                        "content-range",
                        format!("bytes */{}", info.size).parse().expect("ascii"),
                    );
                    err.set_headers(headers);
                    return Err(err);
                }
                Err(e) => return Err(to_s3_error(e)),
            }),
            // 无 Range 时必须是 None：硬凑成 0..size-1 会让 content_range 变成 Some，
            // 于是 s3s 把普通 GET 悄悄设成 206，破坏「无 Range → 200」的既有契约。
            None => None,
        };
        let out = self
            .store
            .get_object(&req.input.bucket, &req.input.key, resolved)
            .await
            .map_err(to_s3_error)?;
        // 有 Range 时 Content-Length 是切片长度，无 Range 时才是整份长度——
        // 写错会让客户端对着短 body 等满整份长度，表现为下载卡住。
        let content_length = match resolved {
            Some(br) => (br.end - br.start + 1) as i64,
            None => out.size as i64,
        };
        Ok(S3Response::new(GetObjectOutput {
            body: Some(StreamingBlob::from_bytes(Bytes::from(out.data))),
            content_length: Some(content_length),
            e_tag: Some(ETag::Strong(out.etag)),
            last_modified: Some(timestamp_of(out.mod_time)),
            accept_ranges: Some("bytes".to_string()),
            // 分母是整份对象长度 `out.size`，不是请求范围的；写错 rclone 会以为被截断。
            content_range: resolved.map(|br| format!("bytes {}-{}/{}", br.start, br.end, out.size)),
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
        // PERF: 见 DESIGN §1.2 与 §20 Phase 2 — 命名空间索引。
        // MVP 靠 `list_objects` 的全盘遍历（4.11）+ 本层过滤/分页，不是终态设计。
        let entries = self
            .store
            .list_objects(&req.input.bucket, req.input.prefix.as_deref())
            .await
            .map_err(to_s3_error)?;

        let prefix = req.input.prefix.as_deref().unwrap_or("");
        // 空字符串的 delimiter 等同没给（否则每个 key 都在开头「命中」空串）。
        let delimiter = req.input.delimiter.as_deref().filter(|d| !d.is_empty());
        // `max-keys` 缺省 1000（S3 默认页大小）；负数按 0 处理——「不要更多」。
        let max_keys = req.input.max_keys.unwrap_or(1000).max(0) as usize;
        // 续传起点：**不解析 token 的内容，就当它是「上一个 key」**。S3 不规定 token
        // 的形状，裸 key 是最小实现（少一个 base64 依赖与一次编解码来回）；代价是
        // token 里能看到对象名，但本 API 本就把对象名给同一个调用方看，不算泄露。
        let start_after = req.input
            .continuation_token
            .clone()
            .or_else(|| req.input.start_after.clone());

        let mut contents: Vec<Object> = Vec::new();
        // `BTreeSet` 顺带保证共同前缀有序且天然去重（同一目录只出一次）。
        let mut common_prefixes: BTreeSet<String> = BTreeSet::new();
        let mut truncated = false;
        // 「最后一条**返回过的**条目」的 key，作为下一页的游标。共同前缀也能当游标：
        // `cp = "dir/"` 是 `"dir/x"` 的前缀，`key <= "dir/"` 恰好跳过整个 dir/ 子树。
        let mut last_key: Option<String> = None;

        for entry in entries {
            // 游标必须最先应用：被跳过的条目不该占 max_keys 的额度，否则第二页会比
            // 第一页短（且只在游标落在前缀内部时暴露）。字符串比较，不是下标。
            if let Some(s) = &start_after {
                if entry.key <= *s {
                    continue;
                }
            }
            // **先判容量，再处理本条**：max_keys=0 时在这里就 `break`，不会漏进循环体；
            // 于是 `is_truncated` 恰好等于「循环因容量而中断」，不必事后猜还有没有剩余。
            if contents.len() + common_prefixes.len() >= max_keys {
                truncated = true;
                break;
            }
            let rel = &entry.key[prefix.len()..];
            match delimiter.and_then(|d| rel.find(d)) {
                Some(rel_idx) => {
                    // 下标是相对 `key` 全串的：相对切片的 rel_idx 必须加回 prefix.len()，
                    // 否则前缀被吃掉，客户端按 "sub/" 列举一条都拿不到。
                    let cp = entry.key[..prefix.len() + rel_idx + 1].to_string();
                    common_prefixes.insert(cp.clone());
                    last_key = Some(cp);
                }
                None => {
                    contents.push(Object {
                        key: Some(entry.key.clone()),
                        size: Some(entry.size as i64),
                        e_tag: Some(ETag::Strong(entry.etag)),
                        last_modified: Some(timestamp_of(entry.mod_time)),
                        storage_class: Some(ObjectStorageClass::from_static("STANDARD")),
                        ..Default::default()
                    });
                    last_key = Some(entry.key);
                }
            }
        }

        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(req.input.bucket),
            prefix: req.input.prefix,
            // 回显 max_keys 时用**生效值**：缺省时 1000，负数按 0。
            max_keys: Some(max_keys as i32),
            // S3 的 KeyCount 定义就是 contents 与 common prefixes 之和。
            key_count: Some((contents.len() + common_prefixes.len()) as i32),
            continuation_token: req.input.continuation_token,
            // 这两个是 `Option`，`None` 会渲染成**缺失元素**，而客户端当必填读；
            // 桶空时也必须是 `Some(false)`。
            is_truncated: Some(truncated),
            next_continuation_token: truncated.then_some(last_key).flatten(),
            contents: Some(contents),
            common_prefixes: Some(
                common_prefixes
                    .into_iter()
                    .map(|p| CommonPrefix { prefix: Some(p) })
                    .collect(),
            ),
            delimiter: req.input.delimiter,
            // encoding-type 刻意不做，两边都不做：请求 url 编码时，响应得把每个 key /
            // prefix / delimiter / common-prefix 都百分号编码并回显 <EncodingType>url。
            // MVP 既不编码也不回显（None），返回原始 key——客户端按**响应里**的
            // EncodingType 决定要不要解码，我们回 None，它们就不解。只回显不编码才是坏的
            // （客户端会把 "a b" 当 "a%20b" 去解）。代价是拿不到想要的编码结果，但不产生错数据。
            ..Default::default()
        }))
    }
}

/// `mod_time`（Unix 纳秒）→ `Timestamp`。
fn timestamp_of(mod_time_nanos: u64) -> Timestamp {
    let system_time = UNIX_EPOCH + Duration::from_nanos(mod_time_nanos);
    Timestamp::from(system_time)
}

/// 把 s3s 解析好的 `Range` 收敛成真实对象的闭区间。越界 → `ApiError::InvalidRange`。
///
/// s3s 只做 `bytes=` 的语法解析，不知道对象多大，所以闭合区间与越界检查在这一层。
/// 保持纯函数：416 响应头需要 `info.size`，由调用点补，这里不掺 HTTP 头。
fn resolve_range(r: Range, size: u64) -> Result<ByteRange, ApiError> {
    match r {
        Range::Int { first, last } => {
            if first >= size {
                return Err(ApiError::InvalidRange);
            }
            let end = last.unwrap_or(size - 1).min(size - 1);
            if end < first {
                return Err(ApiError::InvalidRange);
            }
            Ok(ByteRange { start: first, end })
        }
        Range::Suffix { length } => {
            if length == 0 || size == 0 {
                return Err(ApiError::InvalidRange);
            }
            let start = size.saturating_sub(length);
            Ok(ByteRange { start, end: size - 1 })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
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
    use crate::mock::MockStore;

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

    /// 打一个请求进**指定的** service，返回 (状态码, 响应头, 响应体字节)。零端口、零等待。
    async fn call_on(
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

    /// 打一个请求进 5.1 的认证 service（`NopStore` + `SimpleAuth`）。
    async fn call(req: http::Request<TestBody>) -> (StatusCode, HeaderMap, Bytes) {
        call_on(service(), req).await
    }

    /// 业务测试用的 service：挂 `MockStore`，**不设 auth**。
    ///
    /// 无 auth 时 s3s 接受匿名请求并跳过鉴权（见 `S3ServiceBuilder` 文档）——
    /// 签名的正确性已由 5.1 的 `rejects_bad_signature` / `accepts_valid_sigv4` 覆盖，
    /// 5.2 只测操作翻译，不必给每个请求再签一遍名。
    fn mock_service(store: Arc<MockStore>) -> s3s::service::S3Service {
        S3ServiceBuilder::new(RstoreFs { store }).build()
    }

    /// 构造一个（通常不签名的）请求。`path` 用 origin-form，如 `/test-bucket`。
    fn request(method: &str, path: &str, body: &[u8]) -> http::Request<TestBody> {
        http::Request::builder()
            .method(method)
            .uri(path)
            .header("host", HOST)
            .body(Full::new(Bytes::copy_from_slice(body)))
            .expect("build request")
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

    // ---- Task 5.2: 桶操作 ----

    #[tokio::test]
    async fn create_bucket_then_head_and_list() {
        let store = Arc::new(MockStore::default());

        // PUT /test-bucket → 200
        let (status, _headers, body) = call_on(
            mock_service(store.clone()),
            request("PUT", "/test-bucket", b""),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );

        // HEAD /test-bucket → 200 空体
        let (status, _headers, body) = call_on(
            mock_service(store.clone()),
            request("HEAD", "/test-bucket", b""),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(body.is_empty(), "HEAD 响应体应为空");

        // GET / → 200，XML 里 <Buckets> 含 <Name>test-bucket</Name>
        let (status, _headers, body) = call_on(mock_service(store), request("GET", "/", b"")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        let xml = std::str::from_utf8(&body).expect("xml body");
        assert!(xml.contains("<Name>test-bucket</Name>"), "xml: {xml}");
    }

    #[tokio::test]
    async fn delete_non_empty_bucket_is_409() {
        let store = Arc::new(MockStore::default());
        store
            .create_bucket("test-bucket")
            .await
            .expect("create bucket");
        store
            .put_object("test-bucket", "obj", b"data".to_vec())
            .await
            .expect("put object");

        let (status, _headers, body) =
            call_on(mock_service(store), request("DELETE", "/test-bucket", b"")).await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(error_code(&body), "BucketNotEmpty");
    }

    #[tokio::test]
    async fn head_missing_bucket_is_404_nosuchbucket() {
        let store = Arc::new(MockStore::default());

        let (status, _headers, body) =
            call_on(mock_service(store), request("HEAD", "/missing-bucket", b"")).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(error_code(&body), "NoSuchBucket");
    }

    // ---- Task 5.3: 对象读写 ----

    /// 所有对象读写用例共用的对象体，恰好 15 字节（断言 `Content-Length == 15`）。
    const OBJ_BODY: &[u8] = b"hello rustorage";

    /// 取一个响应头为 `&str`。
    fn header<'a>(headers: &'a HeaderMap, name: &str) -> &'a str {
        headers
            .get(name)
            .unwrap_or_else(|| panic!("缺 {name} 响应头"))
            .to_str()
            .unwrap_or_else(|e| panic!("{name} 应是 ASCII: {e}"))
    }

    /// 断言 `Last-Modified` 存在**且是合法的 HTTP-date**。
    ///
    /// 客户端（`rclone` / `mc`）会读这个头做增量同步；`mod_time` 为 0 或格式不对时
    /// 这条会红。用 `jiff::fmt::rfc2822` 解析——s3s 正是按 RFC 1123（RFC 2822 的
    /// HTTP-date 子集，结尾 `GMT`）序列化的。
    fn assert_valid_last_modified(headers: &HeaderMap) {
        let raw = header(headers, "last-modified");
        jiff::fmt::rfc2822::parse(raw)
            .unwrap_or_else(|e| panic!("Last-Modified 不是合法 HTTP-date: {raw:?} ({e})"));
    }

    #[tokio::test]
    async fn put_then_get_round_trips_bytes() {
        let store = Arc::new(MockStore::default());

        // PUT /test-bucket/k → 200，且响应头带 ETag
        let (status, headers, body) = call_on(
            mock_service(store.clone()),
            request("PUT", "/test-bucket/k", OBJ_BODY),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(!header(&headers, "etag").is_empty(), "PUT 应回非空 ETag");

        // GET /test-bucket/k → 200，体与长度都对
        let (status, headers, body) = call_on(
            mock_service(store),
            request("GET", "/test-bucket/k", b""),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(body.as_ref(), OBJ_BODY, "GET 体应与 PUT 进去的一致");
        assert_eq!(header(&headers, "content-length"), "15");
        assert!(!header(&headers, "etag").is_empty(), "GET 应回非空 ETag");
        // accept-ranges 是客户端发起 Range 请求的前提（Task 5.4），这里先立住。
        assert_eq!(header(&headers, "accept-ranges"), "bytes");
        assert_valid_last_modified(&headers);
    }

    #[tokio::test]
    async fn head_object_has_length_but_no_body() {
        let store = Arc::new(MockStore::default());
        let _ = call_on(
            mock_service(store.clone()),
            request("PUT", "/test-bucket/k", OBJ_BODY),
        )
        .await;

        let (status, headers, body) = call_on(
            mock_service(store),
            request("HEAD", "/test-bucket/k", b""),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(header(&headers, "content-length"), "15");
        assert!(
            body.is_empty(),
            "HEAD 响应体必须为空（否则就是把整份对象读回来再丢掉）"
        );
        assert!(!header(&headers, "etag").is_empty(), "HEAD 应回非空 ETag");
        assert_eq!(header(&headers, "accept-ranges"), "bytes");
        assert_valid_last_modified(&headers);
    }

    #[tokio::test]
    async fn get_missing_key_is_404_nosuchkey() {
        let store = Arc::new(MockStore::default());

        let (status, _headers, body) = call_on(
            mock_service(store),
            request("GET", "/test-bucket/missing", b""),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(error_code(&body), "NoSuchKey");
    }

    #[tokio::test]
    async fn delete_object_is_204_and_idempotent() {
        let store = Arc::new(MockStore::default());
        let _ = call_on(
            mock_service(store.clone()),
            request("PUT", "/test-bucket/k", OBJ_BODY),
        )
        .await;

        // 删两次：S3 的 DELETE 幂等，第二次（对象已不在）仍须 204。
        for round in 0..2 {
            let (status, _headers, body) = call_on(
                mock_service(store.clone()),
                request("DELETE", "/test-bucket/k", b""),
            )
            .await;
            assert_eq!(
                status,
                StatusCode::NO_CONTENT,
                "第 {} 次 DELETE，body: {}",
                round + 1,
                String::from_utf8_lossy(&body)
            );
            assert!(body.is_empty(), "DELETE 响应体应为空");
        }
    }

    // ---- Task 5.4: GetObject 的 HTTP Range ----

    /// Range 用例共用的对象体，26 字节（断言 `Content-Range` 的分母是 `26`）。
    const RANGE_BODY: &[u8] = b"abcdefghijklmnopqrstuvwxyz";

    /// 构造一个带 `Range` 头的 GET 请求，其余走 `request` 夹具。
    fn request_with_range(path: &str, range: &str) -> http::Request<TestBody> {
        let mut req = request("GET", path, b"");
        req.headers_mut()
            .insert("range", range.parse().expect("valid range header"));
        req
    }

    /// 存入 `RANGE_BODY` 后带 `range` 头 GET 一次。
    async fn get_range(store: Arc<MockStore>, range: &str) -> (StatusCode, HeaderMap, Bytes) {
        let _ = call_on(
            mock_service(store.clone()),
            request("PUT", "/test-bucket/k", RANGE_BODY),
        )
        .await;
        call_on(mock_service(store), request_with_range("/test-bucket/k", range)).await
    }

    #[tokio::test]
    async fn range_int_form_returns_206_with_content_range() {
        let (status, headers, body) = get_range(Arc::new(MockStore::default()), "bytes=2-5").await;
        assert_eq!(
            status,
            StatusCode::PARTIAL_CONTENT,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(body.as_ref(), b"cdef");
        assert_eq!(header(&headers, "content-range"), "bytes 2-5/26");
        assert_eq!(header(&headers, "content-length"), "4");
    }

    #[tokio::test]
    async fn range_open_ended_form() {
        let (status, headers, body) = get_range(Arc::new(MockStore::default()), "bytes=22-").await;
        assert_eq!(
            status,
            StatusCode::PARTIAL_CONTENT,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(body.as_ref(), b"wxyz");
        assert_eq!(header(&headers, "content-range"), "bytes 22-25/26");
        assert_eq!(header(&headers, "content-length"), "4");
    }

    #[tokio::test]
    async fn range_suffix_form() {
        let (status, headers, body) = get_range(Arc::new(MockStore::default()), "bytes=-4").await;
        assert_eq!(
            status,
            StatusCode::PARTIAL_CONTENT,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(body.as_ref(), b"wxyz");
        assert_eq!(header(&headers, "content-range"), "bytes 22-25/26");
        assert_eq!(header(&headers, "content-length"), "4");
    }

    #[tokio::test]
    async fn range_beyond_size_is_416() {
        let (status, headers, body) =
            get_range(Arc::new(MockStore::default()), "bytes=100-200").await;
        assert_eq!(
            status,
            StatusCode::RANGE_NOT_SATISFIABLE,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(error_code(&body), "InvalidRange");
        // RFC 9110 §15.5.17 的 SHOULD：416 应带回整份长度，客户端据此判断对象没变短。
        assert_eq!(header(&headers, "content-range"), "bytes */26");
    }

    // ---- Task 5.5: ListObjectsV2 ----

    /// 列出用例共用的 5 条 key，已按 key 升序（字节序）：顶层对象、一级子目录、
    /// 二级子目录各覆盖到。注意 `dir/sub/z` 排在 `dir/x` 之前——升序是纯字典序，
    /// 不把 `/` 当目录分隔符看待。
    const LIST_KEYS: [&str; 5] = ["a.txt", "dir/sub/z", "dir/x", "dir/y", "z.txt"];

    /// 备好 `test-bucket`，放入 `LIST_KEYS`（每条内容是它自己的 key，长度无关紧要）。
    async fn seeded_store() -> Arc<MockStore> {
        let store = Arc::new(MockStore::default());
        for key in LIST_KEYS {
            store
                .put_object("test-bucket", key, key.as_bytes().to_vec())
                .await
                .expect("put object");
        }
        store
    }

    /// 打一个 `GET /test-bucket?list-type=2[&<query>]`，返回 (状态码, 响应体)。
    async fn list(store: Arc<MockStore>, query: &str) -> (StatusCode, Bytes) {
        let path = if query.is_empty() {
            "/test-bucket?list-type=2".to_string()
        } else {
            format!("/test-bucket?list-type=2&{query}")
        };
        let (status, _headers, body) = call_on(mock_service(store), request("GET", &path, b"")).await;
        (status, body)
    }

    /// 取出所有 `<tag>...</tag>` 的内层文本（同一标签出现多次时按响应顺序全给）。
    fn blocks(body: &[u8], tag: &str) -> Vec<String> {
        let xml = std::str::from_utf8(body).expect("xml body");
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let mut out = Vec::new();
        let mut rest = xml;
        while let Some(i) = rest.find(&open) {
            let start = i + open.len();
            let end = start + rest[start..].find(&close).expect("未闭合的标签");
            out.push(rest[start..end].to_string());
            rest = &rest[end + close.len()..];
        }
        out
    }

    /// 单个标量元素的内层文本，如 `<KeyCount>5</KeyCount>` → `"5"`；缺元素即 panic。
    fn scalar(body: &[u8], tag: &str) -> String {
        blocks(body, tag)
            .into_iter()
            .next()
            .unwrap_or_else(|| panic!("响应缺少 <{tag}>"))
    }

    /// 响应里 `<Contents>` 的 key 列表（按响应顺序）。
    fn content_keys(body: &[u8]) -> Vec<String> {
        blocks(body, "Contents")
            .iter()
            .map(|c| {
                blocks(c.as_bytes(), "Key")
                    .into_iter()
                    .next()
                    .expect("Contents 缺少 Key")
            })
            .collect()
    }

    /// 响应里 `<CommonPrefixes>` 的 prefix 列表（按响应顺序）。
    fn common_prefixes(body: &[u8]) -> Vec<String> {
        blocks(body, "CommonPrefixes")
            .iter()
            .map(|c| {
                blocks(c.as_bytes(), "Prefix")
                    .into_iter()
                    .next()
                    .expect("CommonPrefixes 缺少 Prefix")
            })
            .collect()
    }

    #[tokio::test]
    async fn lists_all_sorted() {
        let (status, body) = list(seeded_store().await, "").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(content_keys(&body), LIST_KEYS);
        assert_eq!(scalar(&body, "KeyCount"), "5");
        assert_eq!(scalar(&body, "IsTruncated"), "false");
    }

    #[tokio::test]
    async fn prefix_filters() {
        let (status, body) = list(seeded_store().await, "prefix=dir/").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(content_keys(&body), ["dir/sub/z", "dir/x", "dir/y"]);
    }

    #[tokio::test]
    async fn delimiter_rolls_up_common_prefixes() {
        let (status, body) = list(seeded_store().await, "delimiter=/").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(content_keys(&body), ["a.txt", "z.txt"]);
        // `dir/` 下所有 key 归并成**一条**共同前缀，而不是 dir/x、dir/y、dir/sub 三条。
        assert_eq!(common_prefixes(&body), ["dir/"]);
        assert_eq!(scalar(&body, "KeyCount"), "3");
    }

    #[tokio::test]
    async fn prefix_and_delimiter_together_keep_the_prefix() {
        let (status, body) = list(seeded_store().await, "prefix=dir/&delimiter=/").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(content_keys(&body), ["dir/x", "dir/y"]);
        // 共同前缀必须保留请求前缀：切出 "sub/" 会让客户端按 "sub/" 列举一条都拿不到。
        assert_eq!(common_prefixes(&body), ["dir/sub/"]);
    }

    #[tokio::test]
    async fn max_keys_zero_returns_empty_and_truncated() {
        let (status, body) = list(seeded_store().await, "max-keys=0").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(content_keys(&body).is_empty(), "max-keys=0 不应返回任何对象");
        assert_eq!(scalar(&body, "KeyCount"), "0");
        // 桶里还有 5 条没返回——`is_truncated` 表示「还有没返回完的条目」。
        assert_eq!(scalar(&body, "IsTruncated"), "true");
    }

    #[tokio::test]
    async fn max_keys_truncates_and_sets_is_truncated() {
        let (status, body) = list(seeded_store().await, "max-keys=2").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(content_keys(&body), ["a.txt", "dir/sub/z"]);
        assert_eq!(scalar(&body, "KeyCount"), "2");
        assert_eq!(scalar(&body, "IsTruncated"), "true");
        assert!(
            !scalar(&body, "NextContinuationToken").is_empty(),
            "截断时应给出续传 token"
        );
    }

    #[tokio::test]
    async fn continuation_token_resumes_without_gap_or_dup() {
        let store = seeded_store().await;
        let (status, page1) = list(store.clone(), "max-keys=2").await;
        assert_eq!(status, StatusCode::OK, "body: {}", String::from_utf8_lossy(&page1));
        let token = scalar(&page1, "NextContinuationToken");

        // 第二页不设 max-keys，把剩下的全取回。
        let (status, page2) = list(store, &format!("continuation-token={token}")).await;
        assert_eq!(status, StatusCode::OK, "body: {}", String::from_utf8_lossy(&page2));

        let first = content_keys(&page1);
        let second = content_keys(&page2);
        assert_eq!(second, ["dir/x", "dir/y", "z.txt"], "第二页应无缺口地接着第一页");
        // 核心断言：并集 == 全集、交集为空。`rclone sync` 依赖它——漏一条会被当成
        // 远端文件不存在而**删除**；重一条则目录里出现重复条目。
        let mut union: BTreeSet<String> = first.iter().cloned().collect();
        union.extend(second.iter().cloned());
        let expected: BTreeSet<String> = LIST_KEYS.iter().map(|k| k.to_string()).collect();
        assert_eq!(union, expected, "两页并集必须等于全集");
        let overlap: Vec<&String> = first.iter().filter(|k| second.contains(k)).collect();
        assert!(overlap.is_empty(), "两页不应有重叠: {overlap:?}");
    }

    #[tokio::test]
    async fn exactly_full_last_page_is_not_truncated() {
        let store = Arc::new(MockStore::default());
        for key in ["a", "b", "c", "d"] {
            store
                .put_object("test-bucket", key, key.as_bytes().to_vec())
                .await
                .expect("put object");
        }
        // 4 条、max-keys=2：页一截断，用它的 token 取页二。
        let (_, page1) = list(store.clone(), "max-keys=2").await;
        let token = scalar(&page1, "NextContinuationToken");
        let (status, page2) = list(store, &format!("max-keys=2&continuation-token={token}")).await;
        assert_eq!(status, StatusCode::OK, "body: {}", String::from_utf8_lossy(&page2));
        assert_eq!(content_keys(&page2), ["c", "d"]);
        // 恰好填满且已到底：必须 false，否则客户端会再多翻一页空页（循环不收敛）。
        assert_eq!(scalar(&page2, "IsTruncated"), "false");
    }
}
