//! `impl S3 for RstoreFs`。
//!
//! `RstoreFs` **只持有 `Arc<dyn ObjectStore>`**——它的 `Cargo.toml` 永远不需要
//! `rstore-store`（allowlist 里 `rstore-s3` 只有 `rstore-common` 与 `rstore-api`），
//! 于是它可以拿一个 mock `ObjectStore` 单独测试。

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use http::StatusCode;
use rstore_api::{ApiError, ByteRange, ObjectInfo, ObjectStore, PutRequest};
use s3s::dto::*;
use s3s::{S3Error, S3Request, S3Response, S3Result, S3};
use tokio::io::AsyncRead;
use tokio_util::io::StreamReader;

use crate::conditional::{self, Verdict};
use crate::errors::to_s3_error;
use crate::validate::validate_object_key;

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

    /// Task 5.9 冒烟发现的真实兼容缺口（非猜测）：`mc alias set` 在探测端点时先发
    /// `GET /<随机桶>?location=`（`mc --debug` 实测，桶名形如
    /// `probe-bsign-<随机串>`）。s3s 的默认实现回 `501 NotImplemented`，mc 据此判定
    /// 「别名不可用」，于是**默认参数的 `mc alias set` 直接失败**——而 Task 5.2 的注记
    /// 曾以为「`mc` 不依赖 region」。AWS / MinIO 对不存在的桶回 `NoSuchBucket`（404），
    /// mc 把非签名类错误当作「S3v4 成立」接受，所以这里按同样的语义补上。
    ///
    /// MVP 没有 region 概念：`location_constraint` 留 `None`，s3s 会序列化成
    /// us-east-1 那种**空** `<LocationConstraint xmlns="...">`（见 s3s 的 `xml::mod`）。
    async fn get_bucket_location(
        &self,
        req: S3Request<GetBucketLocationInput>,
    ) -> S3Result<S3Response<GetBucketLocationOutput>> {
        // 先确认桶存在：不存在的桶必须回 NoSuchBucket，与 head/delete 的语义一致，
        // 也和 AWS 对齐（`?location` 不是「什么都答 200」的端点）。
        self.store
            .head_bucket(&req.input.bucket)
            .await
            .map_err(to_s3_error)?;
        Ok(S3Response::new(GetBucketLocationOutput::default()))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        // 写入入口：两条命名规则都在这里挡（DESIGN §6.3 + 盘上别名）。
        validate_object_key(&req.input.key).map_err(to_s3_error)?;

        // 请求体**不再收全**：`StreamingBlob` 是一个字节流，直接转成 `AsyncRead`
        // 交给存储层逐块消费。空 body 是合法的 PUT（`touch` 一个 0 字节文件），
        // `tokio::io::empty()` 就是那个「立刻 EOF」的流。
        //
        // `StreamExt` 只在**这里**导入，不放模块级：测试模块里
        // `http_body_util::BodyExt` 也有个 `collect`，两者同时可见会变成
        // 「multiple applicable items in scope」。
        use futures::StreamExt as _;
        let body: Box<dyn AsyncRead + Unpin + Send> = match req.input.body {
            Some(blob) => Box::new(StreamReader::new(
                // `StreamingBlob` 的 Item 是 `Result<Bytes, Box<dyn Error + Send + Sync>>`，
                // 而 `StreamReader` 要求 `Result<_, io::Error>`。这层映射只是把错误
                // 换个盒子——真正的错误语义在存储层的 `read_block` 里统一成
                // `StoreError::Internal`，不会在这里丢信息。
                blob.map(|r| r.map_err(std::io::Error::other)),
            )),
            None => Box::new(tokio::io::empty()),
        };

        let info = self
            .store
            .put_object(PutRequest {
                bucket: req.input.bucket,
                key: req.input.key,
                body,
                etag: None,
            })
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
        // 读路径也必须拦：`..` 在盘层是 Fatal(FatalKind::PathEscape)，不拦会一路
        // 变成 500；而不合法的 key 是客户端的错（400），不是「服务端坏了」。
        validate_object_key(&req.input.key).map_err(to_s3_error)?;
        // 闭合 Range 需要对象长度，先 head 拿 size 再带 range get——MVP 不流式，
        // 两次调用可接受，不必为此改 `ObjectStore` trait。这次 head 同时也是条件
        // 求值的数据来源，**不要 head 两遍**。
        let info = self
            .store
            .head_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(to_s3_error)?;
        // 条件求值必须在**查到对象之后**：先求条件后查对象的话，不存在的 key 会
        // 被 `If-Match` 误判成 412，而正确答案是 404。
        match conditional::evaluate(
            conditional::Conditions {
                if_match: req.input.if_match.as_ref(),
                if_none_match: req.input.if_none_match.as_ref(),
                if_modified_since: req.input.if_modified_since.as_ref(),
                if_unmodified_since: req.input.if_unmodified_since.as_ref(),
            },
            &info,
        ) {
            // 条件错误不是 `ApiError`，映射表里没有对应变体，只能在调用点构造
            // （与 416 挂 `Content-Range` 是同一类例外）。
            Verdict::PreconditionFailed => return Err(s3s::s3_error!(PreconditionFailed)),
            // 304 无 body；响应头只回验证器 ETag / Last-Modified（见 `not_modified`）。
            Verdict::NotModified => return Err(not_modified(&info)),
            Verdict::Proceed => {}
        }
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
        validate_object_key(&req.input.key).map_err(to_s3_error)?;
        // 与 get_object 同形，但调 `head_object`——否则 HEAD 会把整份对象读进内存再丢掉。
        let info = self
            .store
            .head_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(to_s3_error)?;
        // 条件求值必须在查到对象之后（理由同 get_object）：不存在的 key 带 `If-Match`
        // 应是 404 NoSuchKey，不是 412。
        match conditional::evaluate(
            conditional::Conditions {
                if_match: req.input.if_match.as_ref(),
                if_none_match: req.input.if_none_match.as_ref(),
                if_modified_since: req.input.if_modified_since.as_ref(),
                if_unmodified_since: req.input.if_unmodified_since.as_ref(),
            },
            &info,
        ) {
            Verdict::PreconditionFailed => return Err(s3s::s3_error!(PreconditionFailed)),
            // HEAD 的 304 同样无 body；只回验证器 ETag / Last-Modified。
            Verdict::NotModified => return Err(not_modified(&info)),
            Verdict::Proceed => {}
        }
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
        validate_object_key(&req.input.key).map_err(to_s3_error)?;
        self.store
            .delete_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(to_s3_error)?;
        // 204 **不是这句 `with_status` 给的**：s3s 0.17 的生成 operation 在成功路径上
        // 不读 `S3Response.status`（详见 `not_modified` 的说明），真正定状态的是
        // `DeleteObject::serialize_http` 里写死的 `NO_CONTENT`。这里保留它只是无害的
        // 显式表达，不要据此以为「删掉这行会变成 200」。
        Ok(S3Response::with_status(
            DeleteObjectOutput::default(),
            StatusCode::NO_CONTENT,
        ))
    }

    /// Task 5.9 冒烟发现的第二个真实兼容缺口：`mc rm`（**即使只删一个对象**，
    /// `mc --debug` 实测）走的是批量 `POST /<桶>?delete=`（`DeleteObjects`），
    /// 不是单对象 `DELETE`。s3s 默认回 `501 NotImplemented`，于是 `mc rm` 永远失败。
    /// 它不在「六条 multipart 一律 501」的刻意范围里（Task 5.6），是漏实现。
    /// 这里用已有的单删原语把它拼出来：逐键独立、成功的进 `Deleted`。
    ///
    /// 语义按 S3 批量删除：请求整体合法就回 200（不是「成功删除的键数」）；
    /// MVP 的删除是幂等的，所以合法键一律进 `Deleted`。key 命名非法（Task 5.7）
    /// 仍是客户端的错，整请求 400——与单对象 `DELETE` 的处理保持一致。
    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        let bucket = &req.input.bucket;
        let mut deleted: Vec<DeletedObject> = Vec::with_capacity(req.input.delete.objects.len());
        for obj in &req.input.delete.objects {
            validate_object_key(&obj.key).map_err(to_s3_error)?;
            self.store
                .delete_object(bucket, &obj.key)
                .await
                .map_err(to_s3_error)?;
            deleted.push(DeletedObject {
                key: Some(obj.key.clone()),
                ..Default::default()
            });
        }
        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: Some(deleted),
            ..Default::default()
        }))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        // PERF: 见 DESIGN §1.2 与 §20 Phase 2 — 命名空间索引。
        // MVP 靠引擎的列举（4.11）+ 本层过滤/分页，不是终态设计。
        //
        // 候选是**分批**向引擎要的，不是一次要全量：`bounded_listing` 打开时
        // 引擎的增量遍历会在够数时提前收手，于是「10 万个 key 的桶只要 1000 条」
        // 只走一小段。模式关着时引擎退回全量遍历，第一批就带回全部、`more = false`，
        // 于是下面的循环只转一圈——**与今天的开销相同**。

        let prefix = req.input.prefix.as_deref().unwrap_or("");
        // 空字符串的 delimiter 等同没给（否则每个 key 都在开头「命中」空串）。
        let delimiter = req.input.delimiter.as_deref().filter(|d| !d.is_empty());
        // `max-keys` 缺省 1000（S3 默认页大小）；负数按 0 处理——「不要更多」。
        let max_keys = req.input.max_keys.unwrap_or(1000).max(0) as usize;
        // 续传起点：**不解析 token 的内容，就当它是「上一个 key」**。S3 不规定 token
        // 的形状，裸 key 是最小实现（少一个 base64 依赖与一次编解码来回）；代价是
        // token 里能看到对象名，但本 API 本就把对象名给同一个调用方看，不算泄露。
        let start_after = req
            .input
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
        // **取批游标**：与 `last_key` 不同——`last_key` 是给客户端的续传 token，
        // 这个是本层内部翻批用的，一次响应里可能前进很多次。
        let mut cursor: Option<String> = start_after.clone();
        // 一批至少覆盖一整页，免得为了凑满一页反复取批。
        let batch_want = max_keys.max(1);

        'batches: loop {
            let (batch, more) = self
                .store
                .list_objects_from(
                    &req.input.bucket,
                    req.input.prefix.as_deref(),
                    cursor.as_deref(),
                    batch_want,
                )
                .await
                .map_err(to_s3_error)?;

            for entry in &batch {
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
                    break 'batches;
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
                            e_tag: Some(ETag::Strong(entry.etag.clone())),
                            last_modified: Some(timestamp_of(entry.mod_time)),
                            storage_class: Some(ObjectStorageClass::from_static("STANDARD")),
                            ..Default::default()
                        });
                        last_key = Some(entry.key.clone());
                    }
                }
            }

            // 本批处理完了，后面没有更多 → 这一页就是最终结果，`truncated` 保持 false。
            if !more {
                break 'batches;
            }
            // 还有更多：从本批最后一个候选之后继续。
            // 引擎的契约保证 `batch` 非空（空批必然 `more == false`），所以这里取不到
            // 最后一个就等于上游违约——**必须停下**，否则就是一个死循环。
            match batch.last() {
                Some(e) => cursor = Some(e.key.clone()),
                None => break 'batches,
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

/// 构造 304 `Not Modified` 错误（GET / HEAD 的条件命中）。
///
/// **为什么走错误通道而不是 `S3Response::with_status(_, NOT_MODIFIED)`**：s3s 0.17 的
/// 生成 operation 在成功路径上**不读** `S3Response.status`（`serialize_http` 把状态
/// 写死成 200，Range 命中才是 206），所以那样返回会变成 200。`S3ErrorCode::NotModified`
/// 的 HTTP 状态恰是 304，而 `serialize_error` 对 304 这类 bodyless 状态会剥掉 body 与
/// 描述 body 的头——正好是 304 要的形状。验证器（ETag / Last-Modified）用 `set_headers`
/// 挂上（RFC 9110 §15.4.5 建议 304 回显当前验证器）。
fn not_modified(info: &ObjectInfo) -> S3Error {
    let mut err = s3s::s3_error!(NotModified);
    let mut headers = http::HeaderMap::new();
    headers.insert(
        http::header::ETAG,
        ETag::Strong(info.etag.clone())
            .to_http_header()
            .expect("etag 是合法 header 值"),
    );
    let mut buf = Vec::new();
    conditional::last_modified(info)
        .format(TimestampFormat::HttpDate, &mut buf)
        .expect("timestamp 可格式化为 HTTP-date");
    headers.insert(
        http::header::LAST_MODIFIED,
        http::HeaderValue::from_bytes(&buf).expect("HTTP-date 是 ASCII"),
    );
    err.set_headers(headers);
    err
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
            Ok(ByteRange {
                start,
                end: size - 1,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use bytes::Bytes;
    use http::{HeaderMap, StatusCode};
    use s3s::service::S3ServiceBuilder;

    use rstore_api::ObjectStore;

    use super::*;
    use crate::build_service;
    use crate::mock::{fake_etag, MockStore};

    /// 把一段内存字节包成 [`PutRequest`]。`body` 是流，每个调用点自己包一遍太吵，
    /// 而测试里要的从来就是「拿这坨字节去 PUT」。
    fn put_req(bucket: &str, key: &str, data: Vec<u8>) -> PutRequest {
        PutRequest {
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            body: Box::new(std::io::Cursor::new(data)),
            etag: None,
        }
    }

    // 签名与请求辅助都搬到 `testutil.rs` 了——`iam.rs` 的权限矩阵测试要用
    // 同一套（复制第二份就等于放弃了对校验的测试）。
    use crate::testutil::{
        call, call_on, error_code, request, signed_list_buckets, signed_request, TestBody,
        ACCESS_KEY, SECRET_KEY,
    };

    /// 业务测试用的 service：挂 `MockStore`，**不设 auth**。
    ///
    /// 无 auth 时 s3s 接受匿名请求并跳过鉴权（见 `S3ServiceBuilder` 文档）——
    /// 签名的正确性已由 5.1 的 `rejects_bad_signature` / `accepts_valid_sigv4` 覆盖，
    /// 5.2 只测操作翻译，不必给每个请求再签一遍名。
    fn mock_service(store: Arc<MockStore>) -> s3s::service::S3Service {
        S3ServiceBuilder::new(RstoreFs { store }).build()
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

    /// 下划线桶名由**上游的**默认校验挡下，不是我们写的规则。
    ///
    /// s3s 的 `S3ServiceBuilder` 在 `validation` 未设时用 AWS 规则
    /// （`check_bucket_name`：只允许 `[a-z0-9.-]`、长度 3..64、首尾须字母或数字），
    /// 而它生效在**路径解析阶段**——所以 `impl S3 for RstoreFs` 的 `create_bucket`
    /// 根本不会被执行。
    ///
    /// 这个测试不是「测我们的代码」，而是把**控制面板所依赖的那条属性**钉成可执行的：
    /// `/_console` 这个路径前缀永远不可能是一个真实桶名，所以面板路由与桶名空间
    /// 不会互相遮蔽。谁把 `set_validation` 换成宽松实现，这里就会红。
    #[tokio::test]
    async fn underscore_bucket_name_is_rejected_by_upstream_validation() {
        let store = Arc::new(MockStore::default());
        let (status, _headers, body) = call_on(
            mock_service(store.clone()),
            request("PUT", "/_private", b""),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(error_code(&body), "InvalidBucketName");

        // handler 压根没跑，所以 store 当然没被动过。断言它，是为了让「拒绝发生在
        // 我们这层还是上游」这个区别在**行为**上有痕迹——若哪天变成我们的 handler
        // 在挡，这条仍会过，但那时就该把注释改掉。
        assert!(
            store.list_buckets().await.expect("list buckets").is_empty(),
            "非法的桶名不得被创建"
        );
    }

    /// 对照组：`console`（不带下划线）是合法桶名，必须照常能建。
    ///
    /// 这正是面板挂 `/_console` 而不是 `/console` 的全部理由——不占用这个正常名字。
    #[tokio::test]
    async fn console_bucket_name_is_still_valid() {
        let store = Arc::new(MockStore::default());
        let (status, _headers, body) =
            call_on(mock_service(store.clone()), request("PUT", "/console", b"")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
    }

    #[tokio::test]
    async fn delete_non_empty_bucket_is_409() {
        let store = Arc::new(MockStore::default());
        store
            .create_bucket("test-bucket")
            .await
            .expect("create bucket");
        store
            .put_object(put_req("test-bucket", "obj", b"data".to_vec()))
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

    /// Task 5.9：`mc alias set` 的 region 探测（`GET /<桶>?location=`）必须返回
    /// us-east-1 风格的空 `<LocationConstraint>`，而不是 s3s 默认的 501。
    /// 去掉 `get_bucket_location` 实现后，这条会退回 501 NotImplemented 而红。
    #[tokio::test]
    async fn get_bucket_location_returns_empty_location_constraint() {
        let store = Arc::new(MockStore::default());
        store
            .create_bucket("test-bucket")
            .await
            .expect("create bucket");

        let (status, _headers, body) = call_on(
            mock_service(store),
            request("GET", "/test-bucket?location=", b""),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        let xml = std::str::from_utf8(&body).expect("xml body");
        assert!(
            xml.contains("<LocationConstraint"),
            "缺少 LocationConstraint 元素: {xml}"
        );
        // 空内容 = us-east-1。带 region 值时这里会多出字符。
        assert!(
            xml.contains("></LocationConstraint>") || xml.contains("/>"),
            "LocationConstraint 应为空: {xml}"
        );
    }

    /// `?location` 不是「什么桶都答 200」的端点：不存在的桶按 AWS 语义回 404。
    #[tokio::test]
    async fn get_bucket_location_missing_bucket_is_404() {
        let store = Arc::new(MockStore::default());

        let (status, _headers, body) = call_on(
            mock_service(store),
            request("GET", "/missing-bucket?location=", b""),
        )
        .await;
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
        let (status, headers, body) =
            call_on(mock_service(store), request("GET", "/test-bucket/k", b"")).await;
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

        let (status, headers, body) =
            call_on(mock_service(store), request("HEAD", "/test-bucket/k", b"")).await;
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

    // ---- Task 5.9: 客户端冒烟暴露的缺口 ----

    /// `mc rm` 走批量 `DeleteObjects`（`POST /<桶>?delete=`，见 tests/compat/mc.sh）。
    /// 去掉 `delete_objects` 实现后，这条会退回 s3s 的 501 NotImplemented 而红。
    #[tokio::test]
    async fn delete_objects_batch_removes_all_keys() {
        let store = Arc::new(MockStore::default());
        store
            .create_bucket("test-bucket")
            .await
            .expect("create bucket");
        store
            .put_object(put_req("test-bucket", "a", b"1".to_vec()))
            .await
            .expect("put a");
        store
            .put_object(put_req("test-bucket", "b", b"2".to_vec()))
            .await
            .expect("put b");

        let body = b"<Delete>\
            <Object><Key>a</Key></Object>\
            <Object><Key>b</Key></Object>\
            </Delete>";
        // s3s 的 `DeleteObjects` 解析要求 `Content-Length`（真实客户端都会发）。
        let mut del_req = request("POST", "/test-bucket?delete=", body);
        del_req.headers_mut().insert(
            "content-length",
            body.len()
                .to_string()
                .parse()
                .expect("valid content-length"),
        );
        let (status, _headers, resp) = call_on(mock_service(store.clone()), del_req).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&resp)
        );
        let xml = std::str::from_utf8(&resp).expect("xml body");
        assert!(
            xml.contains("<Key>a</Key>") && xml.contains("<Key>b</Key>"),
            "DeleteResult 应回显两个 key: {xml}"
        );

        // 两个 key 都必须真的没了：删除不能只体现在响应里。
        for key in ["a", "b"] {
            let (s, _h, _b) = call_on(
                mock_service(store.clone()),
                request("HEAD", &format!("/test-bucket/{key}"), b""),
            )
            .await;
            assert_eq!(s, StatusCode::NOT_FOUND, "key {key} 应已删除");
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
        call_on(
            mock_service(store),
            request_with_range("/test-bucket/k", range),
        )
        .await
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
                .put_object(put_req("test-bucket", key, key.as_bytes().to_vec()))
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
        let (status, _headers, body) =
            call_on(mock_service(store), request("GET", &path, b"")).await;
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
        assert!(
            content_keys(&body).is_empty(),
            "max-keys=0 不应返回任何对象"
        );
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
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&page1)
        );
        let token = scalar(&page1, "NextContinuationToken");

        // 第二页不设 max-keys，把剩下的全取回。
        let (status, page2) = list(store, &format!("continuation-token={token}")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&page2)
        );

        let first = content_keys(&page1);
        let second = content_keys(&page2);
        assert_eq!(
            second,
            ["dir/x", "dir/y", "z.txt"],
            "第二页应无缺口地接着第一页"
        );
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
                .put_object(put_req("test-bucket", key, key.as_bytes().to_vec()))
                .await
                .expect("put object");
        }
        // 4 条、max-keys=2：页一截断，用它的 token 取页二。
        let (_, page1) = list(store.clone(), "max-keys=2").await;
        let token = scalar(&page1, "NextContinuationToken");
        let (status, page2) = list(store, &format!("max-keys=2&continuation-token={token}")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&page2)
        );
        assert_eq!(content_keys(&page2), ["c", "d"]);
        // 恰好填满且已到底：必须 false，否则客户端会再多翻一页空页（循环不收敛）。
        assert_eq!(scalar(&page2, "IsTruncated"), "false");
    }

    // ---- Task 5.6: Multipart 一律 501 ----

    #[tokio::test]
    async fn all_six_multipart_ops_are_501_not_implemented() {
        // 六种请求形状各自路由到 s3s 的一个 multipart trait 方法；本项目都没覆写
        // （`impl S3 for RstoreFs` 里一个都没有），于是命中 s3s 的默认实现
        // → `501 NotImplemented`。MVP 不做 multipart，这是刻意的。
        //
        // **六个都单独断言**：trait 方法众多，日后有人覆写其中一个（例如为实现
        // 别的目的而实现了 `UploadPart`），只有逐条断言才能发现——只测一个再
        // `..` 掉是不行的。
        // CompleteMultipartUpload 的输入里有 XML body 字段，s3s 会在**调用 handler
        // 之前**先解析它：空体会先变成一个 `400 MalformedXML`，根本够不到那个默认的
        // 501。所以这一形状要给一份合法（Part 列表为空）的 XML，才真正走到默认实现。
        let empty_complete_xml = b"<CompleteMultipartUpload></CompleteMultipartUpload>";
        let shapes: [(&str, &str, &[u8]); 6] = [
            ("POST", "/test-bucket/k?uploads", b""),
            ("PUT", "/test-bucket/k?partNumber=1&uploadId=x", b""),
            ("POST", "/test-bucket/k?uploadId=x", empty_complete_xml),
            ("DELETE", "/test-bucket/k?uploadId=x", b""),
            ("GET", "/test-bucket/k?uploadId=x", b""),
            ("GET", "/test-bucket?uploads", b""),
        ];
        for (method, path, payload) in shapes {
            // `request` 夹具不带 Content-Length；CompleteMultipartUpload 的 XML
            // 解析要求它，缺了会先撞上 `411 MissingContentLength`（s3s 要求带体的
            // 请求显式声明长度）。补上，其余形状带 0 也无害。
            let mut req = request(method, path, payload);
            req.headers_mut().insert(
                http::header::CONTENT_LENGTH,
                http::HeaderValue::from_str(&payload.len().to_string()).expect("长度是 ASCII"),
            );
            let (status, _headers, body) =
                call_on(mock_service(Arc::new(MockStore::default())), req).await;
            assert_eq!(
                status,
                StatusCode::NOT_IMPLEMENTED,
                "{method} {path} 应回 501，body: {}",
                String::from_utf8_lossy(&body)
            );
            assert_eq!(
                error_code(&body),
                "NotImplemented",
                "{method} {path} 的 error code 应为 NotImplemented，body: {}",
                String::from_utf8_lossy(&body)
            );
        }
    }

    // ---- Task 5.7: 命名校验 ----

    /// PUT 一个非法 key，断言 `400 InvalidArgument`。
    async fn put_key_expect_invalid_argument(key: &str) {
        let (status, _headers, body) = call_on(
            mock_service(Arc::new(MockStore::default())),
            request("PUT", &format!("/test-bucket/{key}"), b""),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "PUT {key} 应回 400，body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(
            error_code(&body),
            "InvalidArgument",
            "PUT {key} 的 error code，body: {}",
            String::from_utf8_lossy(&body)
        );
    }

    #[tokio::test]
    async fn put_key_with_reserved_first_segment_is_400() {
        // 规则 1：第一段不得以 `.rstore` 开头（DESIGN §6.3）。
        put_key_expect_invalid_argument(".rstore.sys/x").await;
    }

    #[tokio::test]
    async fn put_key_that_aliases_on_disk_is_400() {
        // 规则 2：`a//b` 会被 `fsx::resolve` 折成 `a/b`，两个不同的 S3 key 落到同一个
        // 文件上——放行就是静默互相覆盖。规则 1 的测试对这条完全无感，必须单独测。
        put_key_expect_invalid_argument("a//b").await;
    }

    /// **读路径上的同一道校验**（`get_object` / `head_object` 各自调
    /// `validate_object_key`）。只测 PUT 侧是不够的：读路径漏拦时，`..` 会一路走到
    /// 盘层的 `Fatal(FatalKind::PathEscape)` 变成 **500**——把客户端的错报成
    /// 「服务端坏了」，而这正是 DESIGN 要求区分开的两种失败。
    #[tokio::test]
    async fn read_key_that_aliases_on_disk_is_400_not_500() {
        for key in ["a//b", ".rstore.sys/x"] {
            for method in ["GET", "HEAD"] {
                let (status, _headers, body) = call_on(
                    mock_service(Arc::new(MockStore::default())),
                    request(method, &format!("/test-bucket/{key}"), b""),
                )
                .await;
                assert_eq!(
                    status,
                    StatusCode::BAD_REQUEST,
                    "{method} {key} 应回 400（**不是 500**），body: {}",
                    String::from_utf8_lossy(&body)
                );
                assert_eq!(
                    error_code(&body),
                    "InvalidArgument",
                    "{method} {key} 的 error code，body: {}",
                    String::from_utf8_lossy(&body)
                );
            }
        }
    }

    // ---- Task 5.10: 条件请求（GET / HEAD） ----

    /// 构造一个带任意请求头的请求，其余走 `request` 夹具。
    fn request_with(method: &str, path: &str, headers: &[(&str, &str)]) -> http::Request<TestBody> {
        let mut req = request(method, path, b"");
        for (name, value) in headers {
            req.headers_mut().insert(
                http::HeaderName::from_bytes(name.as_bytes()).expect("合法请求头名"),
                http::HeaderValue::from_str(value).expect("合法请求头值"),
            );
        }
        req
    }

    #[tokio::test]
    async fn get_with_if_none_match_hit_is_304_and_has_no_body() {
        let store = Arc::new(MockStore::default());
        let _ = call_on(
            mock_service(store.clone()),
            request("PUT", "/test-bucket/k", OBJ_BODY),
        )
        .await;
        // 客户端手里的 ETag 就是线格式（带引号），直接用夹具算出的期望值构造。
        let etag = format!("\"{}\"", fake_etag(OBJ_BODY));

        let (status, headers, body) = call_on(
            mock_service(store),
            request_with("GET", "/test-bucket/k", &[("if-none-match", etag.as_str())]),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::NOT_MODIFIED,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert!(body.is_empty(), "304 响应不能带 body");
        // RFC 9110 §15.4.5：304 应回显当前验证器；也不能带描述 body 的长度头。
        assert_eq!(header(&headers, "etag"), etag);
        assert!(
            headers.get("content-length").is_none(),
            "304 不应带 Content-Length"
        );
    }

    #[tokio::test]
    async fn get_with_if_match_mismatch_is_412() {
        let store = Arc::new(MockStore::default());
        let _ = call_on(
            mock_service(store.clone()),
            request("PUT", "/test-bucket/k", OBJ_BODY),
        )
        .await;

        let (status, _headers, body) = call_on(
            mock_service(store),
            request_with("GET", "/test-bucket/k", &[("if-match", "\"deadbeef\"")]),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::PRECONDITION_FAILED,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(error_code(&body), "PreconditionFailed");
    }

    #[tokio::test]
    async fn conditional_header_does_not_turn_404_into_412() {
        // 盯的是「先求条件再查对象」的写法：那样对不存在的 key 会先撞上 If-Match
        // 不命中而回 412，正确顺序是先查到对象（这里 404）再求条件。
        let (status, _headers, body) = call_on(
            mock_service(Arc::new(MockStore::default())),
            request_with(
                "GET",
                "/test-bucket/missing",
                &[("if-match", "\"deadbeef\"")],
            ),
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

    /// 条件请求**只覆盖 GET / HEAD**（见「MVP 的已知限制」）：S3 里 `PUT` 带
    /// `If-None-Match: *` 是「条件创建」，而 MVP 的 `put_object` **完全不求值**条件头
    /// （这个文件里 `put_object` 没有任何 `conditional::evaluate` 的调用），
    /// 于是它被当成普通 PUT。
    ///
    /// **这是一条负向测试：它钉住「现在就是这样」。** 日后给 `ObjectStore` 加上了
    /// 原子的 conditional-put，这条会红——**那时才该改它**，而不是现在为了让测试
    /// 「看起来正确」把它删掉。上面 `get_with_*` 那两条是正向的，两者一起才说明
    /// 「条件求值只挂在读路径上」。
    #[tokio::test]
    async fn put_ignores_if_none_match_instead_of_412() {
        let store = Arc::new(MockStore::default());
        let _ = call_on(
            mock_service(store.clone()),
            request("PUT", "/test-bucket/k", OBJ_BODY),
        )
        .await;

        // 对象已存在，带 `If-None-Match: *` 再 PUT。S3 语义下该回 412，MVP 下**成功**。
        let mut req = request("PUT", "/test-bucket/k", b"replaced");
        req.headers_mut().insert(
            http::HeaderName::from_static("if-none-match"),
            http::HeaderValue::from_static("*"),
        );
        let (status, _headers, body) = call_on(mock_service(store.clone()), req).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "PUT 不应对 If-None-Match 求值（这是被钉住的已知限制），body: {}",
            String::from_utf8_lossy(&body)
        );

        // 「回了 200 但根本没写」也是坏法：必须真的**覆盖**掉了旧内容。
        let (get_status, _h, got) =
            call_on(mock_service(store), request("GET", "/test-bucket/k", b"")).await;
        assert_eq!(get_status, StatusCode::OK);
        assert_eq!(
            &got[..],
            b"replaced",
            "第二次 PUT 必须真的覆盖，读回来应是新内容"
        );
    }

    // ---- Task 5.11: 虚拟主机风格寻址 ----

    #[tokio::test]
    async fn virtual_host_style_host_header_selects_bucket() {
        let store = Arc::new(MockStore::default());
        // `build_service` 设了 auth，所以这一节的请求**必须签名**——这正是把
        // `signed_list_buckets` 泛化成 `signed_request` 的原因。
        let service = build_service(
            store.clone(),
            Arc::new(rstore_iam::IamStore::root_only(ACCESS_KEY, SECRET_KEY)),
            Some("example.com"),
        )
        .expect("valid base domain");

        let payload = b"hello vhost";
        let (status, _headers, resp) = call_on(
            service.clone(),
            signed_request("PUT", "/obj", "test-bucket.example.com", payload),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&resp)
        );

        // 本用例的核心：桶名被解析成 `test-bucket`，而不是整个 host。
        let keys: Vec<String> = store
            .list_objects("test-bucket", None)
            .await
            .expect("list objects")
            .into_iter()
            .map(|e| e.key)
            .collect();
        assert_eq!(keys, ["obj"], "桶名应解析成 test-bucket");
        let wrong = store
            .list_objects("test-bucket.example.com", None)
            .await
            .expect("list objects");
        assert!(wrong.is_empty(), "不应把整个 host 当桶名: {wrong:?}");

        // GET 同一个路径、同一个 Host → 200，且体与 PUT 进去的一致。
        let (status, _headers, resp) = call_on(
            service,
            signed_request("GET", "/obj", "test-bucket.example.com", b""),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&resp)
        );
        assert_eq!(resp.as_ref(), payload, "GET 体应与 PUT 进去的一致");
    }

    #[tokio::test]
    async fn path_style_still_works_when_base_domain_is_set() {
        // 开了虚拟主机没有把 path-style 关掉：`SingleDomain` 内部对 `host_part ==
        // base_part` 返回**不带 bucket** 的 `VirtualHost`，于是回落到 path-style。
        let store = Arc::new(MockStore::default());
        let service = build_service(
            store.clone(),
            Arc::new(rstore_iam::IamStore::root_only(ACCESS_KEY, SECRET_KEY)),
            Some("example.com"),
        )
        .expect("valid base domain");

        let payload = b"hello path style";
        let (status, _headers, resp) = call_on(
            service.clone(),
            signed_request("PUT", "/test-bucket/obj", "example.com", payload),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&resp)
        );

        let (status, _headers, resp) = call_on(
            service,
            signed_request("GET", "/test-bucket/obj", "example.com", b""),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&resp)
        );
        assert_eq!(resp.as_ref(), payload, "GET 体应与 PUT 进去的一致");
        assert_eq!(
            store
                .list_objects("test-bucket", None)
                .await
                .expect("list objects")
                .into_iter()
                .map(|e| e.key)
                .collect::<Vec<_>>(),
            ["obj"]
        );
    }

    #[tokio::test]
    async fn domain_outside_base_domain_becomes_its_own_bucket() {
        // **这记录的是一个反直觉的行为，不是我们想要的功能**：`SingleDomain` 的
        // CNAME 回退（默认开启）把 base domain 之外的整个 host 当桶名，所以
        // `Host: other.example.net` 会被解析成 `bucket = "other.example.net"`。
        //
        // 为什么是 `DELETE /` 而不是 `GET /`：`MockStore::list_objects` 对不存在的桶
        // 回 `Ok(vec![])`（它不查桶是否存在），`GET /` 会得到 200，区分不出桶名对
        // 不对；只有 `head_bucket` / `delete_bucket` 会真的回 `NoSuchBucket`。
        // （`HEAD /` 也能拿到 404，但 s3s 会把 HEAD 的响应体剥掉，断言不了 `<Code>`。）
        let store = Arc::new(MockStore::default());
        let service = build_service(
            store,
            Arc::new(rstore_iam::IamStore::root_only(ACCESS_KEY, SECRET_KEY)),
            Some("example.com"),
        )
        .expect("valid base domain");

        let (status, _headers, body) = call_on(
            service,
            signed_request("DELETE", "/", "other.example.net", b""),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        assert_eq!(error_code(&body), "NoSuchBucket");
    }

    #[tokio::test]
    async fn ip_host_is_path_style_even_with_base_domain() {
        // 机制：`parse_request_host`（s3s 的 `ops/mod.rs:511`）的条件是
        // `if let (Some(host_header), Some(s3_host)) = (host_header, ccx.host)
        //  && !is_socket_addr_or_ip_addr(host_header)`——**IP / socket 形式的 host
        // 会把虚拟主机解析整段跳过**，`SingleDomain` 根本不会被调用，Host 头被丢弃。
        //
        // 对比：`localhost:9000` 既不是合法 `SocketAddr` 也不是 `IpAddr`，**不**享受
        // 这个豁免，会被 CNAME 回退拿去做桶名（而且那处**不剥端口**），于是桶名校验
        // 失败。别把 `127.0.0.1` 和 `localhost` 混为一谈。
        let store = Arc::new(MockStore::default());
        let service = build_service(
            store,
            Arc::new(rstore_iam::IamStore::root_only(ACCESS_KEY, SECRET_KEY)),
            Some("example.com"),
        )
        .expect("valid base domain");

        let (status, _headers, body) =
            call_on(service, signed_request("GET", "/", "127.0.0.1:9000", b"")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        let xml = std::str::from_utf8(&body).expect("xml body");
        assert!(xml.contains("ListAllMyBucketsResult"), "xml: {xml}");
    }

    #[test] // 同步的 `#[test]`：构造期校验，不起 runtime。
    fn invalid_base_domain_is_rejected_at_construction() {
        // 这属于**启动期**配置错误，要在进程启动时明确报错退出，而不是拖到第一个
        // 请求变成一个费解的 400——那时运维看到的是「服务起来了但客户端全挂」，
        // 没有任何线索指向 `--base-domain`。
        let store = Arc::new(MockStore::default());
        // `let-else` 而不是 `expect_err`：`S3Service` 没实现 `Debug`，`expect_err`
        // 编译不过；而 `.err().expect()` 会被 clippy 的 `err_expect` 挡下。
        let Err(err) = build_service(
            store,
            Arc::new(rstore_iam::IamStore::root_only(ACCESS_KEY, SECRET_KEY)),
            Some("not a domain"),
        ) else {
            panic!("非法域名应在构造期被拒绝");
        };
        assert!(err.contains("not a domain"), "错误应含原始输入: {err}");
    }

    // ---- Task 5.11: `bounded_listing` 的分批列举 ----

    /// 从 ListObjectsV2 的 XML 里按出现顺序取出所有 `<Key>..</Key>`。
    ///
    /// 手撕而不是引 XML 库：这里只需要「有几个、什么顺序」，而多一个依赖要过
    /// allowlist 与 license 检查，不划算。
    fn keys_in(body: &[u8]) -> Vec<String> {
        let xml = std::str::from_utf8(body).expect("list body is utf-8 xml");
        let mut out = Vec::new();
        let mut rest = xml;
        while let Some(i) = rest.find("<Key>") {
            let after = &rest[i + "<Key>".len()..];
            let end = after.find("</Key>").expect("well-formed <Key>");
            out.push(after[..end].to_string());
            rest = &after[end..];
        }
        out
    }

    /// 取 `<NextContinuationToken>..</NextContinuationToken>`；没有（= 最后一页）返回 `None`。
    fn next_token(body: &[u8]) -> Option<String> {
        let xml = std::str::from_utf8(body).expect("list body is utf-8 xml");
        let open = "<NextContinuationToken>";
        let i = xml.find(open)? + open.len();
        let end = xml[i..].find("</NextContinuationToken>")?;
        Some(xml[i..i + end].to_string())
    }

    /// **逐页取要拼出与一次全量完全相同的 key 串**——`max-keys=2` 对 5 个 key，
    /// 必然要走完 `list_objects_v2` 里那个 `'batches` 循环的三圈。
    ///
    /// 这条盯的就是那个循环：它把「一次拿全」换成了「多次拿一小段」，游标前进、
    /// 容量判定、`is_truncated` / `next_continuation_token` 的任何一个写错都会在这里显形。
    ///
    /// **用 `MockStore` 是够的**：它没覆盖 `ObjectStore::list_objects_from`，
    /// 于是走 trait 的默认实现（一次全量 + 按 `after`/`want` 过滤），
    /// 那正是「旧模式」的形状，S3 层照样会收到 `more = true` 并被迫翻批。
    /// 引擎那一侧的**提前停**由 `crates/store/src/list.rs` 的单测负责，
    /// 两件事各测各的，不要在这里混着测。
    #[tokio::test]
    async fn list_v2_paging_reassembles_the_whole_bucket() {
        let store = Arc::new(MockStore::default());
        for i in 0..5 {
            let (status, _, body) = call_on(
                mock_service(store.clone()),
                request("PUT", &format!("/test-bucket/k{i}"), OBJ_BODY),
            )
            .await;
            assert_eq!(
                status,
                StatusCode::OK,
                "body: {}",
                String::from_utf8_lossy(&body)
            );
        }

        let mut seen: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        let mut pages = 0usize;
        loop {
            pages += 1;
            assert!(pages <= 10, "分页没有终止——游标没有前进");
            let path = match &token {
                Some(t) => format!("/test-bucket?list-type=2&max-keys=2&continuation-token={t}"),
                None => "/test-bucket?list-type=2&max-keys=2".to_string(),
            };
            let (status, _, body) =
                call_on(mock_service(store.clone()), request("GET", &path, b"")).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "body: {}",
                String::from_utf8_lossy(&body)
            );

            seen.extend(keys_in(&body));
            match next_token(&body) {
                Some(t) => token = Some(t),
                None => break,
            }
        }

        assert_eq!(
            seen,
            vec!["k0", "k1", "k2", "k3", "k4"],
            "分页拼接必须与全量列举逐条相同、同序"
        );
        assert_eq!(pages, 3, "5 个 key、每页 2 个 → 恰好三页（2+2+1）");
    }
}
