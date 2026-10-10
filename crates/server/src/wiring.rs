//! 组合根：把 `ErasureSet` 的固有方法适配成 `rstore_api::ObjectStore`。
//!
//! `rstore-api` 与 `rstore-store` 是兄弟，allowlist 里没有 `api -> store` 这条边，
//! 所以「引擎 → S3」的适配只能落在同时看得见两边的 `rstore-server`。

use std::sync::Arc;

use async_trait::async_trait;
use rstore_api::{
    ApiError, ByteRange, ObjectData, ObjectEntry, ObjectInfo, ObjectStore, PutRequest,
};
use rstore_common::error::DiskError;
use rstore_store::error::StoreError;
use rstore_store::put::PutArgs;
use rstore_store::set::ErasureSet;

use crate::metrics::Metrics;

/// `ErasureSet` → `ObjectStore` 的适配器。
pub struct Wiring {
    set: Arc<ErasureSet>,
    metrics: Arc<Metrics>,
}

impl Wiring {
    pub fn new(set: Arc<ErasureSet>, metrics: Arc<Metrics>) -> Self {
        Self { set, metrics }
    }

    /// 只记账、不做映射。错误映射仍归 `map_object_err` / `map_bucket_err`。
    ///
    /// `op` 用于 quorum 失败指标的分档（`put` / `get` 等，与 Task 6.2 的标签对齐）。
    fn note_err(&self, e: &StoreError, op: &str) {
        match e {
            StoreError::WriteQuorum { .. } | StoreError::ReadQuorum { .. } => {
                self.metrics.record_quorum_failure(op);
            }
            StoreError::Disk(DiskError::Transient(_)) => {
                self.metrics.record_disk_error("transient");
            }
            StoreError::Disk(DiskError::Corrupt(_)) => {
                self.metrics.record_disk_error("corrupt");
                self.metrics.record_bitrot_mismatch();
            }
            StoreError::Disk(DiskError::Fatal(_)) => {
                self.metrics.record_disk_error("fatal");
            }
            _ => {}
        }
    }
}

/// 对象路径上的 `StoreError -> ApiError`。两个「不存在」行不能合并成同一个变体：
/// `GetObject` 要 `NoSuchKey`，`HeadBucket` 要 `NoSuchBucket`。
fn map_object_err(e: StoreError) -> ApiError {
    match e {
        StoreError::NotFound => ApiError::NoSuchKey,
        StoreError::Disk(DiskError::NotFound) => ApiError::NoSuchKey,
        StoreError::Disk(DiskError::Transient(_)) => ApiError::Unavailable,
        StoreError::Disk(DiskError::Corrupt(k)) => ApiError::Internal(format!("corrupt: {k}")),
        StoreError::Disk(DiskError::Fatal(k)) => ApiError::Internal(format!("fatal: {k}")),
        StoreError::WriteQuorum { .. } | StoreError::ReadQuorum { .. } => ApiError::Unavailable,
        StoreError::BucketNotEmpty => ApiError::BucketNotEmpty,
        StoreError::ShardLayout(msg) | StoreError::Internal(msg) => ApiError::Internal(msg),
        // `DiskError` 是 `#[non_exhaustive]`，必须有兜底。
        StoreError::Disk(other) => ApiError::Internal(format!("disk error: {other}")),
    }
}

/// 桶路径上的 `StoreError -> ApiError`。
fn map_bucket_err(e: StoreError) -> ApiError {
    match e {
        StoreError::NotFound => ApiError::NoSuchBucket,
        StoreError::Disk(DiskError::NotFound) => ApiError::NoSuchBucket,
        StoreError::Disk(DiskError::Transient(_)) => ApiError::Unavailable,
        StoreError::Disk(DiskError::Corrupt(k)) => ApiError::Internal(format!("corrupt: {k}")),
        StoreError::Disk(DiskError::Fatal(k)) => ApiError::Internal(format!("fatal: {k}")),
        StoreError::WriteQuorum { .. } | StoreError::ReadQuorum { .. } => ApiError::Unavailable,
        StoreError::BucketNotEmpty => ApiError::BucketNotEmpty,
        StoreError::ShardLayout(msg) | StoreError::Internal(msg) => ApiError::Internal(msg),
        StoreError::Disk(other) => ApiError::Internal(format!("disk error: {other}")),
    }
}

#[async_trait]
impl ObjectStore for Wiring {
    async fn create_bucket(&self, bucket: &str) -> Result<(), ApiError> {
        match self.set.create_bucket(bucket).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.note_err(&e, "create_bucket");
                Err(map_bucket_err(e))
            }
        }
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<(), ApiError> {
        match self.set.delete_bucket(bucket).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.note_err(&e, "delete_bucket");
                Err(map_bucket_err(e))
            }
        }
    }

    async fn head_bucket(&self, bucket: &str) -> Result<(), ApiError> {
        match self.set.bucket_exists(bucket).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(ApiError::NoSuchBucket),
            Err(e) => {
                self.note_err(&e, "head_bucket");
                Err(map_bucket_err(e))
            }
        }
    }

    async fn list_buckets(&self) -> Result<Vec<String>, ApiError> {
        self.set.list_buckets().await.map_err(map_bucket_err)
    }

    async fn put_object(&self, req: PutRequest) -> Result<ObjectInfo, ApiError> {
        let started = self.metrics.is_enabled().then(std::time::Instant::now);
        let out = self
            .set
            .put_object(PutArgs {
                bucket: req.bucket,
                key: req.key,
                body: req.body,
                etag: req.etag,
            })
            .await;
        if let Some(t0) = started {
            self.metrics.record_put(t0.elapsed());
        }
        match out {
            Ok(v) => Ok(ObjectInfo {
                size: v.size,
                etag: v.etag,
                // `PutOut` 不带 `mod_time`，而 S3 的 PUT 响应只消费 etag，这里填 0。
                mod_time: 0,
            }),
            Err(e) => {
                self.note_err(&e, "put");
                Err(map_object_err(e))
            }
        }
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<ObjectData, ApiError> {
        let started = self.metrics.is_enabled().then(std::time::Instant::now);
        let store_range = range.map(|r| rstore_store::get::ByteRange {
            start: r.start,
            end: r.end,
        });
        let out = self.set.get_object(bucket, key, store_range).await;
        if let Some(t0) = started {
            self.metrics.record_get(t0.elapsed());
        }
        match out {
            Ok(v) => Ok(ObjectData {
                data: v.data,
                size: v.size,
                etag: v.etag,
                mod_time: v.mod_time,
            }),
            Err(e) => {
                self.note_err(&e, "get");
                Err(map_object_err(e))
            }
        }
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo, ApiError> {
        match self.set.head_object(bucket, key).await {
            Ok(v) => Ok(ObjectInfo {
                size: v.size,
                etag: v.etag,
                mod_time: v.mod_time,
            }),
            Err(e) => {
                self.note_err(&e, "head");
                Err(map_object_err(e))
            }
        }
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), ApiError> {
        match self.set.delete_object(bucket, key).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.note_err(&e, "delete");
                Err(map_object_err(e))
            }
        }
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ObjectEntry>, ApiError> {
        match self.set.list_objects(bucket, prefix).await {
            Ok(entries) => Ok(entries
                .into_iter()
                .map(|e| ObjectEntry {
                    key: e.key,
                    size: e.size,
                    etag: e.etag,
                    mod_time: e.mod_time,
                })
                .collect()),
            Err(e) => {
                self.note_err(&e, "list");
                Err(map_object_err(e))
            }
        }
    }

    /// 转发给引擎的增量遍历。模式关着时引擎自己会退回全量（见 `list_objects_from`
    /// 在 `crates/store/src/list.rs` 里的分派），所以这一层不需要认识模式。
    async fn list_objects_from(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        after: Option<&str>,
        want: usize,
    ) -> Result<(Vec<ObjectEntry>, bool), ApiError> {
        match self
            .set
            .list_objects_from(bucket, prefix, after, want)
            .await
        {
            Ok((entries, more)) => Ok((
                entries
                    .into_iter()
                    .map(|e| ObjectEntry {
                        key: e.key,
                        size: e.size,
                        etag: e.etag,
                        mod_time: e.mod_time,
                    })
                    .collect(),
                more,
            )),
            Err(e) => {
                self.note_err(&e, "list");
                Err(map_object_err(e))
            }
        }
    }
}
