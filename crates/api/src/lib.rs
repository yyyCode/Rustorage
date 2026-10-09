//! 存储契约 trait，供上层消费。不得反向依赖任何实现 crate。

pub mod error;

pub use error::ApiError;

use async_trait::async_trait;

/// S3 层能对引擎提出的全部问题。**刻意不含 multipart**——MVP 一律返回 501，
/// 所以 trait 上根本没有对应方法，编译期就堵死了「不小心实现了半个 multipart」。
///
/// 所有方法返回 [`ApiError`] 而**不是** `rstore_store::StoreError`：`rstore-api` 与
/// `rstore-store` 是兄弟，allowlist 里没有这条边。`StoreError -> ApiError`
/// 的映射写在组合根 `rstore-server/src/wiring.rs`。
#[async_trait]
pub trait ObjectStore: Send + Sync + 'static {
    async fn create_bucket(&self, bucket: &str) -> Result<(), ApiError>;
    async fn delete_bucket(&self, bucket: &str) -> Result<(), ApiError>;
    async fn head_bucket(&self, bucket: &str) -> Result<(), ApiError>;
    async fn list_buckets(&self) -> Result<Vec<String>, ApiError>;

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
    ) -> Result<ObjectInfo, ApiError>;
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<ObjectData, ApiError>;
    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo, ApiError>;
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), ApiError>;

    /// MVP 是**全盘遍历**（见 Task 4.11），返回已按 key 升序。
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ObjectEntry>, ApiError>;
}

/// 闭区间 `[start, end]`。**不复用 `rstore_store::ByteRange`**（那条边不存在）——
/// 两边各定一份，由组合根转换。把 `bytes=a-b` / `bytes=a-` / `bytes=-n` 解析并裁剪成
/// 闭区间是 S3 层的职责（Task 5.4），到这里必须已经是越界检查过的。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectInfo {
    pub size: u64,
    pub etag: String,
    /// Unix 纳秒。`rstore_meta` 里是 `Option<u64>`；本层统一成 `u64`
    /// （PUT 总会写它，见 4.5），组合根负责 `unwrap_or(0)`。
    pub mod_time: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectData {
    /// **请求范围内**的字节（`range` 为 `None` 时是整份）。
    pub data: Vec<u8>,
    /// 整个对象的原始长度（`Content-Range` 要它）。
    pub size: u64,
    pub etag: String,
    pub mod_time: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectEntry {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub mod_time: u64,
}
