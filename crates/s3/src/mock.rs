//! 内存版 `ObjectStore`，只为 S3 层的协议测试服务。
//!
//! **为什么不用真的 `ErasureSet`**：`rstore-s3` 的 allowlist 里没有 `rstore-store`
//! （见 `scripts/check_layer_deps.py`），依赖边根本不存在。而这也正是对的——
//! 引擎的行为由 M4 的测试负责，这里只该验证「协议翻译」这一层（状态码、响应头、
//! XML 形状与字段）。
//!
//! **锁的纪律**：一律 `std::sync::Mutex`，且**临界区里绝不出现 `.await`**。
//! workspace lints 把 `clippy::await_holding_lock` 设成 `deny`，跨 await 持锁
//! 会直接挡死门禁。每个方法都是「加锁 → 取值/改值 → 出作用域」之后再 await。

use std::collections::BTreeMap;
use std::sync::Mutex;

use rstore_api::{ApiError, ByteRange, ObjectData, ObjectEntry, ObjectInfo, ObjectStore};

/// 所有对象共用的固定 `mod_time`（Unix 纳秒，2023-11-14）。
/// 用固定值而不是 `SystemTime::now()`：测试断言可复现，且不必引入时间依赖。
const MOCK_MOD_TIME_NANOS: u64 = 1_700_000_000_000_000_000;

/// 一个已存对象：`(内容, etag, mod_time_nanos)`。
type StoredObject = (Vec<u8>, String, u64);
/// 全部对象，按 `(bucket, key)` 升序。
type ObjectMap = BTreeMap<(String, String), StoredObject>;

#[derive(Default)]
pub struct MockStore {
    objects: Mutex<ObjectMap>,
    buckets: Mutex<Vec<String>>,
    /// 下一次调用要失败时返回的错误，取一次就清空。
    /// 5.8 的错误映射测试靠它让 store 返回 `NoSuchKey`，从而断言「S3 层回 404」
    /// ——否则那条断言根本没法触发。
    pub fail_next_with: Mutex<Option<ApiError>>,
}

impl MockStore {
    /// 若设了 `fail_next_with`，取出并清空。调用方据此提前返回错误。
    fn take_failure(&self) -> Option<ApiError> {
        self.fail_next_with
            .lock()
            .expect("mock mutex poisoned")
            .take()
    }

    /// 确保 `bucket` 出现在桶列表里（`put_object` 会调用它）。
    fn ensure_bucket(&self, bucket: &str) {
        let mut buckets = self.buckets.lock().expect("mock mutex poisoned");
        if !buckets.iter().any(|b| b == bucket) {
            buckets.push(bucket.to_string());
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for MockStore {
    async fn create_bucket(&self, bucket: &str) -> Result<(), ApiError> {
        if let Some(err) = self.take_failure() {
            return Err(err);
        }
        self.ensure_bucket(bucket);
        Ok(())
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<(), ApiError> {
        if let Some(err) = self.take_failure() {
            return Err(err);
        }
        let has_objects = {
            let objects = self.objects.lock().expect("mock mutex poisoned");
            objects.keys().any(|(b, _)| b == bucket)
        };
        if has_objects {
            return Err(ApiError::BucketNotEmpty);
        }
        let mut buckets = self.buckets.lock().expect("mock mutex poisoned");
        match buckets.iter().position(|b| b == bucket) {
            Some(pos) => {
                buckets.remove(pos);
                Ok(())
            }
            None => Err(ApiError::NoSuchBucket),
        }
    }

    async fn head_bucket(&self, bucket: &str) -> Result<(), ApiError> {
        if let Some(err) = self.take_failure() {
            return Err(err);
        }
        let exists = {
            let buckets = self.buckets.lock().expect("mock mutex poisoned");
            buckets.iter().any(|b| b == bucket)
        };
        if exists {
            Ok(())
        } else {
            Err(ApiError::NoSuchBucket)
        }
    }

    async fn list_buckets(&self) -> Result<Vec<String>, ApiError> {
        if let Some(err) = self.take_failure() {
            return Err(err);
        }
        let buckets = self.buckets.lock().expect("mock mutex poisoned");
        Ok(buckets.clone())
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Vec<u8>,
    ) -> Result<ObjectInfo, ApiError> {
        if let Some(err) = self.take_failure() {
            return Err(err);
        }
        let etag = fake_etag(&data);
        let size = data.len() as u64;
        {
            let mut objects = self.objects.lock().expect("mock mutex poisoned");
            objects.insert(
                (bucket.to_string(), key.to_string()),
                (data, etag.clone(), MOCK_MOD_TIME_NANOS),
            );
        }
        // PUT 自动建桶：M5 只测协议翻译，5.3 的 `PUT /b/k` 不会先建桶
        // （真实 S3 会回 `NoSuchBucket`，这里刻意宽松）。
        self.ensure_bucket(bucket);
        Ok(ObjectInfo {
            size,
            etag,
            mod_time: MOCK_MOD_TIME_NANOS,
        })
    }

    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<ObjectData, ApiError> {
        if let Some(err) = self.take_failure() {
            return Err(err);
        }
        let objects = self.objects.lock().expect("mock mutex poisoned");
        let (data, etag, mod_time) = objects
            .get(&(bucket.to_string(), key.to_string()))
            .ok_or(ApiError::NoSuchKey)?;
        // `size` 是**整个对象**的长度，不是返回切片的长度——这正是 `ObjectData.size`
        // 的契约（`Content-Range` 要它）。5.4 的断言依赖这一点。
        let full_size = data.len() as u64;
        let sliced = match range {
            Some(r) => data[r.start as usize..=r.end as usize].to_vec(),
            None => data.clone(),
        };
        Ok(ObjectData {
            data: sliced,
            size: full_size,
            etag: etag.clone(),
            mod_time: *mod_time,
        })
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo, ApiError> {
        if let Some(err) = self.take_failure() {
            return Err(err);
        }
        let objects = self.objects.lock().expect("mock mutex poisoned");
        let (data, etag, mod_time) = objects
            .get(&(bucket.to_string(), key.to_string()))
            .ok_or(ApiError::NoSuchKey)?;
        Ok(ObjectInfo {
            size: data.len() as u64,
            etag: etag.clone(),
            mod_time: *mod_time,
        })
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), ApiError> {
        if let Some(err) = self.take_failure() {
            return Err(err);
        }
        // S3 的 DELETE 幂等：删不存在的 key 也返回成功。
        let mut objects = self.objects.lock().expect("mock mutex poisoned");
        objects.remove(&(bucket.to_string(), key.to_string()));
        Ok(())
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<ObjectEntry>, ApiError> {
        if let Some(err) = self.take_failure() {
            return Err(err);
        }
        let objects = self.objects.lock().expect("mock mutex poisoned");
        // `BTreeMap<(bucket, key), _>` 已按 `(bucket, key)` 升序，过滤后天然是
        // key 升序——`list_objects` 契约要求「已按 key 升序」。
        let entries = objects
            .iter()
            .filter(|((b, k), _)| b == bucket && prefix.is_none_or(|p| k.starts_with(p)))
            .map(|((_, k), (data, etag, mod_time))| ObjectEntry {
                key: k.clone(),
                size: data.len() as u64,
                etag: etag.clone(),
                mod_time: *mod_time,
            })
            .collect();
        Ok(entries)
    }
}

/// 内容派生的确定性 etag（FNV-1a 64 位，格式化成 16 位十六进制）。
///
/// **不是真 MD5**：测试只需要 etag 稳定、可预测、内容不同则 etag 不同，
/// 这样才断言得了「它原样传到了响应头」。引入 `md-5` 只为这个夹具不值得。
fn fake_etag(data: &[u8]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in data {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    format!("{hash:016x}")
}
