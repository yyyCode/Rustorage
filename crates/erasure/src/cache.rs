//! 编解码器外壳的 LRU 缓存。对应 DESIGN §10.2。

use std::num::NonZeroUsize;
use std::sync::{Mutex, PoisonError};

use lru::LruCache;

use crate::error::ErasureConstructionError;
use crate::Codec;

/// 默认容量：32（DESIGN §10.2）。冷 GET 时避免重复构造编解码器。
pub const DEFAULT_CAPACITY: usize = 32;

/// 按 `(data, parity, shard_size)` 缓存 [`Codec`] 外壳的 LRU 缓存。
///
/// `Mutex` 提供内部可变性，因此 `get` / `len` 只取 `&self`。
pub struct CodecCache {
    inner: Mutex<LruCache<(usize, usize, usize), Codec>>,
}

impl CodecCache {
    /// 容量下限为 1。容量 0 无意义（什么都存不下），收敛到 1 而不是 panic。
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity).unwrap_or(NonZeroUsize::MIN);
        Self {
            inner: Mutex::new(LruCache::new(cap)),
        }
    }

    /// 命中则返回缓存的 `Codec`（`Copy`，不泄漏锁内引用）；
    /// 未命中则先 `Codec::new`，**只有 `Ok` 才插入缓存**，`Err` 直接上抛。
    pub fn get(
        &self,
        data: usize,
        parity: usize,
        shard_size: usize,
    ) -> Result<Codec, ErasureConstructionError> {
        // Mutex 中毒（某线程持锁时 panic）不应把 panic 传播给调用方：
        // 缓存里存的是纯值 `Codec`，没有跨线程共享的破损不变量，
        // 直接取回内部数据继续用即可。
        let mut cache = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        let key = (data, parity, shard_size);
        if let Some(codec) = cache.get(&key) {
            return Ok(*codec);
        }
        // 构造失败不插入：无效几何不该占用缓存槽位。
        let codec = Codec::new(data, parity, shard_size)?;
        cache.put(key, codec);
        Ok(codec)
    }

    /// 当前缓存条目数。
    pub fn len(&self) -> usize {
        let cache = self.inner.lock().unwrap_or_else(PoisonError::into_inner);
        cache.len()
    }

    /// 缓存是否为空。与 [`len`](Self::len) 成对存在，避免
    /// `clippy::len_without_is_empty`（`-D warnings` 下会报错）。
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_same_codec_for_same_key() {
        let c = CodecCache::new(4);
        let a = c.get(4, 2, 1024).unwrap();
        let b = c.get(4, 2, 1024).unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn evicts_beyond_capacity() {
        let c = CodecCache::new(2);
        c.get(4, 2, 1024).unwrap();
        c.get(6, 3, 1024).unwrap();
        c.get(8, 4, 1024).unwrap();
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn invalid_geometry_is_not_cached() {
        let c = CodecCache::new(4);
        assert!(c.get(0, 0, 1024).is_err());
        assert_eq!(c.len(), 0);
    }
}
