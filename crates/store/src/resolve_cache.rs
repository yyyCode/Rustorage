//! `resolve_version` 的结果缓存（`metadata_cache` 机制）。
//!
//! **为什么值得缓存**：[`resolve_version`](crate::get::resolve_version) 要对
//! **每块盘**做一次 `list_dir`（发现候选版本目录）再逐盘读 `meta.xl` 仲裁。
//! 一个只做 HEAD 的客户端因此把整条元数据路径走了 N 遍，而它问的问题在同一秒里
//! 往往是同一个。
//!
//! **怎么保证不读到过期的**：每个分片一个**单调递增**的计数器。`put_object` /
//! `delete_object` 提交成功后把该 key 所在分片的计数 +1；缓存条目记下自己写入时的
//! 计数值，命中要求两者相等。于是：
//!
//! - 写成功之后，该分片的所有旧条目立刻失效（宁可多解析一次，不可给旧值）；
//! - 读者「取计数 → 解析 → 存条目」这中间若发生了写入，存储时会发现手上的计数
//!   已经不等于当前值，于是**放弃写入**而不是塞一份旧的进去；
//! - 计数器**只增不减**，所以即使条目被淘汰后重建，也不会出现
//!   「旧读者与新建条目恰好同号」这种 ABA。
//!
//! **刻意做粗**：一次写入让整个分片的条目一起失效，而不是只失效那一个 key。
//! 这是基准专用代码，粗粒度换来的是「正确性一眼可验」——精确到 key 的代际表
//! 本身也要有界，两套结构保持同步的收益不抵它的复杂度。见设计文档 §9。
//!
//! **谁不走这里**：写路径（`delete.rs`）、对账（`reconcile.rs`）与列举
//! （`list.rs`）。它们要的是盘上的**真相**，不是「最近为真」。见设计文档 §4.2。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::get::Resolved;

/// 分片数。按 key 哈希打散、不搞全局锁（§16.1 的意图），取 16 个定值——
/// 这是单节点、开关式的缓存，不需要跟着 CPU 核数走。
const SHARDS: usize = 16;

/// 每个分片最多驻留多少条目。超了**整片清空**：淘汰只会造成 miss、不会造成错值，
/// 所以这里不需要真正的 LRU 语义。
const SHARD_CAPACITY: usize = 1024;

/// 缓存键：`(bucket, key)`。
type CacheKey = (String, String);
/// 缓存值：`(写入时的分片计数, 解析结果)`。
type CacheEntry = (u64, Arc<Resolved>);

struct Shard {
    /// 单调递增。**只增不减**——见模块文档里的 ABA 论证。
    generation: AtomicU64,
    entries: Mutex<HashMap<CacheKey, CacheEntry>>,
}

/// 按 `(bucket, key)` 缓存的版本解析结果。
pub(crate) struct ResolveCache {
    shards: Vec<Shard>,
}

impl ResolveCache {
    pub(crate) fn new() -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| Shard {
                    generation: AtomicU64::new(0),
                    entries: Mutex::new(HashMap::new()),
                })
                .collect(),
        }
    }

    fn shard(&self, bucket: &str, key: &str) -> &Shard {
        // 默认哈希器足够：这里要的是「打散」，不是抗碰撞（碰撞只影响命中率）。
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        bucket.hash(&mut h);
        key.hash(&mut h);
        &self.shards[(h.finish() as usize) % SHARDS]
    }

    /// 取当前计数。**必须在解析之前取**，解析完成后连同结果一起交给 [`Self::store`]。
    pub(crate) fn generation(&self, bucket: &str, key: &str) -> u64 {
        self.shard(bucket, key).generation.load(Ordering::SeqCst)
    }

    /// 命中则返回条目；计数对不上、或压根没有条目，都返回 `None`。
    pub(crate) fn get(&self, bucket: &str, key: &str, gen: u64) -> Option<Arc<Resolved>> {
        let shard = self.shard(bucket, key);
        let entries = shard.entries.lock().expect("resolve cache poisoned");
        let (stored_gen, value) = entries.get(&(bucket.to_string(), key.to_string()))?;
        (*stored_gen == gen).then(|| Arc::clone(value))
    }

    /// 存入解析结果。**只在计数没变过时才写**：变了说明解析期间发生过写入，
    /// 这份结果已经是旧的，塞进去就等于制造一个错值。
    pub(crate) fn store(&self, bucket: &str, key: &str, gen: u64, value: Arc<Resolved>) {
        let shard = self.shard(bucket, key);
        if shard.generation.load(Ordering::SeqCst) != gen {
            return;
        }
        let mut entries = shard.entries.lock().expect("resolve cache poisoned");
        if entries.len() >= SHARD_CAPACITY {
            entries.clear();
        }
        entries.insert((bucket.to_string(), key.to_string()), (gen, value));
    }

    /// 一次写入**成功之后**调用：让这个 key 所在分片的所有条目失效。
    pub(crate) fn invalidate(&self, bucket: &str, key: &str) {
        self.shard(bucket, key)
            .generation
            .fetch_add(1, Ordering::SeqCst);
    }
}
