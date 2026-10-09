//! 池：`ErasureSet` 的集合与对象到 set 的路由（Task 4.3）。

use std::sync::Arc;

use crate::error::StoreError;
use crate::set::ErasureSet;

/// 一个池子持有若干个 erasure set。MVP 下 `set_count == 1`（所有盘同属一个 set），
/// 因此路由是恒等映射。
pub struct Pool {
    sets: Vec<Arc<ErasureSet>>,
}

impl Pool {
    /// `sets` 不得为空：一个没有 set 的池子任何操作都做不了，
    /// 让它在构造期就失败，胜过让每个调用点各自处理 `Vec` 为空。
    pub fn new(sets: Vec<Arc<ErasureSet>>) -> Result<Self, StoreError> {
        if sets.is_empty() {
            return Err(StoreError::Internal(
                "pool must contain at least one erasure set".into(),
            ));
        }
        Ok(Self { sets })
    }

    pub fn sets(&self) -> &[Arc<ErasureSet>] {
        &self.sets
    }

    /// 按对象键选 set。MVP 下恒等返回第一个。
    pub fn pick_set(&self, key: &str) -> &ErasureSet {
        // MVP: set_count == 1，路由恒等（`key` 未使用）。
        // 多 set 时的 SipHash 路由见 DESIGN §9.2（Phase 3）。
        let _ = key;
        &self.sets[0]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_pool() {
        assert!(Pool::new(vec![]).is_err());
    }

    /// 单 set 池子：`pick_set` 恒等返回唯一那个 set，且 `sets()` 原样透出。
    #[test]
    fn single_set_pool_returns_that_set() {
        let set = Arc::new(ErasureSet::new(vec![None; 6], 2).unwrap());
        let pool = Pool::new(vec![Arc::clone(&set)]).unwrap();
        assert_eq!(pool.sets().len(), 1);
        // `ErasureSet` 不实现 `PartialEq`，用指针同一性断言「返回的就是那一个」。
        assert!(std::ptr::eq(pool.pick_set("any/key"), &*set));
    }
}
