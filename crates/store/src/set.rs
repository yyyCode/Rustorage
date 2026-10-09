//! erasure set：盘数 / 分片数几何与 quorum 规则（Task 4.3）。
//!
//! **盘数与分片数一一对应：每块盘持有一份分片。** 例如 `total = 6, parity = 2`
//! 即 4+2 配置，需要 6 块盘。

use std::sync::Arc;

use rstore_disk::DiskAPI;
use rstore_erasure::CodecCache;

use crate::error::StoreError;

/// `CodecCache` 的容量。一个 set 的分片几何（`data`/`parity`/`shard_size`）组合很少，
/// 常数容量足够；取与 DESIGN §10.2 默认值同量级的 64。
const CODEC_CACHE_CAPACITY: usize = 64;

/// DESIGN §10.1 的默认 parity 表。
pub fn default_parity(total: u8) -> u8 {
    match total {
        0 | 1 => 0,
        2..=3 => 1,
        4..=5 => 2,
        6..=7 => 3,
        _ => 4,
    }
}

pub fn read_quorum(total: u8, parity: u8) -> u8 {
    total - parity
}

pub fn write_quorum(data: u8, parity: u8) -> u8 {
    if data == parity {
        data + 1
    } else {
        data
    }
}

pub fn delete_quorum(total: u8) -> u8 {
    total / 2 + 1
}

/// 一个 erasure set 的盘数 = N = data + parity。
/// **盘数与分片数一一对应：每块盘持有一份分片。**
/// 例如 `total = 6, parity = 2` 即 4+2 配置，需要 6 块盘。
pub struct ErasureSet {
    disks: Vec<Option<Arc<dyn DiskAPI>>>,
    data: u8,   // = total - parity
    parity: u8, // = total - data
    codec_cache: CodecCache,
}

impl ErasureSet {
    /// 从槽位视图构造。`disks.len()` 即 `total = data + parity`；
    /// `parity` 必须 `< total`，否则没有数据分片，任何编码都无意义。
    ///
    /// `Vec` 里的 `None` 表示该槽位的盘已经掉线（这类槽位仍占据一个分片下标）。
    pub fn new(disks: Vec<Option<Arc<dyn DiskAPI>>>, parity: u8) -> Result<Self, StoreError> {
        let len = disks.len();
        // `total` 以 u8 参与几何运算；超过 255 块盘无法表示，直接在构造期拒绝，
        // 胜过把截断后的盘数带到读取/编码路径里再出问题。
        let total = u8::try_from(len).map_err(|_| {
            StoreError::Internal(format!("erasure set has {len} disks, exceeds u8::MAX"))
        })?;
        if parity >= total {
            return Err(StoreError::Internal(format!(
                "parity {parity} must be < total {total}: no data shards left"
            )));
        }
        Ok(Self {
            disks,
            data: total - parity,
            parity,
            codec_cache: CodecCache::new(CODEC_CACHE_CAPACITY),
        })
    }

    /// 槽位视图，下标即分片下标。`None` = 该盘掉线。Task 4.4/4.6/4.7 都要按槽位遍历。
    pub fn disks(&self) -> &[Option<Arc<dyn DiskAPI>>] {
        &self.disks
    }

    pub fn data(&self) -> u8 {
        self.data
    }

    pub fn parity(&self) -> u8 {
        self.parity
    }

    pub fn total(&self) -> u8 {
        self.data + self.parity
    }

    pub fn read_quorum(&self) -> u8 {
        read_quorum(self.total(), self.parity)
    }

    pub fn write_quorum(&self) -> u8 {
        write_quorum(self.data, self.parity)
    }

    /// 取第 `index` 个槽位的盘。越界或该槽位掉线时返回 `None`。
    ///
    /// 返回的是 `Arc` 的一份克隆而非引用：调用方通常要把它搬进 `async` 任务里，
    /// 借用会与遍历 `&self` 的生命周期纠缠在一起。
    pub fn pick_slot_for(&self, index: usize) -> Option<Arc<dyn DiskAPI>> {
        self.disks.get(index).and_then(|slot| slot.clone())
    }

    /// 当前在线（槽位非 `None`）的盘数。quorum 判定就是拿它与 `write_quorum()` /
    /// `read_quorum()` 比较。
    pub fn available_disks(&self) -> usize {
        self.disks.iter().filter(|slot| slot.is_some()).count()
    }

    /// 该 set 的编解码器缓存。读取/编码路径按 `(data, parity, shard_size)` 取用。
    pub fn codec_cache(&self) -> &CodecCache {
        &self.codec_cache
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_parity_matches_design_table() {
        assert_eq!(default_parity(1), 0);
        assert_eq!(default_parity(2), 1);
        assert_eq!(default_parity(3), 1);
        assert_eq!(default_parity(4), 2);
        assert_eq!(default_parity(5), 2);
        assert_eq!(default_parity(6), 3);
        assert_eq!(default_parity(7), 3);
        assert_eq!(default_parity(8), 4);
        assert_eq!(default_parity(16), 4);
    }

    #[test]
    fn parity_never_exceeds_half() {
        for n in 2..=16u8 {
            assert!(default_parity(n) * 2 <= n, "n={n}");
        }
    }

    #[test]
    fn write_quorum_bumps_on_symmetric_geometry() {
        assert_eq!(write_quorum(4, 2), 4); // 4+2: 不对称
        assert_eq!(write_quorum(3, 3), 4); // 3+3: 对称，+1
                                           // 4+2 的 read_quorum：入参是 `(total, parity)`（见实现与
                                           // `ErasureSet::read_quorum`），故是 6 而非 4。
        assert_eq!(read_quorum(6, 2), 4);
    }

    #[test]
    fn delete_quorum_is_strict_majority() {
        assert_eq!(delete_quorum(6), 4);
        assert_eq!(delete_quorum(1), 1);
        // 恰好过半的边界：5 的一半是 2，加 1 得 3。
        assert_eq!(delete_quorum(5), 3);
    }

    /// 全掉线的空位也能构造：`new` 只关心几何，不关心盘此刻是否可用。
    #[test]
    fn geometry_derives_data_from_total_and_parity() {
        let set = ErasureSet::new(vec![None; 6], 2).unwrap();
        assert_eq!(set.total(), 6);
        assert_eq!(set.data(), 4);
        assert_eq!(set.parity(), 2);
        assert_eq!(set.disks().len(), 6);
        assert_eq!(set.read_quorum(), 4);
        assert_eq!(set.write_quorum(), 4);
        assert_eq!(set.available_disks(), 0);
        assert!(set.pick_slot_for(0).is_none());
        // 越界槽位同样返回 None，不 panic。
        assert!(set.pick_slot_for(99).is_none());
    }

    /// 对称几何：write_quorum 要在 data 上加一。
    #[test]
    fn symmetric_geometry_bumps_write_quorum() {
        let set = ErasureSet::new(vec![None; 6], 3).unwrap();
        assert_eq!(set.data(), 3);
        assert_eq!(set.parity(), 3);
        assert_eq!(set.write_quorum(), 4);
        assert_eq!(set.read_quorum(), 3);
    }

    /// parity == total 时没有数据分片，必须拒绝。
    #[test]
    fn rejects_parity_not_below_total() {
        assert!(ErasureSet::new(vec![None; 2], 2).is_err());
        assert!(ErasureSet::new(vec![None; 1], 1).is_err());
    }

    /// 空集合：`total == 0`，parity 必然 >= total，也拒绝。
    #[test]
    fn rejects_empty_set() {
        assert!(ErasureSet::new(vec![], 0).is_err());
    }

    /// parity = 0 合法（纯镜像），data 等于 total。
    #[test]
    fn zero_parity_is_allowed() {
        let set = ErasureSet::new(vec![None; 3], 0).unwrap();
        assert_eq!(set.data(), 3);
        assert_eq!(set.parity(), 0);
        assert_eq!(set.read_quorum(), 3);
        assert_eq!(set.write_quorum(), 3);
    }
}
