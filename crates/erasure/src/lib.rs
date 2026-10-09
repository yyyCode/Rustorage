//! 纠删码门面与编解码器缓存。只依赖 common。

pub mod error;

pub use error::{ErasureConstructionError, ErasureError};

/// 纠删码门面。对应 DESIGN §10。
/// 上层只见这个接口，不感知底层库（`reed-solomon-simd`）的存在。
/// 这样库被替换时，只有在 `encode`/`decode` 内部需要改动。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Codec {
    data: usize,
    parity: usize,
    shard_size: usize,
}

impl Codec {
    /// 校验几何合法性：`2 <= data+parity <= 16`、`parity >= 1`、`shard_size > 0`。
    pub fn new(
        data: usize,
        parity: usize,
        shard_size: usize,
    ) -> Result<Self, ErasureConstructionError> {
        let total = data + parity;
        if !(2..=16).contains(&total) || parity < 1 {
            return Err(ErasureConstructionError::InvalidGeometry { data, parity });
        }
        if shard_size == 0 {
            return Err(ErasureConstructionError::ZeroShardSize);
        }
        Ok(Self {
            data,
            parity,
            shard_size,
        })
    }

    pub fn data_shards(&self) -> usize {
        self.data
    }

    pub fn parity_shards(&self) -> usize {
        self.parity
    }

    pub fn total_shards(&self) -> usize {
        self.data + self.parity
    }

    /// 输入 `data` 个等长分片，输出 `parity` 个校验分片。
    pub fn encode(&self, data_shards: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, ErasureError> {
        if data_shards.len() != self.data {
            return Err(ErasureError::WrongShardCount {
                expected: self.data,
                got: data_shards.len(),
            });
        }
        if data_shards.iter().any(|s| s.len() != self.shard_size) {
            return Err(ErasureError::UnequalShardLength);
        }

        reed_solomon_simd::encode(self.data, self.parity, data_shards)
            .map_err(|e| ErasureError::Backend(format!("{e}")))
    }

    /// 输入长度为 `total_shards` 的槽位数组（`None` 表示该槽位缺失），
    /// 输出全部 `data` 个数据分片。缺失数 > `parity` 时返回 `ErasureError::TooFewShards`。
    pub fn decode(&self, slots: &[Option<Vec<u8>>]) -> Result<Vec<Vec<u8>>, ErasureError> {
        if slots.len() != self.total_shards() {
            return Err(ErasureError::WrongShardCount {
                expected: self.total_shards(),
                got: slots.len(),
            });
        }
        if slots.iter().flatten().any(|s| s.len() != self.shard_size) {
            return Err(ErasureError::UnequalShardLength);
        }

        let available = slots.iter().filter(|s| s.is_some()).count();
        if available < self.data {
            return Err(ErasureError::TooFewShards {
                available,
                needed: self.data,
            });
        }

        // 库的索引：数据分片 `0..data` 与槽位下标一致；
        // 校验分片在库中是 `0..parity`（从 0 开始），对应槽位 `data..total`。
        let originals = slots[..self.data]
            .iter()
            .enumerate()
            .filter_map(|(i, s)| s.as_ref().map(|d| (i, d)));
        let recovery = slots[self.data..]
            .iter()
            .enumerate()
            .filter_map(|(j, s)| s.as_ref().map(|d| (j, d)));

        // 库只返回「被重建的」原始分片；已在槽位中的分片原样保留。
        let restored = reed_solomon_simd::decode(self.data, self.parity, originals, recovery)
            .map_err(|e| ErasureError::Backend(format!("{e}")))?;

        let mut out = Vec::with_capacity(self.data);
        for (i, slot) in slots[..self.data].iter().enumerate() {
            match slot {
                Some(shard) => out.push(shard.clone()),
                None => match restored.get(&i) {
                    Some(shard) => out.push(shard.clone()),
                    None => {
                        return Err(ErasureError::Backend(format!(
                            "backend did not restore original shard {i}"
                        )))
                    }
                },
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn pad_chunks(payload: &[u8], count: usize, shard_size: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::with_capacity(count);
        for i in 0..count {
            let lo = (i * shard_size).min(payload.len());
            let hi = ((i + 1) * shard_size).min(payload.len());
            let mut s = vec![0u8; shard_size];
            s[..hi - lo].copy_from_slice(&payload[lo..hi]);
            out.push(s);
        }
        out
    }

    #[test]
    fn rejects_invalid_geometry() {
        assert!(Codec::new(1, 0, 1024).is_err());
        assert!(Codec::new(17, 1, 1024).is_err());
        assert!(Codec::new(4, 0, 1024).is_err());
        assert!(Codec::new(4, 2, 0).is_err());
    }

    proptest! {
        /// 核心不变量：任意数据、任意几何，编码后再用任意 >= data 份分片解码，结果恒等。
        #[test]
        fn roundtrip_with_arbitrary_loss(
            data in 1usize..=8,
            parity in 1usize..=8,
            payload in prop::collection::vec(any::<u8>(), 1..2048),
            drop_mask in any::<u16>(),
        ) {
            prop_assume!(data + parity <= 16);
            let shard_size = 1024;
            let codec = Codec::new(data, parity, shard_size).unwrap();
            let originals = pad_chunks(&payload, data, shard_size);
            let checks = codec.encode(&originals).unwrap();
            prop_assert_eq!(checks.len(), parity);

            // 拼出全部 N 个槽位
            let mut slots: Vec<Option<Vec<u8>>> =
                originals.iter().cloned().map(Some).collect();
            slots.extend(checks.into_iter().map(Some));

            // 按 drop_mask 丢弃若干槽位
            let mut kept = 0;
            for (i, slot) in slots.iter_mut().enumerate() {
                if drop_mask & (1 << i) != 0 {
                    *slot = None;
                } else {
                    kept += 1;
                }
            }
            prop_assume!(kept >= data);

            let recovered = codec.decode(&slots).unwrap();
            prop_assert_eq!(recovered.len(), data);
            for (a, b) in recovered.iter().zip(originals.iter()) {
                prop_assert_eq!(a, b);
            }
        }

        /// 丢太多必须报错，绝不返回错误数据。
        #[test]
        fn too_few_shards_fails_closed(
            data in 2usize..=6,
            parity in 1usize..=4,
            payload in prop::collection::vec(any::<u8>(), 1..512),
        ) {
            let shard_size = 512;
            let codec = Codec::new(data, parity, shard_size).unwrap();
            let originals = pad_chunks(&payload, data, shard_size);
            let checks = codec.encode(&originals).unwrap();
            let mut slots: Vec<Option<Vec<u8>>> =
                originals.iter().cloned().map(Some).collect();
            slots.extend(checks.into_iter().map(Some));

            // 只留 data-1 份
            let mut kept = 0;
            for s in slots.iter_mut() {
                if kept < data - 1 { kept += 1; } else { *s = None; }
            }
            // 注：`prop_assert!` 会把条件 stringify 后交给 `format!`，条件里的 `{ .. }`
            // 会被当成格式串而编译失败，故先算成 bool 再断言。
            let failed_closed =
                matches!(codec.decode(&slots), Err(ErasureError::TooFewShards { .. }));
            prop_assert!(failed_closed);
        }
    }
}
