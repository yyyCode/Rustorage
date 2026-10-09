//! 分片分布排列：把逻辑块号映射到物理槽位，使同一对象的各分片在盘间错开。
//!
//! 对应 DESIGN §9.3。算法固定为 CRC32C 旋转；**不实现多代算法**，
//! 稳定性由「deployment id 不可变」这一约束保证，而非算法版本兜底。

use rstore_common::error::PipeError;

/// 返回长度为 `n` 的排列，元素取值 `1..=n`。
/// `dist[k] - 1` 即逻辑块 `k` 的物理槽位下标。
pub fn distribution(object_key: &str, n: u8) -> Result<Vec<u8>, PipeError> {
    if !(2..=16).contains(&n) {
        return Err(PipeError::InvalidShardCount(n));
    }
    let n_usize = n as usize;
    let start = (crc32c::crc32c(object_key.as_bytes()) as usize) % n_usize;
    let mut d = Vec::with_capacity(n_usize);
    for k in 1..=n_usize {
        d.push(((start + k) % n_usize + 1) as u8);
    }
    Ok(d)
}

/// 校验 `d` 是 `1..=d.len()` 的严格排列。
/// **绝不 panic**：所有访问都经过范围检查（DESIGN §9.3）。
pub fn is_valid_distribution(d: &[u8]) -> bool {
    if d.is_empty() || d.len() > 16 {
        return false;
    }
    let mut seen: u32 = 0;
    for &x in d {
        if x == 0 || x as usize > d.len() {
            return false;
        }
        let bit = 1u32 << (x - 1);
        if seen & bit != 0 {
            return false;
        }
        seen |= bit;
    }
    seen == (1u32 << d.len()) - 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn rejects_out_of_range_n() {
        assert!(distribution("k", 1).is_err());
        assert!(distribution("k", 17).is_err());
    }

    #[test]
    fn is_valid_distribution_rejects_malformed_slices() {
        // distribution() 输出的守门人，DESIGN §9.3 要求它绝不 panic。
        // 属性测试只喂 distribution() 的合法输出，拒绝路径完全没被覆盖——
        // 而畸形输入正是它唯一会被真正调用的场合，所以边界要在这里钉住。
        assert!(is_valid_distribution(&[1])); // 长度 1 合法
        assert!(is_valid_distribution(&[3, 1, 2])); // 循环移位合法
        assert!(!is_valid_distribution(&[])); // 空
        assert!(!is_valid_distribution(&[0])); // 0 不在 1..=len 内
        assert!(!is_valid_distribution(&[1, 1])); // 重复
        assert!(!is_valid_distribution(&[1, 3])); // 3 > len = 2
        let seventeen: Vec<u8> = (1..=17).collect();
        assert!(!is_valid_distribution(&seventeen)); // len > 16
    }

    proptest! {
        #[test]
        fn always_a_strict_permutation(key in ".*", n in 2u8..=16) {
            let d = distribution(&key, n).unwrap();
            prop_assert_eq!(d.len(), n as usize);
            prop_assert!(is_valid_distribution(&d));
            let mut sorted = d.clone();
            sorted.sort();
            prop_assert_eq!(sorted, (1..=n).collect::<Vec<_>>());
        }

        #[test]
        fn deterministic_for_same_input(key in ".*", n in 2u8..=16) {
            prop_assert_eq!(distribution(&key, n).unwrap(), distribution(&key, n).unwrap());
        }
    }
}
