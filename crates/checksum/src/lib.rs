//! bitrot 校验和。对应 DESIGN §11。
//!
//! 落盘格式为逐块交错 `[hash(32B)][data]`，由 `disk` 层的 writer/reader 负责，
//! 本 crate 只提供哈希与尺寸计算。

pub const HASH_LEN: usize = 32;

/// 具名 key 常量。**不得内联到调用点**——改变它会让所有既有数据校验失败。
///
/// 长度必须恰好 32：`"rustorage.bitrot.key.v1"` 是 23 字节，补 **9** 个 `\0`
/// 凑满 32。下面的编译期断言兜住数错 `\0` 的情况（数错会直接编译失败）。
pub const BITROT_KEY_V1: [u8; 32] = *b"rustorage.bitrot.key.v1\0\0\0\0\0\0\0\0\0";
const _: () = assert!(BITROT_KEY_V1.len() == 32);

pub fn bitrot_hash(block: &[u8]) -> [u8; HASH_LEN] {
    let mut h = blake3::Hasher::new_keyed(&BITROT_KEY_V1);
    h.update(block);
    *h.finalize().as_bytes()
}

/// 单个分片落盘后的字节数：每个 block 前加一个摘要。
pub fn bitrot_size(size: u64, shard_size: u64) -> u64 {
    if size == 0 {
        return 0;
    }
    let blocks = size.div_ceil(shard_size);
    blocks * HASH_LEN as u64 + size
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// 钉住常量：首次运行时用 `cargo test kat_pin -- --nocapture` 打印实际值，
    /// 粘贴回 `KAT_EMPTY_V1`。作用是在依赖升级后立刻发现哈希行为变化。
    const KAT_EMPTY_V1: [u8; 32] = [
        93, 43, 197, 35, 210, 94, 225, 132, 41, 28, 118, 217, 14, 137, 198, 93, 135, 153, 74, 46,
        36, 125, 237, 149, 10, 220, 223, 110, 196, 247, 93, 253,
    ];

    #[test]
    fn kat_pin() {
        let got = bitrot_hash(b"");
        if KAT_EMPTY_V1 == [0u8; 32] {
            println!("KAT_EMPTY_V1 = {:?}", got);
            return;
        }
        assert_eq!(got, KAT_EMPTY_V1, "bitrot hash behavior changed!");
    }

    #[test]
    fn deterministic() {
        assert_eq!(bitrot_hash(b"hello"), bitrot_hash(b"hello"));
    }

    #[test]
    fn detects_single_bit_flip() {
        let a = bitrot_hash(b"aaaaaaaa");
        let b = bitrot_hash(b"aaaaaaab");
        assert_ne!(a, b);
    }

    #[test]
    fn size_arithmetic() {
        assert_eq!(bitrot_size(0, 1024), 0);
        assert_eq!(bitrot_size(1, 1024), 32 + 1);
        assert_eq!(bitrot_size(1024, 1024), 32 + 1024);
        assert_eq!(bitrot_size(1025, 1024), 64 + 1025);
    }

    proptest! {
        #[test]
        fn size_is_monotonic(a in 0u64..1_000_000, b in 0u64..1_000_000) {
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            prop_assert!(bitrot_size(lo, 1024) <= bitrot_size(hi, 1024));
        }
    }
}
