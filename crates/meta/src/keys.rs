//! 内部元数据键常量（DESIGN §8.5）。
//!
//! **键名字符串是载荷承重结构**：改动一个字节会让旧 `meta.xl` 读不出来。
//! 所有内部键一律以 [`RUSTORAGE_KEY_PREFIX`] 开头；用户侧键（`x-amz-*`）原样保留，
//! 不经此表。

/// 内部键的保留前缀。凡本模块的常量都必须以此开头。
pub const RUSTORAGE_KEY_PREFIX: &str = "x-rs-";

/// 存在该键即表示该版本携带内联数据（DESIGN §8.4）。
pub const INLINE_DATA: &str = "x-rs-inline-data";

/// 对象的真实字节数（纠删编码前的原始长度）。
pub const ACTUAL_SIZE: &str = "x-rs-actual-size";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_constants_are_pinned() {
        // 这些字符串是线格式的一部分，落盘后不可改（DESIGN §8.5）。
        // 测试里故意重复一遍字面量：常量被误改时这里会红。
        assert_eq!(RUSTORAGE_KEY_PREFIX, "x-rs-");
        assert_eq!(INLINE_DATA, "x-rs-inline-data");
        assert_eq!(ACTUAL_SIZE, "x-rs-actual-size");
    }

    #[test]
    fn all_internal_keys_use_reserved_prefix() {
        for k in [INLINE_DATA, ACTUAL_SIZE] {
            assert!(k.starts_with(RUSTORAGE_KEY_PREFIX), "{k} 未使用保留前缀");
        }
    }
}
