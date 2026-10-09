//! 跨层共享的常量与门限。

pub const DEFAULT_INLINE_BLOCK: u64 = 128 * 1024;

/// 版本化桶取 1/8；MVP 未启用版本化，但函数签名保留该维度。
pub fn should_inline(size: u64, versioned_bucket: bool) -> bool {
    let threshold = if versioned_bucket {
        DEFAULT_INLINE_BLOCK / 8
    } else {
        DEFAULT_INLINE_BLOCK
    };
    size <= threshold
}
