//! 跨层共享的常量与门限。

pub const DEFAULT_INLINE_BLOCK: u64 = 128 * 1024;

/// 用户可见命名空间里的保留前缀（DESIGN §6.3）：对象 key 的首段、
/// 以及盘根 / 桶根下的系统目录都不得以它开头。
/// **store 的目录遍历与 s3 的 key 校验都引用这一个常量，不得内联字面量。**
pub const RESERVED_PREFIX: &str = ".rstore";

/// 版本化桶取 1/8；MVP 未启用版本化，但函数签名保留该维度。
pub fn should_inline(size: u64, versioned_bucket: bool) -> bool {
    let threshold = if versioned_bucket {
        DEFAULT_INLINE_BLOCK / 8
    } else {
        DEFAULT_INLINE_BLOCK
    };
    size <= threshold
}
