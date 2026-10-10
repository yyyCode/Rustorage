//! 对象 key 校验（Task 5.7）。

use rstore_api::ApiError;

/// 桶名校验。返回 `ApiError::InvalidBucketName`（→ 对外 400 `InvalidBucketName`）。
///
/// **只加一条规则**：首字符不得为 `_`。这不是「桶名校验已完整」——长度、字符集、
/// 大小写、`..` 等一概**尚未校验**，本函数刻意不顺手补上（那是一次与本设计无关、
/// 影响面更大的兼容性变更）。这一条规则的存在只为让 `/_console` 成为**任何合法桶名
/// 都构不成**的路径（设计 §3.2/§3.3）。
///
/// 注意：`console`（不带下划线）**必须继续合法**。
pub fn validate_bucket_name(bucket: &str) -> Result<(), ApiError> {
    if bucket.starts_with('_') {
        return Err(ApiError::InvalidBucketName);
    }
    Ok(())
}

/// 对象 key 校验。返回 `ApiError::InvalidObjectName`（→ 对外 400 `InvalidArgument`）。
///
/// 两条独立规则，都由安全/数据正确性而来：
///
/// 1. 第一段不得以 [`rstore_common::consts::RESERVED_PREFIX`] 开头 —— DESIGN §6.3。
///    只限第一段，深层允许。
/// 2. **逐段**不得为空串、`.` 或 `..` —— 这三个形状会被 `fsx::resolve` 折叠，
///    让两个不同的 S3 key 落到同一个文件上。
pub fn validate_object_key(key: &str) -> Result<(), ApiError> {
    let mut segments = key.split('/');
    // `key` 为空时 `split` 产出单个空段，被下面的循环拒掉——不必先判空。
    if segments
        .next()
        .is_some_and(|first| first.starts_with(rstore_common::consts::RESERVED_PREFIX))
    {
        return Err(ApiError::InvalidObjectName);
    }
    // 规则 2。**注意这同时覆盖了空 key**。`..b` / `b.` 只是普通名字，不能误伤——
    // 所以是逐段**相等**比较，不是 `starts_with(".")`。
    if key
        .split('/')
        .any(|seg| seg.is_empty() || seg == "." || seg == "..")
    {
        return Err(ApiError::InvalidObjectName);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_object_key_with_reserved_first_segment() {
        // DESIGN §6.3：对象 key 的第一段不得以 `.rstore` 开头
        assert!(validate_object_key(".rstore/x").is_err());
        assert!(validate_object_key(".rstore.uploads/abc").is_err());
        assert!(validate_object_key(".rstore.sys").is_err());
        // 只有第一段受限，深层路径允许
        assert!(validate_object_key("a/.rstore/x").is_ok());
        assert!(validate_object_key("normal/key").is_ok());
        assert!(validate_object_key("").is_err());
    }

    #[test]
    fn rejects_keys_that_would_alias_on_disk() {
        // `crates/disk/src/fsx.rs` 的 `resolve()` 逐 Component 拼接：`Component::CurDir`
        // 被丢弃、空段被折叠。于是 a//b、a/./b、a/、/a 都会落到与 a/b 或 a 相同的文件上，
        // 而 S3 把它们当成**不同的 key**。放行 = 后写的静默覆盖先写的，LIST 又只列一个，
        // 中间没有任何报错。
        //
        // 归一化（把 a//b 改写成 a/b 后放行）不成立：那只是把同一个静默别名换个方向——
        // PUT a//b 写进 a/b，之后 GET a/b 会读到它。真正的修法是**在盘上编码 key**
        // （Phase 2 改 fsx）。s3s 自带的 `normalize_forward_slash_path` 开关也不能替代：
        // 它只 `filter(|s| !s.is_empty())`，只处理空段，`.` 与 `..` 原样留下。
        assert!(validate_object_key("a//b").is_err(), "空段会折成 a/b");
        assert!(validate_object_key("a/./b").is_err(), "`.` 段会被丢弃");
        assert!(validate_object_key("a/").is_err(), "尾随斜杠会折成 a");
        assert!(validate_object_key("/a").is_err(), "前导斜杠会折成 a");
        // `..` 在盘层是 Fatal(FatalKind::PathEscape)——不在这里挡，到 S3 层就是 500，
        // 而客户端拿到的应该是 400。
        assert!(validate_object_key("a/../b").is_err(), "禁止上溯段");
        assert!(validate_object_key("..").is_err());
        // 干净的多段 key 照常通过。
        assert!(validate_object_key("a/b/c").is_ok());
        assert!(validate_object_key("a/..b").is_ok(), "`..b` 只是普通名字");
        assert!(validate_object_key("a/b.").is_ok(), "`b.` 只是普通名字");
    }

    #[test]
    fn rejects_bucket_name_with_leading_underscore() {
        // 设计 §3.2：只有「首字符是 `_`」这一条被禁，**不做**完整的 DNS 风格校验。
        // 理由是 `/_console` 要成为任何合法桶名都构不成的路径，而扩大校验面是
        // 与本设计无关的兼容性变更，会掩盖真正的改动。
        assert!(validate_bucket_name("_console").is_err());
        assert!(validate_bucket_name("_x").is_err());
        assert!(validate_bucket_name("_").is_err());
    }

    #[test]
    fn accepts_ordinary_bucket_names() {
        // `console` 必须**继续合法**：这正是选用 `/_console`（而不是 `/console`）
        // 的全部意义——不把一个正常名字变成禁词。这条断言是那个决定的门闩，
        // 谁把规则改成「禁止 console」都会在这里红。
        for ok in ["console", "x_y", "test-bucket", "a", "my.bucket", "123"] {
            assert!(validate_bucket_name(ok).is_ok(), "{ok} 应当合法");
        }
    }

    #[test]
    fn reserved_prefix_constant_is_not_empty() {
        // 防止有人在重构中把常量改成空串，让校验静默失效
        assert!(!rstore_common::consts::RESERVED_PREFIX.is_empty());
    }
}
