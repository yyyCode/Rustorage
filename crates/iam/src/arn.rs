//! 资源 ARN 的构造（设计 §4.3）。
//!
//! **格式只在这里定义一处**——策略是照着它写的。

/// 根路径（`ListBuckets`）用的资源。
pub const ARN_ROOT: &str = "arn:aws:s3:::*";

/// 桶级操作（`HeadBucket` / `ListBucket` / `CreateBucket` / `DeleteBucket`）。
pub fn arn_bucket(bucket: &str) -> String {
    format!("arn:aws:s3:::{bucket}")
}

/// 对象级操作（`GetObject` / `PutObject` / `HeadObject` / `DeleteObject`）。
pub fn arn_object(bucket: &str, key: &str) -> String {
    format!("arn:aws:s3:::{bucket}/{key}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_is_a_bare_star() {
        assert_eq!(ARN_ROOT, "arn:aws:s3:::*");
    }

    #[test]
    fn bucket_has_no_slash() {
        assert_eq!(arn_bucket("photos"), "arn:aws:s3:::photos");
    }

    /// key 里的 `/` **原样保留**，不转义——AWS 就是这么做的，
    /// 而策略里的 `photos/*` 靠 `*` 跨 `/` 来匹配它。
    #[test]
    fn object_keeps_slashes_in_the_key() {
        assert_eq!(
            arn_object("photos", "a/b.jpg"),
            "arn:aws:s3:::photos/a/b.jpg"
        );
    }

    /// 空 key（例如 `GET /bucket/` 被解析成对象路径）也要能构造出 ARN。
    #[test]
    fn empty_key_is_allowed() {
        assert_eq!(arn_object("photos", ""), "arn:aws:s3:::photos/");
    }
}
