//! 动作名：s3s 的 op 名 ↔ AWS 的动作名（设计 §4.2）。

/// 把 s3s 的 op 名规范化成 AWS 的动作名后缀。
///
/// **只列与 s3s 命名不一致的**，其余同名返回。s3s 构造 `S3AccessContext` 时传的是
/// `op.name()`（`s3s-0.17.0/src/ops/mod.rs:612`），取值如 `GetObject` / `ListObjectsV2`。
pub fn canonical_op(op_name: &str) -> &str {
    match op_name {
        // AWS 的 `ListBuckets` API 对应的动作叫 `s3:ListAllMyBuckets`。
        "ListBuckets" => "ListAllMyBuckets",
        // 两个列举 API 共用 `s3:ListBucket`。
        "ListObjects" | "ListObjectsV2" => "ListBucket",
        // 批量删除用的仍是 `s3:DeleteObject`。
        "DeleteObjects" => "DeleteObject",
        other => other,
    }
}

/// 把策略里写的动作串（`s3:GetObject`）规范化成同样的后缀。
///
/// **两侧过的是同一个函数**，于是 `s3:ListBucket`（AWS 习惯）与
/// `s3:ListObjectsV2`（s3s 的名字）都能命中实际的操作。
///
/// 不是 `s3:` 开头的一律原样返回，于是它匹配不上任何 s3 动作——**失败方向是拒绝**。
pub fn canonical_policy_action(action: &str) -> &str {
    match action.strip_prefix("s3:") {
        Some(rest) => canonical_op(rest),
        None => action,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 表里每一行。**这张表存在的唯一目的**是让照抄 AWS 文档写的策略可以直接用。
    #[test]
    fn aws_action_names_resolve_to_our_op_names() {
        assert_eq!(canonical_op("ListBuckets"), "ListAllMyBuckets");
        assert_eq!(canonical_op("ListObjects"), "ListBucket");
        assert_eq!(canonical_op("ListObjectsV2"), "ListBucket");
        assert_eq!(canonical_op("DeleteObjects"), "DeleteObject");
    }

    /// 其余的 op 名同名返回。
    #[test]
    fn other_op_names_pass_through() {
        for op in [
            "GetObject",
            "PutObject",
            "HeadObject",
            "DeleteObject",
            "CreateBucket",
            "DeleteBucket",
            "HeadBucket",
            "GetBucketLocation",
        ] {
            assert_eq!(canonical_op(op), op);
        }
    }

    /// 策略侧剥掉 `s3:` 再规范化——**与 op 侧过的是同一个函数**，
    /// 所以 `s3:ListBucket`、`s3:ListObjects`、`s3:ListObjectsV2` 三者等价。
    #[test]
    fn policy_actions_normalise_the_same_way() {
        assert_eq!(canonical_policy_action("s3:GetObject"), "GetObject");
        assert_eq!(canonical_policy_action("s3:ListBucket"), "ListBucket");
        assert_eq!(canonical_policy_action("s3:ListObjects"), "ListBucket");
        assert_eq!(canonical_policy_action("s3:ListObjectsV2"), "ListBucket");
        assert_eq!(
            canonical_policy_action("s3:ListAllMyBuckets"),
            "ListAllMyBuckets"
        );
        assert_eq!(canonical_policy_action("s3:DeleteObjects"), "DeleteObject");
    }

    /// 通配符原样保留——匹配交给 `glob`，这里只管名字。
    #[test]
    fn wildcards_survive_normalisation() {
        assert_eq!(canonical_policy_action("s3:*"), "*");
        assert_eq!(canonical_policy_action("s3:Get*"), "Get*");
    }

    /// **不是 `s3:` 开头的一律不匹配任何 s3 动作**（失败方向是拒绝）。
    /// `admin:` / `sts:` 是 M5 之后的事，现在它们不该意外命中。
    #[test]
    fn non_s3_actions_never_match() {
        assert_eq!(
            canonical_policy_action("admin:ServerInfo"),
            "admin:ServerInfo"
        );
        assert_eq!(canonical_policy_action("*"), "*");
        assert_ne!(
            canonical_policy_action("admin:DeleteObject"),
            "DeleteObject"
        );
    }
}
