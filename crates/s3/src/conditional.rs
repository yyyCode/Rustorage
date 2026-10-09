//! 条件请求求值（Task 5.10，DESIGN §15.4）。
//!
//! s3s 已经把 `If-Match` / `If-None-Match` / `If-Modified-Since` /
//! `If-Unmodified-Since` 解析进 `GetObjectInput` / `HeadObjectInput`，但**从不求值**
//! ——求值是本层的活。
//!
//! **只做 GET / HEAD**：条件 PUT 需要原子的 conditional-put，在 S3 层「先查再写」
//! 是 TOCTOU，不能这么补。求值也**只放在 s3 层**，不搬进 `ObjectStore`：那会把 s3s 的
//! `ETagCondition` / `Timestamp` 拖进 `rstore-api`，而它的 allowlist 里没有 s3s。

use std::time::{Duration, UNIX_EPOCH};

use rstore_api::ObjectInfo;
use s3s::dto::{ETag, ETagCondition, Timestamp};

/// 条件求值的结果。
pub(crate) enum Verdict {
    /// 所有条件都通过，按原路径处理。
    Proceed,
    /// 某个前置条件不成立 → 412 `PreconditionFailed`。
    PreconditionFailed,
    /// 资源未被修改，且请求方已持有 → 304 `Not Modified`。
    NotModified,
}

/// 一次求值要用到的四个条件头（都借自 s3s 解析好的输入）。
pub(crate) struct Conditions<'a> {
    pub if_match: Option<&'a ETagCondition>,
    pub if_none_match: Option<&'a ETagCondition>,
    pub if_modified_since: Option<&'a Timestamp>,
    pub if_unmodified_since: Option<&'a Timestamp>,
}

/// 按 RFC 9110 §13 求值四个条件头。
///
/// 两条「只在前一个头缺席时才求值」的规则是刻意的：
/// - `If-Match` 在场时忽略 `If-Unmodified-Since`；
/// - `If-None-Match` 在场时忽略 `If-Modified-Since`。
///
/// 原因是日期头只有秒精度、且容易与 etag 冲突；按 RFC，etag 更精确，一旦给了就以它为准。
pub(crate) fn evaluate(c: Conditions<'_>, info: &ObjectInfo) -> Verdict {
    let current = ETag::Strong(info.etag.clone());

    if let Some(cond) = c.if_match {
        let ok = match cond {
            ETagCondition::Any => true,
            // If-Match 用**强**比较：弱 etag 不算命中。
            ETagCondition::ETag(e) => e.strong_cmp(&current),
        };
        if !ok {
            return Verdict::PreconditionFailed;
        }
    }
    // If-Match 缺席时才看 If-Unmodified-Since（前一个头在场时它被忽略）。
    if c.if_match.is_none() {
        if let Some(limit) = c.if_unmodified_since {
            if truncated(info.mod_time) > *limit {
                return Verdict::PreconditionFailed;
            }
        }
    }

    if let Some(cond) = c.if_none_match {
        let matched = match cond {
            ETagCondition::Any => true,
            // If-None-Match 用**弱**比较：值相等即命中，不看强弱标记。
            ETagCondition::ETag(e) => e.weak_cmp(&current),
        };
        if matched {
            return Verdict::NotModified;
        }
    }
    // If-None-Match 缺席时才看 If-Modified-Since。
    if c.if_none_match.is_none() {
        if let Some(since) = c.if_modified_since {
            if truncated(info.mod_time) <= *since {
                return Verdict::NotModified;
            }
        }
    }

    Verdict::Proceed
}

/// 截断到**秒**：HTTP 日期头只有秒精度，`mod_time` 是纳秒。
///
/// 不截断的话，同一个对象在自己那一秒内发起的 `If-Modified-Since`（值取自上一个
/// 响应的 `Last-Modified`，已经是整秒）会被判成「更晚」，从而漏掉 304。
fn truncated(mod_time_nanos: u64) -> Timestamp {
    Timestamp::from(UNIX_EPOCH + Duration::from_secs(mod_time_nanos / 1_000_000_000))
}

/// `ObjectInfo.mod_time` → 可放进响应头的 `Last-Modified`（同样截断到秒）。
pub(crate) fn last_modified(info: &ObjectInfo) -> Timestamp {
    truncated(info.mod_time)
}

#[cfg(test)]
mod tests {
    use rstore_api::ObjectInfo;
    use s3s::dto::{ETag, ETagCondition, Timestamp};

    use std::time::{Duration, UNIX_EPOCH};

    use super::{evaluate, Conditions, Verdict};

    /// 夹具对象的 `mod_time`：整整 1 秒。
    const MOD_TIME: u64 = 1_000_000_000;
    const ETAG: &str = "d41d8cd98f00b204";

    fn info() -> ObjectInfo {
        ObjectInfo {
            size: 3,
            etag: ETAG.to_string(),
            mod_time: MOD_TIME,
        }
    }

    fn ts(nanos: u64) -> Timestamp {
        Timestamp::from(UNIX_EPOCH + Duration::from_nanos(nanos))
    }

    fn strong(value: &str) -> ETagCondition {
        ETagCondition::ETag(ETag::Strong(value.to_string()))
    }

    fn weak(value: &str) -> ETagCondition {
        ETagCondition::ETag(ETag::Weak(value.to_string()))
    }

    /// 持有四个条件的用例构造器。
    ///
    /// 之所以**拥有**条件值（而不是像 `Conditions` 那样借）：`&strong("x")` 借的是
    /// 临时值，赋值语句一结束就悬空。这里让 `Case` 活到 `verdict()` 结束。
    #[derive(Default)]
    struct Case {
        if_match: Option<ETagCondition>,
        if_none_match: Option<ETagCondition>,
        if_modified_since: Option<Timestamp>,
        if_unmodified_since: Option<Timestamp>,
    }

    impl Case {
        fn verdict(&self) -> Verdict {
            evaluate(
                Conditions {
                    if_match: self.if_match.as_ref(),
                    if_none_match: self.if_none_match.as_ref(),
                    if_modified_since: self.if_modified_since.as_ref(),
                    if_unmodified_since: self.if_unmodified_since.as_ref(),
                },
                &info(),
            )
        }
    }

    #[test]
    fn if_match_hit_proceeds() {
        let c = Case {
            if_match: Some(strong(ETAG)),
            ..Default::default()
        };
        assert!(matches!(c.verdict(), Verdict::Proceed));
    }

    #[test]
    fn if_match_mismatch_is_precondition_failed() {
        let c = Case {
            if_match: Some(strong("other")),
            ..Default::default()
        };
        assert!(matches!(c.verdict(), Verdict::PreconditionFailed));
    }

    #[test]
    fn if_match_any_proceeds() {
        let c = Case {
            if_match: Some(ETagCondition::Any),
            ..Default::default()
        };
        assert!(matches!(c.verdict(), Verdict::Proceed));
    }

    #[test]
    fn if_none_match_hit_is_not_modified() {
        // 弱比较：Weak 值相等也算命中。
        let c = Case {
            if_none_match: Some(weak(ETAG)),
            ..Default::default()
        };
        assert!(matches!(c.verdict(), Verdict::NotModified));
    }

    #[test]
    fn if_none_match_miss_proceeds() {
        let c = Case {
            if_none_match: Some(strong("other")),
            ..Default::default()
        };
        assert!(matches!(c.verdict(), Verdict::Proceed));
    }

    #[test]
    fn if_none_match_any_is_not_modified() {
        let c = Case {
            if_none_match: Some(ETagCondition::Any),
            ..Default::default()
        };
        assert!(matches!(c.verdict(), Verdict::NotModified));
    }

    #[test]
    fn if_modified_since_earlier_proceeds() {
        // since < mod_time：资源在 since 之后被改过 → 返回新内容。
        let c = Case {
            if_modified_since: Some(ts(MOD_TIME - 1)),
            ..Default::default()
        };
        assert!(matches!(c.verdict(), Verdict::Proceed));
    }

    #[test]
    fn if_modified_since_equal_or_later_is_not_modified() {
        for nanos in [MOD_TIME, MOD_TIME + 1_000_000_000] {
            let c = Case {
                if_modified_since: Some(ts(nanos)),
                ..Default::default()
            };
            assert!(
                matches!(c.verdict(), Verdict::NotModified),
                "If-Modified-Since={nanos} 应回 NotModified"
            );
        }
    }

    #[test]
    fn if_unmodified_since_earlier_is_precondition_failed() {
        // limit < mod_time：资源在 limit 之后被改过 → 前置条件不成立。
        let c = Case {
            if_unmodified_since: Some(ts(MOD_TIME - 1)),
            ..Default::default()
        };
        assert!(matches!(c.verdict(), Verdict::PreconditionFailed));
    }

    #[test]
    fn if_unmodified_since_later_proceeds() {
        let c = Case {
            if_unmodified_since: Some(ts(MOD_TIME + 1_000_000_000)),
            ..Default::default()
        };
        assert!(matches!(c.verdict(), Verdict::Proceed));
    }

    #[test]
    fn if_match_present_suppresses_if_unmodified_since() {
        // If-Match 命中，但把 If-Unmodified-Since 设成「会让它失败」的过去时刻。
        // 规则要求 Ignore If-Unmodified-Since → Proceed；若实现无条件取更严者，
        // 这里会错误地变成 PreconditionFailed。
        let c = Case {
            if_match: Some(strong(ETAG)),
            if_unmodified_since: Some(ts(MOD_TIME - 1)),
            ..Default::default()
        };
        assert!(matches!(c.verdict(), Verdict::Proceed));
    }

    #[test]
    fn if_none_match_present_suppresses_if_modified_since() {
        // If-None-Match 不命中（etag 不同），If-Modified-Since 若被求值会命中
        // （since == mod_time → NotModified）。规则要求 Ignore If-Modified-Since
        // → Proceed；「两个头都在就取更严的那个」的实现会错给 NotModified。
        let c = Case {
            if_none_match: Some(strong("other")),
            if_modified_since: Some(ts(MOD_TIME)),
            ..Default::default()
        };
        assert!(matches!(c.verdict(), Verdict::Proceed));
    }

    #[test]
    fn if_none_match_present_with_stale_if_modified_since_proceeds() {
        // 同上，但 If-Modified-Since 用**过去**的时刻（since < mod_time）：即便被
        // 求值也是 Proceed。两种读取顺序都该得 Proceed —— 它钉的是「不会因为过去
        // 的日期把对象错报成未修改」。
        let c = Case {
            if_none_match: Some(strong("other")),
            if_modified_since: Some(ts(MOD_TIME - 1_000_000_000)),
            ..Default::default()
        };
        assert!(matches!(c.verdict(), Verdict::Proceed));
    }
}
