//! AWS 风格的通配符匹配。

/// `pattern` 是否匹配 `text`。区分大小写。
///
/// 语义照 AWS（设计 §4.4）：
/// - `*` 匹配任意长度（含空串），**且跨 `/`**；
/// - `?` 匹配恰好一个字符；
/// - 其余字符按字面比较。
///
/// **不用 `regex`**：依赖换不来什么，而 `*` 的语义与正则本来就不同
/// （正则的 `*` 只作用于前一个字符）。
///
/// 双指针 + 单点回退：最坏 O(n·m)、常见 O(n+m)，且**不用递归**——
/// 模式里 `*` 一多，递归实现会爆栈。
pub fn wildcard_match(pattern: &str, text: &str) -> bool {
    // 按 `char` 而不是字节走：`?` 必须匹配一个**字符**，不是 UTF-8 的一个字节。
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();

    let (mut pi, mut ti) = (0usize, 0usize);
    // 最近一个 `*` 在模式里的位置，以及它当时把文本吃到了哪。
    let mut star: Option<usize> = None;
    let mut star_ti = 0usize;

    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            star_ti = ti;
            pi += 1;
        } else if let Some(sp) = star {
            // 回退：让那个 `*` 多吞一个字符，从它后面重新比。
            star_ti += 1;
            ti = star_ti;
            pi = sp + 1;
        } else {
            return false;
        }
    }
    // 文本走完了，模式尾部剩下的必须全是 `*`。
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `*` **跨 `/`**——这是 `arn:aws:s3:::photos/*` 能匹配 `photos/a/b.jpg` 的前提。
    #[test]
    fn star_matches_across_slashes() {
        assert!(wildcard_match(
            "arn:aws:s3:::photos/*",
            "arn:aws:s3:::photos/a/b.jpg"
        ));
    }

    /// `*` 匹配空串。
    #[test]
    fn star_matches_empty() {
        assert!(wildcard_match("photos/*", "photos/"));
        assert!(wildcard_match("*", ""));
    }

    /// `?` 匹配**恰好**一个字符，多一个少一个都不行。
    #[test]
    fn question_matches_exactly_one() {
        assert!(wildcard_match("a?c", "abc"));
        assert!(!wildcard_match("a?c", "ac"));
        assert!(!wildcard_match("a?c", "abbc"));
    }

    /// 没有通配符时是精确比较。
    #[test]
    fn literal_is_exact() {
        assert!(wildcard_match("s3:GetObject", "s3:GetObject"));
        assert!(!wildcard_match("s3:GetObject", "s3:GetObjects"));
        assert!(!wildcard_match("s3:GetObject", "s3:getobject"));
    }

    /// 区分大小写。
    #[test]
    fn case_sensitive() {
        assert!(!wildcard_match("Photos/*", "photos/x"));
    }

    /// 裸 `*` 匹配一切，含空串。
    #[test]
    fn bare_star_matches_everything() {
        for t in ["", "a", "a/b/c", "arn:aws:s3:::*"] {
            assert!(wildcard_match("*", t), "text={t}");
        }
    }

    /// 回退：第一个 `*` 必须能"反悔"，多吞一个字符再试。
    /// 不做回退的贪心实现会在这一条上返回 false。
    #[test]
    fn star_backtracks() {
        assert!(wildcard_match("*a*b", "axbyb"));
        assert!(wildcard_match("a*c*e", "abcde"));
        assert!(!wildcard_match("*a*b", "axbyc"));
    }

    /// 模式比文本长、或有剩余字面量时不匹配。
    #[test]
    fn longer_pattern_does_not_match() {
        assert!(!wildcard_match("abc", "ab"));
        assert!(!wildcard_match("a*c", "ab"));
    }
}
