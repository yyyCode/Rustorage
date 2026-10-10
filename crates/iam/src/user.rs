//! 用户记录（设计 §3.3）。

use serde::Deserialize;

/// 一个用户。**access key 不在文件里**——文件名（去掉 `.json`）就是它，
/// 于是结构上不可能出现「两个文件声明同一个 access key」的歧义（设计 §3.1）。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserRecord {
    pub secret_key: String,
    /// **必填**：缺省成 `enabled` 会在作者本想停用某个用户时留下一对可用凭证。
    pub status: UserStatus,
    /// 缺省为 `[]`：空策略在 deny-by-default 下是**最窄**的取值。
    #[serde(default)]
    pub policies: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UserStatus {
    Enabled,
    Disabled,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_full_record() {
        let json = r#"{"secret_key":"s3cret","status":"enabled","policies":["readonly","logs"]}"#;
        let u: UserRecord = serde_json::from_str(json).unwrap();
        assert_eq!(u.secret_key, "s3cret");
        assert_eq!(u.status, UserStatus::Enabled);
        assert_eq!(u.policies, ["readonly", "logs"]);
    }

    /// `policies` 缺省为 `[]`：空策略在 deny-by-default 下是**最窄**的取值，
    /// 猜错也不会放宽权限（设计 §3.3 的分界规则）。
    #[test]
    fn policies_default_to_empty() {
        let u: UserRecord =
            serde_json::from_str(r#"{"secret_key":"s","status":"enabled"}"#).unwrap();
        assert!(u.policies.is_empty());
    }

    /// `status` **必填**：缺省成 `enabled` 会在作者本想停用某个用户时
    /// 留下一对可用凭证——那是放宽。
    #[test]
    fn status_is_required() {
        let e = serde_json::from_str::<UserRecord>(r#"{"secret_key":"s"}"#).unwrap_err();
        assert!(e.to_string().contains("status"), "实际：{e}");
    }

    #[test]
    fn secret_key_is_required() {
        assert!(serde_json::from_str::<UserRecord>(r#"{"status":"enabled"}"#).is_err());
    }

    /// 两个取值是**小写**，与 MinIO / AWS 的习惯一致。
    #[test]
    fn status_values_are_lowercase() {
        let u: UserRecord =
            serde_json::from_str(r#"{"secret_key":"s","status":"disabled"}"#).unwrap();
        assert_eq!(u.status, UserStatus::Disabled);
        assert!(
            serde_json::from_str::<UserRecord>(r#"{"secret_key":"s","status":"Enabled"}"#).is_err()
        );
        assert!(
            serde_json::from_str::<UserRecord>(r#"{"secret_key":"s","status":"paused"}"#).is_err()
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let e = serde_json::from_str::<UserRecord>(
            r#"{"secret_key":"s","status":"enabled","policy":["x"]}"#,
        )
        .unwrap_err();
        assert!(e.to_string().contains("policy"), "实际：{e}");
    }
}
