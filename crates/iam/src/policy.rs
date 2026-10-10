//! AWS IAM policy 文档（子集，设计 §3.2）。

use serde::Deserialize;

/// 一份策略文档。
///
/// **`deny_unknown_fields` 是有意的**（设计 §2 决策六）：静默忽略一个 `Condition`
/// 会让策略比作者预期**更宽**。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyDoc {
    /// AWS 的版本串（`2012-10-17` / `2008-10-17`）。
    /// **必填但不校验取值**——它在本实现里不影响任何语义。
    #[serde(rename = "Version")]
    pub version: String,
    #[serde(rename = "Statement")]
    pub statement: Vec<Statement>,
}

/// 一条语句。
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Statement {
    /// 纯元数据，允许出现但本实现不使用。**它不改语义，所以是唯一豁免的字段。**
    #[serde(rename = "Sid", default)]
    pub sid: Option<String>,
    #[serde(rename = "Effect")]
    pub effect: Effect,
    #[serde(rename = "Action")]
    pub action: OneOrMany,
    #[serde(rename = "Resource")]
    pub resource: OneOrMany,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Effect {
    Allow,
    Deny,
}

/// AWS 允许 `Action` / `Resource` 是字符串**或**字符串数组。两种都得收。
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

impl OneOrMany {
    /// 统一成切片，调用方不必再 match 一次。
    pub fn as_slice(&self) -> &[String] {
        match self {
            OneOrMany::One(s) => std::slice::from_ref(s),
            OneOrMany::Many(v) => v,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FULL: &str = r#"{
      "Version": "2012-10-17",
      "Statement": [
        {
          "Effect": "Allow",
          "Action": ["s3:GetObject", "s3:PutObject"],
          "Resource": ["arn:aws:s3:::photos/*"]
        },
        {
          "Sid": "no-delete",
          "Effect": "Deny",
          "Action": ["s3:DeleteObject"],
          "Resource": ["arn:aws:s3:::photos/locked/*"]
        }
      ]
    }"#;

    /// 两条语句、`Action` 是数组、`Sid` 只出现在第二条上。
    #[test]
    fn parses_a_full_document() {
        let doc: PolicyDoc = serde_json::from_str(FULL).unwrap();
        assert_eq!(doc.version, "2012-10-17");
        assert_eq!(doc.statement.len(), 2);
        assert_eq!(doc.statement[0].effect, Effect::Allow);
        assert_eq!(
            doc.statement[0].action.as_slice(),
            ["s3:GetObject", "s3:PutObject"]
        );
        assert_eq!(doc.statement[0].sid, None);
        assert_eq!(doc.statement[1].effect, Effect::Deny);
        assert_eq!(doc.statement[1].sid.as_deref(), Some("no-delete"));
    }

    /// `Action` 与 `Resource` 在 AWS 里既可以是字符串也可以是数组，两种都得收。
    #[test]
    fn single_string_and_array_are_equivalent() {
        let one = r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}"#;
        let many = r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::b/*"]}]}"#;
        let a: PolicyDoc = serde_json::from_str(one).unwrap();
        let b: PolicyDoc = serde_json::from_str(many).unwrap();
        assert_eq!(
            a.statement[0].action.as_slice(),
            b.statement[0].action.as_slice()
        );
        assert_eq!(
            a.statement[0].resource.as_slice(),
            b.statement[0].resource.as_slice()
        );
        assert_eq!(a.statement[0].action.as_slice(), ["s3:GetObject"]);
    }

    /// **`Condition` 必须被拒**（设计 §2 决策六）：静默忽略一个条件会让策略
    /// 比作者预期**更宽**——那是安全缺陷，不是便利问题。
    #[test]
    fn rejects_condition() {
        let doc = r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*",
             "Condition":{"IpAddress":{"aws:SourceIp":"10.0.0.0/8"}}}]}"#;
        let err = serde_json::from_str::<PolicyDoc>(doc)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("Condition"),
            "错误信息应点出字段名，实际：{err}"
        );
    }

    /// 同族的另外三个也改语义，同样要拒。
    #[test]
    fn rejects_other_semantic_fields() {
        for field in [
            r#""NotAction":"s3:DeleteObject""#,
            r#""NotResource":"arn:aws:s3:::b/*""#,
            r#""Principal":"*""#,
        ] {
            let doc = format!(
                r#"{{"Version":"2012-10-17","Statement":[
                    {{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*",{field}}}]}}"#
            );
            assert!(
                serde_json::from_str::<PolicyDoc>(&doc).is_err(),
                "{field} 必须被拒绝"
            );
        }
    }

    /// 字段名拼错（`Actions` 少了个 s）同样要拒——否则它会被静默读成「没有 Action」，
    /// 而 deny-by-default 会把它变成一条什么都不允许的策略，用户只会觉得权限莫名其妙没了。
    #[test]
    fn rejects_typo_in_field_name() {
        let doc = r#"{"Version":"2012-10-17","Statement":[
            {"Effect":"Allow","Actions":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}"#;
        assert!(serde_json::from_str::<PolicyDoc>(doc).is_err());
    }

    /// 必填字段缺失。
    #[test]
    fn rejects_missing_required_fields() {
        for doc in [
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}"#,
            r#"{"Version":"2012-10-17","Statement":[{"Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}"#,
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Resource":"arn:aws:s3:::b/*"}]}"#,
        ] {
            assert!(serde_json::from_str::<PolicyDoc>(doc).is_err(), "doc={doc}");
        }
    }

    /// `Version` **必填但不校验取值**：它不影响任何语义，把语义无关的字符串
    /// 变成加载失败只会制造无谓的摩擦。
    #[test]
    fn accepts_any_version_string() {
        let doc = r#"{"Version":"2077-01-01","Statement":[
            {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::b/*"}]}"#;
        assert!(serde_json::from_str::<PolicyDoc>(doc).is_ok());
    }
}
