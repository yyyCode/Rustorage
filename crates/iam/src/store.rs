//! `IamStore`：身份、策略与求值（设计 §4.1）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

use crate::action::canonical_policy_action;
use crate::error::IamError;
use crate::glob::wildcard_match;
use crate::policy::{Effect, PolicyDoc};
use crate::user::{UserRecord, UserStatus};

/// 身份、策略与求值。**唯一的完整构造入口是 [`IamStore::load`]**（Task 6）与
/// [`IamStore::root_only`]；字段私有，所以不存在"绕过校验的半个 store"。
pub struct IamStore {
    root_access_key: String,
    root_secret_key: String,
    users: HashMap<String, UserRecord>,
    policies: HashMap<String, PolicyDoc>,
}

/// 一次授权判定的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    Deny(DenyReason),
}

/// 拒绝原因。**公开**：拒绝日志要打出它，否则运维面对一个 403 无从下手
/// （是没这份策略？还是被一条 Deny 压掉了？）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DenyReason {
    /// 请求没有凭证。
    Anonymous,
    /// 这个 access key 没有任何身份。
    UnknownPrincipal,
    /// 用户存在但被停用了。
    Disabled,
    /// 有身份，但没有任何语句命中。
    NoMatchingStatement,
    /// 有语句命中，且其中至少一条是 `Deny`。
    ExplicitDeny,
}

impl IamStore {
    /// 从盘上加载。**唯一的完整构造入口**，所有校验都在这里发生（设计 §7.1）。
    ///
    /// 目录不存在 = 没有非 root 用户，**不是错误**。
    pub fn load(
        dir: &Path,
        root_access_key: &str,
        root_secret_key: &str,
    ) -> Result<Self, IamError> {
        let mut users: HashMap<String, UserRecord> = HashMap::new();
        for (name, path) in read_json_dir(&dir.join("users"))? {
            users.insert(name, parse_json(&path)?);
        }

        let mut policies: HashMap<String, PolicyDoc> = HashMap::new();
        for (name, path) in read_json_dir(&dir.join("policies"))? {
            policies.insert(name, parse_json(&path)?);
        }

        // 引用完整性：**启动期就炸**，不等运行时静默降级（不变量 #6）。
        for (name, u) in &users {
            for p in &u.policies {
                if !policies.contains_key(p) {
                    return Err(IamError::UnknownPolicy {
                        user: name.clone(),
                        policy: p.clone(),
                        expected: dir.join("policies").join(format!("{p}.json")),
                    });
                }
            }
        }

        // 同一个 key 不能既是 root 又是受限用户。
        if users.contains_key(root_access_key) {
            return Err(IamError::UserShadowsRoot {
                key: root_access_key.to_owned(),
            });
        }

        Ok(Self {
            root_access_key: root_access_key.to_owned(),
            root_secret_key: root_secret_key.to_owned(),
            users,
            policies,
        })
    }

    /// 直接给全部部件。**给测试与将来的管理 API 用**——`load` 会在这里
    /// 再补上引用完整性与 root 冲突的校验。
    pub fn with_parts(
        root_access_key: &str,
        root_secret_key: &str,
        users: Vec<(String, UserRecord)>,
        policies: Vec<(String, PolicyDoc)>,
    ) -> Self {
        Self {
            root_access_key: root_access_key.to_owned(),
            root_secret_key: root_secret_key.to_owned(),
            users: users.into_iter().collect(),
            policies: policies.into_iter().collect(),
        }
    }

    /// 只有 root 的 store：给测试与「还没有 IAM 目录」的场景用。
    pub fn root_only(access_key: &str, secret_key: &str) -> Self {
        Self {
            root_access_key: access_key.to_owned(),
            root_secret_key: secret_key.to_owned(),
            users: HashMap::new(),
            policies: HashMap::new(),
        }
    }

    /// 认证用：这个 access key 的 secret 是什么。
    ///
    /// **停用的用户返回 `None`**，与「key 不存在」给出同一个结果。
    pub fn secret_key(&self, access_key: &str) -> Option<&str> {
        if access_key == self.root_access_key {
            return Some(&self.root_secret_key);
        }
        match self.users.get(access_key) {
            Some(u) if u.status == UserStatus::Enabled => Some(&u.secret_key),
            _ => None,
        }
    }

    /// 授权：设计 §4.1 的九步。
    ///
    /// `access_key` 是 `Option` 而不是 `&str`——**让「没有凭证」成为类型的一部分**，
    /// 而不是调用方可能忘记写的 `if`。忘了它会让服务变成公开存储桶（设计 §1.3）。
    ///
    /// `action` 形如 `"s3:GetObject"`（调用方用 `cx.s3_op().name()` 拼出来）；
    /// `resource` 形如 `"arn:aws:s3:::bucket/key"`。
    pub fn authorize(&self, access_key: Option<&str>, action: &str, resource: &str) -> Decision {
        // 1. 匿名。
        let Some(ak) = access_key else {
            return Decision::Deny(DenyReason::Anonymous);
        };
        // 2. root 恒 Allow（设计 §2 决策五）：运维需要一个永不锁死自己的后门。
        if ak == self.root_access_key {
            return Decision::Allow;
        }
        // 3/4. 与认证阶段冗余，**是有意的纵深防御**：`authorize` 是唯一的安全边界，
        //      不该依赖「上游一定先跑过认证」这个假设。
        let Some(user) = self.users.get(ak) else {
            return Decision::Deny(DenyReason::UnknownPrincipal);
        };
        if user.status != UserStatus::Enabled {
            return Decision::Deny(DenyReason::Disabled);
        }

        // 5. 两侧都过同一个规范化函数，再通配匹配。
        let want = canonical_policy_action(action);
        let mut effects: Vec<Effect> = Vec::new();
        for name in &user.policies {
            // `load` 已校验策略存在；查不到只能是手工构造的 store。
            // 仍然 fail-closed：当作这份策略没有任何语句。
            let Some(doc) = self.policies.get(name) else {
                continue;
            };
            for st in &doc.statement {
                let action_hit = st
                    .action
                    .as_slice()
                    .iter()
                    .any(|p| wildcard_match(canonical_policy_action(p), want));
                let resource_hit = st
                    .resource
                    .as_slice()
                    .iter()
                    .any(|p| wildcard_match(p, resource));
                if action_hit && resource_hit {
                    effects.push(st.effect);
                }
            }
        }

        // 7. 显式拒绝优先（AWS / MinIO 语义）。
        if effects.contains(&Effect::Deny) {
            return Decision::Deny(DenyReason::ExplicitDeny);
        }
        // 8. 有 Allow 才放行。
        if effects.contains(&Effect::Allow) {
            return Decision::Allow;
        }
        // 9. deny-by-default。
        Decision::Deny(DenyReason::NoMatchingStatement)
    }

    /// 启动期日志用。
    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    /// 启动期日志用。
    pub fn policy_count(&self) -> usize {
        self.policies.len()
    }
}

/// 读一个目录下所有 `*.json`，返回 `(文件名去掉 .json, 完整路径)`。
///
/// 三条规则（设计 §3.4）：
/// - **目录不存在 ≠ 错误**，等于「一个都没有」；
/// - 子目录 → 报错（放错了层级）；
/// - 非 `.json` 的文件忽略（编辑器的 `.swp` 之类不该让服务起不来）。
///
/// 结果按名字排序：`HashMap` 的迭代序本来就不稳，不排的话「哪个错误先报出来」
/// 会随运行漂移，测试与运维都要面对不稳定的现象。
fn read_json_dir(dir: &Path) -> Result<Vec<(String, PathBuf)>, IamError> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(source) => {
            return Err(IamError::Io {
                path: dir.to_path_buf(),
                source,
            });
        }
    };

    let mut out = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| IamError::Io {
            path: dir.to_path_buf(),
            source,
        })?;
        let path = entry.path();
        // `file_type()` 不跟随符号链接。IAM 目录由运维手工维护，
        // 链接指向哪里是运维的事；这里只按目录处理。
        let ft = entry.file_type().map_err(|source| IamError::Io {
            path: path.clone(),
            source,
        })?;
        // **子目录的判断在 `.json` 过滤之前**：否则 `users/nested/` 会因为
        // 名字不含 `.json` 被跳过，配错层级的人得不到任何提示。
        if ft.is_dir() {
            return Err(IamError::UnexpectedDir { path });
        }
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        // `.json` 这种只有扩展名的文件名，`file_stem` 仍是 `.json`（Rust 把
        // 前导点当作名字的一部分），而 `extension()` 返回 `None`——所以上面
        // 那一步已经把它滤掉了，这里不会是空串。
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        out.push((stem.to_owned(), path));
    }
    out.sort();
    Ok(out)
}

fn parse_json<T: DeserializeOwned>(path: &Path) -> Result<T, IamError> {
    let bytes = std::fs::read(path).map_err(|source| IamError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    serde_json::from_slice(&bytes).map_err(|source| IamError::Parse {
        path: path.to_path_buf(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{OneOrMany, Statement};

    /// 造一份单语句策略。字段是 `pub`，同 crate 的测试可以直接构造。
    fn policy(effect: Effect, action: &str, resource: &str) -> PolicyDoc {
        PolicyDoc {
            version: "2012-10-17".into(),
            statement: vec![Statement {
                sid: None,
                effect,
                action: OneOrMany::One(action.into()),
                resource: OneOrMany::One(resource.into()),
            }],
        }
    }

    fn user(status: UserStatus, policies: &[&str]) -> UserRecord {
        UserRecord {
            secret_key: "u-secret".into(),
            status,
            policies: policies.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// 手工装配一个 store。**不走 `load`**——本任务测的是求值，
    /// 盘上加载是 Task 6 的事，两边分开测才不会互相遮蔽。
    fn store(users: Vec<(&str, UserRecord)>, policies: Vec<(&str, PolicyDoc)>) -> IamStore {
        IamStore {
            root_access_key: "root".into(),
            root_secret_key: "root-secret".into(),
            users: users.into_iter().map(|(k, v)| (k.to_string(), v)).collect(),
            policies: policies
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        }
    }

    const PHOTO: &str = "arn:aws:s3:::photos/a/b.jpg";

    /// 匿名必拒。**这是整套改动里最要紧的一条**（设计 §1.3）：装上 `set_access`
    /// 之后 s3s 的 `default_check` 不再兜底，安全性完全落在这个分支上。
    #[test]
    fn anonymous_is_denied() {
        let s = store(vec![], vec![]);
        assert_eq!(
            s.authorize(None, "s3:GetObject", PHOTO),
            Decision::Deny(DenyReason::Anonymous)
        );
    }

    /// root 恒 Allow——**哪怕给它绑一份全是 Deny 的策略**（设计 §2 决策五）。
    #[test]
    fn root_bypasses_policies() {
        let s = store(
            vec![("root", user(UserStatus::Enabled, &["nothing"]))],
            vec![("nothing", policy(Effect::Deny, "s3:*", "*"))],
        );
        assert_eq!(
            s.authorize(Some("root"), "s3:GetObject", PHOTO),
            Decision::Allow
        );
    }

    /// 未知主体：**与认证阶段冗余，是有意的纵深防御**（设计 §4.1）。
    #[test]
    fn unknown_principal_is_denied() {
        let s = store(vec![], vec![]);
        assert_eq!(
            s.authorize(Some("nobody"), "s3:GetObject", PHOTO),
            Decision::Deny(DenyReason::UnknownPrincipal)
        );
    }

    /// 停用用户：同样在这里独立地拒一次。
    #[test]
    fn disabled_user_is_denied() {
        let s = store(
            vec![("alice", user(UserStatus::Disabled, &["ro"]))],
            vec![("ro", policy(Effect::Allow, "s3:*", "*"))],
        );
        assert_eq!(
            s.authorize(Some("alice"), "s3:GetObject", PHOTO),
            Decision::Deny(DenyReason::Disabled)
        );
    }

    /// deny-by-default：用户存在、策略也绑了，但没有一条命中。
    #[test]
    fn no_matching_statement_is_denied() {
        let s = store(
            vec![("alice", user(UserStatus::Enabled, &["ro"]))],
            vec![(
                "ro",
                policy(Effect::Allow, "s3:GetObject", "arn:aws:s3:::other/*"),
            )],
        );
        assert_eq!(
            s.authorize(Some("alice"), "s3:GetObject", PHOTO),
            Decision::Deny(DenyReason::NoMatchingStatement)
        );
    }

    /// 一条命中就放行，且 `*` 跨 `/`。
    #[test]
    fn matching_allow_wins() {
        let s = store(
            vec![("alice", user(UserStatus::Enabled, &["ro"]))],
            vec![(
                "ro",
                policy(Effect::Allow, "s3:GetObject", "arn:aws:s3:::photos/*"),
            )],
        );
        assert_eq!(
            s.authorize(Some("alice"), "s3:GetObject", PHOTO),
            Decision::Allow
        );
    }

    /// **显式拒绝优先**：同一主体上 Allow 与 Deny 同时命中时，Deny 赢。
    #[test]
    fn explicit_deny_beats_allow() {
        let s = store(
            vec![("alice", user(UserStatus::Enabled, &["wide", "narrow"]))],
            vec![
                (
                    "wide",
                    policy(Effect::Allow, "s3:*", "arn:aws:s3:::photos/*"),
                ),
                (
                    "narrow",
                    policy(
                        Effect::Deny,
                        "s3:DeleteObject",
                        "arn:aws:s3:::photos/locked/*",
                    ),
                ),
            ],
        );
        // 宽策略允许一切，窄策略拒掉 locked/ 下的删除——后者赢。
        assert_eq!(
            s.authorize(
                Some("alice"),
                "s3:DeleteObject",
                "arn:aws:s3:::photos/locked/x"
            ),
            Decision::Deny(DenyReason::ExplicitDeny)
        );
        // 不在 locked/ 下的删除仍然放行。
        assert_eq!(
            s.authorize(Some("alice"), "s3:DeleteObject", "arn:aws:s3:::photos/free"),
            Decision::Allow
        );
    }

    /// 规范化表在求值路径上真的起作用：策略写 AWS 的名字也能命中 s3s 的操作名。
    #[test]
    fn action_aliases_work_end_to_end() {
        let s = store(
            vec![("alice", user(UserStatus::Enabled, &["ro"]))],
            vec![(
                "ro",
                policy(Effect::Allow, "s3:ListBucket", "arn:aws:s3:::photos"),
            )],
        );
        // 实际操作名是 ListObjectsV2（或 ListObjects），都不是 "ListBucket"。
        assert_eq!(
            s.authorize(Some("alice"), "s3:ListObjectsV2", "arn:aws:s3:::photos"),
            Decision::Allow
        );
        assert_eq!(
            s.authorize(Some("alice"), "s3:ListObjects", "arn:aws:s3:::photos"),
            Decision::Allow
        );
    }

    /// 用户绑了多份策略时取并集（任一份允许即可）。
    #[test]
    fn policies_are_unioned() {
        let s = store(
            vec![("alice", user(UserStatus::Enabled, &["a", "b"]))],
            vec![
                (
                    "a",
                    policy(Effect::Allow, "s3:GetObject", "arn:aws:s3:::photos/*"),
                ),
                (
                    "b",
                    policy(Effect::Allow, "s3:PutObject", "arn:aws:s3:::photos/*"),
                ),
            ],
        );
        assert_eq!(
            s.authorize(Some("alice"), "s3:GetObject", PHOTO),
            Decision::Allow
        );
        assert_eq!(
            s.authorize(Some("alice"), "s3:PutObject", PHOTO),
            Decision::Allow
        );
        assert_eq!(
            s.authorize(Some("alice"), "s3:DeleteObject", PHOTO),
            Decision::Deny(DenyReason::NoMatchingStatement)
        );
    }

    /// 绑了不存在的策略时 fail-closed——`load` 会拒这种配置（Task 6），
    /// 但手工构造的 store 也不该因为查不到就放行。
    #[test]
    fn missing_policy_fails_closed() {
        let s = store(
            vec![("alice", user(UserStatus::Enabled, &["ghost"]))],
            vec![],
        );
        assert_eq!(
            s.authorize(Some("alice"), "s3:GetObject", PHOTO),
            Decision::Deny(DenyReason::NoMatchingStatement)
        );
    }

    /// 空策略列表 = 什么都做不了（deny-by-default 的最窄形态）。
    #[test]
    fn user_without_policies_can_do_nothing() {
        let s = store(vec![("alice", user(UserStatus::Enabled, &[]))], vec![]);
        assert_eq!(
            s.authorize(Some("alice"), "s3:GetObject", PHOTO),
            Decision::Deny(DenyReason::NoMatchingStatement)
        );
    }

    // ---- secret_key() ----

    #[test]
    fn secret_key_lookup() {
        let s = store(vec![("alice", user(UserStatus::Enabled, &[]))], vec![]);
        assert_eq!(s.secret_key("root"), Some("root-secret"));
        assert_eq!(s.secret_key("alice"), Some("u-secret"));
        assert_eq!(s.secret_key("nobody"), None);
    }

    /// **停用用户在认证阶段就查不到密钥**——于是它拿不到任何「这个账号存在
    /// 但被停了」的信号，与未知 key 完全同形（设计 §4.1 的注）。
    #[test]
    fn disabled_user_has_no_secret_key() {
        let s = store(vec![("alice", user(UserStatus::Disabled, &[]))], vec![]);
        assert_eq!(s.secret_key("alice"), None);
    }

    #[test]
    fn counts_are_reported() {
        let s = store(
            vec![("alice", user(UserStatus::Enabled, &[]))],
            vec![("ro", policy(Effect::Allow, "s3:*", "*"))],
        );
        assert_eq!(s.user_count(), 1);
        assert_eq!(s.policy_count(), 1);
    }

    #[test]
    fn root_only_has_no_users() {
        let s = IamStore::root_only("ak", "sk");
        assert_eq!(s.secret_key("ak"), Some("sk"));
        assert_eq!(s.user_count(), 0);
        assert_eq!(s.policy_count(), 0);
    }

    // ---- load() ----
    //
    // 这些用真实文件系统（tempfile），因为要测的正是「盘上长什么样 → 加载成什么」。

    /// 建一个 IAM 目录并写入给定内容。`("users", "alice", json)` 写到 `users/alice.json`。
    fn iam_dir(dir: &Path, files: &[(&str, &str, &str)]) {
        for (sub, name, body) in files {
            let d = dir.join(sub);
            std::fs::create_dir_all(&d).unwrap();
            std::fs::write(d.join(format!("{name}.json")), body).unwrap();
        }
    }

    fn load(dir: &Path) -> Result<IamStore, IamError> {
        IamStore::load(dir, "root", "root-secret")
    }

    /// 只取错误信息，不碰 `Ok` 里的值。
    ///
    /// **`IamStore` 故意没有 `Debug`**：它有 `secret_key` 字段，一旦实现了
    /// `Debug`，某个手滑的 `tracing::debug!(?iam)` 就会把全部密钥打进日志
    /// （`docs/DESIGN.md` §18.3 要求 access_key 与 secret 脱敏）。
    /// 代价是 `unwrap_err()` 用不了——它要求 `Ok` 类型是 `Debug`——所以这里
    /// 走 `.err()`，它没有这个约束。
    fn load_err(dir: &Path) -> String {
        load(dir).err().expect("这个配置应当加载失败").to_string()
    }

    /// 目录不存在 = 没有非 root 用户，**不是错误**（设计 §3.4 / §7.1）。
    #[test]
    fn missing_directory_is_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let s = load(&tmp.path().join("nope")).unwrap();
        assert_eq!(s.user_count(), 0);
        assert_eq!(s.policy_count(), 0);
        assert_eq!(s.secret_key("root"), Some("root-secret"));
    }

    /// `users/` 与 `policies/` 缺失同样是正常的。
    #[test]
    fn missing_subdirectories_are_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        let s = load(tmp.path()).unwrap();
        assert_eq!(s.user_count(), 0);
    }

    /// **文件名（去掉 `.json`）就是 access key**（设计 §3.1）。
    #[test]
    fn file_name_is_the_access_key() {
        let tmp = tempfile::tempdir().unwrap();
        iam_dir(
            tmp.path(),
            &[
                (
                    "policies",
                    "ro",
                    r#"{"Version":"2012-10-17","Statement":[
                    {"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::photos/*"}]}"#,
                ),
                (
                    "users",
                    "alice",
                    r#"{"secret_key":"a-secret","status":"enabled","policies":["ro"]}"#,
                ),
                (
                    "users",
                    "robot.backup",
                    r#"{"secret_key":"r-secret","status":"enabled","policies":["ro"]}"#,
                ),
            ],
        );
        let s = load(tmp.path()).unwrap();
        assert_eq!(s.user_count(), 2);
        assert_eq!(s.policy_count(), 1);
        assert_eq!(s.secret_key("alice"), Some("a-secret"));
        // 文件名里的 `.` 不算扩展名的一部分——`robot.backup.json` 的 stem 是 `robot.backup`。
        assert_eq!(s.secret_key("robot.backup"), Some("r-secret"));
        assert_eq!(
            s.authorize(Some("alice"), "s3:GetObject", "arn:aws:s3:::photos/x"),
            Decision::Allow
        );
    }

    /// 非 `.json` 的文件被忽略——编辑器的 `.swp` 不该让服务起不来。
    #[test]
    fn non_json_files_are_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        iam_dir(
            tmp.path(),
            &[("users", "alice", r#"{"secret_key":"s","status":"enabled"}"#)],
        );
        std::fs::write(tmp.path().join("users/alice.json.swp"), b"junk").unwrap();
        std::fs::write(tmp.path().join("users/README"), b"junk").unwrap();
        let s = load(tmp.path()).unwrap();
        assert_eq!(s.user_count(), 1);
    }

    /// 子目录要报错：说明放错了层级，静默忽略会让人以为配置生效了。
    #[test]
    fn subdirectory_in_users_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("users/nested")).unwrap();
        let e = load_err(tmp.path());
        assert!(e.contains("nested"), "实际：{e}");
    }

    /// JSON 语法错 / 必填字段缺失 → 拒绝启动，且错误信息里带**文件路径**。
    #[test]
    fn parse_errors_name_the_file() {
        let tmp = tempfile::tempdir().unwrap();
        iam_dir(tmp.path(), &[("users", "alice", r#"{"secret_key":"s"}"#)]);
        let e = load_err(tmp.path());
        assert!(e.contains("alice.json"), "实际：{e}");
        assert!(e.contains("status"), "实际：{e}");
    }

    /// **引用不存在的策略 → 拒绝启动**（设计 §7.1、不变量 #6），
    /// 而不是运行时静默降级成「没这条策略」。
    #[test]
    fn unknown_policy_reference_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        iam_dir(
            tmp.path(),
            &[(
                "users",
                "alice",
                r#"{"secret_key":"s","status":"enabled","policies":["ghost"]}"#,
            )],
        );
        let e = load_err(tmp.path());
        assert!(e.contains("ghost"), "实际：{e}");
        assert!(e.contains("alice"), "实际：{e}");
    }

    /// 用户的 access key 与 root 相同 → 拒绝启动：那会产生
    /// 「既是 root 又受限」的矛盾身份。
    #[test]
    fn user_shadowing_root_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        iam_dir(
            tmp.path(),
            &[(
                "users",
                "root",
                r#"{"secret_key":"evil","status":"enabled"}"#,
            )],
        );
        let e = load_err(tmp.path());
        assert!(e.contains("root"), "实际：{e}");
    }

    /// 策略文件里的未知字段同样在加载期炸掉。
    #[test]
    fn policy_with_unknown_field_is_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        iam_dir(
            tmp.path(),
            &[(
                "policies",
                "p",
                r#"{"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":"s3:*","Resource":"*","Condition":{}}]}"#,
            )],
        );
        let e = load_err(tmp.path());
        assert!(e.contains("Condition"), "实际：{e}");
    }

    /// 空目录（`users/` 建了但没文件）也算正常。
    #[test]
    fn empty_subdirectories_are_not_an_error() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("users")).unwrap();
        std::fs::create_dir_all(tmp.path().join("policies")).unwrap();
        assert_eq!(load(tmp.path()).unwrap().user_count(), 0);
    }
}
