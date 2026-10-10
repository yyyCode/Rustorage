//! s3s 的两个扩展点：`S3Auth`（认证）与 `S3Access`（授权）。
//!
//! **两者必须成对装**（设计 §1.4）：只装 `set_access` 而没装 `set_auth` 时，
//! `authorize()` 的第一行就 `return Ok(())`，检查被**整体跳过**，而且不报任何错。
//! 这一条有测试钉住（`access_without_auth_skips_every_check`）。
//!
//! 这里只实现 `S3Access::check()`，不实现任何 per-op 方法（设计 §2 决策二）：
//! `S3AccessContext` 已经带 `credentials` / `s3_path` / `s3_op`，一个函数覆盖
//! 全部操作——**包括 P2 的 multipart，届时授权零改动**。

use std::sync::Arc;

use rstore_iam::{Decision, IamStore};
use s3s::access::{S3Access, S3AccessContext};
use s3s::auth::{S3Auth, SecretKey};
use s3s::path::S3Path;
use s3s::{s3_error, S3Result};

/// 认证侧：这个 access key 的 secret 是什么。签名验证本身由 s3s 做。
pub struct IamAuth {
    pub iam: Arc<IamStore>,
}

#[async_trait::async_trait]
impl S3Auth for IamAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        match self.iam.secret_key(access_key) {
            Some(s) => Ok(SecretKey::from(s.to_owned())),
            // AWS 的归位。`SimpleAuth` 用的是 `NotSignedUp`（上游自己的选择），
            // 这里换掉是为了与 AWS 的错误码表一致。
            //
            // 停用用户走的也是这条路：`IamStore::secret_key` 对它返回 `None`，
            // 于是它与「这个 key 不存在」完全同形——不留枚举的口子。
            None => Err(s3_error!(InvalidAccessKeyId)),
        }
    }
}

/// 授权侧。
pub struct IamAccess {
    pub iam: Arc<IamStore>,
}

#[async_trait::async_trait]
impl S3Access for IamAccess {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // `credentials()` 对未签名请求是 `None`——**必须在这里拒掉**。
        // 上游的 `default_check` 会替我们拒，但我们一装上 `set_access` 它就不再
        // 参与了：这一步没有兜底（设计 §1.3），漏掉它服务就变成公开存储桶。
        let access_key = cx.credentials().map(|c| c.access_key.as_str());
        let action = format!("s3:{}", cx.s3_op().name());
        let resource = arn_of_path(cx.s3_path());

        match self.iam.authorize(access_key, &action, &resource) {
            Decision::Allow => Ok(()),
            Decision::Deny(reason) => {
                // 每次拒绝都留痕，否则运维面对一个 403 只能靠猜（设计 §7.2）。
                // `secret_key` 永不入日志；`access_key` 是身份标识、不是凭证。
                tracing::warn!(
                    access_key = access_key.unwrap_or("<anonymous>"),
                    action = %action,
                    resource = %resource,
                    reason = ?reason,
                    "IAM 拒绝"
                );
                Err(s3_error!(AccessDenied))
            }
        }
    }
}

/// `S3Path` → 资源 ARN。ARN 的**格式**定义在 `rstore-iam`（策略是照着它写的），
/// 这里只做「哪种路径用哪种」的映射。
fn arn_of_path(path: &S3Path) -> String {
    match path {
        S3Path::Root => rstore_iam::arn::ARN_ROOT.to_owned(),
        S3Path::Bucket { bucket } => rstore_iam::arn::arn_bucket(bucket),
        S3Path::Object { bucket, key } => rstore_iam::arn::arn_object(bucket, key),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use http::StatusCode;
    use s3s::service::S3ServiceBuilder;

    use rstore_iam::{IamStore, PolicyDoc, UserRecord, UserStatus};

    use super::*;
    use crate::impl_s3::RstoreFs;
    use crate::testutil::{
        call_on, request, signed_request_as, NopStore, ACCESS_KEY, HOST, SECRET_KEY,
    };

    /// 一个只有一个用户的 store：`alice` 拿着 `alice-secret`，
    /// 绑一份能读 `photos/*` 的策略。
    fn store_with_alice() -> Arc<IamStore> {
        let doc: PolicyDoc = serde_json::from_str(
            r#"{"Version":"2012-10-17","Statement":[
                {"Effect":"Allow","Action":["s3:GetObject"],"Resource":["arn:aws:s3:::photos/*"]}]}"#,
        )
        .unwrap();
        Arc::new(IamStore::with_parts(
            ACCESS_KEY,
            SECRET_KEY,
            vec![(
                "alice".to_string(),
                UserRecord {
                    secret_key: "alice-secret".into(),
                    status: UserStatus::Enabled,
                    policies: vec!["ro".into()],
                },
            )],
            vec![("ro".to_string(), doc)],
        ))
    }

    fn service(iam: Arc<IamStore>) -> s3s::service::S3Service {
        let mut b = S3ServiceBuilder::new(RstoreFs {
            store: Arc::new(NopStore),
        });
        // 两者成对，中间不放别的东西——只装一个不会报错，只会静态地失去防护。
        b.set_auth(IamAuth {
            iam: Arc::clone(&iam),
        });
        b.set_access(IamAccess { iam });
        b.build()
    }

    /// **本次改动最要紧的一条**：装上 `set_access` 之后 s3s 的 `default_check`
    /// 不再兜底（设计 §1.3），匿名请求的安全性完全落在我们的 `check()` 上。
    /// 这条测试红了，就意味着服务变成了公开存储桶。
    #[tokio::test]
    async fn anonymous_is_still_rejected() {
        let (status, _h, _b) = call_on(service(store_with_alice()), request("GET", "/", b"")).await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    /// `set_auth` 与 `set_access` 缺一不可：只装 access 时检查会被整体跳过。
    /// 这条测试是「不变量 #7」的可执行版本——它断言的是一个**上游的行为**，
    /// 所以它变红意味着 s3s 改了语义，而不是我们的代码错了。
    #[tokio::test]
    async fn access_without_auth_skips_every_check() {
        let iam = store_with_alice();
        let mut b = S3ServiceBuilder::new(RstoreFs {
            store: Arc::new(NopStore),
        });
        b.set_access(IamAccess { iam });
        let bare = b.build();

        // 只装 access：匿名请求会**通过**——正是要防的那种静默失效。
        let (status, _h, _b) = call_on(bare, request("GET", "/", b"")).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "这条断言绿了说明「只装 access」确实会跳过检查——这正是必须成对设置的理由"
        );
    }

    /// root 签名 → 放行。
    #[tokio::test]
    async fn root_is_allowed() {
        let (status, _h, body) = call_on(
            service(store_with_alice()),
            signed_request_as(ACCESS_KEY, SECRET_KEY, "GET", "/", HOST, b""),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
    }

    /// 有身份但策略没允许 → 403（`ListBuckets` 的 ARN 是 `arn:aws:s3:::*`，
    /// 而 alice 的策略只覆盖 `photos/*`）。
    #[tokio::test]
    async fn alice_cannot_list_buckets() {
        let (status, _h, _b) = call_on(
            service(store_with_alice()),
            signed_request_as("alice", "alice-secret", "GET", "/", HOST, b""),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
    }

    /// 同一个 alice，读 `photos/` 下的对象：授权通过，于是落到 `NopStore` 上，
    /// 由它返回 500。**这条测的是「授权放行了」**——403 与 500 的区别就是判据。
    #[tokio::test]
    async fn alice_can_reach_get_object() {
        let (status, _h, _b) = call_on(
            service(store_with_alice()),
            signed_request_as("alice", "alice-secret", "GET", "/photos/a.jpg", HOST, b""),
        )
        .await;
        assert_ne!(
            status,
            StatusCode::FORBIDDEN,
            "授权不该拦下这条请求——策略是允许 photos/* 的读"
        );
    }

    /// 未知 access key：认证阶段就拒，**错误码是 AWS 的 `InvalidAccessKeyId`**。
    ///
    /// 它与「未签名」走的是两条不同的路：s3s 在 `authorize` **之前**就
    /// `verify_signature(...)?`，所以认证失败时我们的 `check()` 根本不会跑。
    #[tokio::test]
    async fn unknown_access_key_is_rejected() {
        let (status, _h, body) = call_on(
            service(store_with_alice()),
            signed_request_as("mallory", "whatever", "GET", "/photos/a.jpg", HOST, b""),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN);
        assert_eq!(crate::testutil::error_code(&body), "InvalidAccessKeyId");
    }

    /// **`set_access` 装了、`check` 忘了拒匿名**是最危险的回归形态。
    /// 这条测试直接对着 `IamStore::authorize` 的契约，与 HTTP 层无关：
    /// 删掉 `Anonymous` 那条分支会让它红。
    #[test]
    fn anonymous_denial_is_not_an_http_layer_detail() {
        let iam = store_with_alice();
        assert_eq!(
            iam.authorize(None, "s3:GetObject", "arn:aws:s3:::photos/a.jpg"),
            rstore_iam::Decision::Deny(rstore_iam::DenyReason::Anonymous)
        );
    }
}
