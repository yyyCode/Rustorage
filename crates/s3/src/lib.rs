//! s3s 集成：S3 trait 实现、认证、错误映射。

use std::sync::Arc;

use rstore_api::ObjectStore;
use s3s::auth::SimpleAuth;
use s3s::host::SingleDomain;
use s3s::service::{S3Service, S3ServiceBuilder};

pub(crate) mod conditional;
pub(crate) mod errors;
pub mod iam;
pub mod impl_s3;
pub(crate) mod validate;

pub use iam::{IamAccess, IamAuth};

#[cfg(test)]
mod testutil;

#[cfg(test)]
mod mock;

pub use impl_s3::RstoreFs;

/// 装配 S3 service。**本 crate 唯一的装配入口**，启动流程（`rstore-server`）调它。
///
/// `base_domain` 为 `None` 时**只支持 path-style**（s3s 的默认 host 解析）；
/// 为 `Some(d)` 时开启虚拟主机风格：`Host: <bucket>.<d>` 会被解析成 `bucket = <bucket>`。
///
/// **返回 `Result` 是因为 `d` 可能不是合法域名**，那属于启动期配置错误——
/// 应该在进程启动时明确报错退出，而不是等到第一个请求变成一个费解的 400。
/// 所以这里用一个简单的 `String` 承载配置错误，不复用 `ApiError`（它是**请求期**的
/// 错误类型；用它会让「启动失败」和「请求失败」在同一处混起来）。
pub fn build_service(
    store: Arc<dyn ObjectStore>,
    access_key: &str,
    secret_key: &str,
    base_domain: Option<&str>,
) -> Result<S3Service, String> {
    let mut builder = S3ServiceBuilder::new(RstoreFs { store });
    builder.set_auth(SimpleAuth::from_single(access_key, secret_key));
    if let Some(domain) = base_domain {
        // `SingleDomain` 默认带 CNAME 回退（域外的 host 被整个当成桶名）。
        // **保留默认**：关掉它（`with_cname_fallback(false)`）会让「用别的域名
        // 指进来」的部署方式失效，而我们没有理由禁止它。
        let host = SingleDomain::new(domain)
            .map_err(|e| format!("invalid --base-domain {domain:?}: {e}"))?;
        builder.set_host(host);
    }
    // 不设 host 时走 s3s 的默认（纯 path-style）——**不要**显式设 PathStyle，
    // 那会把 s3s 换默认实现时的新行为挡在外面，而我们的默认行为就是它的默认行为。
    Ok(builder.build())
}
