//! 加载期的错误。
//!
//! **全部在启动期产生**（设计 §7.1）：配置错误必须在进程起来之前炸掉。
//! 请求期的拒绝不走这里——它由 [`crate::Decision`] 表达，在 `rstore-s3` 里
//! 变成 `403 AccessDenied`。

use std::path::PathBuf;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum IamError {
    #[error("读取 {path} 失败：{source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("{path} 解析失败：{source}")]
    Parse {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },

    /// IAM 目录下的条目必须是 `*.json` 文件。出现目录说明放错了层级——
    /// 静默忽略会让人以为配置生效了。
    #[error("IAM 目录下不应出现子目录：{path}")]
    UnexpectedDir { path: PathBuf },

    #[error("用户 {user:?} 引用了不存在的策略 {policy:?}（期望 {expected}）")]
    UnknownPolicy {
        user: String,
        policy: String,
        expected: PathBuf,
    },

    #[error("用户的 access key {key:?} 与 root 凭据相同——同一个 key 不能既是 root 又是受限用户")]
    UserShadowsRoot { key: String },
}
