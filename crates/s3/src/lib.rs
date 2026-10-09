//! s3s 集成：S3 trait 实现、认证、错误映射。

pub(crate) mod errors;
pub mod impl_s3;
pub(crate) mod validate;

#[cfg(test)]
mod mock;

pub use impl_s3::RstoreFs;
