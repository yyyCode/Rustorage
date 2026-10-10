//! 身份与访问管理：策略文档、身份存储与求值。
//!
//! **不依赖任何 `rstore-*` crate，也不认识 s3s**——动作名与资源名都是 `&str`，
//! 盘上目录由调用方拼好路径传进来。于是它的单元测试不需要起 HTTP、不需要构造
//! `S3Service`，`cargo test -p rstore-iam` 秒过。
//!
//! 与 s3s 的接线（`S3Auth` / `S3Access` 两个适配器）在 `crates/s3/src/iam.rs`。
//!
//! 设计依据：`docs/superpowers/specs/2026-10-10-iam-design.md`。

pub mod action;
pub mod arn;
pub mod glob;
pub mod policy;
