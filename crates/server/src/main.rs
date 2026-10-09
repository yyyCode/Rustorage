//! 二进制入口：解析参数、初始化日志、调 `startup::serve`。
//!
//! 这是独立的 crate root，不是 lib 的一部分，因此用 `rstore_server::...` 而不是
//! `crate::...`。

use anyhow::Context;
use clap::Parser;

use rstore_server::config::Config;
use rstore_server::startup;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Config::parse();
    startup::serve(&cfg).await.context("服务运行失败")
}
