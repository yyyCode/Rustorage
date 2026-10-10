//! 命令行配置（启动契约）。MVP 的配置全部来自命令行参数，不读配置文件。

use std::path::PathBuf;

use rstore_store::set::default_parity;

/// 启动参数。6.4 的验收脚本与 5.9 的冒烟脚本都依赖这里的参数名与默认值。
///
/// 字段标成 `pub(crate)`：`startup` 模块里的测试要写结构体字面量造 `Config`，
/// 而 clap `derive` 出的字段默认是模块私有的，测试看不见会直接编译不过。
#[derive(clap::Parser)]
#[command(name = "rstore-server", about = "Rustorage MVP 单节点服务")]
pub struct Config {
    /// 盘根目录列表（`nargs(1..)`：接受一个或多个路径）。
    #[arg(long, required = true, num_args = 1..)]
    pub(crate) volumes: Vec<PathBuf>,

    /// HTTP 监听端口，只绑 `127.0.0.1`。
    #[arg(long, default_value_t = 9000)]
    pub(crate) port: u16,

    /// 校验分片数。未给时按盘数取默认。
    #[arg(long)]
    pub(crate) parity: Option<u8>,

    /// S3 访问密钥（与 5.9 冒烟脚本硬编码一致）。
    #[arg(long, default_value = "rustorage")]
    pub(crate) access_key: String,

    /// S3 秘密密钥（与 5.9 冒烟脚本硬编码一致）。
    #[arg(long, default_value = "rustorage-secret")]
    pub(crate) secret_key: String,

    /// 虚拟主机寻址的基础域名；不给 = 纯 path-style。
    #[arg(long)]
    pub(crate) base_domain: Option<String>,

    /// 打开 `/metrics`。
    #[arg(long)]
    pub(crate) metrics: bool,

    /// 打开只读控制面板（`/_console/`）。默认关闭：与 `--metrics` 一致（同样是
    /// opt-in），且开关关闭时既有验收脚本的行为零变化。
    #[arg(long)]
    pub(crate) console: bool,
}

impl Config {
    /// `--parity` 未给时按盘数取默认（`default_parity` 是 rstore-store 的公开函数：
    /// `rstore_store::set::default_parity`）。**不能用 clap 的 `default_value_t`**：
    /// 那个默认值在解析期就得是常量，而这里依赖 `--volumes` 的个数。
    /// 默认值 6 块盘时是 3（3+3），而 6.4 的验收脚本要 4+2 → 脚本必须显式传 `--parity 2`。
    pub(crate) fn parity(&self) -> u8 {
        self.parity
            .unwrap_or_else(|| default_parity(self.volumes.len() as u8))
    }
}
