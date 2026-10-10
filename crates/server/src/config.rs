//! 命令行配置（启动契约）。MVP 的配置全部来自命令行参数，不读配置文件。

use std::path::PathBuf;

use rstore_common::modes::IoModes;
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

    /// IAM 配置目录（`users/*.json` 与 `policies/*.json`）。未给时用
    /// `<volumes[0]>/.rstore/iam`——放在盘上而不是进程的工作目录里，
    /// 因为「配置跟着数据走」：换台机器起同一个盘集，身份与权限原样带过去。
    ///
    /// 该目录放在 `RESERVED_PREFIX`（`.rstore`）之下，而 S3 命名空间里的用户 key
    /// 不允许以 `.rstore` 开头，所以桶/对象**结构上够不到**这份配置。
    #[arg(long)]
    pub(crate) iam_dir: Option<PathBuf>,

    /// 读写路径模式：`old` = 今天的实现；`new` = 四个加速机制全开。
    ///
    /// **默认 `old`**：新路径还没跑过完整接受度验证，不该在无人察觉时成为默认。
    /// 这是性能对比用的开关，基准做完后整体删除（见设计文档 §10）。
    ///
    /// **只暴露两档**，不做逐位开关：CLI 面上出现「半新半旧」的组合只会让
    /// 每一次测量都需要额外解释自己开的是哪几个机制。
    #[arg(long, value_name = "MODE", default_value = "old", value_parser = ["old", "new"])]
    pub(crate) io_mode: String,
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

    /// IAM 目录的**实际**取值：显式给了就用，否则 `<volumes[0]>/<RESERVED_PREFIX>/iam`。
    ///
    /// 与 `parity()` 同一形状——clap 的 `default_value_t` 表达不了「依赖另一个参数」，
    /// 所以默认值在这里拼。
    pub(crate) fn iam_dir(&self) -> PathBuf {
        self.iam_dir.clone().unwrap_or_else(|| {
            self.volumes[0]
                .join(rstore_common::consts::RESERVED_PREFIX)
                .join("iam")
        })
    }

    /// 解析成 [`IoModes`]。
    ///
    /// clap 的 `value_parser` 已经把取值限死在 `old` / `new`，所以这个 `expect`
    /// 是**可达性断言**而不是错误处理：真走到 `None` 说明 `arg` 属性被改坏了。
    pub(crate) fn modes(&self) -> IoModes {
        IoModes::from_io_mode(&self.io_mode).expect("clap value_parser 已把取值限死为 old|new")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(volumes: Vec<PathBuf>, iam_dir: Option<PathBuf>) -> Config {
        Config {
            volumes,
            port: 0,
            parity: None,
            access_key: "rustorage".into(),
            secret_key: "rustorage-secret".into(),
            base_domain: None,
            metrics: false,
            console: false,
            iam_dir,
            io_mode: "old".into(),
        }
    }

    /// 未给 `--iam-dir` 时落在**第一块盘**的 `.rstore/iam` 下。
    /// 用 `RESERVED_PREFIX` 常量而不是字面量 `.rstore`：拼错了这条测试会红，
    /// 而不是让服务悄悄去一个用户 key 够得到的目录里读凭据。
    #[test]
    fn iam_dir_defaults_to_first_volume() {
        let c = cfg(vec![PathBuf::from("/d1"), PathBuf::from("/d2")], None);
        assert_eq!(
            c.iam_dir(),
            PathBuf::from("/d1")
                .join(rstore_common::consts::RESERVED_PREFIX)
                .join("iam")
        );
    }

    /// 显式给了 `--iam-dir` 就以它为准——运维要能把配置放到别处（例如只读挂载）。
    #[test]
    fn explicit_iam_dir_wins() {
        let c = cfg(
            vec![PathBuf::from("/d1")],
            Some(PathBuf::from("/etc/rstore-iam")),
        );
        assert_eq!(c.iam_dir(), PathBuf::from("/etc/rstore-iam"));
    }

    /// 不指定时必须是 `old`（全关）——这条钉住「默认行为与今天一致」。
    #[test]
    fn io_mode_defaults_to_old_and_new_switches_everything_on() {
        let mut c = cfg(vec![PathBuf::from("/d1")], None);
        assert_eq!(c.modes(), IoModes::default());

        c.io_mode = "new".into();
        assert_eq!(c.modes(), IoModes::ALL);
    }
}
