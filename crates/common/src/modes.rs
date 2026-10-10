//! 读写路径的模式开关（**临时**）。
//!
//! **存在的唯一目的**：让「旧路径」（今天的实现）与「新路径」（四个加速机制）
//! 同时存在于一个二进制里，从而能在同一份数据集、同一台机器上直接对比测量。
//!
//! **全部字段为 `false` 时，行为与引入本模块之前逐字节相同**——
//! `ErasureSet::new` 的语义没有变，它只是委托给了 `with_modes(.., IoModes::default())`。
//!
//! 基准跑完即整体删除本模块，各机制各自成为唯一实现。见
//! `docs/superpowers/specs/2026-10-10-read-write-path-modes-design.md` §10。

/// 四个机制各自独立，互不耦合。
///
/// 用四个具名 `bool` 而不是 bitflags：为了**可 grep、可逐个删**。
/// 加一个 `bitflags` 依赖换来的只是更短的代码，而这份代码只活到基准跑完。
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IoModes {
    /// 范围读：只读命中的块区间，不再把整份分片读进内存。
    pub ranged_shard_read: bool,
    /// 元数据缓存：`get_object` / `head_object` 复用最近解析出的版本。
    pub metadata_cache: bool,
    /// 有界列举：LIST 增量遍历，够数即停，不再全量物化。
    pub bounded_listing: bool,
    /// 写入缓冲复用：单次 PUT 内跨块复用分片缓冲。
    pub pooled_write_buffers: bool,
}

impl IoModes {
    /// 四个机制全开。
    pub const ALL: Self = Self {
        ranged_shard_read: true,
        metadata_cache: true,
        bounded_listing: true,
        pooled_write_buffers: true,
    };

    /// 从 CLI 的 `old` / `new` 解析；其他值返回 `None`（由调用方报错）。
    ///
    /// **刻意不暴露逐位开关**：CLI 面上只留两档，避免产生「半新半旧」的运维组合。
    /// 逐位组合只在测试里用（阶梯式归因需要 `+A` → `+A,C` → `+A,C,D`）。
    pub fn from_io_mode(s: &str) -> Option<Self> {
        match s {
            "old" => Some(Self::default()),
            "new" => Some(Self::ALL),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 「旧」必须恰好等于 `Default`（全关），「新」必须四项全开。
    /// 这条钉住 CLI 两档与结构体默认值之间的对应关系——漂了就会出现
    /// 「指定 `old` 却拿到了部分新机制」这种最难查的对比失真。
    #[test]
    fn old_is_all_off_and_new_is_all_on() {
        // 走 `from_io_mode` 拿值而不是直接读 `IoModes::ALL` / `default()`：
        // 一来顺带把解析路径测了，二来对常量表达式直接断言会被 clippy 的
        // `assertions_on_constants` 拒掉（它求值后发现恒真）。
        let off = IoModes::from_io_mode("old").expect("old 是合法取值");
        let on = IoModes::from_io_mode("new").expect("new 是合法取值");

        assert_eq!(off, IoModes::default());
        assert_eq!(on, IoModes::ALL);

        // 逐字段再断一遍：整体比较已经覆盖「全关/全开」，但逐项写出来，
        // 失败时能直接指出是哪一个开关漂了。
        assert!(!off.ranged_shard_read);
        assert!(!off.metadata_cache);
        assert!(!off.bounded_listing);
        assert!(!off.pooled_write_buffers);

        assert!(on.ranged_shard_read);
        assert!(on.metadata_cache);
        assert!(on.bounded_listing);
        assert!(on.pooled_write_buffers);
    }

    /// 未知取值必须返回 `None` 而不是悄悄退化成某一档：静默的默认值会让
    /// 「我明明指定了 new」变成一次无效的测量。
    #[test]
    fn unknown_mode_is_none() {
        assert_eq!(IoModes::from_io_mode("half"), None);
        assert_eq!(IoModes::from_io_mode(""), None);
        assert_eq!(IoModes::from_io_mode("NEW"), None);
    }
}
