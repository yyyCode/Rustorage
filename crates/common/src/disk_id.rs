//! 盘标识。

use uuid::Uuid;

/// 一块盘的稳定标识（DESIGN §6.2：落在 `<disk>/.rstore.sys/disk_id`）。
///
/// **定义在 `common` 而不是 `disk`**：Task 3.3 的 `format.json` 也要用它，
/// 而依赖方向是 `disk → meta`，meta 无法反向依赖 disk。
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct DiskId(Uuid);

impl DiskId {
    /// 生成一个新的随机（v4）盘标识。
    pub fn new_v4() -> Self {
        Self(Uuid::new_v4())
    }

    /// 从 16 字节裸表示构造。
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(Uuid::from_bytes(bytes))
    }

    /// 16 字节裸表示。
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

/// canonical（带连字符）形式，例如 `550e8400-e29b-41d4-a716-446655440000`。
/// Task 3.3 要把它写进 `format.json`，直接复用 `Uuid` 的 `Display`，不自行拼十六进制。
impl std::fmt::Display for DiskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
