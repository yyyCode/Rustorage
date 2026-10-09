//! DiskAPI 契约与本地盘实现。
//!
//! 本模块只定义**契约**（trait + 共享契约测试），具体实现见 Task 3.2 的 `local.rs`。

pub mod error_map;
pub mod fsx;
pub mod local;

pub use local::LocalDisk;

// 故障注入盘（Task 3.4）。门控用 `any(test, feature)`：crate 内单测靠 `cfg(test)`
// 自动可见；跨 crate（如 `rstore-store` 的集成测试）是独立编译的 crate，
// `cfg(test)` 不生效，需显式开启 `fault-injection` feature。
#[cfg(any(test, feature = "fault-injection"))]
pub mod faulty;
#[cfg(any(test, feature = "fault-injection"))]
pub use faulty::{Fault, FaultKind, FaultyDisk};

use rstore_common::disk_id::DiskId;
// 公开再导出：集成测试按 `rstore_disk::DiskError` 引用它（`rstore-disk` 是独立编译的
// crate，只能看到本 crate 的公开路径），无需再单独依赖 `rstore-common`。
pub use rstore_common::error::DiskError;

/// 一块盘的元信息。只有 disk 层用（不是线格式），故定义在这里而非 common。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStat {
    pub size: u64,
    pub is_dir: bool,
}

/// 一块盘。对应 DESIGN §4。
///
/// **所有方法都返回 `Result<_, DiskError>`，不允许 panic。**
/// 实现必须把 IO 错误映射为 `DiskError` 的三级分类。（Task 3.2 会加 `error_map.rs`。）
#[async_trait::async_trait]
pub trait DiskAPI: Send + Sync {
    async fn write_all(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError>;
    async fn read_exact_at(
        &self,
        rel_path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, DiskError>;
    async fn rename(&self, from_rel: &str, to_rel: &str) -> Result<(), DiskError>;
    async fn remove_dir_all(&self, rel_path: &str) -> Result<(), DiskError>;
    async fn list_dir(&self, rel_path: &str) -> Result<Vec<String>, DiskError>;
    async fn stat(&self, rel_path: &str) -> Result<Option<FileStat>, DiskError>;
    /// fsync 文件本身与父目录（保证 rename 的持久性）。
    async fn sync_file_and_parent(&self, rel_path: &str) -> Result<(), DiskError>;
    fn disk_id(&self) -> &DiskId;
    fn is_local(&self) -> bool;
}

/// 任意 `DiskAPI` 实现都必须通过的共享契约测试。
///
/// 盘根由 `disk` 自身携带，不另传 `tmp`（Task 3.2 / 3.4 的调用点都是 `run_all(&d).await`）。
pub mod contract_tests {
    use super::DiskAPI;
    use rstore_common::error::DiskError;

    /// 对任意 `D: DiskAPI` 都应通过。断言必须是实现无关的——不依赖具体路径布局，
    /// 只依赖契约约定的可观察行为。
    pub async fn run_all<D: DiskAPI + ?Sized>(disk: &D) {
        // 契约内部自造的临时相对路径。先尽力清理上一次可能残留的目录（忽略错误）。
        let base = "__contract__";
        let _ = disk.remove_dir_all(base).await;

        let file = "__contract__/probe";
        let payload: &[u8] = b"contract-payload";

        // 写后读回。
        disk.write_all(file, payload)
            .await
            .expect("write_all should succeed");
        let got = disk
            .read_exact_at(file, 0, payload.len())
            .await
            .expect("read_exact_at should succeed after write");
        assert_eq!(got.as_slice(), payload, "read back mismatch");

        // stat 反映大小（size 是 M4 读路径需要的）。
        let st = disk
            .stat(file)
            .await
            .expect("stat should succeed")
            .expect("probe must exist");
        assert_eq!(st.size, payload.len() as u64, "stat size mismatch");
        assert!(!st.is_dir, "probe must not be a directory");

        // 读不存在的路径 -> NotFound，而非 panic。
        match disk
            .read_exact_at("__contract__/does-not-exist", 0, 1)
            .await
        {
            Err(DiskError::NotFound) => {}
            other => panic!("missing path must be NotFound, got {other:?}"),
        }

        // rename 后旧路径 NotFound、新路径可读。
        let src = "__contract__/src";
        let dst = "__contract__/dst";
        disk.write_all(src, payload)
            .await
            .expect("write src should succeed");
        disk.rename(src, dst).await.expect("rename should succeed");
        match disk.read_exact_at(src, 0, 1).await {
            Err(DiskError::NotFound) => {}
            other => panic!("old path must be NotFound after rename, got {other:?}"),
        }
        let moved = disk
            .read_exact_at(dst, 0, payload.len())
            .await
            .expect("read dst should succeed after rename");
        assert_eq!(moved.as_slice(), payload, "renamed content mismatch");

        // list_dir 排序稳定：返回的 Vec 必须已按字典序升序。
        let entries = disk.list_dir(base).await.expect("list_dir should succeed");
        assert!(
            entries.windows(2).all(|w| w[0] <= w[1]),
            "list_dir must be sorted ascending, got {entries:?}"
        );

        // sync_file_and_parent 幂等：连调两次都成功。
        disk.sync_file_and_parent(file)
            .await
            .expect("first sync should succeed");
        disk.sync_file_and_parent(file)
            .await
            .expect("second sync should be idempotent");

        // 清理自造的临时目录。
        disk.remove_dir_all(base)
            .await
            .expect("cleanup should succeed");
    }
}
