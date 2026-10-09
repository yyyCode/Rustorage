//! 本地盘实现（Task 3.2）。
//!
//! [`LocalDisk`] 把 [`DiskAPI`] 的每个 async 方法都包装成一次
//! `tokio::task::spawn_blocking`：文件 IO 是阻塞的，直接放在 async 上下文里会占住
//! executor 线程。闭包必须是 `'static + Send`，因此盘根用 `Arc<PathBuf>`、
//! 相对路径用 owned `String` 传进去——**不能捕获 `&self`**。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use rstore_common::disk_id::DiskId;
use rstore_common::error::{DiskError, TransientKind};
use tokio::task::spawn_blocking;

use crate::error_map::map_io;
use crate::fsx;
use crate::{DiskAPI, FileStat};

/// 本地盘根。
pub struct LocalDisk {
    /// 盘根目录。`Arc` 便于 clone 进 `spawn_blocking` 闭包。
    root: Arc<PathBuf>,
    disk_id: DiskId,
}

impl LocalDisk {
    /// 打开一块盘。`root` 是盘根目录；`disk_id` 来自 format.json，首次初始化时新生成。
    ///
    /// 若盘根不存在则创建之（镜像 `mkdir -p` 语义）。
    pub fn open(root: impl AsRef<Path>, disk_id: DiskId) -> Result<Self, DiskError> {
        let root = root.as_ref();
        std::fs::create_dir_all(root).map_err(map_io)?;
        Ok(Self {
            root: Arc::new(root.to_path_buf()),
            disk_id,
        })
    }

    /// 盘根目录（供 M4 需要拼装物理路径时使用）。
    pub fn root(&self) -> &Path {
        &self.root
    }
}

/// 在阻塞线程池上运行一个可能返回 [`DiskError`] 的闭包。
///
/// `JoinError`（任务 panic / 被取消）**不**是数据损坏，映射为
/// `Transient(Io)`——调用方可重试。
async fn run_blocking<T, F>(f: F) -> Result<T, DiskError>
where
    F: FnOnce() -> Result<T, DiskError> + Send + 'static,
    T: Send + 'static,
{
    match spawn_blocking(f).await {
        Ok(res) => res,
        Err(_) => Err(DiskError::Transient(TransientKind::Io)),
    }
}

#[async_trait]
impl DiskAPI for LocalDisk {
    async fn write_all(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError> {
        let root = Arc::clone(&self.root);
        let rel = rel_path.to_owned();
        let data = data.to_vec();
        run_blocking(move || fsx::write_all_fsync(&root, &rel, &data)).await
    }

    async fn read_exact_at(
        &self,
        rel_path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, DiskError> {
        let root = Arc::clone(&self.root);
        let rel = rel_path.to_owned();
        run_blocking(move || fsx::read_exact_at(&root, &rel, offset, len)).await
    }

    async fn rename(&self, from_rel: &str, to_rel: &str) -> Result<(), DiskError> {
        let root = Arc::clone(&self.root);
        let from = from_rel.to_owned();
        let to = to_rel.to_owned();
        run_blocking(move || fsx::rename_fsync(&root, &from, &to)).await
    }

    async fn remove_dir_all(&self, rel_path: &str) -> Result<(), DiskError> {
        let root = Arc::clone(&self.root);
        let rel = rel_path.to_owned();
        run_blocking(move || fsx::remove_dir_all(&root, &rel)).await
    }

    async fn list_dir(&self, rel_path: &str) -> Result<Vec<String>, DiskError> {
        let root = Arc::clone(&self.root);
        let rel = rel_path.to_owned();
        run_blocking(move || fsx::list_dir(&root, &rel)).await
    }

    async fn stat(&self, rel_path: &str) -> Result<Option<FileStat>, DiskError> {
        let root = Arc::clone(&self.root);
        let rel = rel_path.to_owned();
        run_blocking(move || fsx::stat(&root, &rel)).await
    }

    async fn sync_file_and_parent(&self, rel_path: &str) -> Result<(), DiskError> {
        let root = Arc::clone(&self.root);
        let rel = rel_path.to_owned();
        run_blocking(move || fsx::sync_file_and_parent(&root, &rel)).await
    }

    fn disk_id(&self) -> &DiskId {
        &self.disk_id
    }

    fn is_local(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn write_then_read() {
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        d.write_all("a/b.txt", b"hello").await.unwrap();
        let got = d.read_exact_at("a/b.txt", 0, 5).await.unwrap();
        assert_eq!(got, b"hello");
    }

    #[tokio::test]
    async fn read_past_eof_is_transient_not_corrupt() {
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        d.write_all("a.txt", b"hello").await.unwrap();
        let err = d.read_exact_at("a.txt", 3, 10).await.unwrap_err();
        assert!(
            matches!(err, DiskError::Transient(_)),
            "short read must not be Corrupt"
        );
    }

    #[tokio::test]
    async fn missing_path_is_not_found() {
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        assert!(matches!(
            d.read_exact_at("nope", 0, 1).await,
            Err(DiskError::NotFound)
        ));
    }

    #[tokio::test]
    async fn rename_moves_and_old_path_gone() {
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        d.write_all("staging/f", b"x").await.unwrap();
        d.rename("staging", "final").await.unwrap();
        assert!(d.stat("staging/f").await.unwrap().is_none());
        assert!(d.stat("final/f").await.unwrap().is_some());
    }

    #[tokio::test]
    async fn rejects_path_escape() {
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        let r = d.write_all("../escape", b"x").await;
        assert!(matches!(r, Err(DiskError::Fatal(_))));
    }

    #[tokio::test]
    async fn rejects_embedded_path_escape() {
        // `../escape` 用字符串前缀检查也能拦下；这个不行——它证明检查是逐段做的。
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        let r = d.write_all("a/../../escape", b"x").await;
        assert!(
            matches!(r, Err(DiskError::Fatal(_))),
            "a/../../escape 逃出了盘根"
        );

        // 绝对路径也必须拒绝（Windows 上也包括盘符前缀）。
        assert!(matches!(
            d.write_all("/etc/passwd", b"x").await,
            Err(DiskError::Fatal(_))
        ));
    }

    #[tokio::test]
    async fn passes_shared_contract_suite() {
        let tmp = TempDir::new().unwrap();
        let d = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        crate::contract_tests::run_all(&d).await;
    }
}
