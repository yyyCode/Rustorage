//! 故障注入盘（Task 3.4）。
//!
//! M4 的崩溃 / 损坏测试要稳定复现「写成功但数据丢失」「静默位翻转」「第 N 次调用
//! 起整盘离线」这类场景，而真实 [`LocalDisk`](crate::local::LocalDisk) 无法制造它们。
//! [`FaultyDisk`] 包住任意 [`DiskAPI`]，在委托给内层盘之前插入可控故障。
//!
//! 模块由 `#[cfg(any(test, feature = "fault-injection"))]` 门控：crate 内单测靠
//! `cfg(test)` 自动可见；跨 crate（如 `rstore-store` 的集成测试）是**独立编译**的
//! crate，`cfg(test)` 在那里不生效，必须显式开启 `fault-injection` feature。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use rstore_common::disk_id::DiskId;
use rstore_common::error::{CorruptKind, DiskError, TransientKind};

use crate::{DiskAPI, FileStat};

/// 注入的故障。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// 写调用对外报成功，但数据不落盘（模拟丢失 fsync）。
    DropWrites,
    /// 只写入前一半字节（模拟撕裂写）。
    PartialWrite,
    /// 写入时在偏移 `at` 处按 `mask` 异或——**静默损坏**：不报任何错，读回来的字节就是错的。
    ///
    /// `at` 越界时视为 no-op（`DiskAPI` 约定实现不得 panic）。
    CorruptBytes { at: usize, mask: u8 },
    /// 只写入前 `len` 字节，其余丢弃（`PartialWrite` 的带参形式）。
    ///
    /// 语义上等价于「写完后把文件截断到 `len`」，但实现是**写前变换**：`DiskAPI` 没有
    /// `truncate`/`set_len`，拿不到任何写入后缩短文件的手段。而 [`LocalDisk`] 的写是
    /// 「创建即截断」，只写前 `len` 字节得到的正是一个 `len` 字节的文件。
    ///
    /// [`LocalDisk`]: crate::local::LocalDisk
    Truncate { len: usize },
    /// **前 `calls` 次调用正常**，第 `calls + 1` 次起一律返回 `kind` 对应的错误。
    ///
    /// **计数器计的是「任意 [`DiskAPI`] 方法的调用次数」，不区分读写**——
    /// `write_all`、`read_exact_at`、`rename`、`remove_dir_all`、`list_dir`、`stat`、
    /// `sync_file_and_parent` 各计一次；`disk_id()` / `is_local()` 这类非 `Result` 方法
    /// **不**计。M4 的崩溃点测试必须按这个口径推算 `calls`，否则断言会随机飘。
    FailAfter { calls: usize, kind: FaultKind },
    /// 所有调用都返回 `Transient`（模拟盘离线）。
    Offline,
}

/// `FailAfter` / `Offline` 要伪造的错误种类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    Transient,
    Corrupt,
    NotFound,
}

impl FaultKind {
    /// 映射为本层要吐出的 [`DiskError`]。
    fn to_error(self) -> DiskError {
        match self {
            FaultKind::Transient => DiskError::Transient(TransientKind::Io),
            // 模拟 bitrot：M4 的 heal 路径最关心的就是这种确定性损坏。
            FaultKind::Corrupt => DiskError::Corrupt(CorruptKind::BitrotMismatch),
            FaultKind::NotFound => DiskError::NotFound,
        }
    }
}

/// 包住内层盘、按需注入故障的 [`DiskAPI`] 实现。
///
/// 默认无故障，此时行为与内层盘**完全一致**（由 `contract_tests` 钉死）。
pub struct FaultyDisk {
    inner: Box<dyn DiskAPI>,
    /// 已发生的、会被 `FailAfter` 计数的调用次数（不含 `disk_id` / `is_local`）。
    calls: AtomicUsize,
    /// 当前故障；`None` 即透明委托。`&self` 下可变，靠 `Mutex` 提供内部可变性。
    fault: Mutex<Option<Fault>>,
}

impl FaultyDisk {
    /// 包一层。初始无故障。
    pub fn wrap(inner: impl DiskAPI + 'static) -> Self {
        Self {
            inner: Box::new(inner),
            calls: AtomicUsize::new(0),
            fault: Mutex::new(None),
        }
    }

    /// builder 风格：消耗并返回，用于构造后立即设一次故障。
    pub fn with(self, fault: Fault) -> Self {
        self.set_fault(fault);
        self
    }

    /// 原地改故障（测试中途切换用）。`&self` —— 内部靠 `Mutex` 提供可变性，
    /// 所以测试里的 `let d = ...` 不需要 `mut`。
    ///
    /// **不重置调用计数器**：它是一块盘自构造以来的累计调用数（口径见
    /// [`Fault::FailAfter`]）。中途切到 `FailAfter` 时 `calls` 是与「构造以来」对齐的，
    /// 不是与「切故障那一刻」对齐的——想从切换点起算，把已发生的调用数加进 `calls`。
    pub fn set_fault(&self, fault: Fault) {
        *self.fault.lock().expect("fault mutex poisoned") = Some(fault);
    }

    /// 清除故障，回到与内层盘完全一致的行为。
    pub fn clear_fault(&self) {
        *self.fault.lock().expect("fault mutex poisoned") = None;
    }

    /// 记一次调用，并返回「当前故障是否要求本次调用直接短路失败」。
    ///
    /// 计数器与判定放在一起，避免漏掉某个方法、让调用次数与 `FailAfter` 口径对不上。
    /// 锁在返回前即释放（`Fault` 是 `Copy`），不会跨 `await` 持锁。
    fn begin_call(&self) -> Result<(), DiskError> {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        match *self.fault.lock().expect("fault mutex poisoned") {
            Some(Fault::FailAfter { calls, kind }) if n > calls => Err(kind.to_error()),
            Some(Fault::Offline) => Err(FaultKind::Transient.to_error()),
            _ => Ok(()),
        }
    }

    /// 取当前故障的快照（同样是取值而非持锁）。
    fn current_fault(&self) -> Option<Fault> {
        *self.fault.lock().expect("fault mutex poisoned")
    }
}

#[async_trait]
impl DiskAPI for FaultyDisk {
    async fn write_all(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError> {
        self.begin_call()?;

        // 写路径的 payload 变换一律在「交给内层盘之前」做（见 `Fault::Truncate` 的说明）。
        match self.current_fault() {
            // 对外报成功，但根本不落盘。
            Some(Fault::DropWrites) => Ok(()),
            Some(Fault::PartialWrite) => {
                let half = data.len() / 2;
                self.inner.write_all(rel_path, &data[..half]).await
            }
            Some(Fault::Truncate { len }) => {
                let n = len.min(data.len());
                self.inner.write_all(rel_path, &data[..n]).await
            }
            Some(Fault::CorruptBytes { at, mask }) => {
                let mut buf = data.to_vec();
                // 越界视为 no-op：`DiskAPI` 约定实现不得 panic。
                if let Some(byte) = buf.get_mut(at) {
                    *byte ^= mask;
                }
                self.inner.write_all(rel_path, &buf).await
            }
            // `None` / `FailAfter`（未触发）/ `Offline`（已由 `begin_call` 拦下）→ 透明委托。
            _ => self.inner.write_all(rel_path, data).await,
        }
    }

    async fn read_exact_at(
        &self,
        rel_path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, DiskError> {
        // 读路径一律不做额外校验：只把已经损坏的字节原样交出去，
        // 否则就模拟不出「静默损坏」了（校验是 meta 层的事）。
        self.begin_call()?;
        self.inner.read_exact_at(rel_path, offset, len).await
    }

    async fn rename(&self, from_rel: &str, to_rel: &str) -> Result<(), DiskError> {
        self.begin_call()?;
        self.inner.rename(from_rel, to_rel).await
    }

    async fn remove_dir_all(&self, rel_path: &str) -> Result<(), DiskError> {
        self.begin_call()?;
        self.inner.remove_dir_all(rel_path).await
    }

    async fn list_dir(&self, rel_path: &str) -> Result<Vec<String>, DiskError> {
        self.begin_call()?;
        self.inner.list_dir(rel_path).await
    }

    async fn stat(&self, rel_path: &str) -> Result<Option<FileStat>, DiskError> {
        self.begin_call()?;
        self.inner.stat(rel_path).await
    }

    async fn sync_file_and_parent(&self, rel_path: &str) -> Result<(), DiskError> {
        self.begin_call()?;
        self.inner.sync_file_and_parent(rel_path).await
    }

    fn disk_id(&self) -> &DiskId {
        self.inner.disk_id()
    }

    fn is_local(&self) -> bool {
        self.inner.is_local()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_common::disk_id::DiskId;

    #[tokio::test]
    async fn passes_shared_contract_suite_when_healthy() {
        // `FaultyDisk` 自身的正确性门禁：无故障注入时它必须与 LocalDisk 行为一致。
        // 否则 M4 里「FaultyDisk 测出来的故障」可能根本是它自己引入的。
        let tmp = tempfile::TempDir::new().unwrap();
        let inner = crate::local::LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        crate::contract_tests::run_all(&FaultyDisk::wrap(inner)).await;
    }

    #[tokio::test]
    async fn truncate_writes_exactly_len_bytes() {
        // 钉住 `Truncate` 的写前变换语义：只写前 `len` 字节。
        let tmp = tempfile::TempDir::new().unwrap();
        let inner = crate::local::LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        let d = FaultyDisk::wrap(inner).with(Fault::Truncate { len: 2 });
        d.write_all("f", b"hello").await.unwrap();
        assert_eq!(d.read_exact_at("f", 0, 2).await.unwrap(), b"he");
        assert!(matches!(
            d.read_exact_at("f", 0, 5).await,
            Err(DiskError::Transient(TransientKind::ShortRead))
        ));
    }

    #[tokio::test]
    async fn fail_after_counts_reads_and_writes_alike() {
        // `FailAfter` 的口径：计数不区分读写。写 1 次 + 读 1 次后，第 3 次读就应失败。
        let tmp = tempfile::TempDir::new().unwrap();
        let inner = crate::local::LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
        let d = FaultyDisk::wrap(inner).with(Fault::FailAfter {
            calls: 2,
            kind: FaultKind::Corrupt,
        });
        d.write_all("f", b"x").await.unwrap(); // 第 1 次
        d.read_exact_at("f", 0, 1).await.unwrap(); // 第 2 次
        assert_eq!(
            d.read_exact_at("f", 0, 1).await.unwrap_err(),
            DiskError::Corrupt(CorruptKind::BitrotMismatch)
        ); // 第 3 次
    }
}
