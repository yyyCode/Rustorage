//! M4 测试共用的夹具。`#[cfg(test)]` 门控（见 `lib.rs`），不进发布产物。

use std::sync::Arc;

use rstore_common::disk_id::DiskId;
use rstore_common::error::DiskError;
use rstore_disk::{DiskAPI, Fault, FaultyDisk, FileStat, LocalDisk};

use rstore_common::modes::IoModes;

use crate::set::ErasureSet;

/// 内存 `AsyncRead`：各模块的测试用它把 `Vec<u8>` 喂进流式的
/// [`PutArgs`](crate::put::PutArgs)。
///
/// tokio 给 `std::io::Cursor<T: AsRef<[u8]>>` 实现了 `AsyncRead`
/// （tokio-1.53.2/src/io/async_read.rs:111），不必自己造一个读取器。
///
/// 放在这里而不是各测试模块各写一份：它已经要被 put / bucket / delete / get /
/// list / reconcile / quorum_boundaries 七处用到。
pub(crate) fn body(data: Vec<u8>) -> Box<dyn tokio::io::AsyncRead + Unpin + Send> {
    Box::new(std::io::Cursor::new(data))
}

/// 一个已挂好 `FaultyDisk` 的 erasure set。
///
/// **为什么需要这层包装**：`ErasureSet::disks` 存的是 `Arc<dyn DiskAPI>`，
/// 类型已经擦除，拿不回 `FaultyDisk` 去调 `set_fault`。所以夹具必须自己
/// 留一份强类型句柄。原计划让 `ErasureSet` 直接提供 `inject_fault_on(i, fault)`，
/// 那是做不到的——`ErasureSet` 是发布代码，不该认识只在测试里存在的 `FaultyDisk`。
///
/// `Deref<Target = ErasureSet>` 让 `set.put_object(..)`、`set.disks()`、
/// `commit(&set, ..)`（`&TestSet` 自动 deref 成 `&ErasureSet`）都能照常写。
pub struct TestSet {
    set: ErasureSet,
    faulties: Vec<Arc<FaultyDisk>>,
    /// 持有临时目录，随 `TestSet` drop 一起清理。
    _dir: tempfile::TempDir,
}

impl std::ops::Deref for TestSet {
    type Target = ErasureSet;
    fn deref(&self) -> &ErasureSet {
        &self.set
    }
}

impl TestSet {
    /// 往第 `i` 块盘注入故障。`&self`（`FaultyDisk` 内部用 `Mutex`），
    /// 所以测试里 `let set = ...` 不必声明 `mut`。
    ///
    /// **实现时必须先调 `FaultyDisk::reset_call_count()` 再 `set_fault()`。**
    /// `FaultyDisk` 的调用计数是「自构造以来」的累计值，`set_fault` 与 `clear_fault`
    /// 都不重置它（见 `faulty.rs` 的文档）。不重置的话，
    /// 「已经跑过一次 PUT 之后再注入 `FailAfter { calls: 2 }`」会立刻全部失败——
    /// 因为计数器早就超过 2 了，于是测试得不到它想要的那个中断位置。
    /// 在这里统一成「从注入这一刻起再放行 `calls` 次」，语义才与直觉一致。
    pub fn inject_fault_on(&self, i: usize, fault: Fault) {
        self.faulties[i].reset_call_count();
        self.faulties[i].set_fault(fault);
    }

    /// 撤销第 `i` 块盘的故障，回到正常行为。
    pub fn clear_fault_on(&self, i: usize) {
        self.faulties[i].clear_fault();
    }

    /// 在**每一块**盘上写出 `rel_path`（只有 `write_all` 会失败才跳过，正常情况全成功）。
    ///
    /// 给 `commit` 造出 staging 目录用的：`commit` 做的是 rename，
    /// **源路径不存在时 rename 会以 `NotFound` 失败**。少了这一步，Task 4.4 里
    /// 每块盘的 rename 都会失败、`achieved` 恒为 0——「绝不在低于 quorum 时报告成功」
    /// 那条最重要的不变量测试就会**空洞地通过**（`Ok` 分支一次都进不去）。
    pub async fn write_probe(&self, rel_path: &str, data: &[u8]) {
        for disk in self.set.disks().iter().flatten() {
            // 失败只跳过：夹具的职责是让大多数盘上有可 rename 的源路径，
            // 个别盘写不进去不该让整个测试 panic。
            let _ = disk.write_all(rel_path, data).await;
        }
    }
}

/// [`set_with_modes`] 的旧模式特化。**`set_with_disks(6, 2)` 读作「6 块盘、parity=2、
/// data=4、四个机制全关」**——整个 M4 的测试都用这个约定。
///
/// 它就是「旧模式」在夹具层的定义：`set_with_disks` 与
/// `set_with_modes(.., IoModes::default())` 必须永远等价，由
/// `set_with_disks_is_old_mode` 钉住。
pub async fn set_with_disks(total: u8, parity: u8) -> TestSet {
    set_with_modes(total, parity, IoModes::default()).await
}

/// 建 `total` 块盘、`parity` 为 `parity` 的 set，并指定读写路径模式。
///
/// 盘 `i` 的根是独立子目录 `{tmp}/disk{i}`：同根的话 6 块「盘」其实是同一个目录，
/// 「6 副本、掉 2 块还能读」这些性质会退化成同义反复，测试全绿却什么都没测到。
pub async fn set_with_modes(total: u8, parity: u8, modes: IoModes) -> TestSet {
    let dir = tempfile::TempDir::new().expect("create tempdir");

    let mut disks: Vec<Option<Arc<dyn DiskAPI>>> = Vec::with_capacity(total as usize);
    let mut faulties: Vec<Arc<FaultyDisk>> = Vec::with_capacity(total as usize);
    for i in 0..total as usize {
        let root = dir.path().join(format!("disk{i}"));
        let inner = LocalDisk::open(&root, DiskId::new_v4()).expect("open local disk");
        let faulty = Arc::new(FaultyDisk::wrap(inner));
        faulties.push(Arc::clone(&faulty));
        // 具体类型 `Arc<FaultyDisk>` 在此处 unsize 成 `Arc<dyn DiskAPI>` 存入槽位。
        let erased: Arc<dyn DiskAPI> = faulty;
        disks.push(Some(erased));
    }

    let set = ErasureSet::with_modes(disks, parity, modes).expect("valid erasure set geometry");
    TestSet {
        set,
        faulties,
        _dir: dir,
    }
}

/// 写入长度日志。`Arc` 包一层是为了让 `RecordingDisk` 与测试各持一份。
///
/// 每条记录是 `(rel_path, payload 长度)`。**为什么要留路径**：`meta.xl` 的长度在
/// 两次独立的 PUT 之间**本来就不一样**——它把两个 UUID 经 `HeaderWire` 的
/// `[u8; 16]` 桥接字段编码，而 `rmp_serde` 对每个 ≥ 0x80 的字节用 2 字节的
/// `uint8`（`0xcc`）、其余用 1 字节的正整数，于是同一个字段的编码长度在
/// 19~35 字节之间随 UUID 的随机字节浮动（实测两份 `meta.xl` 相差 185~193）。
/// 这是既有的格式性质，与任何一次写路径改动无关。要比「写入的形状没变」，
/// 就只能比**受该改动影响的那部分**——见 [`WriteLog::sizes_of`]。
#[derive(Clone, Default)]
pub struct WriteLog(Arc<std::sync::Mutex<Vec<(String, usize)>>>);

impl WriteLog {
    /// 所有写入 payload 的长度，`write_all` 与 `append` 合并、按时序。
    pub fn sizes(&self) -> Vec<usize> {
        self.0
            .lock()
            .expect("write log poisoned")
            .iter()
            .map(|(_, n)| *n)
            .collect()
    }

    /// 只取**最后一段路径等于 `file_name`** 的那些写入的长度（如 `"part.1"`）。
    ///
    /// **按最后一段匹配而不是全路径**：`rel_path` 里带着每次 PUT 都不一样的
    /// `data_dir` uuid 与 `.staging-<txid>`，全路径比对永远不等。分片文件名
    /// （`part.1`）才是两次 PUT 之间可比的部分。
    pub fn sizes_of(&self, file_name: &str) -> Vec<usize> {
        self.0
            .lock()
            .expect("write log poisoned")
            .iter()
            .filter(|(p, _)| p.rsplit('/').next() == Some(file_name))
            .map(|(_, n)| *n)
            .collect()
    }

    fn record(&self, rel_path: &str, n: usize) {
        self.0
            .lock()
            .expect("write log poisoned")
            .push((rel_path.to_string(), n));
    }
}

/// 记录**每次写入 payload 长度**的盘，用于断言写路径的内存上界。
///
/// **为什么不是「分配计数探针」**：那需要一个 `GlobalAlloc` 实现，而它必须
/// `unsafe`，workspace lint 是 `unsafe_code = "forbid"`。记录写入长度是同一件事的
/// 可实现版本：整份缓冲会表现为「一次巨大的写入」，照样能抓住。
///
/// **`write_all` 与 `append` 都要记**——只看 `append` 的话，一个退回
/// 「缓冲整份分片再 `write_all`」的实现会在这一层完全隐形（那正是要防的回归）。
///
/// **它包在最内层**（`LocalDisk` 之外、`FaultyDisk` 之内）：故障注入与写入计数
/// 于是观测的是同一批调用，两者不会互相遮蔽。见 [`set_with_recording_disks`]。
pub struct RecordingDisk {
    inner: Arc<dyn DiskAPI>,
    log: WriteLog,
}

impl RecordingDisk {
    pub fn new(inner: Arc<dyn DiskAPI>, log: WriteLog) -> Self {
        Self { inner, log }
    }
}

#[async_trait::async_trait]
impl DiskAPI for RecordingDisk {
    async fn write_all(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError> {
        self.log.record(rel_path, data.len());
        self.inner.write_all(rel_path, data).await
    }

    async fn append(&self, rel_path: &str, data: &[u8]) -> Result<(), DiskError> {
        self.log.record(rel_path, data.len());
        self.inner.append(rel_path, data).await
    }

    async fn read_exact_at(
        &self,
        rel_path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, DiskError> {
        self.inner.read_exact_at(rel_path, offset, len).await
    }
    async fn rename(&self, from_rel: &str, to_rel: &str) -> Result<(), DiskError> {
        self.inner.rename(from_rel, to_rel).await
    }
    async fn remove_dir_all(&self, rel_path: &str) -> Result<(), DiskError> {
        self.inner.remove_dir_all(rel_path).await
    }
    async fn list_dir(&self, rel_path: &str) -> Result<Vec<String>, DiskError> {
        self.inner.list_dir(rel_path).await
    }
    async fn stat(&self, rel_path: &str) -> Result<Option<FileStat>, DiskError> {
        self.inner.stat(rel_path).await
    }
    async fn sync_file_and_parent(&self, rel_path: &str) -> Result<(), DiskError> {
        self.inner.sync_file_and_parent(rel_path).await
    }
    fn disk_id(&self) -> &DiskId {
        self.inner.disk_id()
    }
    fn is_local(&self) -> bool {
        self.inner.is_local()
    }
}

/// [`set_with_recording_disks_modes`] 的旧模式特化。
pub async fn set_with_recording_disks(total: u8, parity: u8) -> (TestSet, WriteLog) {
    set_with_recording_disks_modes(total, parity, IoModes::default()).await
}

/// [`set_with_modes`] 的录制版：每块盘的最内层是 `RecordingDisk`，
/// 所有盘的写入长度汇总到同一个 `WriteLog`，并指定读写路径模式。返回 `(set, log)`。
///
/// 包装顺序是 `LocalDisk` → `RecordingDisk` → `FaultyDisk`。**这个顺序不能反**：
/// `RecordingDisk` 记的是「最终落到盘上的那些写入」，若把它套在 `FaultyDisk`
/// 外面，`Fault::DropWrites` 之类「假装成功」的故障就不会出现在日志里。
pub async fn set_with_recording_disks_modes(
    total: u8,
    parity: u8,
    modes: IoModes,
) -> (TestSet, WriteLog) {
    let dir = tempfile::TempDir::new().expect("create tempdir");
    let log = WriteLog::default();

    let mut disks: Vec<Option<Arc<dyn DiskAPI>>> = Vec::with_capacity(total as usize);
    let mut faulties: Vec<Arc<FaultyDisk>> = Vec::with_capacity(total as usize);
    for i in 0..total as usize {
        let root = dir.path().join(format!("disk{i}"));
        let inner: Arc<dyn DiskAPI> =
            Arc::new(LocalDisk::open(&root, DiskId::new_v4()).expect("open local disk"));
        // `FaultyDisk::wrap` 取 `impl DiskAPI + 'static`（按值），所以先把
        // `RecordingDisk` 建出来、按值传进去，之后再 Arc/unsize。
        let recorded = RecordingDisk::new(inner, log.clone());
        let faulty = Arc::new(FaultyDisk::wrap(recorded));
        faulties.push(Arc::clone(&faulty));
        let erased: Arc<dyn DiskAPI> = faulty;
        disks.push(Some(erased));
    }

    let set = ErasureSet::with_modes(disks, parity, modes).expect("valid erasure set geometry");
    (
        TestSet {
            set,
            faulties,
            _dir: dir,
        },
        log,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstore_disk::FaultKind;

    /// 每块盘必须有**独立**的盘根：逐盘写不同内容再逐盘读回，若共享根则只剩最后一个。
    #[tokio::test]
    async fn each_disk_has_its_own_root() {
        let set = set_with_disks(3, 1).await;
        for (i, slot) in set.disks().iter().enumerate() {
            let d = slot.as_ref().expect("fixture disks are all online");
            d.write_all("probe", format!("disk{i}").as_bytes())
                .await
                .unwrap();
        }
        for (i, slot) in set.disks().iter().enumerate() {
            let d = slot.as_ref().unwrap();
            let got = d.read_exact_at("probe", 0, 5).await.unwrap();
            assert_eq!(
                got,
                format!("disk{i}").as_bytes(),
                "disk {i} does not have an independent root"
            );
        }
    }

    /// `inject_fault_on` 从注入点起算：先跑掉若干调用后，`FailAfter { calls: 1 }`
    /// 仍应恰好放行 1 次。不调 `reset_call_count` 的话第一次就会失败。
    #[tokio::test]
    async fn inject_fault_counts_from_injection_point() {
        let set = set_with_disks(2, 0).await;
        // 制造累计调用，使「构造以来」的计数远大于 1。
        set.write_probe("burn", b"x").await;
        set.write_probe("burn", b"x").await;

        set.inject_fault_on(
            0,
            Fault::FailAfter {
                calls: 1,
                kind: FaultKind::Transient,
            },
        );
        let d = set.disks()[0].as_ref().unwrap();
        d.write_all("a", b"x").await.expect("first call is allowed");
        assert!(
            d.write_all("b", b"x").await.is_err(),
            "second call must fail"
        );
    }

    /// 撤销故障后回到正常行为。
    #[tokio::test]
    async fn clear_fault_restores_healthy_disk() {
        let set = set_with_disks(2, 0).await;
        set.inject_fault_on(0, Fault::Offline);
        let d = set.disks()[0].as_ref().unwrap();
        assert!(d.write_all("a", b"x").await.is_err());

        set.clear_fault_on(0);
        d.write_all("a", b"x")
            .await
            .expect("healthy again after clear");
    }

    /// `set_with_disks` 必须恰好是「旧模式」。谁要是让默认夹具悄悄带上新模式，
    /// 整个 M4 的单测都会在测另一条路径，而所有断言照样是绿的。
    #[tokio::test]
    async fn set_with_disks_is_old_mode() {
        let set = set_with_disks(2, 0).await;
        assert_eq!(set.modes(), IoModes::default());

        let set = set_with_modes(2, 0, IoModes::ALL).await;
        assert_eq!(set.modes(), IoModes::ALL);
        assert_eq!(set.total(), 2, "模式不该影响几何");
    }
}
