//! M4 测试共用的夹具。`#[cfg(test)]` 门控（见 `lib.rs`），不进发布产物。

use std::sync::Arc;

use rstore_common::disk_id::DiskId;
use rstore_disk::{DiskAPI, Fault, FaultyDisk, LocalDisk};

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

/// 建 `total` 块盘、`parity` 为 `parity` 的 set（`data = total - parity`）。
/// **`set_with_disks(6, 2)` 读作「6 块盘、parity=2、data=4」**——整个 M4 的测试都用这个约定。
///
/// 盘 `i` 的根是独立子目录 `{tmp}/disk{i}`：同根的话 6 块「盘」其实是同一个目录，
/// 「6 副本、掉 2 块还能读」这些性质会退化成同义反复，测试全绿却什么都没测到。
pub async fn set_with_disks(total: u8, parity: u8) -> TestSet {
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

    let set = ErasureSet::new(disks, parity).expect("valid erasure set geometry");
    TestSet {
        set,
        faulties,
        _dir: dir,
    }
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
}
