// `faulty` 模块由 feature 门控，而集成测试是**独立编译**的 crate，
// 不带 `--features fault-injection` 时 `rstore_disk::faulty` 根本不存在。
// 没有这一行，`cargo test --workspace`（不带 feature）会**编译失败**——
// 门禁命令就会红，而红的原因跟被测代码毫无关系。
#![cfg(feature = "fault-injection")]

use rstore_common::disk_id::DiskId;
use rstore_disk::faulty::{Fault, FaultKind, FaultyDisk};
use rstore_disk::{DiskAPI, DiskError, LocalDisk};

#[tokio::test]
async fn can_drop_writes() {
    let tmp = tempfile::TempDir::new().unwrap();
    let inner = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
    let d = FaultyDisk::wrap(inner).with(Fault::DropWrites);
    d.write_all("f", b"x").await.unwrap(); // 对外报成功
    assert!(matches!(
        d.read_exact_at("f", 0, 1).await,
        Err(DiskError::NotFound)
    ));
}

#[tokio::test]
async fn can_corrupt_bytes_silently() {
    let tmp = tempfile::TempDir::new().unwrap();
    let inner = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
    let d = FaultyDisk::wrap(inner);
    d.write_all("f", b"hello").await.unwrap();
    d.set_fault(Fault::CorruptBytes { at: 0, mask: 0xFF });
    d.write_all("g", b"hello").await.unwrap();
    // 读回来内容与写入不同，且没有任何 API 报错 —— 模拟静默损坏
    let got = d.read_exact_at("g", 0, 5).await.unwrap();
    assert_ne!(got, b"hello");
}

#[tokio::test]
async fn can_fail_after_n_calls() {
    let tmp = tempfile::TempDir::new().unwrap();
    let inner = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
    let d = FaultyDisk::wrap(inner).with(Fault::FailAfter {
        calls: 2,
        kind: FaultKind::Transient,
    });
    d.write_all("a", b"1").await.unwrap();
    d.write_all("b", b"2").await.unwrap();
    assert!(matches!(
        d.write_all("c", b"3").await,
        Err(DiskError::Transient(_))
    ));
}

#[tokio::test]
async fn can_return_to_healthy_after_fault() {
    // 故障注入必须是可逆的：否则「故障排除后原数据还读得回来吗」这类断言写不出来。
    let tmp = tempfile::TempDir::new().unwrap();
    let inner = LocalDisk::open(tmp.path(), DiskId::new_v4()).unwrap();
    let d = FaultyDisk::wrap(inner).with(Fault::Offline);
    assert!(matches!(
        d.write_all("f", b"x").await,
        Err(DiskError::Transient(_))
    ));

    d.clear_fault();
    d.write_all("f", b"x").await.unwrap();
    assert_eq!(d.read_exact_at("f", 0, 1).await.unwrap(), b"x");
}
