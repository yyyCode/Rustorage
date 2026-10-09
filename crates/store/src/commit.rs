//! 提交协议（Task 4.4）：把 staging 目录 rename 成 final 目录，达到 quorum 才算成功。
//!
//! 硬承诺（DESIGN §12.2）：**绝不在低于 quorum 时报告成功**。回滚是 best-effort。

use futures::future::join_all;
use rstore_common::error::TransientKind;
use rstore_disk::DiskError;

use crate::error::StoreError;
use crate::set::ErasureSet;

/// 提交结果。达到 quorum 时返回。
///
/// 未派生 `Clone`：`DiskError` 本身不是 `Clone`（`crates/common/src/error.rs`），
/// 而 `failures` 内含它的值，故 `CommitOutcome` 也随之不能 `Clone`。
/// 规格里写了 `Clone`，那是与本仓库实际的 `DiskError` 定义冲突之处，此处按实际代码收敛。
#[derive(Debug, PartialEq, Eq)]
pub struct CommitOutcome {
    /// 成功 rename 的盘数（= `renamed.len()`）。
    pub achieved: u8,
    /// 成功的盘下标。
    pub renamed: Vec<usize>,
    /// 失败的盘下标与原因。**不要丢**——上层要据此把这些盘标记为落后，
    /// 交给反熵 / heal 补数据。丢掉它们等于永远不知道谁落后了。
    pub failures: Vec<(usize, DiskError)>,
}

/// 把 staging 目录 rename 到 final 目录，达到 `write_quorum` 才算成功。
///
/// 硬承诺（DESIGN §12.2）：**绝不在低于 quorum 时报告成功**。
/// 回滚是 best-effort：失败时的残留由对账流程清理，本函数不保证不留字节。
pub async fn commit(
    set: &ErasureSet,
    staging_rel: &str,
    final_rel: &str,
    write_quorum: u8,
) -> Result<CommitOutcome, StoreError> {
    let mut renamed: Vec<usize> = Vec::new();
    let mut failures: Vec<(usize, DiskError)> = Vec::new();

    // 并行向所有可用盘发起 rename。先收集全部结果，再统计成功数——不要写成
    // 「遇到第一个失败就提前返回」，那会漏掉其余盘的成败、破坏 quorum 判定。
    let renames = set.disks().iter().enumerate().map(|(i, slot)| async move {
        match slot {
            // 盘离线是「暂时够不着」，不是「确定性缺失」——归 Transient，
            // 免得被 heal 当成损坏统计进去（DESIGN §17）。
            None => (i, Err(DiskError::Transient(TransientKind::Io))),
            Some(d) => (i, d.rename(staging_rel, final_rel).await),
        }
    });
    for (i, res) in join_all(renames).await {
        match res {
            Ok(()) => renamed.push(i),
            Err(e) => failures.push((i, e)),
        }
    }

    let achieved = renamed.len() as u8;
    if achieved >= write_quorum {
        Ok(CommitOutcome {
            achieved,
            renamed,
            failures,
        })
    } else {
        // 尽力回滚：只清理我们自己 rename 过去的那些盘。删除失败只忽略——
        // 残留交给对账处理，回滚本身不上抛，以免掩盖真正的 quorum 失败原因。
        for i in &renamed {
            let _ = set.disks()[*i]
                .as_ref()
                .unwrap()
                .remove_dir_all(final_rel)
                .await;
        }
        Err(StoreError::WriteQuorum {
            achieved,
            required: write_quorum,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::StoreError;
    use crate::testutil::set_with_disks;
    use rstore_disk::faulty::{Fault, FaultKind};
    use rstore_disk::DiskError;

    /// 每块盘都失败时用这个：`FailAfter { calls: 0 }` 表示「一次都不成功，第 1 次起就失败」。
    fn always_fail() -> Fault {
        Fault::FailAfter {
            calls: 0,
            kind: FaultKind::Transient,
        }
    }

    #[tokio::test]
    async fn commits_when_quorum_reached() {
        let set = set_with_disks(6, 2).await;
        // **必须先造出 staging 目录**：`commit` 做的是 rename，源路径不存在时
        // rename 会以 `NotFound` 失败。不写这一步的话 achieved 恒为 0，
        // 所有测试都会以一种「看起来在测、其实什么都没测」的方式失败或通过。
        set.write_probe("b/o/tx1/meta.xl", b"probe").await;

        let r = commit(&set, "b/o/tx1", "b/o/0000", 4).await;
        let outcome = r.expect("6 块盘全健康，必须达到 quorum=4");
        assert_eq!(outcome.achieved, 6);
        assert_eq!(outcome.renamed.len(), 6);

        // 舞台目录确实被搬走了，而不是复制了一份。
        let d = set.disks()[0].as_ref().unwrap();
        assert!(matches!(
            d.stat("b/o/tx1").await,
            Ok(None) | Err(DiskError::NotFound)
        ));
        assert!(matches!(d.stat("b/o/0000").await, Ok(Some(_))));
    }

    #[tokio::test]
    async fn fails_and_reports_when_below_quorum() {
        // 前 3 块盘 rename 必失败，只剩 3 块能成功；write_quorum = 4。
        let set = set_with_disks(6, 2).await;
        set.write_probe("b/o/tx1/meta.xl", b"probe").await;
        for i in 0..3 {
            set.inject_fault_on(i, always_fail());
        }

        let r = commit(&set, "b/o/tx1", "b/o/0000", 4).await;
        assert!(
            matches!(
                r,
                Err(StoreError::WriteQuorum {
                    achieved: 3,
                    required: 4
                })
            ),
            "got {r:?}"
        );
    }

    /// 回滚必须真的动手：2 块盘 rename 成功后失败，这 2 个目录不能被留下。
    /// （若删除本身也失败，残留由对账处理——所以只断言"尽力而为"的可见结果。）
    #[tokio::test]
    async fn rollback_removes_already_renamed_dirs() {
        let set = set_with_disks(6, 2).await;
        set.write_probe("b/o/tx1/meta.xl", b"probe").await;
        // 0/1 正常 → rename 成功；其余全部立即失败
        for i in 2..6 {
            set.inject_fault_on(i, always_fail());
        }

        let r = commit(&set, "b/o/tx1", "b/o/0000", 4).await;
        assert!(
            matches!(r, Err(StoreError::WriteQuorum { achieved: 2, .. })),
            "got {r:?}"
        );

        for i in [0usize, 1] {
            let d = set.disks()[i].as_ref().expect("这两块盘应当存在");
            assert!(
                matches!(
                    d.stat("b/o/0000").await,
                    Ok(None) | Err(DiskError::NotFound)
                ),
                "盘 {i} 上残留了回滚不掉的目录"
            );
        }
    }

    /// **硬承诺（DESIGN §12.2）**：只要返回 Ok，成功盘数就不可能低于 write_quorum。
    /// 这是全项目最重要的一条不变量。
    /// 6 块盘、每块"成功 / 失败"两种状态 → 用位掩码穷举全部 64 种组合，不做抽样。
    ///
    /// 注意这条测试有两个容易写成「空洞通过」的地方，两个都要盯住：
    /// 一是忘了 `write_probe`，于是每块盘的 rename 都因源路径不存在而失败、
    /// `Ok` 分支一次都进不去；二是只断言 `Ok` 时的 `achieved`，
    /// 就没人发现「其实一次都没成功过」。所以下面同时统计 `ok_count`。
    #[tokio::test]
    async fn never_reports_success_below_quorum() {
        const QUORUM: u8 = 4;
        let mut ok_count = 0usize;

        for mask in 0u32..64 {
            let set = set_with_disks(6, 2).await;
            set.write_probe("b/o/tx1/meta.xl", b"probe").await;
            for i in 0..6 {
                if mask & (1 << i) != 0 {
                    set.inject_fault_on(i, always_fail());
                }
            }

            if let Ok(outcome) = commit(&set, "b/o/tx1", "b/o/0000", QUORUM).await {
                ok_count += 1;
                assert!(
                    outcome.achieved >= QUORUM,
                    "mask={mask:#07b}: 报了成功，但只达成 {} < {QUORUM}",
                    outcome.achieved
                );
            }
        }

        // mask=0（全健康）与 mask 中失败盘数 ≤ 2 的那些都必须成功。
        // 若这里变成 0，说明「Ok 分支」根本没被走到，上面的断言全是空转。
        assert!(
            ok_count > 0,
            "没有任何一轮达成 quorum，这条测试没有测到东西"
        );
    }
}
