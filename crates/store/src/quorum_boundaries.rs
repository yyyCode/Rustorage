//! DESIGN §19.2 的边界矩阵。与其余测试的分工：这里**只回答**「掉几块盘、
//! 哪种操作、该成功还是该失败」，具体数据是否正确由 4.7/4.8 各自的用例负责。
#![cfg(test)]

use rstore_disk::faulty::Fault;

use crate::error::StoreError;
use crate::put::PutArgs;
use crate::testutil::{body, set_with_disks, TestSet};
// 本文件不需要 `ByteRange`：矩阵只跑整对象读。别顺手 import 它——
// `cargo clippy --all-targets -- -D warnings` 会把未使用的导入判成失败。

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    Read,
    Write,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    Ok,
    ReadQuorum,
    WriteQuorum,
}

/// 对象大小固定 2 MiB：跨 2 个 block，既走真编码路径又不至于让 7 个用例跑太久。
const BODY: usize = 2 * 1024 * 1024;

fn payload(seed: u8) -> Vec<u8> {
    let mut v = vec![0u8; BODY];
    for (i, b) in v.iter_mut().enumerate() {
        *b = (i as u8) ^ seed;
    }
    v
}

/// 建 set → 先健康地 PUT 一份 → 注入 `offline` 块掉线 → 执行 `op` → 断言结果类别。
///
/// **Read / Delete 用例必须先有一份成功写入的对象**：否则「读不存在的对象」
/// 会以 `NotFound` 收场，而 `Expect::Ok` 的断言根本到不了——那是一种
/// 「看起来测了，其实测的是别的东西」的假绿。PUT 阶段掉线数必须是 0。
async fn run_case(offline: usize, op: Op, expect: Expect) {
    let set = set_with_disks(6, 2).await;
    let data = payload(7);

    if matches!(op, Op::Read | Op::Delete) {
        set.put_object(PutArgs {
            bucket: "b".into(),
            key: "k".into(),
            body: body(data.clone()),
            etag: None,
        })
        .await
        .expect("健康状态下 PUT 必须成功");
    }

    for i in 0..offline {
        set.inject_fault_on(i, Fault::Offline);
    }

    let r = match op {
        Op::Read => set.get_object("b", "k", None).await.map(|out| {
            // **`Ok` 必须是「内容正确」的 `Ok`。** 只断言 `is_ok()` 的话，
            // 一个返回全零缓冲区的实现也能过。
            assert_eq!(out.data, data, "offline={offline} 读回的内容不对");
        }),
        Op::Write => set
            .put_object(PutArgs {
                bucket: "b".into(),
                key: "w".into(),
                body: body(payload(9)),
                etag: None,
            })
            .await
            .map(|_| ()),
        Op::Delete => set.delete_object("b", "k").await,
    };

    match (expect, r) {
        (Expect::Ok, Ok(())) => {}
        (Expect::Ok, Err(e)) => panic!("offline={offline} {op:?}: 期望成功，got {e:?}"),
        // 这里**不能**再补一个 `(Expect::Ok, _) => unreachable!()`：
        // `Result` 只有 `Ok`/`Err` 两个变体，上面两条已经覆盖了 `Expect::Ok` 的全部，
        // 那条通配臂会被 `unreachable_patterns` 判成警告，而门禁是 `-D warnings`。
        (Expect::ReadQuorum, Err(StoreError::ReadQuorum { .. })) => {}
        (Expect::WriteQuorum, Err(StoreError::WriteQuorum { .. })) => {}
        (expect, Err(e)) => panic!("offline={offline} {op:?}: 期望 {expect:?}，got {e:?}"),
        (expect, Ok(())) => panic!("offline={offline} {op:?}: 期望 {expect:?}，却成功了"),
    }
}

/// 4+2 配置下的完整边界矩阵。每行是一个独立用例。
#[tokio::test]
async fn matrix_4_plus_2() {
    let cases = [
        // (掉线盘数, 操作, 期望)
        (0, Op::Read, Expect::Ok),
        (2, Op::Read, Expect::Ok), // read_quorum = 6 - 2 = 4，刚好还能读
        (3, Op::Read, Expect::ReadQuorum), // 低于 read_quorum
        (0, Op::Write, Expect::Ok),
        (2, Op::Write, Expect::Ok), // write_quorum = data = 4
        (3, Op::Write, Expect::WriteQuorum),
        (1, Op::Delete, Expect::Ok), // delete_quorum = 6/2 + 1 = 4，5 可用 ≥ 4
        (3, Op::Delete, Expect::WriteQuorum), // 3 可用 < 4
        (4, Op::Delete, Expect::WriteQuorum), // 2 可用 < 4
    ];
    for (offline, op, expect) in cases {
        run_case(offline, op, expect).await;
    }
}

/// 少数盘静默损坏：读必须成功，而且结果必须正确。
/// 「少数」的界是 `parity`——损坏盘数 ≤ parity 时纠删码能把数据重建出来。
#[tokio::test]
async fn bitrot_on_minority_still_reads_correctly() {
    let set = set_with_disks(6, 2).await;
    let data = payload(3);
    let out = set
        .put_object(PutArgs {
            bucket: "b".into(),
            key: "k".into(),
            body: body(data.clone()),
            etag: None,
        })
        .await
        .unwrap();

    for i in 0..2 {
        corrupt_shard(&set, i, &out.data_dir).await;
    }

    let got = set.get_object("b", "k", None).await.unwrap();
    assert_eq!(got.data, data, "损坏盘数 == parity 时必须靠校验分片重建");
}

/// **DESIGN §2 的 P1**：宁可报错，绝不返回错数据。
/// 损坏盘数 > parity 时，能用于解码的份数已经不够，此时任何「尽力而为」的重建
/// 都会产生一段**看起来正常但没有校验能发现**的字节。
#[tokio::test]
async fn bitrot_on_majority_exposes_corruption_not_wrong_data() {
    let set = set_with_disks(6, 2).await;
    let data = payload(5);
    let out = set
        .put_object(PutArgs {
            bucket: "b".into(),
            key: "k".into(),
            body: body(data.clone()),
            etag: None,
        })
        .await
        .unwrap();

    for i in 0..3 {
        corrupt_shard(&set, i, &out.data_dir).await;
    }

    match set.get_object("b", "k", None).await {
        Err(StoreError::ReadQuorum { .. }) => {}
        // 宁可在这里因为「实现了某种超出 MVP 范围的恢复」而红，也不要放过
        // 一个返回了错误字节却报成功的实现。
        Ok(out) => panic!(
            "损坏超过 parity 时返回了 {} 字节数据，未报错",
            out.data.len()
        ),
        Err(e) => panic!("期望 ReadQuorum，got {e:?}"),
    }
}

/// 直接对盘上的分片文件做读-改-写，制造**真·静默损坏**：文件长度不变、
/// 没有任何 API 报错，只有 bitrot 摘要能发现它。
///
/// 不用 `Fault::CorruptBytes` 的原因：那是在**写入时**变换 payload，
/// 而这里要损坏的是**已经提交在地上**的数据——两者是完全不同的故障场景
/// （前者模拟坏盘，后者模拟 bit rot / 静默错写），后者才是 heal 的触发条件。
async fn corrupt_shard(set: &TestSet, disk_idx: usize, data_dir: &uuid::Uuid) {
    use rstore_checksum::HASH_LEN;

    let d = set.disks()[disk_idx].as_ref().expect("该盘应当在线");
    let rel = format!("b/k/{data_dir}/part.1");
    let len = d.stat(&rel).await.unwrap().expect("分片必须存在").size as usize;
    let mut bytes = d.read_exact_at(&rel, 0, len).await.unwrap();
    bytes[HASH_LEN] ^= 0xFF;
    d.write_all(&rel, &bytes).await.unwrap();
}
