//! `format.json` 模型与拓扑校验（对应 DESIGN §7）。
//!
//! `FormatV1` 写在每块盘根，记录部署 id、池/集拓扑与分布算法版本。启动时把所有盘的
//! 副本读上来做 quorum 协商，选出权威拓扑；不一致则拒绝启动。

use rstore_common::disk_id::DiskId;
use rstore_common::error::DiskError;
// `TransientKind` 只在测试里构造样本错误码，非测试构建下导入会成为未使用告警
// （`-D warnings` 会因此挂掉），故按 cfg 门控。
#[cfg(test)]
use rstore_common::error::TransientKind;

/// 本项目唯一认可的格式名（DESIGN §7）。
pub const FORMAT_ERASURE: &str = "erasure";

/// 本项目唯一认可的分片分布算法：§9.3 的 CRC32C 旋转，**不是** MinIO 的 `sipmod`。
/// 字段保留是为了将来更换算法时旧数据仍可读（DESIGN §7）。
pub const DISTRIBUTION_ALGO: &str = "crc32c-rot-v1";

/// 拓扑协商失败。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FormatError {
    #[error("unknown format: {0}")]
    UnknownFormat(String),
    #[error("inconsistent topology: {0}")]
    Inconsistent(String),
    #[error("no quorum among {total} disks (best identity got {best} votes)")]
    NoQuorum { total: usize, best: usize },
}

/// 盘根 `format.json` 的顶层结构（DESIGN §7）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FormatV1 {
    pub version: String, // "1"（DESIGN §7 用字符串，不是数字）
    pub format: String,  // "erasure"
    pub id: String,      // deployment uuid
    pub erasure: FormatErasureV1,
    pub disk_info: DiskInfo,
}

/// 纠删拓扑部分。盘与 set 的归属在格式化时一次性生成并持久化，运行时只查表。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FormatErasureV1 {
    pub version: String, // "1"
    pub this: DiskId,
    pub sets: Vec<Vec<DiskId>>,
    pub distribution_algo: String, // 本项目只认 "crc32c-rot-v1"
}

/// 本盘的容量信息，运维可读，但**不参与拓扑一致性**。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DiskInfo {
    pub total: u64,
    pub free: u64,
}

impl FormatV1 {
    /// 测试样本：至少一个 set、每 set ≥2 块盘，让 `sets[0][1]` 可寻址。
    ///
    /// `this` **只影响 `erasure.this`**，其余字段（`id`/`version`/`format`/
    /// `distribution_algo`/`sets`）一律取固定常量。若 `this` 渗进其他字段，
    /// 「两个 `sample` 的 identity 应相等/不等」那几条断言就失去意义了。
    pub fn sample(this: DiskId) -> Self {
        // 固定盘号：用重复字节构造确定性的 DiskId，与 `this` 无关。
        let disk = |n: u8| DiskId::from_bytes([n; 16]);
        Self {
            version: "1".into(),
            format: FORMAT_ERASURE.into(),
            id: "00000000-0000-0000-0000-0000000000ff".into(),
            erasure: FormatErasureV1 {
                version: "1".into(),
                this,
                // 2 个 set 各 4 块盘：既满足宽度 2..=16，也让「pop 掉一块导致
                // 各 set 宽度不一致」能被 validate 抓到。
                sets: vec![
                    vec![disk(1), disk(2), disk(3), disk(4)],
                    vec![disk(5), disk(6), disk(7), disk(8)],
                ],
                distribution_algo: DISTRIBUTION_ALGO.into(),
            },
            disk_info: DiskInfo { total: 0, free: 0 },
        }
    }

    /// 逻辑拓扑的精确字节表示，用于 quorum 投票时判定「两块盘的拓扑是否一致」。
    ///
    /// 覆盖**除 `this` 与 `disk_info` 之外**的一切：`this` 是「本盘」天然各不相同；
    /// `disk_info` 含 `free`，每块盘必然不同——把任一算进来，同一 pool 的盘永远
    /// 凑不出多数派，quorum 协商整体失效。
    ///
    /// 返回 `Vec<u8>` 而非哈希指纹：这是**精确比对**，没有碰撞带来的歧义，
    /// 且天然 `PartialEq + Debug`、便于排障。
    ///
    /// 编码**手写、不走 serde**：`serde_json::to_vec` 会引入一条现实中不可能失败、
    /// 只能 `expect` 的 panic 路径，而这里要的是一个不可失败的纯函数。
    /// 规则：字符串 = `u32 LE` 长度 + 字节；`sets` = `u32 LE` set 数，每组再
    /// `u32 LE` 盘数，随后逐盘 16 字节裸 id。**长度/计数前缀不是装饰**——
    /// 丢了它们，`[[d1,d2]]` 与 `[[d1],[d2]]` 会压平成同一串字节，
    /// 两种截然不同的纠删拓扑就能互相投票凑成多数派。
    pub fn shared_identity(&self) -> Vec<u8> {
        let mut out = Vec::new();
        push_str(&mut out, &self.version);
        push_str(&mut out, &self.format);
        push_str(&mut out, &self.id);
        push_str(&mut out, &self.erasure.version);
        push_str(&mut out, &self.erasure.distribution_algo);
        out.extend_from_slice(&(self.erasure.sets.len() as u32).to_le_bytes());
        for set in &self.erasure.sets {
            out.extend_from_slice(&(set.len() as u32).to_le_bytes());
            for id in set {
                out.extend_from_slice(id.as_bytes());
            }
        }
        out
    }

    /// 校验一份拓扑自身是否合法（与别的盘是否一致是另一回事，见 `select_authoritative`）。
    pub fn validate(&self) -> Result<(), FormatError> {
        if self.format != FORMAT_ERASURE {
            return Err(FormatError::UnknownFormat(self.format.clone()));
        }
        if self.erasure.distribution_algo != DISTRIBUTION_ALGO {
            return Err(FormatError::Inconsistent(format!(
                "unsupported distribution_algo: {}",
                self.erasure.distribution_algo
            )));
        }
        if self.erasure.sets.is_empty() {
            return Err(FormatError::Inconsistent("no erasure sets".into()));
        }
        // 所有 set 必须等宽且落在 2..=16：宽度即纠删几何，不等宽说明拓扑已损坏。
        let width = self.erasure.sets[0].len();
        if !(2..=16).contains(&width) {
            return Err(FormatError::Inconsistent(format!(
                "set width {width} out of range 2..=16"
            )));
        }
        for (i, set) in self.erasure.sets.iter().enumerate() {
            if set.len() != width {
                return Err(FormatError::Inconsistent(format!(
                    "set {i} has {} disks, expected {width}",
                    set.len()
                )));
            }
        }
        Ok(())
    }
}

/// 写 `u32 LE` 长度前缀 + 字节，保证相邻字段不会因拼接而歧义。
fn push_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

/// 多盘 `format.json` 的版本仲裁：按 `shared_identity()` 分组计票，
/// 取得多数派（`总数 / 2 + 1`）的那组为权威拓扑。未达 quorum 报错。
///
/// 选出赢家后会**再跑一次 `validate()`**：投票只保证各盘「彼此一致」，
/// 不保证一致的那份合法；不校验就可能把一个所有人认可但非法的拓扑扶正。
pub fn select_authoritative(formats: &[FormatV1]) -> Result<FormatV1, FormatError> {
    use std::collections::HashMap;

    let mut votes: HashMap<Vec<u8>, (usize, &FormatV1)> = HashMap::new();
    for f in formats {
        votes
            .entry(f.shared_identity())
            .and_modify(|(n, _)| *n += 1)
            .or_insert((1, f));
    }

    let total = formats.len();
    let majority = total / 2 + 1;
    let best = votes.values().map(|(n, _)| *n).max().unwrap_or(0);

    // 各组票数之和为 total，故至多有一组能超过 total/2 达到多数派。
    let winner = votes
        .values()
        .find(|(n, _)| *n >= majority)
        .map(|&(_, f)| f.clone())
        .ok_or(FormatError::NoQuorum { total, best })?;

    winner.validate()?;
    Ok(winner)
}

/// 全新初始化闸门：**仅当所有盘都返回 `NotFound`**（即读不到 `format.json`）时才为真。
///
/// 对应 DESIGN §7「网络不可达的盘绝不被当作新拓扑的证据」——只要有一块盘给出了
/// 任何别的结果（哪怕是瞬时的超时），就说明我们没看全，不能格式化。这比 quorum
/// 更严格，是刻意为之。空切片返回 false（没有盘就不能初始化）。
///
/// `DiskError` 是 `#[non_exhaustive]`，这里用 `matches!` 而非穷举 `match`，
/// 将来新增变体时此函数无需改动即可安全退化。
pub fn should_initialize(errs: &[DiskError]) -> bool {
    !errs.is_empty() && errs.iter().all(|e| matches!(e, DiskError::NotFound))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_identity_excludes_this_disk() {
        let a = FormatV1::sample(DiskId::new_v4());
        let mut b = a.clone();
        b.erasure.this = DiskId::new_v4();
        assert_eq!(a.shared_identity(), b.shared_identity());
    }

    #[test]
    fn shared_identity_includes_topology() {
        let a = FormatV1::sample(DiskId::new_v4());
        let mut b = a.clone();
        b.erasure.sets[0][1] = DiskId::new_v4();
        assert_ne!(a.shared_identity(), b.shared_identity());
    }

    #[test]
    fn shared_identity_excludes_disk_info() {
        // `disk_info.free` 每块盘必然不同。若把它算进 identity，同一 pool 的盘
        // 永远凑不出多数派，quorum 协商整体失效——这是设计文档里一处真实的错
        // （DESIGN §7 原文写「除 this 之外的全部字段」），必须由测试钉住。
        let a = FormatV1::sample(DiskId::new_v4());
        let mut b = a.clone();
        b.disk_info = DiskInfo {
            total: 999,
            free: 123,
        };
        assert_eq!(a.shared_identity(), b.shared_identity());
    }

    #[test]
    fn rejects_unknown_format() {
        let mut a = FormatV1::sample(DiskId::new_v4());
        a.format = "xl".into();
        assert!(a.validate().is_err());
    }

    #[test]
    fn rejects_inconsistent_set_sizes() {
        let mut a = FormatV1::sample(DiskId::new_v4());
        a.erasure.sets[0].pop();
        assert!(a.validate().is_err());
    }

    /// quorum 投票：多数派的 identity 胜出，少数派被忽略。
    /// 比较 identity 而不是内部字段，避免绑死 `sample` 的具体内容。
    #[test]
    fn quorum_picks_the_majority_identity() {
        let id = DiskId::new_v4();
        let a = FormatV1::sample(id);
        let mut same = a.clone();
        same.erasure.this = DiskId::new_v4(); // 只有 this 不同 → 同一 identity
        let mut other = FormatV1::sample(id);
        other.erasure.sets[0][1] = DiskId::new_v4(); // 拓扑不同 → 另一种 identity

        let chosen = select_authoritative(&[a.clone(), same, other]).unwrap();
        assert_eq!(chosen.shared_identity(), a.shared_identity());
    }

    /// 票数打平（无多数）必须报错，不能随便挑一个。
    #[test]
    fn no_quorum_is_an_error() {
        let id = DiskId::new_v4();
        let a = FormatV1::sample(id);
        let mut b = FormatV1::sample(id);
        b.erasure.sets[0][1] = DiskId::new_v4();
        assert!(select_authoritative(&[a, b]).is_err());
    }

    /// 初始化闸门：**只要有一块盘不是 NotFound 就不能当新拓扑**。
    /// 这条是 DESIGN §7「网络不可达的盘绝不被当作新拓扑的证据」的直接落地。
    #[test]
    fn init_only_when_every_disk_is_missing() {
        assert!(should_initialize(&[
            DiskError::NotFound,
            DiskError::NotFound
        ]));
        assert!(!should_initialize(&[
            DiskError::NotFound,
            DiskError::Transient(TransientKind::Timeout),
        ]));
        assert!(!should_initialize(&[]));
    }

    /// format.json 是运维会直接打开看的文件，字段名与 `DiskId` 的**字符串**形态
    /// 都是对外契约。`DiskId` 是 Uuid 的 newtype——万一将来有人把它换成
    /// `[u8; 16]`，JSON 里就会冒出 `[17,34,...]` 这样的数组，人读不了，
    /// 而只有这条测试会拦下来。
    #[test]
    fn format_json_shape_is_stable() {
        let v = serde_json::to_value(FormatV1::sample(DiskId::new_v4())).unwrap();
        assert_eq!(v["format"], "erasure");
        assert_eq!(v["erasure"]["distribution_algo"], "crc32c-rot-v1");
        assert!(v["erasure"]["this"].is_string(), "got {v:#?}");
        assert!(v["erasure"]["sets"][0][0].is_string(), "got {v:#?}");
    }

    /// 集合的**计数**必须进 identity。丢了计数的话，同样的盘按不同方式分组会编出
    /// 同一串字节——`[[d1,d2]]` 与 `[[d1],[d2]]` 是两种完全不同的纠删拓扑
    /// （前者 1 个 2 盘组，后者 2 个 1 盘组），却会互相投票凑成多数派。
    #[test]
    fn shared_identity_distinguishes_set_grouping() {
        let (d1, d2) = (DiskId::new_v4(), DiskId::new_v4());
        let mut a = FormatV1::sample(DiskId::new_v4());
        a.erasure.sets = vec![vec![d1, d2]];
        let mut b = FormatV1::sample(DiskId::new_v4());
        b.erasure.sets = vec![vec![d1], vec![d2]];
        assert_ne!(a.shared_identity(), b.shared_identity());
    }
}
