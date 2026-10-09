//! 对象元数据的数据模型：header / body / 懒解析体积。
//!
//! 对应 DESIGN §8.2。核心不变量：`None` 与「零值」在线格式上必须可区分——
//! 具体映射只在 [`encode_header`] / [`decode_header`] 两处发生。

use std::collections::BTreeMap;

use rstore_common::error::{CorruptKind, DiskError};
use uuid::Uuid;

/// 版本类型。普通对象，或删除标记（删除标记只有 header 有意义）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum VersionType {
    #[default]
    Object,
    DeleteMarker,
}

/// header 标志位：`u8` newtype + 具名位。
///
/// 不引入 `bitflags`（workspace 没有该依赖，位运算本身也不需要）。
/// **位值是线格式的一部分，一旦落盘不可改**（DESIGN §8.3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct Flags(u8);

impl Flags {
    /// 位 0：该版本的数据已被释放——分片已删，仅保留元数据。
    pub const FREE_VERSION: Flags = Flags(1 << 0);
    /// 位 1：该版本使用了 `data_dir`（否则数据目录由部署/桶级推导）。
    pub const USES_DATA_DIR: Flags = Flags(1 << 1);
    /// 位 2：提示该版本可能携带内联数据；读路径以 body 里的标记为准（DESIGN §8.4）。
    pub const INLINE_DATA: Flags = Flags(1 << 2);

    /// 无任何位置位。
    pub const fn empty() -> Self {
        Flags(0)
    }

    /// `self` 是否包含 `other` 的全部位。
    pub const fn contains(&self, other: Flags) -> bool {
        self.0 & other.0 == other.0
    }

    /// 置上 `other` 的位。
    pub fn insert(&mut self, other: Flags) {
        self.0 |= other.0;
    }
}

/// 校验和算法。MVP 只有 CRC32C 一种；保留枚举是为了线格式上以后加算法时
/// 不用改字段形状（DESIGN §8.3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ChecksumAlgo {
    Crc32c,
}

/// 存储类别。MVP 只有 STANDARD。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum StorageClass {
    Standard,
}

/// 已解析的版本 header。LIST / HEAD 只看它就够，无需解析 body。
///
/// 序列化经 `HeaderWire` 桥接（`from` / `into`），**不是**直接 derive——否则
/// `Option<u64>` 的 `None` 会编成 msgpack nil 而非约定的 0，uuid 也会走
/// `is_human_readable()` 分流。容器端因此可以对一个 `&mut Read` 直接
/// `rmp_serde::from_read`，并用 `position()` 拿记录边界。
#[derive(Debug, Clone, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(from = "HeaderWire", into = "HeaderWire")]
pub struct FileVersionHeader {
    /// 版本 id。**nil UUID 与 `None` 语义不同**，编码时必须保留区别。
    pub version_id: Option<Uuid>,
    pub ty: VersionType,
    pub size: u64,
    /// unix 纳秒。`None` 在线格式上编码为 `0`，解码时 `0` 还原为 `None`。
    ///
    /// 这是对 DESIGN §8.2 原文 `mod_time: u64` 的有意收紧：让「未设置」在类型里
    /// 显式可表达，而不是靠 0 这个魔数。代价是 epoch（真实的 0）被折叠为 `None`，
    /// 可接受——文件不可能真有 1970 年的 mtime。
    pub mod_time: Option<u64>,
    /// data shards。quorum 决策直接可读，无需解析 body。
    pub ec_m: u8,
    /// total shards（data + parity）。
    pub ec_n: u8,
    pub flags: Flags,
    /// 数据目录 id，走 16 字节原始 UUID。
    pub data_dir: Option<Uuid>,
}

impl FileVersionHeader {
    /// 纠删编码的数据分片数。
    pub fn data_shards(&self) -> usize {
        self.ec_m as usize
    }

    /// 纠删编码的分片总数（data + parity）。
    pub fn total_shards(&self) -> usize {
        self.ec_n as usize
    }
}

/// 私有 wire 表示。`FileVersionHeader` 经它桥接做 serde。
///
/// 内存侧有两个「坑」，都由这里的字段类型抹平：`Option<u64>` 的 `None` 若直接
/// derive 会编成 msgpack nil 而非约定的 0；uuid crate 的 serde 走
/// `is_human_readable()` 分流，可能编成带连字符的字符串。落到定长 `[u8; 16]` /
/// 整数上后，线格式就确定了。
#[derive(serde::Serialize, serde::Deserialize)]
struct HeaderWire {
    /// 空 = `None`；否则原始 16 字节（定长 array，不含连字符）。
    version_id: Option<[u8; 16]>,
    ty: VersionType,
    size: u64,
    /// 内存侧是 `Option`，这里恒是整数（`None` 写 0）。
    mod_time: u64,
    ec_m: u8,
    ec_n: u8,
    flags: Flags,
    data_dir: Option<[u8; 16]>,
}

impl From<FileVersionHeader> for HeaderWire {
    fn from(h: FileVersionHeader) -> Self {
        HeaderWire {
            version_id: h.version_id.map(|u| *u.as_bytes()),
            ty: h.ty,
            size: h.size,
            mod_time: h.mod_time.unwrap_or(0),
            ec_m: h.ec_m,
            ec_n: h.ec_n,
            flags: h.flags,
            data_dir: h.data_dir.map(|u| *u.as_bytes()),
        }
    }
}

impl From<HeaderWire> for FileVersionHeader {
    fn from(wire: HeaderWire) -> Self {
        FileVersionHeader {
            version_id: wire.version_id.map(Uuid::from_bytes),
            ty: wire.ty,
            size: wire.size,
            // 线格式的 0 还原为「未设置」；epoch 被折叠为 None（见字段文档）。
            mod_time: if wire.mod_time == 0 {
                None
            } else {
                Some(wire.mod_time)
            },
            ec_m: wire.ec_m,
            ec_n: wire.ec_n,
            flags: wire.flags,
            data_dir: wire.data_dir.map(Uuid::from_bytes),
        }
    }
}

/// header 的 msgpack 编码。`Option` ↔ 线格式的映射**只发生在 `HeaderWire` 的
/// `From` 实现里**；这里只是薄包装。
pub fn encode_header(h: &FileVersionHeader) -> Result<Vec<u8>, DiskError> {
    rmp_serde::to_vec(h).map_err(|_| DiskError::Corrupt(CorruptKind::MalformedHeader))
}

/// header 的 msgpack 解码。缺字段 / 长度不符 / 畸形输入一律映射为
/// `MalformedHeader`（msgpack 解码失败统一走这条路）。
pub fn decode_header(bytes: &[u8]) -> Result<FileVersionHeader, DiskError> {
    rmp_serde::from_slice(bytes).map_err(|_| DiskError::Corrupt(CorruptKind::MalformedHeader))
}

/// 内联数据帧：version-key -> 原始字节（DESIGN §8.4）。
///
/// **必须是 newtype 而不是 `type` 别名**——Task 2.3 要在它上面挂 `encode`/`decode`，
/// 而类型别名不能带固有方法。`#[serde(transparent)]` 让它在 msgpack 上就是那个 map。
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(transparent)]
pub struct InlineData(BTreeMap<String, Vec<u8>>);

impl InlineData {
    /// 空帧。
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// 插入一条，返回被覆盖的旧值（若有）。
    pub fn insert(&mut self, k: impl Into<String>, v: Vec<u8>) -> Option<Vec<u8>> {
        self.0.insert(k.into(), v)
    }

    /// 按键取原始字节。
    pub fn get(&self, k: &str) -> Option<&[u8]> {
        self.0.get(k).map(Vec::as_slice)
    }
    // encode / decode 在 Task 2.3 的 inline.rs 里实现（同一 crate 内可跨模块 impl）
}

/// 懒解析的 body 原始字节。
pub type OpaqueBody = Vec<u8>;

/// 浅版本：header 已解析，body 保持不透明（懒解析）。
///
/// LIST / HEAD 只看 `header` 就能判定「最新版本是不是删除标记」「对象多大」。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShallowVersion {
    pub header: FileVersionHeader,
    pub body: OpaqueBody,
}

/// 一个对象的全部版本 + 内联数据 + 容器格式**主**版本（major）。
///
/// 注意是 major 而非 minor：容器 `major` 只在不兼容变更时递增，与魔数 `RSM1` 里的
/// `1` 同源；`minor` 承载兼容性的字段增删（DESIGN §8.3），不在这里暴露。
/// `container::encode` 会拒绝 `meta_ver != 1`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectMeta {
    pub versions: Vec<ShallowVersion>,
    pub inline: InlineData,
    pub meta_ver: u8,
}

/// body 解析后的内容（DESIGN §8.2）。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectBody {
    /// 数据目录/对象 id。此字段走 `Uuid` 自身的 serde（**不经 `HeaderWire` 桥接**），
    /// 因此线上是 bin8（`0xc4 0x10` + 16 字节），而 header 里的 UUID 是 array16——
    /// 同一格式里两种 UUID 表示是刻意接受的差异，不是笔误（见
    /// `uuid_encodes_as_raw_bytes_not_a_string`）。
    pub id: Option<Uuid>,
    pub parts: Vec<PartInfo>,
    pub ec_dist: Vec<u8>,
    pub checksum_algo: ChecksumAlgo,
    pub storage_class: StorageClass,
    pub meta_user: BTreeMap<String, String>,
    pub meta_sys: BTreeMap<String, Vec<u8>>,
}

/// 单个 part。`parts` 中 `number` / `size` 的并行数组一致性由**解析方**校验
/// （DESIGN §8.2）；本模块只定义结构。
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PartInfo {
    pub number: u16,
    pub size: u64,
    pub actual_size: u64,
    pub etag: String,
    /// 压缩时使用；MVP 恒为 `None`。
    pub index: Option<Vec<u8>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nil_version_id_differs_from_absent() {
        let with_nil = FileVersionHeader {
            version_id: Some(Uuid::nil()),
            ..Default::default()
        };
        let without = FileVersionHeader {
            version_id: None,
            ..Default::default()
        };
        assert_ne!(with_nil.version_id, without.version_id);
        assert!(with_nil.version_id.is_some());

        // 内存里不同还不够——线格式上必须也不同，否则落盘后区分不出来。
        let a = encode_header(&with_nil).unwrap();
        let b = encode_header(&without).unwrap();
        assert_ne!(a, b, "nil UUID 与 None 编码成了同样的字节");
        // uuid 必须走原始 16 字节，不是带连字符的字符串——
        // 线格式一旦落盘不可改（DESIGN §8.3），在这里钉住。
        assert!(!a.contains(&b'-'), "uuid 被编码成了人类可读字符串: {a:?}");
    }

    #[test]
    fn epoch_decodes_to_none_mod_time() {
        let h = FileVersionHeader {
            mod_time: Some(0),
            ..Default::default()
        };
        let enc = encode_header(&h).unwrap();
        let dec = decode_header(&enc).unwrap();
        assert_eq!(dec.mod_time, None);
    }

    #[test]
    fn caps_geometry_is_readable_from_header() {
        let h = FileVersionHeader {
            ec_m: 4,
            ec_n: 6,
            ..Default::default()
        };
        assert_eq!(h.data_shards(), 4);
        assert_eq!(h.total_shards(), 6);
    }

    #[test]
    fn two_headers_roundtrip_back_to_back() {
        // 单条 roundtrip 无法暴露记录边界问题：把两个**不同**的 header 拼在一起，
        // 依次读回必须各自相等，否则改动会悄悄破坏边界。
        let a = FileVersionHeader {
            version_id: Some(Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888)),
            ty: VersionType::Object,
            size: 1234,
            mod_time: Some(1_700_000_000_000_000_000),
            ec_m: 4,
            ec_n: 6,
            ..Default::default()
        };
        let b = FileVersionHeader {
            version_id: None,
            ty: VersionType::DeleteMarker,
            mod_time: None,
            ec_m: 4,
            ec_n: 6,
            ..Default::default()
        };
        let mut buf = encode_header(&a).unwrap();
        buf.extend_from_slice(&encode_header(&b).unwrap());
        let (head, tail) = buf.split_at(encode_header(&a).unwrap().len());
        assert_eq!(decode_header(head).unwrap(), a);
        assert_eq!(decode_header(tail).unwrap(), b);
    }

    #[test]
    fn msgpack_does_not_overread_on_a_cursor() {
        use std::io::{Cursor, Read};

        // 容器格式的地基：容器靠「读完 header 后 cursor 的位置」定位随后的
        // body_len u32 BE。若 rmp_serde 预读，位置就会跑过头——而单条 roundtrip
        // 测试发现不了，所以必须在这里钉住。
        let h = FileVersionHeader {
            version_id: Some(Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888)),
            mod_time: Some(1_700_000_000_000_000_000),
            ec_m: 4,
            ec_n: 6,
            ..Default::default()
        };
        let enc = encode_header(&h).unwrap();

        // 在一个 cursor 里连续放两条记录 + 一个哨兵字节。
        let mut buf = enc.clone();
        buf.extend_from_slice(&enc);
        buf.push(0xAB);
        let mut cursor = Cursor::new(buf.as_slice());

        // 读第一条，位置必须正好停在第一条的末尾。
        let a: FileVersionHeader = rmp_serde::from_read(&mut cursor).expect("first header");
        assert_eq!(a, h);
        assert_eq!(
            cursor.position() as usize,
            enc.len(),
            "msgpack 读了 {} 字节，header 只有 {} 字节——存在预读，容器格式不成立",
            cursor.position(),
            enc.len()
        );

        // 第二条也必须能读出来。
        let b: FileVersionHeader = rmp_serde::from_read(&mut cursor).expect("second header");
        assert_eq!(b, h);
        assert_eq!(cursor.position() as usize, enc.len() * 2);

        // 且哨兵字节原封不动地留在后面。
        let mut rest = Vec::new();
        cursor.read_to_end(&mut rest).unwrap();
        assert_eq!(rest, vec![0xAB]);
    }

    #[test]
    fn uuid_encodes_as_raw_bytes_not_a_string() {
        // ObjectBody.id / PartInfo 走 Uuid 的 serde，不经 HeaderWire 桥接，
        // 因此它的线格式取决于 rmp-serde 的 is_human_readable()。
        // 探针只能证明「今天如此」；钉成测试才能挡住将来 rmp-serde 升级
        // 把它翻成人类可读字符串（36 字节带连字符）——那会让旧 meta.xl 读不出来。
        let body = ObjectBody {
            id: Some(Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888)),
            parts: Vec::new(),
            ec_dist: Vec::new(),
            checksum_algo: ChecksumAlgo::Crc32c,
            storage_class: StorageClass::Standard,
            meta_user: BTreeMap::new(),
            meta_sys: BTreeMap::new(),
        };
        let enc = rmp_serde::to_vec(&body).unwrap();

        // UUID 的 32 个十六进制字符一个都不许出现在线格式里。
        assert!(
            !enc.contains(&b'-'),
            "Uuid 被编成了带连字符的字符串: {enc:?}"
        );
        assert_ne!(
            enc,
            rmp_serde::to_vec(&ObjectBody {
                id: None,
                ..body.clone()
            })
            .unwrap()
        );

        // 且必须能读回——钉住的是编码形状，不是往返本身。
        let back: ObjectBody = rmp_serde::from_slice(&enc).unwrap();
        assert_eq!(back, body);

        // 钉死 bin8 前缀 `0xc4 0x10`（msgpack bin8，长度 16）+ 原始 16 字节。
        let mut expected = vec![0xc4, 0x10];
        let uuid = Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888);
        expected.extend_from_slice(uuid.as_bytes());
        let pos = enc
            .windows(expected.len())
            .position(|w| w == expected.as_slice())
            .expect("未找到 bin8 编码的 16 字节 UUID");
        assert_eq!(&enc[pos..pos + expected.len()], expected.as_slice());
    }
}
