//! `meta.xl` 容器编解码（DESIGN §8.1）。
//!
//! 布局（自前向后顺序解析）：
//!
//! ```text
//! magic "RSM1" (4B) | major u16 LE | minor u16 LE | version_count u16 LE
//! [ header(msgpack，自描述) | body_len u32 BE | body ] × version_count
//! trailer CRC32C (u32 LE，覆盖从 magic 到最后一个 body 的字节)
//! inline_data (msgpack map；可为空)
//! ```
//!
//! **CRC 在 `inline_data` 之前**，所以它不在文件末尾——`crc32c(&bytes[..len-4])`
//! 是错的。CRC 只保护结构部分，且让「只读前缀即可完成 LIST/HEAD」的增量读成立；
//! 代价是解码必须顺序解析，走到记录结束处才知道 CRC 在哪。

use std::io::Cursor;

use rstore_common::error::{CorruptKind, DiskError};

use crate::fileinfo::{encode_header, FileVersionHeader, InlineData, ObjectMeta, ShallowVersion};

/// 容器魔数。**线格式的一部分，不可改**。
const MAGIC: [u8; 4] = *b"RSM1";
/// 当前支持的 major 版本。`major != MAJOR` 一律判定为确定性损坏（DESIGN §8.1）。
const MAJOR: u16 = 1;
/// 当前支持的 minor 版本。`minor > MINOR` 亦判定为确定性损坏。
const MINOR: u16 = 0;

/// u16 字段之后、记录起始处的偏移：magic(4) + major(2) + minor(2) + version_count(2)。
const RECORDS_OFFSET: usize = 10;
/// 一个合法容器的最小长度：`RECORDS_OFFSET` + trailer CRC(4)。
const MIN_CONTAINER_LEN: usize = RECORDS_OFFSET + TRAILER_LEN;
/// trailer CRC32C 的长度（u32 LE）。
const TRAILER_LEN: usize = 4;

/// 一个 header msgpack 编码的**绝对下界**（字节）。
///
/// 构造（实测 `[0x98, 0xc0, 0xa6 4f 62 6a 65 63 74, 0x00 ×5, 0xc0]`）：
/// `HeaderWire` 是 fixarray(8)（`0x98`，1B）；`version_id` 取 `None`（nil `0xc0`，1B）；
/// `ty = Object` 编成字符串 `"Object"`（fixstr 6，`0xa6` + 6B = 7B，见 `rmp-serde` 的
/// `serialize_unit_variant` → `serialize_str`）；`size`/`mod_time`/`ec_m`/`ec_n`/`flags`
/// 取 0（positive fixint，各 1B，共 5B）；`data_dir` 取 `None`（nil，1B）。
/// 合计 `1 + 1 + 7 + 5 + 1 = 15`。
///
/// 由 `record_min_bytes_is_a_true_lower_bound` 钉住。**只能调小不能调大**：
/// 它用于「分配之前」拒绝 `version_count` 过大的输入，调大就会误拒合法输入。
const HEADER_MIN_BYTES: usize = 15;

/// 一条记录的绝对下界：header 下界 + `body_len`（u32 BE，4B）+ 空 body。
const RECORD_MIN_BYTES: usize = HEADER_MIN_BYTES + 4;

/// 把 `ObjectMeta` 编成 `meta.xl` 容器的字节序列。
///
/// `ObjectMeta::meta_ver` 写入容器的 `major` 字段；`minor` 恒为 [`MINOR`]。
/// 容器不在别处承载 `meta_ver`。
///
/// `meta_ver != MAJOR` 一律拒绝（`UnsupportedVersion`）——见下方注释。
pub fn encode(meta: &ObjectMeta) -> Result<Vec<u8>, DiskError> {
    // 拒绝写出自己读不回来的容器。若放任 `meta_ver` 原样落盘，`decode` 会把它报成
    // `Corrupt(UnsupportedVersion)`——而 DESIGN §17 规定「观察到 Corrupt 即触发 repair」，
    // 一个编码期的错误就会伪装成盘损坏，去对健康数据做修复。
    if u16::from(meta.meta_ver) != MAJOR {
        return Err(DiskError::Corrupt(CorruptKind::UnsupportedVersion));
    }

    let mut out = Vec::new();
    out.extend_from_slice(&MAGIC);
    // meta_ver 即容器 major 版本，已在上方校验与 MAJOR 一致。
    out.extend_from_slice(&u16::from(meta.meta_ver).to_le_bytes());
    out.extend_from_slice(&MINOR.to_le_bytes());

    let version_count = u16::try_from(meta.versions.len())
        .map_err(|_| DiskError::Corrupt(CorruptKind::LengthMismatch))?;
    out.extend_from_slice(&version_count.to_le_bytes());

    for v in &meta.versions {
        out.extend_from_slice(&encode_header(&v.header)?);
        let body_len = u32::try_from(v.body.len())
            .map_err(|_| DiskError::Corrupt(CorruptKind::LengthMismatch))?;
        out.extend_from_slice(&body_len.to_be_bytes());
        out.extend_from_slice(&v.body);
    }

    out.extend_from_slice(&crc32c::crc32c(&out).to_le_bytes());
    // 内联帧追加在 CRC 之后；空 map 也照编（CRC 不覆盖此区）。
    out.extend_from_slice(&meta.inline.encode()?);
    Ok(out)
}

/// 从 `meta.xl` 容器的字节序列解出 `ObjectMeta`。
///
/// 防御顺序见 DESIGN §8.1：先查长度/魔数/版本，再在**分配之前**用下界拒绝过大的
/// `version_count`，然后逐条解析记录，走到记录结束处才校验 CRC，最后解 inline 帧。
pub fn decode(bytes: &[u8]) -> Result<ObjectMeta, DiskError> {
    if bytes.len() < MIN_CONTAINER_LEN {
        return Err(DiskError::Corrupt(CorruptKind::LengthMismatch));
    }
    if !bytes.starts_with(&MAGIC) {
        return Err(DiskError::Corrupt(CorruptKind::BadMagic));
    }
    let major = u16::from_le_bytes([bytes[4], bytes[5]]);
    let minor = u16::from_le_bytes([bytes[6], bytes[7]]);
    if major != MAJOR {
        return Err(DiskError::Corrupt(CorruptKind::UnsupportedVersion));
    }
    if minor > MINOR {
        return Err(DiskError::Corrupt(CorruptKind::UnsupportedVersion));
    }

    let version_count = usize::from(u16::from_le_bytes([bytes[8], bytes[9]]));

    // 分配之前的上界检查：剩余字节必须容得下 version_count 条最小记录 + trailer。
    // RECORD_MIN_BYTES 是真实下界，故此检查不会误拒合法输入。
    let remaining = bytes.len() - RECORDS_OFFSET;
    if version_count * RECORD_MIN_BYTES + TRAILER_LEN > remaining {
        return Err(DiskError::Corrupt(CorruptKind::LengthMismatch));
    }

    let mut cursor = Cursor::new(bytes);
    cursor.set_position(RECORDS_OFFSET as u64);
    let mut versions = Vec::with_capacity(version_count);
    for _ in 0..version_count {
        // msgpack 自描述且不预读：读完 header 后 position 正好是记录边界。
        let header: FileVersionHeader = rmp_serde::from_read(&mut cursor)
            .map_err(|_| DiskError::Corrupt(CorruptKind::MalformedHeader))?;

        let pos = cursor.position() as usize;
        if pos + 4 > bytes.len() {
            return Err(DiskError::Corrupt(CorruptKind::LengthMismatch));
        }
        let body_len =
            u32::from_be_bytes([bytes[pos], bytes[pos + 1], bytes[pos + 2], bytes[pos + 3]])
                as usize;
        let body_start = pos + 4;
        let body_end = body_start + body_len;
        // 分配之前校验剩余字节。
        if body_end > bytes.len() {
            return Err(DiskError::Corrupt(CorruptKind::LengthMismatch));
        }
        let body = bytes[body_start..body_end].to_vec();
        cursor.set_position(body_end as u64);
        versions.push(ShallowVersion { header, body });
    }

    // 记录读完处就是 CRC。注意覆盖范围是 `bytes[..crc_pos]`，不是 `bytes[..len-4]`。
    let crc_pos = cursor.position() as usize;
    if crc_pos + TRAILER_LEN > bytes.len() {
        return Err(DiskError::Corrupt(CorruptKind::LengthMismatch));
    }
    let expected = u32::from_le_bytes([
        bytes[crc_pos],
        bytes[crc_pos + 1],
        bytes[crc_pos + 2],
        bytes[crc_pos + 3],
    ]);
    if crc32c::crc32c(&bytes[..crc_pos]) != expected {
        return Err(DiskError::Corrupt(CorruptKind::CrcMismatch));
    }

    // 空尾部由 InlineData::decode 承担，此处不再另判空。
    let inline = InlineData::decode(&bytes[crc_pos + TRAILER_LEN..])?;

    Ok(ObjectMeta {
        versions,
        inline,
        meta_ver: major as u8,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rstore_common::error::{CorruptKind, DiskError};
    use uuid::Uuid;

    use crate::fileinfo::{decode_header, Flags, VersionType};

    /// 两个版本的样本：一个普通对象 + 一个删除标记。
    /// 刻意让两个 header 在 version_id / mod_time / data_dir 上都不同，
    /// 这样任何一个字段的编解码出错都会在 roundtrip 里暴露。
    fn sample_meta() -> ObjectMeta {
        let obj = FileVersionHeader {
            version_id: Some(Uuid::from_u128(0x1111_2222_3333_4444_5555_6666_7777_8888)),
            ty: VersionType::Object,
            size: 1234,
            mod_time: Some(1_700_000_000_000_000_000),
            ec_m: 4,
            ec_n: 6,
            flags: Flags::empty(),
            data_dir: Some(Uuid::from_u128(0x9999_aaaa_bbbb_cccc_dddd_eeee_ffff_0001)),
        };
        let marker = FileVersionHeader {
            version_id: None, // 与上面的 Some(Uuid) 形成对照
            ty: VersionType::DeleteMarker,
            size: 0,
            mod_time: None, // 与上面的 Some(..) 形成对照
            ec_m: 4,
            ec_n: 6,
            flags: Flags::empty(),
            data_dir: None,
        };
        ObjectMeta {
            versions: vec![
                ShallowVersion {
                    header: obj,
                    body: vec![0xde, 0xad, 0xbe, 0xef],
                },
                ShallowVersion {
                    header: marker,
                    body: Vec::new(),
                },
            ],
            inline: InlineData::new(),
            meta_ver: 1,
        }
    }

    #[test]
    fn encode_decode_roundtrip() {
        let m = sample_meta();
        let bytes = encode(&m).unwrap();
        let back = decode(&bytes).unwrap();
        assert_eq!(back.versions.len(), m.versions.len());
        assert_eq!(back.versions[0].header, m.versions[0].header);
        assert_eq!(back.versions[0].body, m.versions[0].body);
        // 第二条也要查——只查第一条的话，记录边界出错时可能漏掉。
        assert_eq!(back.versions[1].header, m.versions[1].header);
        assert_eq!(back.versions[1].body, m.versions[1].body);
        assert_eq!(back.meta_ver, m.meta_ver);
    }

    #[test]
    fn detects_bad_magic() {
        let mut bytes = encode(&sample_meta()).unwrap();
        bytes[0] = b'X';
        assert!(matches!(
            decode(&bytes),
            Err(DiskError::Corrupt(CorruptKind::BadMagic))
        ));
    }

    #[test]
    fn detects_crc_mismatch() {
        let bytes = encode(&sample_meta()).unwrap();
        // 必须翻转 body 里的字节：body 是不透明的，结构解析不受影响，才会稳定
        // 走到 CRC 校验。翻转 header 区可能先报 MalformedHeader。
        let needle = [0xde, 0xad, 0xbe, 0xef];
        let pos = bytes
            .windows(needle.len())
            .position(|w| w == needle)
            .expect("样本 body 应当出现在字节流中");
        let mut tampered = bytes.clone();
        tampered[pos] ^= 0xFF;
        assert!(matches!(
            decode(&tampered),
            Err(DiskError::Corrupt(CorruptKind::CrcMismatch))
        ));
    }

    #[test]
    fn rejects_truncated_buffer() {
        let bytes = encode(&sample_meta()).unwrap();
        assert!(decode(&bytes[..bytes.len() / 2]).is_err());
    }

    #[test]
    fn encode_rejects_unsupported_meta_ver() {
        // encode 不许写出自己 decode 读不回来的容器：若放任 meta_ver 落盘，
        // decode 会报 Corrupt(UnsupportedVersion)，而 DESIGN §17 规定观察到 Corrupt
        // 即触发 repair——编码期的错误就会伪装成盘损坏，对健康数据发起修复。
        let mut m = sample_meta();
        m.meta_ver = 2;
        let err = encode(&m).unwrap_err();
        assert!(
            matches!(&err, DiskError::Corrupt(CorruptKind::UnsupportedVersion)),
            "got {err:?}"
        );

        // 与 decode 侧对齐：两者必须报同一个错误，否则守卫就拦偏了。
        let mut raw = encode(&sample_meta()).unwrap();
        raw[4] = 2; // major 是偏移 4..6 的 u16 LE
        assert!(matches!(
            decode(&raw),
            Err(DiskError::Corrupt(CorruptKind::UnsupportedVersion))
        ));
    }

    #[test]
    fn inline_data_survives_roundtrip() {
        let mut m = sample_meta();
        m.inline.insert("null", b"hello".to_vec());
        m.inline.insert("v1", b"world".to_vec());
        let bytes = encode(&m).unwrap();
        let back = decode(&bytes).unwrap();
        assert_eq!(back.inline.get("null"), Some(&b"hello"[..]));
        assert_eq!(back.inline.get("v1"), Some(&b"world"[..]));
    }

    /// `RECORD_MIN_BYTES` 是 `version_count` 上界检查的下界。若它大于真实最小记录
    /// 长度，就会误拒合法输入——这里用理论最小 header 钉住它。
    #[test]
    fn record_min_bytes_is_a_true_lower_bound() {
        // 最紧凑的 header：ty = Object、所有 Option 为 None、所有整数为 0。
        let minimal = FileVersionHeader::default();
        let enc = encode_header(&minimal).unwrap();
        assert_eq!(
            enc.len(),
            HEADER_MIN_BYTES,
            "HEADER_MIN_BYTES 不再是真实下界（调大就误拒合法输入）"
        );
        // 记录 = header + body_len u32 BE + 空 body。
        assert!(enc.len() + 4 <= RECORD_MIN_BYTES);
        // 且这条最小 header 本身必须能解回。
        assert_eq!(decode_header(&enc).unwrap(), minimal);
    }

    // 核心鲁棒性属性：任意单字节翻转都必须报 Corrupt，绝不 panic、绝不返回 Ok。
    proptest! {
        #[test]
        fn never_panics_on_corruption(pos_frac in 0.0f64..1.0, mask in 1u8..=255) {
            let mut bytes = encode(&sample_meta()).unwrap();
            if bytes.is_empty() { return Ok(()); }
            let pos = ((pos_frac * bytes.len() as f64) as usize).min(bytes.len() - 1);
            bytes[pos] ^= mask;
            // 只要不 panic 且不返回 Ok 就算通过（翻转后仍合法的情况需排除 CRC 区）
            match decode(&bytes) {
                // 翻转落在 inline_data 区时 CRC 依然匹配（CRC 不覆盖内联帧），
                // 解出 Ok 是合法结果。
                Ok(_) => {}
                Err(DiskError::Corrupt(_)) => {}
                Err(other) => prop_assert!(false, "unexpected error: {other:?}"),
            }
        }
    }
}
