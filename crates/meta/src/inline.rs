//! 内联数据帧的编解码（DESIGN §8.4）。
//!
//! 帧就是纯 msgpack map：`version-key → bytes`，由 [`InlineData`] 经
//! `#[serde(transparent)]` 直接序列化而来。**帧内不带版本字节**——整个 `meta.xl`
//! 的 `major`/`minor` 已承载格式演进（DESIGN §8.3），再挂一套版本号是重复的版本
//! 承载点。

use rstore_common::error::{CorruptKind, DiskError};

use crate::fileinfo::InlineData;

impl InlineData {
    /// 编码成 msgpack map（DESIGN §8.4）。
    ///
    /// 帧内不带版本字节：格式演进由容器的 `major`/`minor` 承载。
    pub fn encode(&self) -> Result<Vec<u8>, DiskError> {
        rmp_serde::to_vec(self).map_err(|_| DiskError::Corrupt(CorruptKind::MalformedHeader))
    }

    /// 从字节解出。
    ///
    /// **空输入视为空 map**——容器解码时尾部可能什么都没有（DESIGN §8.4），
    /// 这不是畸形输入，否则一个不含内联数据的合法容器会读不出来。
    /// 其余解码失败统一映射为 `MalformedHeader`。
    pub fn decode(bytes: &[u8]) -> Result<Self, DiskError> {
        if bytes.is_empty() {
            return Ok(Self::new());
        }
        rmp_serde::from_slice(bytes).map_err(|_| DiskError::Corrupt(CorruptKind::MalformedHeader))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_multiple_versions() {
        let mut d = InlineData::default();
        d.insert("null", b"hello".to_vec());
        d.insert("v1", b"world".to_vec());
        let bytes = d.encode().unwrap();
        let back = InlineData::decode(&bytes).unwrap();
        assert_eq!(back.get("null"), Some(&b"hello"[..]));
        assert_eq!(back.get("v1"), Some(&b"world"[..]));
        assert_eq!(back.get("missing"), None);
    }

    #[test]
    fn empty_input_decodes_to_empty_map() {
        // 容器尾部理论上可能什么都没有（DESIGN §8.4）。这是解码侧要容忍的形状，
        // 不能被当成畸形输入——否则一个合法容器会因为「没有内联数据」而读不出来。
        assert_eq!(InlineData::decode(&[]).unwrap(), InlineData::new());
    }

    #[test]
    fn garbage_decodes_to_malformed_header() {
        // 0x91 = fixarray(1)，不是 map——解不成 `InlineData`。
        let err = InlineData::decode(&[0x91, 0x01]).unwrap_err();
        assert!(
            matches!(&err, DiskError::Corrupt(CorruptKind::MalformedHeader)),
            "got {err:?}"
        );
    }

    #[test]
    fn should_inline_respects_thresholds() {
        assert!(rstore_common::consts::should_inline(64 * 1024, false));
        assert!(!rstore_common::consts::should_inline(256 * 1024, false));
        // 版本化桶门限更严格（1/8）
        assert!(!rstore_common::consts::should_inline(32 * 1024, true));
        assert!(rstore_common::consts::should_inline(8 * 1024, true));
    }
}
