//! 元数据容器 meta.xl、format.json、分片分布排列。

pub mod container;
pub mod distribution;
pub mod fileinfo;
pub mod inline;
pub mod keys;

pub use container::{decode, encode};
pub use fileinfo::{
    decode_header, encode_header, ChecksumAlgo, FileVersionHeader, Flags, InlineData, ObjectBody,
    ObjectMeta, OpaqueBody, PartInfo, ShallowVersion, StorageClass, VersionType,
};
