//! IO 错误 → [`DiskError`] 的分级映射。
//!
//! 核心不变量（DESIGN §17）：heal 的触发条件是「观察到 `Corrupt`」，
//! 因此 **IO 层永不产生 `Corrupt`**——那只能由上层（meta 层的 CRC/格式校验）产生。
//! 宁可把瞬时抖动当成可重试的 `Transient`，也不要把未知错误误判为数据损坏。

use std::io;

use rstore_common::error::{DiskError, FatalKind, TransientKind};

/// 把一次 `std::io` 操作失败映射为领域错误。
///
/// 分级：
/// - `NotFound` → [`DiskError::NotFound`]（确定性缺失，quorum 计为「缺失」）。
/// - `UnexpectedEof` → `Transient(ShortRead)`；`WouldBlock` / `TimedOut` → `Transient(Timeout)`。
/// - `PermissionDenied` → `Fatal(PermissionDenied)`；只读文件系统 → `Fatal(ReadOnly)`。
/// - **其余一律 `Transient(Io)`**：默认可重试，绝不升级为 `Corrupt`。
pub fn map_io(err: io::Error) -> DiskError {
    match err.kind() {
        io::ErrorKind::NotFound => DiskError::NotFound,
        io::ErrorKind::UnexpectedEof => DiskError::Transient(TransientKind::ShortRead),
        io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut => {
            DiskError::Transient(TransientKind::Timeout)
        }
        io::ErrorKind::PermissionDenied => DiskError::Fatal(FatalKind::PermissionDenied),
        // 兜底：未知/其余错误 → Transient(Io)，除非 OS 明确指出是只读挂载。
        _ => {
            if is_read_only(&err) {
                DiskError::Fatal(FatalKind::ReadOnly)
            } else {
                DiskError::Transient(TransientKind::Io)
            }
        }
    }
}

/// `ErrorKind` 没有稳定的「只读文件系统」变体，因此退回 `raw_os_error` 判定。
fn is_read_only(err: &io::Error) -> bool {
    match err.raw_os_error() {
        Some(code) => is_read_only_code(code),
        None => false,
    }
}

#[cfg(unix)]
fn is_read_only_code(code: i32) -> bool {
    // EROFS：Linux 与 macOS 均为 30。
    code == 30
}

#[cfg(windows)]
fn is_read_only_code(code: i32) -> bool {
    // ERROR_WRITE_PROTECT。
    code == 19
}

#[cfg(not(any(unix, windows)))]
fn is_read_only_code(_code: i32) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn not_found_maps_to_not_found() {
        let e = io::Error::from(io::ErrorKind::NotFound);
        assert_eq!(map_io(e), DiskError::NotFound);
    }

    #[test]
    fn permission_denied_maps_to_fatal() {
        let e = io::Error::from(io::ErrorKind::PermissionDenied);
        assert_eq!(map_io(e), DiskError::Fatal(FatalKind::PermissionDenied));
    }

    #[test]
    fn unknown_error_defaults_to_transient_io_never_corrupt() {
        // 关键回归：兜底分支不得把未知错误归为 Corrupt。
        let e = io::Error::from(io::ErrorKind::Other);
        assert_eq!(map_io(e), DiskError::Transient(TransientKind::Io));
    }

    #[test]
    fn eof_and_timeouts_are_transient() {
        assert_eq!(
            map_io(io::Error::from(io::ErrorKind::UnexpectedEof)),
            DiskError::Transient(TransientKind::ShortRead)
        );
        assert_eq!(
            map_io(io::Error::from(io::ErrorKind::TimedOut)),
            DiskError::Transient(TransientKind::Timeout)
        );
        assert_eq!(
            map_io(io::Error::from(io::ErrorKind::WouldBlock)),
            DiskError::Transient(TransientKind::Timeout)
        );
    }
}
