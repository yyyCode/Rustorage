//! 阻塞版文件系统原语。
//!
//! 全部函数接收**盘根**与**相对路径**，内部先经 [`resolve`] 做路径逃逸检查，
//! 再把 IO 错误映射为 [`DiskError`]。`local.rs` 用 `spawn_blocking` 调它们。
//!
//! 这些原语刻意保持同步：即便是在非阻塞运行时里，直接做文件 IO 会占住 executor
//! 线程；把整段临界区丢进 `spawn_blocking` 也顺带满足 `await_holding_lock` 的约束。

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};

use rstore_common::error::{DiskError, FatalKind, TransientKind};

use crate::error_map::map_io;
use crate::FileStat;

/// 把相对路径解析到盘根之下，拒绝任何逃逸。
///
/// **逐 `Component` 判断**（而非 `starts_with("..")` 之类字符串前缀检查——
/// 后者会漏掉 `a/../../b`，也会误判 `..foo`）。目标路径可能尚不存在，
/// 因此**不使用 `canonicalize()`**（它会访问文件系统并在缺失时失败）。
///
/// 拒绝：`..`、绝对路径（`/...`、`\...`）、Windows 盘符前缀（`C:\...`）。
pub fn resolve(root: &Path, rel: &str) -> Result<PathBuf, DiskError> {
    // 以下两条可移植防线在 *所有* 平台生效（`Component` 只在 Windows 上识别它们）：
    // - 前导反斜杠 `\...`：Windows 上是根相对绝对路径；Linux 上被当作普通文件名。
    // - 盘符前缀 `C:` / `C:\...`：Windows 上是 `Component::Prefix`。
    // 存储层的相对路径不应含这些形态，一律 fail closed。
    if rel.starts_with('\\') || looks_like_windows_prefix(rel) {
        return Err(DiskError::Fatal(FatalKind::PathEscape));
    }

    let mut safe = PathBuf::new();
    for comp in Path::new(rel).components() {
        match comp {
            Component::Normal(seg) => safe.push(seg),
            Component::CurDir => {}
            // `..` 逃逸、根目录、盘符前缀全部拒绝。
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(DiskError::Fatal(FatalKind::PathEscape));
            }
        }
    }
    Ok(root.join(safe))
}

/// `X:` / `x:\...` 形态的盘符前缀检测（跨平台）。
fn looks_like_windows_prefix(rel: &str) -> bool {
    let b = rel.as_bytes();
    b.len() >= 2 && b[0].is_ascii_alphabetic() && b[1] == b':'
}

/// 定长定位读取某一文件区间。
///
/// `std::os::unix::fs::FileExt::read_at` 与 `std::os::windows::fs::FileExt::seek_read`
/// 是**两个不同的 trait**，签名参数顺序一致（`buf`, `offset`），故此处统一封装。
/// `#[cfg]` 分流保证两平台各自选到正确的实现。
///
/// 返回 `io::Result<usize>`：允许短读（0 表示到 EOF），由调用方决定如何处置。
#[cfg(unix)]
pub fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

/// 见 unix 版本的说明。
#[cfg(windows)]
pub fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

/// 读满 `len` 字节的定位读。
///
/// 语义分叉（测试会检验）：
/// - 路径不存在 → `NotFound`（确定性缺失）。
/// - 文件存在但 `offset + len` 越界（含偏移越过 EOF）→ `Transient(ShortRead)`
///   （瞬时、可重试；**绝不是 `Corrupt`**）。
pub fn read_exact_at(
    root: &Path,
    rel: &str,
    offset: u64,
    len: usize,
) -> Result<Vec<u8>, DiskError> {
    let path = resolve(root, rel)?;
    let file = File::open(&path).map_err(map_io)?;

    let mut buf = vec![0u8; len];
    let mut filled = 0usize;
    while filled < len {
        let pos = offset
            .checked_add(filled as u64)
            .ok_or(DiskError::Transient(TransientKind::ShortRead))?;
        match read_at(&file, pos, &mut buf[filled..]) {
            // 提前 EOF：读到 0 字节即短读。
            Ok(0) => return Err(DiskError::Transient(TransientKind::ShortRead)),
            Ok(n) => filled += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(map_io(e)),
        }
    }
    Ok(buf)
}

/// 全量写文件（自动创建父目录）并 fsync 文件本身。
pub fn write_all_fsync(root: &Path, rel: &str, data: &[u8]) -> Result<(), DiskError> {
    let path = resolve(root, rel)?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(map_io)?;
    }
    let mut file = File::create(&path).map_err(map_io)?;
    file.write_all(data).map_err(map_io)?;
    file.sync_all().map_err(map_io)?;
    Ok(())
}

/// 原子重命名，并 fsync 目标父目录，保证 rename 在崩溃后仍可见。
pub fn rename_fsync(root: &Path, from_rel: &str, to_rel: &str) -> Result<(), DiskError> {
    let from = resolve(root, from_rel)?;
    let to = resolve(root, to_rel)?;
    if let Some(parent) = to.parent() {
        fs::create_dir_all(parent).map_err(map_io)?;
    }
    fs::rename(&from, &to).map_err(map_io)?;
    if let Some(parent) = to.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

/// 删除目录树；**幂等**——路径不存在时返回 `Ok(())`。
pub fn remove_dir_all(root: &Path, rel: &str) -> Result<(), DiskError> {
    let path = resolve(root, rel)?;
    match fs::remove_dir_all(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(map_io(e)),
    }
}

/// 列目录，返回**按字典序升序**排序的条目名。
pub fn list_dir(root: &Path, rel: &str) -> Result<Vec<String>, DiskError> {
    let path = resolve(root, rel)?;
    let read = fs::read_dir(&path).map_err(map_io)?;
    let mut names = Vec::new();
    for entry in read {
        let entry = entry.map_err(map_io)?;
        names.push(entry.file_name().to_string_lossy().into_owned());
    }
    names.sort();
    Ok(names)
}

/// 取元信息；路径不存在返回 `Ok(None)`（不是 `Err(NotFound)`）。
pub fn stat(root: &Path, rel: &str) -> Result<Option<FileStat>, DiskError> {
    let path = resolve(root, rel)?;
    match fs::metadata(&path) {
        Ok(md) => Ok(Some(FileStat {
            size: md.len(),
            is_dir: md.is_dir(),
        })),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(map_io(e)),
    }
}

/// fsync 文件本身，**然后** fsync 父目录——顺序不可颠倒，否则 rename 可能不持久。
///
/// 幂等：连调两次都成功。父目录 fsync 是平台相关的（Windows 不支持对目录句柄
/// `FlushFileBuffers`），见 [`sync_dir`]。
pub fn sync_file_and_parent(root: &Path, rel: &str) -> Result<(), DiskError> {
    let path = resolve(root, rel)?;
    // 用可写句柄打开：Windows 的 `FlushFileBuffers` 要求句柄具备 GENERIC_WRITE，
    // 只读句柄会返回 ERROR_ACCESS_DENIED。Linux 上 O_RDWR 同样可用。
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(map_io)?;
    file.sync_all().map_err(map_io)?;
    drop(file);
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

/// fsync 一个目录。Windows 上为 no-op（`std` 无法以写权限打开目录句柄）。
pub fn sync_dir(dir: &Path) -> Result<(), DiskError> {
    #[cfg(unix)]
    {
        let f = File::open(dir).map_err(map_io)?;
        f.sync_all().map_err(map_io)?;
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
    }
    Ok(())
}

/// 深度优先遍历 `rel` 之下所有条目，返回以 `rel` 为前缀、以 `/` 连接的相对路径。
///
/// 供上层（如对账 / heal 扫描）复用；不跟随符号链接。
pub fn walk(root: &Path, rel: &str) -> Result<Vec<String>, DiskError> {
    let base = resolve(root, rel)?;
    let mut out = Vec::new();
    walk_rec(root, &base, &mut out)?;
    out.sort();
    Ok(out)
}

fn walk_rec(root: &Path, dir: &Path, out: &mut Vec<String>) -> Result<(), DiskError> {
    let read = fs::read_dir(dir).map_err(map_io)?;
    for entry in read {
        let entry = entry.map_err(map_io)?;
        let path = entry.path();
        let ft = entry.file_type().map_err(map_io)?;
        let rel = path
            .strip_prefix(root)
            .map_err(|_| DiskError::Fatal(FatalKind::PathEscape))?;
        out.push(rel.to_string_lossy().replace('\\', "/"));
        if ft.is_dir() {
            walk_rec(root, &path, out)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_rejects_escapes_and_accepts_normal() {
        let root = Path::new("/disk/root");
        for bad in ["../escape", "a/../../escape", "/etc/passwd", "..foo/../x"] {
            assert!(
                matches!(
                    resolve(root, bad),
                    Err(DiskError::Fatal(FatalKind::PathEscape))
                ),
                "should reject {bad}"
            );
        }
        assert!(resolve(root, "a/b.txt").is_ok());
        assert!(resolve(root, "./a/./b").is_ok());
    }

    #[test]
    fn resolve_rejects_absolute_and_drive_prefix() {
        let root = Path::new("/disk/root");
        assert!(matches!(
            resolve(root, "/etc/passwd"),
            Err(DiskError::Fatal(FatalKind::PathEscape))
        ));
        // 盘符前缀在 Linux 上也应被拒。
        assert!(matches!(
            resolve(root, "C:\\Windows\\system32"),
            Err(DiskError::Fatal(FatalKind::PathEscape))
        ));
        assert!(matches!(
            resolve(root, "\\Windows"),
            Err(DiskError::Fatal(FatalKind::PathEscape))
        ));
    }
}
