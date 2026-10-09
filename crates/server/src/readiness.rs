//! 就绪探针与存活探针（DESIGN §18.1）。
//!
//! 本模块只提供**构造响应的纯函数**（`http::Response<bytes::Bytes>`），
//! 不建服务器、不做路由、不开监听——路由与监听属于 Task 6.3。
//! 因此这里是同步代码，测试用 `#[test]` 而非 `#[tokio::test]`。

use std::sync::atomic::{AtomicU8, Ordering};

/// 进程启动阶段，**单调递增**。
///
/// 派生 `PartialEq + Eq` 是测试里 `assert_eq!(r.stage(), SystemStage::Booting)`
/// 所需；`Debug` 让断言失败时能打印。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemStage {
    /// 正在启动（初始态）。
    Booting = 0,
    /// 存储层已就绪：可以开始接流量。
    StorageReady = 1,
    /// 全部子系统就绪（全量就绪）。
    FullReady = 2,
}

impl SystemStage {
    /// 把裸 `u8` 还原成枚举。未知值一律当 `Booting`：
    /// `load()` 读到的只可能是我们写过的值，无需 `Option` 去污染公开 API。
    fn from_u8(v: u8) -> Self {
        match v {
            1 => SystemStage::StorageReady,
            2 => SystemStage::FullReady,
            _ => SystemStage::Booting,
        }
    }
}

/// 就绪门。`stage` 用 `AtomicU8` 存，支持多线程无锁读写。
///
/// `#[derive(Default)]` 不是可选项：`AtomicU8` 的 `Default` 就是 0（= `Booting`），
/// 而少了它 `clippy::new_without_default` 会在 `-D warnings` 下直接挡死门禁。
#[derive(Default)]
pub struct Readiness {
    stage: AtomicU8,
}

impl Readiness {
    /// 新建时就绪门，初始阶段为 [`SystemStage::Booting`]。
    pub fn new() -> Self {
        Self::default()
    }

    /// 读取当前阶段。
    pub fn stage(&self) -> SystemStage {
        SystemStage::from_u8(self.stage.load(Ordering::Acquire))
    }

    /// **单向**推进阶段：只允许升，返回 `false` 表示这次调用被拒且 stage 未变。
    ///
    /// 用 `fetch_max` 而不是 `store`——比较与写入必须是一步原子操作。
    /// 「先 `load` 再比较再 `store`」在并发下会让两个请求双双通过，
    /// 而这条规则恰恰是给并发路径用的。
    pub fn mark_stage(&self, s: SystemStage) -> bool {
        let old = self.stage.fetch_max(s as u8, Ordering::AcqRel);
        old < s as u8
    }

    /// 就绪探针：达到 [`SystemStage::StorageReady`] 及以上返回 `200` 空体；
    /// 否则 `503` + `Retry-After: 5`。
    pub fn ready_response(&self) -> http::Response<bytes::Bytes> {
        if (self.stage() as u8) >= (SystemStage::StorageReady as u8) {
            http::Response::new(bytes::Bytes::new())
        } else {
            http::Response::builder()
                .status(http::StatusCode::SERVICE_UNAVAILABLE)
                .header(http::header::RETRY_AFTER, "5")
                .body(bytes::Bytes::new())
                .expect("静态状态码与响应头必然合法")
        }
    }

    /// 存活探针：**不看 stage**，进程活着即 `200` 空体。
    pub fn health_response() -> http::Response<bytes::Bytes> {
        http::Response::new(bytes::Bytes::new())
    }
}

// 这四个都是 `#[test]` 而不是 `#[tokio::test]`：本模块只有同步的响应构造，
// 没有 IO。用 async 测试只会让编译变慢、并让人误以为这里在起服务器。
#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[test]
    fn ready_is_503_with_retry_after_while_booting() {
        let r = Readiness::new();
        let resp = r.ready_response();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(resp.headers()["retry-after"], "5");
    }

    #[test]
    fn ready_is_200_after_storage_ready() {
        let r = Readiness::new();
        r.mark_stage(SystemStage::StorageReady);
        let resp = r.ready_response();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(!resp.headers().contains_key("retry-after")); // 就绪后不能再劝客户端重试
    }

    #[test]
    fn stage_is_monotonic() {
        let r = Readiness::new();
        assert!(r.mark_stage(SystemStage::FullReady));
        assert!(!r.mark_stage(SystemStage::StorageReady)); // 被拒
        assert_eq!(r.stage(), SystemStage::FullReady); // **重新读一次**，不能只调用了事
    }

    #[test]
    fn health_is_independent_of_readiness() {
        let r = Readiness::new(); // 仍是 Booting
        assert_eq!(r.stage(), SystemStage::Booting);
        assert_eq!(Readiness::health_response().status(), StatusCode::OK);
    }
}
