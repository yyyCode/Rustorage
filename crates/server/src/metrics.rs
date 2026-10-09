//! Prometheus 指标（DESIGN §18.2）。
//!
//! 本模块只提供**构造响应的纯函数**（`http::Response<bytes::Bytes>`）与一段
//! 文本渲染，不建服务器、不做路由——路由属于 Task 6.3，故这里是同步代码。
//!
//! 刻意**不引入 `prometheus` crate**：MVP 只需要原子计数 + 一段文本渲染，而
//! `prometheus` 会带来一个全局注册表（测试之间互相污染）和一堆用不到的
//! Gauge/Histogram 类型。`render()` 返回 `String` 是纯函数，测起来也干净。

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

/// 指标名常量集中定义，避免散落的字符串字面量。
const PUT_DURATION: &str = "put_duration_seconds";
const GET_DURATION: &str = "get_duration_seconds";
const QUORUM_FAILURES: &str = "erasure_quorum_failures_total";
const BITROT_MISMATCH: &str = "bitrot_mismatch_total";
const DISK_ERRORS: &str = "disk_errors_total";

/// `put_duration_seconds` / `get_duration_seconds` 的 `stage` 标签值。
///
/// MVP 的 `record_put` / `record_get` 只拿到一次**整体**耗时（调用方尚无
/// rename / fsync 等阶段细分信息），故统一记在 `total` 这一档。等调用方
/// 能提供阶段信息时，再按 DESIGN §18.2 拆出更细的 stage。
const STAGE_TOTAL: &str = "total";

/// 指标收集器。`enabled=false` 时所有 `record_*` 均为空操作（DESIGN §18.2：
/// 开关关闭时调用方跳过 `Instant::now()`，这里连原子写也一并跳过）。
pub struct Metrics {
    enabled: bool,
    /// 时长类指标只累计「纳秒总和」，不做桶——Histogram 要的是一组边界，
    /// 而 MVP 没有分位数消费方，加了只会得到一份没人看的输出。
    put_nanos: AtomicU64,
    get_nanos: AtomicU64,
    bitrot_mismatch: AtomicU64,
    /// 带动态标签值的计数器族：`op` -> 次数、`kind` -> 次数。
    /// 用 `Mutex` 守护「注册表」的插入，每格的值仍是 `AtomicU64`。
    quorum_failures: Mutex<HashMap<String, AtomicU64>>,
    disk_errors: Mutex<HashMap<String, AtomicU64>>,
}

impl Metrics {
    /// 新建收集器；`enabled` 为总开关。
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            put_nanos: AtomicU64::new(0),
            get_nanos: AtomicU64::new(0),
            bitrot_mismatch: AtomicU64::new(0),
            quorum_failures: Mutex::new(HashMap::new()),
            disk_errors: Mutex::new(HashMap::new()),
        }
    }

    /// 记一次 PUT 的整体耗时。
    pub fn record_put(&self, d: Duration) {
        if self.enabled {
            self.put_nanos.fetch_add(nanos(d), Ordering::Relaxed);
        }
    }

    /// 记一次 GET 的整体耗时。
    pub fn record_get(&self, d: Duration) {
        if self.enabled {
            self.get_nanos.fetch_add(nanos(d), Ordering::Relaxed);
        }
    }

    /// 记一次纠删码 quorum 失败，按操作类型（如 `put` / `get`）分档。
    pub fn record_quorum_failure(&self, op: &str) {
        if self.enabled {
            inc_labeled(&self.quorum_failures, op);
        }
    }

    /// 记一次 bitrot（静默损坏）校验不匹配。
    pub fn record_bitrot_mismatch(&self) {
        if self.enabled {
            self.bitrot_mismatch.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 记一次盘错误，按错误类型（`transient` / `corrupt` / `fatal`，
    /// 对应 `DiskError` 的三级分类）分档。
    pub fn record_disk_error(&self, kind: &str) {
        if self.enabled {
            inc_labeled(&self.disk_errors, kind);
        }
    }

    /// 渲染成 Prometheus 文本格式。开关关闭时返回常量（两次调用逐字节相同）。
    ///
    /// 每行都是合法的 `name{label="v"} value`，指标名与标签之间无空格；
    /// 带标签的族按标签值排序，保证同一份状态两次渲染逐行一致。
    pub fn render(&self) -> String {
        if !self.enabled {
            return String::new();
        }
        let mut out = String::new();
        line(
            &mut out,
            PUT_DURATION,
            Some(("stage", STAGE_TOTAL)),
            secs(self.put_nanos.load(Ordering::Relaxed)),
        );
        line(
            &mut out,
            GET_DURATION,
            Some(("stage", STAGE_TOTAL)),
            secs(self.get_nanos.load(Ordering::Relaxed)),
        );
        for (op, count) in snapshot(&self.quorum_failures) {
            line(&mut out, QUORUM_FAILURES, Some(("op", &op)), count);
        }
        line(
            &mut out,
            BITROT_MISMATCH,
            None,
            self.bitrot_mismatch.load(Ordering::Relaxed),
        );
        for (kind, count) in snapshot(&self.disk_errors) {
            line(&mut out, DISK_ERRORS, Some(("kind", &kind)), count);
        }
        out
    }

    /// `/metrics` 的响应：`Content-Type: text/plain; version=0.0.4`，正文是 `render()`。
    /// 路由在 Task 6.3，本模块只造响应。
    pub fn metrics_response(&self) -> http::Response<bytes::Bytes> {
        http::Response::builder()
            .status(http::StatusCode::OK)
            .header(http::header::CONTENT_TYPE, "text/plain; version=0.0.4")
            .body(bytes::Bytes::from(self.render()))
            .expect("静态状态码与响应头必然合法")
    }
}

/// `Duration` -> 纳秒（u64）。纳秒总和的量级远超现实耗时，截断无风险。
fn nanos(d: Duration) -> u64 {
    d.as_nanos() as u64
}

/// 纳秒 -> 秒的文本（`render()` 输出用秒）。
fn secs(nanos: u64) -> f64 {
    nanos as f64 / 1_000_000_000.0
}

/// 往带标签的计数器族里 +1。锁只守护「注册表」的插入，值本身是原子加。
/// 锁中毒时静默跳过：指标失败不该连累主流程，更不该 panic。
fn inc_labeled(map: &Mutex<HashMap<String, AtomicU64>>, label: &str) {
    let Ok(mut m) = map.lock() else {
        return;
    };
    m.entry(label.to_owned())
        .or_default()
        .fetch_add(1, Ordering::Relaxed);
}

/// 取带标签族的快照，**按标签值排序**，保证渲染可复现。
fn snapshot(map: &Mutex<HashMap<String, AtomicU64>>) -> Vec<(String, u64)> {
    let Ok(m) = map.lock() else {
        return Vec::new();
    };
    let mut rows: Vec<(String, u64)> = m
        .iter()
        .map(|(k, c)| (k.clone(), c.load(Ordering::Relaxed)))
        .collect();
    rows.sort();
    rows
}

/// 追加一行合法的 Prometheus 文本。单标签族只需一个 `k="v"`，无需多键排序。
fn line(out: &mut String, name: &str, label: Option<(&str, &str)>, value: impl std::fmt::Display) {
    match label {
        Some((k, v)) => out.push_str(&format!("{name}{{{k}=\"{v}\"}} {value}\n")),
        None => out.push_str(&format!("{name} {value}\n")),
    }
}

// 同 6.1：本任务全是同步代码，用 `#[test]`。
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn metrics_disabled_is_noop() {
        let m = Metrics::new(false);
        let before = m.render(); // 读一次快照
        m.record_put(Duration::from_millis(3));
        m.record_get(Duration::from_millis(3));
        m.record_quorum_failure("put");
        m.record_bitrot_mismatch();
        m.record_disk_error("transient");
        assert_eq!(m.render(), before); // **再读一次比对**：只写「没 panic」是空断言
    }

    #[test]
    fn exposes_prometheus_text_format() {
        let m = Metrics::new(true);
        m.record_put(Duration::from_millis(3));
        m.record_quorum_failure("put");
        m.record_bitrot_mismatch();
        m.record_disk_error("transient");
        let text = m.render();
        // 断言字符串包含，不是断言 `is_ok()`——「返回 200 但正文是空表」只有 contains 抓得到。
        for name in [
            "put_duration_seconds",
            "get_duration_seconds",
            "erasure_quorum_failures_total",
            "bitrot_mismatch_total",
            "disk_errors_total",
        ] {
            assert!(text.contains(name), "指标 {name} 不在输出里:\n{text}");
        }
        // 计数不能是「写死的 0」：record_* 之后的取值必须真的变化。
        assert!(text.contains("erasure_quorum_failures_total{op=\"put\"} 1"));
        assert!(text.contains("bitrot_mismatch_total 1"));
        assert!(text.contains("disk_errors_total{kind=\"transient\"} 1"));
        assert!(text.contains("put_duration_seconds{stage=\"total\"} 0.003"));
    }
}
