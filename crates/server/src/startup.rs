//! 启动与关闭编排（Task 6.3）。
//!
//! `parse → LocalDisk::open 各盘 → 逐盘读 format.json → select_authoritative →
//! ErasureSet::new → build_service → 起监听 → FullReady`。盘上 `format.json` 的读写
//! 在这里落地（Task 3.3 刻意留给 6.3）：这是全流程唯一一处直接碰文件系统的地方。

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::{service_fn, Service};
use hyper_util::rt::TokioIo;
use rstore_api::ObjectStore;
use rstore_common::disk_id::DiskId;
use rstore_disk::error_map::map_io;
use rstore_disk::{DiskAPI, DiskError, LocalDisk};
use rstore_meta::format::{
    select_authoritative, should_initialize, DiskInfo, FormatErasureV1, FormatV1,
    DISTRIBUTION_ALGO, FORMAT_ERASURE,
};
use rstore_store::set::ErasureSet;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::readiness::{Readiness, SystemStage};
use crate::wiring::Wiring;

/// `shutdown` 等待已登记任务的上限。超时就放弃并告警，别让一个卡住的请求
/// 让进程永远退不出去。
const SHUTDOWN_TASK_TIMEOUT: Duration = Duration::from_secs(5);

/// `open_disks` 的产出：一个已装配好的 erasure set。
pub struct OpenOutcome {
    pub(crate) set: Arc<ErasureSet>,
}

/// 正在运行的服务：监听地址、取消令牌与在飞任务句柄。
pub struct Running {
    local_addr: SocketAddr,
    token: CancellationToken,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl Running {
    /// 在 `addr` 上监听（用 `cfg.port`，`0` 表示由内核分配），把 `/health` `/ready`
    /// `/metrics` 三条精确路径与 `--console` 打开时的 `/_console` 命名空间截下来，
    /// 其余交给 s3s。返回后服务已在跑。
    pub async fn bind(
        cfg: &Config,
        ready: Arc<Readiness>,
        m: Arc<Metrics>,
    ) -> anyhow::Result<Self> {
        // 从 cfg 到「可以接受连接」的整条链都在这里。
        let outcome = open_disks(cfg).await?;
        ready.mark_stage(SystemStage::StorageReady);

        let store: Arc<dyn ObjectStore> = Arc::new(Wiring::new(outcome.set, Arc::clone(&m)));
        // build_service 返回 Err 是「--base-domain 不是合法域名」——运维输入错误，
        // 必须打印那条信息并以非零码退出，不要 unwrap()。
        let s3 = rstore_s3::build_service(
            store,
            &cfg.access_key,
            &cfg.secret_key,
            cfg.base_domain.as_deref(),
        )
        .map_err(|e| anyhow!(e))?;

        // MVP 只绑 127.0.0.1，不做 TLS 也不对外。
        let addr = SocketAddr::from(([127, 0, 0, 1], cfg.port));
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .with_context(|| format!("绑定 127.0.0.1:{} 失败", cfg.port))?;
        let local = listener.local_addr()?;

        let token = CancellationToken::new();
        let tasks: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::new(Mutex::new(Vec::new()));
        let running = Running {
            local_addr: local,
            token: token.clone(),
            tasks: Arc::clone(&tasks),
        };

        // accept 循环自己也是一个被 track_task 登记的任务。
        let loop_handle = tokio::spawn(accept_loop(
            listener,
            s3,
            Arc::clone(&ready),
            Arc::clone(&m),
            cfg.console,
            token,
        ));
        running.track_task(loop_handle);

        // TcpListener 绑好、accept 循环已 spawn → FullReady。
        ready.mark_stage(SystemStage::FullReady);
        Ok(running)
    }

    /// 真实监听地址（`port: 0` 时由内核分配，测试靠它取端口）。
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// 登记一个在飞任务的句柄，`shutdown()` 会等它跑完。
    pub fn track_task(&self, handle: JoinHandle<()>) {
        if let Ok(mut tasks) = self.tasks.lock() {
            tasks.push(handle);
        }
    }

    /// 先停止 accept，再等在飞请求跑完（有上限，别无限等）。
    pub async fn shutdown(self) {
        self.token.cancel();
        // 加锁取出句柄后立刻出作用域再 `.await`（`clippy::await_holding_lock` 是 deny）。
        let handles: Vec<JoinHandle<()>> = match self.tasks.lock() {
            Ok(mut tasks) => std::mem::take(&mut *tasks),
            Err(_) => Vec::new(),
        };
        for handle in handles {
            match tokio::time::timeout(SHUTDOWN_TASK_TIMEOUT, handle).await {
                Ok(Ok(())) => {}
                Ok(Err(join_err)) => tracing::warn!(error = %join_err, "关闭时任务以错误结束"),
                Err(_) => tracing::warn!("关闭等待任务超时，放弃"),
            }
        }
    }
}

/// accept 循环：取消令牌触发时退出，退出时 `drop(listener)` → 端口释放。
async fn accept_loop(
    listener: tokio::net::TcpListener,
    s3: s3s::service::S3Service,
    ready: Arc<Readiness>,
    metrics: Arc<Metrics>,
    console: bool,
    token: CancellationToken,
) {
    loop {
        let (stream, _peer) = tokio::select! {
            _ = token.cancelled() => break,
            accept = listener.accept() => match accept {
                Ok(pair) => pair,
                Err(_) => break,
            },
        };
        let s3 = s3.clone(); // S3Service: Clone（内部是 Arc，很便宜）
        let ready = Arc::clone(&ready);
        let metrics = Arc::clone(&metrics);
        tokio::spawn(async move {
            let conn = http1::Builder::new().serve_connection(
                TokioIo::new(stream),
                service_fn(move |req: http::Request<Incoming>| {
                    let s3 = s3.clone();
                    let ready = Arc::clone(&ready);
                    let metrics = Arc::clone(&metrics);
                    async move {
                        // **精确路径匹配**，不是前缀——`GET /metrics/` 必须落到 s3s 去
                        // （一个名叫 metrics 的桶）。路径先拷成 owned，避免借用 req 再把它 move 给 s3s。
                        let path = req.uri().path().to_owned();
                        match path.as_str() {
                            "/health" => Ok::<_, s3s::HttpError>(
                                Readiness::health_response().map(s3s::Body::from),
                            ),
                            "/ready" => Ok(ready.ready_response().map(s3s::Body::from)),
                            "/metrics" => Ok(metrics.metrics_response().map(s3s::Body::from)),
                            // 控制面板也判在这一层，理由有三：
                            // 1. 它的路由是**命名空间**匹配（`/_console` 或 `/_console/...`），
                            //    与上面三条的精确匹配不是一套规则，判断收在 `maybe_route` 里；
                            // 2. 必须在 s3s **之前**——`_console` 含下划线，会被 s3s 的
                            //    `check_bucket_name` 在路径解析期判成非法桶名直接 400，
                            //    永远轮不到我们（见 `crates/s3` 的 underscore 测试）；
                            // 3. 落在 ready 门**之前**，所以启动中也能打开页面看阶段
                            //    （预备页面本身就要显示 Booting），这是设计 §2.2 要的。
                            // `S3Service` 有同名的固有 `call(req: Request<Body>)`，
                            // 会遮蔽 hyper trait 方法，必须用全限定语法走 trait 那个
                            // `call(req: Request<Incoming>)`。
                            _ => match crate::console::maybe_route(console, &path) {
                                Some(resp) => Ok::<_, s3s::HttpError>(resp.map(s3s::Body::from)),
                                None => Service::call(&s3, req).await,
                            },
                        }
                    }
                }),
            );
            // `Connection` 是个 future，不 await 它什么都不发生。
            let _ = conn.await;
        });
    }
}

/// 打开各盘、校验 `format.json`、构造 `ErasureSet`。
pub async fn open_disks(cfg: &Config) -> anyhow::Result<OpenOutcome> {
    // `validate()` 要求每个 set 宽度 2..=16，MVP 是全部盘一个 set，所以盘数必须在
    // 2..=16。只给一块盘时这里报一句人话，而不是让 `ErasureSet::new` / `validate`
    // 抛出一堆看不出是参数问题的错误。
    if !(2..=16).contains(&cfg.volumes.len()) {
        return Err(anyhow!(
            "--volumes 需要 2..=16 块盘（一个 set 至少要 2 块、最多 16 块），当前给了 {} 块",
            cfg.volumes.len()
        ));
    }

    // 1. bootstrap：用 std::fs 逐盘读 <volume>/format.json。绕开 DiskAPI 是因为
    //    `LocalDisk::open(root, disk_id)` 需要先有 DiskId，而 id 只可能来自盘上。
    let mut formats: Vec<(PathBuf, FormatV1)> = Vec::new();
    let mut disk_errs: Vec<DiskError> = Vec::new();
    let mut missing_paths: Vec<PathBuf> = Vec::new();

    for volume in &cfg.volumes {
        match std::fs::read(volume.join("format.json")) {
            Ok(bytes) => {
                let fmt: FormatV1 = serde_json::from_slice(&bytes)
                    .map_err(|e| anyhow!("盘 {} 的 format.json 解析失败: {e}", volume.display()))?;
                formats.push((volume.clone(), fmt));
            }
            Err(e) => {
                let de = map_io(e);
                if de == DiskError::NotFound {
                    missing_paths.push(volume.clone());
                }
                disk_errs.push(de);
            }
        }
    }

    // 2. 决策。`should_initialize` 只看失败列表，所以这里还必须 `formats.is_empty()`：
    //    否则「一盘有格式、一盘 NotFound」时 errs=[NotFound] 会误判成「全部缺失」。
    if formats.is_empty() && should_initialize(&disk_errs) {
        let disks = initialize_disks(cfg).await?;
        let set = Arc::new(
            ErasureSet::new(disks, cfg.parity())
                .map_err(|e| anyhow!("构造 erasure set 失败: {e}"))?,
        );
        return Ok(OpenOutcome { set });
    }

    // 有盘读到了、有盘 NotFound → 拒绝：把空盘也格式化会悄悄把它认成老成员。
    if !formats.is_empty() && !missing_paths.is_empty() {
        return Err(anyhow!(
            "拒绝启动：部分盘缺少 format.json（{}），但其它盘已有格式——把空盘初始化会把它悄悄认成老成员。请补齐或移除这些盘",
            missing_paths
                .iter()
                .map(|p| p.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    // 3. 没有任何 NotFound：协商权威拓扑。
    let owned: Vec<FormatV1> = formats.iter().map(|(_, f)| f.clone()).collect();
    let authoritative = match select_authoritative(&owned) {
        Ok(f) => f,
        Err(e) => {
            // `FormatError` 不带盘路径，路径由 6.3 在调用处补：按 shared_identity
            // 分组，把「盘路径 → 组号」的清单打出来，运维才知道去修哪块盘。
            let groups = group_by_identity(&formats);
            return Err(anyhow!(
                "format.json 协商失败: {e}；各盘分组: {}",
                groups
                    .iter()
                    .map(|(p, g)| format!("{} -> 组{}", p.display(), g))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
    };

    // 4. 非初始化路径：id 取该盘自己的 `format.erasure.this`，盘序按权威 `format.sets`
    //    （不是 --volumes 顺序——重启用不同顺序给盘时，分片下标必须仍与 format 一致）。
    let mut by_id: HashMap<DiskId, PathBuf> = HashMap::new();
    for (path, fmt) in &formats {
        by_id.insert(fmt.erasure.this, path.clone());
    }
    let mut disks: Vec<Option<Arc<dyn DiskAPI>>> = Vec::new();
    for set in &authoritative.erasure.sets {
        for id in set {
            match by_id.get(id) {
                Some(volume) => {
                    let disk = LocalDisk::open(volume, *id)
                        .map_err(|e| anyhow!("打开盘 {} 失败: {e}", volume.display()))?;
                    disks.push(Some(Arc::new(disk) as Arc<dyn DiskAPI>));
                }
                // 这轮启动没读到该盘（Transient）→ 槽位留空，由纠删码按「缺失」处理。
                None => disks.push(None),
            }
        }
    }

    let set = Arc::new(
        ErasureSet::new(disks, cfg.parity()).map_err(|e| anyhow!("构造 erasure set 失败: {e}"))?,
    );
    Ok(OpenOutcome { set })
}

/// 全新初始化：每块盘一个 `DiskId::new_v4()`，全部盘组成一个 set（顺序即 `--volumes`），
/// 逐盘写 `format.json` 与 `.rstore.sys/disk_id`，每个都跟着 `sync_file_and_parent`。
async fn initialize_disks(cfg: &Config) -> anyhow::Result<Vec<Option<Arc<dyn DiskAPI>>>> {
    let ids: Vec<DiskId> = cfg.volumes.iter().map(|_| DiskId::new_v4()).collect();
    let deployment_id = DiskId::new_v4().to_string();
    let sets = vec![ids.clone()];

    let mut disks = Vec::with_capacity(cfg.volumes.len());
    for (volume, &this_id) in cfg.volumes.iter().zip(&ids) {
        let disk = LocalDisk::open(volume, this_id)
            .map_err(|e| anyhow!("打开盘 {} 失败: {e}", volume.display()))?;

        let fmt = FormatV1 {
            version: "1".into(),
            format: FORMAT_ERASURE.into(),
            id: deployment_id.clone(),
            erasure: FormatErasureV1 {
                version: "1".into(),
                this: this_id,
                sets: sets.clone(),
                distribution_algo: DISTRIBUTION_ALGO.into(),
            },
            // 容量信息不参与一致性，填零值即可。
            disk_info: DiskInfo { total: 0, free: 0 },
        };
        let json = serde_json::to_vec_pretty(&fmt)
            .map_err(|e| anyhow!("序列化盘 {} 的 format.json 失败: {e}", volume.display()))?;

        disk.write_all("format.json", &json)
            .await
            .map_err(|e| anyhow!("写盘 {} 的 format.json 失败: {e}", volume.display()))?;
        disk.sync_file_and_parent("format.json")
            .await
            .map_err(|e| anyhow!("fsync 盘 {} 的 format.json 失败: {e}", volume.display()))?;

        disk.write_all(".rstore.sys/disk_id", this_id.to_string().as_bytes())
            .await
            .map_err(|e| anyhow!("写盘 {} 的 .rstore.sys/disk_id 失败: {e}", volume.display()))?;
        disk.sync_file_and_parent(".rstore.sys/disk_id")
            .await
            .map_err(|e| {
                anyhow!(
                    "fsync 盘 {} 的 .rstore.sys/disk_id 失败: {e}",
                    volume.display()
                )
            })?;

        disks.push(Some(Arc::new(disk) as Arc<dyn DiskAPI>));
    }
    Ok(disks)
}

/// 把 `(路径, FormatV1)` 按 `shared_identity()` 分组，返回 `(路径, 组号)`。
fn group_by_identity(formats: &[(PathBuf, FormatV1)]) -> Vec<(PathBuf, usize)> {
    let mut identities: Vec<Vec<u8>> = Vec::new();
    let mut out = Vec::with_capacity(formats.len());
    for (path, f) in formats {
        let id = f.shared_identity();
        let group = identities.iter().position(|g| *g == id).unwrap_or_else(|| {
            identities.push(id);
            identities.len() - 1
        });
        out.push((path.clone(), group));
    }
    out
}

/// `main.rs` 的入口：`bind` → 等关闭信号 → `shutdown`。
pub async fn serve(cfg: &Config) -> anyhow::Result<()> {
    let ready = Arc::new(Readiness::new());
    let metrics = Arc::new(Metrics::new(cfg.metrics));
    let running = Running::bind(cfg, ready, metrics).await?;
    tracing::info!(addr = %running.local_addr(), "服务已就绪");

    // 只等 Ctrl-C，不引入 `tokio::signal::unix`（`#[cfg(unix)]`，Windows 上编译不过）。
    tokio::signal::ctrl_c()
        .await
        .context("等待 Ctrl-C 信号失败")?;
    running.shutdown().await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config(volumes: Vec<PathBuf>) -> Config {
        Config {
            volumes,
            port: 0,
            parity: Some(1),
            access_key: "rustorage".into(),
            secret_key: "rustorage-secret".into(),
            base_domain: None,
            metrics: false,
            console: false,
        }
    }

    #[tokio::test]
    async fn refuses_to_start_on_inconsistent_formats() {
        // 两盘 format.json 的 shared_identity 不一致 → 启动返回 Err，
        // 且错误信息里包含出问题的那块盘的路径。
        let dirs: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let volumes: Vec<PathBuf> = dirs.iter().map(|d| d.path().to_path_buf()).collect();

        // 先在空目录上初始化成一致的两盘格式。
        open_disks(&make_config(volumes.clone())).await.unwrap();

        // 把其中一块盘的 format.json 改写成另一个 identity（改参与身份的 `format.id`）。
        let target = dirs[1].path().join("format.json");
        let mut fmt: FormatV1 = serde_json::from_slice(&std::fs::read(&target).unwrap()).unwrap();
        fmt.id = "different-deployment-id".into();
        std::fs::write(&target, serde_json::to_vec_pretty(&fmt).unwrap()).unwrap();

        let err = match open_disks(&make_config(volumes.clone())).await {
            Err(e) => e,
            Ok(_) => panic!("拓扑不一致时必须拒绝启动"),
        };
        let msg = format!("{err:#}");
        assert!(
            msg.contains(&volumes[0].display().to_string()),
            "错误信息应包含第一块盘路径: {msg}"
        );
        assert!(
            msg.contains(&volumes[1].display().to_string()),
            "错误信息应包含第二块盘路径: {msg}"
        );
    }

    #[tokio::test]
    async fn refuses_to_reformat_reachable_disks() {
        // 一盘写好 format.json、一盘是空目录 → 启动返回 Err，
        // 且不得把那个空目录初始化成新盘。
        let dirs: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let volumes: Vec<PathBuf> = dirs.iter().map(|d| d.path().to_path_buf()).collect();

        // 只给第一块盘写 format.json（直接构造一份合法的 FormatV1 即可）。
        let this_id = DiskId::new_v4();
        let fmt = FormatV1 {
            version: "1".into(),
            format: FORMAT_ERASURE.into(),
            id: DiskId::new_v4().to_string(),
            erasure: FormatErasureV1 {
                version: "1".into(),
                this: this_id,
                sets: vec![vec![this_id, DiskId::new_v4()]],
                distribution_algo: DISTRIBUTION_ALGO.into(),
            },
            disk_info: DiskInfo { total: 0, free: 0 },
        };
        std::fs::write(
            dirs[0].path().join("format.json"),
            serde_json::to_vec_pretty(&fmt).unwrap(),
        )
        .unwrap();

        let err = match open_disks(&make_config(volumes.clone())).await {
            Err(e) => e,
            Ok(_) => panic!("空盘与有格式盘混合时必须拒绝启动"),
        };
        let msg = format!("{err:#}");
        assert!(msg.contains(&volumes[1].display().to_string()), "got {msg}");

        // 空盘绝不能被顺手初始化。
        assert!(
            !dirs[1].path().join("format.json").exists(),
            "空盘被悄悄初始化成了新成员"
        );
    }

    #[tokio::test]
    async fn shutdown_stops_accepting_then_drains() {
        // 两块空目录 → 全部 NotFound → 走初始化路径，顺带把「初始化」也测了。
        let dirs: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let volumes: Vec<PathBuf> = dirs.iter().map(|d| d.path().to_path_buf()).collect();
        let ready = Arc::new(Readiness::new());
        let metrics = Arc::new(Metrics::new(false));

        let running = Running::bind(&make_config(volumes), ready, metrics)
            .await
            .unwrap();
        let addr = running.local_addr();

        // (b) 关闭发起之前已经接住的在飞工作要跑完才返回：
        // 这个任务完全不看 token（真实请求也不会因为关闭信号自己中止）。
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done_flag = Arc::clone(&done);
        let work = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(200)).await;
            done_flag.store(true, std::sync::atomic::Ordering::SeqCst);
        });
        // 顺序：先 track_task 再 shutdown（shutdown 吃掉 self）。
        running.track_task(work);
        running.shutdown().await;

        // (b) shutdown 返回时在飞工作已经跑完。
        assert!(done.load(std::sync::atomic::Ordering::SeqCst));

        // (a) 关闭后新连接被拒——不是回 503，而是根本连不上（监听套接字已释放）。
        // 必须在 shutdown().await 返回之后才断言。
        assert!(
            tokio::net::TcpStream::connect(addr).await.is_err(),
            "shutdown 返回后监听套接字必须已释放"
        );
    }

    /// 用裸 TCP 发一个请求并读回整个响应。**必须带 `Connection: close`**：
    /// 不带的话 hyper 保持 keep-alive，`read_to_end` 会一直等下去（测试挂死）。
    async fn raw_get(addr: std::net::SocketAddr, path: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        stream.write_all(req.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        String::from_utf8_lossy(&buf).into_owned()
    }

    async fn bind_with(volumes: Vec<PathBuf>, console: bool) -> Running {
        let mut cfg = make_config(volumes);
        cfg.console = console;
        Running::bind(
            &cfg,
            Arc::new(Readiness::new()),
            Arc::new(Metrics::new(false)),
        )
        .await
        .unwrap()
    }

    /// 面板路由真的接上了没有——`maybe_route` 自身的行为由 `console.rs` 的测试负责，
    /// 这里验的是**接线**，所以打的是真实 TCP。
    #[tokio::test]
    async fn console_route_is_wired_and_off_by_default() {
        let dirs: Vec<tempfile::TempDir> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let volumes: Vec<PathBuf> = dirs.iter().map(|d| d.path().to_path_buf()).collect();

        // 默认（`console: false`）时 `/_console/` 落到 s3s。状态码是 **400 而不是 403**：
        // `_console` 含下划线，被 s3s 的 `check_bucket_name` 在路径解析期就判成非法桶名，
        // 根本走不到鉴权那一步。这个区别值得钉住——它同时也是「面板必须在 s3s 之前
        // 截获」这条设计约束的证据：一旦截晚了，路径就已经是个 400 了。
        let running = bind_with(volumes.clone(), false).await;
        let body = raw_get(running.local_addr(), "/_console/").await;
        assert!(
            body.starts_with("HTTP/1.1 400"),
            "应落到 s3s 并判非法桶名: {body}"
        );
        running.shutdown().await;

        // 打开开关：同一路径变成面板页面。
        let running = bind_with(volumes, true).await;
        let addr = running.local_addr();
        let body = raw_get(addr, "/_console/").await;
        assert!(body.starts_with("HTTP/1.1 200"), "应命中面板: {body}");
        assert!(body.contains("text/html"), "应当给 HTML: {body}");
        assert!(body.contains("Rustorage Console"), "应拿到页面正文: {body}");

        // 同前缀不同段：仍然落到 s3s（同样是 s3s 给的 400）。
        let body = raw_get(addr, "/_consoleX").await;
        assert!(
            body.starts_with("HTTP/1.1 400"),
            "/_consoleX 不该被面板带走: {body}"
        );

        // **回归**：`/metrics/`（带尾斜杠）必须继续落到 s3s。这里是 403 而不是 400——
        // `metrics` 是**合法**桶名，所以它走得到鉴权那一步。启动脚本里那条「精确路径
        // 匹配」注释的可执行版本就是这条断言。
        let body = raw_get(addr, "/metrics/").await;
        assert!(
            body.starts_with("HTTP/1.1 403"),
            "/metrics/ 不该命中运维端点: {body}"
        );
        running.shutdown().await;
    }
}
