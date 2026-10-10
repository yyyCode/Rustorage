//! 控制面板静态资源（设计 §3.3/§4.2）。
//!
//! 与 `metrics.rs` / `readiness.rs` 的分工一致：本模块只提供**构造响应的纯函数**，
//! 不建服务器、不做路由分发、不碰文件系统。因此是同步代码，测试用 `#[test]`。

use bytes::Bytes;
use http::{header, StatusCode};
use rstore_common::consts::CONSOLE_PREFIX;

/// 一条内嵌资源。`body` 是 `include_str!` 在**编译期**读进来的源码文本——
/// 二进制里因此带着完整的页面，部署时没有任何静态文件要拷。
pub struct Asset {
    /// `CONSOLE_PREFIX` 之后的**单段**名字；空串表示该命名空间的根（index.html）。
    pub name: &'static str,
    pub mime: &'static str,
    pub body: &'static str,
}

/// 全部资源。**这是唯一一份资源清单**：加文件要同时在这里登记，
/// 漏登记会被 `index_references_only_registered_assets` 抓住。
pub const ASSETS: &[Asset] = &[
    Asset {
        name: "",
        mime: "text/html; charset=utf-8",
        body: include_str!("../console/index.html"),
    },
    Asset {
        name: "style.css",
        mime: "text/css; charset=utf-8",
        body: include_str!("../console/style.css"),
    },
    Asset {
        name: "app.js",
        mime: "text/javascript; charset=utf-8",
        body: include_str!("../console/app.js"),
    },
    Asset {
        name: "sigv4.js",
        mime: "text/javascript; charset=utf-8",
        body: include_str!("../console/sigv4.js"),
    },
    Asset {
        name: "s3api.js",
        mime: "text/javascript; charset=utf-8",
        body: include_str!("../console/s3api.js"),
    },
    Asset {
        name: "metrics.js",
        mime: "text/javascript; charset=utf-8",
        body: include_str!("../console/metrics.js"),
    },
    Asset {
        name: "ui.js",
        mime: "text/javascript; charset=utf-8",
        body: include_str!("../console/ui.js"),
    },
];

/// 面板不引任何外部资源，这条 CSP 是对「以后有人往 HTML 里塞 CDN `<script>`」的护栏。
/// 它也是 index.html 里不能出现内联脚本的原因（`default-src 'self'` 会拒掉内联）。
const CSP: &str = "default-src 'self'; connect-src 'self'; img-src 'self' data:; \
                   base-uri 'none'; form-action 'none'";

/// 路径是否落在面板命名空间里：前缀本身，或前缀之后**紧跟一个 `/`** 的更深路径。
///
/// 「紧跟 `/`」这个条件是必须的：没有它，`/_consoleX`、`/_console.html` 也会被截下来，
/// 而那正是 `startup.rs` 里「精确路径匹配」那条注释要避免的事。
fn in_namespace(path: &str) -> bool {
    path == CONSOLE_PREFIX
        || path
            .strip_prefix(CONSOLE_PREFIX)
            .is_some_and(|rest| rest.starts_with('/'))
}

/// 面板路由。返回 `None` = 「不是我的路径」，调用方原样交给 s3s。
///
/// `enabled` 为 `false` 时**恒为 `None`**：把开关收在这一个函数里，调用点就只是
/// 一次 `match`，不会出现「某条路径忘了判开关」。
pub fn maybe_route(enabled: bool, path: &str) -> Option<http::Response<Bytes>> {
    if !enabled || !in_namespace(path) {
        return None;
    }
    let name = if path == CONSOLE_PREFIX {
        ""
    } else {
        &path[CONSOLE_PREFIX.len() + 1..]
    };
    Some(match ASSETS.iter().find(|a| a.name == name) {
        Some(asset) => asset_response(asset),
        None => not_found(),
    })
}

fn asset_response(asset: &Asset) -> http::Response<Bytes> {
    http::Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, asset.mime)
        // 本地面板，别让浏览器缓存一个旧的 app.js 掩盖更新。
        .header(header::CACHE_CONTROL, "no-store")
        .header("content-security-policy", CSP)
        .body(Bytes::from_static(asset.body.as_bytes()))
        .expect("静态资源的状态码与响应头必然合法")
}

fn not_found() -> http::Response<Bytes> {
    http::Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header(header::CONTENT_TYPE, "text/plain; charset=utf-8")
        .header("content-security-policy", CSP)
        .body(Bytes::from_static(b"no such console asset\n"))
        .expect("静态状态码与响应头必然合法")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_never_intercepts() {
        // 开关关闭时 `/_console` 必须**原样落到 s3s**——这与 `/metrics/` 的既有处理
        // 是同一条纪律（见 startup.rs 里「精确路径匹配」的注释）。
        for p in ["/_console", "/_console/", "/_console/app.js"] {
            assert!(maybe_route(false, p).is_none(), "{p} 在关闭时不该被拦");
        }
    }

    #[test]
    fn unrelated_paths_are_not_intercepted() {
        // `/_consoleX` 与 `/_console.html` 只是**同前缀**，不是同段——必须落到 s3s。
        for p in [
            "/",
            "/metrics",
            "/metrics/",
            "/_consoleX",
            "/_console.html",
            "/_consolex/a",
        ] {
            assert!(maybe_route(true, p).is_none(), "{p} 不该被拦");
        }
    }

    #[test]
    fn index_is_served_at_the_namespace_roots() {
        // `/_console` 与 `/_console/` 都要给 index.html：前者靠绝对路径的资源引用
        // （见 index.html 里的注释），所以不需要重定向。
        for p in ["/_console", "/_console/"] {
            let resp = maybe_route(true, p).expect("应当命中");
            assert_eq!(resp.status(), StatusCode::OK);
            assert_eq!(
                resp.headers()[header::CONTENT_TYPE],
                "text/html; charset=utf-8"
            );
            assert!(!resp.body().is_empty());
        }
    }

    #[test]
    fn every_asset_is_served_with_the_right_mime() {
        for asset in ASSETS {
            let path = if asset.name.is_empty() {
                format!("{CONSOLE_PREFIX}/")
            } else {
                format!("{CONSOLE_PREFIX}/{}", asset.name)
            };
            let resp = maybe_route(true, &path).expect("应当命中");
            assert_eq!(resp.status(), StatusCode::OK, "{path}");
            assert_eq!(resp.headers()[header::CONTENT_TYPE], asset.mime, "{path}");
            assert!(!resp.body().is_empty(), "{path} 资源是空的");
        }
    }

    #[test]
    fn unknown_asset_is_404_without_spa_fallback() {
        // **不降级到 index.html**：本面板用 hash 路由，服务端不需要 catch-all，
        // 而 catch-all 只会白白扩大被 `/_console` 占住的路径空间（设计 §4.3）。
        let resp = maybe_route(true, "/_console/nope.js").expect("应当命中");
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    /// 从 HTML 里抠出所有 `CONSOLE_PREFIX` 之后的资源名。
    ///
    /// 只取**文件名字符**（`[A-Za-z0-9._-]`），取不到就是空串：index.html 的注释里
    /// 也写着 `/_console`，用「以引号/空格切分」那种写法会把注释里的反引号内容
    /// 当成资源名，测试于是对着一条注释报错。
    fn referenced_assets(html: &str) -> Vec<String> {
        html.split(CONSOLE_PREFIX)
            .skip(1)
            .map(|chunk| {
                chunk
                    .trim_start_matches('/')
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
                    .collect()
            })
            .collect()
    }

    #[test]
    fn index_references_only_registered_assets() {
        // 手写资源表的代价就用这条兜住：index.html 里写错资源名（或少登记一个文件）
        // 在这里红，而不是等到浏览器里 404。
        let index = ASSETS
            .iter()
            .find(|a| a.name.is_empty())
            .expect("资源表里必须有 index.html")
            .body;
        let referenced = referenced_assets(index);

        // 先证明提取本身没退化——否则下面那个断言在「一个引用都没抠出来」时也会过，
        // 变成一条永远绿的假测试。
        for expected in ["style.css", "app.js"] {
            assert!(
                referenced.iter().any(|n| n == expected),
                "没能从 index.html 抠出 {expected}，提取逻辑可能已失效: {referenced:?}"
            );
        }

        for name in referenced.iter().filter(|n| !n.is_empty()) {
            assert!(
                ASSETS.iter().any(|a| a.name == name),
                "index.html 引用了未登记的资源: {name}"
            );
        }
    }

    /// 抠出源码里 `getElementById('x')` 的 `x`。
    fn looked_up_ids(js: &str) -> Vec<String> {
        js.split("getElementById('")
            .skip(1)
            .map(|chunk| chunk.chars().take_while(|&c| c != '\'').collect())
            .collect()
    }

    #[test]
    fn app_looks_up_ids_that_index_actually_provides() {
        // 面板没有构建链、也没有 DOM 层的测试，`getElementById` 取到 `null` 是最可能的
        // 「白屏」原因：`app.js` 一开头就对这些节点赋值，少一个就整页不渲染。
        // 这条不让浏览器出场就能钉住它——按 id 建节点是 HTML 与 JS 之间唯一的硬约定。
        let index = ASSETS
            .iter()
            .find(|a| a.name.is_empty())
            .expect("资源表里必须有 index.html")
            .body;
        let app = ASSETS
            .iter()
            .find(|a| a.name == "app.js")
            .expect("资源表里必须有 app.js")
            .body;

        let ids = looked_up_ids(app);
        // 同样先证明提取没退化，否则「一个都没查到」会让下面的循环变成空转。
        assert!(
            !ids.is_empty(),
            "没能从 app.js 抠出任何 getElementById，提取逻辑可能已失效"
        );

        for id in &ids {
            assert!(
                index.contains(&format!("id=\"{id}\"")),
                "app.js 取了 #{id}，但 index.html 里没有这个 id（会白屏）"
            );
        }
    }
}
