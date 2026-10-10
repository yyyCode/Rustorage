#!/usr/bin/env bash
# 控制面板验收（设计 §8.2）。与 tests/acceptance.sh 同一套约定：
# 先构建再起进程（冷构建时间不能算进轮询窗口）、轮询 /ready、trap 收尾。
set -euo pipefail

for tool in curl; do
    command -v "$tool" >/dev/null || { echo "$tool 未安装，无法跑控制面板验收" >&2; exit 1; }
done

PORT=${PORT:-9200}
ROOT=/tmp/rs-console
BASE="http://127.0.0.1:$PORT"

rm -rf "$ROOT"
mkdir -p "$ROOT"/{d1,d2}
cargo build -p rstore-server
BIN=target/debug/rstore-server
[ -e "$BIN" ] || BIN="$BIN.exe"
[ -e "$BIN" ] || { echo "找不到构建产物 rstore-server" >&2; exit 1; }

start() {
    # 直接跑构建产物而非 `cargo run`：后者会留下一个 cargo 父进程，
    # `kill` 杀掉它之后真正的服务可能还占着端口（理由见 tests/acceptance.sh）。
    "$BIN" --volumes "$ROOT"/d{1,2} --parity 1 --port "$PORT" "$@" &
    SERVER_PID=$!
    for _ in $(seq 1 120); do
        curl -fsS -o /dev/null "$BASE/ready" && return 0
        sleep 0.5
    done
    echo "server did not become ready" >&2
    exit 1
}

stop() {
    # `SERVER_PID` 用 `${VAR:-}`：脚本在 `start` 之前失败时（`set -u` 下）EXIT trap
    # 仍会跑 `stop`，那时变量还不存在，直接引用会让收尾动作本身报错、盖掉真正的失败原因。
    if [ -n "${SERVER_PID:-}" ]; then
        kill "$SERVER_PID" 2>/dev/null || true
        wait "$SERVER_PID" 2>/dev/null || true
        SERVER_PID=
    fi
}
trap 'stop; rm -rf "$ROOT"' EXIT

expect() { # expect <路径> <期望状态码> [期望 content-type 前缀]
    local code mime
    code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE$1")
    [ "$code" = "$2" ] || { echo "FAIL $1: 期望 $2，得到 $code" >&2; exit 1; }
    if [ $# -ge 3 ]; then
        mime=$(curl -s -o /dev/null -w '%{content_type}' "$BASE$1")
        case "$mime" in
            "$3"*) ;;
            *) echo "FAIL $1: 期望 content-type $3*，得到 $mime" >&2; exit 1 ;;
        esac
    fi
    echo "ok $1 -> $code"
}

# ============================================================================
# 关闭时（默认）：面板路径**原样落到 s3s**，因此返回的是 S3 层的答复，
# 不是面板的 404。
#
# 这里的状态码是**实测**出来的，与最初的计划不同：s3s 在**路径解析阶段**就把
# 首字符为 `_` 的段判为非法桶名（`check_bucket_name`：只允许 `[a-z0-9.-]`），
# 于是 `/_console/` 是 **400 InvalidBucketName**，而不是「走到鉴权后被拒」的 403。
# 这条断言的价值正在于此：它证明关闭开关时面板**连一个字节都没拦**。
# ============================================================================
echo "== 关闭时（默认）=="
start
expect /_console/ 400
expect /_console 400
expect /_console/app.js 400
# 同前缀不同段，同样进不了面板命名空间，同样被 s3s 判为非法桶名。
expect /_consoleX 400
stop

echo "== 打开 --console =="
start --console --metrics
expect /_console/ 200 text/html
expect /_console 200 text/html
for asset in style.css app.js sigv4.js s3api.js metrics.js ui.js; do
    expect "/_console/$asset" 200
done

# CSP 是设计 §4.2 的护栏（禁止外链与内联），必须在**每一条**面板响应上——
# 包括 404，否则「打错一个资源名」这条路径就少了护栏。
echo "== 安全响应头 =="
for path in /_console/ /_console/app.js /_console/nope.js; do
    csp=$(curl -s -D - -o /dev/null "$BASE$path" | tr -d '\r' | grep -i '^content-security-policy:' || true)
    [ -n "$csp" ] || { echo "FAIL $path 缺少 CSP 头" >&2; exit 1; }
    case "$csp" in
        *"default-src 'self'"*) ;;
        *) echo "FAIL $path 的 CSP 不含 default-src 'self'：$csp" >&2; exit 1 ;;
    esac
    echo "ok $path 带 CSP"
done

echo "== 不越界 =="
# 未登记的资源 404，**不降级到 index.html**（设计 §4.3）。
expect /_console/nope.js 404
# 同前缀不同段：必须落到 s3s（未签名 → 400 非法桶名，理由同「关闭时」那一节）。
expect /_consoleX 400
# **回归**：`/metrics/` 带尾斜杠仍是 s3s 的桶路径空间（`metrics` 是**合法**桶名，
# 所以它能一路走到鉴权，未签名 → 403，与 `/_console/` 的 400 形成对照）。
expect /metrics/ 403
# 运维端点本身照常。
expect /metrics 200
expect /health 200
expect /ready 200

echo "== 前端资源正文 =="
# 每个资源都认领一个**只属于它**的标识符：状态码 200 只证明「有个文件被送出来了」，
# 登记表写对、内容却是占位或串了文件时它一样绿。这里是「送出来的确实是那一份」。
# 注意 `signRequest` 属于 sigv4.js 而**不是** app.js——app.js 只是 import 它。
while IFS='|' read -r path token; do
    curl -fsS "$BASE/_console/$path" | grep -q "$token" \
        || { echo "FAIL $path 正文不含 $token" >&2; exit 1; }
    echo "ok $path 含 $token"
done <<'EOF'
|Rustorage Console
app.js|renderBuckets
sigv4.js|signRequest
s3api.js|listBuckets
metrics.js|parseMetrics
ui.js|openDrawer
EOF

echo "== CSP 护栏：不得有内联样式 =="
# CSP 是 `default-src 'self'`，内联 `style=` 与 `<style>` 块都会被浏览器**静默**
# 拒掉——页面看着只是「样式没生效」，报错只在 DevTools 里。这条把静默失败
# 变成验收脚本里的响亮失败。（`.style.` 是 JS 里写内联样式的另一种形态。）
if grep -q 'style=' crates/server/console/index.html; then
    echo "FAIL index.html 含内联 style=，会被 CSP 拒掉" >&2; exit 1
fi
if grep -q '<style' crates/server/console/index.html; then
    echo "FAIL index.html 含 <style> 块，会被 CSP 拒掉" >&2; exit 1
fi
for js in app.js ui.js; do
    if grep -q '\.style\.' "crates/server/console/$js"; then
        echo "FAIL $js 写了 node.style.*，请改用 style.css 的类名" >&2; exit 1
    fi
done
echo "ok 无内联样式"

echo "== 有 node 时顺带查一遍 ES module 语法（可选）=="
if command -v node >/dev/null; then
    for js in app.js sigv4.js s3api.js metrics.js ui.js; do
        # node --check 对 .js 默认按 CommonJS 解析，会误报 import/export，故拷成 .mjs。
        cp "crates/server/console/$js" /tmp/rs-check.mjs
        node --check /tmp/rs-check.mjs || { echo "FAIL $js 语法" >&2; exit 1; }
        echo "ok $js 语法"
    done
    rm -f /tmp/rs-check.mjs
else
    echo "skip 未装 node"
fi

echo "CONSOLE: OK"
