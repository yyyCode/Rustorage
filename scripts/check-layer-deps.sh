#!/usr/bin/env bash
# 校验 DESIGN §5 的依赖方向规则（R1–R4）。
# 白名单语义：只允许表中列出的内部依赖边。表已按传递闭包补齐。
set -euo pipefail

# 解释器探测：必须真正执行一段程序并核对输出，退出码不可信。
# Windows 上 python3 可能是 Store/MSIX 别名：`python3 --version` 正常、
# `python3 -c ''` 返回 0，但 `python3 -c 'print(1)'` 却是 Permission denied(126)。
# 所以判据是「跑得出 1」，而不是「命令存在」或「退出码为 0」。
PYTHON=""
for cand in python3 python; do
    if command -v "$cand" >/dev/null 2>&1 \
       && [ "$("$cand" -c 'print(1)' 2>/dev/null)" = "1" ]; then
        PYTHON="$cand"
        break
    fi
done
if [ -z "$PYTHON" ]; then
    echo "ERROR: 未找到可用的 python3/python 解释器" >&2
    exit 2
fi

# 注意：这里必须写成 "$PYTHON" -c "$(cat <<'PY' ... PY)"。
# 不能写成 `cargo metadata ... | python3 - <<'PY'`——管道虽先绑定 fd 0，
# 但同一条命令上的 heredoc 重定向会覆盖它，于是 python 从 heredoc 读"程序"，
# 而 json.load(sys.stdin) 读到 EOF。这是 shell 重定向语义，与平台无关。
cargo metadata --format-version 1 --no-deps | "$PYTHON" -c "$(cat <<'PY'
import json, sys

ALLOWED = {
    "rstore-common":    set(),
    "rstore-checksum":  {"rstore-common"},
    "rstore-erasure":   {"rstore-common"},
    "rstore-meta":      {"rstore-common", "rstore-checksum"},
    "rstore-disk":      {"rstore-common", "rstore-meta", "rstore-checksum"},
    "rstore-store":     {"rstore-common", "rstore-checksum", "rstore-erasure",
                         "rstore-meta", "rstore-disk"},
    "rstore-api":       {"rstore-common"},
    "rstore-s3":        {"rstore-common", "rstore-api"},
    "rstore-s3-compat": {"rstore-common"},
    # 组合根：允许看见全部（DESIGN §5 R4 规定绑定实现只在这里发生）
    "rstore-server":    {"rstore-common", "rstore-checksum", "rstore-erasure",
                         "rstore-meta", "rstore-disk", "rstore-store",
                         "rstore-api", "rstore-s3", "rstore-s3-compat"},
}

meta = json.load(sys.stdin)
fail = False

for pkg in meta["packages"]:
    name = pkg["name"]
    if name not in ALLOWED:
        print(f"UNKNOWN CRATE: {name} 未在护栏表中登记 —— 新增 crate 必须显式登记其允许依赖")
        fail = True
        continue
    for dep in pkg["dependencies"]:
        dep_name = dep["name"]
        if not dep_name.startswith("rstore-"):
            continue                      # 只约束内部 crate
        if dep_name not in ALLOWED[name]:
            kind = dep.get("kind") or "normal"
            print(f"FORBIDDEN EDGE: {name} -> {dep_name}  (kind={kind})")
            fail = True

sys.exit(1 if fail else 0)
PY
)"
