#!/usr/bin/env bash
# 校验 DESIGN §5 的依赖方向规则（R1–R4）。策略在 check_layer_deps.py。
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

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

# 注意：管道右侧的命令绝不能带 heredoc 重定向。
# `cargo metadata ... | python3 - <<'PY'` 是错的——管道虽先绑定 fd 0，
# 但同一条命令上的 heredoc 会覆盖它，于是 python 读到的是程序而非 JSON。
# `set -o pipefail` 让 cargo 失败时整条管道失败；检查器的 2 号退出码
# 再把「护栏自身跑不起来」与「发现违规」区分开。
#
# 左侧保持在工作区根目录执行（cargo 需要它）。右侧把 SCRIPT_DIR 作为参数
# 传给原生 python 时，Git Bash 会做 POSIX→Windows 路径转换，
# 因此 Windows 上也能跑通。若日后报 “No such file”，
# 说明该转换失效，改用 `(cd "$SCRIPT_DIR" && "$PYTHON" ./check_layer_deps.py)`。
cargo metadata --format-version 1 --no-deps | "$PYTHON" "$SCRIPT_DIR/check_layer_deps.py"
