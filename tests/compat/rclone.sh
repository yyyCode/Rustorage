#!/usr/bin/env bash
# Task 5.9 冒烟测试：rclone（本机 v1.75.2）。
#
# 被测行为：rclone 的 --s3-upload-cutoff 默认 200 MiB，1 MiB 载荷是单次 PUT。
set -euo pipefail
command -v rclone >/dev/null || { echo "rclone 未安装" >&2; exit 1; }
EP="http://127.0.0.1:9000"
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT

# 用环境变量定义 remote，不写 ~/.config/rclone/rclone.conf（那会污染开发机）。
# `provider=Minio` 是为了让 rclone 用 path-style 寻址并挑一套对自建端点更宽松的
# 签名细节；`provider=Other` 也能用，但对 AWS 专有行为更敏感。
export RCLONE_CONFIG_RS_TYPE=s3
export RCLONE_CONFIG_RS_PROVIDER=Minio
export RCLONE_CONFIG_RS_ENDPOINT="$EP"
export RCLONE_CONFIG_RS_ACCESS_KEY_ID=rustorage
export RCLONE_CONFIG_RS_SECRET_ACCESS_KEY=rustorage-secret
export RCLONE_CONFIG_RS_FORCE_PATH_STYLE=true

# 远程语法是 `rs:路径`（带冒号）。写成 `rs/test-bucket`（不带冒号）时 rclone 会把它
# 当成**本地相对目录**，全程不碰服务端，还会在仓库根留下一个 `rs/test-bucket/` 目录。
rclone mkdir rs:test-bucket
mkdir -p "$WORK/src"
head -c 1048576 /dev/urandom > "$WORK/src/1m.bin"
rclone copy "$WORK/src" rs:test-bucket/
# 必须是 `copyto`，不能写 `copy`。`rclone copy <文件> <路径>` 把目标当目录，
# 实际产出 `$WORK/roundtrip.bin/1m.bin`，而且退出码是 0。下面那行 `cmp` 于是
# 变成「拿文件比目录」而失败，报错完全指不到真正的原因。
rclone copyto rs:test-bucket/1m.bin "$WORK/roundtrip.bin"
cmp "$WORK/src/1m.bin" "$WORK/roundtrip.bin"
# `check` 会比对大小与 ETag——它正是「HEAD 的 ETag 必须与 LIST 的一致」的验收点。
# ETag 两处算法分叉时，这条会失败而 `cmp` 不会。
rclone check "$WORK/src" rs:test-bucket --one-way
echo "rclone smoke: OK"
