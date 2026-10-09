#!/usr/bin/env bash
# Task 5.9 冒烟测试：mc（本机 RELEASE.2025-07-21）。
#
# 被测行为：mc cp 默认 64 MiB 以上才走 multipart，1 MiB 载荷是单次 PUT。
set -euo pipefail
command -v mc >/dev/null || { echo "mc 未安装" >&2; exit 1; }
EP="http://127.0.0.1:9000"
WORK=$(mktemp -d); trap 'rm -rf "$WORK"' EXIT

# 把 mc 的配置目录也隔离到 `$WORK` 里（`--config-dir` 是 mc 的全局参数）。
# 裸跑 `mc alias set` 会写开发机的 `~/.mc/config.json`：既会永久留下一个指向
# 一次性测试服务的别名，也可能覆盖掉使用者自己已有的别名——那是这个冒烟脚本
# 最不该有的副作用。全局参数必须写在子命令之前，所以包一层函数，别写成别名。
mc() { command mc --config-dir "$WORK/mc" "$@"; }

mc alias set rs "$EP" rustorage rustorage-secret
mc mb --ignore-existing rs/test-bucket
head -c 1048576 /dev/urandom > "$WORK/1m.bin"
mc cp "$WORK/1m.bin" rs/test-bucket/1m.bin
mc cat rs/test-bucket/1m.bin > "$WORK/roundtrip.bin"
cmp "$WORK/1m.bin" "$WORK/roundtrip.bin"
mc ls rs/test-bucket
mc rm rs/test-bucket/1m.bin
echo "mc smoke: OK"
