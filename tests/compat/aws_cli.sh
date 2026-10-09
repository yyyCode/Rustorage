#!/usr/bin/env bash
# Task 5.9 冒烟测试：aws-cli（本机 v1.46.1）。
#
# 被测行为：aws-cli 的 s3 cp 对超过 8 MiB 的文件会自动改走 multipart 上传，
# 而 MVP 对 multipart 返回 501。因此载荷刻意取 1 MiB——对 aws/mc/rclone 三者
# 都落在单次 PUT 范围内。不要调大。
set -euo pipefail
command -v aws >/dev/null || { echo "aws CLI 未安装" >&2; exit 1; }

export AWS_ACCESS_KEY_ID=rustorage
export AWS_SECRET_ACCESS_KEY=rustorage-secret
export AWS_DEFAULT_REGION=us-east-1
EP="http://127.0.0.1:9000"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# `mb` 在桶已存在时会非零退出；我们的 CreateBucket 是幂等的（返回 200），
# 但客户端自己也可能先行报错——吞掉退出码，别让 `set -e` 在这里把脚本带走。
aws --endpoint-url "$EP" s3 mb s3://test-bucket || true

head -c 1048576 /dev/urandom > "$WORK/1m.bin"
aws --endpoint-url "$EP" s3 cp "$WORK/1m.bin" s3://test-bucket/1m.bin
aws --endpoint-url "$EP" s3 cp s3://test-bucket/1m.bin "$WORK/roundtrip.bin"
cmp "$WORK/1m.bin" "$WORK/roundtrip.bin"

aws --endpoint-url "$EP" s3api list-objects-v2 --bucket test-bucket --prefix "" --max-keys 1
aws --endpoint-url "$EP" s3api list-objects-v2 --bucket test-bucket --delimiter "/"
echo "aws-cli smoke: OK"
