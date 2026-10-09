#!/usr/bin/env bash
# Task 6.4：端到端验收。
#
# 前置：Task 6.3（可执行服务）与 Task 5.9（tests/compat/*.sh）都已完成。
set -euo pipefail

# 前置检查：**挨个查工具名**。没有这一段时，缺 `aws` 会以「command not found」
# 失败、看起来像是服务端的问题；而本脚本第 2 步起自己直接调 `aws`，
# 另外还要 `curl`（轮询 /ready）与 `cmp`（**唯一**有诊断价值的比对）。
for tool in aws curl cmp; do
    command -v "$tool" >/dev/null || { echo "$tool 未安装，无法跑验收" >&2; exit 1; }
done

ENDPOINT=http://127.0.0.1:9000
ROOT=/tmp/rs
WORK=$(mktemp -d)          # 载荷与比对结果放服务端数据目录**之外**

# 本脚本自己直接调 `aws`（第 2 步起），所以**必须**在这里给凭据：
# `tests/compat/*.sh` 里的 export 是子进程，出了那个脚本就没了。
# 少了这三行，在有 ~/.aws/credentials 的开发机上能跑、在干净的 CI 上必然
# 「Unable to locate credentials」，而这跟服务端毫无关系。
# 这三个值必须与 Task 6.3 的默认参数（--access-key / --secret-key）一致。
export AWS_ACCESS_KEY_ID=rustorage
export AWS_SECRET_ACCESS_KEY=rustorage-secret
export AWS_DEFAULT_REGION=us-east-1
AWS="aws --endpoint-url $ENDPOINT"

# 启动 6 盘 4+2 实例（4 数据分片 + 2 校验分片 = 6 块盘）。
# **`--parity 2` 必须显式给**：6 块盘的默认 parity 是 3（见 Task 4.3 的 default_parity），
# 那样 read_quorum = 6-3 = 3，下面「掉 2 块」还剩 4 块，离边界很远，测不到那条边界。
# 给 2 之后 read_quorum = 6-2 = 4，掉 2 块恰好**只剩 4 块**——一步不多、一步不少，
# 这是纠删码最有价值的那个用例；而下面引用的「4+2」注释也才对得上。
#
# **每次都从空目录开始**，否则脚本不可重跑：上一次若在第 3 步中途失败（那时 d5/d6
# 已被 `mv` 成 `d5.off`/`d6.off`），残留目录会让下一次的 `mv $ROOT/d5 $ROOT/d5.off`
# 变成「把一个目录移进已存在的目录里」，`set -e` 于是在**服务启动之前**就退出，
# 报错还完全看不出是残留目录造成的。
rm -rf "$ROOT"
mkdir -p "$ROOT"/{d1,d2,d3,d4,d5,d6}
# **先单独构建，再起进程。** `cargo run` 会把冷构建时间算进下面那段轮询窗口里：
# 干净的 CI 上全工作区冷编译远超 30 秒，脚本会以「server did not become ready」
# 失败——而这跟服务端毫无关系。构建放在启动之前，轮询窗口就只覆盖真正的启动。
cargo build -p rstore-server
# **直接跑构建产物，不要 `cargo run`。** `cargo run` 后面还挂着一个 cargo 进程，
# `kill $SERVER_PID` 杀掉的是 cargo，真正的服务进程很可能继续占着 9000 端口——
# 下一次运行就会以「server did not become ready」失败，而端口是被上一次占着的。
# Windows 上产物名带 `.exe`，两种都试。
BIN=target/debug/rstore-server
[ -e "$BIN" ] || BIN="$BIN.exe"
[ -e "$BIN" ] || { echo "找不到构建产物 rstore-server" >&2; exit 1; }
"$BIN" --volumes "$ROOT"/d{1,2,3,4,5,6} --parity 2 --port 9000 &
SERVER_PID=$!
# 无论从哪一条 `set -e` 退出，都别把服务留在后台占着 9000，也别留下临时目录：
trap 'kill $SERVER_PID 2>/dev/null || true; rm -rf "$WORK"' EXIT

# 不要 `sleep 3`：慢机器上会假失败，快机器上白等。轮询 /ready 直到 200。
# (`curl … && break` 里 curl 不是 `&&` 列表中的最后一条，所以失败不会触发 errexit。)
# 120 次 × 0.5s = 60s：启动本身只是开 6 个目录 + 读 6 份 format.json，
# 60s 只有在文件系统卡死时才用得上。
for _ in $(seq 1 120); do
    curl -fsS -o /dev/null $ENDPOINT/ready && break
    sleep 0.5
done
curl -fsS -o /dev/null $ENDPOINT/ready || {
    echo "server did not become ready" >&2; exit 1
}

# 1. 客户端冒烟（三个脚本各自的 endpoint / 凭据 / 建桶约定见 tests/compat/）
bash tests/compat/aws_cli.sh
bash tests/compat/mc.sh
bash tests/compat/rclone.sh

# 2. 放一份**可逐字节比对**的载荷，作为容错验收的基准。
#    必须 < 8 MiB：MVP 不支持 multipart，aws-cli 超阈值会自动改走分片上传。
head -c 3000000 /dev/urandom > $WORK/payload.bin
#    建桶：已存在时 `mb` 会失败，所以吞掉它的退出码，别让 `set -e` 在这里把脚本带走。
$AWS s3 mb s3://accept 2>/dev/null || true
$AWS s3 cp $WORK/payload.bin s3://accept/big.bin

# 3. 容错：停掉两块盘 —— **用 `mv` 把盘目录挪走，不要用 `chmod 000`**。
#    本项目的开发与验收环境是 Windows（Git Bash），`chmod 000` 在那里是空操作：
#    脚本会一路绿灯，却一块盘都没停掉，于是这条容错验收等于没测。
#    `mv` 在两个平台都真的让路径消失，`LocalDisk` 会得到 NotFound/IO 错误，
#    正是「盘掉线」要模拟的东西。
mv $ROOT/d5 $ROOT/d5.off
mv $ROOT/d6 $ROOT/d6.off

#    4+2 掉 2 块，read_quorum = 4，读**必须**成功且**内容逐字节相同**。
#    原计划这一步只有一行「→ 读仍成功」的注释、没有任何命令——那等于什么都没测：
#    分片读错、解码错位、返回截断的数据，这条注释全都发现不了。
$AWS s3 cp s3://accept/big.bin $WORK/degraded.bin
cmp $WORK/payload.bin $WORK/degraded.bin

# 3b. 掉 2 块时**写**也必须成功：write_quorum(data=4, parity=2) = 4，
#     而此刻恰好剩 4 块——同样一步不多、一步不少。
#     （「掉 2 盘可写」是完成检查清单里的一条，原先这份脚本根本没有覆盖它。）
$AWS s3 cp $WORK/payload.bin s3://accept/write-while-degraded.bin
$AWS s3 cp s3://accept/write-while-degraded.bin $WORK/write-back.bin
cmp $WORK/payload.bin $WORK/write-back.bin

# 3c. 再掉一块（剩 3 < read_quorum 4）→ 读**必须失败**，而**绝不能**返回
#     残缺或错误的数据（P1：宁可报错，绝不返回错数据）。
#     用 `if cmd; then 失败; fi` 而不是 `cmd || true`：后者会把「本该失败却成功」
#     和「正常报错」一起吞掉，这条就等于没测。注意放在 `if` 条件里的命令
#     不会触发 `set -e`。
mv $ROOT/d4 $ROOT/d4.off
if $AWS s3 cp s3://accept/big.bin $WORK/too-far.bin 2>$WORK/too-far.err; then
    echo "低于 read_quorum 时读竟然成功了" >&2
    exit 1
fi
# **只断言「失败了」还不够**：服务整个挂掉、桶被删了、网络断了，都会让上面那条
# `if` 成立——那样这条边界验收就是**假通过**。必须断言失败的原因**就是**读 quorum
# 不足。Task 5.8 把 `StoreError::ReadQuorum` 映射成 503 `ServiceUnavailable`
# （wiring.rs 的 `map_object_err`），所以这里钉住它。
#
# **两个大版本打印的形态不同，只匹配一个就会在换版本时假失败**（本机 aws-cli
# v1.46.1 实测）：
#   v1：`fatal error: An error occurred (503) when calling the HeadObject operation
#        (reached max retries: 4): Service Unavailable`   ← 打的是**状态码**
#   v2：`An error occurred (ServiceUnavailable) when calling …`  ← 打的是**错误码**
# 顺带记住：`s3 cp` 下载前先发 `HeadObject`，所以 quorum 不足是在 **HEAD** 上就
# 暴露的，不是等到 GET——「读失败」发生在更早的一步。
grep -qE '\(503\)|ServiceUnavailable' $WORK/too-far.err || {
    echo "读确实失败了，但原因不是读 quorum 不足（期望 ServiceUnavailable）：" >&2
    cat $WORK/too-far.err >&2
    exit 1
}
mv $ROOT/d4.off $ROOT/d4

# 4. 恢复，再读一次确认恢复没把数据改坏（同一条比对，但走的是另一条盘路径）。
mv $ROOT/d5.off $ROOT/d5
mv $ROOT/d6.off $ROOT/d6
$AWS s3 cp s3://accept/big.bin $WORK/healed.bin
cmp $WORK/payload.bin $WORK/healed.bin

echo "ACCEPTANCE: OK"
