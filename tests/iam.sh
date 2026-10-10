#!/usr/bin/env bash
# IAM 端到端验收（设计 §8.3）。
#
# 起一个**真实服务**，用盘上的 IAM 配置打真实的 S3 请求。单元测试已经覆盖了
# 求值语义（`crates/iam/src/store.rs`），这里要的是那些测试看不见的东西：
# 目录布局、`--iam-dir` 的默认值、s3s 那侧的认证/授权接线，以及**匿名请求真的被拒**。
set -euo pipefail

# 前置检查：挨个查工具名。`aws` 是本脚本的主体，`curl` 有两处独立用途——
# 轮询 /ready，以及打那条**不带 Authorization 的裸请求**（aws 总会签名，测不到匿名）。
for tool in aws curl; do
    command -v "$tool" >/dev/null || { echo "$tool 未安装，无法跑 IAM 验收" >&2; exit 1; }
done

ROOT=/tmp/rs-iam
WORK=$(mktemp -d)              # 载荷放服务端数据目录**之外**
ENDPOINT=http://127.0.0.1:9101 # 换个端口，免得和 acceptance.sh 的 9000 撞车
REGION=us-east-1
# **故意不给 `--iam-dir`**：这里走的是默认路径 `<volumes[0]>/.rstore/iam`，
# 于是「默认值拼错了」也会被这条验收逮住。
IAM="$ROOT/d1/.rstore/iam"

# 服务端 root 凭据（与 config.rs 的默认值一致）和三个用户的凭据分开，
# 切换身份靠这三个环境变量。
ROOT_AK=rustorage
ROOT_SK=rustorage-secret
ALICE_AK=alice
ALICE_SK=alice-secret
CAROL_AK=carol
CAROL_SK=carol-secret
BOB_AK=bob
BOB_SK=bob-secret

export AWS_DEFAULT_REGION=$REGION
# 本脚本自己直接调 `aws`，所以**必须**显式给凭据：开发机上若存在
# ~/.aws/credentials，「该 403 的请求 200 了」这种失败看起来会像是服务端的问题。
AWS="aws --endpoint-url $ENDPOINT"

as_root()  { export AWS_ACCESS_KEY_ID=$ROOT_AK  AWS_SECRET_ACCESS_KEY=$ROOT_SK; }
as_alice() { export AWS_ACCESS_KEY_ID=$ALICE_AK AWS_SECRET_ACCESS_KEY=$ALICE_SK; }
as_carol() { export AWS_ACCESS_KEY_ID=$CAROL_AK AWS_SECRET_ACCESS_KEY=$CAROL_SK; }
as_bob()   { export AWS_ACCESS_KEY_ID=$BOB_AK   AWS_SECRET_ACCESS_KEY=$BOB_SK; }

# 每次都从空目录开始，否则脚本不可重跑（残留的 IAM 目录会让下一次读到上一次的策略）。
rm -rf "$ROOT"
mkdir -p "$ROOT"/{d1,d2,d3,d4,d5,d6}
mkdir -p "$IAM/users" "$IAM/policies"

# 文件名（去掉 .json）就是 access key——没有单独的「用户名」字段，
# 免得两处不一致时还得猜哪个说了算。
#
# alice：只读 photos/ 下的对象。
cat >"$IAM/users/alice.json" <<'JSON'
{"secret_key": "alice-secret", "status": "enabled", "policies": ["readonly"]}
JSON
cat >"$IAM/policies/readonly.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::photos/*"]}
  ]
}
JSON

# carol：photos/ 下全权，**但** locked/ 子前缀明确禁止删除。
# 这一份是「显式拒绝优先」的唯一端到端证据：同一个对象上 Allow 与 Deny 同时命中时，
# Deny 必须赢。只写 Allow 的策略测不出这条规则——那样 Deny 从来没被求值过。
cat >"$IAM/users/carol.json" <<'JSON'
{"secret_key": "carol-secret", "status": "enabled", "policies": ["rw-with-lock"]}
JSON
cat >"$IAM/policies/rw-with-lock.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:*"], "Resource": ["arn:aws:s3:::photos/*"]},
    {"Effect": "Deny",  "Action": ["s3:DeleteObject"], "Resource": ["arn:aws:s3:::photos/locked/*"]}
  ]
}
JSON

# bob 被停用：签名能算出来，但**认证阶段**就该被拒——连授权都不会走到。
# `IamStore::secret_key` 对停用用户返回 None，于是它与「这个 key 不存在」完全同形。
cat >"$IAM/users/bob.json" <<'JSON'
{"secret_key": "bob-secret", "status": "disabled", "policies": ["readonly"]}
JSON

# 先单独构建，再起进程：`cargo run` 会把冷构建时间算进下面那段轮询窗口里。
cargo build -p rstore-server
BIN=target/debug/rstore-server
[ -e "$BIN" ] || BIN="$BIN.exe"
[ -e "$BIN" ] || { echo "找不到构建产物 rstore-server" >&2; exit 1; }

# 6 盘 4+2（`--parity 2` 与 acceptance.sh 同款）。IAM 不影响纠删码，但这条链路
# 上任何一环断了都会让下面的断言失败，而我们希望失败来自 IAM。
"$BIN" --volumes "$ROOT"/d{1,2,3,4,5,6} --parity 2 --port 9101 &
SERVER_PID=$!
trap 'kill $SERVER_PID 2>/dev/null || true; rm -rf "$WORK"' EXIT

# 轮询 /ready，不要 sleep（慢机器上会假失败，快机器上白等）。
for _ in $(seq 1 120); do
    curl -fsS -o /dev/null "$ENDPOINT/ready" && break
    sleep 0.5
done
curl -fsS -o /dev/null "$ENDPOINT/ready" || {
    echo "server did not become ready" >&2; exit 1
}

PASS=0
FAIL=0

# 用退出码判成败：`aws` 失败时 `--output text` 拿不到东西，而我们要的判据就是
# 「这条命令成功了吗」。
#
# 只吞 **stdout**（成功的 `s3api` 会打一坨 JSON，读起来全是噪声），**stderr 留着**：
# 失败时那行错误信息是唯一能区分「403」和「服务挂了」的东西。
expect_ok() {   # expect_ok <描述> <命令...>
    local desc=$1; shift
    if "$@" >/dev/null; then
        echo "  ok   $desc"; PASS=$((PASS + 1))
    else
        echo "  FAIL $desc（期望成功）"; FAIL=$((FAIL + 1))
    fi
}
expect_denied() {  # expect_denied <描述> <命令...>
    local desc=$1; shift
    if "$@" >/dev/null; then
        echo "  FAIL $desc（期望被拒，却成功了）"; FAIL=$((FAIL + 1))
    else
        echo "  ok   $desc"; PASS=$((PASS + 1))
    fi
}

echo "== root（不受策略约束）=="
as_root
expect_ok "root 建桶"        $AWS s3api create-bucket --bucket photos
printf 'hello iam\n' >"$WORK/payload"
expect_ok "root 上传对象"    $AWS s3api put-object --bucket photos --key a/b.txt --body "$WORK/payload"
expect_ok "root 读回对象"    $AWS s3api get-object --bucket photos --key a/b.txt "$WORK/back"
expect_ok "root 列桶"        $AWS s3api list-buckets

echo "== alice（只读 photos/*）=="
as_alice
expect_ok     "alice 读对象"          $AWS s3api get-object --bucket photos --key a/b.txt "$WORK/alice-back"
expect_denied "alice 写对象"          $AWS s3api put-object --bucket photos --key c.txt --body "$WORK/payload"
expect_denied "alice 列桶"            $AWS s3api list-buckets
expect_denied "alice 建桶"            $AWS s3api create-bucket --bucket other

echo "== carol（photos/ 全权，locked/ 禁删）=="
as_root
# 先备好两个待删对象：都在 photos/ 下，区别只在锁定前缀。
expect_ok "root 备好待删对象"  $AWS s3api put-object --bucket photos --key deletable.txt --body "$WORK/payload"
expect_ok "root 备好锁定对象"  $AWS s3api put-object --bucket photos --key locked/x.txt --body "$WORK/payload"
as_carol
# 这两条**成对**才有意义：只测一边的话，「两条都被拒」和「两条都放行」都能让一边变绿，
# 而我们要的恰恰是它们**不同**——那才是 Deny 赢了 Allow 的证据。
expect_ok     "carol 删除普通对象"  $AWS s3api delete-object --bucket photos --key deletable.txt
expect_denied "carol 删除锁定对象"  $AWS s3api delete-object --bucket photos --key locked/x.txt

echo "== 匿名 =="
# 不带 Authorization 的裸请求。`aws` 总会签名，所以这里只能用 curl。
# `/photos/a/b.txt` 是对象路径，s3s 解析得动，于是请求会走到授权那一步。
#
# **这条是本脚本最要紧的一条**：装上 `set_access` 之后 s3s 的 `default_check`
# 不再兜底（设计 §1.3），匿名请求的安全性完全落在 `IamAccess::check` 上。
# 它返回 200 就意味着服务变成了公开存储桶。
code=$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/photos/a/b.txt")
if [ "$code" = "403" ]; then
    echo "  ok   匿名直连被拒（403）"; PASS=$((PASS + 1))
else
    echo "  FAIL 匿名直连应 403，实际 $code"; FAIL=$((FAIL + 1))
fi

echo "== bob（停用）=="
as_bob
expect_denied "停用用户读对象" $AWS s3api get-object --bucket photos --key a/b.txt "$WORK/bob-back"

echo
echo "IAM ACCEPTANCE: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] || exit 1
echo "IAM ACCEPTANCE: OK"
