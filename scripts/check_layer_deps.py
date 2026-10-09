#!/usr/bin/env python3
"""校验 DESIGN §5 的依赖方向规则（R1–R4）。

从 stdin 读 `cargo metadata --format-version 1 --no-deps` 的 JSON。

退出码：
  0  合规
  1  发现违规（未登记 crate，或跨层边）
  2  护栏自身无法工作（输入不是合法 JSON，或白名单未按传递闭包补齐）
     必须与 1 分开，否则 CI 日志会把基础设施故障读成「有人加了违规依赖」。
"""
import json
import sys

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


def table_errors(table):
    """白名单必须自洽：按传递闭包补齐，且不引用未登记的名字。

    闭包：否则 `api → X → store` 这类绕道违规会溜过去——api 只直接依赖 X，
    看似合规，但 X 依赖 store，实际传递依赖已经越界。

    悬空引用：`ALLOWED[c]` 里出现了表中不存在条目。正常表里每个内部 crate
    都是键，所以这多半是拼写错误（`rstore-meta` 写成 `rstore-metaa`）。
    这种情况绝不能静默当叶子节点放过——那等于把一条依赖边从图上抹掉，
    护栏会转而"证明"一个不存在的结论。

    返回错误列表，空列表表示自洽。这里刻意不抛异常：任何未捕获的异常都会以
    退出码 1 结束，而 1 在本脚本里表示"发现违规"，正好是 docstring 警告的
    那种混淆。
    """
    errors = []
    for crate, deps in table.items():
        for reachable in deps:
            if reachable not in table:
                errors.append(
                    f"白名单引用了未登记的 crate：{crate} → {reachable}；"
                    f"表里没有 {reachable} 这一项（拼写错误？）"
                )
                continue
            extra = table[reachable] - deps
            if extra:
                errors.append(
                    f"白名单未闭包：{crate} → {reachable} → {sorted(extra)}；"
                    f"应把 {sorted(extra)} 并入 {crate} 的允许集合"
                )
    return errors


def check(meta):
    """返回违规消息列表。空列表表示合规。"""
    violations = []
    for pkg in meta["packages"]:
        name = pkg["name"]
        if name not in ALLOWED:
            violations.append(
                f"UNKNOWN CRATE: {name} 未在护栏表中登记 —— 新增 crate 必须显式登记其允许依赖"
            )
            continue
        for dep in pkg["dependencies"]:
            dep_name = dep["name"]
            if not dep_name.startswith("rstore-"):
                continue                      # 只约束内部 crate
            if dep_name not in ALLOWED[name]:
                kind = dep.get("kind") or "normal"
                violations.append(f"FORBIDDEN EDGE: {name} -> {dep_name}  (kind={kind})")
    return violations


def main():
    errors = table_errors(ALLOWED)
    if errors:
        for e in errors:
            print(f"ERROR: {e}", file=sys.stderr)
        return 2

    try:
        meta = json.load(sys.stdin)
    except json.JSONDecodeError as e:
        print(f"ERROR: 无法解析 cargo metadata 输出：{e}", file=sys.stderr)
        return 2

    violations = check(meta)
    for v in violations:
        print(v)
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
