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


class MetadataShapeError(Exception):
    """cargo metadata 的结构和预期不符。

    单独定义一个类型，是为了让 main() 能精确接住"输入不是我们以为的东西"。
    靠枚举 KeyError/TypeError/AttributeError……是在猜自己已知的坏法，而契约
    要求的是**任何**坏法都归 2。check() 里所有结构断言统一抛它，main() 里再
    加一层 except Exception 兜底，这样"意外异常以 1 逃逸"在结构上不可能发生。
    """


def require(cond, what):
    """结构断言。不成立就抛 MetadataShapeError，由 main() 转成退出码 2。"""
    if not cond:
        raise MetadataShapeError(what)


def check(meta):
    """返回违规消息列表。空列表表示合规。

    结构不符时抛 MetadataShapeError，而不是返回违规——「cargo 的输出看不懂」
    和「架构违规」必须分开报，前者是护栏故障（2），后者才是发现违规（1）。

    路径依赖（带 path 字段）一律按下内部依赖约束：这类依赖必然来自本仓库或
    本地目录，名字不带 rstore- 并不代表它不在图里。只按前缀过滤的话，一个放在
    crates/ 之外、名字又没前缀的内部 crate 会同时漏掉 UNKNOWN CRATE 和这条边。
    """
    require(isinstance(meta, dict), f"顶层不是对象：{type(meta).__name__}")
    packages = meta.get("packages")
    require(isinstance(packages, list), f"packages 不是列表：{type(packages).__name__}")
    require(packages, "packages 为空——workspace 里应当有 crate")

    violations = []
    for pkg in packages:
        require(isinstance(pkg, dict), "packages 的元素不是对象")
        name = pkg.get("name")
        require(isinstance(name, str), f"包的 name 不是字符串：{name!r}")

        if name not in ALLOWED:
            violations.append(
                f"UNKNOWN CRATE: {name} 未在护栏表中登记 —— 新增 crate 必须显式登记其允许依赖"
            )
            continue

        deps = pkg.get("dependencies")
        require(isinstance(deps, list), f"{name} 的 dependencies 不是列表")
        for dep in deps:
            require(isinstance(dep, dict), f"{name} 的依赖项不是对象")
            dep_name = dep.get("name")
            require(isinstance(dep_name, str), f"{name} 的依赖名不是字符串：{dep_name!r}")

            if dep.get("path") is None and not dep_name.startswith("rstore-"):
                continue                      # 注册表依赖：不受内部层次约束
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

    # 明确按 UTF-8 解码，不用 sys.stdin。JSON 规范即要求 UTF-8，cargo 也按 UTF-8
    # 输出；而 sys.stdin 用的是**区域编码**（Windows 上是 GBK）。依赖它意味着
    # 解码行为随环境漂移，两种结果都是坏的：
    #   - 解不开：checkout 路径里出现「一」(U+4E00) 这类常见汉字，GBK 就会在
    #     E4 B8 80 的尾字节 0x80 上抛 UnicodeDecodeError；
    #   - 解得开：字节碰巧凑成合法 GBK 对，于是静默乱码。当前 check() 只读
    #     name/dependencies（纯 ASCII），乱码落在 manifest_path 之类字段上时
    #     没有可见症状——但「护栏读到的输入随本机区域设置漂移」本身就是缺陷，
    #     哪天多读一个字段就会变成误报或漏报。
    # 前者以退出码 1 结束，正是最容易被误读成「发现违规」的那种失败。
    # CI 在 Linux 上永远是 UTF-8，这类问题只在本机出现，最难发现。
    try:
        meta = json.loads(sys.stdin.buffer.read().decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        print(f"ERROR: 无法解析 cargo metadata 输出：{e}", file=sys.stderr)
        return 2

    # 合法 JSON 不等于预期的结构。cargo 若改了输出格式，这里必须落回 2
    # （护栏故障），而不是让异常冒出去变成 1。
    #
    # 第二层 except Exception 是兜底，不是冗余：check() 内部已经用 require()
    # 显式断言了结构，但那段代码将来会长出新的字段访问。契约是"除真正的违规
    # 之外一律不返回 1"，能表达这个契约的只有"接住一切"——枚举异常类型永远
    # 慢一步，而漏掉的那种会以"发现违规"的假象出现在 CI 里。
    try:
        violations = check(meta)
    except MetadataShapeError as e:
        print(f"ERROR: cargo metadata 结构不符合预期：{e}", file=sys.stderr)
        return 2
    except Exception as e:                    # noqa: BLE001
        print(f"ERROR: 检查元数据时发生意外异常：{type(e).__name__}: {e}", file=sys.stderr)
        return 2

    for v in violations:
        print(v)
    return 1 if violations else 0


if __name__ == "__main__":
    sys.exit(main())
