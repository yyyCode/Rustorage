#!/usr/bin/env python3
"""scripts/check_layer_deps.py 的回归测试。

用 subprocess 走真实接口（stdin 喂 JSON，断言退出码与输出），
测的是守卫本身而不是它的复刻。

运行：python3 scripts/tests/test_check_layer_deps.py
"""
import json
import os
import subprocess
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent
CHECKER = SCRIPTS_DIR / "check_layer_deps.py"

sys.path.insert(0, str(SCRIPTS_DIR))
import check_layer_deps as chk  # noqa: E402


def pkg(name, *deps):
    return {"name": name, "dependencies": [{"name": d} for d in deps]}


# (用例名, stdin 载荷, 期望退出码, 期望出现在 stdout 的片段)
CASES = [
    ("合规输入", {"packages": [
        pkg("rstore-common"),
        pkg("rstore-checksum", "rstore-common"),
        pkg("rstore-api", "rstore-common"),
    ]}, 0, None),

    ("非法边 api -> store", {"packages": [
        pkg("rstore-api", "rstore-store"),
    ]}, 1, "FORBIDDEN EDGE: rstore-api -> rstore-store  (kind=normal)"),

    ("非法边 s3-compat -> store", {"packages": [
        pkg("rstore-s3-compat", "rstore-store"),
    ]}, 1, "FORBIDDEN EDGE: rstore-s3-compat -> rstore-store  (kind=normal)"),

    ("未登记 crate", {"packages": [
        pkg("rstore-scratch"),
    ]}, 1, "UNKNOWN CRATE: rstore-scratch"),

    ("外部依赖不算违规", {"packages": [
        pkg("rstore-common", "serde", "bytes", "tokio"),
    ]}, 0, None),

    ("未登记 crate 也要继续查其余包", {"packages": [
        pkg("rstore-scratch"),
        pkg("rstore-api", "rstore-store"),
    ]}, 1, "FORBIDDEN EDGE: rstore-api -> rstore-store"),

    ("输入不是合法 JSON", None, 2, None),
]


def run_checker(payload):
    """跑一次检查器，返回 CompletedProcess。

    编码必须两端都钉死成 UTF-8。`text=True` 只让 Python 用**区域编码**解码子进程
    输出——Windows 上是 GBK。子进程若按另一种编码写，父进程解码失败后
    `proc.stdout` 会静默变成 `None`，测试随即以 `TypeError: argument of type
    'NoneType' is not iterable` 崩掉，看不出真正原因。这里的断言比对的是中文消息，
    所以编码必须确定，而不是碰巧两边一致。
    """
    stdin = "" if payload is None else json.dumps(payload)
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    return subprocess.run(
        [sys.executable, str(CHECKER)],
        input=stdin, capture_output=True, text=True,
        encoding="utf-8", env=env,
    )


def test_real_table_is_closed():
    """真实白名单必须自洽——这是「绕道违规不可能」这条论证的全部依据。"""
    assert chk.table_errors(chk.ALLOWED) == []


def test_closure_check_catches_non_closed_table():
    """未闭包的表必须被检出，否则检查器形同虚设。

    api → mid → store 是一条绕道：api 只直接依赖 mid，但 mid 依赖 store，
    实际传递依赖已经越界。三个名字都在表中登记，所以这是纯粹的闭包违规，
    不掺杂悬空引用。
    """
    bad = {
        "rstore-api": {"rstore-mid"},
        "rstore-mid": {"rstore-store"},
        "rstore-store": set(),
    }
    assert chk.table_errors(bad) != []


def test_dangling_reference_is_a_table_error():
    """允许集合里出现表中没有的名字，必须报错，既不能崩也不能放过。

    崩（KeyError）会以退出码 1 结束，被误读成「发现违规」；
    静默当叶子节点放过，等于把一条依赖边从图上抹掉，
    护栏会转而"证明"一个不存在的结论。
    """
    bad = {"rstore-api": {"rstore-typo"}}
    errs = chk.table_errors(bad)
    assert errs != [], "悬空引用必须被检出"
    assert any("rstore-typo" in e for e in errs), f"错误信息应指出该名字：{errs}"


def main():
    failures = []

    for label, payload, want_code, want_substr in CASES:
        proc = run_checker(payload)
        problems = []
        if proc.returncode != want_code:
            problems.append(f"退出码 {proc.returncode}，期望 {want_code}")
        if want_substr is not None and want_substr not in proc.stdout:
            problems.append(f"stdout 缺少 {want_substr!r}（实际 {proc.stdout!r}）")
        if want_substr is None and want_code == 0 and proc.stdout.strip():
            problems.append(f"合规输入不该有 stdout 输出，却有 {proc.stdout!r}")
        failures += [f"{label}: {p}" for p in problems]

    unit_tests = (
        test_real_table_is_closed,
        test_closure_check_catches_non_closed_table,
        test_dangling_reference_is_a_table_error,
    )
    for fn in unit_tests:
        try:
            fn()
        except AssertionError as e:
            failures.append(f"{fn.__name__}: {e}")

    for f in failures:
        print(f"FAIL {f}")
    print(f"\n{len(CASES) + len(unit_tests) - len(failures)} 项通过，{len(failures)} 项失败")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
