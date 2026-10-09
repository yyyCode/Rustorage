#!/usr/bin/env python3
"""scripts/check_layer_deps.py 的回归测试。

用 subprocess 走真实接口（stdin 喂 JSON，断言退出码与输出），
测的是守卫本身而不是它的复刻。

运行：python3 scripts/tests/test_check_layer_deps.py
"""
import contextlib
import io
import json
import os
import subprocess
import sys
import types
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

    # 路径依赖带着 path 字段，即使名字没有 rstore- 前缀也必须被约束。
    # 不能写成 pkg(...)：那个辅助函数只造 {"name": ...}，造不出 path 字段，
    # 而 path 字段正是这条用例要验的东西。
    ("路径依赖绕过前缀过滤", {"packages": [
        {"name": "rstore-server", "dependencies": [
            {"name": "evilhelper", "path": "../tools/evilhelper"}]},
    ]}, 1, "FORBIDDEN EDGE: rstore-server -> evilhelper"),
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


def run_fixture_cases():
    """执行全部 fixture 用例，返回失败描述列表。供 main() 与 pytest 共用。"""
    failures = []
    for label, payload, want_code, want_substr in CASES:
        proc = run_checker(payload)
        problems = []
        if proc.returncode != want_code:
            problems.append(f"退出码 {proc.returncode}，期望 {want_code}")
        if want_substr is not None and want_substr not in (proc.stdout or ""):
            problems.append(f"stdout 缺少 {want_substr!r}（实际 {proc.stdout!r}）")
        if want_substr is None and want_code == 0 and (proc.stdout or "").strip():
            problems.append(f"合规输入不该有 stdout 输出，却有 {proc.stdout!r}")
        # 每个用例**至多**记一条：main() 的通过计数按「用例数 - 失败数」算，
        # 一个用例记多条会让计数变成负数或虚高。
        if problems:
            failures.append(f"{label}: {'；'.join(problems)}")
    return failures


def test_fixture_cases():
    """pytest 入口。

    fixture 用例原本直接写在 main() 里，那样 `pytest` 只会收集到几个 test_*
    函数，子进程用例一条都不跑——测试看着全绿，实测只覆盖了一小部分。
    整进一个 test_* 函数后，两种跑法覆盖同一批用例。
    """
    failures = run_fixture_cases()
    assert not failures, "\n".join(failures)


def test_metadata_shape_errors_exit_2():
    """结构不符必须一律归 2，逐条钉住。

    这些形状在真实 cargo 输出里不该出现，所以它们的作用是"cargo 改格式时
    及时报警"。关键在于报警方式：必须报成"护栏故障"(2)，不能报成
    "发现违规"(1)——后者会让人去翻 manifest，而真正出问题的是护栏自己。

    第一版只 except 了 (KeyError, TypeError)，漏掉了 dep name 非字符串时
    `int.startswith` 抛的 AttributeError——它带着完整 traceback 以 1 逃逸。
    所以现在 check() 用 require() 显式断言结构，main() 再加 except Exception
    兜底，两层各自独立成立。
    """
    shapes = [
        {},                                                    # 没有 packages
        {"packages": None},                                    # 类型不对
        {"packages": []},                                      # 空 workspace
        {"packages": [{}]},                                    # 包没有 name
        {"packages": [{"name": 123}]},                         # name 不是字符串
        {"packages": [{"name": "rstore-common"}]},             # 没有 dependencies
        {"packages": [{"name": "rstore-common",
                       "dependencies": [{"name": 123}]}]},     # 依赖名不是字符串
        {"packages": [{"name": "rstore-common",
                       "dependencies": [123]}]},               # 依赖项不是对象
    ]
    for shape in shapes:
        proc = run_checker(shape)
        assert proc.returncode == 2, (
            f"{shape} 期望退出码 2，实际 {proc.returncode}；stderr={proc.stderr!r}"
        )
        assert (proc.stdout or "").strip() == "", (
            f"{shape} 的诊断不该出现在 stdout：{proc.stdout!r}"
        )


def test_invalid_json_reports_on_stderr_only():
    """退出码 2 的路径必须只写 stderr。

    诊断若混进 stdout，CI 里会被当成违规清单。
    """
    proc = run_checker(None)
    assert proc.returncode == 2, f"期望退出码 2，实际 {proc.returncode}"
    assert (proc.stdout or "").strip() == "", f"stdout 必须为空，实际 {proc.stdout!r}"
    assert "无法解析" in (proc.stderr or ""), f"stderr 缺少诊断：{proc.stderr!r}"


def test_non_closed_table_makes_main_exit_2():
    """白名单不自洽时 main 必须以 2 退出（护栏故障），而不是 1（发现违规）。

    这条分支在 table_errors 之后、读 stdin 之前，所以 stdin 内容无关紧要。

    stderr 必须捕获：main 会往那里打诊断，不拦的话自测跑绿也会在终端上
    印出一行 `ERROR: 白名单引用了未登记的 crate`，看着像失败。
    """
    original_table, original_stdin = chk.ALLOWED, sys.stdin
    chk.ALLOWED = {"rstore-api": {"rstore-typo"}}
    sys.stdin = types.SimpleNamespace(buffer=io.BytesIO(b""))
    try:
        with contextlib.redirect_stderr(io.StringIO()) as err:
            code = chk.main()
    finally:
        chk.ALLOWED = original_table
        sys.stdin = original_stdin
    assert code == 2, f"期望退出码 2，实际 {code}"
    assert "rstore-typo" in err.getvalue(), f"stderr 应指出违规项：{err.getvalue()!r}"


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
    failures = run_fixture_cases()

    # 按 test_ 前缀**自动发现**，不手写清单。脚本方式才是 CI 的入口，手写清单
    # 漏掉一个测试就等于 CI 静默跳过它——而"被跳过"和"通过"在日志里长得一模
    # 一样，这正是最难发现的失败。skip 里只放那些已知会被重复执行的包装函数。
    #
    # test_fixture_cases 只是 run_fixture_cases 的 pytest 包装，放进来会把同一批
    # fixture 用例跑两遍、并让计数重复。它是唯一需要排除的。
    skip = {"test_fixture_cases"}
    unit_tests = [
        fn for nm, fn in sorted(globals().items())
        if nm.startswith("test_") and callable(fn) and nm not in skip
    ]
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
