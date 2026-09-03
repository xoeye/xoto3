"""Tests for xoto3.lam.finalize's awslambdaric version-aware hook installation.

finalize.py installs its post-invocation hooks at import time, keyed off the
installed awslambdaric major version. To exercise each supported version
independently (and avoid the module-load-time state that would otherwise make
these order-dependent), every scenario runs in a fresh interpreter against a
synthetic ``awslambdaric`` package placed on ``PYTHONPATH``.
"""

import os
import subprocess
import sys
import textwrap
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

_FOUR_X_CLIENT = """
class BaseLambdaRuntimeClient:
    def post_invocation_result(self, *a, **k):
        return ("orig_result", a, k)
    def post_invocation_error(self, *a, **k):
        return ("orig_error", a, k)
class LambdaRuntimeClient(BaseLambdaRuntimeClient):
    pass
class LambdaMultiConcurrentRuntimeClient(BaseLambdaRuntimeClient):
    pass
"""

_LEGACY_CLIENT = """
class LambdaRuntimeClient:
    def post_invocation_result(self, *a, **k):
        return ("orig_result", a, k)
    def post_invocation_error(self, *a, **k):
        return ("orig_error", a, k)
"""


def _write_fake_awslambdaric(base: Path, version: str, four_x: bool) -> None:
    pkg = base / "awslambdaric"
    pkg.mkdir(parents=True)
    (pkg / "__init__.py").write_text(f'__version__ = "{version}"\n')
    (pkg / "lambda_runtime_client.py").write_text(_FOUR_X_CLIENT if four_x else _LEGACY_CLIENT)
    (pkg / "bootstrap.py").write_text("from .lambda_runtime_client import LambdaRuntimeClient\n")


def _run_driver(base: Path, driver: str) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "AWS_EXECUTION_ENV": "AWS_Lambda_python3.12",
        "PYTHONPATH": os.pathsep.join([str(base), str(REPO_ROOT)]),
    }
    return subprocess.run(
        [sys.executable, "-c", driver],
        env=env,
        cwd=str(base),
        capture_output=True,
        text=True,
    )


_LEGACY_DRIVER = textwrap.dedent(
    """
    calls = []
    import xoto3.lam.finalize as F
    F.register_lambda_finalize_thunk(lambda: calls.append("thunk"))
    from awslambdaric.lambda_runtime_client import LambdaRuntimeClient

    inst = LambdaRuntimeClient()
    r = inst.post_invocation_result("id", "data", "ct")
    assert calls == ["thunk"], calls
    assert r[0] == "orig_result", r
    calls.clear()
    e = inst.post_invocation_error("id", "err", "xray")
    assert calls == ["thunk"], calls
    assert e[0] == "orig_error", e
    print("OK")
    """
)

_FOUR_X_DRIVER = textwrap.dedent(
    """
    calls = []
    import xoto3.lam.finalize as F
    F.register_lambda_finalize_thunk(lambda: calls.append("thunk"))
    from awslambdaric.lambda_runtime_client import (
        LambdaRuntimeClient,
        LambdaMultiConcurrentRuntimeClient,
    )

    # 4.x passes an extra invocation_id positional; the wrapper is signature-agnostic.
    r = LambdaRuntimeClient().post_invocation_result("id", "data", "ct", "iid")
    assert calls == ["thunk"], calls
    assert r[0] == "orig_result", r
    calls.clear()
    e = LambdaRuntimeClient().post_invocation_error("id", "err", "xray", "iid")
    assert calls == ["thunk"], calls
    assert e[0] == "orig_error", e
    # Multi-concurrent mode uses a sibling client; patching the shared base must cover it.
    calls.clear()
    LambdaMultiConcurrentRuntimeClient().post_invocation_result("id", "data", "ct", "iid")
    assert calls == ["thunk"], calls
    print("OK")
    """
)


def test_hooks_fire_for_awslambdaric_2x(tmp_path):
    _write_fake_awslambdaric(tmp_path, "2.2.1", four_x=False)
    p = _run_driver(tmp_path, _LEGACY_DRIVER)
    assert p.returncode == 0, p.stderr
    assert p.stdout.strip() == "OK"


def test_hooks_fire_for_awslambdaric_3x(tmp_path):
    _write_fake_awslambdaric(tmp_path, "3.1.1", four_x=False)
    p = _run_driver(tmp_path, _LEGACY_DRIVER)
    assert p.returncode == 0, p.stderr
    assert p.stdout.strip() == "OK"


def test_hooks_fire_for_awslambdaric_4x_standard_and_multiconcurrent(tmp_path):
    _write_fake_awslambdaric(tmp_path, "4.0.2", four_x=True)
    p = _run_driver(tmp_path, _FOUR_X_DRIVER)
    assert p.returncode == 0, p.stderr
    assert p.stdout.strip() == "OK"


def test_unsupported_major_version_logs_error_and_does_not_patch(tmp_path):
    _write_fake_awslambdaric(tmp_path, "5.0.0", four_x=False)
    driver = textwrap.dedent(
        """
        import io, logging
        buf = io.StringIO()
        logging.basicConfig(stream=buf, level=logging.ERROR)
        calls = []
        import xoto3.lam.finalize as F
        F.register_lambda_finalize_thunk(lambda: calls.append("thunk"))
        from awslambdaric.lambda_runtime_client import LambdaRuntimeClient

        assert LambdaRuntimeClient().post_invocation_result() == ("orig_result", (), {})
        assert calls == [], "thunk must not fire on unsupported version"
        assert "Unimplemented" in buf.getvalue(), buf.getvalue()
        print("OK")
        """
    )
    p = _run_driver(tmp_path, driver)
    assert p.returncode == 0, p.stderr
    assert p.stdout.strip() == "OK"
