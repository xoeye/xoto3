"""Canary check for xoto3.lam.finalize against the installed awslambdaric.

Run on a schedule (see .github/workflows/canary.yml) with the *latest*
awslambdaric installed. It verifies that finalize actually installs its
post-invocation hook onto the real runtime client. If a new awslambdaric major
ships that finalize does not yet handle, the hook is not installed, the thunk
never fires, and this script exits non-zero -- signalling that
xoto3/lam/finalize.py needs a new supported code path.

This deliberately exercises the *real* awslambdaric wheel, complementing the
hermetic, synthetic-package unit tests in tests/xoto3/lam/finalize_test.py.
"""

import os

# finalize only installs hooks in a Lambda-like environment; make is_aws_env()
# true before importing the module (its one-time setup runs at import).
os.environ.setdefault("AWS_EXECUTION_ENV", "canary")

import awslambdaric  # noqa: E402
from awslambdaric import bootstrap  # noqa: E402

import xoto3.lam.finalize as finalize  # noqa: E402

version = awslambdaric.__version__
major = version.split(".")[0]

fired: list = []
finalize.register_lambda_finalize_thunk(lambda: fired.append(True))

# bootstrap.LambdaRuntimeClient exists in every major; its post_invocation_result
# resolves to the finalize-wrapped method either directly (2.x/3.x) or via the
# patched BaseLambdaRuntimeClient (4.x). Construction opens no connection.
client = bootstrap.LambdaRuntimeClient("127.0.0.1:9001")
try:
    client.post_invocation_result("canary-request-id", "{}", "application/json")
except Exception:
    # The wrapped original then calls into the absent native runtime client;
    # we only care that our finalize thunk ran first.
    pass

if not fired:
    raise SystemExit(
        f"FINALIZE CANARY FAILED: awslambdaric {version} (major {major}) is installed, "
        f"but xoto3.lam.finalize did not install its post-invocation hook. "
        f"A new major likely needs a supported code path in xoto3/lam/finalize.py."
    )

print(f"OK: finalize hooks installed against awslambdaric {version} (major {major}).")
