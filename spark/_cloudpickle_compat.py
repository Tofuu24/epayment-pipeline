"""Make PySpark use a modern cloudpickle (import this FIRST in any job that ships
Python code to executors: Python UDFs, applyInPandas / applyInPandasWithState).

Why this exists
---------------
PySpark 3.5.x vendors its own cloudpickle under ``pyspark.cloudpickle``. That
bundled copy hits a ``RecursionError: maximum recursion depth exceeded`` when it
tries to pickle *any* function on modern CPython (3.12+) — even ``lambda x: x``.
That breaks every Python UDF and every ``applyInPandas*`` call. It is NOT a bug
in our code and it is NOT "cloudpickle is fundamentally broken": standalone
cloudpickle >= 3.1 fixed it. We just have to make PySpark use that newer copy.

This is the single, shared fix for the whole pipeline. Job C needs it because
its settlement state machine (``applyInPandasWithState``) must serialize a Python
function — there is no pure-SQL equivalent for arbitrary per-key, event-time
stateful processing. (Job B avoids the issue differently, by expressing its
holiday/window logic as native Spark SQL in ``sql_udfs.py``; both are valid.)

Resolution order
----------------
1. A pip-installed ``cloudpickle`` (preferred — ``cloudpickle>=3.1`` is in
   requirements.txt; install it into the interpreter Spark uses via PYSPARK_PYTHON).
2. Otherwise a vendored copy at ``<repo>/cloudpickle_pkg`` (path derived from
   this file, never hardcoded), for offline use.

If neither yields a usable cloudpickle, PySpark's bundled copy is left in place
and a warning is emitted, so a missing dependency fails loudly rather than
silently reintroducing the recursion crash.
"""
import os
import sys
import warnings

_MIN = (3, 1)


def _version_tuple(mod):
    try:
        return tuple(int(p) for p in mod.__version__.split(".")[:2])
    except Exception:
        return (0, 0)


def ensure_modern_cloudpickle():
    """Point ``pyspark.cloudpickle`` at a standalone cloudpickle >= 3.1. Idempotent."""
    try:
        import cloudpickle
    except ModuleNotFoundError:
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        vendored = os.path.join(repo_root, "cloudpickle_pkg")
        if os.path.isdir(vendored):
            sys.path.insert(0, vendored)
            try:
                import cloudpickle  # noqa: F811
            except ModuleNotFoundError:
                cloudpickle = None
        else:
            cloudpickle = None

    if cloudpickle is None or _version_tuple(cloudpickle) < _MIN:
        warnings.warn(
            "No standalone cloudpickle >= 3.1 found; falling back to PySpark's "
            "bundled copy. Python UDFs / applyInPandas* may crash with "
            "RecursionError on Python 3.12+. Install cloudpickle>=3.1 into the "
            "interpreter Spark uses (PYSPARK_PYTHON).",
            RuntimeWarning,
        )
        return

    import pyspark.cloudpickle
    for attr in ("CloudPickler", "dumps", "dump", "loads", "load"):
        setattr(pyspark.cloudpickle, attr, getattr(cloudpickle, attr))


ensure_modern_cloudpickle()
