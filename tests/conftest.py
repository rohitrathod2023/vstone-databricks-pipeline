"""
Auto-configures what PySpark needs to start a local Spark session, so
`pytest tests/unit -v` just works out of the box -- no per-shell env vars,
no assuming anything about what's already installed.

Runs once, at collection time, before any test's `spark` fixture executes:

  - PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON: always pinned to sys.executable
    (whichever interpreter is running pytest). Needed because Spark's worker
    subprocess otherwise calls a bare "python" command -- on Windows that
    hits the fake Microsoft Store alias instead of your venv's real
    interpreter, and everything fails with a confusing JAVA_GATEWAY_EXITED
    or socket timeout that has nothing to do with your code.
  - JAVA_HOME: left alone if already set. Otherwise, tries a handful of
    common per-OS JDK install locations. If none is found, tests fail
    immediately with one clear line telling you what to install, instead of
    PySpark's multi-page traceback.
"""
from __future__ import annotations

import os
import platform
import subprocess
import sys
from pathlib import Path

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

_INSTALL_HINT = (
    "No JDK found and JAVA_HOME is not set. PySpark needs a JDK (Java 17 recommended) "
    "to run these tests.\n"
    "  Windows: winget install Microsoft.OpenJDK.17\n"
    "  macOS:   brew install openjdk@17\n"
    "  Linux:   sudo apt install openjdk-17-jdk\n"
    "Then re-run pytest -- this file auto-detects common install locations, or set "
    "JAVA_HOME yourself first if yours lives somewhere unusual."
)


def _find_java_home() -> str | None:
    system = platform.system()

    if system == "Windows":
        for base in (r"C:\Program Files\Microsoft", r"C:\Program Files\Java", r"C:\Program Files\Eclipse Adoptium"):
            root = Path(base)
            if root.is_dir():
                matches = sorted(root.glob("jdk*"), reverse=True)
                if matches:
                    return str(matches[0])
        return None

    if system == "Darwin":
        try:
            result = subprocess.run(
                ["/usr/libexec/java_home"], capture_output=True, text=True, timeout=5, check=True
            )
            return result.stdout.strip() or None
        except (OSError, subprocess.SubprocessError):
            return None

    # Linux and everything else: look under the conventional JVM directory.
    root = Path("/usr/lib/jvm")
    if root.is_dir():
        matches = sorted(root.glob("*-17*"), reverse=True) or sorted(root.glob("*"), reverse=True)
        if matches:
            return str(matches[0])
    return None


if "JAVA_HOME" not in os.environ:
    _java_home = _find_java_home()
    if _java_home:
        os.environ["JAVA_HOME"] = _java_home
    else:
        import pytest

        pytest.exit(_INSTALL_HINT, returncode=1)
