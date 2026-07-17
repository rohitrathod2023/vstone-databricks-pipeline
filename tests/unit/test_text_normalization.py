"""
Unit tests for pipelines.silver.text_normalization -- the genuine Pandas UDF
deliverable, normalizing streets_list.street VALUES (not column names, see
test_header_standardization.py for that). normalize_street_name (the plain
function the UDF wraps) is tested here on bare strings, no Spark session
needed.

Any Python worker subprocess invocation (pandas_udf, plain udf(), even with
spark.python.worker.reuse=false) crashes repeatedly on this local Windows
PySpark environment (a socket connection getting aborted after the first
call, regardless of UDF flavor) -- not a bug in this project's code, and not
reproducible on real Databricks Runtime. So the actual
normalize_street_name_udf() wrapper mechanism isn't exercised here; this
tests only the plain function it wraps, which carries the real logic.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_text_normalization.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipelines.silver.text_normalization import normalize_street_name  # noqa: E402


def test_strips_leading_and_trailing_whitespace():
    assert normalize_street_name("  Corts Valencianes 1A  ") == "Corts Valencianes 1A"


def test_collapses_repeated_internal_whitespace():
    assert normalize_street_name("Corts   Valencianes    1A") == "Corts Valencianes 1A"


def test_collapses_tabs_and_mixed_whitespace():
    assert normalize_street_name("9\tOctubreA  \n Extra") == "9 OctubreA Extra"


def test_leaves_already_clean_input_untouched():
    assert normalize_street_name("Corts Valencianes 1A") == "Corts Valencianes 1A"


def test_does_not_alter_casing():
    """Street names are proper nouns -- only whitespace is in scope."""
    assert normalize_street_name("corts VALENCIANES 1a") == "corts VALENCIANES 1a"


def test_none_passes_through_unchanged():
    assert normalize_street_name(None) is None


def test_empty_string_stays_empty():
    assert normalize_street_name("") == ""
