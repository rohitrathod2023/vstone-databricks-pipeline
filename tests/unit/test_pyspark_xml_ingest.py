"""
Unit tests for pipelines.bronze.pyspark_xml_ingest's column-name sanitizing
— pure logic, no Spark session needed.

Run locally:
    pip install -r tests/requirements.txt
    pytest tests/unit/test_pyspark_xml_ingest.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from pipelines.bronze.pyspark_xml_ingest import sanitize_column_name  # noqa: E402


def test_sanitize_replaces_hyphens_and_colons():
    assert sanitize_column_name("street-name") == "street_name"
    assert sanitize_column_name("node:id") == "node_id"


def test_sanitize_replaces_spaces():
    assert sanitize_column_name("col with spaces") == "col_with_spaces"


def test_sanitize_leaves_already_valid_names_untouched():
    assert sanitize_column_name("valid_name") == "valid_name"
    assert sanitize_column_name("enter") == "enter"


def test_sanitize_handles_multiple_consecutive_invalid_chars():
    assert sanitize_column_name("a--b::c") == "a__b__c"
