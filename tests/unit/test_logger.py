"""
Unit tests for utils.logger's Delta audit table-name resolution -- the
`<catalog>.<audit_schema>.pipeline_logs` name must track the same
per-environment schema prefixing that raw_schema/bronze_schema get under
mode: development, not a hardcoded "audit" that's only ever correct in
test/prod (mode: production leaves schema names unprefixed there).
"""
from __future__ import annotations

import sys
from pathlib import Path

SRC_DIR = Path(__file__).resolve().parents[2] / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))


def test_audit_table_uses_dev_prefixed_schema(monkeypatch):
    from utils.logger import _DeltaAuditHandler

    monkeypatch.setenv("DATABRICKS_BUNDLE_TARGET", "dev")
    handler = _DeltaAuditHandler(catalog="vstone_traffic_dev", job_name="Test")

    assert handler._table == "`vstone_traffic_dev`.dev_rohitrathodcomp_audit.pipeline_logs"


def test_audit_table_uses_plain_schema_in_prod(monkeypatch):
    from utils.logger import _DeltaAuditHandler

    monkeypatch.setenv("DATABRICKS_BUNDLE_TARGET", "prod")
    handler = _DeltaAuditHandler(catalog="vstone_traffic_prod", job_name="Test")

    assert handler._table == "`vstone_traffic_prod`.audit.pipeline_logs"


def test_resolve_table_falls_back_to_literal_audit_on_config_error(monkeypatch):
    """If env.yml can't be loaded for any reason, table-name resolution must
    still return something rather than raising -- this handler is designed to
    never break the pipeline it's logging for."""
    from utils import logger as logger_mod

    def broken_get_env_config(env=None):
        raise RuntimeError("config unavailable")

    monkeypatch.setattr("utils.config_loader.get_env_config", broken_get_env_config)

    handler = logger_mod._DeltaAuditHandler(catalog="vstone_traffic_dev", job_name="Test")

    assert handler._table == "`vstone_traffic_dev`.audit.pipeline_logs"
