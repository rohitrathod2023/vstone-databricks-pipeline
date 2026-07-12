"""
One generic COPY INTO loader, reused for every Day 2 Bronze source
(chunk1_csv, bronze_streets, bronze_node_locations, bronze_streets_list,
bronze_telegram) — the technique and audit columns are identical across all
5, only the source_key (and therefore path/target_table via config_loader)
differs, so there's exactly one function instead of five near-duplicates.

COPY INTO tracks which source files it has already loaded internally, so
re-running this against the same source file is naturally idempotent —
no manual dedup/checkpoint needed for this technique specifically (unlike
chunk4's plain PySpark read/write, which has no such built-in guarantee).

    from pipelines.bronze.copy_into import run_copy_into
    result = run_copy_into(spark, "chunk1_csv", env="dev")
"""
from __future__ import annotations

from typing import Any, Dict

from common.config_loader import get_source_config
from common.logger import current_run_id

_AUDIT_COLUMNS = ("load_dt", "source_format", "source_file", "run_id")


def _select_clause(cfg: Dict[str, Any]) -> str:
    """chunk1_csv/chunk2_csv/chunk3_json/chunk4_xml were already written by
    chunking.py's add_audit_columns() call, so they already carry
    load_dt/source_format/source_file/run_id from that extraction event --
    "SELECT *" would collide with them (COLUMN_ALREADY_EXISTS). Bronze's own
    load re-tags with its own load event instead of colliding with or
    silently keeping the chunking step's stale values."""
    if cfg.get("has_audit_columns"):
        return f"* EXCEPT ({', '.join(_AUDIT_COLUMNS)})"
    return "*"


def build_create_table_sql(cfg: Dict[str, Any]) -> str:
    """COPY INTO requires its target table to already exist (confirmed against
    a real Databricks SQL warehouse -- it does NOT auto-create one). This
    creates it empty, via a zero-row CTAS, so the table's schema exactly
    matches what the COPY INTO SELECT below will insert (including audit
    columns) -- no schema drift between the two statements. IF NOT EXISTS
    makes this a no-op on every run after the first."""
    target_table = cfg["target_table"]
    source_path = cfg["path"]
    source_format = cfg["format"]
    source_file = source_path.rsplit("/", 1)[-1]

    return f"""
        CREATE TABLE IF NOT EXISTS {target_table}
        USING DELTA
        AS SELECT {_select_clause(cfg)},
                  current_timestamp() AS load_dt,
                  '{source_format}' AS source_format,
                  '{source_file}' AS source_file,
                  '' AS run_id
           FROM read_files('{source_path}', format => '{source_format}', header => true, inferSchema => true)
           WHERE 1 = 0
    """.strip()


def build_copy_into_sql(cfg: Dict[str, Any], run_id: str) -> str:
    """Pure string construction, no Spark session needed -- kept separate from
    run_copy_into() so the generated SQL is unit-testable on its own."""
    target_table = cfg["target_table"]
    source_path = cfg["path"]
    source_format = cfg["format"]
    source_file = source_path.rsplit("/", 1)[-1]

    return f"""
        COPY INTO {target_table}
        FROM (
            SELECT {_select_clause(cfg)},
                   current_timestamp() AS load_dt,
                   '{source_format}' AS source_format,
                   '{source_file}' AS source_file,
                   '{run_id}' AS run_id
            FROM '{source_path}'
        )
        FILEFORMAT = CSV
        FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true', 'mergeSchema' = 'true')
        COPY_OPTIONS ('mergeSchema' = 'true')
    """.strip()


def run_copy_into(spark, source_key: str, env: str = "dev") -> Dict[str, Any]:
    """Loads source_key's file into its Bronze table via COPY INTO. Returns a
    dict of {source_key, target_table, row_count, run_id} for logging/sanity
    checks — same shape convention as pipelines.ingestion.chunking.run()."""
    cfg = get_source_config(source_key, env=env)
    run_id = current_run_id()

    spark.sql(build_create_table_sql(cfg))
    spark.sql(build_copy_into_sql(cfg, run_id))
    row_count = spark.table(cfg["target_table"]).count()

    return {
        "source_key": source_key,
        "target_table": cfg["target_table"],
        "row_count": row_count,
        "run_id": run_id,
    }
