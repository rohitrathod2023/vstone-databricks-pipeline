"""
Config-driven table/column comments — every Bronze/Silver/Gold table gets its
description from sources.yml (already there for every source) rather than a
separate comments file, so there's one place to describe a dataset, not two.

    from common.metadata import apply_table_comments
    apply_table_comments(spark, cfg["target_table"], cfg["description"])
"""
from __future__ import annotations

from typing import Dict, List, Optional


def _escape(text: str) -> str:
    return text.replace("'", "''")


def build_comment_sql(
    table: str, table_comment: str, column_comments: Optional[Dict[str, str]] = None
) -> List[str]:
    """Pure string construction, no Spark session needed."""
    statements = [f"COMMENT ON TABLE {table} IS '{_escape(table_comment)}'"]
    for column, comment in (column_comments or {}).items():
        statements.append(f"COMMENT ON COLUMN {table}.{column} IS '{_escape(comment)}'")
    return statements


def apply_table_comments(
    spark, table: str, table_comment: str, column_comments: Optional[Dict[str, str]] = None
) -> None:
    for statement in build_comment_sql(table, table_comment, column_comments):
        spark.sql(statement)
