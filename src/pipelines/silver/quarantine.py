"""Shared dead-letter/quarantine split, reused across every Silver table with
validation rules. No single DLT expectation decorator produces a quarantine
table on its own (confirmed against Databricks' own docs, which state
quarantining needs "additional logic" beyond expect/expect_or_drop) -- the
actual pattern is one rejection_reason column computed once, then two
downstream filters (valid_rows/rejected_rows) derived from the same checked
DataFrame.
"""
from __future__ import annotations

from typing import Dict, Tuple

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def build_rejection_reason_expr(rules: Dict[str, Tuple[str, str]]) -> Column:
    """Build a STRING column naming every validation rule a row fails.

    Args:
        rules: Mapping of rule_name -> (failure_condition_sql, reason_message).
            failure_condition_sql must evaluate TRUE when the row is BAD
            (should be quarantined), not when it passes.

    Returns:
        Column: NULL when a row fails zero rules; otherwise every matching
        reason_message joined by "; " -- a row failing multiple rules at
        once names all of them, not just the first.
    """
    reason_when_failed = [F.when(F.expr(condition), F.lit(message)) for condition, message in rules.values()]
    # concat_ws skips NULL arguments, so only reasons for rules that actually
    # failed end up in the string -- but it returns "" (not NULL) when every
    # argument is NULL, so that case needs converting back to NULL explicitly.
    combined = F.concat_ws("; ", *reason_when_failed)
    return F.when(F.length(combined) > 0, combined).otherwise(F.lit(None).cast("string"))


def valid_rows(checked_df: DataFrame) -> DataFrame:
    """Return only rows that passed every validation rule.

    Args:
        checked_df: DataFrame with a rejection_reason column already computed.

    Returns:
        DataFrame with rejection_reason dropped, containing only passing rows.
    """
    return checked_df.filter(F.col("rejection_reason").isNull()).drop("rejection_reason")


def rejected_rows(checked_df: DataFrame) -> DataFrame:
    """Return only rows that failed at least one validation rule.

    Args:
        checked_df: DataFrame with a rejection_reason column already computed.

    Returns:
        DataFrame with rejection_reason retained, containing only failing rows.
    """
    return checked_df.filter(F.col("rejection_reason").isNotNull())
