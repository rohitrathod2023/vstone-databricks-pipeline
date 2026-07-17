"""Strict, evidence-based Silver schemas for the dimension staging tables.

Deliberately separate from config/schemas.py (Bronze's permissive
STRING-only schemas) so a Silver caller can never accidentally receive a
permissive schema meant for Bronze. Types below come from real Phase 1
profiling (docs/dataset_profiling.md), not assumption.
"""
from __future__ import annotations

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

# Real observed ranges: location 1-14, 6-decimal lat/long precision.
SILVER_LOCATIONS_SCHEMA = StructType(
    [
        StructField("location", IntegerType(), nullable=True),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
    ]
)

SILVER_LOCATIONS_REJECTED_SCHEMA = StructType(
    SILVER_LOCATIONS_SCHEMA.fields + [StructField("rejection_reason", StringType(), nullable=True)]
)

# `long` is street length in meters, not longitude. Real observed ranges:
# street_id 1-36 (unique per row), dangerous 0.1-0.9.
SILVER_STREETS_SCHEMA = StructType(
    [
        StructField("street", StringType(), nullable=True),
        StructField("long", IntegerType(), nullable=True),
        StructField("latitude", DoubleType(), nullable=True),
        StructField("longitude", DoubleType(), nullable=True),
        StructField("dangerous", DoubleType(), nullable=True),
        StructField("street_id", IntegerType(), nullable=True),
    ]
)

SILVER_STREETS_REJECTED_SCHEMA = StructType(
    SILVER_STREETS_SCHEMA.fields + [StructField("rejection_reason", StringType(), nullable=True)]
)

# Real observed ranges (24.7M rows, all 4 Bronze union branches): id 0-998,
# location 1-14, enter/exit 0-35. `date` is ISO8601 (unambiguous, unlike
# telegram.csv). `source_technique` traces which of the 4 Bronze ingestion
# techniques a row came from -- not brief-required, added for observability.
SILVER_TRAFFIC_SCHEMA = StructType(
    [
        StructField("id", IntegerType(), nullable=True),
        StructField("location", IntegerType(), nullable=True),
        StructField("enter", IntegerType(), nullable=True),
        StructField("exit", IntegerType(), nullable=True),
        StructField("date", TimestampType(), nullable=True),
        StructField("source_technique", StringType(), nullable=True),
    ]
)

SILVER_TRAFFIC_REJECTED_SCHEMA = StructType(
    SILVER_TRAFFIC_SCHEMA.fields + [StructField("rejection_reason", StringType(), nullable=True)]
)

# Real observed ranges (87.8M rows): street_id 1-36, noise/pollution 0-~46.9,
# light 5.14E-8-83.7. `raining` is the one real found defect this phase's
# quarantine exists to catch: observed -0.9999999834 to 100.9999, outside
# the physically-valid [0, 100] percentage range.
SILVER_ENVIRONMENT_SCHEMA = StructType(
    [
        StructField("street_id", IntegerType(), nullable=True),
        StructField("date", TimestampType(), nullable=True),
        StructField("noise", DoubleType(), nullable=True),
        StructField("pollution", DoubleType(), nullable=True),
        StructField("light", DoubleType(), nullable=True),
        StructField("raining", DoubleType(), nullable=True),
    ]
)

SILVER_ENVIRONMENT_REJECTED_SCHEMA = StructType(
    SILVER_ENVIRONMENT_SCHEMA.fields + [StructField("rejection_reason", StringType(), nullable=True)]
)

# event_timestamp combines telegram.csv's separate date (confirmed dd/MM/yyyy)
# + hour (confirmed HH:mm:ss) strings into one real timestamp -- the
# original date/hour columns are dropped once combined, no reason to carry
# three overlapping time representations forward. `message` gets the same
# whitespace-normalization Pandas UDF already built for streets_list.street
# (real leading-whitespace values confirmed in Bronze, e.g. " The car does
# not respect...").
SILVER_TELEGRAM_SCHEMA = StructType(
    [
        StructField("message", StringType(), nullable=True),
        StructField("event_timestamp", TimestampType(), nullable=True),
    ]
)

SILVER_TELEGRAM_REJECTED_SCHEMA = StructType(
    SILVER_TELEGRAM_SCHEMA.fields + [StructField("rejection_reason", StringType(), nullable=True)]
)

SILVER_SCHEMAS = {
    "silver_locations": SILVER_LOCATIONS_SCHEMA,
    "silver_locations_rejected": SILVER_LOCATIONS_REJECTED_SCHEMA,
    "silver_streets": SILVER_STREETS_SCHEMA,
    "silver_streets_rejected": SILVER_STREETS_REJECTED_SCHEMA,
    "silver_traffic": SILVER_TRAFFIC_SCHEMA,
    "silver_traffic_rejected": SILVER_TRAFFIC_REJECTED_SCHEMA,
    "silver_environment": SILVER_ENVIRONMENT_SCHEMA,
    "silver_environment_rejected": SILVER_ENVIRONMENT_REJECTED_SCHEMA,
    "silver_telegram": SILVER_TELEGRAM_SCHEMA,
    "silver_telegram_rejected": SILVER_TELEGRAM_REJECTED_SCHEMA,
}
