"""
Explicit, string-typed schemas for every Day 1 / Bronze source -- deliberately
permissive per Databricks' own medallion architecture guidance: Bronze fields
stay loosely typed (here, StringType) so an unexpected value in the source
never breaks raw ingestion. Real, strict typing (IntegerType, TimestampType,
etc.) and schema-equality validation against those strict types belong to the
Silver layer (Day 4-5), not here.

Every column is still explicitly declared -- nothing is inferred, it's just
deliberately the most permissive type.

Column names AND ORDER come from the actual raw file headers (verified
directly against the Volume, not just docs/dataset_profiling.md's listing --
column order matters here since CSV schema application is positional).
"""
from __future__ import annotations

from pyspark.sql.types import StructType, StructField, StringType

# cars.csv real header order: enter,exit,date,id,location
CARS_SCHEMA = StructType([
    StructField("enter", StringType(), nullable=True),
    StructField("exit", StringType(), nullable=True),
    StructField("date", StringType(), nullable=True),
    StructField("id", StringType(), nullable=True),
    StructField("location", StringType(), nullable=True),
])

# streets.csv real header order: noise,pollution,date,light,raining,street_id
STREETS_SCHEMA = StructType([
    StructField("noise", StringType(), nullable=True),
    StructField("pollution", StringType(), nullable=True),
    StructField("date", StringType(), nullable=True),
    StructField("light", StringType(), nullable=True),
    StructField("raining", StringType(), nullable=True),
    StructField("street_id", StringType(), nullable=True),
])

# node_locations.csv real header order: latitude,longitude,location
NODE_LOCATIONS_SCHEMA = StructType([
    StructField("latitude", StringType(), nullable=True),
    StructField("longitude", StringType(), nullable=True),
    StructField("location", StringType(), nullable=False),  # PK -- one row per intersection, always present
])

# streets_list.csv real header order: street,long,latitude,longitude,dangerous,street_id
STREETS_LIST_SCHEMA = StructType([
    StructField("street", StringType(), nullable=True),
    StructField("long", StringType(), nullable=True),
    StructField("latitude", StringType(), nullable=True),
    StructField("longitude", StringType(), nullable=True),
    StructField("dangerous", StringType(), nullable=True),
    StructField("street_id", StringType(), nullable=False),  # PK -- one row per street, always present
])

# telegram.csv real header order: message,date,hour
TELEGRAM_SCHEMA = StructType([
    StructField("message", StringType(), nullable=True),
    StructField("date", StringType(), nullable=True),
    StructField("hour", StringType(), nullable=True),
])

# chunk1_csv..chunk4_xml share cars.csv's logical columns -- same source, only
# the on-disk format differs (CSV/JSON/XML). chunking.py's add_audit_columns()
# only adds columns, never renames/reshapes the original 5, so CARS_SCHEMA
# applies unchanged. Column order only matters for the CSV chunks (positional
# schema application) -- JSON/XML schema application is name-based, so reusing
# the same StructType for chunk3_json/chunk4_xml is safe regardless of order.
SCHEMAS = {
    "raw_cars": CARS_SCHEMA,
    "chunk1_csv": CARS_SCHEMA,
    "chunk2_csv": CARS_SCHEMA,
    "chunk3_json": CARS_SCHEMA,
    "chunk4_xml": CARS_SCHEMA,
    "raw_streets": STREETS_SCHEMA,
    "raw_node_locations": NODE_LOCATIONS_SCHEMA,
    "raw_streets_list": STREETS_LIST_SCHEMA,
    "raw_telegram": TELEGRAM_SCHEMA,
}
