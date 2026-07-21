# Databricks notebook source
# MAGIC %md
# MAGIC # Gold DLT pipeline — dimensions, unified fact, and aggregates
# MAGIC Every `@dlt.table` function here is a thin wrapper -- real generation
# MAGIC logic lives in plain, pytest-testable functions under
# MAGIC `src/pipelines/gold/*.py` (dim_date.py, dim_location.py, ...), same
# MAGIC notebook/pipeline separation pattern already used for Bronze and Silver.
# MAGIC
# MAGIC **Materialized views, not streaming tables.** Gold-layer tables use
# MAGIC `spark.read`/`spark.table` (not `spark.readStream`), which is what makes
# MAGIC DLT treat them as materialized views -- Databricks' own Lakeflow docs
# MAGIC reserve streaming tables for row-level ingestion/transformation and
# MAGIC materialized views for aggregation/dimension-style tables that can be
# MAGIC fully recomputed each refresh.
# MAGIC
# MAGIC **Unified fact design:** one fact table, `fact_city_observations`,
# MAGIC combines traffic, environmental, and telegram observations via an
# MAGIC `observation_type` discriminator, resolved against `dim_date`,
# MAGIC `dim_location`, `dim_street`, `dim_technique`, and `dim_audit`.
# MAGIC `agg_daily_street_conditions`, `agg_daily_location_traffic`,
# MAGIC `agg_monthly_street_summary`, and `agg_hourly_telegram_activity` roll it
# MAGIC up further, each at its own street/location/date/hour grain.
# MAGIC
# MAGIC Catalog/target schema are set at the pipeline level (see
# MAGIC `resources/pipelines/gold_dlt_pipeline.yml`), not in this file -- same
# MAGIC convention as Bronze/Silver.
# MAGIC
# MAGIC **`import dlt` (legacy), not `from pyspark import pipelines as dp`** --
# MAGIC matches Bronze's and Silver's DLT pipelines, the proven-working
# MAGIC technique in this workspace.

# COMMAND ----------

import os
import sys


def _find_src_dir(start: str) -> str:
    """Walk upward from the notebook's own directory looking for the `src/`
    root (identified by a `pipelines` package inside it) instead of assuming
    a fixed number of parent levels -- same helper already used by Silver's
    DLT notebook.
    """
    current = start
    for _ in range(6):
        if os.path.isdir(os.path.join(current, "pipelines")):
            return current
        if os.path.isdir(os.path.join(current, "src", "pipelines")):
            return os.path.join(current, "src")
        current = os.path.dirname(current)
    raise RuntimeError(f"Could not locate src/ from {start}")


NOTEBOOK_DIR = os.path.dirname(os.path.abspath(__file__)) if "__file__" in dir() else os.getcwd()
SRC_DIR = _find_src_dir(NOTEBOOK_DIR)
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# COMMAND ----------

import dlt  # noqa: E402

# Dimension builders
from pipelines.gold.dim_date import build_dim_date, compute_date_range  # noqa: E402
from pipelines.gold.dim_location import build_dim_location  # noqa: E402
from pipelines.gold.dim_street import TRACKED_COLUMNS, build_dim_street  # noqa: E402
from pipelines.gold.dim_technique import build_dim_technique  # noqa: E402
from pipelines.gold.dim_audit import build_dim_audit  # noqa: E402
from pipelines.gold.dim_time import build_dim_time  # noqa: E402

# Unified fact builder
from pipelines.gold.fact_city_observations import build_fact_city_observations  # noqa: E402

# Aggregates, on top of the unified fact
from pipelines.gold.agg_daily_street_conditions import build_agg_daily_street_conditions  # noqa: E402
from pipelines.gold.agg_daily_location_traffic import build_agg_daily_location_traffic  # noqa: E402
from pipelines.gold.agg_monthly_street_summary import build_agg_monthly_street_summary  # noqa: E402
from pipelines.gold.agg_hourly_telegram_activity import build_agg_hourly_telegram_activity  # noqa: E402

# Schemas
from pipelines.gold.table_schemas import (  # noqa: E402
    DIM_DATE_SCHEMA,
    DIM_LOCATION_SCHEMA,
    DIM_STREET_SCHEMA,
    DIM_TECHNIQUE_SCHEMA,
    DIM_AUDIT_SCHEMA,
    DIM_TIME_SCHEMA,
    FACT_CITY_OBSERVATIONS_SCHEMA,
    AGG_DAILY_STREET_CONDITIONS_SCHEMA,
    AGG_DAILY_LOCATION_TRAFFIC_SCHEMA,
    AGG_MONTHLY_STREET_SUMMARY_SCHEMA,
    AGG_HOURLY_TELEGRAM_ACTIVITY_SCHEMA,
)
from utils.config_loader import get_source_config  # noqa: E402

_ENV = spark.conf.get("env", "dev")

_DIM_DATE_CFG = get_source_config("dim_date", env=_ENV)
_DIM_LOCATION_CFG = get_source_config("dim_location", env=_ENV)
_STG_DIM_STREET_SCD2_CFG = get_source_config("stg_dim_street_scd2", env=_ENV)
_DIM_STREET_CFG = get_source_config("dim_street", env=_ENV)
_DIM_TECHNIQUE_CFG = get_source_config("dim_technique", env=_ENV)
_DIM_AUDIT_CFG = get_source_config("dim_audit", env=_ENV)
_DIM_TIME_CFG = get_source_config("dim_time", env=_ENV)
_FACT_CITY_OBSERVATIONS_CFG = get_source_config("fact_city_observations", env=_ENV)
_AGG_DAILY_STREET_CONDITIONS_CFG = get_source_config("agg_daily_street_conditions", env=_ENV)
_AGG_DAILY_LOCATION_TRAFFIC_CFG = get_source_config("agg_daily_location_traffic", env=_ENV)
_AGG_MONTHLY_STREET_SUMMARY_CFG = get_source_config("agg_monthly_street_summary", env=_ENV)
_AGG_HOURLY_TELEGRAM_ACTIVITY_CFG = get_source_config("agg_hourly_telegram_activity", env=_ENV)
_TRAFFIC_TABLE = get_source_config("silver_traffic", env=_ENV)["target_table"]
_ENVIRONMENT_TABLE = get_source_config("silver_environment", env=_ENV)["target_table"]
_TELEGRAM_TABLE = get_source_config("silver_telegram", env=_ENV)["target_table"]
_LOCATIONS_TABLE = get_source_config("silver_locations", env=_ENV)["target_table"]
_STREETS_TABLE = get_source_config("silver_streets", env=_ENV)["target_table"]

# COMMAND ----------

# DBTITLE 1,Dim_Date


@dlt.table(
    name="dim_date",
    comment=_DIM_DATE_CFG["description"],
    schema=DIM_DATE_SCHEMA,
)
def dim_date():
    # Plain spark.table() reads (not spark.readStream), matching the
    # materialized-view convention for Gold -- also needed here since
    # compute_date_range must see the FULL current Silver table each
    # refresh, not just an incremental slice.
    traffic_df = spark.table(_TRAFFIC_TABLE)
    environment_df = spark.table(_ENVIRONMENT_TABLE)
    telegram_df = spark.table(_TELEGRAM_TABLE)

    min_date, max_date = compute_date_range(traffic_df, environment_df, telegram_df)
    return build_dim_date(spark, min_date, max_date)


# COMMAND ----------

# DBTITLE 1,Dim_Location


@dlt.table(
    name="dim_location",
    comment=_DIM_LOCATION_CFG["description"],
    schema=DIM_LOCATION_SCHEMA,
)
def dim_location():
    return build_dim_location(spark.table(_LOCATIONS_TABLE))


# COMMAND ----------

# DBTITLE 1,Dim_Street (SCD2 via AUTO CDC FROM SNAPSHOT), two-hop
# Only Gold table with true SCD2. Two hops, not one -- an earlier
# single-streaming-table design computed street_key directly on the AUTO
# CDC output and hit two real failures: monotonically_increasing_id()
# overflowed IntegerType, and row_number() is flatly unsupported on a
# streaming DataFrame. The fix is structural: split the CDC plumbing
# (stg_dim_street_scd2, a streaming table -- AUTO CDC FROM SNAPSHOT's
# target requirement) from the real public dimension (dim_street, a
# materialized view reading stg_dim_street_scd2 via a plain batch read,
# where row_number() works normally). stg_dim_street_scd2 is internal CDC
# plumbing, not part of the documented Gold dimension list -- same framing
# as a Bronze table feeding Silver.
#
# _dim_street_snapshot is a plain (batch) view over the full current
# silver_streets table -- AUTO CDC's "periodic snapshot" mode treats each
# pipeline update's full read as one new numbered snapshot to diff against
# the previous, so this must NOT be a streaming read.


@dlt.view(name="_dim_street_snapshot")
def _dim_street_snapshot():
    return spark.table(_STREETS_TABLE)


dlt.create_streaming_table(name="stg_dim_street_scd2", comment=_STG_DIM_STREET_SCD2_CFG["description"])

dlt.create_auto_cdc_from_snapshot_flow(
    target="stg_dim_street_scd2",
    source="_dim_street_snapshot",
    keys=["street_id"],
    stored_as_scd_type="2",
    track_history_column_list=TRACKED_COLUMNS,
)


@dlt.table(
    name="dim_street",
    comment=_DIM_STREET_CFG["description"],
    schema=DIM_STREET_SCHEMA,
)
def dim_street():
    # Plain spark.table() (not spark.readStream) -- this is what makes
    # Dim_Street a materialized view, and what makes row_number() work in
    # build_dim_street(): the streaming restriction applies to
    # spark.readStream queries specifically, not a batch read of a
    # streaming table's stored data. Reads the FULL history (every SCD2
    # version), not just current -- fact_city_observations' environmental
    # branch needs that to resolve the correct version per observation date.
    return build_dim_street(spark.table("stg_dim_street_scd2"))


# COMMAND ----------

# DBTITLE 1,Dim_Technique


@dlt.table(
    name="dim_technique",
    comment=_DIM_TECHNIQUE_CFG["description"],
    schema=DIM_TECHNIQUE_SCHEMA,
)
def dim_technique():
    return build_dim_technique(spark)


# COMMAND ----------

# DBTITLE 1,Dim_Audit


@dlt.table(
    name="dim_audit",
    comment=_DIM_AUDIT_CFG["description"],
    schema=DIM_AUDIT_SCHEMA,
)
def dim_audit():
    traffic_df = spark.table(_TRAFFIC_TABLE)
    environment_df = spark.table(_ENVIRONMENT_TABLE)
    telegram_df = spark.table(_TELEGRAM_TABLE)
    return build_dim_audit(traffic_df, environment_df, telegram_df)


# COMMAND ----------

# DBTITLE 1,Dim_Time


@dlt.table(
    name="dim_time",
    comment=_DIM_TIME_CFG["description"],
    schema=DIM_TIME_SCHEMA,
)
def dim_time():
    return build_dim_time(spark)


# COMMAND ----------

# DBTITLE 1,Fact_City_Observations


@dlt.table(
    name="fact_city_observations",
    comment=_FACT_CITY_OBSERVATIONS_CFG["description"],
    schema=FACT_CITY_OBSERVATIONS_SCHEMA,
    cluster_by=["observation_type", "date_key"],
)
def fact_city_observations():
    # Source data
    traffic_df = spark.table(_TRAFFIC_TABLE)
    environment_df = spark.table(_ENVIRONMENT_TABLE)
    telegram_df = spark.table(_TELEGRAM_TABLE)

    # Dimensions for FK lookups
    dim_street_df = spark.table("dim_street")
    dim_location_df = spark.table("dim_location")
    dim_date_df = spark.table("dim_date")
    dim_audit_df = spark.table("dim_audit")
    dim_technique_df = spark.table("dim_technique")

    return build_fact_city_observations(
        traffic_df,
        environment_df,
        telegram_df,
        dim_street_df,
        dim_location_df,
        dim_date_df,
        dim_audit_df,
        dim_technique_df,
    )


# COMMAND ----------

# DBTITLE 1,agg_daily_street_conditions


@dlt.table(
    name="agg_daily_street_conditions",
    comment=_AGG_DAILY_STREET_CONDITIONS_CFG["description"],
    schema=AGG_DAILY_STREET_CONDITIONS_SCHEMA,
)
def agg_daily_street_conditions():
    return build_agg_daily_street_conditions(
        spark.table("fact_city_observations"),
        spark.table("dim_street"),
        spark.table("dim_date"),
    )


# COMMAND ----------

# DBTITLE 1,agg_daily_location_traffic


@dlt.table(
    name="agg_daily_location_traffic",
    comment=_AGG_DAILY_LOCATION_TRAFFIC_CFG["description"],
    schema=AGG_DAILY_LOCATION_TRAFFIC_SCHEMA,
)
def agg_daily_location_traffic():
    return build_agg_daily_location_traffic(
        spark.table("fact_city_observations"),
        spark.table("dim_location"),
        spark.table("dim_date"),
    )


# COMMAND ----------

# DBTITLE 1,agg_monthly_street_summary


@dlt.table(
    name="agg_monthly_street_summary",
    comment=_AGG_MONTHLY_STREET_SUMMARY_CFG["description"],
    schema=AGG_MONTHLY_STREET_SUMMARY_SCHEMA,
)
def agg_monthly_street_summary():
    return build_agg_monthly_street_summary(
        spark.table("fact_city_observations"),
        spark.table("dim_street"),
        spark.table("dim_date"),
    )


# COMMAND ----------

# DBTITLE 1,agg_hourly_telegram_activity


@dlt.table(
    name="agg_hourly_telegram_activity",
    comment=_AGG_HOURLY_TELEGRAM_ACTIVITY_CFG["description"],
    schema=AGG_HOURLY_TELEGRAM_ACTIVITY_SCHEMA,
)
def agg_hourly_telegram_activity():
    return build_agg_hourly_telegram_activity(
        spark.table("fact_city_observations"),
        spark.table("dim_date"),
        spark.table("dim_time"),
    )
