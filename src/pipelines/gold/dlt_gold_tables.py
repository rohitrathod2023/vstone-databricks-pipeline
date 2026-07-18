# Databricks notebook source
# MAGIC %md
# MAGIC # Gold DLT pipeline — dimensions and facts
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
# MAGIC fully recomputed each refresh. `Dim_Date` has no SCD2/CDC involvement,
# MAGIC so a plain materialized view is the correct fit.
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

from pipelines.gold.dim_date import build_dim_date, compute_date_range  # noqa: E402
from pipelines.gold.dim_location import build_dim_location  # noqa: E402
from pipelines.gold.dim_street import TRACKED_COLUMNS, build_dim_street  # noqa: E402
from pipelines.gold.fact_street_conditions import build_fact_street_conditions  # noqa: E402
from pipelines.gold.fact_traffic_counts import build_fact_traffic_counts  # noqa: E402
from pipelines.gold.gold_monthly_traffic_summary import build_gold_monthly_traffic_summary  # noqa: E402
from pipelines.gold.gold_street_risk_summary import build_gold_street_risk_summary  # noqa: E402
from utils.config_loader import get_source_config  # noqa: E402

_ENV = spark.conf.get("env", "dev")

_DIM_DATE_CFG = get_source_config("dim_date", env=_ENV)
_DIM_LOCATION_CFG = get_source_config("dim_location", env=_ENV)
_STG_DIM_STREET_SCD2_CFG = get_source_config("stg_dim_street_scd2", env=_ENV)
_DIM_STREET_CFG = get_source_config("dim_street", env=_ENV)
_FACT_TRAFFIC_COUNTS_CFG = get_source_config("fact_traffic_counts", env=_ENV)
_FACT_STREET_CONDITIONS_CFG = get_source_config("fact_street_conditions", env=_ENV)
_GOLD_MONTHLY_TRAFFIC_SUMMARY_CFG = get_source_config("gold_monthly_traffic_summary", env=_ENV)
_GOLD_STREET_RISK_SUMMARY_CFG = get_source_config("gold_street_risk_summary", env=_ENV)

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
    schema="""
        date_key        INT     NOT NULL,
        full_date       DATE,
        year            INT,
        month           INT,
        month_name      STRING,
        day_of_month    INT,
        day_of_week     INT,
        day_name        STRING,
        quarter         INT,
        is_weekend      BOOLEAN,
        load_dt         TIMESTAMP,
        source_format   STRING,
        source_file     STRING,
        run_id          STRING,
        CONSTRAINT dim_date_pk PRIMARY KEY (date_key)
    """,
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
    schema="""
        location_key    INT     NOT NULL,
        location        INT,
        latitude        DOUBLE,
        longitude       DOUBLE,
        load_dt         TIMESTAMP,
        source_format   STRING,
        source_file     STRING,
        run_id          STRING,
        CONSTRAINT dim_location_pk PRIMARY KEY (location_key)
    """,
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
    schema="""
        street_key      INT     NOT NULL,
        street_id       INT,
        street          STRING,
        long            INT,
        latitude        DOUBLE,
        longitude       DOUBLE,
        dangerous       DOUBLE,
        __START_AT      TIMESTAMP,
        __END_AT        TIMESTAMP,
        is_current      BOOLEAN,
        load_dt         TIMESTAMP,
        source_format   STRING,
        source_file     STRING,
        run_id          STRING,
        CONSTRAINT dim_street_pk PRIMARY KEY (street_key)
    """,
)
def dim_street():
    # Plain spark.table() (not spark.readStream) -- this is what makes
    # Dim_Street a materialized view, and what makes row_number() work in
    # build_dim_street(): the streaming restriction applies to
    # spark.readStream queries specifically, not a batch read of a
    # streaming table's stored data. Reads the FULL history (every SCD2
    # version), not just current -- Fact_Street_Conditions needs that to
    # resolve the correct version per fact date.
    return build_dim_street(spark.table("stg_dim_street_scd2"))


# COMMAND ----------

# DBTITLE 1,Fact_Traffic_Counts


@dlt.table(
    name="fact_traffic_counts",
    comment=_FACT_TRAFFIC_COUNTS_CFG["description"],
    schema="""
        location_key        INT,
        date_key            INT,
        id                  INT,
        enter               INT,
        exit                INT,
        source_technique    STRING,
        load_dt             TIMESTAMP,
        source_format       STRING,
        source_file         STRING,
        run_id              STRING,
        CONSTRAINT fact_traffic_counts_location_fk
            FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
        CONSTRAINT fact_traffic_counts_date_fk
            FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
    """,
)
def fact_traffic_counts():
    traffic_df = spark.table(_TRAFFIC_TABLE)
    dim_location_df = spark.table("dim_location")
    dim_date_df = spark.table("dim_date")
    return build_fact_traffic_counts(traffic_df, dim_location_df, dim_date_df)


# COMMAND ----------

# DBTITLE 1,Fact_Street_Conditions


@dlt.table(
    name="fact_street_conditions",
    comment=_FACT_STREET_CONDITIONS_CFG["description"],
    schema="""
        street_key      INT,
        date_key        INT,
        noise           DOUBLE,
        pollution       DOUBLE,
        light           DOUBLE,
        raining         DOUBLE,
        load_dt         TIMESTAMP,
        source_format   STRING,
        source_file     STRING,
        run_id          STRING,
        CONSTRAINT fact_street_conditions_street_fk
            FOREIGN KEY (street_key) REFERENCES dim_street(street_key),
        CONSTRAINT fact_street_conditions_date_fk
            FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
    """,
)
def fact_street_conditions():
    environment_df = spark.table(_ENVIRONMENT_TABLE)
    dim_street_df = spark.table("dim_street")
    dim_date_df = spark.table("dim_date")
    return build_fact_street_conditions(environment_df, dim_street_df, dim_date_df)


# COMMAND ----------

# DBTITLE 1,gold_monthly_traffic_summary


@dlt.table(
    name="gold_monthly_traffic_summary",
    comment=_GOLD_MONTHLY_TRAFFIC_SUMMARY_CFG["description"],
    # No PRIMARY KEY here: the grain is (location_key, year, month), but
    # location_key is legitimately NULL for 10 rows -- silver_traffic has
    # real location=7 readings even though location=7 was excluded from
    # Dim_Location at Silver (bad coordinates), and that orphan surfaces
    # here as its own NULL-location group per month (see
    # docs/gold_data_model.md). A PRIMARY KEY member can't be NULL, so
    # declaring one on this grain would either fail deployment or
    # misrepresent the data; the FK below is unaffected since FK columns
    # are allowed to be NULL (same as Fact_Traffic_Counts.location_key).
    schema="""
        location_key                INT,
        year                        INT,
        month                       INT,
        total_enter                 LONG,
        total_exit                  LONG,
        total_traffic_volume        LONG,
        avg_daily_traffic_volume    DOUBLE,
        busiest_rank_in_month       INT,
        load_dt                     TIMESTAMP,
        source_format               STRING,
        source_file                 STRING,
        run_id                      STRING,
        CONSTRAINT gold_monthly_traffic_summary_location_fk
            FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
    """,
)
def gold_monthly_traffic_summary():
    fact_traffic_counts_df = spark.table("fact_traffic_counts")
    dim_date_df = spark.table("dim_date")
    return build_gold_monthly_traffic_summary(fact_traffic_counts_df, dim_date_df)


# COMMAND ----------

# DBTITLE 1,gold_street_risk_summary


@dlt.table(
    name="gold_street_risk_summary",
    comment=_GOLD_STREET_RISK_SUMMARY_CFG["description"],
    # No FOREIGN KEY to dim_street here: dim_street's PRIMARY KEY is the
    # surrogate street_key, not street_id -- street_id repeats across SCD2
    # versions (by design), so it isn't unique in dim_street and can't be a
    # valid FK target there. street_id/year/month are verified NOT NULL in
    # real data (0 nulls found live), safe to declare as the PK.
    schema="""
        street_id                       INT     NOT NULL,
        year                            INT     NOT NULL,
        month                           INT     NOT NULL,
        avg_noise                       DOUBLE,
        avg_pollution                   DOUBLE,
        avg_light                       DOUBLE,
        rain_event_count                LONG,
        dangerous_rating_this_month     DOUBLE,
        dangerous_rating_prior_month    DOUBLE,
        risk_changed_flag               BOOLEAN,
        risk_direction                  STRING,
        load_dt                         TIMESTAMP,
        source_format                   STRING,
        source_file                     STRING,
        run_id                          STRING,
        CONSTRAINT gold_street_risk_summary_pk PRIMARY KEY (street_id, year, month)
    """,
)
def gold_street_risk_summary():
    fact_street_conditions_df = spark.table("fact_street_conditions")
    dim_street_df = spark.table("dim_street")
    dim_date_df = spark.table("dim_date")
    return build_gold_street_risk_summary(fact_street_conditions_df, dim_street_df, dim_date_df)
