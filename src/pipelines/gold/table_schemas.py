"""Explicit column schemas + PK/FK constraints for every Gold DLT table, kept
out of dlt_gold_tables.py so the notebook only wires functions to tables and
doesn't also carry long inline DDL strings.

Each constant is passed straight to `schema=` in a `@dlt.table(...)` call.
Lakeflow Declarative Pipelines accepts `CONSTRAINT ... PRIMARY KEY`/
`FOREIGN KEY` clauses inline here, at table-creation time -- a different
code path from `ALTER TABLE ... ADD CONSTRAINT` (which fails against these
tables, since Unity Catalog registers DLT materialized views as VIEW
objects). Confirmed live via information_schema.table_constraints /
key_column_usage: every constraint below really is registered, and
information_schema.tables.table_type still reports MATERIALIZED_VIEW
throughout -- see docs/gold_data_model.md for the full writeup.
"""
from __future__ import annotations

# ============================================================================
# Dimension tables
# ============================================================================

DIM_DATE_SCHEMA = """
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
"""

DIM_LOCATION_SCHEMA = """
    location_key    INT     NOT NULL,
    location        INT,
    latitude        DOUBLE,
    longitude       DOUBLE,
    load_dt         TIMESTAMP,
    source_format   STRING,
    source_file     STRING,
    run_id          STRING,
    CONSTRAINT dim_location_pk PRIMARY KEY (location_key)
"""

DIM_STREET_SCHEMA = """
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
"""

DIM_TECHNIQUE_SCHEMA = """
    technique_key       INT     NOT NULL,
    technique_name      STRING  NOT NULL,
    technique_type      STRING,
    description         STRING,
    supports_streaming  BOOLEAN,
    created_date        DATE,
    CONSTRAINT dim_technique_pk PRIMARY KEY (technique_key)
"""

DIM_AUDIT_SCHEMA = """
    audit_key           INT         NOT NULL,
    load_dt             TIMESTAMP   NOT NULL,
    source_format       STRING,
    source_file         STRING,
    run_id              STRING      NOT NULL,
    created_timestamp   TIMESTAMP,
    CONSTRAINT dim_audit_pk PRIMARY KEY (audit_key)
"""

DIM_TIME_SCHEMA = """
    time_key            INT     NOT NULL,
    full_time           STRING  NOT NULL,
    hour                INT     NOT NULL,
    minute              INT     NOT NULL,
    second              INT     NOT NULL,
    hour_12             INT,
    am_pm               STRING,
    time_of_day         STRING,
    is_business_hours   BOOLEAN,
    minute_of_day       INT,
    second_of_day       LONG,
    CONSTRAINT dim_time_pk PRIMARY KEY (time_key)
"""

# ============================================================================
# fact_city_observations -- the one Gold fact table. observation_type
# discriminates between 'environmental'/'traffic'/'telegram' rows; each
# branch populates its own measures and leaves the others NULL.
# ============================================================================

FACT_CITY_OBSERVATIONS_SCHEMA = """
    observation_type    STRING  NOT NULL,
    street_key          INT,
    location_key        INT,
    date_key            INT     NOT NULL,
    time_key            INT     NOT NULL,
    technique_key       INT     NOT NULL,
    audit_key           INT     NOT NULL,
    noise               DOUBLE,
    pollution           DOUBLE,
    light               DOUBLE,
    raining             DOUBLE,
    enter               INT,
    exit                INT,
    message_count       INT,
    message_length      INT,
    observation_id      STRING,
    CONSTRAINT fact_city_observations_street_fk
        FOREIGN KEY (street_key) REFERENCES dim_street(street_key),
    CONSTRAINT fact_city_observations_location_fk
        FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
    CONSTRAINT fact_city_observations_date_fk
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    CONSTRAINT fact_city_observations_time_fk
        FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
    CONSTRAINT fact_city_observations_technique_fk
        FOREIGN KEY (technique_key) REFERENCES dim_technique(technique_key),
    CONSTRAINT fact_city_observations_audit_fk
        FOREIGN KEY (audit_key) REFERENCES dim_audit(audit_key)
"""

# ============================================================================
# gold_daily_summary -- simple daily rollup of fact_city_observations
# ============================================================================

# No cluster_by/partition spec: at one row per date (~283 rows on the real
# dataset), partitioning by date_key would create one tiny file per
# partition with no pruning benefit -- same reasoning already applied
# elsewhere in this project for small aggregate tables.
GOLD_DAILY_SUMMARY_SCHEMA = """
    date_key                    INT     NOT NULL,
    total_vehicles_entered      BIGINT,
    total_vehicles_exited       BIGINT,
    net_traffic_flow            BIGINT,
    avg_noise                   DOUBLE,
    avg_pollution               DOUBLE,
    avg_light                   DOUBLE,
    avg_raining                 DOUBLE,
    telegram_message_count      BIGINT,
    total_observations          BIGINT,
    load_dt                     TIMESTAMP,
    source_format                STRING,
    source_file                 STRING,
    run_id                      STRING,
    CONSTRAINT gold_daily_summary_pk PRIMARY KEY (date_key),
    CONSTRAINT gold_daily_summary_date_fk
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
"""

# ============================================================================
# gold_location_summary -- simple per-location traffic rollup, answers the
# brief's "busiest intersections" question
# ============================================================================

# No PRIMARY KEY: location_key is legitimately NULL for one group (the
# location=7 orphan -- traffic readings whose location was excluded from
# Dim_Location at Silver for bad coordinates, see docs/gold_data_model.md).
# A PRIMARY KEY member can't be NULL. The FOREIGN KEY below is unaffected --
# FK columns are allowed to be NULL.
GOLD_LOCATION_SUMMARY_SCHEMA = """
    location_key                INT,
    total_vehicles_entered      BIGINT,
    total_vehicles_exited       BIGINT,
    total_traffic_volume        BIGINT,
    total_traffic_readings      BIGINT,
    load_dt                     TIMESTAMP,
    source_format               STRING,
    source_file                 STRING,
    run_id                      STRING,
    CONSTRAINT gold_location_summary_location_fk
        FOREIGN KEY (location_key) REFERENCES dim_location(location_key)
"""
