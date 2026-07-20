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

FACT_TRAFFIC_COUNTS_SCHEMA = """
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
"""

FACT_STREET_CONDITIONS_SCHEMA = """
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
"""

# No PRIMARY KEY: the grain is (location_key, year, month), but location_key
# is legitimately NULL for 10 of 140 rows (the location=7 orphan -- real
# silver_traffic readings for a location excluded from Dim_Location at
# Silver for bad coordinates, see docs/gold_data_model.md). A PRIMARY KEY
# member can't be NULL. The FOREIGN KEY below is unaffected -- FK columns
# are allowed to be NULL, same as Fact_Traffic_Counts.location_key.
GOLD_MONTHLY_TRAFFIC_SUMMARY_SCHEMA = """
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
"""

# No FOREIGN KEY to dim_street: Dim_Street's primary key is the surrogate
# street_key, not street_id -- street_id repeats across SCD2 versions by
# design, so it isn't unique in Dim_Street and can't be a valid FK target
# there. street_id/year/month are verified NOT NULL in real data (0 nulls
# found live), safe to declare as the PK.

FACT_DAILY_SUMMARY_SCHEMA = """
    date_key                    INT         NOT NULL,
    total_vehicles_entered      BIGINT      NOT NULL,
    total_vehicles_exited       BIGINT      NOT NULL,
    net_traffic_flow            BIGINT      NOT NULL,
    avg_noise                   DOUBLE      NOT NULL,
    avg_pollution               DOUBLE      NOT NULL,
    avg_light                   DOUBLE      NOT NULL,
    avg_raining                 DOUBLE      NOT NULL,
    max_pollution               DOUBLE      NOT NULL,
    dangerous_streets_count     INT         NOT NULL,
    telegram_message_count      INT         NOT NULL,
    load_dt                     TIMESTAMP   NOT NULL,
    source_format               STRING,
    source_file                 STRING,
    run_id                      STRING,
    CONSTRAINT fact_daily_summary_pk PRIMARY KEY (date_key),
    CONSTRAINT fact_daily_summary_date_fk
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
"""

GOLD_STREET_RISK_SUMMARY_SCHEMA = """
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
"""
