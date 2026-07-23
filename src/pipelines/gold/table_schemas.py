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

Column comments are inline `COMMENT '...'` clauses on the same DDL string --
same mechanism, same proven-live code path as the PK/FK constraints above,
so no separate ALTER COLUMN step is needed the way Bronze DLT/Silver's
comment-application notebooks require. A schema/comment-only change here
needs a DLT Full Refresh to actually apply, same as the constraint DDL --
a normal run reuses the table as already created.
"""
from __future__ import annotations

# ============================================================================
# Dimension tables
# ============================================================================

DIM_DATE_SCHEMA = """
    date_key        INT     NOT NULL COMMENT 'Surrogate key, YYYYMMDD as an int.',
    full_date       DATE    COMMENT 'Calendar date, one row per day.',
    year            INT     COMMENT 'Calendar year.',
    month           INT     COMMENT 'Calendar month, 1-12.',
    month_name      STRING  COMMENT 'Full month name, e.g. January.',
    day_of_month    INT     COMMENT 'Day of month, 1-31.',
    day_of_week     INT     COMMENT 'Spark dayofweek(): 1=Sunday..7=Saturday -- NOT ISO-8601 (1=Monday).',
    day_name        STRING  COMMENT 'Full day name, e.g. Monday.',
    quarter         INT     COMMENT 'Calendar quarter, 1-4.',
    is_weekend      BOOLEAN COMMENT 'True for Sat/Sun, using the day_of_week convention (Sunday=1 or Saturday=7).',
    load_dt         TIMESTAMP COMMENT 'Timestamp this row was loaded.',
    source_format   STRING  COMMENT 'Always "generated" -- derived from the observed min/max date across Silver.',
    source_file     STRING  COMMENT 'Always "date_dimension_generator" -- see source_format.',
    run_id          STRING  COMMENT 'Identifier for the pipeline run that generated this row.',
    CONSTRAINT dim_date_pk PRIMARY KEY (date_key)
"""

DIM_LOCATION_SCHEMA = """
    location_key    INT     NOT NULL COMMENT 'Surrogate key.',
    location        INT     COMMENT 'Natural key: intersection identifier, 1-14, from node_locations.csv.',
    latitude        DOUBLE  COMMENT 'Intersection latitude, 6-decimal precision.',
    longitude       DOUBLE  COMMENT 'Intersection longitude, 6-decimal precision.',
    load_dt         TIMESTAMP COMMENT 'Timestamp this row was loaded.',
    source_format   STRING  COMMENT 'File format of the source this row was loaded from.',
    source_file     STRING  COMMENT 'Name of the source file this row was loaded from.',
    run_id          STRING  COMMENT 'Identifier for the pipeline run that loaded this row.',
    CONSTRAINT dim_location_pk PRIMARY KEY (location_key)
"""

DIM_STREET_SCHEMA = """
    street_key      INT     NOT NULL COMMENT 'Surrogate key -- unique per SCD2 version, NOT per street.',
    street_id       INT     COMMENT 'Natural key: street identifier, 1-36, from streets_list.csv.',
    street          STRING  COMMENT 'Street name.',
    long            INT     COMMENT 'Street length in meters -- NOT longitude, despite the name.',
    latitude        DOUBLE  COMMENT 'Street latitude, 6-decimal precision.',
    longitude       DOUBLE  COMMENT 'Street longitude, 6-decimal precision.',
    dangerous       DOUBLE  COMMENT 'Danger rating, valid range 0.1-0.9. SCD2-tracked: a change creates a new version.',
    __START_AT      TIMESTAMP COMMENT 'SCD2: when this version of the row became active.',
    __END_AT        TIMESTAMP COMMENT 'SCD2: when this version stopped being active. NULL for the current version.',
    is_current      BOOLEAN COMMENT 'SCD2: true for exactly one version per street_id -- the currently-active one.',
    load_dt         TIMESTAMP COMMENT 'Timestamp this row was loaded.',
    source_format   STRING  COMMENT 'File format of the source this row was loaded from.',
    source_file     STRING  COMMENT 'Name of the source file this row was loaded from.',
    run_id          STRING  COMMENT 'Identifier for the pipeline run that loaded this row.',
    CONSTRAINT dim_street_pk PRIMARY KEY (street_key)
"""

DIM_TECHNIQUE_SCHEMA = """
    technique_key       INT     NOT NULL COMMENT 'Surrogate key.',
    technique_name      STRING  NOT NULL COMMENT 'Short technique identifier, e.g. copyinto/dlt/autoloader/pyspark.',
    technique_type      STRING  COMMENT 'Category of ingestion technique (batch/streaming/etc.).',
    description         STRING  COMMENT 'Human-readable description of what this technique does.',
    supports_streaming  BOOLEAN COMMENT 'Whether this technique supports incremental/streaming ingestion.',
    created_date        DATE    COMMENT 'Date this technique row was added to the dimension.',
    CONSTRAINT dim_technique_pk PRIMARY KEY (technique_key)
"""

DIM_AUDIT_SCHEMA = """
    audit_key           INT         NOT NULL COMMENT 'Surrogate key.',
    load_dt             TIMESTAMP   NOT NULL COMMENT 'Timestamp the underlying Bronze/Silver row was loaded.',
    source_format       STRING      COMMENT 'File format of the source row (csv/json/xml).',
    source_file         STRING      COMMENT 'Name of the specific source file the row was loaded from.',
    run_id              STRING      NOT NULL COMMENT 'Identifier for the pipeline run that loaded the row.',
    created_timestamp   TIMESTAMP   COMMENT 'Timestamp this Dim_Audit row itself was created.',
    CONSTRAINT dim_audit_pk PRIMARY KEY (audit_key)
"""

DIM_TIME_SCHEMA = """
    time_key            INT     NOT NULL COMMENT 'Surrogate key, HHMMSS as an int.',
    full_time           STRING  NOT NULL COMMENT 'Time of day as HH:mm:ss.',
    hour                INT     NOT NULL COMMENT 'Hour, 0-23.',
    minute              INT     NOT NULL COMMENT 'Minute, 0-59.',
    second              INT     NOT NULL COMMENT 'Second, 0-59.',
    hour_12             INT     COMMENT 'Hour in 12-hour format, 1-12.',
    am_pm               STRING  COMMENT 'AM or PM.',
    time_of_day         STRING  COMMENT 'Named part of day, e.g. Morning/Afternoon/Evening/Night.',
    is_business_hours   BOOLEAN COMMENT 'True if this time falls within standard business hours.',
    minute_of_day       INT     COMMENT 'Minutes elapsed since midnight, 0-1439.',
    second_of_day       LONG    COMMENT 'Seconds elapsed since midnight, 0-86399.',
    CONSTRAINT dim_time_pk PRIMARY KEY (time_key)
"""

# ============================================================================
# fact_city_observations -- the one Gold fact table. ALTERNATIVE DESIGN,
# pending trainer review: no observation_type/observation_id discriminator
# columns. Branch membership (environmental/traffic/telegram) is inferred
# by consumers from which measures are populated -- see
# docs/fact_table_without_discriminator_alternative.md.
# ============================================================================

FACT_CITY_OBSERVATIONS_SCHEMA = """
    street_key          INT     COMMENT 'FK to dim_street. NULL for non-environmental rows.',
    location_key        INT     COMMENT 'FK to dim_location. NULL for non-traffic rows, or the location=7 orphan.',
    date_key            INT     NOT NULL COMMENT 'FK to dim_date.',
    time_key            INT     NOT NULL COMMENT 'FK to dim_time.',
    technique_key        INT     NOT NULL COMMENT 'FK to dim_technique -- which Bronze technique produced this row.',
    audit_key           INT     NOT NULL COMMENT 'FK to dim_audit.',
    noise               DOUBLE  COMMENT 'Environmental measure: noise reading. NULL for non-environmental rows.',
    pollution           DOUBLE  COMMENT 'Environmental measure: pollution reading. NULL for non-environmental rows.',
    light               DOUBLE  COMMENT 'Environmental measure: light reading. NULL for non-environmental rows.',
    raining             DOUBLE  COMMENT 'Environmental measure: rain intensity, valid range 0-100. NULL otherwise.',
    enter               INT     COMMENT 'Traffic measure: vehicles entering. NULL for non-traffic rows.',
    exit                INT     COMMENT 'Traffic measure: vehicles exiting. NULL for non-traffic rows.',
    vehicle_plate_id    INT     COMMENT 'Traffic measure: reading sequence number (cars.csv id). NULL otherwise.',
    message_count       INT     COMMENT 'Telegram measure: citizen reports in this observation. NULL for other rows.',
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
# agg_daily_street_conditions -- daily environmental rollup per street
# ============================================================================

# Grain is the street_key/date_key pair itself (no surrogate key minted for
# an aggregate table) -- composite PRIMARY KEY, same convention DLT accepts
# for dimensions' single-column PKs. No street=NULL orphan case (unlike
# location=7): every street_id has a dim_street row, so this PK can never be
# NULL.
AGG_DAILY_STREET_CONDITIONS_SCHEMA = """
    street_key            INT     NOT NULL COMMENT 'FK to dim_street, part of this composite grain.',
    street_id             INT     COMMENT 'Natural key, denormalized from dim_street.',
    street                STRING  COMMENT 'Street name, denormalized from dim_street.',
    date_key              INT     NOT NULL COMMENT 'FK to dim_date, part of this composite grain.',
    full_date             DATE    COMMENT 'Calendar date, denormalized from dim_date.',
    year                  INT     COMMENT 'Calendar year, denormalized from dim_date.',
    month                 INT     COMMENT 'Calendar month, denormalized from dim_date.',
    day_name              STRING  COMMENT 'Day name, denormalized from dim_date.',
    is_weekend            BOOLEAN COMMENT 'Weekend flag, denormalized from dim_date.',
    avg_noise             DOUBLE  COMMENT 'Average noise reading for this street on this date.',
    max_noise             DOUBLE  COMMENT 'Maximum noise reading for this street on this date.',
    min_noise             DOUBLE  COMMENT 'Minimum noise reading for this street on this date.',
    avg_pollution         DOUBLE  COMMENT 'Average pollution reading for this street on this date.',
    max_pollution         DOUBLE  COMMENT 'Maximum pollution reading for this street on this date.',
    min_pollution         DOUBLE  COMMENT 'Minimum pollution reading for this street on this date.',
    avg_light             DOUBLE  COMMENT 'Average light reading for this street on this date.',
    max_light             DOUBLE  COMMENT 'Maximum light reading for this street on this date.',
    min_light             DOUBLE  COMMENT 'Minimum light reading for this street on this date.',
    rain_intensity_sum    DOUBLE  COMMENT 'Sum of rain intensity readings for this street on this date.',
    observation_count     BIGINT  COMMENT 'Raw environmental observations rolled up into this row.',
    load_dt               TIMESTAMP COMMENT 'Timestamp this row was computed.',
    source_format         STRING  COMMENT 'Always "aggregated" -- a Gold rollup, not a direct source load.',
    source_file           STRING  COMMENT 'Identifies this as a derived aggregate, not a source file.',
    run_id                STRING  COMMENT 'Identifier for the pipeline run that computed this row.',
    CONSTRAINT agg_daily_street_conditions_pk PRIMARY KEY (street_key, date_key),
    CONSTRAINT agg_daily_street_conditions_street_fk
        FOREIGN KEY (street_key) REFERENCES dim_street(street_key),
    CONSTRAINT agg_daily_street_conditions_date_fk
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
"""

# ============================================================================
# agg_daily_location_traffic -- daily traffic rollup per location
# ============================================================================

# No PRIMARY KEY: location_key is legitimately NULL for one group (the
# location=7 orphan -- traffic readings whose location was excluded from
# Dim_Location at Silver for bad coordinates, see docs/gold_data_model.md).
# A PRIMARY KEY member can't be NULL, same reasoning the old
# gold_location_summary applied. The FOREIGN KEY is unaffected -- FK columns
# are allowed to be NULL.
AGG_DAILY_LOCATION_TRAFFIC_SCHEMA = """
    location_key          INT     COMMENT 'FK to dim_location. NULL for the location=7 orphan group.',
    location              INT     COMMENT 'Natural key, denormalized from dim_location.',
    latitude              DOUBLE  COMMENT 'Intersection latitude, denormalized from dim_location.',
    longitude             DOUBLE  COMMENT 'Intersection longitude, denormalized from dim_location.',
    date_key              INT     NOT NULL COMMENT 'FK to dim_date, part of this composite grain.',
    full_date             DATE    COMMENT 'Calendar date, denormalized from dim_date.',
    year                  INT     COMMENT 'Calendar year, denormalized from dim_date.',
    month                 INT     COMMENT 'Calendar month, denormalized from dim_date.',
    day_name              STRING  COMMENT 'Day name, denormalized from dim_date.',
    is_weekend            BOOLEAN COMMENT 'Weekend flag, denormalized from dim_date.',
    total_enter           BIGINT  COMMENT 'Total vehicles entering this intersection on this date.',
    total_exit            BIGINT  COMMENT 'Total vehicles exiting this intersection on this date.',
    net_traffic           BIGINT  COMMENT 'total_enter minus total_exit for this intersection/date.',
    avg_enter             DOUBLE  COMMENT 'Average vehicles entering per reading.',
    avg_exit              DOUBLE  COMMENT 'Average vehicles exiting per reading.',
    max_enter             INT     COMMENT 'Maximum vehicles entering in a single reading.',
    max_exit              INT     COMMENT 'Maximum vehicles exiting in a single reading.',
    observation_count     BIGINT  COMMENT 'Raw traffic observations rolled up into this row.',
    load_dt               TIMESTAMP COMMENT 'Timestamp this row was computed.',
    source_format         STRING  COMMENT 'Always "aggregated" -- a Gold rollup, not a direct source load.',
    source_file           STRING  COMMENT 'Identifies this as a derived aggregate, not a source file.',
    run_id                STRING  COMMENT 'Identifier for the pipeline run that computed this row.',
    CONSTRAINT agg_daily_location_traffic_location_fk
        FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
    CONSTRAINT agg_daily_location_traffic_date_fk
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
"""

# ============================================================================
# agg_monthly_street_summary -- monthly environmental rollup per street
# ============================================================================

# No FK on (year, month): dim_date's grain is one row per day, so there's no
# single dim_date row a (year, month) pair could reference -- same reasoning
# as not inventing a fake relationship elsewhere in this model.
AGG_MONTHLY_STREET_SUMMARY_SCHEMA = """
    street_key            INT     NOT NULL COMMENT 'FK to dim_street, part of this composite grain.',
    street_id             INT     COMMENT 'Natural key, denormalized from dim_street.',
    street                STRING  COMMENT 'Street name, denormalized from dim_street.',
    dangerous             DOUBLE  COMMENT 'Danger rating, denormalized from dim_street''s current version.',
    year                  INT     NOT NULL COMMENT 'Calendar year, part of this composite grain.',
    month                 INT     NOT NULL COMMENT 'Calendar month, part of this composite grain.',
    avg_noise             DOUBLE  COMMENT 'Average noise reading for this street in this month.',
    avg_pollution         DOUBLE  COMMENT 'Average pollution reading for this street in this month.',
    avg_light             DOUBLE  COMMENT 'Average light reading for this street in this month.',
    max_noise             DOUBLE  COMMENT 'Maximum noise reading for this street in this month.',
    max_pollution         DOUBLE  COMMENT 'Maximum pollution reading for this street in this month.',
    max_light             DOUBLE  COMMENT 'Maximum light reading for this street in this month.',
    days_with_rain        BIGINT  COMMENT 'Distinct days this month with a nonzero rain reading for this street.',
    observation_count     BIGINT  COMMENT 'Raw environmental observations rolled up into this row.',
    observation_days      BIGINT  COMMENT 'Distinct days with at least one observation this month.',
    load_dt               TIMESTAMP COMMENT 'Timestamp this row was computed.',
    source_format         STRING  COMMENT 'Always "aggregated" -- a Gold rollup, not a direct source load.',
    source_file           STRING  COMMENT 'Identifies this as a derived aggregate, not a source file.',
    run_id                STRING  COMMENT 'Identifier for the pipeline run that computed this row.',
    CONSTRAINT agg_monthly_street_summary_pk PRIMARY KEY (street_key, year, month),
    CONSTRAINT agg_monthly_street_summary_street_fk
        FOREIGN KEY (street_key) REFERENCES dim_street(street_key)
"""

# ============================================================================
# agg_hourly_telegram_activity -- hourly telegram message-volume rollup
# ============================================================================

# No FK to dim_time: this table's grain collapses time_key down to just
# `hour` (0-23), which isn't dim_time's PRIMARY KEY (time_key, HHMMSS) --
# declaring a FOREIGN KEY against a non-PK column isn't a real relationship,
# so it's left undeclared rather than faked.
AGG_HOURLY_TELEGRAM_ACTIVITY_SCHEMA = """
    date_key              INT     NOT NULL COMMENT 'FK to dim_date, part of this composite grain.',
    full_date             DATE    COMMENT 'Calendar date, denormalized from dim_date.',
    year                  INT     COMMENT 'Calendar year, denormalized from dim_date.',
    month                 INT     COMMENT 'Calendar month, denormalized from dim_date.',
    day_name              STRING  COMMENT 'Day name, denormalized from dim_date.',
    is_weekend            BOOLEAN COMMENT 'Weekend flag, denormalized from dim_date.',
    hour                  INT     NOT NULL COMMENT 'Hour 0-23. Not FK -- dim_time''s PK is (time_key, HHMMSS).',
    total_messages        BIGINT  COMMENT 'Total citizen reports received in this date/hour.',
    observation_count     BIGINT  COMMENT 'Raw telegram observations rolled up into this row.',
    load_dt               TIMESTAMP COMMENT 'Timestamp this row was computed.',
    source_format         STRING  COMMENT 'Always "aggregated" -- a Gold rollup, not a direct source load.',
    source_file           STRING  COMMENT 'Identifies this as a derived aggregate, not a source file.',
    run_id                STRING  COMMENT 'Identifier for the pipeline run that computed this row.',
    CONSTRAINT agg_hourly_telegram_activity_pk PRIMARY KEY (date_key, hour),
    CONSTRAINT agg_hourly_telegram_activity_date_fk
        FOREIGN KEY (date_key) REFERENCES dim_date(date_key)
"""
