"""fact_city_observations - Unified fact table for all city observations.

Combines environmental sensor readings, traffic counts, and telegram messages
into a single polymorphic fact table using an observation_type discriminator.

Grain: One row per observation event (sensor reading, traffic count, or message).
Size: ~112.6M rows (87.8M environmental + 24.7M traffic + 128K telegram).
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# The one real ingestion technique behind every non-chunked Bronze source
# (bronze_streets, bronze_telegram -- see src/config/sources.yml's
# `technique: copy_into` on both). Environmental/telegram silver tables carry
# no per-row source_technique column (unlike silver_traffic, which genuinely
# varies across 4 techniques and is resolved via a real join below), so their
# technique_key is looked up once rather than joined per row.
_SINGLE_SOURCE_TECHNIQUE_NAME = "copyinto"


def _single_technique_lookup(dim_technique_df: DataFrame, technique_name: str) -> DataFrame:
    """A lazy, one-row DataFrame to crossJoin technique_key onto every
    environmental/telegram row -- NOT a collected scalar.

    An earlier version of this function called .collect() to resolve
    technique_name to a plain Python int, then used F.lit(...) to stamp it
    onto every row. That broke DLT's flow resolution: dim_technique is a
    sibling table computed in this SAME pipeline run (unlike dim_date's
    compute_date_range(), which safely collects over already-materialized
    upstream SILVER tables from a separate pipeline) -- .collect() forces
    an eager action during DLT's graph-analysis pass, before dim_technique's
    own flow has actually executed, and reads whatever (stale/absent) state
    happens to exist at that moment. Confirmed live:
    "dim_technique has no row for technique_name='copyinto'" raised 8
    seconds into a fresh full-refresh, well before any table had run.

    Staying lazy (a crossJoin, not a collect) lets Spark/DLT resolve this
    against dim_technique's real output at actual execution time, in the
    correct dependency order. If technique_name has no match, this returns
    zero rows and the crossJoin below makes the affected observation_type
    disappear entirely from the union -- a loud, easy-to-catch row-count
    failure, not a silent NULL.
    """
    return dim_technique_df.filter(F.col("technique_name") == technique_name).select(
        F.col("technique_key").alias("_single_technique_key")
    )


def build_fact_city_observations(
    traffic_df: DataFrame,
    environment_df: DataFrame,
    telegram_df: DataFrame,
    dim_street_df: DataFrame,
    dim_location_df: DataFrame,
    dim_date_df: DataFrame,
    dim_audit_df: DataFrame,
    dim_technique_df: DataFrame,
) -> DataFrame:
    """
    Build unified fact_city_observations combining environmental, traffic, and telegram data.

    Uses observation_type discriminator pattern:
        - 'environmental': has street_key, env measures, NULL traffic/telegram measures
        - 'traffic': has location_key, traffic measures, NULL env/telegram measures
        - 'telegram': has NO street/location, telegram measures, NULL env/traffic measures

    All observation types share: date_key, time_key, technique_key, audit_key

    Args:
        traffic_df: Silver traffic table
        environment_df: Silver environment table
        telegram_df: Silver telegram table (has event_timestamp, NOT date/hour)
        dim_street_df: Street dimension (for environmental FK)
        dim_location_df: Location dimension (for traffic FK)
        dim_date_df: Date dimension (shared)
        dim_audit_df: Audit dimension (shared)
        dim_technique_df: Technique dimension (shared) -- traffic resolves
            technique_key per row from its own source_technique column;
            environmental/telegram resolve to the single "copyinto" technique
            (see _SINGLE_SOURCE_TECHNIQUE_NAME).

    Returns:
        Unified fact DataFrame with all observation types
    """
    single_technique_df = _single_technique_lookup(dim_technique_df, _SINGLE_SOURCE_TECHNIQUE_NAME)

    # Build environmental observations
    environmental_fact = _build_environmental_observations(
        environment_df, dim_street_df, dim_date_df, dim_audit_df, single_technique_df
    )

    # Build traffic observations
    traffic_fact = _build_traffic_observations(
        traffic_df, dim_location_df, dim_date_df, dim_audit_df, dim_technique_df
    )

    # Build telegram observations
    telegram_fact = _build_telegram_observations(telegram_df, dim_date_df, dim_audit_df, single_technique_df)

    # Union all observation types -- unionByName (not union) so a future
    # reordering of any one branch's select() can't silently misalign
    # columns by position.
    unified_fact = environmental_fact.unionByName(traffic_fact).unionByName(telegram_fact)

    return unified_fact


def _build_environmental_observations(
    environment_df: DataFrame,
    dim_street_df: DataFrame,
    dim_date_df: DataFrame,
    dim_audit_df: DataFrame,
    single_technique_df: DataFrame,
) -> DataFrame:
    """
    Transform environmental silver data into fact observations.

    observation_type = 'environmental'
    FK: street_key (yes), location_key (NULL), date_key, time_key, technique_key, audit_key
    Measures: noise, pollution, light, raining
    """
    # Join to get street_key
    env_with_street = environment_df.alias("env").join(
        dim_street_df.filter(F.col("is_current")).alias("street"),
        F.col("env.street_id") == F.col("street.street_id"),
        "left",
    )

    # Join to get date_key -- env.date is a TIMESTAMP with a real
    # time-of-day component, dim_date.full_date is a DATE. Comparing them
    # directly would implicitly promote full_date to midnight-timestamp and
    # only match rows whose timestamp happens to be exactly 00:00:00 --
    # F.to_date() normalizes env.date to a DATE first.
    env_with_date = env_with_street.join(
        dim_date_df.alias("date"),
        F.to_date(F.col("env.date")) == F.col("date.full_date"),
        "left",
    )

    # Join to get audit_key
    env_with_audit = env_with_date.join(
        dim_audit_df.alias("audit"),
        (F.col("env.load_dt") == F.col("audit.load_dt"))
        & (F.col("env.source_format") == F.col("audit.source_format"))
        & (F.col("env.source_file") == F.col("audit.source_file"))
        & (F.col("env.run_id") == F.col("audit.run_id")),
        "left",
    )

    # crossJoin against the single-row technique lookup -- lazy, not a
    # collected scalar (see _single_technique_lookup's Notes).
    env_with_technique = env_with_audit.crossJoin(F.broadcast(single_technique_df))

    # Build environmental fact
    return env_with_technique.select(
        F.lit("environmental").alias("observation_type"),
        # Foreign keys
        F.col("street.street_key"),
        F.lit(None).cast("int").alias("location_key"),
        F.col("date.date_key"),
        F.lit(0).cast("int").alias("time_key"),  # TODO: extract from timestamp when available
        F.col("_single_technique_key").cast("int").alias("technique_key"),
        F.col("audit.audit_key"),
        # Environmental measures
        F.col("env.noise"),
        F.col("env.pollution"),
        F.col("env.light"),
        F.col("env.raining"),
        # Traffic measures (NULL)
        F.lit(None).cast("int").alias("enter"),
        F.lit(None).cast("int").alias("exit"),
        # Telegram measures (NULL)
        F.lit(None).cast("int").alias("message_count"),
        F.lit(None).cast("int").alias("message_length"),
        # Degenerate dimension
        F.concat(F.lit("env_"), F.col("env.street_id"), F.lit("_"), F.col("date.date_key")).alias("observation_id"),
    )


def _build_traffic_observations(
    traffic_df: DataFrame,
    dim_location_df: DataFrame,
    dim_date_df: DataFrame,
    dim_audit_df: DataFrame,
    dim_technique_df: DataFrame,
) -> DataFrame:
    """
    Transform traffic silver data into fact observations.

    observation_type = 'traffic'
    FK: street_key (NULL), location_key (yes), date_key, time_key, technique_key, audit_key
    Measures: enter, exit

    technique_key resolves per row from traffic.source_technique -- unlike
    environmental/telegram, silver_traffic genuinely varies across all 4
    real ingestion techniques (copyinto/dlt/autoloader/pyspark, confirmed
    live in docs/gold_data_model.md), so this must be a real join against
    dim_technique.technique_name, not a hardcoded constant.
    """
    # Join to get location_key
    traffic_with_location = traffic_df.alias("traffic").join(
        dim_location_df.alias("location"),
        F.col("traffic.location") == F.col("location.location"),
        "left",
    )

    # Join to get date_key -- see _build_environmental_observations' Notes
    # for why F.to_date() is required here (traffic.date also carries a
    # real time-of-day component).
    traffic_with_date = traffic_with_location.join(
        dim_date_df.alias("date"),
        F.to_date(F.col("traffic.date")) == F.col("date.full_date"),
        "left",
    )

    # Join to get technique_key
    traffic_with_technique = traffic_with_date.join(
        dim_technique_df.select(
            F.col("technique_key").alias("_technique_key"), F.col("technique_name").alias("_technique_name")
        ),
        F.col("traffic.source_technique") == F.col("_technique_name"),
        "left",
    )

    # Join to get audit_key
    traffic_with_audit = traffic_with_technique.join(
        dim_audit_df.alias("audit"),
        (F.col("traffic.load_dt") == F.col("audit.load_dt"))
        & (F.col("traffic.source_format") == F.col("audit.source_format"))
        & (F.col("traffic.source_file") == F.col("audit.source_file"))
        & (F.col("traffic.run_id") == F.col("audit.run_id")),
        "left",
    )

    # Build traffic fact
    return traffic_with_audit.select(
        F.lit("traffic").alias("observation_type"),
        # Foreign keys
        F.lit(None).cast("int").alias("street_key"),
        F.col("location.location_key"),
        F.col("date.date_key"),
        F.lit(0).cast("int").alias("time_key"),  # TODO: extract from timestamp when available
        F.col("_technique_key").cast("int").alias("technique_key"),
        F.col("audit.audit_key"),
        # Environmental measures (NULL)
        F.lit(None).cast("double").alias("noise"),
        F.lit(None).cast("double").alias("pollution"),
        F.lit(None).cast("double").alias("light"),
        F.lit(None).cast("double").alias("raining"),
        # Traffic measures
        F.col("traffic.enter"),
        F.col("traffic.exit"),
        # Telegram measures (NULL)
        F.lit(None).cast("int").alias("message_count"),
        F.lit(None).cast("int").alias("message_length"),
        # Degenerate dimension
        F.concat(F.lit("traffic_"), F.col("traffic.location"), F.lit("_"), F.col("date.date_key")).alias(
            "observation_id"
        ),
    )


def _build_telegram_observations(
    telegram_df: DataFrame,
    dim_date_df: DataFrame,
    dim_audit_df: DataFrame,
    single_technique_df: DataFrame,
) -> DataFrame:
    """
    Transform telegram silver data into fact observations.

    observation_type = 'telegram'
    FK: street_key (NULL), location_key (NULL), date_key, time_key, technique_key, audit_key
    Measures: message_count, message_length

    SIMPLE integration - no text parsing, no NLP!

    NOTE: Silver telegram table has event_timestamp (TIMESTAMP), NOT separate date/hour columns
    """
    # Extract date and time from event_timestamp
    telegram_with_keys = telegram_df.alias("telegram").withColumn(
        "parsed_date", F.to_date(F.col("event_timestamp"))
    ).withColumn(
        "time_key",
        # Extract HHMMSS integer from timestamp
        (
            F.hour(F.col("event_timestamp")) * 10000
            + F.minute(F.col("event_timestamp")) * 100
            + F.second(F.col("event_timestamp"))
        ).cast("int"),
    )

    # Join to get date_key
    telegram_with_date = telegram_with_keys.join(
        dim_date_df.alias("date"),
        F.col("parsed_date") == F.col("date.full_date"),
        "left",
    )

    # Join to get audit_key
    telegram_with_audit = telegram_with_date.join(
        dim_audit_df.alias("audit"),
        (F.col("telegram.load_dt") == F.col("audit.load_dt"))
        & (F.col("telegram.source_format") == F.col("audit.source_format"))
        & (F.col("telegram.source_file") == F.col("audit.source_file"))
        & (F.col("telegram.run_id") == F.col("audit.run_id")),
        "left",
    )

    # crossJoin against the single-row technique lookup -- lazy, not a
    # collected scalar (see _single_technique_lookup's Notes).
    telegram_with_technique = telegram_with_audit.crossJoin(F.broadcast(single_technique_df))

    # Build telegram fact
    return telegram_with_technique.select(
        F.lit("telegram").alias("observation_type"),
        # Foreign keys
        F.lit(None).cast("int").alias("street_key"),
        F.lit(None).cast("int").alias("location_key"),
        F.col("date.date_key"),
        # time_key was added via withColumn() after .alias("telegram"), so it
        # carries no table qualifier -- F.col("telegram.time_key") fails to
        # resolve (confirmed live: UNRESOLVED_COLUMN). Reference it bare.
        F.col("time_key"),
        F.col("_single_technique_key").cast("int").alias("technique_key"),
        F.col("audit.audit_key"),
        # Environmental measures (NULL)
        F.lit(None).cast("double").alias("noise"),
        F.lit(None).cast("double").alias("pollution"),
        F.lit(None).cast("double").alias("light"),
        F.lit(None).cast("double").alias("raining"),
        # Traffic measures (NULL)
        F.lit(None).cast("int").alias("enter"),
        F.lit(None).cast("int").alias("exit"),
        # Telegram measures (SIMPLE!)
        F.lit(1).alias("message_count"),
        F.length(F.col("telegram.message")).alias("message_length"),
        # Degenerate dimension
        F.concat(F.lit("telegram_"), F.col("date.date_key"), F.lit("_"), F.col("time_key")).alias(
            "observation_id"
        ),
    )
