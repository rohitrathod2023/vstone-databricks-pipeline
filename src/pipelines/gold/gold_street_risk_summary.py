"""gold_street_risk_summary: one row per street per calendar month,
aggregated from Fact_Street_Conditions joined back to Dim_Street's full SCD2
version history. This is the domain's "churn-style metric" -- whether a
street's danger classification changes over time -- made possible only
because Dim_Street tracks history instead of overwriting it.
"""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.audit import add_audit_columns

DANGEROUS_THRESHOLD = 0.5


def build_gold_street_risk_summary(
    fact_street_conditions_df: DataFrame, dim_street_df: DataFrame, dim_date_df: DataFrame
) -> DataFrame:
    """Aggregate Fact_Street_Conditions to one row per street per calendar month.

    Args:
        fact_street_conditions_df: Fact_Street_Conditions (street_key,
            date_key, noise, pollution, light, raining, ...).
        dim_street_df: Dim_Street's full version history (street_key,
            street_id, dangerous, __START_AT, __END_AT, ...) -- every SCD2
            version, not just current, so month-over-month comparison is
            possible once a street's rating actually changes.
        dim_date_df: Dim_Date (date_key, year, month, ...).

    Returns:
        DataFrame with street_id, year, month, avg_noise, avg_pollution,
        avg_light, rain_event_count, dangerous_rating_this_month,
        dangerous_rating_prior_month, risk_changed_flag, risk_direction,
        plus the 4 standard audit columns.

    Notes:
        dangerous_rating_this_month takes the FIRST non-null dangerous
        value across the readings grouped into that street+month, not an
        average -- dangerous is a dimensional attribute (constant for
        whichever Dim_Street version each reading resolved to), not a
        continuous measurement, so averaging it is the wrong operation
        and was tried first: F.avg() over millions of identical double
        values accumulates floating-point summation error on the order of
        1e-14 between differently-sized groups, which was enough to make
        a truly constant rating look like it "increased"/"decreased"
        month over month. F.first() picks the stored value directly with
        no summation, so a street with one Dim_Street version reports the
        exact same value every month (verified live: distinct-value count
        per street dropped from up to 10 to 1 after this fix).

        risk_changed_flag/risk_direction compare against the immediately
        preceding calendar month for the same street_id via LAG. On this
        first build every row's risk_direction is "stable" and
        risk_changed_flag is False -- there is no Dim_Street history yet
        for any street's rating to have changed against, and even the
        very first calendar month of data has no prior month to compare
        to. This is expected, not a bug: the metric only becomes
        meaningful once real SCD2 history accumulates across months.
    """
    with_street = fact_street_conditions_df.alias("f").join(
        dim_street_df.select("street_key", "street_id", "dangerous").alias("s"),
        F.col("f.street_key") == F.col("s.street_key"),
        "left",
    )
    with_date = with_street.join(
        dim_date_df.select("date_key", "year", "month").alias("d"),
        F.col("f.date_key") == F.col("d.date_key"),
        "left",
    )

    grouped = with_date.groupBy(F.col("s.street_id").alias("street_id"), "d.year", "d.month").agg(
        F.avg("f.noise").alias("avg_noise"),
        F.avg("f.pollution").alias("avg_pollution"),
        F.avg("f.light").alias("avg_light"),
        F.count(F.when(F.col("f.raining") > 100, 1)).alias("rain_event_count"),
        F.first("s.dangerous", ignorenulls=True).alias("dangerous_rating_this_month"),
    )

    month_window = Window.partitionBy("street_id").orderBy("year", "month")
    with_prior = grouped.withColumn(
        "dangerous_rating_prior_month", F.lag("dangerous_rating_this_month").over(month_window)
    )

    is_dangerous_this_month = F.col("dangerous_rating_this_month") >= DANGEROUS_THRESHOLD
    is_dangerous_prior_month = F.col("dangerous_rating_prior_month") >= DANGEROUS_THRESHOLD

    with_flag = with_prior.withColumn(
        "risk_changed_flag",
        F.when(
            F.col("dangerous_rating_prior_month").isNull(),
            F.lit(False),
        ).otherwise(is_dangerous_this_month != is_dangerous_prior_month),
    ).withColumn(
        "risk_direction",
        F.when(F.col("dangerous_rating_prior_month").isNull(), F.lit("stable"))
        .when(F.col("dangerous_rating_this_month") > F.col("dangerous_rating_prior_month"), F.lit("increased"))
        .when(F.col("dangerous_rating_this_month") < F.col("dangerous_rating_prior_month"), F.lit("decreased"))
        .otherwise(F.lit("stable")),
    )

    return add_audit_columns(with_flag, source_format="generated", source_file="gold_street_risk_summary_aggregation")
