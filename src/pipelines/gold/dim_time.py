"""dim_time - Time-of-day dimension for intraday analysis.

Generates a complete time dimension with 86,400 rows (one per second of day:
00:00:00 - 23:59:59) to support rush hour analysis, hourly patterns, and
business hours filtering.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


def build_dim_time(spark: SparkSession) -> DataFrame:
    """
    Build dim_time dimension with all 86,400 seconds of the day.

    Generates time attributes for each second: hour, minute, time_of_day period,
    business hours flag, etc.

    Args:
        spark: SparkSession

    Returns:
        DataFrame with columns:
            - time_key (INT): Surrogate key in HHMMSS format (e.g. 143052 = 2:30:52 PM)
            - full_time (STRING): Time in HH:mm:ss format
            - hour (INT): Hour (0-23)
            - minute (INT): Minute (0-59)
            - second (INT): Second (0-59)
            - hour_12 (INT): Hour in 12-hour format (1-12)
            - am_pm (STRING): 'AM' or 'PM'
            - time_of_day (STRING): Period name
            - is_business_hours (BOOLEAN): TRUE if 9 AM - 5 PM
            - minute_of_day (INT): Minutes since midnight (0-1439)
            - second_of_day (LONG): Seconds since midnight (0-86399) - LONG not INT
    """
    # Generate all 86,400 seconds (0 to 86399)
    # spark.range() returns LONG type, which we keep for second_of_day
    seconds_df = spark.range(0, 86400).toDF("second_of_day")

    hour_expr = (F.col("second_of_day") / 3600).cast(IntegerType())
    minute_expr = ((F.col("second_of_day") % 3600) / 60).cast(IntegerType())
    second_expr = (F.col("second_of_day") % 60).cast(IntegerType())

    # Calculate all time components in one select() for better performance
    time_df = seconds_df.select(
        # Base column - keep as LONG (from spark.range)
        F.col("second_of_day"),
        # Derived integer components
        hour_expr.alias("hour"),
        minute_expr.alias("minute"),
        second_expr.alias("second"),
        (F.col("second_of_day") / 60).cast(IntegerType()).alias("minute_of_day"),
        # time_key: HHMMSS format
        (hour_expr * 10000 + minute_expr * 100 + second_expr).alias("time_key"),
        # full_time: HH:mm:ss string
        F.concat(
            F.lpad(hour_expr, 2, "0"),
            F.lit(":"),
            F.lpad(minute_expr, 2, "0"),
            F.lit(":"),
            F.lpad(second_expr, 2, "0"),
        ).alias("full_time"),
        # 12-hour format
        F.when(hour_expr == 0, 12)
        .when(hour_expr <= 12, hour_expr)
        .otherwise(hour_expr - 12)
        .alias("hour_12"),
        # AM/PM
        F.when(hour_expr < 12, "AM").otherwise("PM").alias("am_pm"),
        # Time of day period
        F.when(hour_expr < 6, "Night")
        .when(hour_expr < 9, "Early Morning")
        .when(hour_expr < 12, "Morning")
        .when(hour_expr < 17, "Afternoon")
        .when(hour_expr < 21, "Evening")
        .otherwise("Night")
        .alias("time_of_day"),
        # Business hours flag (9 AM - 5 PM)
        ((hour_expr >= 9) & (hour_expr < 17)).alias("is_business_hours"),
    )

    # Return columns in the order expected by the schema
    return time_df.select(
        "time_key",
        "full_time",
        "hour",
        "minute",
        "second",
        "hour_12",
        "am_pm",
        "time_of_day",
        "is_business_hours",
        "minute_of_day",
        "second_of_day",
    )
