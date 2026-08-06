from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    hour,
    when
)


def add_features(df: DataFrame) -> DataFrame:
    """
    Add derived columns used for analytics.
    """

    df = df.withColumn(
        "timestamp",
        to_timestamp("timestamp")
    )

    df = df.withColumn(
        "date",
        to_date(col("timestamp"))
    )

    df = df.withColumn(
        "hour",
        hour(col("timestamp"))
    )

    df = df.withColumn(
        "connection_quality",
        when(
            (col("download_speed_mbps") >= 100)
            & (col("latency_ms") <= 20),
            "Excellent"
        ).when(
            (col("download_speed_mbps") >= 50)
            & (col("latency_ms") <= 40),
            "Good"
        ).otherwise("Poor")
    )

    return df