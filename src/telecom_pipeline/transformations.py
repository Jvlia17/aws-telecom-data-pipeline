from pyspark.sql import functions as F
from pyspark.sql import Window

VALID_CITIES = [
    "VANCOUVER",
    "TORONTO",
    "MONTREAL",
    "OTTAWA",
    "CALGARY",
]

VALID_NETWORK_TYPES = ["4G", "5G"]


def normalize_measurements(raw_df):
    """
    Normalize source values and convert them to appropriate types.
    Source fields are initially read as strings.
    """

    return (
        raw_df
        .withColumn("id", F.trim(F.col("id")))
        .withColumn("city", F.upper(F.trim(F.col("city"))))
        .withColumn(
            "network_type",
            F.upper(F.trim(F.col("network_type"))),
        )
        .withColumn(
            "event_timestamp",
            F.expr("try_to_timestamp(`timestamp`)"),
        )
        .withColumn(
            "download_speed_mbps",
            F.expr("try_cast(download_speed_mbps AS DOUBLE)"),
        )
        .withColumn(
            "upload_speed_mbps",
            F.expr("try_cast(upload_speed_mbps AS DOUBLE)"),
        )
        .withColumn(
            "latency_ms",
            F.expr("try_cast(latency_ms AS DOUBLE)"),
        )
    )

def validate_measurements(df):
    """
    Assign a quality issue to each invalid record.
    The first matching condition determines the issue.
    """

    return df.withColumn(
        "quality_issue",
        F.when(
            F.col("id").isNull() | (F.col("id") == ""),
            "missing_id",
        )
        .when(
            F.col("event_timestamp").isNull(),
            "invalid_timestamp",
        )
        .when(
            F.col("event_timestamp") > F.current_timestamp(),
            "future_timestamp",
        )
        .when(
            F.col("city").isNull()
            | ~F.col("city").isin(VALID_CITIES),
            "invalid_city",
        )
        .when(
            F.col("network_type").isNull()
            | ~F.col("network_type").isin(VALID_NETWORK_TYPES),
            "invalid_network_type",
        )
        .when(
            F.col("download_speed_mbps").isNull(),
            "invalid_download_speed",
        )
        .when(
            F.col("upload_speed_mbps").isNull(),
            "invalid_upload_speed",
        )
        .when(
            F.col("latency_ms").isNull(),
            "invalid_latency",
        )
        .when(
            (F.col("download_speed_mbps") <= 0)
            | (F.col("upload_speed_mbps") <= 0)
            | (F.col("latency_ms") <= 0),
            "non_positive_measurement",
        )
        .when(
            (F.col("download_speed_mbps") > 1000)
            | (F.col("upload_speed_mbps") > 500)
            | (F.col("latency_ms") > 1000),
            "measurement_outlier",
        )
    )


def split_measurements(df):
    """
    Split validated records into Silver and Quarantine.
    For each valid ID, keep the earliest timestamp.
    """

    invalid = df.filter(F.col("quality_issue").isNotNull())
    valid_candidates = df.filter(F.col("quality_issue").isNull())

    duplicate_window = (
        Window
        .partitionBy("id")
        .orderBy(
            F.col("event_timestamp").asc(),
            F.col("timestamp").asc(),
        )
    )

    ranked = valid_candidates.withColumn(
        "duplicate_rank",
        F.row_number().over(duplicate_window),
    )

    duplicates = (
        ranked
        .filter(F.col("duplicate_rank") > 1)
        .withColumn("quality_issue", F.lit("duplicate_id"))
        .drop("duplicate_rank")
    )

    valid = (
        ranked
        .filter(F.col("duplicate_rank") == 1)
        .drop("duplicate_rank", "quality_issue")
    )

    quarantine = invalid.unionByName(duplicates)

    silver = valid.select(
        "id",
        "event_timestamp",
        "city",
        "network_type",
        "download_speed_mbps",
        "upload_speed_mbps",
        "latency_ms",
    )

    return silver, quarantine


from pyspark.sql import functions as F

def aggregate_city_metrics(df):
    return (
        df.groupBy("city", "network_type")
        .agg(
            F.count("*").alias("measurement_count"),
            F.round(F.avg("download_speed_mbps"), 2).alias("avg_download_speed_mbps"),
            F.round(F.avg("upload_speed_mbps"), 2).alias("avg_upload_speed_mbps"),
            F.round(F.avg("latency_ms"), 2).alias("avg_latency_ms"),
        )
    )

