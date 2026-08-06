from pyspark.sql.functions import (
    avg,
    count,
    round,
    to_date
)


def create_city_summary(df):

    return (
        df
        .groupBy("city")
        .agg(
            round(avg("download_speed_mbps"), 2).alias("avg_download_speed_mbps"),
            round(avg("upload_speed_mbps"), 2).alias("avg_upload_speed_mbps"),
            round(avg("latency_ms"), 2).alias("avg_latency_ms"),
            round(avg("signal_strength_dbm"), 2).alias("avg_signal_strength_dbm"),
            count("*").alias("measurements")
        )
    )


def create_network_summary(df):

    return (
        df
        .groupBy("network_type")
        .agg(
            round(avg("download_speed_mbps"), 2).alias("avg_download_speed_mbps"),
            round(avg("upload_speed_mbps"), 2).alias("avg_upload_speed_mbps"),
            round(avg("latency_ms"), 2).alias("avg_latency_ms"),
            count("*").alias("measurements")
        )
    )


def create_device_summary(df):

    return (
        df
        .groupBy("device_model")
        .agg(
            round(avg("download_speed_mbps"), 2).alias("avg_download_speed_mbps"),
            round(avg("latency_ms"), 2).alias("avg_latency_ms"),
            count("*").alias("measurements")
        )
    )


def create_daily_summary(df):

    df = df.withColumn(
        "date",
        to_date("timestamp")
    )

    return (
        df
        .groupBy("date")
        .agg(
            round(avg("download_speed_mbps"), 2).alias("avg_download_speed_mbps"),
            round(avg("latency_ms"), 2).alias("avg_latency_ms"),
            count("*").alias("measurements")
        )
    )