from datetime import datetime

from pyspark.sql.types import StructField, StructType, StringType

from telecom_pipeline.transformations import normalize_measurements


def test_normalize_measurements(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("city", StringType(), True),
        StructField("network_type", StringType(), True),
        StructField("download_speed_mbps", StringType(), True),
        StructField("upload_speed_mbps", StringType(), True),
        StructField("latency_ms", StringType(), True),
    ])

    raw_df = spark.createDataFrame(
        [
            (
                " 1001 ",
                "2025-01-15 10:30:00",
                " calgary ",
                " 5g ",
                "120.5",
                "25.2",
                "18.0",
            )
        ],
        schema=schema,
    )

    result = normalize_measurements(raw_df).first()

    assert result["id"] == "1001"
    assert result["city"] == "CALGARY"
    assert result["network_type"] == "5G"
    assert result["download_speed_mbps"] == 120.5
    assert result["upload_speed_mbps"] == 25.2
    assert result["latency_ms"] == 18.0
    assert result["event_timestamp"] is not None

def test_validate_invalid_city(spark):
    from telecom_pipeline.transformations import (
        normalize_measurements,
        validate_measurements,
    )

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("city", StringType(), True),
        StructField("network_type", StringType(), True),
        StructField("download_speed_mbps", StringType(), True),
        StructField("upload_speed_mbps", StringType(), True),
        StructField("latency_ms", StringType(), True),
    ])

    raw_df = spark.createDataFrame(
        [
            (
                "2001",
                "2025-01-15 10:30:00",
                "LONDON",
                "5G",
                "120.5",
                "25.2",
                "18.0",
            )
        ],
        schema=schema,
    )

    normalized = normalize_measurements(raw_df)
    validated = validate_measurements(normalized)

    result = validated.first()

    assert result["quality_issue"] == "invalid_city"

def test_validate_invalid_download_speed(spark):
    from telecom_pipeline.transformations import (
        normalize_measurements,
        validate_measurements,
    )

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("city", StringType(), True),
        StructField("network_type", StringType(), True),
        StructField("download_speed_mbps", StringType(), True),
        StructField("upload_speed_mbps", StringType(), True),
        StructField("latency_ms", StringType(), True),
    ])

    raw_df = spark.createDataFrame(
        [
            (
                "2002",
                "2025-01-15 10:30:00",
                "CALGARY",
                "5G",
                "not-a-number",
                "25.2",
                "18.0",
            )
        ],
        schema=schema,
    )

    normalized = normalize_measurements(raw_df)
    validated = validate_measurements(normalized)

    result = validated.first()

    assert result["download_speed_mbps"] is None
    assert result["quality_issue"] == "invalid_download_speed"


def test_split_measurements_keeps_earliest_duplicate(spark):
    from telecom_pipeline.transformations import (
        normalize_measurements,
        validate_measurements,
        split_measurements,
    )

    schema = StructType([
        StructField("id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("city", StringType(), True),
        StructField("network_type", StringType(), True),
        StructField("download_speed_mbps", StringType(), True),
        StructField("upload_speed_mbps", StringType(), True),
        StructField("latency_ms", StringType(), True),
    ])

    raw_df = spark.createDataFrame(
        [
            (
                "3001",
                "2025-01-15 12:00:00",
                "CALGARY",
                "5G",
                "120.0",
                "25.0",
                "18.0",
            ),
            (
                "3001",
                "2025-01-15 10:00:00",
                "CALGARY",
                "5G",
                "100.0",
                "20.0",
                "15.0",
            ),
        ],
        schema=schema,
    )

    normalized = normalize_measurements(raw_df)
    validated = validate_measurements(normalized)
    silver, quarantine = split_measurements(validated)

    silver_rows = silver.collect()
    quarantine_rows = quarantine.collect()

    assert len(silver_rows) == 1
    assert silver_rows[0]["download_speed_mbps"] == 100.0
    assert len(quarantine_rows) == 1
    assert quarantine_rows[0]["quality_issue"] == "duplicate_id"
