from pyspark.sql.types import StructField, StructType, StringType

from telecom_pipeline.transformations import (
    aggregate_city_metrics,
    normalize_measurements,
    split_measurements,
    validate_measurements,
)


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



def test_aggregate_city_metrics(spark):
    data = [
        ("Calgary", "4G", 100.0, 20.0, 10.0),
        ("Calgary", "4G", 200.0, 40.0, 30.0),
        ("Calgary", "5G", 300.0, 60.0, 15.0),
        ("Toronto", "4G", 150.0, 30.0, 20.0),
    ]

    df = spark.createDataFrame(
        data,
        [
            "city",
            "network_type",
            "download_speed_mbps",
            "upload_speed_mbps",
            "latency_ms",
        ],
    )

    result = aggregate_city_metrics(df)
    rows = {
        (row["city"], row["network_type"]): row.asDict()
        for row in result.collect()
    }

    calgary_4g = rows[("Calgary", "4G")]
    assert calgary_4g["measurement_count"] == 2
    assert calgary_4g["avg_download_speed_mbps"] == 150.0
    assert calgary_4g["avg_upload_speed_mbps"] == 30.0
    assert calgary_4g["avg_latency_ms"] == 20.0

    calgary_5g = rows[("Calgary", "5G")]
    assert calgary_5g["measurement_count"] == 1
    assert calgary_5g["avg_download_speed_mbps"] == 300.0

    toronto_4g = rows[("Toronto", "4G")]
    assert toronto_4g["measurement_count"] == 1
    assert toronto_4g["avg_download_speed_mbps"] == 150.0

    assert len(rows) == 3
