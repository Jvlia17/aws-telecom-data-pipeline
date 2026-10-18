
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import StorageLevel
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

# --------------------------------------------------
# Configuration
# --------------------------------------------------

BUCKET = "s3://telecom-data-pipeline-jvlia17"

BRONZE_PATH = f"{BUCKET}/bronze/telecom_measurements.csv"
SILVER_PATH = f"{BUCKET}/silver/telecom_measurements/"
QUARANTINE_PATH = f"{BUCKET}/quarantine/telecom_measurements/"

VALID_CITIES = [
    "VANCOUVER",
    "TORONTO",
    "MONTREAL",
    "OTTAWA",
    "CALGARY",
]

VALID_NETWORK_TYPES = ["4G", "5G"]

# --------------------------------------------------
# Initialize AWS Glue and Spark
# --------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glue_context = GlueContext(sc)
spark = glue_context.spark_session

job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------
# Read Bronze CSV
# Read source fields as strings to validate explicitly
# --------------------------------------------------

schema = StructType([
    StructField("id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("city", StringType(), True),
    StructField("network_type", StringType(), True),
    StructField("download_speed_mbps", StringType(), True),
    StructField("upload_speed_mbps", StringType(), True),
    StructField("latency_ms", StringType(), True),
])

raw_df = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(BRONZE_PATH)
)

# --------------------------------------------------
# Normalize and convert data types safely
# try_cast / try_to_timestamp return NULL for
# values that cannot be converted
# --------------------------------------------------

df = (
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

# --------------------------------------------------
# Data quality validation
# The first matching condition determines the reason
# --------------------------------------------------

df = df.withColumn(
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
        F.col("city").isNull() | ~F.col("city").isin(VALID_CITIES),
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

# Cache validated rows because they are used more than once.
df = df.persist(StorageLevel.MEMORY_AND_DISK)

input_count = df.count()

invalid = df.filter(F.col("quality_issue").isNotNull())
valid_candidates = df.filter(F.col("quality_issue").isNull())

# --------------------------------------------------
# Duplicate handling
# Keep the earliest timestamp for each valid ID.
# Route additional records to quarantine.
# --------------------------------------------------

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

# --------------------------------------------------
# Silver output
# Store clean, typed data as Parquet
# --------------------------------------------------

silver = valid.select(
    "id",
    "event_timestamp",
    "city",
    "network_type",
    "download_speed_mbps",
    "upload_speed_mbps",
    "latency_ms",
)

# --------------------------------------------------
# Report counts
# --------------------------------------------------

silver_count = silver.count()
quarantine_count = quarantine.count()

print(f"Input rows: {input_count}")
print(f"Silver rows: {silver_count}")
print(f"Quarantine rows: {quarantine_count}")

# --------------------------------------------------
# Write results
# Default error-if-exists behavior is intentional:
# do not overwrite previous output accidentally.
# --------------------------------------------------

silver.write \
    .mode("errorifexists") \
    .parquet(SILVER_PATH)

quarantine.write \
    .mode("errorifexists") \
    .parquet(QUARANTINE_PATH)

print("Bronze-to-Silver processing completed successfully.")

df.unpersist()
job.commit()
