import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import StorageLevel
from pyspark.context import SparkContext
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)

from telecom_pipeline.transformations import (
    normalize_measurements,
    split_measurements,
    validate_measurements,
)

# --------------------------------------------------
# Configuration
# --------------------------------------------------

BUCKET = "s3://telecom-data-pipeline-jvlia17"

BRONZE_PATH = f"{BUCKET}/bronze/telecom_measurements.csv"
SILVER_PATH = f"{BUCKET}/silver/telecom_measurements/"
QUARANTINE_PATH = f"{BUCKET}/quarantine/telecom_measurements/"

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
# Shared transformation logic
# These functions are also covered by local unit tests.
# --------------------------------------------------

df = normalize_measurements(raw_df)
df = validate_measurements(df)

# Cache rows because they are used for counts, splitting
# and output writes.
df = df.persist(StorageLevel.MEMORY_AND_DISK)

input_count = df.count()

silver, quarantine = split_measurements(df)

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
# Do not overwrite previous output accidentally.
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
