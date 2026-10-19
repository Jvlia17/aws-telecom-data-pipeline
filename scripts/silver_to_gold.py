
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from telecom_pipeline.transformations import aggregate_city_metrics


SILVER_PATH = (
    "s3://telecom-data-pipeline-jvlia17/"
    "silver/telecom_measurements/"
)

GOLD_PATH = (
    "s3://telecom-data-pipeline-jvlia17/"
    "gold/telecom_city_metrics/"
)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

silver_df = spark.read.parquet(SILVER_PATH)

gold_df = aggregate_city_metrics(silver_df)

gold_df.write \
    .mode("overwrite") \
    .parquet(GOLD_PATH)

print(f"Silver rows: {silver_df.count()}")
print(f"Gold rows: {gold_df.count()}")

job.commit()
