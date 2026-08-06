import boto3


BUCKET_NAME = "telecom-data-pipeline-152174417047-ca-central-1-an"
SILVER_PREFIX = "silver/"


def check_silver_data():

    s3 = boto3.client("s3")

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=SILVER_PREFIX
    )

    parquet_files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not parquet_files:
        raise Exception("Silver layer is empty!")

    print(
        f"Found {len(parquet_files)} parquet files in Silver"
    )

    print("Silver quality check passed!")