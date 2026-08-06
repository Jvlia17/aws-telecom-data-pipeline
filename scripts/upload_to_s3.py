import boto3
import os


BUCKET_NAME = os.getenv(
    "BUCKET_NAME",
    "your-bucket-name"
)

FILE_PATH = "../data/raw/network_measurements.csv"

S3_KEY = "raw/network_measurements/network_measurements.csv"


def upload_file():

    s3_client = boto3.client("s3")

    s3_client.upload_file(
        FILE_PATH,
        BUCKET_NAME,
        S3_KEY
    )

    print("Upload completed successfully!")


if __name__ == "__main__":
    upload_file()