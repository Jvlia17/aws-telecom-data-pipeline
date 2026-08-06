AWS_REGION = "your-region"

BUCKET_NAME = "your-bucket-name"

BRONZE_PATH = (
    f"s3://{BUCKET_NAME}/bronze/network_measurements.csv"
)

SILVER_PATH = (
    f"s3://{BUCKET_NAME}/silver/"
)

GOLD_CITY_SUMMARY_PATH = (
    f"s3://{BUCKET_NAME}/gold/city_summary/"
)

GOLD_NETWORK_SUMMARY_PATH = (
    f"s3://{BUCKET_NAME}/gold/network_summary/"
)

GOLD_DEVICE_SUMMARY_PATH = (
    f"s3://{BUCKET_NAME}/gold/device_summary/"
)

GOLD_DAILY_SUMMARY_PATH = (
    f"s3://{BUCKET_NAME}/gold/daily_summary/"
)