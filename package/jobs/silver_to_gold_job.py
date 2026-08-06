from pyspark.sql import SparkSession

from transformations.aggregations import (
    create_city_summary,
    create_network_summary,
    create_device_summary,
    create_daily_summary
)

from config import (
    SILVER_PATH,
    GOLD_CITY_SUMMARY_PATH,
    GOLD_NETWORK_SUMMARY_PATH,
    GOLD_DEVICE_SUMMARY_PATH,
    GOLD_DAILY_SUMMARY_PATH
)


def create_spark_session():
    return (
        SparkSession
        .builder
        .appName("TelecomGoldPipeline")
        .getOrCreate()
    )


def save_parquet(df, path):

    (
        df.write
        .mode("overwrite")
        .parquet(path)
    )


def main():

    spark = create_spark_session()

    print("Reading Silver layer...")

    df = spark.read.parquet(SILVER_PATH)

    print(f"Loaded {df.count()} rows.")

    print("Creating City Summary...")
    city_summary = create_city_summary(df)

    print("Creating Network Summary...")
    network_summary = create_network_summary(df)

    print("Creating Device Summary...")
    device_summary = create_device_summary(df)

    print("Creating Daily Summary...")
    daily_summary = create_daily_summary(df)

    print("Saving Gold datasets...")

    save_parquet(
        city_summary,
        GOLD_CITY_SUMMARY_PATH
    )

    save_parquet(
        network_summary,
        GOLD_NETWORK_SUMMARY_PATH
    )

    save_parquet(
        device_summary,
        GOLD_DEVICE_SUMMARY_PATH
    )

    save_parquet(
        daily_summary,
        GOLD_DAILY_SUMMARY_PATH
    )

    print("Gold pipeline completed successfully!")

    spark.stop()


if __name__ == "__main__":
    main()