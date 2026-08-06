from pyspark.sql import SparkSession

from transformations.cleaning import clean_measurements
from transformations.features import add_features

from config import (
    BRONZE_PATH,
    SILVER_PATH
)


def create_spark_session():
    return (
        SparkSession
        .builder
        .appName("TelecomDataPipeline")
        .getOrCreate()
    )


def main():

    spark = create_spark_session()

    print("Reading bronze data...")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(BRONZE_PATH)
    )


    print("Raw data:")
    df.show(5)


    print("Cleaning data...")

    df_clean = clean_measurements(df)


    print("Adding features...")

    df_silver = add_features(df_clean)


    print("Saving silver layer...")


    (
        df_silver
        .write
        .mode("overwrite")
        .parquet(SILVER_PATH)
    )


    print("ETL job completed!")


    spark.stop()


if __name__ == "__main__":
    main()