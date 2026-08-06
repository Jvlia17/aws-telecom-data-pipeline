from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def clean_measurements(df: DataFrame) -> DataFrame:
    """
    Clean raw telecom measurements.
    """

    return (
        df
        .dropDuplicates()
        .filter(col("download_speed_mbps") > 0)
        .filter(col("upload_speed_mbps") > 0)
        .filter(col("latency_ms") > 0)
        .filter(col("signal_strength_dbm").between(-130, -40))
    )