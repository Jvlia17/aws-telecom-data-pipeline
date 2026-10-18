import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("TelecomPipelineTests")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    session.conf.set("spark.sql.session.timeZone", "UTC")

    yield session

    session.stop()
