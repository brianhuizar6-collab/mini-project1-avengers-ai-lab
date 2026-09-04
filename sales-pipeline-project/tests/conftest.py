import pytest
from pyspark.sql import SparkSession
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder
        .appName("pytest-sales-pipeline")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield spark
    spark.stop()
