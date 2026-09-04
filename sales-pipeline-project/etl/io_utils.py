"""
Read/write helpers shared by the Glue job and local runs.
Isolates the one setting that makes reruns safe: dynamic partition overwrite.
"""
from pyspark.sql import DataFrame, SparkSession


def configure_dynamic_overwrite(spark: SparkSession) -> None:
    """
    Without this, mode('overwrite') replaces the WHOLE table on every run.
    With it, a write only replaces the specific partitions present in the
    DataFrame being written — the mechanism behind requirement #8
    (rerunning the same input does not duplicate output).
    """
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def write_curated(df: DataFrame, path: str) -> None:
    (
        df.write
          .mode("overwrite")
          .partitionBy("year", "month", "day")
          .parquet(path)
    )


def write_rejected(df: DataFrame, path: str, run_id: str) -> None:
    (
        df.write
          .mode("overwrite")
          .parquet(f"{path.rstrip('/')}/run_id={run_id}")
    )


def write_run_metrics(spark: SparkSession, counts: dict, stage: str, path: str) -> None:
    import datetime
    row = {**counts, "stage": stage, "ts": datetime.datetime.utcnow().isoformat()}
    df = spark.createDataFrame([row])
    df.write.mode("append").parquet(path)
