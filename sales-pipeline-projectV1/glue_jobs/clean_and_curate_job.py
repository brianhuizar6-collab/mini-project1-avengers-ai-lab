"""
Stage 1 Glue job — "CleanAndCurate".

Deploy this as a Glue 4.0 (Spark) job. Job parameters (set as Glue job
arguments, either in the console or via the deploy script):

    --RAW_TRANSACTIONS_PATH   s3://<bucket>/raw/transactions/
    --RAW_CUSTOMERS_PATH      s3://<bucket>/raw/customers/
    --RAW_PRODUCTS_PATH       s3://<bucket>/raw/products/
    --CURATED_PATH            s3://<bucket>/curated/transactions/
    --REJECTED_PATH           s3://<bucket>/rejected/transactions/
    --METRICS_PATH            s3://<bucket>/control/run_metrics/
    --RUN_ID                  passed in by Step Functions at execution time

Local smoke test (no AWS needed — everything below the getResolvedOptions
line is plain PySpark):

    spark-submit glue_jobs/clean_and_curate_job.py \
        --RAW_TRANSACTIONS_PATH data/raw/transactions \
        --RAW_CUSTOMERS_PATH data/raw/customers \
        --RAW_PRODUCTS_PATH data/raw/products \
        --CURATED_PATH data/curated/transactions \
        --REJECTED_PATH data/rejected/transactions \
        --METRICS_PATH data/control/run_metrics \
        --RUN_ID local-test-1 --JOB_NAME local-test
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

sys.path.append(".")  # lets this file import the etl/ package when zipped alongside it as --extra-py-files
from etl.io_utils import configure_dynamic_overwrite, read_csv, write_curated, write_rejected, write_run_metrics
from etl.transform import clean_and_curate

REQUIRED_ARGS = [
    "JOB_NAME", "RAW_TRANSACTIONS_PATH", "RAW_CUSTOMERS_PATH", "RAW_PRODUCTS_PATH",
    "CURATED_PATH", "REJECTED_PATH", "METRICS_PATH", "RUN_ID",
]

args = getResolvedOptions(sys.argv, REQUIRED_ARGS)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

configure_dynamic_overwrite(spark)

transactions_df = read_csv(spark, args["RAW_TRANSACTIONS_PATH"])
customers_df = read_csv(spark, args["RAW_CUSTOMERS_PATH"])
products_df = read_csv(spark, args["RAW_PRODUCTS_PATH"])

curated_df, rejected_df, counts = clean_and_curate(
    transactions_df, customers_df, products_df, run_id=args["RUN_ID"]
)

# CloudWatch picks up stdout/stderr from Glue jobs automatically — this is
# requirement #10 (record counts at raw/rejected/curated) satisfied as a log line.
print(f"[run={args['RUN_ID']}] raw_count={counts['raw_count']} "
      f"rejected_count={counts['rejected_count']} curated_count={counts['curated_count']}")

write_curated(curated_df, args["CURATED_PATH"])
write_rejected(rejected_df, args["REJECTED_PATH"], args["RUN_ID"])
write_run_metrics(spark, counts, stage="clean_and_curate", path=args["METRICS_PATH"])

job.commit()
