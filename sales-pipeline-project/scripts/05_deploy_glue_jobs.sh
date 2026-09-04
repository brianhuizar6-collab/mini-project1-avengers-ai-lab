#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_env.sh"

# Package the etl/ module alongside the job so Glue can import it via --extra-py-files
cd "$(dirname "$0")/.."
zip -r /tmp/etl_pkg.zip etl -x "*.pyc" "__pycache__/*"
aws s3 cp /tmp/etl_pkg.zip "s3://$BUCKET/artifacts/etl_pkg.zip"
aws s3 cp glue_jobs/clean_and_curate_job.py "s3://$BUCKET/artifacts/clean_and_curate_job.py"
aws s3 cp glue_jobs/catalog_and_publish_job.py "s3://$BUCKET/artifacts/catalog_and_publish_job.py"

aws glue create-job \
  --name clean-and-curate \
  --role "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
  --glue-version "4.0" \
  --number-of-workers 2 \
  --worker-type G.1X \
  --command "Name=glueetl,ScriptLocation=s3://$BUCKET/artifacts/clean_and_curate_job.py,PythonVersion=3" \
  --default-arguments "{\"--extra-py-files\": \"s3://$BUCKET/artifacts/etl_pkg.zip\", \"--RAW_TRANSACTIONS_PATH\": \"s3://$BUCKET/raw/transactions/\", \"--RAW_CUSTOMERS_PATH\": \"s3://$BUCKET/raw/customers/\", \"--RAW_PRODUCTS_PATH\": \"s3://$BUCKET/raw/products/\", \"--CURATED_PATH\": \"s3://$BUCKET/curated/transactions/\", \"--REJECTED_PATH\": \"s3://$BUCKET/rejected/transactions/\", \"--METRICS_PATH\": \"s3://$BUCKET/control/run_metrics/\"}"

aws glue create-job \
  --name catalog-and-publish \
  --role "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
  --command "Name=pythonshell,ScriptLocation=s3://$BUCKET/artifacts/catalog_and_publish_job.py,PythonVersion=3.9" \
  --default-arguments "{\"--CURATED_DATABASE\": \"curated\", \"--CURATED_TABLE\": \"transactions\", \"--ATHENA_OUTPUT\": \"s3://$BUCKET/athena-results/\", \"--ATHENA_WORKGROUP\": \"primary\"}"

echo "Created Glue jobs: clean-and-curate, catalog-and-publish"
