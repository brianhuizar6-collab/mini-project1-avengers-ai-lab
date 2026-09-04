#!/usr/bin/env bash
# Requirement #2 — catalog source data (inferred schema).
set -euo pipefail
source "$(dirname "$0")/00_env.sh"

aws glue create-database --database-input Name=raw_zone 2>/dev/null || true

aws glue create-crawler \
  --name sales-pipeline-raw-crawler \
  --role "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
  --database-name raw_zone \
  --targets "{\"S3Targets\": [{\"Path\": \"s3://$BUCKET/raw/customers/\"}, {\"Path\": \"s3://$BUCKET/raw/products/\"}, {\"Path\": \"s3://$BUCKET/raw/transactions/\"}]}"

aws glue start-crawler --name sales-pipeline-raw-crawler

echo "Crawler running — check status with:"
echo "  aws glue get-crawler --name sales-pipeline-raw-crawler --query 'Crawler.State'"
echo "Once READY, inspect the inferred schema with:"
echo "  aws glue get-tables --database-name raw_zone"
echo "Compare that inferred schema against docs/schema-*.md and correct any mismatches there."
