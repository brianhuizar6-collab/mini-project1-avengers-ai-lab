#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/00_env.sh"

python data/generate_synthetic_data.py --out data/raw --n-transactions 5000

aws s3 cp data/raw/customers/customers.csv "s3://$BUCKET/raw/customers/customers.csv"
aws s3 cp data/raw/products/products.csv   "s3://$BUCKET/raw/products/products.csv"
aws s3 cp data/raw/transactions/transactions.csv "s3://$BUCKET/raw/transactions/transactions.csv"

echo "Uploaded synthetic datasets to s3://$BUCKET/raw/"
