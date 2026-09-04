#!/usr/bin/env bash
# Requirement #1 — controlled raw, rejected, curated zones (as prefixes in one bucket).
set -euo pipefail
source "$(dirname "$0")/00_env.sh"

aws s3api create-bucket --bucket "$BUCKET" --region "$AWS_REGION" \
  $( [ "$AWS_REGION" != "us-east-1" ] && echo "--create-bucket-configuration LocationConstraint=$AWS_REGION" )

aws s3api put-bucket-versioning --bucket "$BUCKET" \
  --versioning-configuration Status=Enabled

aws s3api put-public-access-block --bucket "$BUCKET" \
  --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true

# Seed the zone prefixes so they're visible in the console immediately.
for prefix in raw/customers raw/products raw/transactions \
              rejected/transactions \
              curated/transactions curated/customers curated/products \
              control/run_metrics athena-results; do
  aws s3api put-object --bucket "$BUCKET" --key "${prefix}/.keep" >/dev/null
done

echo "Created s3://$BUCKET with raw/, rejected/, curated/, control/ and athena-results/ zones."
