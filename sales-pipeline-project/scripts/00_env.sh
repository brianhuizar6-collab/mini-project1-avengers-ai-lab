#!/usr/bin/env bash
# Source this first in every terminal: `source scripts/00_env.sh`
# Fill in the four values below once, everything else derives from them.

export AWS_REGION="us-east-1"
export BUCKET="sales-pipeline-REPLACE_WITH_UNIQUE_SUFFIX"     # bucket names are globally unique
export ACCOUNT_ID="$(aws sts get-caller-identity --query Account --output text)"
export ROLE_NAME="sales-pipeline-execution-role"

echo "AWS_REGION=$AWS_REGION"
echo "BUCKET=$BUCKET"
echo "ACCOUNT_ID=$ACCOUNT_ID"
echo "ROLE_NAME=$ROLE_NAME"
