#!/usr/bin/env bash
# One shared execution role for the Glue jobs and the Step Functions state machine
# (see the "do I need IAM roles" note in README.md — this is the minimum AWS requires).
# SKIP this script entirely if your account already has a usable pre-provisioned
# role (e.g. a training-account "LabRole") — just point the job/deploy scripts at
# that role's ARN instead.
set -euo pipefail
source "$(dirname "$0")/00_env.sh"

TRUST_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"},
    {"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"}, "Action": "sts:AssumeRole"}
  ]
}
EOF
)

aws iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document "$TRUST_POLICY" \
  --description "Shared execution role for the sales analytics pipeline (Glue + Step Functions)"

aws iam attach-role-policy --role-name "$ROLE_NAME" \
  --policy-arn arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole

# Scoped S3 access instead of full-account S3 access
S3_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"],
      "Resource": ["arn:aws:s3:::$BUCKET", "arn:aws:s3:::$BUCKET/*"]
    },
    {
      "Effect": "Allow",
      "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:StartCrawler", "glue:GetCrawler",
                 "athena:StartQueryExecution", "athena:GetQueryExecution"],
      "Resource": "*"
    }
  ]
}
EOF
)

aws iam put-role-policy --role-name "$ROLE_NAME" \
  --policy-name sales-pipeline-scoped-access \
  --policy-document "$S3_POLICY"

echo "Created IAM role: arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
