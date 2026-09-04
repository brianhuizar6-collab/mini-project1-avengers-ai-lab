#!/usr/bin/env bash
# OPTIONAL — only run this if the assignment requires a schedule.
set -euo pipefail
source "$(dirname "$0")/00_env.sh"

aws events put-rule \
  --name sales-pipeline-nightly \
  --schedule-expression "cron(0 3 * * ? *)" \
  --state ENABLED

aws events put-targets \
  --rule sales-pipeline-nightly \
  --targets "Id"="sales-pipeline-state-machine","Arn"="arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:sales-pipeline","RoleArn"="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

echo "EventBridge rule 'sales-pipeline-nightly' created (03:00 UTC daily)."
