#!/usr/bin/env bash
# Requirement #7 — orchestrate at least two pipeline stages.
set -euo pipefail
source "$(dirname "$0")/00_env.sh"

sed "s#REPLACE_BUCKET#$BUCKET#g" ../orchestration/state_machine.asl.json > /tmp/state_machine.asl.json 2>/dev/null \
  || sed "s#REPLACE_BUCKET#$BUCKET#g" "$(dirname "$0")/../orchestration/state_machine.asl.json" > /tmp/state_machine.asl.json

aws stepfunctions create-state-machine \
  --name sales-pipeline \
  --definition file:///tmp/state_machine.asl.json \
  --role-arn "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}" \
  --type STANDARD

echo "State machine created. Run it manually with:"
echo "  aws stepfunctions start-execution --state-machine-arn arn:aws:states:${AWS_REGION}:${ACCOUNT_ID}:stateMachine:sales-pipeline"
