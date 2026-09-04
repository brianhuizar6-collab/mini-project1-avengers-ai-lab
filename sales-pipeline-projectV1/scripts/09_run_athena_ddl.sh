#!/usr/bin/env bash
# Requirement #6 — make curated data queryable through Athena.
set -euo pipefail
source "$(dirname "$0")/00_env.sh"

sed "s#REPLACE_BUCKET#$BUCKET#g" "$(dirname "$0")/../athena/ddl_curated.sql" > /tmp/ddl_curated.sql

while IFS= read -r -d ';' stmt; do
  stmt="$(echo "$stmt" | sed '/^--/d')"
  [ -z "$(echo "$stmt" | tr -d '[:space:]')" ] && continue
  echo "Running: $(echo "$stmt" | head -c 80)..."
  qid=$(aws athena start-query-execution \
    --query-string "$stmt" \
    --result-configuration "OutputLocation=s3://$BUCKET/athena-results/" \
    --query 'QueryExecutionId' --output text)
  aws athena get-query-execution --query-execution-id "$qid" --query 'QueryExecution.Status.State'
done < /tmp/ddl_curated.sql

echo "Athena databases/tables created. Run athena/queries.sql next."
