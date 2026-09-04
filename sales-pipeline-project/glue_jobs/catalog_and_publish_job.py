"""
Stage 2 Glue job — "CatalogAndPublish".

Deploy as a Glue Python Shell job (cheaper/faster than Spark for this —
it's just metadata work, no data processing). Job parameters:

    --CURATED_DATABASE   curated
    --CURATED_TABLE      transactions
    --ATHENA_OUTPUT       s3://<bucket>/athena-results/
    --ATHENA_WORKGROUP    primary   (or your team's workgroup)

What it does: runs MSCK REPAIR TABLE against the curated Athena table so
newly written year/month/day partitions are visible immediately, instead
of waiting on a scheduled crawler. This is the "Catalog & Publish" half
of the two-stage pipeline referenced in the architecture diagram.
"""
import sys
import time

import boto3
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, [
    "CURATED_DATABASE", "CURATED_TABLE", "ATHENA_OUTPUT", "ATHENA_WORKGROUP",
])

athena = boto3.client("athena")

query = f"MSCK REPAIR TABLE {args['CURATED_DATABASE']}.{args['CURATED_TABLE']}"

resp = athena.start_query_execution(
    QueryString=query,
    QueryExecutionContext={"Database": args["CURATED_DATABASE"]},
    ResultConfiguration={"OutputLocation": args["ATHENA_OUTPUT"]},
    WorkGroup=args["ATHENA_WORKGROUP"],
)
qid = resp["QueryExecutionId"]

for _ in range(60):
    state = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]["State"]
    if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
        break
    time.sleep(2)

if state != "SUCCEEDED":
    raise RuntimeError(f"MSCK REPAIR TABLE did not succeed: {state} (query_id={qid})")

print(f"Partitions refreshed for {args['CURATED_DATABASE']}.{args['CURATED_TABLE']} (query_id={qid})")
