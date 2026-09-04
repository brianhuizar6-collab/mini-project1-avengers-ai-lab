# Rerun-safety proof (requirement #8)

Fill this in after running `scripts/local_rerun_proof.py` locally, and again
after running the deployed Step Functions state machine twice from the
console against the same S3 input. Full steps: `docs/console-walkthrough.md` §11.

## Local proof

```
$ python scripts/local_rerun_proof.py
=== Run 1 ===
{'run_id': 'proof-run-1', 'raw_count': ..., 'rejected_count': ..., 'curated_count': ...} checksum: ...
=== Run 2 (same input) ===
{'run_id': 'proof-run-2', 'raw_count': ..., 'rejected_count': ..., 'curated_count': ...} checksum: ...

PASS — identical input produced identical curated output on both runs.
```

Paste the real output above once you've run it.

## AWS proof (Console)

1. Step Functions console → `sales-pipeline` state machine → **Start execution**, name it `proof-run-1`, input `{}`. Wait for **Succeeded**.
2. Athena query editor → `SELECT COUNT(*) FROM curated.transactions;` — record the count. Also open `curated/transactions/` in the S3 console and note the object count.
3. Step Functions → **Start execution** again, name it `proof-run-2`, same input, raw files unchanged.
4. Repeat step 2 — record the count again.
5. Paste both counts below. They should match, confirming the dynamic-partition-overwrite write replaced the same partitions rather than appending duplicates.

| Run | curated row count | curated object count (S3) |
|---|---|---|
| proof-run-1 | | |
| proof-run-2 | | |
