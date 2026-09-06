# Troubleshooting Log — Sales Pipeline Project

A record of the issues hit while building and deploying the Customer Sales
Analytics Pipeline, how each was diagnosed, and the fix applied. Kept here
so the same problems don't cost anyone else on the team the same debugging
time.

---

## 1. Local environment — Python version conflict with PySpark

**Symptom:** Couldn't reliably `pip install pyspark==3.5.4` / run the local
test suite on a machine that had Python 3.12 installed.

**Root cause:** PySpark 3.5.4 ships prebuilt wheels tested against
Python 3.11; compatibility with 3.12 was unreliable at the time.

**Fix:** Installed Python 3.11 alongside the existing 3.12 (via the
official installer, which registers with the Windows `py` launcher), then
created the project's virtual environment explicitly with that version:

```powershell
py -0                     # confirm 3.11 is now listed
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
```

If `Activate.ps1` is blocked by PowerShell's execution policy, call the
venv's Python directly instead of activating:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

**Note:** this only affects the local dev/test environment. AWS Glue
provides its own managed Spark/Python runtime per Glue version — the local
Python version has no bearing on what Glue runs in the console.

---

## 2. `pip install` failing with SSL certificate errors

**Symptom:**
```
WARNING: Retrying ... SSLError(SSLCertVerificationError(1, '[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: unable to get local issuer certificate ...
ERROR: Could not find a version that satisfies the requirement pyspark==3.5.4
```

**Root cause:** Corporate/managed-network TLS interception (common on
TCS-managed machines) breaks pip's certificate validation against
pypi.org.

**Fix:**
```powershell
pip install --use-feature=truststore pyspark==3.5.4 pytest pytest-asyncio faker
```
`--use-feature=truststore` tells pip to use the OS's own trusted
certificate store instead of its bundled CA list.

---

## 3. `pytest tests/ -v` — all 9 tests failing with PySpark/Windows errors

**Symptom:**
```
WARN Shell: Did not find winutils.exe: java.io.FileNotFoundException: ...
HADOOP_HOME and hadoop.home.dir are unset.
...
py4j.protocol.Py4JJavaError: An error occurred while calling o331.approxQuantile.
java.lang.UnsatisfiedLinkError: 'boolean org.apache.hadoop.io.nativeio.NativeIO$Windows.access0(...)'
```
All 9 unit tests in `tests/test_transform.py` failed — not on assertion
logic, but on this same underlying error every time.

**Root cause:** PySpark bundles Hadoop libraries, and Hadoop's local
filesystem access on Windows requires a native helper binary
(`winutils.exe` + `hadoop.dll`) that isn't included by default. Without
it, Spark can't even list files in a local directory.

**Fix:**
1. Downloaded `winutils.exe` and `hadoop.dll` for Hadoop 3.3.x from
   https://github.com/cdarlint/winutils (the community-maintained build,
   since Apache doesn't publish official Windows binaries).
2. Placed both in `C:\hadoop\bin`.
3. Set environment variables (per session, or permanently via `setx`):
   ```powershell
   $env:HADOOP_HOME = "C:\hadoop"
   $env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
   ```
4. Also copied `hadoop.dll` into `C:\Windows\System32` (needed an elevated/
   Administrator PowerShell window — `Copy-Item` fails with "Access
   Denied" otherwise). In practice, once `HADOOP_HOME` was on `PATH`, the
   System32 copy turned out to be unnecessary — Spark found the DLL via
   `PATH` alone.

**Result:** `scripts/local_rerun_proof.py` and `pytest tests/ -v` both ran
cleanly afterward, e.g.:
```
=== Run 1 ===
{'run_id': 'proof-run-1', 'raw_count': 5125, 'rejected_count': 422, 'curated_count': 4595} checksum: e878de8a74a5123ce13367a9007af0a8
=== Run 2 (same input) ===
{'run_id': 'proof-run-2', ...} checksum: e878de8a74a5123ce13367a9007af0a8
PASS — identical input produced identical curated output on both runs.
```

---

## 4. Glue job failing with `GlueArgumentError`

**Symptom:**
```
Error Category: INVALID_ARGUMENT_ERROR
GlueArgumentError: the following arguments are required: --RAW_TRANSACTIONS_PATH,
--RAW_CUSTOMERS_PATH, --RAW_PRODUCTS_PATH, --CURATED_PATH, --REJECTED_PATH,
--METRICS_PATH, --RUN_ID
```

**Root cause:** The Glue job was run before any **Job parameters** were
configured in the console. `getResolvedOptions` in
`glue_jobs/clean_and_curate_job.py` requires all seven arguments to be
present as job arguments — nothing defaults them.

**Fix:** In Glue Studio → job → **Job details** tab → **Job parameters**,
added:

| Key | Value |
|---|---|
| `--RAW_TRANSACTIONS_PATH` | `s3://<bucket>/raw/transactions/` |
| `--RAW_CUSTOMERS_PATH` | `s3://<bucket>/raw/customers/` |
| `--RAW_PRODUCTS_PATH` | `s3://<bucket>/raw/products/` |
| `--CURATED_PATH` | `s3://<bucket>/curated/transactions/` |
| `--REJECTED_PATH` | `s3://<bucket>/rejected/transactions/` |
| `--METRICS_PATH` | `s3://<bucket>/control/run_metrics/` |
| `--RUN_ID` | `manual-test-1` (a run label; Step Functions supplies this automatically in orchestrated runs) |

Also confirmed **Advanced properties → Python library path** was set to
`s3://<bucket>/artifacts/etl_pkg.zip`, since the job also needs to import
the shared `etl/` package.

---

## 5. Job reported "Succeeded" but S3 curated folder looked empty

**Symptom:** After the parameter fix, the job ran to completion
(`Run status: Succeeded`), but browsing to `curated/transactions/` in the
S3 console appeared to show nothing.

**Diagnosis path:**
1. Pulled the job's stdout from CloudWatch (Glue job run → **Output
   logs**, easier to reach this way than hunting through the raw
   `/aws-glue/jobs/logs-v2` log group's 14 streams) and found:
   ```
   [run=manual-test-1] raw_count=5125 rejected_count=422 curated_count=4595
   ```
   This matched the local proof exactly — `curated_count` was **not**
   zero, so the transform logic was producing real output.
2. Root cause was simpler than expected: `write_curated()` writes with
   `.partitionBy("year", "month", "day")`, so files land nested at
   `curated/transactions/year=YYYY/month=MM/day=DD/part-....parquet`, not
   as flat files directly under `curated/transactions/`. The folder
   "looked empty" only because the S3 console browsing stopped one level
   too shallow.

**Fix:** No code change needed here — just drilled into the
`year=.../month=.../day=.../` subfolders in the S3 console to confirm the
Parquet files were actually there.

---

## 6. `curated/customers/` and `curated/products/` genuinely empty

**Symptom:** Unlike issue #5, these two folders were actually empty at
every depth — no files existed anywhere under them.

**Root cause:** A real gap in the pipeline code, not a false alarm.
`glue_jobs/clean_and_curate_job.py` reads `customers_df` and `products_df`
from the raw zone, but only uses them as **in-memory lookup tables**
inside `clean_and_curate()` for orphan-reference validation — it never
writes them anywhere. Similarly, `glue_jobs/catalog_and_publish_job.py`
(the Stage 2 job) only runs `MSCK REPAIR TABLE` for
`curated.transactions` — it doesn't publish the dimension tables either.
Meanwhile `athena/ddl_curated.sql` defines `curated.customers` and
`curated.products` as external tables pointing at those (permanently
empty) S3 locations, and every analytical query that needs customer or
product names `JOIN`s against them — so those joins silently returned
zero rows.

**Fix:** Added two new write calls at the end of
`clean_and_curate_job.py`, plus two new required job parameters:

```python
from etl.transform import clean_and_curate, standardize_column_names

REQUIRED_ARGS = [
    "JOB_NAME", "RAW_TRANSACTIONS_PATH", "RAW_CUSTOMERS_PATH", "RAW_PRODUCTS_PATH",
    "CURATED_PATH", "CURATED_CUSTOMERS_PATH", "CURATED_PRODUCTS_PATH",
    "REJECTED_PATH", "METRICS_PATH", "RUN_ID",
]

# ... existing reads/transform/writes unchanged ...

standardize_column_names(customers_df).write.mode("overwrite").parquet(args["CURATED_CUSTOMERS_PATH"])
standardize_column_names(products_df).write.mode("overwrite").parquet(args["CURATED_PRODUCTS_PATH"])

job.commit()
```

New job parameters added in the console:

| Key | Value |
|---|---|
| `--CURATED_CUSTOMERS_PATH` | `s3://<bucket>/curated/customers/` |
| `--CURATED_PRODUCTS_PATH` | `s3://<bucket>/curated/products/` |

After re-running the job with the updated script, `.parquet` files
appeared directly under `curated/customers/` and `curated/products/`
(no partitioning on these — they're small, unpartitioned dimension
tables, republished in full on every run).

---

## 7. Athena — `SCHEMA_NOT_FOUND: Schema 'curated' does not exist`

**Symptom:**
```
SCHEMA_NOT_FOUND: line 4:6: Schema 'curated' does not exist
Esta consulta se ejecutó con respecto a la base de datos "raw_zone" ...
```

**Root cause:** The DDL step (`athena/ddl_curated.sql`) that creates the
`curated`/`rejected` databases and their external table definitions had
never been run — the analytical queries were being executed against
whatever database happened to be selected in the editor (`raw_zone`),
and `curated` simply didn't exist in the Data Catalog yet.

**Fix:** Ran each statement in `athena/ddl_curated.sql` individually in
the Athena query editor (one `CREATE DATABASE` / `CREATE EXTERNAL TABLE`
at a time), with `REPLACE_BUCKET` swapped for the real bucket name. Then,
since `curated.transactions` and `rejected.transactions` are
**partitioned** tables, ran:
```sql
MSCK REPAIR TABLE curated.transactions;
MSCK REPAIR TABLE rejected.transactions;
```
to make the `year=/month=/day=` and `run_id=` partitions Glue had already
written visible to Athena. (`curated.customers`, `curated.products`, and
`curated.run_metrics` are unpartitioned, so no repair needed for those.)

Confirmed with:
```sql
SELECT COUNT(*) FROM curated.transactions;
```

---

## 8. Analytical queries — final versions used

Once the above was resolved, these three questions were answered directly
against the curated layer (adapted from the six base queries in
`athena/queries.sql`):

**Total revenue by month:**
```sql
SELECT year, month,
       SUM(amount) AS revenue,
       COUNT(*)    AS orders
FROM curated.transactions
GROUP BY year, month
ORDER BY year, month;
```

**Top 10 customers by revenue:**
```sql
SELECT c.customer_id, c.customer_name,
       SUM(t.amount) AS lifetime_spend,
       COUNT(*)      AS orders
FROM curated.transactions t
JOIN curated.customers c ON t.customer_id = c.customer_id
GROUP BY c.customer_id, c.customer_name
ORDER BY lifetime_spend DESC
LIMIT 10;
```

**Product with the highest transaction volume:**
```sql
SELECT p.product_name,
       COUNT(*) AS transaction_count
FROM curated.transactions t
JOIN curated.products p ON t.product_id = p.product_id
GROUP BY p.product_name
ORDER BY transaction_count DESC
LIMIT 10;
```
(Swap `COUNT(*)` for `SUM(t.quantity)` if "volume" should mean units sold
rather than number of orders.)

Verified result for the volume query: **Product 147** led with 48
transactions, ahead of Product 11 (46), Product 114 (44), Product 103
(42), Product 35 (41), and Product 110 (40).

---

## Summary of root causes, for quick reference

| # | Symptom | Root cause | Fix category |
|---|---|---|---|
| 1 | pip/venv issues on 3.12 | PySpark wheel compatibility | Use Python 3.11 locally |
| 2 | SSL cert errors on pip install | Corporate TLS interception | `--use-feature=truststore` |
| 3 | All pytest tests fail identically | Missing `winutils.exe`/`hadoop.dll` on Windows | Install Hadoop native binaries, set `HADOOP_HOME` |
| 4 | `GlueArgumentError` | Job parameters never configured | Add the 7 required `--KEY value` pairs |
| 5 | "Empty" curated folder | Looked one directory level too shallow (partitioned write) | Drill into `year=/month=/day=` subfolders |
| 6 | Customers/products folders truly empty | Job never wrote dimension tables, only used them as lookups | Add explicit write calls + new job params |
| 7 | `SCHEMA_NOT_FOUND` in Athena | DDL never run | Run `ddl_curated.sql`, then `MSCK REPAIR TABLE` |
