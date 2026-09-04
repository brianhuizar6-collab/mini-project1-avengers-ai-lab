# AWS Management Console walkthrough (no AWS CLI)

Every step below is a click-path in the AWS Console. Do them in order — each
section builds on S3 paths and names created in the one before it. Replace
`<bucket>` with whatever bucket name you pick in step 1 (bucket names are
globally unique, so add a suffix like `sales-pipeline-brian-2026`).

---

## 1. S3 — create the raw / rejected / curated zones (requirement 1)

1. Console → **S3** → **Create bucket**.
2. Bucket name: `<bucket>`. Region: pick one and use it everywhere below (e.g. `us-east-1`).
3. Leave "Block all public access" **checked**. Enable **Bucket Versioning** (Properties tab, after creation, or under the creation form's "Bucket Versioning" section — turn it on).
4. Click **Create bucket**.
5. Open the bucket → **Create folder**, and create these folders one at a time (S3 "folders" are just key prefixes — this also gives you a clean place to drag files into):
   - `raw/customers/`
   - `raw/products/`
   - `raw/transactions/`
   - `rejected/transactions/`
   - `curated/transactions/`
   - `curated/customers/`
   - `curated/products/`
   - `control/run_metrics/`
   - `athena-results/`
   - `artifacts/` (holds the ETL code the Glue job will read)

## 2. Generate and upload the synthetic datasets

This part runs on your laptop, not AWS — no CLI needed for AWS itself, just Python locally.

```bash
pip install -r requirements.txt --break-system-packages   # or a venv
python data/generate_synthetic_data.py --out data/raw --n-transactions 5000
```

That writes `data/raw/customers/customers.csv`, `data/raw/products/products.csv`,
`data/raw/transactions/transactions.csv` — with deliberately injected
duplicates, missing fields, bad amounts/dates, and orphan references, so the
validation rules below have something real to catch.

Back in the S3 console:
- Open `raw/customers/` → **Upload** → **Add files** → select `customers.csv` → **Upload**.
- Repeat for `raw/products/products.csv` → `raw/products/`.
- Repeat for `raw/transactions/transactions.csv` → `raw/transactions/`.

## 3. IAM — one shared execution role (used by every step below)

Glue, Step Functions and EventBridge each need a role to call other AWS
services on your behalf — this is unavoidable no matter how you build the
pipeline, console or not. One role, reused everywhere, is the minimum.

**Skip this whole section if your account already has a broad pre-made role**
(common in training/sandbox AWS accounts — often named something like
`LabRole`). Just note its name and use it everywhere the steps below say
"select the sales-pipeline role."

1. Console → **IAM** → **Roles** → **Create role**.
2. Trusted entity type: **AWS service**. Use case: **Glue**. Click **Next**.
3. Attach policy: search for and check **AWSGlueServiceRole**. Click **Next**.
4. Role name: `sales-pipeline-execution-role`. Click **Create role**.
5. Open the role you just created → **Trust relationships** tab → **Edit trust policy**. Add a second trusted service so the same role also works for Step Functions — replace the policy with:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {"Effect": "Allow", "Principal": {"Service": "glue.amazonaws.com"}, "Action": "sts:AssumeRole"},
       {"Effect": "Allow", "Principal": {"Service": "states.amazonaws.com"}, "Action": "sts:AssumeRole"}
     ]
   }
   ```
   Click **Update policy**.
6. Back on the role's **Permissions** tab → **Add permissions** → **Create inline policy** → **JSON** tab → paste (replace `<bucket>`):
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:DeleteObject"],
         "Resource": ["arn:aws:s3:::<bucket>", "arn:aws:s3:::<bucket>/*"]
       },
       {
         "Effect": "Allow",
         "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:StartCrawler", "glue:GetCrawler",
                    "athena:StartQueryExecution", "athena:GetQueryExecution"],
         "Resource": "*"
       }
     ]
   }
   ```
   Click **Next**, name it `sales-pipeline-scoped-access`, **Create policy**.

## 4. Glue Data Catalog — crawl the raw zone (requirement 2)

1. Console → **AWS Glue** → **Data Catalog** → **Databases** → **Add database**. Name: `raw_zone`. **Create database**.
2. **Crawlers** → **Create crawler**.
3. Name: `sales-pipeline-raw-crawler`. **Next**.
4. Data source: **Add a data source** → S3 → browse to `s3://<bucket>/raw/customers/` → **Add**. Repeat for `raw/products/` and `raw/transactions/` so the crawler has all three S3 targets. **Next**.
5. Existing IAM role: select `sales-pipeline-execution-role`. **Next**.
6. Target database: `raw_zone`. Frequency: **On demand**. **Next** → **Create crawler**.
7. Select the crawler → **Run crawler**. Wait for status **Ready**.
8. **Data Catalog** → **Tables** — open `customers`, `products`, `transactions` and check the **Schema** tab. This is the *inferred* schema.
9. Compare it against `docs/schema-customers.md`, `docs/schema-products.md`, `docs/schema-transactions.md` in this repo — those three files are the *corrected* schema with the reasoning already written up (this satisfies "document inferred versus corrected schemas").

## 5. Package and upload the ETL code

The Glue job script (`glue_jobs/clean_and_curate_job.py`) imports the shared
`etl/` package. Zip it locally, then upload the zip through the console —
still no AWS CLI:

```bash
cd sales-pipeline-project
zip -r etl_pkg.zip etl
```

In the S3 console: open `artifacts/` → **Upload** → add `etl_pkg.zip`,
`glue_jobs/clean_and_curate_job.py`, and `glue_jobs/catalog_and_publish_job.py`
(drag all three into the upload dialog) → **Upload**.

## 6. Glue Studio — Stage 1 job: CleanAndCurate (requirements 3, 4, 5, 8, 10)

1. Console → **AWS Glue** → **ETL jobs** → **Author code with a script editor**.
2. Engine: **Spark**. Options: **Upload and edit an existing script** → choose `clean_and_curate_job.py` (or select "Create a new script" and paste its contents from this repo). **Create**.
3. **Job details** tab:
   - Name: `clean-and-curate`.
   - IAM role: `sales-pipeline-execution-role`.
   - Glue version: **Glue 4.0**.
   - Worker type: `G.1X`, Number of workers: `2` (plenty for a synthetic dataset this size).
4. Still in **Job details**, expand **Advanced properties** → **Python library path** → enter `s3://<bucket>/artifacts/etl_pkg.zip`. This is the console equivalent of `--extra-py-files`.
5. Scroll to **Job parameters** and add these key/value pairs (keys need the leading `--`):

   | Key | Value |
   |---|---|
   | `--RAW_TRANSACTIONS_PATH` | `s3://<bucket>/raw/transactions/` |
   | `--RAW_CUSTOMERS_PATH` | `s3://<bucket>/raw/customers/` |
   | `--RAW_PRODUCTS_PATH` | `s3://<bucket>/raw/products/` |
   | `--CURATED_PATH` | `s3://<bucket>/curated/transactions/` |
   | `--REJECTED_PATH` | `s3://<bucket>/rejected/transactions/` |
   | `--METRICS_PATH` | `s3://<bucket>/control/run_metrics/` |
   | `--RUN_ID` | `manual-test-1` (Step Functions will override this per run later) |

6. Click **Save**, then **Run**. Watch the **Runs** tab until **Run status** is `Succeeded`.
7. Click into the run → **CloudWatch logs / Output logs** to see the printed line `raw_count=... rejected_count=... curated_count=...` — this is requirement 10 (recording counts) satisfied as a log line, viewable straight from the console.

This one job does everything requirement 3 asks for: standardizes column
names/types, applies the missing-value rules, validates amount/date/customer+
product references, deterministically dedupes, and routes invalid rows to the
rejected zone — see `etl/transform.py` for the exact, separately-named
function behind each rule. It writes curated data as **Parquet, partitioned by
year/month/day** (requirements 4–5), using dynamic-partition overwrite so a
rerun never duplicates output (requirement 8 — see §9 below for the proof).

## 7. Glue Studio — Stage 2 job: CatalogAndPublish

1. **ETL jobs** → **Author code with a script editor** → Engine: **Python Shell**.
2. Upload/paste `glue_jobs/catalog_and_publish_job.py`. **Create**.
3. **Job details**: Name `catalog-and-publish`, IAM role `sales-pipeline-execution-role`, Python version 3.9.
4. **Job parameters**:

   | Key | Value |
   |---|---|
   | `--CURATED_DATABASE` | `curated` |
   | `--CURATED_TABLE` | `transactions` |
   | `--ATHENA_OUTPUT` | `s3://<bucket>/athena-results/` |
   | `--ATHENA_WORKGROUP` | `primary` |

5. **Save**. Don't run it yet — it needs the Athena tables from §8 to exist first.

## 8. Athena — make curated data queryable (requirement 6)

1. Console → **Athena** → **Query editor**. First time only: **Settings** → **Manage** → set **Location of query results** to `s3://<bucket>/athena-results/` → **Save**.
2. Open `athena/ddl_curated.sql` from this repo, replace every `REPLACE_BUCKET` with your bucket name, and run each `CREATE DATABASE` / `CREATE EXTERNAL TABLE` statement one at a time in the query editor (paste one statement, click **Run**, repeat).
3. Now go back to the **catalog-and-publish** job (§7) and click **Run** — it executes `MSCK REPAIR TABLE curated.transactions` so Athena sees the partitions the Stage 1 job just wrote.
4. Run `SELECT COUNT(*) FROM curated.transactions;` to confirm rows are visible.
5. Open `athena/queries.sql` and run each of the six analytical queries (requirement 9) plus the bonus `run_metrics` query — paste one, **Run**, read the results grid, repeat.

**Why Athena instead of standing up a Redshift cluster:** the assignment
allows an approved substitute, and Athena needs no cluster to provision or
size — it queries the curated Parquet directly from S3. Redshift Spectrum is
a drop-in upgrade later if you ever need it: same external table definition,
no re-modeling.

## 9. Step Functions — orchestrate the two stages (requirement 7)

1. Console → **Step Functions** → **State machines** → **Create state machine**.
2. Choose **Write your workflow in code**. Type: **Standard**.
3. Open `orchestration/state_machine.asl.json` from this repo, replace every `REPLACE_BUCKET` with your bucket name, and paste the whole thing into the code editor (it already wires `CleanAndCurate` → `CatalogAndPublish` with retries and a `Fail` state).
4. Click **Next** (or **Config** tab). Name: `sales-pipeline`. Permissions: **Choose an existing role** → `sales-pipeline-execution-role`.
5. **Create state machine**.
6. **Start execution** → leave the input as `{}` → **Start execution**. Watch the graph — both states should turn green.

## 10. EventBridge — schedule it (optional, requirement mentions it only if scheduling is needed)

Skip this entirely if on-demand runs are acceptable — that's the simpler
default for a graded exercise.

1. Console → **Amazon EventBridge** → **Rules** → **Create rule**.
2. Name: `sales-pipeline-nightly`. Rule type: **Schedule**. **Next**.
3. Schedule pattern: **Cron-based schedule** → `0 3 * * ? *` (03:00 UTC daily). **Next**.
4. Target: **Step Functions state machine** → select `sales-pipeline`.
5. For the execution role, choose **Create a new role for this specific resource** — the console generates a correctly-scoped role automatically, no manual IAM work needed. **Next** → **Create rule**.

## 11. Prove reruns don't duplicate output (requirement 8)

**Locally** (no AWS needed):
```bash
python scripts/local_rerun_proof.py
```
This runs the Stage-1 logic twice against the same input and checksums the
curated output — prints `PASS` if both runs match byte-for-byte.

**In AWS**, from the console:
1. Step Functions → your `sales-pipeline` state machine → **Start execution** (name it `proof-run-1`). Wait for **Succeeded**.
2. Athena query editor → `SELECT COUNT(*) FROM curated.transactions;` — note the count. Also open the `curated/transactions/` folder in the S3 console and note the object count.
3. Start execution again (name it `proof-run-2`), same input, no changes to the raw files.
4. Repeat step 2 — the counts should be identical to the first run, proving the dynamic-partition-overwrite write replaced the same partitions rather than appending duplicates.
5. Record both runs' numbers in `docs/rerun-proof-template.md`.

## 12. Where each requirement lives

| # | Requirement | Console location |
|---|---|---|
| 1 | Raw / rejected / curated zones | §1 — S3 folders |
| 2 | Catalog + inferred vs. corrected schema | §4 — Glue crawler, `docs/schema-*.md` |
| 3 | Glue/PySpark ETL rules | §6 — `clean-and-curate` job, logic in `etl/transform.py` |
| 4 | Curated data in Parquet | §6 — job writes Parquet automatically |
| 5 | Partitioning strategy + justification | §6 + README "Partitioning strategy" |
| 6 | Queryable via Athena/Redshift | §8 |
| 7 | ≥2 orchestrated stages | §9 — Step Functions |
| 8 | Rerun ≠ duplicate output | §11 |
| 9 | ≥6 analytical SQL queries | §8 step 5, `athena/queries.sql` |
| 10 | Counts at raw/rejected/curated | §6 step 7 (CloudWatch logs), `curated.run_metrics` table |
