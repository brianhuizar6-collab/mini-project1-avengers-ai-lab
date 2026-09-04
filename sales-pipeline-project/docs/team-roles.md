# Who does what in the AWS Console

Six roles, mapped to the exact console pages in `docs/console-walkthrough.md`.
Each section says what that person owns, exactly which screens they touch,
what they need from a teammate before they can start, and what they hand off
when they're done. Work through roughly in order — 1 and 2 can start
immediately and in parallel; 3/4, 5, and 6 each need something from an
earlier role before their AWS work can begin (their code/config can still be
written and tested locally in the meantime).

---

## Role 1 — Landing & zones

**Owns:** the S3 bucket and its folder structure; getting the synthetic data into it.

**Needs before starting:** nothing — this is the first thing anyone touches.

**Console work** (`console-walkthrough.md` §1–2):
1. **S3 console → Create bucket.** Pick the bucket name everyone else will reuse (globally unique — something like `sales-pipeline-<yourname>-2026`), enable **Bucket Versioning**, leave public access blocked.
2. **Create folder** ten times to lay out the zone prefixes: `raw/customers/`, `raw/products/`, `raw/transactions/`, `rejected/transactions/`, `curated/transactions/`, `curated/customers/`, `curated/products/`, `control/run_metrics/`, `athena-results/`, `artifacts/`.
3. Run `python data/generate_synthetic_data.py` locally, then **Upload** the three resulting CSVs into `raw/customers/`, `raw/products/`, `raw/transactions/` via drag-and-drop in the console.

**Hands off:** the bucket name and the fact that raw data is live — post it to the team so everyone else can fill in `<bucket>` in their own steps.

---

## Role 2 — Cataloging & schema documentation

**Owns:** making the raw data's structure visible to the rest of AWS, and writing up the schema decisions.

**Needs before starting:** Role 1's bucket and uploaded raw CSVs.

**Console work** (`console-walkthrough.md` §4):
1. **AWS Glue → Data Catalog → Databases → Add database** — create `raw_zone`.
2. **Crawlers → Create crawler** named `sales-pipeline-raw-crawler`. Add all three `raw/.../ ` S3 paths as data sources, pick the shared IAM role (from Role 6/whoever sets it up — see below), target database `raw_zone`, frequency **On demand**.
3. **Run crawler**, wait for **Ready**.
4. **Data Catalog → Tables** — open each of the three tables' **Schema** tab. This is the crawler's *inferred* schema.
5. Compare it line-by-line against `docs/schema-customers.md`, `docs/schema-products.md`, `docs/schema-transactions.md` in the repo — those already contain the *corrected* types and the reasoning (e.g. `amount` inferred as `string`, corrected to `decimal(10,2)` because the source mixes `"100.00"` and `"$1,204.50"`). Adjust the docs if your actual generated data infers differently.

**Hands off:** confirmation that the raw tables are cataloged and the schema docs are accurate — Role 3/4 reference these corrected types inside the ETL job.

---

## Role 3 & Role 4 — the ETL job (paired — one Glue job, two areas of ownership)

These two roles share a single AWS artifact (the `clean-and-curate` Glue job), so
on the AWS side they typically sit down together for the console steps, even
though the logic each of them is responsible for lives in different functions
inside `etl/transform.py`.

**Role 3 owns:** standardizing column names/types, missing-value rules, and the amount/date/reference validation checks — everything up through `assign_reject_reason()` in `etl/transform.py`.

**Role 4 owns:** deterministic deduplication and the partitioned, rerun-safe curated write — `deterministic_dedupe()`, `add_partition_columns()`, and the write configuration in `etl/io_utils.py`.

**Needs before starting:** Role 1's bucket/raw data and Role 2's corrected schema docs (the type-casting logic in `etl/transform.py` follows exactly what those docs specify). The code itself can be written and unit-tested locally (`pytest tests/ -v`) before any AWS console work starts — do that first, it catches almost every bug before it costs a Glue job run.

**Console work** (`console-walkthrough.md` §5–6), once the local tests pass:
1. Locally: `zip -r etl_pkg.zip etl` (zips the shared `etl/` package).
2. **S3 console** → open `artifacts/` → **Upload** `etl_pkg.zip`, `glue_jobs/clean_and_curate_job.py`, and `glue_jobs/catalog_and_publish_job.py`.
3. **AWS Glue → ETL jobs → Author code with a script editor.** Engine **Spark**. Upload/paste `clean_and_curate_job.py`. Name the job `clean-and-curate`.
4. **Job details** tab: IAM role = the shared role (see Role 6), Glue version **4.0**, worker type `G.1X` × 2.
5. **Advanced properties → Python library path** = `s3://<bucket>/artifacts/etl_pkg.zip` — this is what makes the console job able to `import etl.transform`.
6. **Job parameters** — add the seven `--KEY value` pairs listed in the walkthrough (`RAW_TRANSACTIONS_PATH`, `RAW_CUSTOMERS_PATH`, `RAW_PRODUCTS_PATH`, `CURATED_PATH`, `REJECTED_PATH`, `METRICS_PATH`, `RUN_ID`).
7. **Save**, then **Run**. Watch the **Runs** tab for `Succeeded`, and open the run's logs to see the `raw_count=... rejected_count=... curated_count=...` line — that line is Role 3+4's proof the job works end to end.

**Hands off:** a working `clean-and-curate` job that writes curated Parquet and rejected rows — Role 5 queries the output, Role 6 wires it into orchestration.

---

## Role 5 — Serving & analytics

**Owns:** making curated data queryable, and the six analytical SQL queries.

**Needs before starting:** the Athena table DDL only needs the S3 paths to exist (Role 1), but running real queries needs at least one successful `clean-and-curate` run (Role 3/4) to have data to query.

**Console work** (`console-walkthrough.md` §7–8):
1. **Athena → Query editor → Settings → Manage** — set query result location to `s3://<bucket>/athena-results/`.
2. Run each statement in `athena/ddl_curated.sql` (with `REPLACE_BUCKET` swapped for the real bucket name) one at a time — creates the `curated` and `rejected` databases and their tables.
3. Build the second Glue job, `catalog-and-publish` (Python Shell engine, paste `glue_jobs/catalog_and_publish_job.py`, job parameters `CURATED_DATABASE`, `CURATED_TABLE`, `ATHENA_OUTPUT`, `ATHENA_WORKGROUP`) — this is the one that runs `MSCK REPAIR TABLE` so new partitions show up. Run it once by hand after Role 3/4's job has produced data.
4. `SELECT COUNT(*) FROM curated.transactions;` to confirm rows are visible.
5. Run every query in `athena/queries.sql` (the six required analytical queries plus the bonus `run_metrics` query), sanity-check the results against what you'd expect from the synthetic data.

**Hands off:** proof the curated layer is queryable — screenshots or exported results of the six queries are usually the actual submission artifact for requirement 9.

---

## Role 6 — Orchestration & QA

**Owns:** the shared IAM role, Step Functions, the optional schedule, and the rerun-safety proof — the glue holding everyone else's pieces together.

**Needs before starting:** should set up the shared IAM role *first*, before Roles 2–5 need it. Step Functions itself needs both Glue jobs (Role 3/4 and Role 5) to already exist.

**Console work** (`console-walkthrough.md` §3, §9–11):
1. **IAM → Roles → Create role**, service **Glue**, attach `AWSGlueServiceRole`, then edit its trust policy to also allow `states.amazonaws.com`, then add the inline S3 + Glue + Athena permissions policy. Do this early and share the role name with the rest of the team. *(Skip entirely and just tell everyone the existing role's name if your AWS account already has a broad pre-made role — common in training accounts.)*
2. Once both Glue jobs exist: **Step Functions → Create state machine → Write your workflow in code.** Paste `orchestration/state_machine.asl.json` (bucket placeholder replaced), name it `sales-pipeline`, select the shared IAM role.
3. **Start execution** with input `{}`, confirm both states go green.
4. *(Optional, only if the assignment needs a schedule)* **EventBridge → Rules → Create rule**, cron schedule, target the state machine, let the console auto-create the invoke role.
5. Run the rerun proof: start a second execution against the same untouched raw data, compare `SELECT COUNT(*) FROM curated.transactions;` and the S3 object count between the two runs, and fill in `docs/rerun-proof-template.md` with both sets of numbers.
6. Also run `pytest tests/ -v` locally as a sanity check that the whole suite is green before the final submission.

**Hands off:** the completed, orchestrated pipeline plus the rerun-safety evidence — this is usually the last piece before the team submits.

---

## Suggested order of operations

```
Role 1 (bucket + raw data)
   │
   ├──► Role 2 (crawler + schema docs)
   │        │
   │        ▼
   ├──► Role 3+4 (build + run clean-and-curate job)   ◄── Role 6 sets up the shared IAM role in parallel with Role 1/2
   │        │
   │        ▼
   ├──► Role 5 (Athena tables + queries, catalog-and-publish job)
   │        │
   │        ▼
   └──► Role 6 (Step Functions, schedule, rerun proof)
```

Roles 3/4, once their local pytest suite is green, don't need to wait on
anyone to *write* their code — only the actual console deployment (§6) needs
Role 1's bucket to exist.
