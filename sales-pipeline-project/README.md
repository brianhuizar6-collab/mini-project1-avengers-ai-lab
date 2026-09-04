# Customer Sales Analytics Pipeline — build instructions

Working code for every mandatory requirement, built entirely through the
**AWS Management Console** — no AWS CLI. The only thing that runs on your
laptop is Python, for generating the sample data and for local testing.

**Start here → [`docs/console-walkthrough.md`](docs/console-walkthrough.md)**
— a numbered, click-by-click sequence through S3, IAM, Glue, Athena, and
Step Functions that builds the whole pipeline, with a table at the end
mapping every assignment requirement to the exact section that satisfies it.

## 0. Local setup, before touching the console

```bash
pip install -r requirements.txt --break-system-packages   # or a venv, your choice
python data/generate_synthetic_data.py --out data/raw --n-transactions 5000
```

This creates `data/raw/{customers,products,transactions}/*.csv` — the three
synthetic datasets, with deliberately injected duplicates, missing fields,
invalid amounts/dates and orphan references, so the validation rules have
something real to catch. `docs/console-walkthrough.md` §2 shows where to
drag these files in the S3 console.

Run the local test suite before touching AWS at all:

```bash
pytest tests/ -v                          # unit tests for every validation/dedupe rule
python scripts/local_rerun_proof.py       # proves rerun-safety locally, no AWS needed
```

If both pass, the ETL logic itself (Roles 3–4's part of the work) is already
correct — everything in the console walkthrough is deployment, not debugging.

## Partitioning strategy

`curated.transactions` is partitioned by `year/month/day` of `transaction_date`.
Customers and products stay unpartitioned — small dimension tables, republished
in full each run. Justification: nearly every stakeholder query in
`athena/queries.sql` filters or groups by a date range, so date partitioning
lets Athena prune everything outside the range before scanning a byte. Day-level
granularity also gives each pipeline run a natural, narrow unit to overwrite —
one run only replaces the date partitions present in its own input batch,
which is the mechanism behind rerun-safety (requirement 8).

## On IAM roles

Glue jobs, crawlers, and the Step Functions state machine all require an
execution role to call other AWS services — that's true whether you build
via console or CLI, there's no way around having at least one. The
walkthrough creates a single shared role (`docs/console-walkthrough.md` §3)
reused by every component. If your TCS/training AWS account already
provisions a broad pre-made role (commonly something like `LabRole` in
sandbox accounts), skip that section and use that role's name everywhere
instead.

## Repo layout

```
docs/console-walkthrough.md       the build guide — follow this top to bottom
data/generate_synthetic_data.py   synthetic Customers/Products/Transactions with injected data-quality issues
etl/transform.py                  all cleaning/validation/dedupe rules — pure PySpark, unit-testable
etl/io_utils.py                   read/write helpers incl. the dynamic-partition-overwrite rerun-safety setting
glue_jobs/                        the two Glue job scripts you paste into Glue Studio's script editor
orchestration/                    Step Functions definition (paste into the console) + optional EventBridge notes
athena/                           table DDL + the six analytical queries (paste into the Athena query editor)
tests/                            pytest suite covering every validation rule and rerun determinism
scripts/local_rerun_proof.py      local-only proof that reruns don't duplicate output — no AWS needed
docs/schema-*.md                  inferred-vs-corrected schema documentation
docs/rerun-proof-template.md      fill in with your own run counts as evidence for requirement 8
```
