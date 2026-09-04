-- Run these once in the Athena query editor (or via `aws athena start-query-execution`).
-- Replace REPLACE_BUCKET with the pipeline's bucket name.

CREATE DATABASE IF NOT EXISTS curated;
CREATE DATABASE IF NOT EXISTS rejected;

-- Curated, partitioned transactions -----------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS curated.transactions (
    transaction_id        string,
    customer_id           string,
    product_id            string,
    amount                decimal(10,2),
    transaction_date      date,
    quantity              int,
    promo_code            string,
    is_imputed            boolean,
    ingestion_timestamp   timestamp,
    source_file           string,
    run_id                string
)
PARTITIONED BY (year string, month string, day string)
STORED AS PARQUET
LOCATION 's3://REPLACE_BUCKET/curated/transactions/';

-- Run after the first load, and again after any manual backfill:
-- MSCK REPAIR TABLE curated.transactions;
-- (the pipeline's Stage 2 Glue job runs this automatically after every batch)

-- Curated dimension tables (small, unpartitioned, republished in full each run)
CREATE EXTERNAL TABLE IF NOT EXISTS curated.customers (
    customer_id     string,
    customer_name   string,
    email           string,
    region          string,
    signup_date     date
)
STORED AS PARQUET
LOCATION 's3://REPLACE_BUCKET/curated/customers/';

CREATE EXTERNAL TABLE IF NOT EXISTS curated.products (
    product_id      string,
    product_name    string,
    category        string,
    unit_price      decimal(10,2)
)
STORED AS PARQUET
LOCATION 's3://REPLACE_BUCKET/curated/products/';

-- Rejected zone, partitioned by run so a bad batch is easy to isolate -------
CREATE EXTERNAL TABLE IF NOT EXISTS rejected.transactions (
    transaction_id        string,
    customer_id           string,
    product_id            string,
    amount                decimal(10,2),
    transaction_date      date,
    quantity              int,
    promo_code            string,
    ingestion_timestamp   timestamp,
    source_file           string,
    reject_reason         string
)
PARTITIONED BY (run_id string)
STORED AS PARQUET
LOCATION 's3://REPLACE_BUCKET/rejected/transactions/';

-- MSCK REPAIR TABLE rejected.transactions;

-- Run metrics — the raw/rejected/curated counts logged by every job run ----
CREATE EXTERNAL TABLE IF NOT EXISTS curated.run_metrics (
    run_id           string,
    stage            string,
    raw_count        bigint,
    rejected_count   bigint,
    curated_count    bigint,
    ts               string
)
STORED AS PARQUET
LOCATION 's3://REPLACE_BUCKET/control/run_metrics/';
