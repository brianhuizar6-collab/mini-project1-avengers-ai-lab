# Schema — Transactions

Source: `raw/transactions/transactions.csv` · Cataloged by: `sales-pipeline-raw-crawler` → database `raw_zone`, table `transactions`

| Column | Crawler-inferred type | Corrected type | Why corrected |
|---|---|---|---|
| transaction_id | string | string | fine as-is |
| customer_id | bigint (sometimes string) | string | crawler infers numeric-looking IDs as bigint; keep identifiers as strings so leading characters/zeros survive and joins stay type-safe |
| product_id | string | string | fine as-is |
| transaction_date | string | date | source mixes `yyyy-MM-dd` and `MM/dd/yyyy` — crawler leaves it as string; ETL job coalesces both formats |
| amount | string | decimal(10,2) | source mixes `"100.00"`, `"$1,204.50"`, `"1,204.50"` — crawler can't infer a numeric type from mixed currency formatting; ETL job strips symbols/separators before casting |
| quantity | bigint | int | bigint is oversized for a 1–5 unit count; int is sufficient and cheaper to store |
| promo_code | string | string | fine as-is; empty strings are imputed to `"NONE"` by the ETL job, see docs below |
| ingestion_timestamp | string | timestamp | crawler leaves timestamps as string when format varies |
| source_file | string | string | fine as-is — used as a dedupe tie-breaker |

## Columns added by the ETL job (not present in raw)

| Column | Type | Meaning |
|---|---|---|
| is_imputed | boolean | true if `promo_code` was defaulted from a missing value |
| reject_reason | string | populated only in the rejected zone — one of `missing_required_field`, `invalid_amount`, `invalid_date`, `orphan_reference` |
| year, month, day | string | derived from `transaction_date`, used as the curated Parquet partition keys |
| run_id | string | the Step Functions execution name that produced the row |

Curated table: `curated.transactions` — partitioned by `year`, `month`, `day` (see README.md "Partitioning strategy").
