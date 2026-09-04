# Schema — Customers

Source: `raw/customers/customers.csv` · Cataloged by: `sales-pipeline-raw-crawler` → database `raw_zone`, table `customers`

| Column | Crawler-inferred type | Corrected type | Why corrected |
|---|---|---|---|
| customer_id | string | string | fine as-is |
| customer_name | string | string | fine as-is |
| email | string | string | fine as-is |
| region | string | string | fine as-is — low-cardinality, keep as string not enum (Athena has no enum type) |
| signup_date | string | date | crawler cannot distinguish a date-shaped string from free text; cast explicitly in the ETL job |

Curated table: `curated.customers` — unpartitioned (small dimension table, republished in full on every run).
