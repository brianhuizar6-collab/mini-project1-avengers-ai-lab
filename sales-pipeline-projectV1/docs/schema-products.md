# Schema — Products

Source: `raw/products/products.csv` · Cataloged by: `sales-pipeline-raw-crawler` → database `raw_zone`, table `products`

| Column | Crawler-inferred type | Corrected type | Why corrected |
|---|---|---|---|
| product_id | string | string | fine as-is |
| product_name | string | string | fine as-is |
| category | string | string | fine as-is |
| unit_price | double | decimal(10,2) | double introduces float rounding error on money; decimal(10,2) is exact |

Curated table: `curated.products` — unpartitioned (small dimension table, republished in full on every run).
