"""
Unit tests for etl/transform.py — no AWS credentials or Glue needed.
Run: pytest tests/ -v
"""
from pyspark.sql.types import StringType, StructField, StructType

from etl.transform import clean_and_curate

TXN_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("transaction_date", StringType()),
    StructField("amount", StringType()),
    StructField("quantity", StringType()),
    StructField("promo_code", StringType()),
    StructField("ingestion_timestamp", StringType()),
    StructField("source_file", StringType()),
])


def _customers(spark):
    return spark.createDataFrame(
        [("C1", "Ana", "ana@x.com", "north", "2023-01-01"),
         ("C2", "Beto", "beto@x.com", "south", "2023-02-01")],
        ["customer_id", "customer_name", "email", "region", "signup_date"],
    )


def _products(spark):
    return spark.createDataFrame(
        [("P1", "Widget", "home", 10.0),
         ("P2", "Gadget", "electronics", 20.0)],
        ["product_id", "product_name", "category", "unit_price"],
    )


def _base_transactions(spark, rows):
    # explicit schema (all-string) — a single-row batch with a None value
    # (e.g. missing promo_code) can't have its type inferred automatically.
    return spark.createDataFrame(rows, TXN_SCHEMA)


def test_valid_row_passes_and_lands_in_curated(spark):
    rows = [("T1", "C1", "P1", "2025-01-15", "100.00", 1, "", "2025-01-15 10:00:00", "f.csv")]
    curated, rejected, counts = clean_and_curate(_base_transactions(spark, rows), _customers(spark), _products(spark), "run1")
    assert counts["curated_count"] == 1
    assert counts["rejected_count"] == 0
    assert curated.collect()[0]["amount"] == 100.00


def test_missing_customer_id_is_rejected(spark):
    rows = [("T1", "", "P1", "2025-01-15", "100.00", 1, "", "2025-01-15 10:00:00", "f.csv")]
    curated, rejected, counts = clean_and_curate(_base_transactions(spark, rows), _customers(spark), _products(spark), "run1")
    assert counts["curated_count"] == 0
    assert rejected.collect()[0]["reject_reason"] == "missing_required_field"


def test_negative_amount_is_rejected(spark):
    rows = [("T1", "C1", "P1", "2025-01-15", "-5.00", 1, "", "2025-01-15 10:00:00", "f.csv")]
    _, rejected, counts = clean_and_curate(_base_transactions(spark, rows), _customers(spark), _products(spark), "run1")
    assert counts["rejected_count"] == 1
    assert rejected.collect()[0]["reject_reason"] == "invalid_amount"


def test_malformed_date_is_rejected(spark):
    rows = [("T1", "C1", "P1", "not-a-date", "50.00", 1, "", "2025-01-15 10:00:00", "f.csv")]
    _, rejected, counts = clean_and_curate(_base_transactions(spark, rows), _customers(spark), _products(spark), "run1")
    assert rejected.collect()[0]["reject_reason"] == "invalid_date"


def test_orphan_customer_reference_is_rejected(spark):
    rows = [("T1", "C999", "P1", "2025-01-15", "50.00", 1, "", "2025-01-15 10:00:00", "f.csv")]
    _, rejected, counts = clean_and_curate(_base_transactions(spark, rows), _customers(spark), _products(spark), "run1")
    assert rejected.collect()[0]["reject_reason"] == "orphan_reference"


def test_currency_formatted_amount_is_parsed(spark):
    rows = [("T1", "C1", "P1", "2025-01-15", "$1,204.50", 1, "", "2025-01-15 10:00:00", "f.csv")]
    curated, _, counts = clean_and_curate(_base_transactions(spark, rows), _customers(spark), _products(spark), "run1")
    assert counts["curated_count"] == 1
    assert float(curated.collect()[0]["amount"]) == 1204.50


def test_missing_promo_code_is_imputed_not_dropped(spark):
    rows = [("T1", "C1", "P1", "2025-01-15", "10.00", 1, None, "2025-01-15 10:00:00", "f.csv")]
    curated, _, _ = clean_and_curate(_base_transactions(spark, rows), _customers(spark), _products(spark), "run1")
    row = curated.collect()[0]
    assert row["promo_code"] == "NONE"
    assert row["is_imputed"] is True


def test_duplicate_transaction_id_deterministic_survivor(spark):
    rows = [
        ("T1", "C1", "P1", "2025-01-15", "10.00", 1, "", "2025-01-15 09:00:00", "early.csv"),
        ("T1", "C1", "P1", "2025-01-15", "10.00", 1, "", "2025-01-15 15:00:00", "late.csv"),
    ]
    curated, _, counts = clean_and_curate(_base_transactions(spark, rows), _customers(spark), _products(spark), "run1")
    assert counts["curated_count"] == 1  # duplicate collapsed to one row
    assert curated.collect()[0]["source_file"] == "late.csv"  # latest ingestion_timestamp wins


def test_rerun_on_identical_input_is_deterministic(spark):
    """Requirement #8: same input -> same curated output, run after run."""
    rows = [
        ("T1", "C1", "P1", "2025-01-15", "10.00", 1, "", "2025-01-15 09:00:00", "a.csv"),
        ("T1", "C1", "P1", "2025-01-15", "10.00", 1, "", "2025-01-15 15:00:00", "b.csv"),
        ("T2", "C2", "P2", "2025-01-16", "$20.00", 2, "SAVE10", "2025-01-16 09:00:00", "a.csv"),
    ]
    txns = _base_transactions(spark, rows)
    curated1, rejected1, counts1 = clean_and_curate(txns, _customers(spark), _products(spark), "run-a")
    curated2, rejected2, counts2 = clean_and_curate(txns, _customers(spark), _products(spark), "run-b")

    cols = ["transaction_id", "customer_id", "product_id", "amount", "transaction_date", "source_file"]
    c1 = sorted(curated1.select(*cols).collect())
    c2 = sorted(curated2.select(*cols).collect())
    assert c1 == c2
    assert counts1["curated_count"] == counts2["curated_count"] == 2
    assert counts1["rejected_count"] == counts2["rejected_count"] == 0
