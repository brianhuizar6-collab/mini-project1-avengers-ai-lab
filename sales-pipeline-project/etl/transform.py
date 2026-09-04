"""
Pure PySpark transformation logic for the Customer Sales Analytics Pipeline.

Kept free of any AWS/Glue imports so it can be unit-tested locally with
plain PySpark (see tests/test_transform.py) and reused unchanged inside the
Glue job entry point (glue_jobs/clean_and_curate_job.py).

Column naming convention: lower_snake_case everywhere, enforced by
standardize_column_names() as the first step of the pipeline.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# amount and transaction_date are deliberately excluded here — a null amount/date
# after type-casting gets the more specific invalid_amount / invalid_date reason
# from validate_amount()/validate_date() rather than a generic "missing" one.
REQUIRED_TXN_COLUMNS = ["transaction_id", "customer_id", "product_id"]
MAX_AMOUNT_MULTIPLIER = 3.0  # outlier ceiling = 3x the 99th percentile amount

# Shape guards for the two accepted transaction_date layouts. Anything that
# doesn't match is never handed to to_date(), so malformed input becomes NULL
# instead of an exception regardless of the ANSI setting.
ISO_DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"
US_DATE_PATTERN = r"^\d{1,2}/\d{1,2}/\d{4}$"


# --------------------------------------------------------------------------- #
# 1. Standardize column names and types
# --------------------------------------------------------------------------- #
def standardize_column_names(df: DataFrame) -> DataFrame:
    """lower_snake_case every column name and trim its header whitespace."""
    for c in df.columns:
        clean = c.strip().lower().replace(" ", "_").replace("-", "_")
        if clean != c:
            df = df.withColumnRenamed(c, clean)
    return df


def _clean_amount_string(col):
    # strip currency symbols and thousands separators: "$1,204.50" -> "1204.50"
    return F.regexp_replace(F.col(col), r"[$,\s]", "")


def standardize_transaction_types(df: DataFrame) -> DataFrame:
    """Cast raw string columns to the corrected types documented in docs/schema-transactions.md."""
    df = df.withColumn("_amount_clean", _clean_amount_string("amount"))
    df = df.withColumn(
        "amount",
        F.when(F.col("_amount_clean").rlike(r"^-?\d+(\.\d+)?$"),
               F.col("_amount_clean").cast(T.DecimalType(10, 2)))
         .otherwise(F.lit(None).cast(T.DecimalType(10, 2)))
    ).drop("_amount_clean")

    # transaction_date arrives as either ISO (yyyy-MM-dd) or US (MM/dd/yyyy).
    # Each parse is guarded by a shape check so unparseable input (e.g. "not-a-date")
    # yields NULL instead of raising -- try_to_date would express this directly but is
    # Spark 4.0+ only, and Glue 4.0 / local pyspark 3.5 don't have it.
    iso = F.when(F.col("transaction_date").rlike(ISO_DATE_PATTERN),
                 F.to_date(F.col("transaction_date"), "yyyy-MM-dd"))
    us = F.when(F.col("transaction_date").rlike(US_DATE_PATTERN),
                F.to_date(F.col("transaction_date"), "MM/dd/yyyy"))
    df = df.withColumn("transaction_date", F.coalesce(iso, us))

    df = df.withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp"))
    df = df.withColumn("customer_id", F.trim(F.col("customer_id")))
    df = df.withColumn("product_id", F.trim(F.col("product_id")))
    df = df.withColumn("transaction_id", F.trim(F.col("transaction_id")))
    return df


# --------------------------------------------------------------------------- #
# 2. Missing-value rules
# --------------------------------------------------------------------------- #
def apply_missing_value_rules(df: DataFrame) -> DataFrame:
    """
    Required fields missing -> flagged for rejection downstream.
    Optional fields (promo_code) missing -> defaulted, flagged as imputed
    rather than silently dropped or left ambiguous.
    """
    missing_required = F.lit(False)
    for c in REQUIRED_TXN_COLUMNS:
        missing_required = missing_required | F.col(c).isNull() | (F.trim(F.col(c).cast("string")) == "")
    df = df.withColumn("_missing_required", missing_required)

    df = df.withColumn(
        "promo_code",
        F.when((F.col("promo_code").isNull()) | (F.trim(F.col("promo_code")) == ""), F.lit("NONE"))
         .otherwise(F.col("promo_code"))
    )
    df = df.withColumn("is_imputed", F.col("promo_code") == F.lit("NONE"))
    return df


# --------------------------------------------------------------------------- #
# 3. Validation rules -> a single reject_reason column
# --------------------------------------------------------------------------- #
def validate_amount(df: DataFrame, amount_ceiling) -> DataFrame:
    valid = F.col("amount").isNotNull() & (F.col("amount") > 0) & (F.col("amount") <= amount_ceiling)
    return df.withColumn("_valid_amount", valid)


def validate_date(df: DataFrame, min_date: str, max_date: str) -> DataFrame:
    valid = F.col("transaction_date").isNotNull() & \
        (F.col("transaction_date") >= F.lit(min_date)) & \
        (F.col("transaction_date") <= F.lit(max_date))
    return df.withColumn("_valid_date", valid)


def validate_references(df: DataFrame, customers_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """Broadcast-join against the dimension tables to catch orphan foreign keys."""
    known_customers = F.broadcast(customers_df.select("customer_id").distinct()
                                   .withColumnRenamed("customer_id", "_known_customer_id"))
    known_products = F.broadcast(products_df.select("product_id").distinct()
                                  .withColumnRenamed("product_id", "_known_product_id"))

    df = df.join(known_customers, df.customer_id == known_customers._known_customer_id, "left")
    df = df.join(known_products, df.product_id == known_products._known_product_id, "left")
    df = df.withColumn("_valid_customer_ref", F.col("_known_customer_id").isNotNull())
    df = df.withColumn("_valid_product_ref", F.col("_known_product_id").isNotNull())
    return df.drop("_known_customer_id", "_known_product_id")


def assign_reject_reason(df: DataFrame) -> DataFrame:
    """First failing check wins — keeps one reason per row for the §11 reject-rate query."""
    df = df.withColumn(
        "reject_reason",
        F.when(F.col("_missing_required"), F.lit("missing_required_field"))
         .when(~F.col("_valid_amount"), F.lit("invalid_amount"))
         .when(~F.col("_valid_date"), F.lit("invalid_date"))
         .when(~F.col("_valid_customer_ref") | ~F.col("_valid_product_ref"), F.lit("orphan_reference"))
         .otherwise(F.lit(None).cast("string"))
    )
    return df


# --------------------------------------------------------------------------- #
# 4. Deterministic deduplication
# --------------------------------------------------------------------------- #
def deterministic_dedupe(df: DataFrame) -> DataFrame:
    """
    Keep exactly one row per transaction_id, same survivor every run:
    latest ingestion_timestamp wins; source_file breaks any remaining tie.
    Applied only to rows that already passed every other check, so a
    duplicate can't "rescue" an otherwise-invalid row.
    """
    w = Window.partitionBy("transaction_id").orderBy(
        F.col("ingestion_timestamp").desc(), F.col("source_file").asc()
    )
    return (
        df.withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .drop("_rn")
    )


# --------------------------------------------------------------------------- #
# 5. Split + partition columns
# --------------------------------------------------------------------------- #
def split_valid_invalid(df: DataFrame):
    valid = df.filter(F.col("reject_reason").isNull())
    invalid = df.filter(F.col("reject_reason").isNotNull())
    return valid, invalid


def add_partition_columns(df: DataFrame, date_col: str = "transaction_date") -> DataFrame:
    return (
        df.withColumn("year", F.date_format(F.col(date_col), "yyyy"))
          .withColumn("month", F.date_format(F.col(date_col), "MM"))
          .withColumn("day", F.date_format(F.col(date_col), "dd"))
    )


def drop_internal_columns(df: DataFrame) -> DataFrame:
    internal = ["_missing_required", "_valid_amount", "_valid_date",
                "_valid_customer_ref", "_valid_product_ref"]
    return df.drop(*[c for c in internal if c in df.columns])


# --------------------------------------------------------------------------- #
# Orchestrating function used by both the Glue job and the pytest suite
# --------------------------------------------------------------------------- #
def clean_and_curate(transactions_df: DataFrame, customers_df: DataFrame, products_df: DataFrame,
                      run_id: str, min_date: str = "2024-01-01", max_date: str = "2026-12-31"):
    """
    Runs the full Stage-1 pipeline end to end.
    Returns (curated_df, rejected_df, counts_dict).
    """
    df = standardize_column_names(transactions_df)
    df = standardize_transaction_types(df)
    df = apply_missing_value_rules(df)

    ninety_ninth = df.approxQuantile("amount", [0.99], 0.01)
    ceiling = float(ninety_ninth[0]) * MAX_AMOUNT_MULTIPLIER if ninety_ninth and ninety_ninth[0] else 1_000_000.0

    df = validate_amount(df, ceiling)
    df = validate_date(df, min_date, max_date)
    df = validate_references(df, standardize_column_names(customers_df), standardize_column_names(products_df))
    df = assign_reject_reason(df)

    raw_count = df.count()

    valid_df, invalid_df = split_valid_invalid(df)
    curated_df = deterministic_dedupe(valid_df)
    curated_df = add_partition_columns(curated_df)
    curated_df = drop_internal_columns(curated_df).withColumn("run_id", F.lit(run_id))

    rejected_df = drop_internal_columns(invalid_df).withColumn("run_id", F.lit(run_id))

    counts = {
        "run_id": run_id,
        "raw_count": raw_count,
        "rejected_count": rejected_df.count(),
        "curated_count": curated_df.count(),
    }
    return curated_df, rejected_df, counts