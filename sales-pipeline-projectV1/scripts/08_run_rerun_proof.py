"""
Runs the Stage-1 pipeline twice against the SAME raw input and proves the
curated output does not change or duplicate — the evidence requirement #8
asks for. Works entirely locally (no AWS needed); run it after
data/generate_synthetic_data.py has produced data/raw/.

    python scripts/08_run_rerun_proof.py

Prints the row counts and a checksum from each run, and fails loudly if
they don't match.
"""
import hashlib
import sys

from pyspark.sql import SparkSession

sys.path.append(".")
from etl.io_utils import read_csv
from etl.transform import clean_and_curate


def checksum(df, cols):
    rows = df.select(*cols).orderBy(*cols).collect()
    payload = "\n".join(str(r.asDict()) for r in rows)
    return hashlib.md5(payload.encode()).hexdigest()


def main():
    spark = SparkSession.builder.appName("rerun-proof").master("local[2]").getOrCreate()

    customers = read_csv(spark, "data/raw/customers")
    products = read_csv(spark, "data/raw/products")
    transactions = read_csv(spark, "data/raw/transactions")

    cols = ["transaction_id", "customer_id", "product_id", "amount", "transaction_date"]

    curated1, rejected1, counts1 = clean_and_curate(transactions, customers, products, run_id="proof-run-1")
    curated2, rejected2, counts2 = clean_and_curate(transactions, customers, products, run_id="proof-run-2")

    sum1, sum2 = checksum(curated1, cols), checksum(curated2, cols)

    print("=== Run 1 ===")
    print(counts1, "checksum:", sum1)
    print("=== Run 2 (same input) ===")
    print(counts2, "checksum:", sum2)

    assert counts1["curated_count"] == counts2["curated_count"], "curated row count changed between runs!"
    assert counts1["rejected_count"] == counts2["rejected_count"], "rejected row count changed between runs!"
    assert sum1 == sum2, "curated output content changed between runs!"

    print("\nPASS — identical input produced identical curated output on both runs.")
    spark.stop()


if __name__ == "__main__":
    main()
