"""
Generates the three synthetic source datasets with deliberately dirty rows,
so the ETL job's validation/dedup rules have something real to catch.

Run:
    python data/generate_synthetic_data.py --out data/raw --n-transactions 5000

Produces:
    data/raw/customers/customers.csv
    data/raw/products/products.csv
    data/raw/transactions/transactions.csv
"""
import argparse
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

REGIONS = ["north", "south", "central", "bajio", "west"]
CATEGORIES = ["electronics", "home", "apparel", "grocery", "sports", "toys"]


def gen_customers(n):
    rows = []
    for i in range(1, n + 1):
        signup = datetime(2022, 1, 1) + timedelta(days=random.randint(0, 900))
        rows.append({
            "customer_id": f"C{i:05d}",
            "customer_name": f"Customer {i}",
            "email": f"customer{i}@example.com",
            "region": random.choice(REGIONS),
            "signup_date": signup.strftime("%Y-%m-%d"),
        })
    return rows


def gen_products(n):
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "product_id": f"P{i:04d}",
            "product_name": f"Product {i}",
            "category": random.choice(CATEGORIES),
            "unit_price": round(random.uniform(5, 500), 2),
        })
    return rows


def format_amount(value):
    """Return amount in one of several messy real-world formats."""
    style = random.random()
    if style < 0.5:
        return f"{value:.2f}"
    if style < 0.8:
        return f"${value:,.2f}"
    return f"{value:,.2f}"


def format_date(dt):
    """Return the date in one of two source formats (ISO or US)."""
    return dt.strftime("%Y-%m-%d") if random.random() < 0.7 else dt.strftime("%m/%d/%Y")


def gen_transactions(n, customers, products):
    rows = []
    base_day = datetime(2025, 1, 1)
    customer_ids = [c["customer_id"] for c in customers]
    product_ids = [p["product_id"] for p in products]

    for i in range(1, n + 1):
        txn_id = f"T{i:06d}"
        cust = random.choice(customer_ids)
        prod = random.choice(product_ids)
        amount = round(random.uniform(8, 900), 2)
        tdate = base_day + timedelta(days=random.randint(0, 240))
        ingested = tdate + timedelta(hours=random.randint(1, 48))

        rows.append({
            "transaction_id": txn_id,
            "customer_id": cust,
            "product_id": prod,
            "transaction_date": format_date(tdate),
            "amount": format_amount(amount),
            "quantity": random.randint(1, 5),
            "promo_code": random.choice(["", "", "", "SAVE10", "WELCOME5"]),
            "ingestion_timestamp": ingested.strftime("%Y-%m-%d %H:%M:%S"),
            "source_file": "batch_2025_01.csv",
        })

    # --- inject deliberate data-quality problems -----------------------
    dirty = list(rows)

    # 1) exact duplicate transaction_ids with different ingestion times
    #    (tests deterministic dedupe tie-break)
    for src in random.sample(rows, k=max(1, n // 40)):
        dup = dict(src)
        dup["ingestion_timestamp"] = (
            datetime.strptime(src["ingestion_timestamp"], "%Y-%m-%d %H:%M:%S")
            + timedelta(hours=6)
        ).strftime("%Y-%m-%d %H:%M:%S")
        dup["source_file"] = "batch_2025_01_late.csv"
        dirty.append(dup)

    # 2) missing required fields
    for r in random.sample(dirty, k=max(1, n // 60)):
        r["customer_id"] = ""
    for r in random.sample(dirty, k=max(1, n // 60)):
        r["amount"] = ""

    # 3) invalid amounts (zero, negative, non-numeric)
    for r in random.sample(dirty, k=max(1, n // 80)):
        r["amount"] = random.choice(["0.00", "-45.00", "N/A"])

    # 4) invalid / out-of-range dates
    for r in random.sample(dirty, k=max(1, n // 80)):
        r["transaction_date"] = random.choice(["2031-01-01", "not-a-date", "2019-13-40"])

    # 5) orphan references (customer/product not in dimension tables)
    for r in random.sample(dirty, k=max(1, n // 70)):
        r["customer_id"] = "C99999"
    for r in random.sample(dirty, k=max(1, n // 70)):
        r["product_id"] = "P9999"

    random.shuffle(dirty)
    return dirty


def write_csv(path: Path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="data/raw")
    ap.add_argument("--n-customers", type=int, default=500)
    ap.add_argument("--n-products", type=int, default=150)
    ap.add_argument("--n-transactions", type=int, default=5000)
    args = ap.parse_args()

    out = Path(args.out)
    customers = gen_customers(args.n_customers)
    products = gen_products(args.n_products)
    transactions = gen_transactions(args.n_transactions, customers, products)

    write_csv(out / "customers" / "customers.csv", customers,
              ["customer_id", "customer_name", "email", "region", "signup_date"])
    write_csv(out / "products" / "products.csv", products,
              ["product_id", "product_name", "category", "unit_price"])
    write_csv(out / "transactions" / "transactions.csv", transactions,
              ["transaction_id", "customer_id", "product_id", "transaction_date", "amount",
               "quantity", "promo_code", "ingestion_timestamp", "source_file"])

    print(f"customers:    {len(customers)} rows -> {out}/customers/customers.csv")
    print(f"products:     {len(products)} rows -> {out}/products/products.csv")
    print(f"transactions: {len(transactions)} rows -> {out}/transactions/transactions.csv "
          f"(includes injected duplicates/nulls/invalid rows)")


if __name__ == "__main__":
    main()
