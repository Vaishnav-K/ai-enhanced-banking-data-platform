"""Generate a large synthetic banking dataset without holding all transactions in memory.

Example:
    python src/generate_scale_data.py --customers 100000 --transactions 1000000
"""
import argparse
import csv
import random
import time
from datetime import datetime, timedelta
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ACCOUNT_TYPES = ("SAVINGS", "CURRENT", "SALARY")
TRANSACTION_TYPES = ("CREDIT", "DEBIT", "TRANSFER", "PAYMENT")
STATUSES = ("SUCCESS", "FAILED", "PENDING")
CUSTOMER_FIELDS = ("customer_id", "customer_name", "account_type", "registration_date")
TRANSACTION_FIELDS = (
    "transaction_id",
    "customer_id",
    "transaction_type",
    "amount",
    "transaction_timestamp",
    "status",
)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--customers", type=int, default=100_000)
    parser.add_argument("--transactions", type=int, default=10_000_000)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "scale" / "raw",
        help="Output directory (separate from the baseline data/raw directory).",
    )
    parser.add_argument("--seed", type=int, default=42)\n    parser.add_argument("--overwrite", action="store_true", help="Allow replacing existing generated CSV files.")
    return parser.parse_args()


def main():
    args = parse_args()
    if args.customers < 1:
        raise SystemExit("--customers must be at least 1")
    if args.transactions < 1:
        raise SystemExit("--transactions must be at least 1")

    rng = random.Random(args.seed)
    output_dir = args.output_dir
    if not output_dir.is_absolute():
        output_dir = (Path.cwd() / output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    customers_path = output_dir / "customers.csv"
    transactions_path = output_dir / "transactions.csv"
    start = time.perf_counter()

    # Keep only customer IDs in memory; transaction rows are streamed directly to disk.
    customer_ids = [f"C{i:07d}" for i in range(1, args.customers + 1)]
    registration_start = datetime(2022, 1, 1)
    with customers_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CUSTOMER_FIELDS)
        writer.writeheader()
        for i, customer_id in enumerate(customer_ids, start=1):
            writer.writerow({
                "customer_id": customer_id,
                "customer_name": f"Customer_{i:07d}",
                "account_type": rng.choice(ACCOUNT_TYPES),
                "registration_date": (
                    registration_start + timedelta(days=rng.randint(0, 1700))
                ).strftime("%Y-%m-%d"),
            })

    transaction_start = datetime(2026, 1, 1)
    max_seconds = 86_400 * 270
    with transactions_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=TRANSACTION_FIELDS)
        writer.writeheader()
        for i in range(1, args.transactions + 1):
            writer.writerow({
                "transaction_id": f"T{i:010d}",
                "customer_id": rng.choice(customer_ids),
                "transaction_type": rng.choice(TRANSACTION_TYPES),
                "amount": f"{rng.uniform(10, 100000):.2f}",
                "transaction_timestamp": (
                    transaction_start + timedelta(seconds=rng.randint(0, max_seconds))
                ).strftime("%Y-%m-%d %H:%M:%S"),
                "status": rng.choice(STATUSES),
            })
            if i % 100_000 == 0:
                print(f"Generated {i:,}/{args.transactions:,} transactions...")

    elapsed = time.perf_counter() - start
    print("\nScale dataset generated.")
    print(f"Customers: {args.customers:,}")
    print(f"Transactions: {args.transactions:,}")
    print(f"Customers file: {customers_path} ({customers_path.stat().st_size:,} bytes)")
    print(f"Transactions file: {transactions_path} ({transactions_path.stat().st_size:,} bytes)")
    print(f"Seed: {args.seed}")
    print(f"Generation time: {elapsed:.2f} seconds")
    print("Note: this 10M scale dataset is clean and separate from data/raw. The baseline generator remains small and includes controlled test defects.")


if __name__ == "__main__":
    main()
