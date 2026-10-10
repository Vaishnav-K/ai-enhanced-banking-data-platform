import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

random.seed(42)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

CUSTOMER_COUNT = 500
TRANSACTION_COUNT = 10_000

ACCOUNT_TYPES = ["SAVINGS", "CURRENT", "SALARY"]
TRANSACTION_TYPES = ["CREDIT", "DEBIT", "TRANSFER", "PAYMENT"]
STATUSES = ["SUCCESS", "FAILED", "PENDING"]

# Generate customers
customers = []

for i in range(1, CUSTOMER_COUNT + 1):
    customers.append({
        "customer_id": f"C{i:05d}",
        "customer_name": f"Customer_{i:05d}",
        "account_type": random.choice(ACCOUNT_TYPES),
        "registration_date": (
            datetime(2022, 1, 1) + timedelta(days=random.randint(0, 1700))
        ).strftime("%Y-%m-%d"),
    })

with (RAW_DIR / "customers.csv").open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=customers[0].keys())
    writer.writeheader()
    writer.writerows(customers)

# Generate transactions
transactions = []
start_time = datetime(2026, 1, 1)

for i in range(1, TRANSACTION_COUNT + 1):
    transactions.append({
        "transaction_id": f"T{i:07d}",
        "customer_id": random.choice(customers)["customer_id"],
        "transaction_type": random.choice(TRANSACTION_TYPES),
        "amount": round(random.uniform(10, 100000), 2),
        "transaction_timestamp": (
            start_time + timedelta(seconds=random.randint(0, 86_400 * 270))
        ).strftime("%Y-%m-%d %H:%M:%S"),
        "status": random.choice(STATUSES),
    })

# Introduce controlled data-quality issues for pipeline testing.
# 1. Duplicate a transaction.
transactions.append(transactions[99].copy())

# 2. Add a transaction with a missing customer ID.
transactions.append({
    "transaction_id": "T_BAD0001",
    "customer_id": "",
    "transaction_type": "DEBIT",
    "amount": 250.00,
    "transaction_timestamp": "2026-09-01 10:00:00",
    "status": "SUCCESS",
})

# 3. Add a transaction with an invalid negative amount.
transactions.append({
    "transaction_id": "T_BAD0002",
    "customer_id": "C00001",
    "transaction_type": "DEBIT",
    "amount": -500.00,
    "transaction_timestamp": "2026-09-01 11:00:00",
    "status": "SUCCESS",
})

# 4. Add a transaction with an unknown customer.
transactions.append({
    "transaction_id": "T_BAD0003",
    "customer_id": "C99999",
    "transaction_type": "TRANSFER",
    "amount": 1000.00,
    "transaction_timestamp": "2026-09-01 12:00:00",
    "status": "SUCCESS",
})

with (RAW_DIR / "transactions.csv").open("w", newline="", encoding="utf-8") as f:
    writer = csv.DictWriter(f, fieldnames=transactions[0].keys())
    writer.writeheader()
    writer.writerows(transactions)

print("Synthetic banking data generated successfully.")
print(f"Customers: {len(customers):,}")
print(f"Transactions: {len(transactions):,}")
print(f"Output directory: {RAW_DIR}")