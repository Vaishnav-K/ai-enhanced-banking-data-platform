from pathlib import Path
import duckdb
import json

ROOT = Path(__file__).resolve().parent.parent
TRANSACTIONS = ROOT / "data" / "processed" / "transactions.parquet"
OUTPUT = ROOT / "data" / "processed" / "transaction_anomalies.json"

con = duckdb.connect()

try:
    rows = con.execute(f"""
        SELECT
            transaction_id,
            customer_id,
            transaction_type,
            amount,
            transaction_timestamp,
            status
        FROM read_parquet('{TRANSACTIONS.as_posix()}')
        WHERE amount >= 100000
           OR amount <= 0
        ORDER BY amount DESC
    """).fetchall()

    anomalies = [
        {
            "transaction_id": row[0],
            "customer_id": row[1],
            "transaction_type": row[2],
            "amount": float(row[3]) if row[3] is not None else None,
            "timestamp": str(row[4]),
            "status": row[5],
            "anomaly_reason": (
                "HIGH_VALUE_TRANSACTION"
                if row[3] is not None and row[3] >= 100000
                else "NON_POSITIVE_AMOUNT"
            )
        }
        for row in rows
    ]

    OUTPUT.write_text(
        json.dumps(anomalies, indent=2),
        encoding="utf-8"
    )

    print("Transactions analyzed: 10000")
    print(f"Potential anomalies found: {len(anomalies)}")
    print(f"Output saved to: {OUTPUT}")

finally:
    con.close()