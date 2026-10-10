from pathlib import Path
import duckdb
import json
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent
CUSTOMERS = ROOT / "data" / "processed" / "customers.parquet"
TRANSACTIONS = ROOT / "data" / "processed" / "transactions.parquet"
QUARANTINE = ROOT / "data" / "quarantine" / "transactions.parquet"
REPORT = ROOT / "data" / "processed" / "quality_report.json"

con = duckdb.connect()

try:
    customers = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{CUSTOMERS.as_posix()}')"
    ).fetchone()[0]

    transactions = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{TRANSACTIONS.as_posix()}')"
    ).fetchone()[0]

    quarantined = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{QUARANTINE.as_posix()}')"
    ).fetchone()[0]

    reasons = con.execute(f"""
        SELECT rejection_reason, COUNT(*)
        FROM read_parquet('{QUARANTINE.as_posix()}')
        GROUP BY rejection_reason
        ORDER BY rejection_reason
    """).fetchall()

    duplicate_ids = con.execute(f"""
        SELECT COUNT(*)
        FROM (
            SELECT transaction_id
            FROM read_parquet('{TRANSACTIONS.as_posix()}')
            GROUP BY transaction_id
            HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    null_transaction_ids = con.execute(f"""
        SELECT COUNT(*)
        FROM read_parquet('{TRANSACTIONS.as_posix()}')
        WHERE transaction_id IS NULL OR TRIM(transaction_id) = ''
    """).fetchone()[0]

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "customers_processed": customers,
        "valid_transactions": transactions,
        "quarantined_transactions": quarantined,
        "reconciliation": transactions + quarantined == 10003,
        "duplicate_transaction_ids": duplicate_ids,
        "missing_transaction_ids": null_transaction_ids,
        "quarantine_reasons": {
            reason: count for reason, count in reasons
        }
    }

    REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"\nQuality report saved to: {REPORT}")

finally:
    con.close()