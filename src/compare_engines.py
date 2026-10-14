import csv
import json
import sys
from collections import Counter
from decimal import Decimal
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"
QUARANTINE = ROOT / "data" / "quarantine"
SPARK = ROOT / "data" / "spark_processed"


def read_csv(path):
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def amount(value):
    return Decimal(str(value or "0")).quantize(Decimal("0.01"))


def main():
    valid_spark = read_csv(
        SPARK / "valid_transactions" / "data.csv"
    )
    quarantine_spark = read_csv(
        SPARK / "quarantined_transactions" / "data.csv"
    )
    analytics_spark = read_csv(
        SPARK / "transaction_analytics" / "data.csv"
    )

    valid_parquet = PROCESSED / "transactions.parquet"
    quarantine_parquet = QUARANTINE / "transactions.parquet"

    for path in (valid_parquet, quarantine_parquet):
        if not path.is_file():
            raise FileNotFoundError(
                f"Missing DuckDB output: {path}. Run src/etl_pipeline.py first."
            )

    con = duckdb.connect()
    try:
        valid_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)", [str(valid_parquet)]
        ).fetchone()[0]

        quarantine_count = con.execute(
            "SELECT COUNT(*) FROM read_parquet(?)",
            [str(quarantine_parquet)],
        ).fetchone()[0]

        duckdb_reasons = dict(con.execute(
            """
            SELECT rejection_reason, COUNT(*)
            FROM read_parquet(?)
            GROUP BY rejection_reason
            """,
            [str(quarantine_parquet)],
        ).fetchall())

        duckdb_analytics = con.execute(
            """
            SELECT
                transaction_type,
                status,
                COUNT(*) AS transaction_count,
                ROUND(SUM(amount), 2) AS total_amount,
                ROUND(AVG(amount), 2) AS average_amount
            FROM read_parquet(?)
            GROUP BY transaction_type, status
            ORDER BY transaction_type, status
            """,
            [str(valid_parquet)],
        ).fetchall()

        duckdb_ids = {
            row[0] for row in con.execute(
                "SELECT transaction_id FROM read_parquet(?)",
                [str(valid_parquet)],
            ).fetchall()
        }
    finally:
        con.close()

    spark_ids = {row["transaction_id"] for row in valid_spark}
    spark_reasons = dict(Counter(
        row["quarantine_reason"] for row in quarantine_spark
    ))

    spark_analytics = {
        (row["transaction_type"], row["status"]): (
            int(row["transaction_count"]),
            amount(row["total_amount"]),
            amount(row["average_amount"]),
        )
        for row in analytics_spark
    }

    duckdb_analytics_map = {
        (row[0], row[1]): (
            int(row[2]),
            amount(row[3]),
            amount(row[4]),
        )
        for row in duckdb_analytics
    }

    checks = {
        "valid transaction count": len(valid_spark) == valid_count,
        "quarantine count": len(quarantine_spark) == quarantine_count,
        "quarantine reason counts": spark_reasons == duckdb_reasons,
        "valid transaction IDs": spark_ids == duckdb_ids,
        "analytics groups and totals": spark_analytics == duckdb_analytics_map,
    }

    print("\n=== SPARK vs DUCKDB RECONCILIATION ===")
    for name, passed in checks.items():
        print(f"{name:<32} {'PASS' if passed else 'FAIL'}")

    report = {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "duckdb_valid_transactions": valid_count,
        "spark_valid_transactions": len(valid_spark),
        "duckdb_quarantined_transactions": quarantine_count,
        "spark_quarantined_transactions": len(quarantine_spark),
    }

    output = PROCESSED / "engine_reconciliation_report.json"
    output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nReport saved to: {output}")

    if not all(checks.values()):
        sys.exit(1)

    print("CROSS-ENGINE RECONCILIATION PASSED")


if __name__ == "__main__":
    main()

