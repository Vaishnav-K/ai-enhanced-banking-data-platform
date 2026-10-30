"""Row-level reconciliation between Spark CSVs and DuckDB Parquet outputs."""
import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"
QUARANTINE = ROOT / "data" / "quarantine"
SPARK = ROOT / "data" / "spark_processed"
TRANSACTION_COLUMNS = (
    "transaction_id", "customer_id", "transaction_type",
    "amount", "transaction_timestamp", "status",
)


def read_csv(path):
    with Path(path).open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def normalize_amount(value):
    if value is None or str(value).strip() == "":
        return None
    return Decimal(str(value)).quantize(Decimal("0.01"))


def normalize_timestamp(value):
    if value is None or str(value).strip() == "":
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    normalized = str(value).strip().replace("T", " ")
    # Spark CSV timestamp strings and DuckDB timestamp strings can differ
    # only by a fractional suffix; normalize both to second precision.
    if "." in normalized:
        normalized = normalized.split(".", 1)[0]
    return normalized


def normalize_transaction(row, include_reason=False):
    normalized = (
        (row.get("transaction_id") or "").strip() or None,
        (row.get("customer_id") or "").strip() or None,
        (row.get("transaction_type") or "").strip().upper() or None,
        normalize_amount(row.get("amount")),
        normalize_timestamp(row.get("transaction_timestamp")),
        (row.get("status") or "").strip().upper() or None,
    )
    if include_reason:
        normalized += ((row.get("quarantine_reason") or row.get("rejection_reason") or "").strip(),)
    return normalized


def read_parquet_records(path, include_reason=False):
    columns = ", ".join(TRANSACTION_COLUMNS)
    if include_reason:
        columns += ", rejection_reason"
    with duckdb.connect() as con:
        rows = con.execute(
            f"SELECT {columns} FROM read_parquet(?)", [str(path)]
        ).fetchall()
    records = []
    for row in rows:
        values = list(row)
        if len(values) >= 6:
            values[3] = normalize_amount(values[3])
            values[4] = normalize_timestamp(values[4])
        records.append(tuple(values))
    return records


def compare_counters(left, right):
    missing = left - right
    extra = right - left
    return {
        "passed": not missing and not extra,
        "left_only_count": sum(missing.values()),
        "right_only_count": sum(extra.values()),
        "left_only_sample": [repr(item) for item in list(missing)[:3]],
        "right_only_sample": [repr(item) for item in list(extra)[:3]],
    }


def reconcile(valid_spark_path, quarantine_spark_path, analytics_spark_path,
              valid_parquet_path, quarantine_parquet_path, report_path):
    spark_valid = [
        normalize_transaction(row)
        for row in read_csv(valid_spark_path)
    ]
    spark_quarantine = [
        normalize_transaction(row, include_reason=True)
        for row in read_csv(quarantine_spark_path)
    ]
    duck_valid = read_parquet_records(valid_parquet_path)
    duck_quarantine = read_parquet_records(
        quarantine_parquet_path, include_reason=True
    )

    valid_rows_check = compare_counters(Counter(spark_valid), Counter(duck_valid))
    quarantine_rows_check = compare_counters(
        Counter(spark_quarantine), Counter(duck_quarantine)
    )

    spark_quarantine_reasons = Counter(row[-1] for row in spark_quarantine)
    duck_quarantine_reasons = Counter(row[-1] for row in duck_quarantine)

    spark_analytics = {}
    for row in read_csv(analytics_spark_path):
        key = (
            (row.get("transaction_type") or "").strip().upper(),
            (row.get("status") or "").strip().upper(),
        )
        spark_analytics[key] = (
            int(row["transaction_count"]),
            normalize_amount(row["total_amount"]),
            normalize_amount(row["average_amount"]),
        )

    with duckdb.connect() as con:
        duck_rows = con.execute(
            """
            SELECT transaction_type, status, COUNT(*) AS transaction_count,
                   ROUND(SUM(amount), 2) AS total_amount,
                   ROUND(AVG(amount), 2) AS average_amount
            FROM read_parquet(?)
            GROUP BY transaction_type, status
            """,
            [str(valid_parquet_path)],
        ).fetchall()
    duck_analytics = {
        (str(row[0]).upper(), str(row[1]).upper()): (
            int(row[2]), normalize_amount(row[3]), normalize_amount(row[4])
        )
        for row in duck_rows
    }

    analytics_check = {
        "passed": spark_analytics == duck_analytics,
        "spark_groups": len(spark_analytics),
        "duckdb_groups": len(duck_analytics),
        "spark_only_groups": [repr(k) for k in spark_analytics.keys() - duck_analytics.keys()],
        "duckdb_only_groups": [repr(k) for k in duck_analytics.keys() - spark_analytics.keys()],
    }
    reasons_check = {
        "passed": spark_quarantine_reasons == duck_quarantine_reasons,
        "spark": dict(sorted(spark_quarantine_reasons.items())),
        "duckdb": dict(sorted(duck_quarantine_reasons.items())),
    }

    checks = {
        "valid_transaction_rows": valid_rows_check["passed"],
        "quarantined_transaction_rows": quarantine_rows_check["passed"],
        "quarantine_reason_distribution": reasons_check["passed"],
        "analytics_groups_and_totals": analytics_check["passed"],
    }
    report = {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "checks": checks,
        "details": {
            "valid_transaction_rows": valid_rows_check,
            "quarantined_transaction_rows": quarantine_rows_check,
            "quarantine_reason_distribution": reasons_check,
            "analytics_groups_and_totals": analytics_check,
        },
        "counts": {
            "spark_valid_transactions": len(spark_valid),
            "duckdb_valid_transactions": len(duck_valid),
            "spark_quarantined_transactions": len(spark_quarantine),
            "duckdb_quarantined_transactions": len(duck_quarantine),
        },
        "comparison_scope": (
            "Full normalized transaction rows, quarantine reasons, and "
            "transaction analytics; duplicate row multiplicity is preserved."
        ),
    }

    report_path = Path(report_path)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")

    print("\n=== SPARK vs DUCKDB ROW-LEVEL RECONCILIATION ===")
    for name, passed in checks.items():
        print(f"{name:<38} {'PASS' if passed else 'FAIL'}")
    print(f"Report saved to: {report_path}")
    if report["status"] != "PASS":
        print(json.dumps(report["details"], indent=2, default=str))
        return report
    print("CROSS-ENGINE RECONCILIATION PASSED")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spark-output-dir", type=Path, default=SPARK)
    parser.add_argument("--processed-dir", type=Path, default=PROCESSED)
    parser.add_argument("--quarantine-dir", type=Path, default=QUARANTINE)
    parser.add_argument(
        "--report", type=Path,
        default=PROCESSED / "engine_reconciliation_report.json",
    )
    args = parser.parse_args()
    spark_dir = args.spark_output_dir
    report = reconcile(
        spark_dir / "valid_transactions" / "data.csv",
        spark_dir / "quarantined_transactions" / "data.csv",
        spark_dir / "transaction_analytics" / "data.csv",
        args.processed_dir / "transactions.parquet",
        args.quarantine_dir / "transactions.parquet",
        args.report,
    )
    if report["status"] != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
