from pathlib import Path
from datetime import datetime, timezone
import json
import sys
import duckdb

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw" / "transactions.csv"
CUSTOMERS = ROOT / "data" / "processed" / "customers.parquet"
TRANSACTIONS = ROOT / "data" / "processed" / "transactions.parquet"
QUARANTINE = ROOT / "data" / "quarantine" / "transactions.parquet"
SOURCE_REPORT = ROOT / "data" / "processed" / "schema_validation_report.json"
REPORT = ROOT / "data" / "processed" / "quality_report.json"


def load_source_report():
    if not SOURCE_REPORT.is_file():
        return {
            "status": "UNAVAILABLE",
            "error": "Source validation report not found",
        }

    try:
        with SOURCE_REPORT.open("r", encoding="utf-8-sig") as handle:
            source_report = json.load(handle)

        return {
            "status": source_report.get("status", "UNKNOWN"),
            "configuration_version": source_report.get(
                "configuration_version"
            ),
            "files": source_report.get("files", {}),
        }
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        return {
            "status": "UNAVAILABLE",
            "error": str(exc),
        }


def main():
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    try:
        raw_count = con.execute(
            f"SELECT COUNT(*) FROM read_csv_auto('{RAW.as_posix()}')"
        ).fetchone()[0]

        deduplicated = con.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT ROW_NUMBER() OVER (
                    PARTITION BY transaction_id ORDER BY transaction_id
                ) AS rn
                FROM read_csv(
                    '{RAW.as_posix()}',
                    header=true,
                    all_varchar=true
                )
            ) WHERE rn = 1
        """).fetchone()[0]

        customers = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{CUSTOMERS.as_posix()}')"
        ).fetchone()[0]
        valid = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{TRANSACTIONS.as_posix()}')"
        ).fetchone()[0]
        quarantined = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{QUARANTINE.as_posix()}')"
        ).fetchone()[0]

        duplicate_groups = con.execute(f"""
            SELECT COUNT(*) FROM (
                SELECT transaction_id
                FROM read_parquet('{TRANSACTIONS.as_posix()}')
                GROUP BY transaction_id HAVING COUNT(*) > 1
            )
        """).fetchone()[0]

        missing_ids = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{TRANSACTIONS.as_posix()}')
            WHERE transaction_id IS NULL OR TRIM(transaction_id) = ''
        """).fetchone()[0]

        invalid_amounts = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{TRANSACTIONS.as_posix()}')
            WHERE amount IS NULL OR amount <= 0
        """).fetchone()[0]

        unknown_customers = con.execute(f"""
            SELECT COUNT(*) FROM read_parquet('{TRANSACTIONS.as_posix()}') t
            WHERE NOT EXISTS (
                SELECT 1 FROM read_parquet('{CUSTOMERS.as_posix()}') c
                WHERE c.customer_id = t.customer_id
            )
        """).fetchone()[0]

        reasons = con.execute(f"""
            SELECT rejection_reason, COUNT(*)
            FROM read_parquet('{QUARANTINE.as_posix()}')
            GROUP BY rejection_reason ORDER BY rejection_reason
        """).fetchall()

        checks = {
            "reconciliation": {
                "passed": valid + quarantined == deduplicated,
                "details": {
                    "deduplicated": deduplicated,
                    "valid": valid,
                    "quarantined": quarantined,
                },
            },
            "no_duplicate_valid_transaction_ids": {
                "passed": duplicate_groups == 0,
                "details": {"duplicate_groups": duplicate_groups},
            },
            "valid_transactions_have_ids": {
                "passed": missing_ids == 0,
                "details": {"missing_ids": missing_ids},
            },
            "valid_transactions_have_positive_amounts": {
                "passed": invalid_amounts == 0,
                "details": {"invalid_amounts": invalid_amounts},
            },
            "valid_transactions_reference_known_customers": {
                "passed": unknown_customers == 0,
                "details": {"unknown_customers": unknown_customers},
            },
        }

        source_report = load_source_report()

        # Source row-level warnings are informational. A source report with
        # blocking schema/configuration errors is not considered healthy.
        source_status = source_report.get("status", "UNAVAILABLE")
        source_blocked = source_status in {"FAIL", "UNAVAILABLE", "UNKNOWN"}

        report = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": (
                "PASS"
                if all(c["passed"] for c in checks.values())
                and not source_blocked
                else "FAIL"
            ),
            "raw_transaction_rows": raw_count,
            "deduplicated_transaction_rows": deduplicated,
            "customers_processed": customers,
            "valid_transactions": valid,
            "quarantined_transactions": quarantined,
            "checks": checks,
            "quarantine_reasons": dict(reasons),
            "source_validation": {
                "status": source_status,
                "configuration_version": source_report.get(
                    "configuration_version"
                ),
                "files": source_report.get("files", {}),
                "error": source_report.get("error"),
            },
        }
    finally:
        con.close()

    REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"\nQuality report saved to: {REPORT}")
    print(f"QUALITY GATES: {report['status']}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
