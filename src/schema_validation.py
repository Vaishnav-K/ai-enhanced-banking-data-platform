from pathlib import Path
import csv
import json
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
REPORT = ROOT / "data" / "processed" / "schema_validation_report.json"

EXPECTED_SCHEMAS = {
    "customers.csv": [
        "customer_id",
        "customer_name",
        "account_type",
        "registration_date",
    ],
    "transactions.csv": [
        "transaction_id",
        "customer_id",
        "transaction_type",
        "amount",
        "transaction_timestamp",
        "status",
    ],
}


def validate_file(filename, expected_columns):
    path = RAW_DIR / filename

    if not path.is_file():
        return {
            "status": "FAIL",
            "missing_columns": expected_columns,
            "unexpected_columns": [],
            "duplicate_columns": [],
            "error": "File does not exist",
        }

    try:
        with path.open("r", newline="", encoding="utf-8-sig") as handle:
            reader = csv.reader(handle)
            header = next(reader, None)

        if not header:
            return {
                "status": "FAIL",
                "missing_columns": expected_columns,
                "unexpected_columns": [],
                "duplicate_columns": [],
                "error": "File is empty or has no header",
            }

        duplicates = sorted({
            column for column in header if header.count(column) > 1
        })
        missing = sorted(set(expected_columns) - set(header))
        unexpected = sorted(set(header) - set(expected_columns))

        passed = not missing and not duplicates

        return {
            "status": "PASS" if passed else "FAIL",
            "expected_columns": expected_columns,
            "actual_columns": header,
            "missing_columns": missing,
            "unexpected_columns": unexpected,
            "duplicate_columns": duplicates,
            "error": None,
        }

    except (OSError, UnicodeError, csv.Error) as exc:
        return {
            "status": "FAIL",
            "missing_columns": expected_columns,
            "unexpected_columns": [],
            "duplicate_columns": [],
            "error": str(exc),
        }


def main():
    results = {
        filename: validate_file(filename, columns)
        for filename, columns in EXPECTED_SCHEMAS.items()
    }

    failed = any(result["status"] == "FAIL" for result in results.values())

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "FAIL" if failed else "PASS",
        "unexpected_columns_policy": "WARN",
        "files": results,
    }

    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(json.dumps(report, indent=2))
    print(f"\nSchema report saved to: {REPORT}")
    print(f"SCHEMA VALIDATION: {report['status']}")

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
