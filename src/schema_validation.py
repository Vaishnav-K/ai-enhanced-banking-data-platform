from pathlib import Path
import csv
import json
import sys
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation

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

ALLOWED_VALUES = {
    "customers.csv": {
        "account_type": {"SAVINGS", "CURRENT", "SALARY"},
    },
    "transactions.csv": {
        "transaction_type": {"CREDIT", "DEBIT", "TRANSFER", "PAYMENT"},
        "status": {"SUCCESS", "FAILED", "PENDING"},
    },
}

REQUIRED_VALUES = {
    "customers.csv": [
        "customer_id", "customer_name", "account_type", "registration_date"
    ],
    "transactions.csv": [
        "transaction_id", "customer_id", "transaction_type",
        "amount", "transaction_timestamp", "status"
    ],
}


def validate_values(filename, expected_columns):
    path = RAW_DIR / filename
    issues = []
    rows_checked = 0

    try:
        with path.open("r", newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)

            for row_number, row in enumerate(reader, start=2):
                rows_checked += 1

                # Malformed rows with extra CSV fields have a None key.
                if None in row:
                    issues.append({
                        "row": row_number,
                        "field": "_row",
                        "reason": "EXTRA_CSV_FIELDS",
                    })

                for column in expected_columns:
                    value = row.get(column)

                    if value is None or not value.strip():
                        if column in REQUIRED_VALUES[filename]:
                            issues.append({
                                "row": row_number,
                                "field": column,
                                "reason": "MISSING_REQUIRED_VALUE",
                            })
                        continue

                    value = value.strip()

                    if column == "registration_date":
                        try:
                            date.fromisoformat(value)
                        except ValueError:
                            issues.append({
                                "row": row_number,
                                "field": column,
                                "reason": "INVALID_DATE_FORMAT",
                            })

                    elif column == "transaction_timestamp":
                        try:
                            datetime.fromisoformat(value)
                        except ValueError:
                            issues.append({
                                "row": row_number,
                                "field": column,
                                "reason": "INVALID_TIMESTAMP_FORMAT",
                            })

                    elif column == "amount":
                        try:
                            amount = Decimal(value)
                            if not amount.is_finite():
                                raise InvalidOperation
                            if amount <= 0:
                                issues.append({
                                    "row": row_number,
                                    "field": column,
                                    "reason": "NON_POSITIVE_AMOUNT",
                                })
                        except (InvalidOperation, ValueError):
                            issues.append({
                                "row": row_number,
                                "field": column,
                                "reason": "INVALID_NUMERIC_AMOUNT",
                            })

                    allowed = ALLOWED_VALUES.get(filename, {}).get(column)
                    if allowed and value.upper() not in allowed:
                        issues.append({
                            "row": row_number,
                            "field": column,
                            "reason": "UNEXPECTED_BUSINESS_VALUE",
                        })

        reason_counts = {}
        for issue in issues:
            reason = issue["reason"]
            reason_counts[reason] = reason_counts.get(reason, 0) + 1

        return {
            "status": "WARN" if issues else "PASS",
            "rows_checked": rows_checked,
            "issue_count": len(issues),
            "issue_counts_by_reason": reason_counts,
            "sample_issues": issues[:20],
            "sample_limit": 20,
            "error": None,
        }

    except (OSError, UnicodeError, csv.Error) as exc:
        return {
            "status": "FAIL",
            "rows_checked": rows_checked,
            "issue_count": len(issues),
            "issue_counts_by_reason": {},
            "sample_issues": issues[:20],
            "error": str(exc),
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

        result = {
            "status": "PASS" if passed else "FAIL",
            "expected_columns": expected_columns,
            "actual_columns": header,
            "missing_columns": missing,
            "unexpected_columns": unexpected,
            "duplicate_columns": duplicates,
            "error": None,
        }

        if passed:
            result["value_validation"] = validate_values(
                filename, expected_columns
            )
            if result["value_validation"]["status"] == "FAIL":
                result["status"] = "FAIL"

        return result

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
        "row_value_policy": (
            "WARN only; row-level issues do not block the ETL pipeline"
        ),
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
