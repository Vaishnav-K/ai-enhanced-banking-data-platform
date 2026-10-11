from pathlib import Path
import csv
import json
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
REPORT = ROOT / "data" / "processed" / "schema_validation_report.json"
CONFIG_FILE = ROOT / "config" / "validation_rules.json"

EXPECTED_SCHEMAS = {
    "customers.csv": [
        "customer_id", "customer_name", "account_type", "registration_date"
    ],
    "transactions.csv": [
        "transaction_id", "customer_id", "transaction_type",
        "amount", "transaction_timestamp", "status"
    ],
}


def load_config():
    with CONFIG_FILE.open("r", encoding="utf-8-sig") as handle:
        config = json.load(handle)
    if not isinstance(config.get("schemas"), dict):
        raise ValueError("Configuration must contain a schemas object")
    return config


def add_issue(issues, config, row_number, field, reason):
    severity = config.get("issue_severities", {}).get(reason, "WARN")
    if severity not in {"WARN", "FAIL"}:
        severity = "WARN"
    issues.append({
        "row": row_number,
        "field": field,
        "reason": reason,
        "severity": severity,
    })


def validate_values(filename, expected_columns):
    path = RAW_DIR / filename
    issues = []
    rows_checked = 0

    try:
        config = load_config()
        schema = config["schemas"][filename]
        required_values = set(schema.get("required_values", []))
        allowed_values = schema.get("allowed_values", {})
        field_rules = config.get("field_rules", {})

        with path.open("r", newline="", encoding="utf-8-sig") as handle:
            reader = csv.DictReader(handle)

            for row_number, row in enumerate(reader, start=2):
                rows_checked += 1

                if None in row:
                    add_issue(
                        issues, config, row_number, "_row", "EXTRA_CSV_FIELDS"
                    )

                for column in expected_columns:
                    value = row.get(column)

                    if value is None or not value.strip():
                        if column in required_values:
                            add_issue(
                                issues, config, row_number, column,
                                "MISSING_REQUIRED_VALUE"
                            )
                        continue

                    value = value.strip()
                    rule = field_rules.get(column, {})
                    rule_type = rule.get("type")

                    if rule_type in {"date", "datetime"}:
                        try:
                            datetime.strptime(value, rule["format"])
                        except (ValueError, KeyError, TypeError):
                            reason = (
                                "INVALID_DATE_FORMAT"
                                if rule_type == "date"
                                else "INVALID_TIMESTAMP_FORMAT"
                            )
                            add_issue(issues, config, row_number, column, reason)

                    elif rule_type == "decimal":
                        try:
                            amount = Decimal(value)
                            if not amount.is_finite():
                                raise InvalidOperation
                            if rule.get("must_be_positive") and amount <= 0:
                                add_issue(
                                    issues, config, row_number, column,
                                    "NON_POSITIVE_AMOUNT"
                                )
                        except (InvalidOperation, ValueError):
                            add_issue(
                                issues, config, row_number, column,
                                "INVALID_NUMERIC_AMOUNT"
                            )

                    allowed = allowed_values.get(column)
                    if allowed and value.upper() not in {
                        str(item).upper() for item in allowed
                    }:
                        add_issue(
                            issues, config, row_number, column,
                            "UNEXPECTED_BUSINESS_VALUE"
                        )

        return summarize_issues(issues, rows_checked)

    except (
        OSError, UnicodeError, csv.Error, json.JSONDecodeError,
        ValueError, KeyError, TypeError
    ) as exc:
        return {
            "status": "FAIL",
            "rows_checked": rows_checked,
            "issue_count": len(issues),
            "issue_counts_by_reason": {},
            "issue_counts_by_severity": {},
            "sample_issues": issues[:20],
            "error": str(exc),
        }


def summarize_issues(issues, rows_checked):
    reason_counts = {}
    severity_counts = {}

    for issue in issues:
        reason = issue["reason"]
        severity = issue["severity"]
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        severity_counts[severity] = severity_counts.get(severity, 0) + 1

    status = (
        "FAIL" if severity_counts.get("FAIL", 0)
        else "WARN" if issues
        else "PASS"
    )

    return {
        "status": status,
        "rows_checked": rows_checked,
        "issue_count": len(issues),
        "issue_counts_by_reason": reason_counts,
        "issue_counts_by_severity": severity_counts,
        "sample_issues": issues[:20],
        "sample_limit": 20,
        "error": None,
    }


def validate_referential_integrity():
    config = load_config()
    issues = []
    rows_checked = 0

    customers_path = RAW_DIR / "customers.csv"
    transactions_path = RAW_DIR / "transactions.csv"

    with customers_path.open("r", newline="", encoding="utf-8-sig") as handle:
        customer_reader = csv.DictReader(handle)
        customer_ids = {
            row["customer_id"].strip()
            for row in customer_reader
            if row.get("customer_id") and row["customer_id"].strip()
        }

    with transactions_path.open(
        "r", newline="", encoding="utf-8-sig"
    ) as handle:
        transaction_reader = csv.DictReader(handle)

        for row_number, row in enumerate(transaction_reader, start=2):
            rows_checked += 1
            customer_id = row.get("customer_id")

            # Missing IDs are already reported by field-level validation.
            if customer_id is None or not customer_id.strip():
                continue

            if customer_id.strip() not in customer_ids:
                add_issue(
                    issues, config, row_number,
                    "customer_id", "UNKNOWN_CUSTOMER_ID"
                )

    return summarize_issues(issues, rows_checked)


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
            header = next(csv.reader(handle), None)

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
    try:
        config = load_config()
        results = {}

        for filename, schema in config["schemas"].items():
            results[filename] = validate_file(
                filename, schema.get("required_columns", [])
            )

        results["referential_integrity"] = validate_referential_integrity()

    except (
        OSError, UnicodeError, csv.Error, json.JSONDecodeError,
        ValueError, KeyError, TypeError
    ) as exc:
        print(f"VALIDATION ERROR: {exc}")
        return 1

    failed = any(
        result["status"] == "FAIL" for result in results.values()
    )

    report = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "status": "FAIL" if failed else "PASS",
        "configuration_version": config.get("version"),
        "unexpected_columns_policy": config.get(
            "unexpected_columns_policy", "WARN"
        ),
        "row_value_policy": config.get("row_value_policy", "WARN"),
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
