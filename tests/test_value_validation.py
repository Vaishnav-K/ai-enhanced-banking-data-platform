import csv
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import schema_validation


class TestFieldValueValidation(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.raw_dir = Path(self.temp_dir.name)

    def write_csv(self, filename, headers, rows):
        path = self.raw_dir / filename
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.writer(handle)
            writer.writerow(headers)
            writer.writerows(rows)
        return path

    def validate_transactions(self, rows):
        headers = schema_validation.EXPECTED_SCHEMAS["transactions.csv"]
        self.write_csv("transactions.csv", headers, rows)

        with patch.object(schema_validation, "RAW_DIR", self.raw_dir):
            return schema_validation.validate_values(
                "transactions.csv", headers
            )

    def test_valid_transaction_passes(self):
        result = self.validate_transactions([
            ["T001", "C001", "CREDIT", "125.50",
             "2026-10-11 12:30:00", "SUCCESS"]
        ])

        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["issue_count"], 0)

    def test_negative_amount_is_reported(self):
        result = self.validate_transactions([
            ["T001", "C001", "DEBIT", "-50",
             "2026-10-11 12:30:00", "SUCCESS"]
        ])

        self.assertEqual(result["status"], "WARN")
        self.assertIn(
            "NON_POSITIVE_AMOUNT",
            result["issue_counts_by_reason"],
        )

    def test_invalid_amount_is_reported(self):
        result = self.validate_transactions([
            ["T001", "C001", "DEBIT", "not-a-number",
             "2026-10-11 12:30:00", "SUCCESS"]
        ])

        self.assertIn(
            "INVALID_NUMERIC_AMOUNT",
            result["issue_counts_by_reason"],
        )

    def test_invalid_timestamp_is_reported(self):
        result = self.validate_transactions([
            ["T001", "C001", "CREDIT", "100",
             "not-a-timestamp", "SUCCESS"]
        ])

        self.assertIn(
            "INVALID_TIMESTAMP_FORMAT",
            result["issue_counts_by_reason"],
        )

    def test_unexpected_status_is_reported(self):
        result = self.validate_transactions([
            ["T001", "C001", "CREDIT", "100",
             "2026-10-11 12:30:00", "UNKNOWN"]
        ])

        self.assertIn(
            "UNEXPECTED_BUSINESS_VALUE",
            result["issue_counts_by_reason"],
        )

    def test_missing_customer_id_is_reported(self):
        result = self.validate_transactions([
            ["T001", "", "CREDIT", "100",
             "2026-10-11 12:30:00", "SUCCESS"]
        ])

        self.assertIn(
            "MISSING_REQUIRED_VALUE",
            result["issue_counts_by_reason"],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
