import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import schema_validation


class TestReferentialIntegrity(unittest.TestCase):

    def test_unknown_customer_is_reported(self):
        with tempfile.TemporaryDirectory() as temp:
            raw_dir = Path(temp)
            (raw_dir / "customers.csv").write_text(
                "customer_id,customer_name,account_type,registration_date\n"
                "C001,Alice,SAVINGS,2026-01-01\n",
                encoding="utf-8",
            )
            (raw_dir / "transactions.csv").write_text(
                "transaction_id,customer_id,transaction_type,amount,"
                "transaction_timestamp,status\n"
                "T001,C999,DEBIT,10,2026-10-11 12:00:00,SUCCESS\n",
                encoding="utf-8",
            )
            config_path = raw_dir / "rules.json"
            config_path.write_text(json.dumps({
                "schemas": {},
                "issue_severities": {"UNKNOWN_CUSTOMER_ID": "WARN"}
            }), encoding="utf-8")

            with (
                patch.object(schema_validation, "RAW_DIR", raw_dir),
                patch.object(schema_validation, "CONFIG_FILE", config_path),
            ):
                result = schema_validation.validate_referential_integrity()

            self.assertEqual(result["status"], "WARN")
            self.assertEqual(result["issue_count"], 1)
            self.assertEqual(
                result["sample_issues"][0]["reason"],
                "UNKNOWN_CUSTOMER_ID",
            )
            self.assertEqual(result["sample_issues"][0]["row"], 2)

    def test_known_customer_passes(self):
        with tempfile.TemporaryDirectory() as temp:
            raw_dir = Path(temp)
            (raw_dir / "customers.csv").write_text(
                "customer_id,customer_name,account_type,registration_date\n"
                "C001,Alice,SAVINGS,2026-01-01\n",
                encoding="utf-8",
            )
            (raw_dir / "transactions.csv").write_text(
                "transaction_id,customer_id,transaction_type,amount,"
                "transaction_timestamp,status\n"
                "T001,C001,DEBIT,10,2026-10-11 12:00:00,SUCCESS\n",
                encoding="utf-8",
            )
            config_path = raw_dir / "rules.json"
            config_path.write_text(json.dumps({
                "schemas": {},
                "issue_severities": {"UNKNOWN_CUSTOMER_ID": "WARN"}
            }), encoding="utf-8")

            with (
                patch.object(schema_validation, "RAW_DIR", raw_dir),
                patch.object(schema_validation, "CONFIG_FILE", config_path),
            ):
                result = schema_validation.validate_referential_integrity()

            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["issue_count"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
