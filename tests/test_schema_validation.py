import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import schema_validation


class TestSchemaValidation(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.raw_dir = Path(self.temp_dir.name)
        self.file_path = self.raw_dir / "customers.csv"

    def validate_header(self, header):
        self.file_path.write_text(header, encoding="utf-8")
        with patch.object(schema_validation, "RAW_DIR", self.raw_dir):
            return schema_validation.validate_file(
                "customers.csv",
                ["customer_id", "customer_name", "account_type", "registration_date"],
            )

    def test_missing_column_fails(self):
        result = self.validate_header(
            "customer_id,customer_name,account_type\n"
        )
        self.assertEqual(result["status"], "FAIL")
        self.assertIn("registration_date", result["missing_columns"])

    def test_extra_column_is_reported_without_failure(self):
        result = self.validate_header(
            "customer_id,customer_name,account_type,registration_date,extra_field\n"
        )
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["unexpected_columns"], ["extra_field"])

    def test_duplicate_header_fails(self):
        result = self.validate_header(
            "customer_id,customer_name,account_type,registration_date,customer_id\n"
        )
        self.assertEqual(result["status"], "FAIL")
        self.assertIn("customer_id", result["duplicate_columns"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
