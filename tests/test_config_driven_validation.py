import csv
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import schema_validation


class TestConfigDrivenValidation(unittest.TestCase):

    def test_allowed_values_come_from_config(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            raw_dir = root / "raw"
            raw_dir.mkdir()

            config_path = root / "validation_rules.json"
            config = {
                "version": 1,
                "schemas": {
                    "transactions.csv": {
                        "required_columns": schema_validation.EXPECTED_SCHEMAS[
                            "transactions.csv"
                        ],
                        "required_values": ["transaction_id", "status"],
                        "allowed_values": {"status": ["REVIEWED"]}
                    }
                },
                "field_rules": {},
                "issue_severities": {}
            }
            config_path.write_text(json.dumps(config), encoding="utf-8")

            headers = schema_validation.EXPECTED_SCHEMAS["transactions.csv"]
            with (raw_dir / "transactions.csv").open(
                "w", newline="", encoding="utf-8"
            ) as handle:
                writer = csv.writer(handle)
                writer.writerow(headers)
                writer.writerow([
                    "T001", "C001", "CREDIT", "100",
                    "2026-10-11 12:30:00", "REVIEWED"
                ])

            with (
                patch.object(schema_validation, "RAW_DIR", raw_dir),
                patch.object(schema_validation, "CONFIG_FILE", config_path),
            ):
                result = schema_validation.validate_values(
                    "transactions.csv", headers
                )

            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["issue_count"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
