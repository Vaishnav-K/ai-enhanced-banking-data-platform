import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import quality_report


class TestSourceReportIntegration(unittest.TestCase):

    def test_loads_source_validation_status_and_files(self):
        with tempfile.TemporaryDirectory() as temp:
            report_path = Path(temp) / "schema_validation_report.json"
            report_path.write_text(json.dumps({
                "status": "PASS",
                "configuration_version": 1,
                "files": {
                    "customers.csv": {"status": "PASS"},
                    "referential_integrity": {"status": "WARN"}
                }
            }), encoding="utf-8")

            with patch.object(quality_report, "SOURCE_REPORT", report_path):
                result = quality_report.load_source_report()

        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["configuration_version"], 1)
        self.assertEqual(
            result["files"]["referential_integrity"]["status"], "WARN"
        )

    def test_missing_source_report_is_unavailable(self):
        with tempfile.TemporaryDirectory() as temp:
            missing_path = Path(temp) / "missing.json"

            with patch.object(quality_report, "SOURCE_REPORT", missing_path):
                result = quality_report.load_source_report()

        self.assertEqual(result["status"], "UNAVAILABLE")
        self.assertIn("not found", result["error"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
