import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from contextlib import redirect_stdout
from io import StringIO

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import recovery_report


class TestRecoveryReportCLI(unittest.TestCase):
    def test_human_report_shows_interrupted_run(self):
        report = {
            "interrupted_runs": [{
                "run_id": "run-123",
                "pipeline_name": "banking-pipeline",
                "started_at_utc": "2026-10-14T10:00:00+00:00",
            }],
            "failed_stages": [],
            "attempt_history": [],
        }
        output = recovery_report.format_report(report)
        self.assertIn("Interrupted runs: 1", output)
        self.assertIn("run-123", output)
        self.assertIn("recovery candidate", output)

    def test_human_report_shows_retry_history(self):
        report = {
            "interrupted_runs": [],
            "failed_stages": [],
            "attempt_history": [{
                "run_id": "run-456",
                "stage_name": "S3 Raw Data Ingestion",
                "attempt_number": 2,
                "status": "PASS",
                "retryable": 0,
                "error": None,
            }],
        }
        output = recovery_report.format_report(report, details=True)
        self.assertIn("Attempt: 2", output)
        self.assertIn("Status: PASS", output)
        self.assertIn("S3 Raw Data Ingestion", output)

    def test_cli_json_mode(self):
        report = {
            "interrupted_runs": [],
            "failed_stages": [],
            "attempt_history": [],
        }
        output = StringIO()
        with patch.object(recovery_report, "get_recovery_report", return_value=report):
            with redirect_stdout(output):
                result = recovery_report.main(["--json"])
        self.assertEqual(result, 0)
        self.assertIn('"interrupted_runs": []', output.getvalue())


if __name__ == "__main__":
    unittest.main()
