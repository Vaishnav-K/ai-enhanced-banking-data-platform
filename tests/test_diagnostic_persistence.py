import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import lineage_tracker as tracker


class TestDiagnosticPersistence(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(
            tracker,
            "LINEAGE_DB",
            Path(self.temp_dir.name) / "lineage.db",
        )
        self.db_patch.start()
        tracker.initialize_lineage()
        self.run_id = tracker.start_run("diagnostic-persistence-test")

    def tearDown(self):
        self.db_patch.stop()
        self.temp_dir.cleanup()

    def test_detailed_failure_survives_recovery_report(self):
        diagnostic = (
            "Process exited with code 1\n"
            "STDERR:\nValueError: invalid transaction amount"
        )
        stage = {
            "stage": "Main ETL",
            "status": "FAIL",
            "started_at_utc": tracker.utc_now(),
            "finished_at_utc": tracker.utc_now(),
            "duration_seconds": 0.5,
            "exit_code": 1,
            "error": diagnostic,
            "attempt_count": 1,
            "max_attempts": 1,
            "attempts": [{
                "attempt": 1,
                "started_at_utc": tracker.utc_now(),
                "finished_at_utc": tracker.utc_now(),
                "duration_seconds": 0.5,
                "exit_code": 1,
                "status": "FAIL",
                "error": diagnostic,
                "retryable": False,
            }],
        }

        tracker.record_stage(self.run_id, 1, stage)
        tracker.record_stage_attempt(self.run_id, 1, stage["attempts"][0])

        report = tracker.get_recovery_report()

        self.assertEqual(len(report["failed_stages"]), 1)
        self.assertEqual(len(report["attempt_history"]), 1)
        self.assertIn(
            "ValueError: invalid transaction amount",
            report["failed_stages"][0]["error"],
        )
        self.assertIn(
            "ValueError: invalid transaction amount",
            report["attempt_history"][0]["error"],
        )


if __name__ == "__main__":
    unittest.main()
