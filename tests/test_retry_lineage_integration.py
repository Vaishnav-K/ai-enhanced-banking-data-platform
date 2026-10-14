import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import demo
import lineage_tracker


class TestRetryLineageIntegration(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "lineage.db"
        self.db_patch = patch.object(
            lineage_tracker, "LINEAGE_DB", self.db_path
        )
        self.db_patch.start()
        lineage_tracker.initialize_lineage()
        self.run_id = lineage_tracker.start_run("integration-test")

    def tearDown(self):
        self.db_patch.stop()
        self.temp_dir.cleanup()

    def test_retry_attempts_are_persisted_in_order(self):
        stage = {
            "stage": "Transient Failure",
            "status": "PASS",
            "started_at_utc": "2026-10-14T10:00:00+00:00",
            "finished_at_utc": "2026-10-14T10:00:02+00:00",
            "duration_seconds": 2.0,
            "exit_code": 0,
            "error": None,
            "attempt_count": 2,
            "max_attempts": 3,
            "attempts": [
                {
                    "attempt": 1,
                    "started_at_utc": "2026-10-14T10:00:00+00:00",
                    "finished_at_utc": "2026-10-14T10:00:01+00:00",
                    "duration_seconds": 1.0,
                    "exit_code": 75,
                    "status": "FAIL",
                    "error": "Temporary failure",
                    "retryable": True,
                },
                {
                    "attempt": 2,
                    "started_at_utc": "2026-10-14T10:00:01+00:00",
                    "finished_at_utc": "2026-10-14T10:00:02+00:00",
                    "duration_seconds": 1.0,
                    "exit_code": 0,
                    "status": "PASS",
                    "error": None,
                    "retryable": False,
                },
            ],
        }

        demo.record_stage_history(self.run_id, [stage])

        attempts = lineage_tracker.get_stage_attempts(self.run_id, 1)
        summary = lineage_tracker.get_run(self.run_id)

        self.assertEqual(
            [item["status"] for item in attempts], ["FAIL", "PASS"]
        )
        self.assertEqual(
            [item["attempt_number"] for item in attempts], [1, 2]
        )
        self.assertEqual(attempts[0]["retryable"], 1)
        self.assertEqual(attempts[1]["exit_code"], 0)
        self.assertEqual(summary["status"], "RUNNING")


if __name__ == "__main__":
    unittest.main()
