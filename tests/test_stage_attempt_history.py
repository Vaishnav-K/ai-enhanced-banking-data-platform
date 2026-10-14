import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import lineage_tracker


class TestStageAttemptHistory(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)

        self.db_path = Path(self.temp_dir.name) / "lineage.db"
        patcher = patch.object(lineage_tracker, "LINEAGE_DB", self.db_path)
        patcher.start()
        self.addCleanup(patcher.stop)

        lineage_tracker.initialize_lineage()
        self.run_id = lineage_tracker.start_run("Attempt History Test")
        lineage_tracker.record_stage(
            self.run_id,
            1,
            {
                "stage": "Test Stage",
                "status": "PASS",
                "started_at_utc": "2026-01-01T00:00:00+00:00",
                "finished_at_utc": "2026-01-01T00:00:01+00:00",
                "duration_seconds": 1,
                "exit_code": 0,
                "error": None,
            },
        )

    def test_records_multiple_attempts(self):
        lineage_tracker.record_stage_attempt(
            self.run_id, 1,
            {
                "attempt": 1, "status": "FAIL", "exit_code": 75,
                "error": "temporary error", "retryable": True,
                "duration_seconds": 0.2,
            },
        )
        lineage_tracker.record_stage_attempt(
            self.run_id, 1,
            {
                "attempt": 2, "status": "PASS", "exit_code": 0,
                "error": None, "retryable": False,
                "duration_seconds": 0.3,
            },
        )

        attempts = lineage_tracker.get_stage_attempts(self.run_id, 1)
        self.assertEqual(len(attempts), 2)
        self.assertEqual([a["status"] for a in attempts], ["FAIL", "PASS"])
        self.assertEqual(attempts[0]["retryable"], 1)

    def test_attempts_are_isolated_by_run(self):
        other_run = lineage_tracker.start_run("Other Test")
        self.assertEqual(lineage_tracker.get_stage_attempts(other_run), [])

    def test_missing_attempt_history_returns_empty_list(self):
        self.assertEqual(lineage_tracker.get_stage_attempts(self.run_id), [])


if __name__ == "__main__":
    unittest.main()
