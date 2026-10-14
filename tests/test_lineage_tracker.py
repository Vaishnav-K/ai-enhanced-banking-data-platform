import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import lineage_tracker


class TestLineageTracker(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "lineage.db"
        self.db_patch = patch.object(
            lineage_tracker, "LINEAGE_DB", self.db_path
        )
        self.db_patch.start()
        self.addCleanup(self.db_patch.stop)
        self.addCleanup(self.temp_dir.cleanup)

        lineage_tracker.initialize_lineage()

    def test_run_ids_are_unique(self):
        first = lineage_tracker.start_run("Test Pipeline")
        second = lineage_tracker.start_run("Test Pipeline")

        self.assertNotEqual(first, second)

    def test_stage_timing_and_status_are_preserved(self):
        run_id = lineage_tracker.start_run("Test Pipeline")
        stage = {
            "stage": "Main ETL",
            "status": "PASS",
            "started_at_utc": "2026-10-14T10:00:00+00:00",
            "finished_at_utc": "2026-10-14T10:00:02+00:00",
            "duration_seconds": 2.0,
            "exit_code": 0,
            "error": None,
        }

        lineage_tracker.record_stage(run_id, 1, stage)
        saved = lineage_tracker.get_run(run_id)

        self.assertEqual(len(saved["stages"]), 1)
        self.assertEqual(saved["stages"][0]["stage_name"], "Main ETL")
        self.assertEqual(saved["stages"][0]["status"], "PASS")
        self.assertEqual(saved["stages"][0]["duration_seconds"], 2.0)

    def test_run_completion_is_saved(self):
        run_id = lineage_tracker.start_run("Test Pipeline")
        report = {
            "run_finished_at_utc": "2026-10-14T10:00:02+00:00",
            "total_duration_seconds": 2.0,
            "status": "PASS",
            "failure_stage": None,
        }

        lineage_tracker.finish_run(run_id, report)
        saved = lineage_tracker.get_run(run_id)

        self.assertEqual(saved["status"], "PASS")
        self.assertEqual(saved["duration_seconds"], 2.0)
        self.assertIsNotNone(saved["finished_at_utc"])

    def test_unknown_run_returns_none(self):
        self.assertIsNone(lineage_tracker.get_run("unknown-run-id"))


if __name__ == "__main__":
    unittest.main()
