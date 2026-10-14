import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import lineage_tracker as tracker


class TestRecoveryReport(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(
            tracker, "LINEAGE_DB",
            Path(self.temp_dir.name) / "lineage.db"
        )
        self.db_patch.start()
        tracker.initialize_lineage()

    def tearDown(self):
        self.db_patch.stop()
        self.temp_dir.cleanup()

    def test_running_unfinished_run_is_detected(self):
        run_id = tracker.start_run("recovery-test")
        result = tracker.get_interrupted_runs()
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["run_id"], run_id)

    def test_finished_run_is_not_interrupted(self):
        run_id = tracker.start_run("finished-test")
        with tracker.closing(tracker.connect()) as conn:
            conn.execute(
                """
                UPDATE pipeline_runs
                SET status = 'PASS', finished_at_utc = ?
                WHERE run_id = ?
                """,
                (tracker.utc_now(), run_id),
            )
            conn.commit()
        self.assertEqual(tracker.get_interrupted_runs(), [])

    def test_recovery_report_has_expected_sections(self):
        report = tracker.get_recovery_report()
        self.assertEqual(
            set(report),
            {"interrupted_runs", "failed_stages", "attempt_history"}
        )
        self.assertEqual(report["interrupted_runs"], [])
        self.assertEqual(report["failed_stages"], [])
        self.assertEqual(report["attempt_history"], [])


if __name__ == "__main__":
    unittest.main()
