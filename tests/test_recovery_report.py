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

    def test_recent_run_is_classified_without_status_mutation(self):
        run_id = tracker.start_run("recent-recovery-test")
        report = tracker.get_recovery_report(stale_after_minutes=60)
        item = next(
            run for run in report["interrupted_runs"]
            if run["run_id"] == run_id
        )
        self.assertEqual(item["recovery_classification"], "RECENTLY_STARTED")
        self.assertGreaterEqual(item["age_minutes"], 0)
        with tracker.closing(tracker.connect()) as conn:
            row = conn.execute(
                "SELECT status FROM pipeline_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        self.assertEqual(row["status"], "RUNNING")

    def test_old_run_is_classified_as_potentially_stale(self):
        run_id = tracker.start_run("stale-recovery-test")
        with tracker.closing(tracker.connect()) as conn:
            conn.execute(
                "UPDATE pipeline_runs SET started_at_utc = ? WHERE run_id = ?",
                ("2020-01-01T00:00:00+00:00", run_id),
            )
            conn.commit()
        report = tracker.get_recovery_report(stale_after_minutes=60)
        item = next(
            run for run in report["interrupted_runs"]
            if run["run_id"] == run_id
        )
        self.assertEqual(item["recovery_classification"], "POTENTIALLY_STALE")
        self.assertGreater(item["age_minutes"], 60)
        with tracker.closing(tracker.connect()) as conn:
            row = conn.execute(
                "SELECT status FROM pipeline_runs WHERE run_id = ?",
                (run_id,),
            ).fetchone()
        self.assertEqual(row["status"], "RUNNING")

    def test_malformed_timestamp_has_unknown_age(self):
        run_id = tracker.start_run("unknown-age-test")
        with tracker.closing(tracker.connect()) as conn:
            conn.execute(
                "UPDATE pipeline_runs SET started_at_utc = ? WHERE run_id = ?",
                ("not-a-timestamp", run_id),
            )
            conn.commit()
        report = tracker.get_recovery_report()
        item = next(
            run for run in report["interrupted_runs"]
            if run["run_id"] == run_id
        )
        self.assertEqual(item["recovery_classification"], "UNKNOWN")
        self.assertIsNone(item["age_minutes"])
        self.assertTrue(item["timestamp_parse_error"])

    def test_invalid_stale_threshold_is_rejected(self):
        for value in (0, -1, True, "60"):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    tracker.get_recovery_report(stale_after_minutes=value)

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
            {
                "interrupted_runs",
                "failed_stages",
                "attempt_history",
                "stale_after_minutes",
                "potentially_stale_count",
                "recently_started_count",
                "unknown_age_count",
            }
        )
        self.assertEqual(report["interrupted_runs"], [])
        self.assertEqual(report["failed_stages"], [])
        self.assertEqual(report["attempt_history"], [])


if __name__ == "__main__":
    unittest.main()
