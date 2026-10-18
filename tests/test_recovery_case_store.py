import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import lineage_tracker as tracker
import recovery_case_store as store


class TestRecoveryCaseStore(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(
            tracker, "LINEAGE_DB",
            Path(self.temp_dir.name) / "lineage.db"
        )
        self.db_patch.start()
        tracker.initialize_lineage()
        self.run_id = tracker.start_run("recovery-case-test")

    def tearDown(self):
        self.db_patch.stop()
        self.temp_dir.cleanup()

    def test_records_decision_with_reason_and_evidence(self):
        event_id = store.record_recovery_decision(
            self.run_id,
            "REQUIRES_MANUAL_INTERVENTION",
            "No stage or artifact evidence exists for this unfinished run.",
            recorded_by="test-operator",
            evidence_snapshot={"stage_record_count": 0},
        )
        events = store.list_recovery_decisions(self.run_id)

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0]["event_id"], event_id)
        self.assertEqual(
            events[0]["decision"], "REQUIRES_MANUAL_INTERVENTION"
        )
        self.assertEqual(events[0]["recorded_by"], "test-operator")
        self.assertEqual(
            events[0]["evidence_snapshot"]["stage_record_count"], 0
        )

    def test_decisions_are_append_only(self):
        store.record_recovery_decision(
            self.run_id, "INVESTIGATED", "Reviewed available evidence."
        )
        store.record_recovery_decision(
            self.run_id,
            "REQUIRES_MANUAL_INTERVENTION",
            "Process state remains unverified.",
        )

        events = store.list_recovery_decisions(self.run_id)
        self.assertEqual(
            [event["decision"] for event in events],
            ["INVESTIGATED", "REQUIRES_MANUAL_INTERVENTION"],
        )

    def test_recording_decision_does_not_change_run_status(self):
        store.record_recovery_decision(
            self.run_id, "SAFE_TO_RETRY",
            "Operator reviewed prerequisites; no retry was executed."
        )
        with tracker.closing(tracker.connect()) as conn:
            row = conn.execute(
                "SELECT status FROM pipeline_runs WHERE run_id = ?",
                (self.run_id,),
            ).fetchone()
        self.assertEqual(row["status"], "RUNNING")

    def test_rejects_invalid_decision_or_empty_reason(self):
        with self.assertRaises(ValueError):
            store.record_recovery_decision(
                self.run_id, "RETRY_NOW", "Not an allowed audit decision."
            )
        with self.assertRaises(ValueError):
            store.record_recovery_decision(
                self.run_id, "INVESTIGATED", "   "
            )
        self.assertEqual(store.list_recovery_decisions(self.run_id), [])

    def test_rejects_unknown_run(self):
        with self.assertRaises(ValueError):
            store.record_recovery_decision(
                "unknown-run", "INVESTIGATED", "Reviewed."
            )

    def test_can_list_all_runs_or_filter_by_run(self):
        another_run = tracker.start_run("another-recovery-case-test")
        store.record_recovery_decision(
            self.run_id, "INVESTIGATED", "First run reviewed."
        )
        store.record_recovery_decision(
            another_run, "INVESTIGATED", "Second run reviewed."
        )

        self.assertEqual(len(store.list_recovery_decisions()), 2)
        self.assertEqual(len(store.list_recovery_decisions(self.run_id)), 1)


if __name__ == "__main__":
    unittest.main()
