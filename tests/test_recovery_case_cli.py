import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import lineage_tracker as tracker
import recovery_case_cli as cli


class TestRecoveryCaseCli(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_patch = patch.object(
            tracker,
            "LINEAGE_DB",
            Path(self.temp_dir.name) / "lineage.db",
        )
        self.db_patch.start()
        tracker.initialize_lineage()
        self.run_id = tracker.start_run("recovery-cli-test")

    def tearDown(self):
        self.db_patch.stop()
        self.temp_dir.cleanup()

    def run_cli(self, args):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            result = cli.main(args)
        return result, json.loads(output.getvalue())

    def test_record_command_persists_and_returns_event(self):
        result, output = self.run_cli([
            "record",
            "--run-id", self.run_id,
            "--decision", "INVESTIGATED",
            "--reason", "Reviewed available recovery evidence.",
            "--recorded-by", "cli-test",
            "--evidence-json", '{"stage_record_count": 0}',
        ])

        self.assertEqual(result, 0)
        self.assertTrue(output["recorded"])
        self.assertEqual(output["decision"], "INVESTIGATED")
        self.assertEqual(output["automatic_actions_taken"], [])

        result, listing = self.run_cli(["list", "--run-id", self.run_id])
        self.assertEqual(result, 0)
        self.assertEqual(listing["count"], 1)
        self.assertEqual(
            listing["events"][0]["evidence_snapshot"]["stage_record_count"],
            0,
        )

    def test_list_command_returns_empty_history(self):
        result, output = self.run_cli(["list", "--run-id", self.run_id])
        self.assertEqual(result, 0)
        self.assertEqual(output["count"], 0)
        self.assertEqual(output["events"], [])

    def test_rejects_invalid_evidence_json(self):
        with self.assertRaises(SystemExit) as error:
            cli.main([
                "record",
                "--run-id", self.run_id,
                "--decision", "INVESTIGATED",
                "--reason", "Reviewed evidence.",
                "--evidence-json", "{invalid",
            ])
        self.assertEqual(error.exception.code, 2)

    def test_rejects_non_object_evidence_json(self):
        with self.assertRaises(SystemExit) as error:
            cli.main([
                "record",
                "--run-id", self.run_id,
                "--decision", "INVESTIGATED",
                "--reason", "Reviewed evidence.",
                "--evidence-json", "[1, 2]",
            ])
        self.assertEqual(error.exception.code, 2)

    def test_rejects_unknown_run(self):
        with self.assertRaises(SystemExit) as error:
            cli.main([
                "record",
                "--run-id", "unknown-run",
                "--decision", "INVESTIGATED",
                "--reason", "Reviewed evidence.",
            ])
        self.assertEqual(error.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
