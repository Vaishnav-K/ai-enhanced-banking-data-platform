import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

import demo
import lineage_tracker


class TestStageArtifactLineage(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

        (self.root / "config").mkdir()
        (self.root / "data" / "processed").mkdir(parents=True)
        (self.root / "data" / "quarantine").mkdir(parents=True)

        self.input_file = self.root / "data" / "raw" / "input.csv"
        self.input_file.parent.mkdir(parents=True)
        self.input_file.write_text("id,value\n1,10\n", encoding="utf-8")

        (self.root / "config" / "data_lineage.json").write_text(
            json.dumps({
                "nodes": [
                    {"id": "raw_input", "type": "artifact",
                     "path": "data/raw/input.csv"},
                    {"id": "main_etl", "type": "stage",
                     "stage_name": "Main ETL"},
                    {"id": "output", "type": "artifact",
                     "path": "data/processed/output.json"},
                ],
                "edges": [
                    {"from": "raw_input", "to": "main_etl"},
                    {"from": "main_etl", "to": "output"},
                ],
            }),
            encoding="utf-8",
        )

        self.root_patch = patch.object(demo, "ROOT", self.root)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)

        self.db_patch = patch.object(
            lineage_tracker, "LINEAGE_DB",
            self.root / "lineage.sqlite",
        )
        self.db_patch.start()
        self.addCleanup(self.db_patch.stop)

        lineage_tracker.initialize_lineage()
        lineage_tracker.initialize_artifact_table()
        lineage_tracker.initialize_attempt_table()
        lineage_tracker.initialize_stage_artifact_observation_table()
        self.run_id = lineage_tracker.start_run("Test pipeline")
        self.addCleanup(self.close_test_database)

    def close_test_database(self):
        import gc
        gc.collect()

    def test_declared_inputs_returns_direct_artifact_inputs(self):
        self.assertEqual(
            demo.declared_inputs_for_stage("Main ETL"),
            ["data/raw/input.csv"],
        )

    def test_successful_stage_records_declared_input_and_changed_output(self):
        output = self.root / "data" / "processed" / "output.json"

        def fake_command(command, cwd=demo.ROOT):
            output.write_text('{"status":"PASS"}', encoding="utf-8")
            return {"exit_code": 0, "stdout": "", "stderr": ""}

        with patch.object(demo, "run_command", side_effect=fake_command):
            result = demo.run_stage(
                "Main ETL", ["python", "fake.py"],
                run_id=self.run_id, stage_order=1,
                max_attempts=1,
            )

        self.assertEqual(result["status"], "PASS")
        observations = lineage_tracker.get_stage_artifact_observations(
            self.run_id, stage_order=1,
        )
        observed = {
            (row["artifact_path"], row["artifact_role"], row["evidence_type"])
            for row in observations
        }
        self.assertIn(
            (self.input_file.resolve().as_posix(), "INPUT", "DECLARED_INPUT"), observed
        )
        self.assertIn(
            ((self.root / "data" / "processed" / "output.json").resolve().as_posix(), "OUTPUT", "OBSERVED_CHANGE"),
            observed,
        )

    def test_retry_records_output_for_each_attempt(self):
        output = self.root / "data" / "processed" / "output.json"
        calls = {"count": 0}

        def fake_command(command, cwd=demo.ROOT):
            calls["count"] += 1
            output.write_text("x" * calls["count"], encoding="utf-8")
            code = 75 if calls["count"] == 1 else 0
            return {"exit_code": code, "stdout": "", "stderr": ""}

        with patch.object(demo, "run_command", side_effect=fake_command):
            with patch.object(demo.time, "sleep"):
                result = demo.run_stage(
                    "Main ETL", ["python", "fake.py"],
                    run_id=self.run_id, stage_order=1,
                    max_attempts=2, retryable_exit_codes=(75,),
                    retry_delay_seconds=0,
                )

        self.assertEqual(result["status"], "PASS")
        observations = lineage_tracker.get_stage_artifact_observations(
            self.run_id, stage_order=1,
        )
        output_attempts = {
            row["attempt_number"] for row in observations
            if row["artifact_role"] == "OUTPUT"
            and row["artifact_path"] == (self.root / "data" / "processed" / "output.json").resolve().as_posix()
        }
        self.assertEqual(output_attempts, {1, 2})

    def test_declared_outputs_are_recorded_even_when_unchanged(self):
        output_path = self.root / "data" / "processed" / "output.parquet"
        output_path.write_bytes(b"existing output")

        from lineage_tracker import (
            record_stage_artifact_observation,
            get_stage_artifact_observations,
        )
        record_stage_artifact_observation(
            self.run_id, 1, "Main ETL", 1,
            output_path, "OUTPUT", "DECLARED_OUTPUT"
        )

        observations = get_stage_artifact_observations(self.run_id, 1)
        matching = [
            row for row in observations
            if row["evidence_type"] == "DECLARED_OUTPUT"
        ]
        self.assertEqual(len(matching), 1)
        self.assertTrue(matching[0]["file_exists"])


if __name__ == "__main__":
    unittest.main()
