import json
from contextlib import closing
import sqlite3
import tempfile
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from runtime_lineage_report import build_report


class TestRuntimeLineageReport(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.db = root / "lineage.db"
        self.config = root / "lineage.json"
        self.config.write_text(json.dumps({
            "nodes": [
                {"id": "raw", "type": "artifact", "path": "data/raw.csv"},
                {"id": "stage_a", "type": "stage", "stage_name": "Stage A"},
                {"id": "out", "type": "artifact", "path": "data/out.parquet"},
                {"id": "stage_b", "type": "stage", "stage_name": "Stage B"},
                {"id": "report", "type": "artifact", "path": "data/report.json"},
            ],
            "edges": [
                {"from": "raw", "to": "stage_a"},
                {"from": "stage_a", "to": "out"},
                {"from": "stage_a", "to": "stage_b"},
                {"from": "stage_b", "to": "report"},
            ],
        }), encoding="utf-8")
        with closing(sqlite3.connect(self.db)) as con:
            con.executescript("""
                CREATE TABLE pipeline_runs (
                    run_id TEXT PRIMARY KEY, status TEXT,
                    started_at_utc TEXT, finished_at_utc TEXT
                );
                CREATE TABLE pipeline_stage_artifact_observations (
                    run_id TEXT, stage_order INTEGER, stage_name TEXT,
                    attempt_number INTEGER, artifact_path TEXT,
                    artifact_role TEXT, evidence_type TEXT, file_exists INTEGER
                );
                INSERT INTO pipeline_runs VALUES
                    ('run-1', 'PASS', '2026-01-01T00:00:00Z', '2026-01-01T00:00:01Z');
            """)
            self.add_obs(con, "Stage A", "data/raw.csv", "INPUT", "DECLARED_INPUT", 1)
            self.add_obs(con, "Stage A", "data/out.parquet", "OUTPUT", "DECLARED_OUTPUT", 1)
            self.add_obs(con, "Stage B", "data/out.parquet", "INPUT", "DECLARED_INPUT", 1)
            self.add_obs(con, "Stage B", "data/report.json", "OUTPUT", "DECLARED_OUTPUT", 1)
            con.commit()

    def add_obs(self, con, stage, path, role, evidence, exists):
        order = 1 if stage == "Stage A" else 2
        con.execute("""
            INSERT INTO pipeline_stage_artifact_observations
            VALUES ('run-1', ?, ?, 1, ?, ?, ?, ?)
        """, (order, stage, path, role, evidence, exists))

    def tearDown(self):
        self.temp.cleanup()

    def test_matching_declared_lineage_passes(self):
        report = build_report(self.db, self.config)
        self.assertEqual(report["status"], "PASS")
        self.assertEqual(report["summary"]["stages_analyzed"], 2)

    def test_missing_input_observation_is_reported(self):
        with closing(sqlite3.connect(self.db)) as con:
            con.execute("""
                DELETE FROM pipeline_stage_artifact_observations
                WHERE stage_name = 'Stage A' AND artifact_role = 'INPUT'
            """)
            con.commit()
        report = build_report(self.db, self.config)
        stage = next(s for s in report["stages"] if s["stage_name"] == "Stage A")
        self.assertEqual(stage["missing_recorded_inputs"], ["data/raw.csv"])
        self.assertEqual(report["status"], "WARN")

    def test_undeclared_changed_output_is_reported(self):
        with closing(sqlite3.connect(self.db)) as con:
            con.execute("""
                INSERT INTO pipeline_stage_artifact_observations
                VALUES ('run-1', 1, 'Stage A', 1, 'data/unexpected.csv',
                        'OUTPUT', 'OBSERVED_CHANGE', 1)
            """)
            con.commit()
        report = build_report(self.db, self.config)
        stage = next(s for s in report["stages"] if s["stage_name"] == "Stage A")
        self.assertEqual(stage["unexpected_changed_outputs"], ["data/unexpected.csv"])
        self.assertEqual(report["status"], "WARN")


if __name__ == "__main__":
    unittest.main()
