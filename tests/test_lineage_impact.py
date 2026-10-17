import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from lineage_impact import analyze_impact, load_graph


class TestLineageImpact(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config = Path(self.temp_dir.name) / "lineage.json"
        self.graph = {
            "version": 1,
            "nodes": [
                {"id": "raw", "type": "artifact", "path": "raw.csv"},
                {"id": "etl", "type": "stage", "stage_name": "ETL"},
                {"id": "quality", "type": "stage", "stage_name": "Quality"},
                {"id": "out", "type": "artifact", "path": "out.parquet"},
                {"id": "unrelated", "type": "artifact", "path": "other.csv"},
            ],
            "edges": [
                {"from": "raw", "to": "etl"},
                {"from": "etl", "to": "quality"},
                {"from": "quality", "to": "out"},
            ],
        }
        self.write_graph()

    def tearDown(self):
        self.temp_dir.cleanup()

    def write_graph(self):
        self.config.write_text(json.dumps(self.graph), encoding="utf-8")

    def test_artifact_path_finds_transitive_impact(self):
        report = analyze_impact("raw.csv", self.config)
        self.assertEqual(report["impacted_stages"], ["ETL", "Quality"])
        self.assertEqual(report["impacted_artifacts"], ["out.parquet"])
        self.assertEqual(report["analysis_mode"], "DECLARED_LINEAGE")

    def test_stage_name_is_accepted(self):
        report = analyze_impact("ETL", self.config)
        self.assertIn("Quality", report["impacted_stages"])
        self.assertIn("out.parquet", report["impacted_artifacts"])

    def test_unrelated_node_is_not_reported(self):
        report = analyze_impact("unrelated", self.config)
        self.assertEqual(report["impact_count"], 0)

    def test_unknown_target_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "Unknown lineage target"):
            analyze_impact("missing.csv", self.config)

    def test_unknown_edge_node_is_rejected(self):
        self.graph["edges"].append({"from": "raw", "to": "missing"})
        self.write_graph()
        with self.assertRaisesRegex(ValueError, "unknown node"):
            load_graph(self.config)

    def test_cycle_is_rejected(self):
        self.graph["edges"].append({"from": "out", "to": "raw"})
        self.write_graph()
        with self.assertRaisesRegex(ValueError, "cycle"):
            load_graph(self.config)

    def test_duplicate_node_is_rejected(self):
        self.graph["nodes"].append(dict(self.graph["nodes"][0]))
        self.write_graph()
        with self.assertRaisesRegex(ValueError, "duplicate"):
            load_graph(self.config)

    def test_invalid_json_is_reported(self):
        self.config.write_text("{invalid", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "Cannot load"):
            load_graph(self.config)


if __name__ == "__main__":
    unittest.main(verbosity=2)
