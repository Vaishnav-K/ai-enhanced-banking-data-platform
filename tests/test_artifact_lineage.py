import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src import lineage_tracker


class TestArtifactLineage(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.db_path = Path(self.temp_dir.name) / "lineage.db"

        patcher = patch.object(lineage_tracker, "LINEAGE_DB", self.db_path)
        patcher.start()
        self.addCleanup(patcher.stop)

        lineage_tracker.initialize_lineage()
        self.run_id = lineage_tracker.start_run("Test Pipeline")

    def test_records_input_checksum_and_size(self):
        file_path = Path(self.temp_dir.name) / "input.csv"
        content = b"id,amount\n1,100\n"
        file_path.write_bytes(content)

        result = lineage_tracker.record_artifact(
            self.run_id, file_path, "INPUT"
        )

        self.assertTrue(result)
        artifacts = lineage_tracker.get_run_artifacts(self.run_id)
        self.assertEqual(len(artifacts), 1)
        self.assertEqual(
            artifacts[0]["checksum_sha256"],
            hashlib.sha256(content).hexdigest(),
        )
        self.assertEqual(artifacts[0]["size_bytes"], len(content))
        self.assertEqual(artifacts[0]["artifact_role"], "INPUT")

    def test_records_output_artifact(self):
        file_path = Path(self.temp_dir.name) / "output.json"
        file_path.write_text('{"status":"PASS"}', encoding="utf-8")

        lineage_tracker.record_artifact(
            self.run_id, file_path, "OUTPUT"
        )

        artifacts = lineage_tracker.get_run_artifacts(self.run_id)
        self.assertEqual(artifacts[0]["artifact_role"], "OUTPUT")

    def test_missing_file_is_skipped(self):
        missing = Path(self.temp_dir.name) / "missing.csv"

        result = lineage_tracker.record_artifact(
            self.run_id, missing, "INPUT"
        )

        self.assertFalse(result)
        self.assertEqual(
            lineage_tracker.get_run_artifacts(self.run_id), []
        )

    def test_invalid_role_is_rejected(self):
        file_path = Path(self.temp_dir.name) / "input.csv"
        file_path.write_text("id\n1\n", encoding="utf-8")

        with self.assertRaises(ValueError):
            lineage_tracker.record_artifact(
                self.run_id, file_path, "UNKNOWN"
            )


if __name__ == "__main__":
    unittest.main()
