import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import sys

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
import demo


class TestAutomaticArtifactDiscovery(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)

        self.processed = self.root / "data" / "processed"
        self.quarantine = self.root / "data" / "quarantine"
        self.processed.mkdir(parents=True)
        self.quarantine.mkdir(parents=True)

        self.roots_patch = patch.object(
            demo,
            "ROOT",
            self.root,
        )
        self.roots_patch.start()
        self.addCleanup(self.roots_patch.stop)

    def test_discovers_new_file(self):
        before = demo.snapshot_artifacts()
        output = self.processed / "new_report.json"
        output.write_text('{"status":"PASS"}', encoding="utf-8")

        changed = demo.discover_changed_artifacts(before)

        self.assertIn(output.resolve(), changed)

    def test_discovers_modified_file(self):
        output = self.processed / "report.json"
        output.write_text("version 1", encoding="utf-8")
        before = demo.snapshot_artifacts()

        output.write_text("version 2 with changes", encoding="utf-8")
        changed = demo.discover_changed_artifacts(before)

        self.assertIn(output.resolve(), changed)

    def test_ignores_unchanged_file(self):
        output = self.processed / "report.json"
        output.write_text("unchanged", encoding="utf-8")
        before = demo.snapshot_artifacts()

        changed = demo.discover_changed_artifacts(before)

        self.assertNotIn(output.resolve(), changed)

    def test_ignores_lineage_database(self):
        database = self.processed / "ingestion_manifest.db"
        database.write_bytes(b"database")
        before = demo.snapshot_artifacts()

        self.assertNotIn(database.resolve(), before)


if __name__ == "__main__":
    unittest.main()
