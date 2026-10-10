import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import src.s3_ingestion as ingestion


class TestS3IngestionHelpers(unittest.TestCase):
    def test_sha256_file_matches_expected_digest(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "sample.csv"
            content = b"id,amount\nT001,25.50\n"
            path.write_bytes(content)

            expected = hashlib.sha256(content).hexdigest()

            self.assertEqual(ingestion.sha256_file(path), expected)

    def test_audit_record_is_written_as_json_line(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            audit_path = Path(temp_dir) / "audit.jsonl"

            with patch.object(ingestion, "AUDIT_FILE", audit_path):
                ingestion.append_audit({
                    "file": "sample.csv",
                    "status": "UPLOADED_AND_VERIFIED",
                })

            records = [
                json.loads(line)
                for line in audit_path.read_text(encoding="utf-8").splitlines()
            ]

            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["file"], "sample.csv")
            self.assertEqual(
                records[0]["status"], "UPLOADED_AND_VERIFIED"
            )


if __name__ == "__main__":
    unittest.main()
