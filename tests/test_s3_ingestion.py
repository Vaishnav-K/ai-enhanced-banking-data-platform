import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

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

class TestS3IngestionMain(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.raw_dir = self.root / "raw"
        self.raw_dir.mkdir()
        self.audit_file = self.root / "processed" / "audit.jsonl"
        self.content = b"id,amount\nT001,25.50\n"
        (self.raw_dir / "sample.csv").write_bytes(self.content)
        self.checksum = hashlib.sha256(self.content).hexdigest()
        self.bucket = "test-bucket"

        self.s3 = MagicMock()
        self.patches = [
            patch.object(ingestion, "RAW_DIR", self.raw_dir),
            patch.object(ingestion, "AUDIT_FILE", self.audit_file),
            patch.object(ingestion, "initialize_manifest"),
            patch.object(ingestion, "record_file"),
            patch.object(ingestion.boto3, "client", return_value=self.s3),
            patch.dict("os.environ", {"BANKING_S3_BUCKET": self.bucket}),
            patch("sys.argv", ["s3_ingestion.py", "--upload"]),
        ]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def test_uploads_and_verifies_new_file(self):
        self.s3.head_object.side_effect = [
            ingestion.ClientError(
                {
                    "Error": {"Code": "404"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            ),
            {
                "ContentLength": len(self.content),
                "Metadata": {"source-sha256": self.checksum},
            },
        ]

        ingestion.main()

        self.s3.upload_file.assert_called_once()
        ingestion.record_file.assert_called_once_with(
            "sample.csv",
            f"s3://{self.bucket}/banking-data/raw/sample.csv",
            self.checksum,
            len(self.content),
            "UPLOADED_AND_VERIFIED",
        )

    def test_skips_file_when_size_and_checksum_match(self):
        self.s3.head_object.return_value = {
            "ContentLength": len(self.content),
            "Metadata": {"source-sha256": self.checksum},
        }

        ingestion.main()

        self.s3.upload_file.assert_not_called()
        ingestion.record_file.assert_called_once_with(
            "sample.csv",
            f"s3://{self.bucket}/banking-data/raw/sample.csv",
            self.checksum,
            len(self.content),
            "SKIPPED_IDENTICAL",
        )

    def test_reuploads_file_when_checksum_differs(self):
        self.s3.head_object.side_effect = [
            {
                "ContentLength": len(self.content),
                "Metadata": {"source-sha256": "old-checksum"},
            },
            {
                "ContentLength": len(self.content),
                "Metadata": {"source-sha256": self.checksum},
            },
        ]

        ingestion.main()

        self.s3.upload_file.assert_called_once()
        ingestion.record_file.assert_called_once_with(
            "sample.csv",
            f"s3://{self.bucket}/banking-data/raw/sample.csv",
            self.checksum,
            len(self.content),
            "UPLOADED_AND_VERIFIED",
        )


    def test_upload_failure_is_recorded_in_manifest(self):
        self.s3.head_object.side_effect = ingestion.ClientError(
            {
                "Error": {"Code": "404"},
                "ResponseMetadata": {"HTTPStatusCode": 404},
            },
            "HeadObject",
        )
        self.s3.upload_file.side_effect = RuntimeError("Simulated upload failure")

        with self.assertRaisesRegex(RuntimeError, "Simulated upload failure"):
            ingestion.main()

        call = ingestion.record_file.call_args
        self.assertEqual(call.args[0], "sample.csv")
        self.assertEqual(call.args[4], "FAILED")
        self.assertIn("Simulated upload failure", call.kwargs["error"])

        audit = json.loads(
            self.audit_file.read_text(encoding="utf-8").strip()
        )
        self.assertEqual(audit["status"], "FAILED")
        self.assertIn("Simulated upload failure", audit["error"])

    def test_verification_mismatch_is_recorded_as_failure(self):
        self.s3.head_object.side_effect = [
            ingestion.ClientError(
                {
                    "Error": {"Code": "404"},
                    "ResponseMetadata": {"HTTPStatusCode": 404},
                },
                "HeadObject",
            ),
            {
                "ContentLength": len(self.content),
                "Metadata": {"source-sha256": "wrong-checksum"},
            },
        ]

        with self.assertRaisesRegex(RuntimeError, "Verification failed"):
            ingestion.main()

        call = ingestion.record_file.call_args
        self.assertEqual(call.args[4], "FAILED")
        self.assertIn("Verification failed", call.kwargs["error"])

        audit = json.loads(
            self.audit_file.read_text(encoding="utf-8").strip()
        )
        self.assertEqual(audit["status"], "FAILED")
        self.assertIn("Verification failed", audit["error"])
