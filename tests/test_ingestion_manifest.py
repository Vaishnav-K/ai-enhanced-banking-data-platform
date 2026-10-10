import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import src.ingestion_manifest as manifest


class TestIngestionManifest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "manifest.db"

        self.db_patch = patch.object(manifest, "MANIFEST_DB", self.db_path)
        self.db_patch.start()
        manifest.initialize_manifest()

    def tearDown(self):
        self.db_patch.stop()
        self.temp_dir.cleanup()

    def test_manifest_starts_empty(self):
        self.assertEqual(manifest.get_manifest_records(), [])

    def test_record_file_creates_manifest_entry(self):
        manifest.record_file(
            "customers.csv",
            "s3://test-bucket/banking-data/raw/customers.csv",
            "abc123",
            2048,
            "UPLOADED_AND_VERIFIED",
        )

        records = manifest.get_manifest_records()

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["file_name"], "customers.csv")
        self.assertEqual(records[0]["checksum_sha256"], "abc123")
        self.assertEqual(records[0]["size_bytes"], 2048)
        self.assertEqual(records[0]["status"], "UPLOADED_AND_VERIFIED")
        self.assertEqual(records[0]["successful_runs"], 1)

    def test_repeat_success_updates_existing_entry(self):
        uri = "s3://test-bucket/banking-data/raw/customers.csv"

        manifest.record_file(
            "customers.csv", uri, "abc123", 2048, "UPLOADED_AND_VERIFIED"
        )
        manifest.record_file(
            "customers.csv", uri, "abc123", 2048, "SKIPPED_IDENTICAL"
        )

        records = manifest.get_manifest_records()

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["status"], "SKIPPED_IDENTICAL")
        self.assertEqual(records[0]["successful_runs"], 2)

    def test_changed_checksum_updates_existing_entry(self):
        uri = "s3://test-bucket/banking-data/raw/customers.csv"

        manifest.record_file(
            "customers.csv", uri, "old-checksum", 2048,
            "UPLOADED_AND_VERIFIED"
        )
        manifest.record_file(
            "customers.csv", uri, "new-checksum", 4096,
            "UPLOADED_AND_VERIFIED"
        )

        records = manifest.get_manifest_records()

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["checksum_sha256"], "new-checksum")
        self.assertEqual(records[0]["size_bytes"], 4096)
        self.assertEqual(records[0]["successful_runs"], 2)

    def test_failed_attempt_records_error_without_counting_success(self):
        uri = "s3://test-bucket/banking-data/raw/customers.csv"

        manifest.record_file(
            "customers.csv", uri, "new-checksum", 4096,
            "FAILED", error="Simulated upload failure"
        )

        records = manifest.get_manifest_records()

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["status"], "FAILED")
        self.assertEqual(records[0]["last_error"], "Simulated upload failure")
        self.assertEqual(records[0]["successful_runs"], 0)
        self.assertIsNone(records[0]["last_successful_checksum"])

    def test_failed_attempt_preserves_last_successful_checksum(self):
        uri = "s3://test-bucket/banking-data/raw/customers.csv"

        manifest.record_file(
            "customers.csv", uri, "good-checksum", 2048,
            "UPLOADED_AND_VERIFIED"
        )
        manifest.record_file(
            "customers.csv", uri, "new-checksum", 4096,
            "FAILED", error="Checksum verification failed"
        )

        records = manifest.get_manifest_records()
        record = records[0]

        self.assertEqual(len(records), 1)
        self.assertEqual(record["status"], "FAILED")
        self.assertEqual(record["checksum_sha256"], "new-checksum")
        self.assertEqual(record["last_successful_checksum"], "good-checksum")
        self.assertEqual(record["last_error"], "Checksum verification failed")
        self.assertEqual(record["successful_runs"], 1)

if __name__ == "__main__":
    unittest.main()

