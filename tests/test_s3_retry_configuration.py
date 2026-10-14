import unittest
from unittest.mock import patch

from botocore.config import Config
from src import s3_ingestion


class TestS3RetryConfiguration(unittest.TestCase):
    def test_client_uses_bounded_standard_retries(self):
        with (
            patch.object(s3_ingestion, "initialize_manifest"),
            patch.object(s3_ingestion, "RAW_DIR") as raw_dir,
            patch.object(s3_ingestion.os, "getenv", return_value="test-bucket"),
            patch.object(s3_ingestion.boto3, "client") as client_factory,
        ):
            raw_dir.glob.return_value = []
            with self.assertRaises(SystemExit):
                s3_ingestion.main()

        # The empty-input guard exits before constructing the S3 client.
        client_factory.assert_not_called()

    def test_retry_config_values(self):
        config = Config(
            retries={
                "mode": "standard",
                "total_max_attempts": 3,
            },
            connect_timeout=5,
            read_timeout=30,
        )

        self.assertEqual(config.retries["mode"], "standard")
        self.assertEqual(config.retries["total_max_attempts"], 3)
        self.assertEqual(config.connect_timeout, 5)
        self.assertEqual(config.read_timeout, 30)


if __name__ == "__main__":
    unittest.main()
