import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
import demo


class TestRetryPolicy(unittest.TestCase):
    def test_loads_default_policy(self):
        policy = demo.load_retry_policy()

        self.assertEqual(policy["version"], 1)
        self.assertEqual(policy["default"]["max_attempts"], 3)
        self.assertEqual(policy["default"]["initial_delay_seconds"], 1.0)
        self.assertEqual(policy["default"]["backoff_multiplier"], 2.0)

    def test_schema_validation_fails_fast(self):
        policy = demo.load_retry_policy()
        settings = demo.stage_retry_settings(policy, "Schema Validation")

        self.assertEqual(settings["max_attempts"], 1)
        self.assertEqual(settings["retryable_exit_codes"], set())

    def test_s3_policy_has_explicit_retryable_code(self):
        policy = demo.load_retry_policy()
        settings = demo.stage_retry_settings(
            policy, "S3 Raw Data Ingestion"
        )

        self.assertEqual(settings["max_attempts"], 3)
        self.assertEqual(settings["retryable_exit_codes"], {75})

    def test_unknown_stage_uses_defaults(self):
        policy = demo.load_retry_policy()
        settings = demo.stage_retry_settings(policy, "New Stage")

        self.assertEqual(settings["max_attempts"], 3)
        self.assertEqual(settings["retryable_exit_codes"], {75})

    def test_rejects_invalid_attempt_count(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "retry.json"
            config_path.write_text(
                json.dumps({
                    "version": 1,
                    "default": {
                        "max_attempts": 0,
                        "initial_delay_seconds": 1,
                        "backoff_multiplier": 2,
                        "retryable_exit_codes": [75],
                    },
                    "stages": {},
                }),
                encoding="utf-8",
            )

            with patch.object(demo, "RETRY_POLICY_FILE", config_path):
                with self.assertRaises(ValueError):
                    demo.load_retry_policy()

    def test_rejects_invalid_json(self):
        with tempfile.TemporaryDirectory() as directory:
            config_path = Path(directory) / "retry.json"
            config_path.write_text("{invalid", encoding="utf-8")

            with patch.object(demo, "RETRY_POLICY_FILE", config_path):
                with self.assertRaises(ValueError):
                    demo.load_retry_policy()


if __name__ == "__main__":
    unittest.main()
