import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))
import demo


class TestStageRetries(unittest.TestCase):
    def test_recovers_after_retryable_exit_code(self):
        with (
            patch.object(demo, "run_command", side_effect=[75, 0]) as run,
            patch.object(demo.time, "sleep") as sleep,
        ):
            result = demo.run_stage(
                "Transient Failure", ["fake"],
                max_attempts=3,
                retry_delay_seconds=1,
                retryable_exit_codes={75},
            )

        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["attempt_count"], 2)
        self.assertEqual(run.call_count, 2)
        sleep.assert_called_once_with(1)

    def test_permanent_exit_code_fails_fast(self):
        with (
            patch.object(demo, "run_command", return_value=1) as run,
            patch.object(demo.time, "sleep") as sleep,
        ):
            result = demo.run_stage(
                "Validation Failure", ["fake"], max_attempts=3
            )

        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["attempt_count"], 1)
        run.assert_called_once()
        sleep.assert_not_called()

    def test_os_error_is_retried(self):
        with (
            patch.object(
                demo, "run_command",
                side_effect=[OSError("temporary error"), 0],
            ),
            patch.object(demo.time, "sleep"),
        ):
            result = demo.run_stage(
                "OS Error Recovery", ["fake"],
                max_attempts=2, retry_delay_seconds=0,
            )

        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["attempts"][0]["exit_code"], -1)
        self.assertTrue(result["attempts"][0]["retryable"])

    def test_stops_after_retry_limit(self):
        with (
            patch.object(demo, "run_command", side_effect=[75, 75, 75]) as run,
            patch.object(demo.time, "sleep") as sleep,
        ):
            result = demo.run_stage(
                "Persistent Transient Failure", ["fake"],
                max_attempts=3,
                retry_delay_seconds=1,
                retryable_exit_codes={75},
            )

        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["attempt_count"], 3)
        self.assertEqual(run.call_count, 3)
        self.assertEqual(
            [call.args[0] for call in sleep.call_args_list], [1, 2]
        )

    def test_rejects_invalid_attempt_limit(self):
        with self.assertRaises(ValueError):
            demo.run_stage("Invalid", [], max_attempts=0)

    def test_rejects_negative_delay(self):
        with self.assertRaises(ValueError):
            demo.run_stage(
                "Invalid", [], max_attempts=1, retry_delay_seconds=-1
            )


if __name__ == "__main__":
    unittest.main()
