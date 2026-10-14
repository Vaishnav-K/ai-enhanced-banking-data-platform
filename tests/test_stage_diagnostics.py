import sys
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

import demo


class TestStageDiagnostics(unittest.TestCase):
    def test_captures_stderr_and_stdout(self):
        result = {
            "exit_code": 1,
            "stdout": "validation started",
            "stderr": "ValueError: invalid amount",
        }
        summary = demo.summarize_process_output(result)
        self.assertIn("STDERR:", summary)
        self.assertIn("ValueError: invalid amount", summary)
        self.assertIn("validation started", summary)

    def test_truncates_large_output(self):
        result = {"stdout": "x" * 5000, "stderr": ""}
        summary = demo.summarize_process_output(result, max_chars=100)
        self.assertLessEqual(len(summary), 130)
        self.assertIn("output truncated", summary)

    def test_stage_records_actual_failure_output(self):
        with patch.object(
            demo,
            "run_command",
            return_value={
                "exit_code": 1,
                "stdout": "",
                "stderr": "ValueError: bad input",
            },
        ):
            result = demo.run_stage(
                "Diagnostic Test",
                ["python", "-c", "pass"],
                max_attempts=1,
            )
        self.assertEqual(result["status"], "FAIL")
        self.assertIn("ValueError: bad input", result["error"])
        self.assertEqual(result["attempts"][0]["error"], result["error"])

    def test_success_has_no_error(self):
        with patch.object(
            demo,
            "run_command",
            return_value={"exit_code": 0, "stdout": "ok", "stderr": ""},
        ):
            result = demo.run_stage(
                "Successful Test",
                ["python", "-c", "pass"],
                max_attempts=1,
            )
        self.assertEqual(result["status"], "PASS")
        self.assertIsNone(result["error"])


if __name__ == "__main__":
    unittest.main()
