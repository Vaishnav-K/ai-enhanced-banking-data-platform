import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
import demo


class TestQualityGateWorkflow(unittest.TestCase):

    @patch("demo.run_command")
    def test_failed_quality_report_stops_later_stages(self, run_command):
        # Schema, Main ETL and Incremental ETL pass; Quality Report fails.
        run_command.side_effect = [0, 0, 0, 1]

        with patch("builtins.print"):
            result = demo.main([])

        self.assertEqual(result, 1)
        self.assertEqual(run_command.call_count, 4)

        scripts = [
            str(args[0][1])
            for args, kwargs in run_command.call_args_list
        ]
        self.assertTrue(scripts[0].endswith("schema_validation.py"))
        self.assertTrue(scripts[1].endswith("etl_pipeline.py"))
        self.assertTrue(scripts[2].endswith("incremental_pipeline.py"))
        self.assertTrue(scripts[3].endswith("quality_report.py"))

    @patch("demo.run_command")
    def test_successful_workflow_reaches_tests(self, run_command):
        run_command.return_value = 0

        with patch("builtins.print"):
            result = demo.main([])

        self.assertEqual(result, 0)
        commands = [args[0] for args, kwargs in run_command.call_args_list]

        # Six pipeline stages plus one automated test command.
        self.assertEqual(len(commands), 7)
        self.assertTrue(commands[0][1].endswith("schema_validation.py"))
        self.assertIn("unittest", commands[6])

    @patch("demo.run_command")
    def test_s3_is_not_run_when_tests_fail(self, run_command):
        # Six pipeline stages pass, but automated tests fail.
        run_command.side_effect = [0, 0, 0, 0, 0, 0, 1]

        with patch("builtins.print"):
            result = demo.main(["--upload-s3"])

        self.assertEqual(result, 1)
        self.assertEqual(run_command.call_count, 7)


if __name__ == "__main__":
    unittest.main(verbosity=2)
