"""Tests for the Airflow preflight and pipeline-evidence contract."""
import ast
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from airflow_pipeline_contract import (
    EXPECTED_STAGES, REQUIRED_ARTIFACTS, S3_AUDIT_ARTIFACT,
    build_pipeline_command, preflight_project, validate_pipeline_evidence,
)


def fake_module_finder(_name):
    return object()


def create_preflight_project(root):
    paths = (
        "data/raw/customers.csv", "data/raw/transactions.csv",
        "data/raw/transactions_incremental.csv",
        "src/demo.py", "src/schema_validation.py", "src/etl_pipeline.py",
        "src/incremental_pipeline.py", "src/quality_report.py",
        "src/anomaly_detection.py", "src/data_quality_investigator.py",
        "src/export_analytics.py", "src/spark_pipeline.py",
        "src/compare_engines.py", "src/s3_ingestion.py",
        "config/data_lineage.json", "config/retry_policy.json",
        "config/validation_rules.json",
    )
    for item in paths:
        path = root / item
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture", encoding="utf-8")


def write_s3_audit(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    records = [
        {"file": name, "mode": "upload", "status": "UPLOADED_AND_VERIFIED"}
        for name in ("customers.csv", "transactions.csv", "transactions_incremental.csv")
    ]
    path.write_text("".join(json.dumps(r) + "\n" for r in records), encoding="utf-8")


def create_evidence_project(root, upload_s3=False):
    for item in REQUIRED_ARTIFACTS:
        path = root / item
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("fixture", encoding="utf-8")
    (root / "data/processed/engine_reconciliation_report.json").write_text(
        json.dumps({"status": "PASS"}), encoding="utf-8"
    )
    if upload_s3:
        write_s3_audit(root / S3_AUDIT_ARTIFACT)
    statuses = {
        name: ("PASS" if name != "S3 Raw Data Ingestion" or upload_s3 else "SKIPPED")
        for name in EXPECTED_STAGES
    }
    stages = [{"stage": name, "status": status} for name, status in statuses.items()]
    report = {
        "pipeline": "AI-Enhanced Banking Data Platform",
        "run_id": "test-run-001",
        "status": "PASS",
        "failure_stage": None,
        "stage_count": len(stages),
        "passed_stages": sum(x == "PASS" for x in statuses.values()),
        "failed_stages": 0,
        "skipped_stages": sum(x == "SKIPPED" for x in statuses.values()),
        "stages": stages,
    }
    report_path = root / "data/processed/pipeline_run_report.json"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report), encoding="utf-8")
    return report_path, report


class TestPipelineCommand(unittest.TestCase):
    def test_s3_is_not_enabled_by_default(self):
        self.assertEqual(
            build_pipeline_command("python", "src/demo.py"),
            ["python", "src/demo.py", "--include-spark"],
        )

    def test_s3_flag_is_added_only_when_requested(self):
        self.assertEqual(
            build_pipeline_command("python", "src/demo.py", upload_s3=True),
            ["python", "src/demo.py", "--include-spark", "--upload-s3"],
        )


class TestAirflowPreflight(unittest.TestCase):
    def test_local_run_does_not_need_aws(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            create_preflight_project(root)
            result = preflight_project(root, environ={}, module_finder=fake_module_finder)
            self.assertEqual(result["status"], "PASS")
            self.assertFalse(result["s3_upload_requested"])

    def test_missing_input_fails_preflight(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            create_preflight_project(root)
            (root / "data/raw/transactions.csv").unlink()
            with self.assertRaisesRegex(ValueError, "transactions.csv"):
                preflight_project(root, environ={}, module_finder=fake_module_finder)

    def test_s3_requires_bucket_and_credentials_without_exposing_values(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            create_preflight_project(root)
            with self.assertRaisesRegex(ValueError, "BANKING_S3_BUCKET"):
                preflight_project(
                    root, upload_s3=True,
                    environ={"AWS_SECRET_ACCESS_KEY": "do-not-print-this"},
                    module_finder=fake_module_finder,
                )

    def test_s3_preflight_passes_with_bucket_and_worker_credentials(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            create_preflight_project(root)
            result = preflight_project(
                root, upload_s3=True,
                environ={
                    "BANKING_S3_BUCKET": "test-bucket",
                    "AWS_ACCESS_KEY_ID": "test-access-key",
                    "AWS_SECRET_ACCESS_KEY": "test-secret-key",
                },
                module_finder=fake_module_finder,
            )
            self.assertEqual(result["status"], "PASS")
            self.assertIn("boto3", result["python_modules_checked"])
            self.assertIn("src/s3_ingestion.py", result["scripts_checked"])


class TestPipelineEvidenceValidation(unittest.TestCase):
    def test_local_run_passes_with_s3_skipped(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report_path, _ = create_evidence_project(root)
            result = validate_pipeline_evidence(root, report_path)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["run_id"], "test-run-001")
            self.assertEqual(result["skipped_stages"], 1)
            self.assertEqual(result["cross_engine_reconciliation"], "PASS")

    def test_s3_requires_fresh_verified_audit_for_all_inputs(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report_path, _ = create_evidence_project(root, upload_s3=True)
            result = validate_pipeline_evidence(root, report_path, upload_s3=True)
            self.assertEqual(result["status"], "PASS")
            self.assertEqual(result["s3_audit"]["records_added"], 3)

    def test_old_s3_audit_is_not_current_run_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report_path, _ = create_evidence_project(root, upload_s3=True)
            prior_size = (root / S3_AUDIT_ARTIFACT).stat().st_size
            with self.assertRaisesRegex(ValueError, "did not grow"):
                validate_pipeline_evidence(
                    root, report_path, upload_s3=True,
                    s3_audit_size_before=prior_size,
                )

    def test_failed_required_stage_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report_path, report = create_evidence_project(root)
            report["stages"][1]["status"] = "FAIL"
            report["passed_stages"] -= 1
            report["failed_stages"] = 1
            report_path.write_text(json.dumps(report), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Main ETL"):
                validate_pipeline_evidence(root, report_path)

    def test_missing_stage_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report_path, report = create_evidence_project(root)
            report["stages"] = [x for x in report["stages"] if x["stage"] != "Quality Report"]
            report["stage_count"] -= 1
            report["passed_stages"] -= 1
            report_path.write_text(json.dumps(report), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Quality Report"):
                validate_pipeline_evidence(root, report_path)

    def test_missing_artifact_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report_path, _ = create_evidence_project(root)
            (root / "data/processed/transactions.parquet").unlink()
            with self.assertRaisesRegex(ValueError, "transactions.parquet"):
                validate_pipeline_evidence(root, report_path)

    def test_failed_reconciliation_is_rejected(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report_path, _ = create_evidence_project(root)
            (root / "data/processed/engine_reconciliation_report.json").write_text(
                json.dumps({"status": "FAIL"}), encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "does not show PASS"):
                validate_pipeline_evidence(root, report_path)

    def test_s3_parameter_must_match_report_stage(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report_path, report = create_evidence_project(root)
            report["stages"][-1]["status"] = "PASS"
            report["passed_stages"] += 1
            report["skipped_stages"] -= 1
            report_path.write_text(json.dumps(report), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "S3 Raw Data Ingestion"):
                validate_pipeline_evidence(root, report_path, upload_s3=False)

    def test_stage_counters_must_match_entries(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            report_path, report = create_evidence_project(root)
            report["passed_stages"] += 1
            report_path.write_text(json.dumps(report), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "passed_stages"):
                validate_pipeline_evidence(root, report_path)


class TestAirflowDagSource(unittest.TestCase):
    def test_dag_parses_and_declares_three_tasks(self):
        path = ROOT / "orchestration/airflow/dags/banking_data_platform_pipeline.py"
        tree = ast.parse(path.read_text(encoding="utf-8-sig"))
        task_ids = {
            node.value.value for node in ast.walk(tree)
            if isinstance(node, ast.keyword) and node.arg == "task_id"
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        }
        self.assertEqual(task_ids, {
            "preflight_pipeline_inputs", "run_banking_pipeline",
            "validate_pipeline_evidence",
        })


if __name__ == "__main__":
    unittest.main(verbosity=2)
