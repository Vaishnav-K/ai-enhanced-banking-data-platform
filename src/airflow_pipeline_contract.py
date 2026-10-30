"""Airflow preflight and post-run evidence validation (Airflow-independent)."""
import importlib.util
import json
import os
from pathlib import Path

REQUIRED_INPUTS = (
    "data/raw/customers.csv",
    "data/raw/transactions.csv",
    "data/raw/transactions_incremental.csv",
)
REQUIRED_SCRIPTS = (
    "src/demo.py", "src/schema_validation.py", "src/etl_pipeline.py",
    "src/incremental_pipeline.py", "src/quality_report.py",
    "src/anomaly_detection.py", "src/data_quality_investigator.py",
    "src/export_analytics.py", "src/spark_pipeline.py", "src/compare_engines.py",
)
S3_SCRIPT = "src/s3_ingestion.py"
REQUIRED_CONFIGS = (
    "config/data_lineage.json", "config/retry_policy.json", "config/validation_rules.json",
)
REQUIRED_MODULES = ("duckdb", "pyspark")
EXPECTED_STAGES = (
    "Schema Validation", "Main ETL", "Incremental ETL", "Quality Report",
    "Anomaly Detection", "Data Quality Investigation", "Analytics Exports",
    "Automated Tests", "Spark Processing", "Cross-Engine Reconciliation",
    "S3 Raw Data Ingestion",
)
REQUIRED_ARTIFACTS = (
    "data/processed/customers.parquet",
    "data/processed/transactions.parquet",
    "data/processed/transactions_incremental.parquet",
    "data/processed/schema_validation_report.json",
    "data/processed/quality_report.json",
    "data/processed/transaction_anomalies.json",
    "data/processed/quality_investigation_report.json",
    "data/processed/customer_transaction_summary.csv",
    "data/processed/transaction_status_summary.csv",
    "data/processed/daily_transaction_summary.csv",
    "data/processed/engine_reconciliation_report.json",
    "data/processed/pipeline_run_report.json",
    "data/quarantine/transactions.parquet",
    "data/quarantine/transactions_incremental.parquet",
    "data/spark_processed/valid_transactions/data.csv",
    "data/spark_processed/quarantined_transactions/data.csv",
    "data/spark_processed/transaction_analytics/data.csv",
)
S3_AUDIT_ARTIFACT = "data/processed/s3_ingestion_audit.jsonl"
S3_VERIFIED_STATUSES = {"UPLOADED_AND_VERIFIED", "SKIPPED_IDENTICAL"}


def build_pipeline_command(python_executable, pipeline_script, upload_s3=False):
    """S3 upload is opt-in and never silently enabled."""
    command = [str(python_executable), str(pipeline_script), "--include-spark"]
    if upload_s3:
        command.append("--upload-s3")
    return command


def preflight_project(project_root, upload_s3=False, environ=None, module_finder=None):
    """Check required files, dependencies, and optional S3 configuration."""
    root = Path(project_root).resolve()
    env = os.environ if environ is None else environ
    finder = importlib.util.find_spec if module_finder is None else module_finder
    scripts = list(REQUIRED_SCRIPTS)
    modules = list(REQUIRED_MODULES)
    if upload_s3:
        scripts.append(S3_SCRIPT)
        modules.append("boto3")

    paths = list(REQUIRED_INPUTS) + scripts + list(REQUIRED_CONFIGS)
    missing_paths = [p for p in paths if not (root / p).is_file()]
    missing_modules = []
    for name in modules:
        try:
            found = finder(name)
        except (ImportError, ModuleNotFoundError, ValueError):
            found = None
        if found is None:
            missing_modules.append(name)

    missing_settings = []
    if upload_s3:
        if not str(env.get("BANKING_S3_BUCKET", "")).strip():
            missing_settings.append("BANKING_S3_BUCKET")
        if not (
            str(env.get("AWS_ACCESS_KEY_ID", "")).strip()
            and str(env.get("AWS_SECRET_ACCESS_KEY", "")).strip()
        ):
            missing_settings.append(
                "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY (worker credentials)"
            )

    problems = []
    if missing_paths:
        problems.append("missing files: " + ", ".join(missing_paths))
    if missing_modules:
        problems.append("missing Python modules: " + ", ".join(missing_modules))
    if missing_settings:
        problems.append("missing environment settings: " + ", ".join(missing_settings))
    if problems:
        raise ValueError("Airflow preflight failed; " + "; ".join(problems))

    return {
        "status": "PASS",
        "project_root": str(root),
        "input_files_checked": list(REQUIRED_INPUTS),
        "scripts_checked": scripts,
        "configuration_files_checked": list(REQUIRED_CONFIGS),
        "python_modules_checked": modules,
        "s3_upload_requested": bool(upload_s3),
    }


def _validate_s3_audit(root, size_before):
    """Validate new, verified audit records appended by this run only."""
    path = root / S3_AUDIT_ARTIFACT
    if not path.is_file():
        raise ValueError(f"S3 audit artifact not found: {path}")
    size_after = path.stat().st_size
    if size_after <= size_before:
        raise ValueError("S3 audit log did not grow during this run; no fresh upload evidence was found.")
    try:
        with path.open("rb") as handle:
            handle.seek(size_before)
            content = handle.read().decode("utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ValueError(f"Could not read new S3 audit records: {exc}") from exc

    records = []
    for index, line in enumerate(content.splitlines(), start=1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"New S3 audit record {index} is invalid JSON: {exc}") from exc
        if not isinstance(record, dict):
            raise ValueError(f"New S3 audit record {index} must be a JSON object.")
        if record.get("mode") != "upload":
            raise ValueError("New S3 audit record was not produced by upload mode.")
        if record.get("status") not in S3_VERIFIED_STATUSES:
            raise ValueError(
                f"S3 audit record for {record.get('file', 'unknown file')} "
                f"has non-verified status {record.get('status')!r}."
            )
        records.append(record)

    expected = {Path(p).name for p in REQUIRED_INPUTS}
    observed = {str(record.get("file", "")) for record in records}
    missing = sorted(expected - observed)
    if missing:
        raise ValueError("Fresh S3 audit evidence is missing raw input files: " + ", ".join(missing))
    return {
        "path": S3_AUDIT_ARTIFACT,
        "records_added": len(records),
        "files_verified": sorted(expected),
        "bytes_before_run": size_before,
        "bytes_after_run": size_after,
    }


def validate_pipeline_evidence(
    project_root, report_path=None, upload_s3=False, s3_audit_size_before=0
):
    """Validate report semantics, stage outcomes, reconciliation, and output artifacts."""
    root = Path(project_root).resolve()
    report_file = Path(report_path).resolve() if report_path else (
        root / "data" / "processed" / "pipeline_run_report.json"
    )
    if not report_file.is_file():
        raise ValueError(f"Pipeline report not found: {report_file}")
    try:
        report = json.loads(report_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Pipeline report could not be read as JSON: {exc}") from exc

    if not isinstance(report, dict):
        raise ValueError("Pipeline report must contain a JSON object.")
    if report.get("status") != "PASS":
        raise ValueError(
            f"Pipeline status is {report.get('status')!r}; "
            f"failure stage: {report.get('failure_stage') or 'Not specified'}"
        )
    run_id = report.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("Pipeline report does not contain a valid run_id.")
    stages = report.get("stages")
    if not isinstance(stages, list):
        raise ValueError("Pipeline report stages must be a list.")

    statuses = {}
    for stage in stages:
        if not isinstance(stage, dict):
            raise ValueError("Pipeline report contains a malformed stage entry.")
        name, status = stage.get("stage"), stage.get("status")
        if not isinstance(name, str) or not name:
            raise ValueError("Pipeline report contains a stage without a name.")
        if name in statuses:
            raise ValueError(f"Pipeline report contains duplicate stage: {name}")
        if status not in ("PASS", "FAIL", "SKIPPED"):
            raise ValueError(f"Pipeline stage {name!r} has invalid status {status!r}.")
        statuses[name] = status

    missing_stages = [name for name in EXPECTED_STAGES if name not in statuses]
    if missing_stages:
        raise ValueError("Pipeline report is missing stages: " + ", ".join(missing_stages))
    for name in EXPECTED_STAGES[:-1]:
        if statuses[name] != "PASS":
            raise ValueError(f"Required pipeline stage {name!r} reported {statuses[name]!r}, expected 'PASS'.")
    expected_s3 = "PASS" if upload_s3 else "SKIPPED"
    if statuses["S3 Raw Data Ingestion"] != expected_s3:
        raise ValueError(
            f"S3 Raw Data Ingestion reported {statuses['S3 Raw Data Ingestion']!r}; "
            f"expected {expected_s3!r} for upload_s3={bool(upload_s3)}."
        )

    counts = {
        "passed_stages": sum(v == "PASS" for v in statuses.values()),
        "failed_stages": sum(v == "FAIL" for v in statuses.values()),
        "skipped_stages": sum(v == "SKIPPED" for v in statuses.values()),
    }
    if report.get("stage_count") != len(stages):
        raise ValueError("Pipeline report stage_count does not match its stages list.")
    for field, expected in counts.items():
        if report.get(field) != expected:
            raise ValueError(f"Pipeline report {field}={report.get(field)!r} does not match observed value {expected}.")
    if counts["failed_stages"]:
        raise ValueError("Pipeline report has one or more failed stages.")

    missing_artifacts = [
        p for p in REQUIRED_ARTIFACTS
        if not (root / p).is_file() or (root / p).stat().st_size == 0
    ]
    if missing_artifacts:
        raise ValueError(
            "Pipeline report passed but required artifacts are missing or empty: "
            + ", ".join(missing_artifacts)
        )

    reconciliation_path = root / "data/processed/engine_reconciliation_report.json"
    try:
        reconciliation = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cross-engine reconciliation report is invalid: {exc}") from exc
    if not isinstance(reconciliation, dict) or reconciliation.get("status") != "PASS":
        raise ValueError("Cross-engine reconciliation report does not show PASS.")

    s3_evidence = _validate_s3_audit(root, int(s3_audit_size_before)) if upload_s3 else None
    return {
        "status": "PASS",
        "run_id": run_id,
        "stage_count": len(stages),
        **counts,
        "s3_upload_requested": bool(upload_s3),
        "validated_artifacts": list(REQUIRED_ARTIFACTS),
        "cross_engine_reconciliation": "PASS",
        "s3_audit": s3_evidence,
        "stage_statuses": statuses,
    }
