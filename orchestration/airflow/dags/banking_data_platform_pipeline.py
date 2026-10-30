"""Airflow orchestration with preflight and post-run evidence gates."""
import json
import subprocess
import sys
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Param

PROJECT_ROOT = Path("/opt/banking-platform")
PIPELINE_SCRIPT = PROJECT_ROOT / "src" / "demo.py"
REPORT_PATH = PROJECT_ROOT / "data" / "processed" / "pipeline_run_report.json"
S3_AUDIT_PATH = PROJECT_ROOT / "data" / "processed" / "s3_ingestion_audit.jsonl"
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from airflow_pipeline_contract import (  # noqa: E402
    build_pipeline_command,
    preflight_project,
    validate_pipeline_evidence,
)


def preflight_banking_pipeline(**context):
    upload_s3 = bool(context["params"]["upload_s3"])
    try:
        result = preflight_project(PROJECT_ROOT, upload_s3=upload_s3)
    except ValueError as exc:
        raise AirflowException(str(exc)) from exc
    print("Banking platform preflight passed:")
    print(json.dumps(result, indent=2))
    return result


def run_banking_pipeline(**context):
    upload_s3 = bool(context["params"]["upload_s3"])
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.unlink(missing_ok=True)

    # Snapshot the append-only audit file so an earlier S3 success cannot pass
    # the evidence gate for this execution.
    audit_size_before = (
        S3_AUDIT_PATH.stat().st_size
        if upload_s3 and S3_AUDIT_PATH.is_file()
        else 0
    )
    command = build_pipeline_command(sys.executable, PIPELINE_SCRIPT, upload_s3=upload_s3)
    print(f"Launching banking pipeline (S3 upload requested: {upload_s3}).")
    result = subprocess.run(command, cwd=str(PROJECT_ROOT), check=False)
    if result.returncode != 0:
        raise AirflowException(f"Banking pipeline process exited with code {result.returncode}")
    return {
        "exit_code": result.returncode,
        "s3_upload_requested": upload_s3,
        "s3_audit_size_before": audit_size_before,
    }


def validate_banking_pipeline_evidence(**context):
    upload_s3 = bool(context["params"]["upload_s3"])
    run_result = context["ti"].xcom_pull(task_ids="run_banking_pipeline") or {}
    audit_size_before = run_result.get("s3_audit_size_before", 0)
    try:
        summary = validate_pipeline_evidence(
            PROJECT_ROOT,
            REPORT_PATH,
            upload_s3=upload_s3,
            s3_audit_size_before=audit_size_before,
        )
    except ValueError as exc:
        raise AirflowException(str(exc)) from exc
    print("Banking pipeline evidence validation passed:")
    print(json.dumps(summary, indent=2))
    return summary


with DAG(
    dag_id="banking_data_platform_pipeline",
    description="Run the banking pipeline with preflight and evidence validation",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "upload_s3": Param(
            False,
            type="boolean",
            title="Upload raw data to S3",
            description=(
                "Enable optional S3 raw-file upload after pipeline stages and automated "
                "tests pass. Requires BANKING_S3_BUCKET and worker AWS credentials."
            ),
        ),
    },
    default_args={
        "owner": "data-platform",
        # The demo runner owns bounded stage retries. Do not replay the entire
        # pipeline at the Airflow task level after a partial execution.
        "retries": 0,
    },
    tags=["banking", "data-engineering", "etl", "evidence-gated"],
) as dag:
    preflight = PythonOperator(
        task_id="preflight_pipeline_inputs",
        python_callable=preflight_banking_pipeline,
    )
    run_pipeline = PythonOperator(
        task_id="run_banking_pipeline",
        python_callable=run_banking_pipeline,
    )
    validate_evidence = PythonOperator(
        task_id="validate_pipeline_evidence",
        python_callable=validate_banking_pipeline_evidence,
    )
    preflight >> run_pipeline >> validate_evidence
