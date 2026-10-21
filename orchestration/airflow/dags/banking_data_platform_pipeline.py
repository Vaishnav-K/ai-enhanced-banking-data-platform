import json
import subprocess
import sys
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.python import PythonOperator

PROJECT_ROOT = Path("/opt/banking-platform")
PIPELINE_SCRIPT = PROJECT_ROOT / "src" / "demo.py"
REPORT_PATH = PROJECT_ROOT / "data" / "processed" / "pipeline_run_report.json"


def run_banking_pipeline():
    # Prevent an old report from being mistaken for this run's result.
    REPORT_PATH.unlink(missing_ok=True)

    result = subprocess.run(
[sys.executable, str(PIPELINE_SCRIPT), "--include-spark", "--upload-s3"],    cwd=str(PROJECT_ROOT),
    check=False,
)

    if result.returncode != 0:
        raise AirflowException(
            f"Banking pipeline process exited with code {result.returncode}"
        )

    if not REPORT_PATH.exists():
        raise AirflowException(f"Pipeline report not found: {REPORT_PATH}")

    try:
        report = json.loads(REPORT_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AirflowException(f"Could not read pipeline report: {exc}") from exc

    if report.get("status") != "PASS":
        raise AirflowException(
            f"Banking pipeline reported status {report.get('status')!r}; "
            f"failure stage: {report.get('failure_stage', 'Not specified')}"
        )

    run_id = report.get("run_id")
    if not run_id:
        raise AirflowException("Pipeline report has no run_id")

    print(
        f"Pipeline completed successfully. "
        f"Run ID: {run_id}; "
        f"Finished: {report.get('run_finished_at_utc', 'Not reported')}"
    )


with DAG(
    dag_id="banking_data_platform_pipeline",
    description="Run and validate the existing banking data platform pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["banking", "data-engineering", "etl"],
) as dag:
    run_pipeline = PythonOperator(
        task_id="run_and_validate_pipeline",
        python_callable=run_banking_pipeline,
    )

