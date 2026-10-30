# Airflow orchestration runbook

## What the DAG does

The DAG `banking_data_platform_pipeline` coordinates the existing `src/demo.py` workflow. It does not reimplement the ETL stages.

The task chain is:

```text
preflight_pipeline_inputs
        |
        v
run_banking_pipeline
        |
        v
validate_pipeline_evidence
```

### 1. Preflight

The preflight task checks the baseline customer and transaction CSVs, stage scripts, configuration files, DuckDB, and PySpark. It fails before starting the longer workflow when a required file or dependency is missing.

When `upload_s3=true`, preflight also requires the S3 ingestion script, boto3, `BANKING_S3_BUCKET`, and both worker AWS credential variables. Secret values are checked for presence only and are not printed.

### 2. Pipeline execution

The run task removes the previous `pipeline_run_report.json` so an earlier successful run cannot be mistaken for the current run. It invokes:

```text
python src/demo.py --include-spark
```

S3 is optional. When the DAG parameter `upload_s3` is set to `true`, the runner adds `--upload-s3`. The default is `false`, allowing the local Airflow workflow to run without AWS access.

The existing demo runner continues to own stage-level bounded retries, run IDs, stage-attempt history, lineage observations, and the final pipeline report. The Airflow task itself has no whole-pipeline retry because replaying a partially completed end-to-end run can repeat work and obscure the original failure.

### 3. Evidence validation

The final task independently validates:

- The report exists, is valid JSON, has overall status `PASS`, and contains a run ID.
- Required stages are present exactly once and have the expected statuses.
- Stage counters agree with the actual stage entries.
- Main ETL, incremental ETL, quarantine, analytics, Spark, and reconciliation artifacts exist and are non-empty.
- The cross-engine reconciliation JSON explicitly reports `PASS`.
- If S3 upload was requested, only new audit records appended during this run are considered; they must show verified upload or checksum-match status for all three raw CSV inputs.

The task returns a compact validation summary to Airflow XCom. A pipeline report that says `PASS` is not enough if expected output files are missing or the reconciliation report is failed.

## Triggering the DAG

1. Start Docker Compose from `orchestration/airflow`. The worker bind-mounts the project `data/` directory using `BANKING_DATA_DIR` (default `../../data` relative to this Compose folder), so the tracked raw CSV inputs remain visible inside the container and outputs remain in the local checkout.
2. Open `http://localhost:8080` by default. Set `AIRFLOW_API_PORT` before starting Compose to use a different host port when another Airflow stack is already running.
3. Unpause `banking_data_platform_pipeline` if required.
4. Trigger a manual run.
5. Leave `upload_s3=false` for a local-only run, or set it to `true` for an authenticated raw-data upload.

For S3, set `BANKING_S3_BUCKET`, `AWS_ACCESS_KEY_ID`, and `AWS_SECRET_ACCESS_KEY` before starting Compose. The Compose file passes these settings only to the worker. Never commit the credentials or print them in logs.

## Operational boundaries

- `max_active_runs=1` because the pipeline shares local output paths and lineage storage.
- `src/demo.py` owns bounded stage retries; Airflow does not retry the entire partially completed pipeline.
- The S3 path is opt-in; a baseline local run does not require AWS.
- The DAG is a local development orchestration example, not a production Airflow deployment.
- All data is synthetic. A green DAG run is development evidence, not a production SLA.
- Stop local services with `docker compose down` without `-v` to preserve named volumes.
