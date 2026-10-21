# Demo Walkthrough

## Objective

Demonstrate a synthetic banking data platform with batch and incremental ETL, data quality validation, anomaly screening, runtime reconciliation, observability, Airflow orchestration, Spark processing, and authenticated S3 ingestion.

## 1. Run the Airflow pipeline

### Prerequisites
- Docker Desktop running.
- AWS CLI configured with a default profile authorized to upload to the target S3 bucket.
- The project repository available locally.

From the repository root, open the Airflow folder:

```powershell
Set-Location .\orchestration\airflow
```

Load the default AWS credentials into the current PowerShell session without printing their values:

```powershell
$awsCreds = Get-Content "$env:USERPROFILE\.aws\credentials" -Raw
$section = [regex]::Match($awsCreds, '(?ms)^\[default\]\s*(.*?)(?=^\[|\z)').Groups[1].Value
$accessKey = [regex]::Match($section, '(?m)^\s*aws_access_key_id\s*=\s*(\S+)').Groups[1].Value
$secretKey = [regex]::Match($section, '(?m)^\s*aws_secret_access_key\s*=\s*(\S+)').Groups[1].Value
if (-not $accessKey -or -not $secretKey) { throw "Default AWS credentials not found." }

$env:AWS_ACCESS_KEY_ID = $accessKey
$env:AWS_SECRET_ACCESS_KEY = $secretKey
$env:AWS_DEFAULT_REGION = "ap-southeast-2"

docker compose config --quiet
docker compose up --build -d
```

Open `http://localhost:8080`, sign in using the local Airflow account configured on your machine, unpause `banking_data_platform_pipeline` if necessary, and trigger a manual run.

## 2. Validate the run

Open the DAG run and inspect task `run_and_validate_pipeline` → **Logs**.

The DAG:
1. Removes the previous `pipeline_run_report.json` so an old result cannot be mistaken for a new run.
2. Executes `src/demo.py` with `--include-spark` and `--upload-s3`.
3. Fails if the subprocess returns a non-zero exit code.
4. Requires a newly generated run report.
5. Parses the report and requires `status: PASS`.

A successful development run was observed with overall status `PASS` and the `S3 Raw Data Ingestion` stage reporting `PASS`, exit code `0`, no error, and one attempt. The stage took approximately 2.479 seconds in that recorded run. Timings vary by machine and run; this is demonstration evidence, not a performance guarantee.

## 3. Verify the S3 objects

Use the bucket configured for your own environment:

```powershell
aws s3 ls s3://<your-bucket-name>/banking-data/raw/ --recursive
```

Expected object names for this project are:
- `banking-data/raw/customers.csv`
- `banking-data/raw/transactions.csv`
- `banking-data/raw/transactions_incremental.csv`

Object timestamps may reflect earlier uploads if unchanged files were skipped by checksum-based idempotency. To prove a particular run uploaded or verified objects, inspect the S3 ingestion stage in that run's `data/processed/pipeline_run_report.json` and associated ingestion audit report.

## 4. Inspect the pipeline report

The worker stores processed outputs and reports in the persistent `banking-data-volume` mounted at `/opt/banking-platform/data`. To inspect the current run report from the worker:

```powershell
docker compose exec airflow-worker python -c "import json; p='/opt/banking-platform/data/processed/pipeline_run_report.json'; r=json.load(open(p,encoding='utf-8')); print('Run ID:',r.get('run_id')); print('Status:',r.get('status')); print(json.dumps([s for s in r.get('stages',[]) if 's3' in str(s).lower() or 'ingestion' in str(s).lower()],indent=2))"
```

The report's stage-level status, exit code, duration, error, and attempt count provide more precise evidence than the Airflow green status alone.

## 5. Present the dashboard

From the repository root:

```powershell
Set-Location ..\..
.\.venv312\Scripts\python.exe src\platform_dashboard.py
```

Open `reports/platform_health_dashboard.html` in a browser.

Walk through:
1. **Pipeline KPIs:** recorded run count, success rate, and available duration metrics.
2. **Data quality:** which scorecard dimensions have evidence and which remain unavailable.
3. **Regression detection:** how comparable observations identify potential quality deterioration.
4. **Recovery and lineage:** how failed stages and artifact discrepancies inform investigation recommendations.
5. **Prioritized actions:** how the summary directs attention to evidence-supported issues.
6. **Raw JSON:** how displayed metrics trace back to structured evidence.

## 6. Explain the engineering decisions

- Synthetic data avoids dependence on real banking customer information.
- Incremental processing avoids treating every run as a full reload.
- Airflow coordinates execution and validates the pipeline's generated run report.
- Spark processing and cross-engine reconciliation provide a second execution path for selected transformations and consistency checks.
- S3 ingestion is checksum-aware and records audit evidence.
- Quality regressions require comparable historical observations.
- Missing metrics are reported as unavailable rather than silently converted to passing scores.
- Diagnostic recommendations are separate from job execution; the dashboard does not restart or remediate jobs.

## 7. Be transparent about limitations

Distinguish local demonstration results from production deployment. Explain which stages are instrumented, whether sufficient historical reports exist for trend analysis, and which integrations have been exercised end to end. Do not claim production-scale performance, automatic recovery, or complete observability unless supported by additional implementation and test evidence.

## Suggested interview narrative

"I built a synthetic banking data platform with batch and incremental ETL, data quality validation, anomaly detection, and lineage tracking. I extended it with runtime reconciliation, recovery diagnostics, performance analytics, and a consolidated dashboard. I then orchestrated the existing pipeline with Apache Airflow in Docker, enabled PySpark processing, and integrated authenticated Amazon S3 raw-data ingestion. The Airflow task validates the generated run report so a failed pipeline or missing/non-passing report causes the task to fail. The focus is not just processing data: it is making failures explainable and using recorded evidence to guide investigation."
