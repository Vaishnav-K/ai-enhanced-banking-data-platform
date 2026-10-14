from pathlib import Path
from datetime import datetime, timezone
import argparse
import json
import subprocess
import sys
import time
from lineage_tracker import (start_run, record_stage, finish_run, record_artifact)

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable
REPORT = ROOT / "data" / "processed" / "pipeline_run_report.json"

STEPS = [
    ("Schema Validation", ROOT / "src" / "schema_validation.py"),
    ("Main ETL", ROOT / "src" / "etl_pipeline.py"),
    ("Incremental ETL", ROOT / "src" / "incremental_pipeline.py"),
    ("Quality Report", ROOT / "src" / "quality_report.py"),
    ("Anomaly Detection", ROOT / "src" / "anomaly_detection.py"),
    ("Analytics Exports", ROOT / "src" / "export_analytics.py"),
]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def run_command(command, cwd=ROOT):
    """Execute a command and return its exit code."""
    return subprocess.run(command, cwd=cwd, check=False).returncode


def run_stage(name, command, cwd=ROOT):
    started_at = utc_now()
    start_clock = time.perf_counter()

    print(f"\n>>> {name}")
    print("-" * 60)

    try:
        exit_code = run_command(command, cwd=cwd)
        error = None if exit_code == 0 else (
            f"Process exited with code {exit_code}"
        )
    except OSError as exc:
        exit_code = -1
        error = str(exc)

    duration = round(time.perf_counter() - start_clock, 3)

    return {
        "stage": name,
        "status": "PASS" if exit_code == 0 else "FAIL",
        "started_at_utc": started_at,
        "finished_at_utc": utc_now(),
        "duration_seconds": duration,
        "exit_code": exit_code,
        "error": error,
    }


def write_report(report):
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nPipeline run report saved to: {REPORT}")



def snapshot_artifacts():
    """Capture existing processed and quarantine files."""
    roots = [
        ROOT / "data" / "processed",
        ROOT / "data" / "quarantine",
    ]
    snapshot = {}

    for directory in roots:
        if not directory.exists():
            continue

        for file_path in directory.rglob("*"):
            if not file_path.is_file():
                continue
            if file_path.name == "ingestion_manifest.db":
                continue
            if file_path.name in (
                "ingestion_manifest.db-wal",
                "ingestion_manifest.db-shm",
            ):
                continue

            stat = file_path.stat()
            snapshot[file_path.resolve()] = (
                stat.st_size,
                stat.st_mtime_ns,
            )

    return snapshot


def discover_changed_artifacts(before):
    """Return files created or modified since the snapshot."""
    after = snapshot_artifacts()
    return sorted(
        file_path
        for file_path, signature in after.items()
        if before.get(file_path) != signature
    )


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Run the banking data pipeline with execution telemetry."
    )
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Upload raw CSV files to S3 after pipeline stages and tests pass.",
    )
    args = parser.parse_args(argv)

    run_started = utc_now()
    run_id = start_run("AI-Enhanced Banking Data Platform")
    overall_start = time.perf_counter()
    stages = []
    overall_status = "PASS"
    failure_stage = None

    print("=" * 60)
    print("AI-ENHANCED BANKING DATA PLATFORM")
    print("PIPELINE OBSERVABILITY ENABLED")
    print("=" * 60)

    for name, script in STEPS:
        result = run_stage(name, [PYTHON, str(script)])
        stages.append(result)

        if result["status"] == "FAIL":
            overall_status = "FAIL"
            failure_stage = name
            break

    if overall_status == "PASS":
        result = run_stage(
            "Automated Tests",
            [
                PYTHON, "-m", "unittest", "discover",
                "-s", "tests", "-p", "test_*.py", "-v",
            ],
        )
        stages.append(result)

        if result["status"] == "FAIL":
            overall_status = "FAIL"
            failure_stage = "Automated Tests"

    if overall_status == "PASS" and args.upload_s3:
        result = run_stage(
            "S3 Raw Data Ingestion",
            [PYTHON, str(ROOT / "src" / "s3_ingestion.py"), "--upload"],
        )
        stages.append(result)

        if result["status"] == "FAIL":
            overall_status = "FAIL"
            failure_stage = "S3 Raw Data Ingestion"
    elif not args.upload_s3:
        stages.append({
            "stage": "S3 Raw Data Ingestion",
            "status": "SKIPPED",
            "started_at_utc": None,
            "finished_at_utc": None,
            "duration_seconds": 0,
            "exit_code": None,
            "error": None,
            "reason": "S3 upload was not requested",
        })

    report = {
        "pipeline": "AI-Enhanced Banking Data Platform",
        "run_id": run_id,
        "run_started_at_utc": run_started,
        "run_finished_at_utc": utc_now(),
        "total_duration_seconds": round(
            time.perf_counter() - overall_start, 3
        ),
        "status": overall_status,
        "failure_stage": failure_stage,
        "stage_count": len(stages),
        "passed_stages": sum(s["status"] == "PASS" for s in stages),
        "failed_stages": sum(s["status"] == "FAIL" for s in stages),
        "skipped_stages": sum(s["status"] == "SKIPPED" for s in stages),
        "stages": stages,
    }

    for stage_order, stage in enumerate(stages, start=1):
        record_stage(run_id, stage_order, stage)
    finish_run(run_id, report)

    input_files = [
        ROOT / "data" / "raw" / "customers.csv",
        ROOT / "data" / "raw" / "transactions.csv",
        ROOT / "data" / "raw" / "transactions_incremental.csv",
    ]

    output_files = [
        ROOT / "data" / "processed" / "customers.parquet",
        ROOT / "data" / "processed" / "transactions.parquet",
        ROOT / "data" / "processed" / "transactions_incremental.parquet",
        ROOT / "data" / "processed" / "schema_validation_report.json",
        ROOT / "data" / "processed" / "quality_report.json",
        ROOT / "data" / "processed" / "transaction_anomalies.json",
        ROOT / "data" / "processed" / "pipeline_run_report.json",
        ROOT / "data" / "processed" / "ingestion_report.json",
        ROOT / "data" / "processed" / "customer_summary.csv",
        ROOT / "data" / "processed" / "transaction_summary.csv",
        ROOT / "data" / "processed" / "daily_transaction_summary.csv",
        ROOT / "data" / "quarantine" / "transactions.parquet",
        ROOT / "data" / "quarantine" / "transactions_incremental.parquet",
    ]

    for artifact in input_files:
        record_artifact(run_id, artifact, "INPUT")

    for artifact in output_files:
        if artifact != REPORT:
            record_artifact(run_id, artifact, "OUTPUT")

    write_report(report)
    record_artifact(run_id, REPORT, "OUTPUT")
    print(f"PIPELINE STATUS: {overall_status}")
    print(f"Total duration: {report['total_duration_seconds']} seconds")

    return 0 if overall_status == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
