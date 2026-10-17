from pathlib import Path
from datetime import datetime, timezone
import argparse
import json
import subprocess
import sys
import time
from lineage_tracker import (start_run, record_stage, finish_run, record_artifact, record_stage_attempt)

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable
REPORT = ROOT / "data" / "processed" / "pipeline_run_report.json"

STEPS = [
    ("Schema Validation", ROOT / "src" / "schema_validation.py"),
    ("Main ETL", ROOT / "src" / "etl_pipeline.py"),
    ("Incremental ETL", ROOT / "src" / "incremental_pipeline.py"),
    ("Quality Report", ROOT / "src" / "quality_report.py"),
    ("Anomaly Detection", ROOT / "src" / "anomaly_detection.py"),
    ("Data Quality Investigation", ROOT / "src" / "data_quality_investigator.py"),
    ("Analytics Exports", ROOT / "src" / "export_analytics.py"),
]


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def run_command(command, cwd=ROOT):
    """Execute a command and return its exit code and diagnostic output."""
    result = subprocess.run(
        command,
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
        errors="replace",
    )
    return {
        "exit_code": result.returncode,
        "stdout": result.stdout or "",
        "stderr": result.stderr or "",
    }


def summarize_process_output(result, max_chars=4000):
    """Return bounded diagnostic text from subprocess output."""
    parts = []
    if result["stderr"].strip():
        parts.append("STDERR:\\n" + result["stderr"].strip())
    if result["stdout"].strip():
        parts.append("STDOUT:\\n" + result["stdout"].strip())

    if not parts:
        return None

    output = "\\n".join(parts)
    if len(output) > max_chars:
        output = output[:max_chars] + "\\n... output truncated ..."
    return output



RETRY_POLICY_FILE = ROOT / "config" / "retry_policy.json"


def load_retry_policy():
    """Load retry defaults and per-stage overrides from JSON."""
    import math

    try:
        policy = json.loads(
            RETRY_POLICY_FILE.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot load retry policy: {exc}") from exc

    if policy.get("version") != 1:
        raise ValueError("Unsupported retry policy version")

    defaults = policy.get("default")
    stages = policy.get("stages")

    if not isinstance(defaults, dict) or not isinstance(stages, dict):
        raise ValueError("Retry policy requires default and stages objects")

    for label, settings in [("default", defaults), *stages.items()]:
        if not isinstance(settings, dict):
            raise ValueError(f"Retry settings for {label} must be an object")

        attempts = settings.get("max_attempts", defaults.get("max_attempts"))
        delay = settings.get(
            "initial_delay_seconds",
            defaults.get("initial_delay_seconds"),
        )
        multiplier = settings.get(
            "backoff_multiplier",
            defaults.get("backoff_multiplier", 2.0),
        )
        codes = settings.get(
            "retryable_exit_codes",
            defaults.get("retryable_exit_codes", []),
        )

        if (
            not isinstance(attempts, int)
            or isinstance(attempts, bool)
            or attempts < 1
        ):
            raise ValueError(f"Invalid max_attempts for {label}")
        if (
            not isinstance(delay, (int, float))
            or isinstance(delay, bool)
            or not math.isfinite(delay)
            or delay < 0
        ):
            raise ValueError(f"Invalid initial_delay_seconds for {label}")
        if (
            not isinstance(multiplier, (int, float))
            or isinstance(multiplier, bool)
            or not math.isfinite(multiplier)
            or multiplier < 1
        ):
            raise ValueError(f"Invalid backoff_multiplier for {label}")
        if (
            not isinstance(codes, list)
            or any(
                not isinstance(code, int) or isinstance(code, bool)
                for code in codes
            )
        ):
            raise ValueError(f"Invalid retryable_exit_codes for {label}")

    return policy


def stage_retry_settings(policy, stage_name):
    """Resolve stage-specific settings over the global defaults."""
    settings = dict(policy["default"])
    settings.update(policy["stages"].get(stage_name, {}))
    return {
        "max_attempts": settings["max_attempts"],
        "retry_delay_seconds": settings["initial_delay_seconds"],
        "backoff_multiplier": settings.get("backoff_multiplier", 2.0),
        "retryable_exit_codes": set(settings["retryable_exit_codes"]),
    }


def run_stage(
    name,
    command,
    cwd=ROOT,
    max_attempts=3,
    retry_delay_seconds=1.0,
    retryable_exit_codes=(),
    backoff_multiplier=2.0,
):
    """Execute a stage with bounded retries and exponential backoff."""
    if not isinstance(max_attempts, int) or isinstance(max_attempts, bool):
        raise ValueError("max_attempts must be a positive integer")
    if max_attempts < 1:
        raise ValueError("max_attempts must be at least 1")
    if retry_delay_seconds < 0:
        raise ValueError("retry_delay_seconds cannot be negative")
    if backoff_multiplier < 1:
        raise ValueError("backoff_multiplier must be at least 1")

    started_at = utc_now()
    start_clock = time.perf_counter()
    attempts = []

    print(f"\\n>>> {name}")
    print("-" * 60)

    for attempt_number in range(1, max_attempts + 1):
        attempt_started_at = utc_now()
        attempt_clock = time.perf_counter()
        retryable_error = False

        try:
            process_result = run_command(command, cwd=cwd)

            # Support older tests or callers that mock run_command with an
            # integer exit code.
            if isinstance(process_result, int):
                exit_code = process_result
                process_output = None
            else:
                exit_code = process_result["exit_code"]
                process_output = summarize_process_output(process_result)

            error = None
            if exit_code != 0:
                error = f"Process exited with code {exit_code}"
                if process_output:
                    error += "\\n" + process_output

            retryable_error = exit_code in retryable_exit_codes
        except OSError as exc:
            exit_code = -1
            error = str(exc)
            retryable_error = True

        attempt_duration = round(time.perf_counter() - attempt_clock, 3)
        attempts.append({
            "attempt": attempt_number,
            "started_at_utc": attempt_started_at,
            "finished_at_utc": utc_now(),
            "duration_seconds": attempt_duration,
            "exit_code": exit_code,
            "status": "PASS" if exit_code == 0 else "FAIL",
            "error": error,
            "retryable": retryable_error,
        })

        if exit_code == 0:
            if attempt_number > 1:
                print(f"{name} recovered on attempt {attempt_number}.")
            break

        print(f"Attempt {attempt_number}/{max_attempts} failed: {error}")

        if not retryable_error:
            print("Failure is not configured as retryable; stopping retries.")
            break

        if attempt_number < max_attempts:
            delay = retry_delay_seconds * (backoff_multiplier ** (attempt_number - 1))
            if delay:
                print(f"Retrying in {delay:g} second(s).")
                time.sleep(delay)

    duration = round(time.perf_counter() - start_clock, 3)
    final_attempt = attempts[-1]

    return {
        "stage": name,
        "status": "PASS" if final_attempt["exit_code"] == 0 else "FAIL",
        "started_at_utc": started_at,
        "finished_at_utc": utc_now(),
        "duration_seconds": duration,
        "exit_code": final_attempt["exit_code"],
        "error": final_attempt["error"],
        "attempt_count": len(attempts),
        "max_attempts": max_attempts,
        "attempts": attempts,
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


def record_stage_history(run_id, stages):
    """Persist stage summaries and their individual retry attempts."""
    for stage_order, stage in enumerate(stages, start=1):
        record_stage(run_id, stage_order, stage)

        for attempt in stage.get("attempts", []):
            record_stage_attempt(run_id, stage_order, attempt)


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Run the banking data pipeline with execution telemetry."
    )
    parser.add_argument(
        "--include-spark",
        action="store_true",
        help="Run Spark processing and cross-engine reconciliation.",
    )
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Upload raw CSV files to S3 after pipeline stages and tests pass.",
    )
    args = parser.parse_args(argv)
    retry_policy = load_retry_policy()

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
        result = run_stage(
            name,
            [PYTHON, str(script)],
            **stage_retry_settings(retry_policy, name),
        )
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
            **stage_retry_settings(retry_policy, "Automated Tests"),
        )
        stages.append(result)

        if result["status"] == "FAIL":
            overall_status = "FAIL"
            failure_stage = "Automated Tests"

    if overall_status == "PASS" and args.include_spark:
        import os

        spark_python = os.environ.get("SPARK_PYTHON_EXECUTABLE", PYTHON)
        os.environ["PYSPARK_PYTHON"] = spark_python
        os.environ["PYSPARK_DRIVER_PYTHON"] = spark_python

        spark_stages = [
            ("Spark Processing", spark_python, ROOT / "src" / "spark_pipeline.py"),
            ("Cross-Engine Reconciliation", PYTHON, ROOT / "src" / "compare_engines.py"),
        ]

        for name, interpreter, script in spark_stages:
            result = run_stage(
                name,
                [interpreter, str(script)],
                **stage_retry_settings(retry_policy, name),
            )
            stages.append(result)

            if result["status"] == "FAIL":
                overall_status = "FAIL"
                failure_stage = name
                break
    elif not args.include_spark:
        stages.extend([
            {
                "stage": "Spark Processing",
                "status": "SKIPPED",
                "started_at_utc": None,
                "finished_at_utc": None,
                "duration_seconds": 0,
                "exit_code": None,
                "error": None,
                "reason": "Use --include-spark to enable Spark",
                "attempts": [],
            },
            {
                "stage": "Cross-Engine Reconciliation",
                "status": "SKIPPED",
                "started_at_utc": None,
                "finished_at_utc": None,
                "duration_seconds": 0,
                "exit_code": None,
                "error": None,
                "reason": "Use --include-spark to enable reconciliation",
                "attempts": [],
            },
        ])

    if overall_status == "PASS" and args.upload_s3:
        result = run_stage(
            "S3 Raw Data Ingestion",
            [PYTHON, str(ROOT / "src" / "s3_ingestion.py"), "--upload"],
            **stage_retry_settings(retry_policy, "S3 Raw Data Ingestion"),
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

    record_stage_history(run_id, stages)
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
        ROOT / "data" / "processed" / "quality_investigation_report.json",
        ROOT / "data" / "processed" / "pipeline_run_report.json",
        ROOT / "data" / "processed" / "ingestion_report.json",
        ROOT / "data" / "processed" / "customer_transaction_summary.csv",
        ROOT / "data" / "processed" / "transaction_status_summary.csv",
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
