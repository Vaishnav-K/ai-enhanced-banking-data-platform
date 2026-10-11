from pathlib import Path
import argparse
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable

STEPS = [
    ("Main ETL", ROOT / "src" / "etl_pipeline.py"),
    ("Incremental ETL", ROOT / "src" / "incremental_pipeline.py"),
    ("Quality Report", ROOT / "src" / "quality_report.py"),
    ("Anomaly Detection", ROOT / "src" / "anomaly_detection.py"),
    ("Analytics Exports", ROOT / "src" / "export_analytics.py"),
]


def run_command(command, cwd=ROOT):
    return subprocess.run(command, cwd=cwd, check=False).returncode


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Upload raw CSV files to S3 after all pipeline stages and tests pass.",
    )
    args = parser.parse_args(argv)

    print("=" * 60)
    print("AI-ENHANCED BANKING DATA PLATFORM")
    print("=" * 60)

    for name, script in STEPS:
        print(f"\n>>> {name}")
        result = run_command([PYTHON, str(script)])

        if result != 0:
            print(f"\nFAILED: {name}")
            return result

    print("\n>>> Automated Tests")
    result = run_command([
        PYTHON, "-m", "unittest", "discover",
        "-s", "tests", "-p", "test_*.py", "-v",
    ])

    if result != 0:
        print("\nFAILED: Automated Tests")
        return result

    if args.upload_s3:
        print("\n>>> S3 Raw Data Ingestion")
        result = run_command([
            PYTHON, str(ROOT / "src" / "s3_ingestion.py"), "--upload",
        ])

        if result != 0:
            print("\nFAILED: S3 Raw Data Ingestion")
            return result
    else:
        print("\n>>> S3 upload skipped (use --upload-s3 to enable).")

    print("\nAll requested pipeline stages completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
