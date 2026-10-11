from pathlib import Path
import argparse
import subprocess
import sys

ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable

parser = argparse.ArgumentParser()
parser.add_argument(
    "--upload-s3",
    action="store_true",
    help="Upload raw CSV files to S3 after all pipeline stages and tests pass."
)
args = parser.parse_args()

steps = [
    ("Main ETL", ROOT / "src" / "etl_pipeline.py"),
    ("Incremental ETL", ROOT / "src" / "incremental_pipeline.py"),
    ("Quality Report", ROOT / "src" / "quality_report.py"),
    ("Anomaly Detection", ROOT / "src" / "anomaly_detection.py"),
    ("Analytics Exports", ROOT / "src" / "export_analytics.py"),
]

print("=" * 60)
print("AI-ENHANCED BANKING DATA PLATFORM")
print("=" * 60)

for name, script in steps:
    print(f"\n>>> {name}")
    result = subprocess.run([PYTHON, str(script)], cwd=ROOT, check=False)
    if result.returncode != 0:
        print(f"\nFAILED: {name}")
        raise SystemExit(result.returncode)

print("\n>>> Automated Tests")
result = subprocess.run(
    [
        PYTHON, "-m", "unittest", "discover",
        "-s", "tests", "-p", "test_*.py", "-v"
    ],
    cwd=ROOT,
    check=False
)
if result.returncode != 0:
    raise SystemExit(result.returncode)

if args.upload_s3:
    print("\n>>> S3 Raw Data Ingestion")
    result = subprocess.run(
        [PYTHON, str(ROOT / "src" / "s3_ingestion.py"), "--upload"],
        cwd=ROOT,
        check=False
    )
    if result.returncode != 0:
        print("\nFAILED: S3 Raw Data Ingestion")
        raise SystemExit(result.returncode)
else:
    print("\n>>> S3 upload is handled separately by scripts\run_demo_with_report.ps1.")

print("\nAll requested pipeline stages completed successfully.")

