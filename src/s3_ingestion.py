from pathlib import Path
import argparse
import os
import boto3

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload", action="store_true")
    args = parser.parse_args()

    bucket = os.getenv("BANKING_S3_BUCKET")
    if not bucket:
        raise SystemExit("Set BANKING_S3_BUCKET first.")

    files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        raise SystemExit(f"No CSV files found in {RAW_DIR}")

    s3 = boto3.client("s3")

    for file_path in files:
        key = f"banking-data/raw/{file_path.name}"
        if not args.upload:
            print(f"[DRY RUN] {file_path.name} -> s3://{bucket}/{key}")
        else:
            s3.upload_file(str(file_path), bucket, key)
            print(f"Uploaded: s3://{bucket}/{key}")

    print(f"Files discovered: {len(files)}")
    print(f"Dry run: {not args.upload}")

if __name__ == "__main__":
    main()
