
from pathlib import Path
import argparse
import os

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
PREFIX = "banking-data/raw"


def main():
    parser = argparse.ArgumentParser(
        description="Upload and verify raw banking CSV files in S3."
    )
    parser.add_argument(
        "--upload",
        action="store_true",
        help="Upload files; otherwise perform a dry run.",
    )
    args = parser.parse_args()

    bucket = os.getenv("BANKING_S3_BUCKET")
    if not bucket:
        raise SystemExit("Set BANKING_S3_BUCKET first.")

    files = sorted(RAW_DIR.glob("*.csv"))
    if not files:
        raise SystemExit(f"No CSV files found in {RAW_DIR}")

    s3 = boto3.client("s3")
    uploaded = 0
    skipped = 0
    verified = 0

    for file_path in files:
        key = f"{PREFIX}/{file_path.name}"
        expected_size = file_path.stat().st_size

        if not args.upload:
            print(
                f"[DRY RUN] {file_path.name} -> "
                f"s3://{bucket}/{key} ({expected_size} bytes)"
            )
            continue

        try:
            response = s3.head_object(Bucket=bucket, Key=key)
            remote_size = response["ContentLength"]

            if remote_size == expected_size:
                print(
                    f"SKIPPED (same size): {file_path.name} "
                    f"({remote_size} bytes)"
                )
                skipped += 1
                verified += 1
                continue

            print(
                f"SIZE MISMATCH: {file_path.name}; "
                f"local={expected_size}, remote={remote_size}. Re-uploading."
            )

        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            status = exc.response.get("ResponseMetadata", {}).get(
                "HTTPStatusCode"
            )

            if error_code not in ("404", "NoSuchKey", "NotFound") and status != 404:
                raise RuntimeError(
                    f"Cannot inspect s3://{bucket}/{key}: {exc}"
                ) from exc

        s3.upload_file(str(file_path), bucket, key)
        uploaded += 1

        response = s3.head_object(Bucket=bucket, Key=key)
        actual_size = response["ContentLength"]

        if actual_size != expected_size:
            raise RuntimeError(
                f"Verification failed for {file_path.name}: "
                f"expected {expected_size} bytes, got {actual_size}"
            )

        verified += 1
        print(
            f"UPLOADED AND VERIFIED: s3://{bucket}/{key} "
            f"({actual_size} bytes)"
        )

    print(f"\nFiles discovered: {len(files)}")
    print(f"Files uploaded: {uploaded}")
    print(f"Files skipped (same size): {skipped}")
    print(f"Files verified: {verified}")
    print(f"Dry run: {not args.upload}")


if __name__ == "__main__":
    main()
