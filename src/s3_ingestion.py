from pathlib import Path
from datetime import datetime, timezone
import argparse
import hashlib
import json
import os

import boto3
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "data" / "raw"
AUDIT_FILE = ROOT / "data" / "processed" / "s3_ingestion_audit.jsonl"
PREFIX = "banking-data/raw"
CHECKSUM_METADATA_KEY = "source-sha256"


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def append_audit(record):
    AUDIT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with AUDIT_FILE.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record) + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Upload, checksum-verify and audit raw banking CSV files."
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
    uploaded = skipped = verified = failed = 0

    for path in files:
        key = f"{PREFIX}/{path.name}"
        size = path.stat().st_size
        checksum = sha256_file(path)
        action = "DRY_RUN"

        record = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "file": path.name,
            "s3_uri": f"s3://{bucket}/{key}",
            "size_bytes": size,
            "sha256": checksum,
            "mode": "upload" if args.upload else "dry_run",
        }

        try:
            if not args.upload:
                print(
                    f"[DRY RUN] {path.name}: {size} bytes, "
                    f"SHA-256 {checksum}"
                )
                continue

            try:
                remote = s3.head_object(Bucket=bucket, Key=key)
            except ClientError as exc:
                error_code = exc.response.get("Error", {}).get("Code", "")
                status = exc.response.get("ResponseMetadata", {}).get(
                    "HTTPStatusCode"
                )
                if error_code not in ("404", "NoSuchKey", "NotFound") and status != 404:
                    raise

                remote = None

            if remote is not None:
                metadata = remote.get("Metadata", {})
                remote_checksum = metadata.get(CHECKSUM_METADATA_KEY)

                if remote_checksum == checksum:
                    action = "SKIPPED_IDENTICAL"
                    skipped += 1
                    verified += 1
                    print(f"SKIPPED (checksum matches): {path.name}")
                    record["status"] = action
                    append_audit(record)
                    continue

                print(f"Uploading {path.name}: missing or different checksum.")

            s3.upload_file(
                str(path),
                bucket,
                key,
                ExtraArgs={"Metadata": {CHECKSUM_METADATA_KEY: checksum}},
            )
            uploaded += 1

            remote = s3.head_object(Bucket=bucket, Key=key)
            actual_size = remote["ContentLength"]
            actual_checksum = remote.get("Metadata", {}).get(
                CHECKSUM_METADATA_KEY
            )

            if actual_size != size or actual_checksum != checksum:
                raise RuntimeError(
                    f"Verification failed for {path.name}: "
                    f"size expected={size}, actual={actual_size}; "
                    f"checksum_match={actual_checksum == checksum}"
                )

            verified += 1
            action = "UPLOADED_AND_VERIFIED"
            print(f"UPLOADED AND VERIFIED: {path.name}")

        except Exception as exc:
            failed += 1
            action = "FAILED"
            record["error"] = str(exc)
            print(f"FAILED: {path.name}: {exc}")
            append_audit({**record, "status": action})
            raise

        record["status"] = action
        append_audit(record)

    print(f"\nFiles discovered: {len(files)}")
    print(f"Files uploaded: {uploaded}")
    print(f"Files skipped (checksum match): {skipped}")
    print(f"Files verified: {verified}")
    print(f"Files failed: {failed}")
    print(f"Dry run: {not args.upload}")
    print(f"Audit log: {AUDIT_FILE.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
