
# Support sibling imports in script and package execution.
import sys as _src_import_sys
from pathlib import Path as _src_import_path
_src_import_dir = str(_src_import_path(__file__).resolve().parent)
if _src_import_dir not in _src_import_sys.path:
    _src_import_sys.path.insert(0, _src_import_dir)
from pathlib import Path
from datetime import datetime, timezone
import argparse
import hashlib
import json
import os

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

if __package__:
    from .ingestion_manifest import initialize_manifest, record_file
else:
    from ingestion_manifest import initialize_manifest, record_file

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

    initialize_manifest()
    retry_config = Config(
        retries={
            "mode": "standard",
            "total_max_attempts": 3,
        },
        connect_timeout=5,
        read_timeout=30,
    )
    s3 = boto3.client("s3", config=retry_config)
    uploaded = skipped = verified = failed = 0

    for path in files:
        key = f"{PREFIX}/{path.name}"
        uri = f"s3://{bucket}/{key}"
        size = path.stat().st_size
        checksum = sha256_file(path)

        record = {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "file": path.name,
            "s3_uri": uri,
            "size_bytes": size,
            "sha256": checksum,
            "mode": "upload" if args.upload else "dry_run",
        }

        if not args.upload:
            print(
                f"[DRY RUN] {path.name}: {size} bytes, SHA-256 {checksum}"
            )
            continue

        try:
            try:
                remote = s3.head_object(Bucket=bucket, Key=key)
            except ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                status = exc.response.get("ResponseMetadata", {}).get(
                    "HTTPStatusCode"
                )
                if code not in ("404", "NoSuchKey", "NotFound") and status != 404:
                    raise
                remote = None

            remote_checksum = (
                remote.get("Metadata", {}).get(CHECKSUM_METADATA_KEY)
                if remote is not None else None
            )

            if (
                remote is not None
                and remote.get("ContentLength") == size
                and remote_checksum == checksum
            ):
                status = "SKIPPED_IDENTICAL"
                skipped += 1
                verified += 1
                print(f"SKIPPED (checksum matches): {path.name}")
            else:
                s3.upload_file(
                    str(path),
                    bucket,
                    key,
                    ExtraArgs={
                        "Metadata": {CHECKSUM_METADATA_KEY: checksum}
                    },
                )

                remote = s3.head_object(Bucket=bucket, Key=key)
                actual_size = remote.get("ContentLength")
                actual_checksum = remote.get("Metadata", {}).get(
                    CHECKSUM_METADATA_KEY
                )

                if actual_size != size or actual_checksum != checksum:
                    raise RuntimeError(
                        f"Verification failed for {path.name}: "
                        f"size expected={size}, actual={actual_size}; "
                        f"checksum_match={actual_checksum == checksum}"
                    )

                status = "UPLOADED_AND_VERIFIED"
                uploaded += 1
                verified += 1
                print(f"UPLOADED AND VERIFIED: {path.name}")

            record_file(path.name, uri, checksum, size, status)
            append_audit({**record, "status": status})

        except Exception as exc:
            failed += 1
            try:
                record_file(
                    path.name,
                    uri,
                    checksum,
                    size,
                    "FAILED",
                    error=str(exc),
                )
            except Exception as manifest_exc:
                print(
                    f"WARNING: Could not record manifest failure "
                    f"for {path.name}: {manifest_exc}"
                )

            append_audit({
                **record,
                "status": "FAILED",
                "error": str(exc),
            })
            print(f"FAILED: {path.name}: {exc}")
            raise

    print(f"\nFiles discovered: {len(files)}")
    print(f"Files uploaded: {uploaded}")
    print(f"Files skipped (checksum match): {skipped}")
    print(f"Files verified: {verified}")
    print(f"Files failed: {failed}")
    print(f"Dry run: {not args.upload}")
    print(f"Audit log: {AUDIT_FILE}")


if __name__ == "__main__":
    main()




