import json
import sys
from collections import Counter
from pathlib import Path

if __package__:
    from .ingestion_manifest import get_manifest_records
else:
    from ingestion_manifest import get_manifest_records

ROOT = Path(__file__).resolve().parent.parent
OUTPUT_PATH = ROOT / "data" / "processed" / "ingestion_report.json"
SUCCESS_STATUSES = {"UPLOADED_AND_VERIFIED", "SKIPPED_IDENTICAL"}


def main():
    records = get_manifest_records()
    counts = Counter(record["status"] for record in records)

    if not records:
        health = "NO DATA"
    elif any(record["status"] not in SUCCESS_STATUSES for record in records):
        health = "FAILED"
    else:
        health = "HEALTHY"

    successful = sum(
        record["status"] in SUCCESS_STATUSES for record in records
    )
    failed = sum(
        record["status"] not in SUCCESS_STATUSES for record in records
    )

    report = {
        "health": health,
        "files_tracked": len(records),
        "successful_latest_statuses": successful,
        "latest_failures": failed,
        "status_counts": dict(counts),
        "files": records,
    }

    print("\n=== BANKING DATA PLATFORM: INGESTION REPORT ===")
    print(f"Health: {health}")
    print(f"Files tracked: {len(records)}")
    print(f"Successful latest statuses: {successful}")
    print(f"Latest failures: {failed}")

    for record in records:
        print(f"\nFile: {record['file_name']}")
        print(f"  Status: {record['status']}")
        print(f"  Successful runs: {record['successful_runs']}")
        print(
            "  Last successful checksum: "
            f"{record['last_successful_checksum'] or 'None'}"
        )
        if record["last_error"]:
            print(f"  Last error: {record['last_error']}")

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(
        json.dumps(report, indent=2),
        encoding="utf-8",
    )
    print(f"\nJSON report saved to: {OUTPUT_PATH.relative_to(ROOT)}")

    # NO DATA is informational; FAILED signals a problem to automation.
    return 1 if health == "FAILED" else 0


if __name__ == "__main__":
    sys.exit(main())
