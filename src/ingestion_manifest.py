from pathlib import Path
from contextlib import closing
import sqlite3
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent
MANIFEST_DB = ROOT / "data" / "processed" / "ingestion_manifest.db"


def initialize_manifest():
    """Create the ingestion manifest table if it does not exist."""
    MANIFEST_DB.parent.mkdir(parents=True, exist_ok=True)

    with closing(sqlite3.connect(MANIFEST_DB)) as connection:
        with connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS ingestion_manifest (
                    file_name TEXT NOT NULL,
                    source_uri TEXT NOT NULL,
                    checksum_sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    first_seen_utc TEXT NOT NULL,
                    last_seen_utc TEXT NOT NULL,
                    successful_runs INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (file_name, source_uri)
                )
            """)


def record_file(file_name, source_uri, checksum_sha256, size_bytes, status):
    """Insert a file record or update its latest processing details."""
    now = datetime.now(timezone.utc).isoformat()
    successful = int(
        status in ("UPLOADED_AND_VERIFIED", "SKIPPED_IDENTICAL")
    )

    with closing(sqlite3.connect(MANIFEST_DB)) as connection:
        with connection:
            connection.execute("""
                INSERT INTO ingestion_manifest (
                    file_name, source_uri, checksum_sha256, size_bytes,
                    status, first_seen_utc, last_seen_utc, successful_runs
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_name, source_uri) DO UPDATE SET
                    checksum_sha256 = excluded.checksum_sha256,
                    size_bytes = excluded.size_bytes,
                    status = excluded.status,
                    last_seen_utc = excluded.last_seen_utc,
                    successful_runs =
                        ingestion_manifest.successful_runs
                        + excluded.successful_runs
            """, (
                file_name,
                source_uri,
                checksum_sha256,
                size_bytes,
                status,
                now,
                now,
                successful,
            ))


def get_manifest_records():
    """Return manifest records in a predictable order."""
    with closing(sqlite3.connect(MANIFEST_DB)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute("""
            SELECT file_name, source_uri, checksum_sha256, size_bytes,
                   status, first_seen_utc, last_seen_utc, successful_runs
            FROM ingestion_manifest
            ORDER BY file_name, source_uri
        """).fetchall()

    return [dict(row) for row in rows]


if __name__ == "__main__":
    initialize_manifest()
    print(f"Ingestion manifest initialized: {MANIFEST_DB}")
    print(f"Existing records: {len(get_manifest_records())}")
