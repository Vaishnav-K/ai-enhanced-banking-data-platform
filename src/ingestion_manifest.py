from pathlib import Path
from contextlib import closing
import sqlite3
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parent.parent
MANIFEST_DB = ROOT / "data" / "processed" / "ingestion_manifest.db"

SUCCESS_STATUSES = ("UPLOADED_AND_VERIFIED", "SKIPPED_IDENTICAL")


def _connect():
    connection = sqlite3.connect(MANIFEST_DB)
    connection.row_factory = sqlite3.Row
    return connection


def initialize_manifest():
    """Create or safely upgrade the ingestion manifest schema."""
    MANIFEST_DB.parent.mkdir(parents=True, exist_ok=True)

    with closing(_connect()) as connection:
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
                    last_successful_checksum TEXT,
                    last_error TEXT,
                    PRIMARY KEY (file_name, source_uri)
                )
            """)

            columns = {
                row["name"]
                for row in connection.execute(
                    "PRAGMA table_info(ingestion_manifest)"
                ).fetchall()
            }

            if "last_successful_checksum" not in columns:
                connection.execute("""
                    ALTER TABLE ingestion_manifest
                    ADD COLUMN last_successful_checksum TEXT
                """)

            if "last_error" not in columns:
                connection.execute("""
                    ALTER TABLE ingestion_manifest
                    ADD COLUMN last_error TEXT
                """)

            # Preserve the checksum of records that were already successful
            # before this schema upgrade.
            connection.execute("""
                UPDATE ingestion_manifest
                SET last_successful_checksum = checksum_sha256
                WHERE status IN (?, ?)
                  AND last_successful_checksum IS NULL
            """, SUCCESS_STATUSES)


def record_file(
    file_name,
    source_uri,
    checksum_sha256,
    size_bytes,
    status,
    error=None,
):
    """Record an ingestion attempt while preserving successful lineage."""
    now = datetime.now(timezone.utc).isoformat()
    successful = status in SUCCESS_STATUSES

    with closing(_connect()) as connection:
        with connection:
            connection.execute("""
                INSERT INTO ingestion_manifest (
                    file_name, source_uri, checksum_sha256, size_bytes,
                    status, first_seen_utc, last_seen_utc, successful_runs,
                    last_successful_checksum, last_error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_name, source_uri) DO UPDATE SET
                    checksum_sha256 = excluded.checksum_sha256,
                    size_bytes = excluded.size_bytes,
                    status = excluded.status,
                    last_seen_utc = excluded.last_seen_utc,
                    successful_runs =
                        ingestion_manifest.successful_runs
                        + excluded.successful_runs,
                    last_successful_checksum =
                        CASE
                            WHEN excluded.status IN (?, ?)
                            THEN excluded.checksum_sha256
                            ELSE ingestion_manifest.last_successful_checksum
                        END,
                    last_error = excluded.last_error
            """, (
                file_name,
                source_uri,
                checksum_sha256,
                size_bytes,
                status,
                now,
                now,
                int(successful),
                checksum_sha256 if successful else None,
                error,
                *SUCCESS_STATUSES,
            ))


def get_manifest_records():
    """Return manifest records in a predictable order."""
    with closing(_connect()) as connection:
        rows = connection.execute("""
            SELECT file_name, source_uri, checksum_sha256, size_bytes,
                   status, first_seen_utc, last_seen_utc, successful_runs,
                   last_successful_checksum, last_error
            FROM ingestion_manifest
            ORDER BY file_name, source_uri
        """).fetchall()

    return [dict(row) for row in rows]


if __name__ == "__main__":
    initialize_manifest()
    print(f"Ingestion manifest initialized: {MANIFEST_DB}")
    print(f"Existing records: {len(get_manifest_records())}")
