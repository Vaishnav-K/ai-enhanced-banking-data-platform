from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import sqlite3
import uuid

ROOT = Path(__file__).resolve().parent.parent
LINEAGE_DB = ROOT / "data" / "processed" / "ingestion_manifest.db"


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def connect():
    LINEAGE_DB.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(LINEAGE_DB)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_lineage():
    """Create run-level and stage-level lineage tables."""
    with closing(connect()) as connection:
        with connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id TEXT PRIMARY KEY,
                    pipeline_name TEXT NOT NULL,
                    started_at_utc TEXT NOT NULL,
                    finished_at_utc TEXT,
                    duration_seconds REAL,
                    status TEXT NOT NULL,
                    failure_stage TEXT
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_stage_runs (
                    run_id TEXT NOT NULL,
                    stage_order INTEGER NOT NULL,
                    stage_name TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at_utc TEXT,
                    finished_at_utc TEXT,
                    duration_seconds REAL NOT NULL DEFAULT 0,
                    exit_code INTEGER,
                    error TEXT,
                    PRIMARY KEY (run_id, stage_order),
                    FOREIGN KEY (run_id)
                        REFERENCES pipeline_runs(run_id)
                )
            """)

            connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started
                ON pipeline_runs(started_at_utc)
            """)

            connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_stage_runs_status
                ON pipeline_stage_runs(status)
            """)



def initialize_artifact_table():
    """Create the table linking pipeline runs to their input/output files."""
    initialize_lineage()
    with closing(connect()) as connection:
        with connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_artifacts (
                    run_id TEXT NOT NULL,
                    artifact_path TEXT NOT NULL,
                    artifact_role TEXT NOT NULL,
                    checksum_sha256 TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    recorded_at_utc TEXT NOT NULL,
                    PRIMARY KEY (run_id, artifact_path, artifact_role),
                    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
                )
            """)
            connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_pipeline_artifacts_run
                ON pipeline_artifacts(run_id)
            """)


def sha256_file(file_path):
    """Calculate a file's SHA-256 checksum without loading it all into memory."""
    import hashlib

    digest = hashlib.sha256()
    with Path(file_path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def record_artifact(run_id, file_path, role):
    """Record an existing input or output file for a pipeline run."""
    import os

    if role not in ("INPUT", "OUTPUT"):
        raise ValueError("role must be INPUT or OUTPUT")

    path = Path(file_path).resolve()
    if not path.is_file():
        return False

    initialize_artifact_table()
    checksum = sha256_file(path)
    size_bytes = path.stat().st_size

    with closing(connect()) as connection:
        with connection:
            connection.execute("""
                INSERT INTO pipeline_artifacts (
                    run_id, artifact_path, artifact_role,
                    checksum_sha256, size_bytes, recorded_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, artifact_path, artifact_role)
                DO UPDATE SET
                    checksum_sha256 = excluded.checksum_sha256,
                    size_bytes = excluded.size_bytes,
                    recorded_at_utc = excluded.recorded_at_utc
            """, (
                run_id,
                (
            path.relative_to(ROOT).as_posix()
            if path.is_relative_to(ROOT)
            else str(path)
        ),
                role,
                checksum,
                size_bytes,
                utc_now(),
            ))
    return True


def get_run_artifacts(run_id):
    """Return recorded input/output artifacts for a pipeline run."""
    initialize_artifact_table()
    with closing(connect()) as connection:
        rows = connection.execute("""
            SELECT artifact_path, artifact_role, checksum_sha256,
                   size_bytes, recorded_at_utc
            FROM pipeline_artifacts
            WHERE run_id = ?
            ORDER BY artifact_role, artifact_path
        """, (run_id,)).fetchall()
    return [dict(row) for row in rows]


def start_run(pipeline_name):
    """Create a uniquely identified pipeline execution."""
    initialize_lineage()
    run_id = str(uuid.uuid4())
    started_at = utc_now()

    with closing(connect()) as connection:
        with connection:
            connection.execute("""
                INSERT INTO pipeline_runs (
                    run_id, pipeline_name, started_at_utc, status
                )
                VALUES (?, ?, ?, 'RUNNING')
            """, (run_id, pipeline_name, started_at))

    return run_id


def record_stage(run_id, stage_order, stage):
    """Persist one stage result from the pipeline execution report."""
    with closing(connect()) as connection:
        with connection:
            connection.execute("""
                INSERT INTO pipeline_stage_runs (
                    run_id, stage_order, stage_name, status,
                    started_at_utc, finished_at_utc,
                    duration_seconds, exit_code, error
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                run_id,
                stage_order,
                stage["stage"],
                stage["status"],
                stage.get("started_at_utc"),
                stage.get("finished_at_utc"),
                stage.get("duration_seconds", 0),
                stage.get("exit_code"),
                stage.get("error"),
            ))


def finish_run(run_id, report):
    """Finalize a run using the pipeline runner's report."""
    with closing(connect()) as connection:
        with connection:
            connection.execute("""
                UPDATE pipeline_runs
                SET finished_at_utc = ?,
                    duration_seconds = ?,
                    status = ?,
                    failure_stage = ?
                WHERE run_id = ?
            """, (
                report["run_finished_at_utc"],
                report["total_duration_seconds"],
                report["status"],
                report.get("failure_stage"),
                run_id,
            ))


def get_run(run_id):
    """Return a run and its recorded stages."""
    with closing(connect()) as connection:
        run = connection.execute("""
            SELECT * FROM pipeline_runs WHERE run_id = ?
        """, (run_id,)).fetchone()

        if run is None:
            return None

        stages = connection.execute("""
            SELECT * FROM pipeline_stage_runs
            WHERE run_id = ?
            ORDER BY stage_order
        """, (run_id,)).fetchall()

    result = dict(run)
    result["stages"] = [dict(stage) for stage in stages]
    return result


def get_recent_runs(limit=10):
    """Return recent pipeline runs, newest first."""
    if not isinstance(limit, int) or limit < 1:
        raise ValueError("limit must be a positive integer")

    with closing(connect()) as connection:
        rows = connection.execute("""
            SELECT * FROM pipeline_runs
            ORDER BY started_at_utc DESC
            LIMIT ?
        """, (limit,)).fetchall()

    return [dict(row) for row in rows]


if __name__ == "__main__":
    initialize_lineage()
    print(f"Lineage tables initialized in: {LINEAGE_DB}")
    print(f"Existing pipeline runs: {len(get_recent_runs(100000))}")
