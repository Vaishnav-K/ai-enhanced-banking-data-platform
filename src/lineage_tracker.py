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



def initialize_attempt_table():
    """Create persistent storage for individual stage execution attempts."""
    initialize_lineage()
    with closing(connect()) as connection:
        with connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_stage_attempts (
                    run_id TEXT NOT NULL,
                    stage_order INTEGER NOT NULL,
                    attempt_number INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    started_at_utc TEXT,
                    finished_at_utc TEXT,
                    duration_seconds REAL NOT NULL DEFAULT 0,
                    exit_code INTEGER,
                    error TEXT,
                    retryable INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (run_id, stage_order, attempt_number),
                    FOREIGN KEY (run_id, stage_order)
                        REFERENCES pipeline_stage_runs(run_id, stage_order)
                )
            """)
            connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_stage_attempts_run
                ON pipeline_stage_attempts(run_id, stage_order)
            """)


def record_stage_attempt(run_id, stage_order, attempt):
    """Persist one attempt after its parent stage summary has been recorded."""
    initialize_attempt_table()

    with closing(connect()) as connection:
        with connection:
            connection.execute("""
                INSERT INTO pipeline_stage_attempts (
                    run_id, stage_order, attempt_number, status,
                    started_at_utc, finished_at_utc, duration_seconds,
                    exit_code, error, retryable
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id, stage_order, attempt_number)
                DO UPDATE SET
                    status=excluded.status,
                    started_at_utc=excluded.started_at_utc,
                    finished_at_utc=excluded.finished_at_utc,
                    duration_seconds=excluded.duration_seconds,
                    exit_code=excluded.exit_code,
                    error=excluded.error,
                    retryable=excluded.retryable
            """, (
                run_id,
                stage_order,
                attempt["attempt"],
                attempt["status"],
                attempt.get("started_at_utc"),
                attempt.get("finished_at_utc"),
                attempt.get("duration_seconds", 0),
                attempt.get("exit_code"),
                attempt.get("error"),
                int(bool(attempt.get("retryable", False))),
            ))


def get_stage_attempts(run_id, stage_order=None):
    """Return stored attempts for a run, optionally filtered by stage order."""
    initialize_attempt_table()

    with closing(connect()) as connection:
        if stage_order is None:
            rows = connection.execute("""
                SELECT * FROM pipeline_stage_attempts
                WHERE run_id=?
                ORDER BY stage_order, attempt_number
            """, (run_id,)).fetchall()
        else:
            rows = connection.execute("""
                SELECT * FROM pipeline_stage_attempts
                WHERE run_id=? AND stage_order=?
                ORDER BY attempt_number
            """, (run_id, stage_order)).fetchall()

    return [dict(row) for row in rows]



def initialize_stage_artifact_observation_table():
    """Create storage for per-attempt stage artifact observations."""
    with closing(connect()) as connection:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_stage_artifact_observations (
                run_id TEXT NOT NULL,
                stage_order INTEGER NOT NULL,
                stage_name TEXT NOT NULL,
                attempt_number INTEGER NOT NULL,
                artifact_path TEXT NOT NULL,
                artifact_role TEXT NOT NULL
                    CHECK (artifact_role IN ('INPUT', 'OUTPUT')),
                evidence_type TEXT NOT NULL
                    CHECK (evidence_type IN ('DECLARED_INPUT', 'OBSERVED_CHANGE', 'DECLARED_OUTPUT')),
                file_exists INTEGER NOT NULL CHECK (file_exists IN (0, 1)),
                size_bytes INTEGER,
                modified_at_ns INTEGER,
                recorded_at_utc TEXT NOT NULL,
                PRIMARY KEY (
                    run_id, stage_order, attempt_number,
                    artifact_path, artifact_role, evidence_type
                ),
                FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
            )
        """)


def record_stage_artifact_observation(
    run_id, stage_order, stage_name, attempt_number,
    file_path, artifact_role, evidence_type,
):
    """Record declared input metadata or a file observed changing in an attempt."""
    if artifact_role not in {"INPUT", "OUTPUT"}:
        raise ValueError("artifact_role must be INPUT or OUTPUT")
    if evidence_type not in {"DECLARED_INPUT", "OBSERVED_CHANGE", "DECLARED_OUTPUT"}:
        raise ValueError("Unsupported evidence_type")
    if evidence_type == "DECLARED_INPUT" and artifact_role != "INPUT":
        raise ValueError("DECLARED_INPUT evidence must have role INPUT")
    if evidence_type == "OBSERVED_CHANGE" and artifact_role != "OUTPUT":
        raise ValueError("OBSERVED_CHANGE evidence must have role OUTPUT")

    for value, label in (
        (stage_order, "stage_order"),
        (attempt_number, "attempt_number"),
    ):
        if not isinstance(value, int) or isinstance(value, bool) or value < 1:
            raise ValueError(f"{label} must be a positive integer")

    path = Path(file_path).resolve()
    try:
        artifact_path = path.relative_to(ROOT).as_posix()
    except ValueError:
        artifact_path = path.as_posix()

    exists = path.is_file()
    size_bytes = None
    modified_at_ns = None
    if exists:
        stat = path.stat()
        size_bytes = stat.st_size
        modified_at_ns = stat.st_mtime_ns

    with connect() as connection:
        connection.execute("""
            INSERT INTO pipeline_stage_artifact_observations (
                run_id, stage_order, stage_name, attempt_number,
                artifact_path, artifact_role, evidence_type,
                file_exists, size_bytes, modified_at_ns, recorded_at_utc
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (
                run_id, stage_order, attempt_number,
                artifact_path, artifact_role, evidence_type
            ) DO UPDATE SET
                stage_name = excluded.stage_name,
                file_exists = excluded.file_exists,
                size_bytes = excluded.size_bytes,
                modified_at_ns = excluded.modified_at_ns,
                recorded_at_utc = excluded.recorded_at_utc
        """, (
            run_id, stage_order, stage_name, attempt_number,
            artifact_path, artifact_role, evidence_type,
            int(exists), size_bytes, modified_at_ns, utc_now(),
        ))


def get_stage_artifact_observations(run_id, stage_order=None):
    """Return stage artifact observations in deterministic order."""
    query = """
        SELECT run_id, stage_order, stage_name, attempt_number,
               artifact_path, artifact_role, evidence_type,
               file_exists, size_bytes, modified_at_ns, recorded_at_utc
        FROM pipeline_stage_artifact_observations
        WHERE run_id = ?
    """
    parameters = [run_id]
    if stage_order is not None:
        query += " AND stage_order = ?"
        parameters.append(stage_order)
    query += """
        ORDER BY stage_order, attempt_number, artifact_role,
                 artifact_path, evidence_type
    """
    with connect() as connection:
        rows = connection.execute(query, parameters).fetchall()

    columns = (
        "run_id", "stage_order", "stage_name", "attempt_number",
        "artifact_path", "artifact_role", "evidence_type",
        "file_exists", "size_bytes", "modified_at_ns", "recorded_at_utc",
    )
    return [dict(zip(columns, row)) for row in rows]

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


def get_interrupted_runs():
    """Return pipeline runs still marked RUNNING without a finish time."""
    initialize_lineage()
    with closing(connect()) as conn:
        rows = conn.execute(
            """
            SELECT run_id, pipeline_name, started_at_utc,
                   finished_at_utc, status, failure_stage
            FROM pipeline_runs
            WHERE status = 'RUNNING'
              AND finished_at_utc IS NULL
            ORDER BY started_at_utc
            """
        ).fetchall()
        return [dict(row) for row in rows]


def get_recovery_report(stale_after_minutes=60):
    """Summarize incomplete runs, failed stages, and retry history."""
    if (
        isinstance(stale_after_minutes, bool)
        or not isinstance(stale_after_minutes, (int, float))
        or stale_after_minutes <= 0
    ):
        raise ValueError("stale_after_minutes must be a positive number")

    initialize_lineage()
    initialize_artifact_table()
    initialize_attempt_table()
    initialize_stage_artifact_observation_table()
    now = datetime.now(timezone.utc)
    threshold_seconds = float(stale_after_minutes) * 60.0

    with closing(connect()) as conn:
        rows = conn.execute(
            """
            SELECT r.run_id, r.pipeline_name, r.started_at_utc,
                   r.finished_at_utc, r.status, r.failure_stage,
                   (SELECT COUNT(*) FROM pipeline_stage_runs s
                    WHERE s.run_id = r.run_id) AS stage_record_count,
                   (SELECT COUNT(*) FROM pipeline_stage_attempts a
                    WHERE a.run_id = r.run_id) AS attempt_record_count,
                   (SELECT COUNT(*) FROM pipeline_artifacts a
                    WHERE a.run_id = r.run_id) AS artifact_record_count,
                   (SELECT COUNT(*) FROM pipeline_stage_artifact_observations o
                    WHERE o.run_id = r.run_id) AS artifact_observation_count
            FROM pipeline_runs r
            WHERE r.status = 'RUNNING'
              AND r.finished_at_utc IS NULL
            ORDER BY r.started_at_utc
            """
        ).fetchall()

        interrupted = []
        for row in rows:
            item = dict(row)
            try:
                started = datetime.fromisoformat(item["started_at_utc"])
                if started.tzinfo is None:
                    started = started.replace(tzinfo=timezone.utc)
                age_seconds = max(
                    0.0,
                    (now - started.astimezone(timezone.utc)).total_seconds(),
                )
                item["age_seconds"] = round(age_seconds, 3)
                item["age_minutes"] = round(age_seconds / 60.0, 2)
                item["recovery_classification"] = (
                    "POTENTIALLY_STALE"
                    if age_seconds >= threshold_seconds
                    else "RECENTLY_STARTED"
                )
                item["timestamp_parse_error"] = None
                evidence_counts = (
                    item["stage_record_count"],
                    item["attempt_record_count"],
                    item["artifact_record_count"],
                    item["artifact_observation_count"],
                )
                item["evidence_classification"] = (
                    "NO_RECORDED_EVIDENCE"
                    if not any(evidence_counts)
                    else "PARTIAL_OR_RECORDED_EVIDENCE"
                )
            except (TypeError, ValueError, OverflowError) as exc:
                item["age_seconds"] = None
                item["age_minutes"] = None
                item["recovery_classification"] = "UNKNOWN"
                item["timestamp_parse_error"] = str(exc)
                evidence_counts = (
                    item["stage_record_count"],
                    item["attempt_record_count"],
                    item["artifact_record_count"],
                    item["artifact_observation_count"],
                )
                item["evidence_classification"] = (
                    "NO_RECORDED_EVIDENCE"
                    if not any(evidence_counts)
                    else "PARTIAL_OR_RECORDED_EVIDENCE"
                )
            interrupted.append(item)

        failed_stages = conn.execute(
            """
            SELECT s.run_id, r.pipeline_name, s.stage_order,
                   s.stage_name, s.status, s.exit_code, s.error
            FROM pipeline_stage_runs s
            JOIN pipeline_runs r ON r.run_id = s.run_id
            WHERE s.status = 'FAIL'
            ORDER BY s.run_id, s.stage_order
            """
        ).fetchall()

        attempts = conn.execute(
            """
            SELECT a.run_id, r.pipeline_name, a.stage_order,
                   s.stage_name, a.attempt_number, a.status,
                   a.exit_code, a.error, a.retryable
            FROM pipeline_stage_attempts a
            JOIN pipeline_runs r ON r.run_id = a.run_id
            JOIN pipeline_stage_runs s
              ON s.run_id = a.run_id
             AND s.stage_order = a.stage_order
            ORDER BY a.run_id, a.stage_order, a.attempt_number
            """
        ).fetchall()

        return {
            "interrupted_runs": interrupted,
            "failed_stages": [dict(row) for row in failed_stages],
            "attempt_history": [dict(row) for row in attempts],
            "stale_after_minutes": float(stale_after_minutes),
            "potentially_stale_count": sum(
                r["recovery_classification"] == "POTENTIALLY_STALE"
                for r in interrupted
            ),
            "recently_started_count": sum(
                r["recovery_classification"] == "RECENTLY_STARTED"
                for r in interrupted
            ),
            "unknown_age_count": sum(
                r["recovery_classification"] == "UNKNOWN"
                for r in interrupted
            ),
        }
