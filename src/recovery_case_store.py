"""Append-only audit history for human recovery decisions."""

import json
from contextlib import closing
from datetime import datetime, timezone

import lineage_tracker as tracker

ALLOWED_DECISIONS = {
    "INVESTIGATED",
    "SAFE_TO_RETRY",
    "REQUIRES_MANUAL_INTERVENTION",
}


def initialize_recovery_case_table():
    """Create the recovery decision audit table without altering existing records."""
    tracker.initialize_lineage()
    with closing(tracker.connect()) as conn:
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS recovery_case_events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT NOT NULL,
                    decision TEXT NOT NULL CHECK (
                        decision IN (
                            'INVESTIGATED',
                            'SAFE_TO_RETRY',
                            'REQUIRES_MANUAL_INTERVENTION'
                        )
                    ),
                    reason TEXT NOT NULL CHECK (length(trim(reason)) > 0),
                    recorded_by TEXT NOT NULL CHECK (length(trim(recorded_by)) > 0),
                    recorded_at_utc TEXT NOT NULL,
                    evidence_snapshot_json TEXT NOT NULL DEFAULT '{}',
                    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_recovery_case_events_run
                ON recovery_case_events(run_id, event_id)
            """)


def record_recovery_decision(
    run_id,
    decision,
    reason,
    recorded_by="operator",
    evidence_snapshot=None,
):
    """Append a human decision; never changes pipeline status or executes recovery."""
    if not isinstance(run_id, str) or not run_id.strip():
        raise ValueError("run_id must be a non-empty string")

    if not isinstance(decision, str) or decision not in ALLOWED_DECISIONS:
        raise ValueError(
            f"decision must be one of: {', '.join(sorted(ALLOWED_DECISIONS))}"
        )

    if not isinstance(reason, str) or not reason.strip():
        raise ValueError("reason must be a non-empty string")

    if not isinstance(recorded_by, str) or not recorded_by.strip():
        raise ValueError("recorded_by must be a non-empty string")

    if evidence_snapshot is None:
        evidence_snapshot = {}
    if not isinstance(evidence_snapshot, dict):
        raise ValueError("evidence_snapshot must be a dictionary")

    snapshot_json = json.dumps(
        evidence_snapshot, sort_keys=True, default=str
    )
    initialize_recovery_case_table()

    with closing(tracker.connect()) as conn:
        with conn:
            run = conn.execute(
                "SELECT status FROM pipeline_runs WHERE run_id = ?",
                (run_id.strip(),),
            ).fetchone()
            if run is None:
                raise ValueError(f"Unknown pipeline run: {run_id}")

            cursor = conn.execute("""
                INSERT INTO recovery_case_events (
                    run_id, decision, reason, recorded_by,
                    recorded_at_utc, evidence_snapshot_json
                ) VALUES (?, ?, ?, ?, ?, ?)
            """, (
                run_id.strip(),
                decision,
                reason.strip(),
                recorded_by.strip(),
                datetime.now(timezone.utc).isoformat(),
                snapshot_json,
            ))
            event_id = cursor.lastrowid

    return event_id


def list_recovery_decisions(run_id=None):
    """Return audit events in insertion order, optionally for one run."""
    initialize_recovery_case_table()

    with closing(tracker.connect()) as conn:
        if run_id is None:
            rows = conn.execute("""
                SELECT event_id, run_id, decision, reason, recorded_by,
                       recorded_at_utc, evidence_snapshot_json
                FROM recovery_case_events
                ORDER BY event_id
            """).fetchall()
        else:
            rows = conn.execute("""
                SELECT event_id, run_id, decision, reason, recorded_by,
                       recorded_at_utc, evidence_snapshot_json
                FROM recovery_case_events
                WHERE run_id = ?
                ORDER BY event_id
            """, (run_id,)).fetchall()

    events = []
    for row in rows:
        item = dict(row)
        item["evidence_snapshot"] = json.loads(
            item.pop("evidence_snapshot_json")
        )
        events.append(item)
    return events
