import argparse
from contextlib import closing
import json
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB = ROOT / "data" / "processed" / "ingestion_manifest.db"
DEFAULT_CONFIG = ROOT / "config" / "data_lineage.json"


def declared_artifacts(config):
    nodes = config["nodes"]
    edges = config["edges"]
    stages = {
        node["id"]: node["stage_name"]
        for node in nodes
        if node.get("type") == "stage" and node.get("stage_name")
    }
    artifacts = {
        node["id"]: node["path"]
        for node in nodes
        if node.get("type") == "artifact" and node.get("path")
    }
    parents, children = {}, {}
    for edge in edges:
        parents.setdefault(edge["to"], []).append(edge["from"])
        children.setdefault(edge["from"], []).append(edge["to"])

    result = {}
    for stage_id, stage_name in stages.items():
        inputs = set()
        for parent in parents.get(stage_id, []):
            if parent in artifacts:
                inputs.add(artifacts[parent])
            elif parent in stages:
                for output_id in children.get(parent, []):
                    if output_id in artifacts:
                        inputs.add(artifacts[output_id])

        outputs = {
            artifacts[child]
            for child in children.get(stage_id, [])
            if child in artifacts
        }
        result[stage_name] = {
            "inputs": sorted(inputs),
            "outputs": sorted(outputs),
        }
    return result


def build_report(db_path=DEFAULT_DB, config_path=DEFAULT_CONFIG, run_id=None):
    config = json.loads(Path(config_path).read_text(encoding="utf-8-sig"))
    declared = declared_artifacts(config)

    with closing(sqlite3.connect(str(db_path))) as con:
        con.row_factory = sqlite3.Row
        if run_id is None:
            run = con.execute("""
                SELECT run_id, status, started_at_utc, finished_at_utc
                FROM pipeline_runs
                ORDER BY started_at_utc DESC LIMIT 1
            """).fetchone()
        else:
            run = con.execute("""
                SELECT run_id, status, started_at_utc, finished_at_utc
                FROM pipeline_runs WHERE run_id = ?
            """, (run_id,)).fetchone()

        if run is None:
            raise ValueError(f"Pipeline run not found: {run_id or 'latest'}")

        observations = con.execute("""
            SELECT stage_name, artifact_path, artifact_role, evidence_type,
                   file_exists, attempt_number
            FROM pipeline_stage_artifact_observations
            WHERE run_id = ?
            ORDER BY stage_order, attempt_number, artifact_path
        """, (run["run_id"],)).fetchall()

    recorded = {}
    for row in observations:
        key = row["stage_name"]
        bucket = recorded.setdefault(key, {
            "inputs": set(), "outputs": set(), "changed": set(),
            "input_exists": {}, "attempts": set(),
        })
        path = row["artifact_path"]
        evidence = row["evidence_type"]
        if row["artifact_role"] == "INPUT" and evidence == "DECLARED_INPUT":
            bucket["inputs"].add(path)
            bucket["input_exists"][path] = bool(row["file_exists"])
        if row["artifact_role"] == "OUTPUT" and evidence == "DECLARED_OUTPUT":
            bucket["outputs"].add(path)
        if row["artifact_role"] == "OUTPUT" and evidence == "OBSERVED_CHANGE":
            bucket["changed"].add(path)
        bucket["attempts"].add(row["attempt_number"])

    stage_reports = []
    observed_stage_names = set(recorded)
    for stage_name in sorted(observed_stage_names):
        expected = declared.get(stage_name, {"inputs": [], "outputs": []})
        actual = recorded.get(stage_name, {
            "inputs": set(), "outputs": set(), "changed": set(),
            "input_exists": {}, "attempts": set(),
        })
        expected_inputs, actual_inputs = set(expected["inputs"]), actual["inputs"]
        expected_outputs, actual_outputs = set(expected["outputs"]), actual["outputs"]
        changed = actual["changed"]

        item = {
            "stage_name": stage_name,
            "declared_inputs": sorted(expected_inputs),
            "recorded_inputs": sorted(actual_inputs),
            "missing_recorded_inputs": sorted(expected_inputs - actual_inputs),
            "unexpected_recorded_inputs": sorted(actual_inputs - expected_inputs),
            "inputs_recorded_missing_on_disk": sorted(
                path for path, exists in actual["input_exists"].items() if not exists
            ),
            "declared_outputs": sorted(expected_outputs),
            "recorded_declared_outputs": sorted(actual_outputs),
            "missing_recorded_outputs": sorted(expected_outputs - actual_outputs),
            "unexpected_recorded_outputs": sorted(actual_outputs - expected_outputs),
            "observed_changed_outputs": sorted(changed),
            "unexpected_changed_outputs": sorted(changed - expected_outputs),
            "declared_outputs_without_detected_change": sorted(expected_outputs - changed),
            "attempt_numbers": sorted(actual["attempts"]),
        }
        stage_reports.append(item)

    mismatch_fields = (
        "missing_recorded_inputs", "unexpected_recorded_inputs",
        "inputs_recorded_missing_on_disk", "missing_recorded_outputs",
        "unexpected_recorded_outputs", "unexpected_changed_outputs",
    )
    issues = sum(
        len(stage[field])
        for stage in stage_reports
        for field in mismatch_fields
    )
    return {
        "run": dict(run),
        "analysis_type": "RUNTIME_LINEAGE_RECONCILIATION",
        "status": "PASS" if issues == 0 else "WARN",
        "summary": {
            "stages_analyzed": len(stage_reports),
            "stages_not_observed": len(set(declared) - observed_stage_names),
            "declared_input_mismatches": sum(
                len(s["missing_recorded_inputs"]) + len(s["unexpected_recorded_inputs"])
                for s in stage_reports
            ),
            "declared_output_mismatches": sum(
                len(s["missing_recorded_outputs"]) + len(s["unexpected_recorded_outputs"])
                for s in stage_reports
            ),
            "inputs_missing_on_disk": sum(
                len(s["inputs_recorded_missing_on_disk"]) for s in stage_reports
            ),
            "unexpected_changed_outputs": sum(
                len(s["unexpected_changed_outputs"]) for s in stage_reports
            ),
            "declared_outputs_without_detected_change": sum(
                len(s["declared_outputs_without_detected_change"]) for s in stage_reports
            ),
        },
        "stages": stage_reports,
        "uninstrumented_stages": sorted(set(declared) - observed_stage_names),
        "limitations": [
            "Declared input/output observations reflect configuration, not proof of physical reads or writes.",
            "A missing change observation does not prove an output was not written; the file may be unchanged.",
            "Changed-file detection is based on the pipeline's snapshot mechanism.",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Reconcile declared lineage with recorded runtime observations."
    )
    parser.add_argument("--run-id", help="Specific pipeline run ID; defaults to latest.")
    parser.add_argument("--json", action="store_true", help="Print full JSON report.")
    args = parser.parse_args(argv)

    try:
        report = build_report(run_id=args.run_id)
    except (ValueError, sqlite3.Error, OSError, json.JSONDecodeError) as exc:
        parser.exit(2, f"ERROR: {exc}\n")

    if args.json:
        print(json.dumps(report, indent=2))
        return 0

    print(f"Run: {report['run']['run_id']}")
    print(f"Pipeline status: {report['run']['status']}")
    print(f"Reconciliation: {report['status']}")
    print(f"Stages analyzed: {report['summary']['stages_analyzed']}")
    print(f"Stages without observations: {report['summary']['stages_not_observed']}")
    if report["uninstrumented_stages"]:
        print("Not instrumented: " + ", ".join(report["uninstrumented_stages"]))
    for key, value in report["summary"].items():
        if key != "stages_analyzed":
            print(f"{key}: {value}")
    for stage in report["stages"]:
        print(f"\n{stage['stage_name']}")
        print(f"  Missing inputs: {stage['missing_recorded_inputs']}")
        print(f"  Unexpected inputs: {stage['unexpected_recorded_inputs']}")
        print(f"  Missing declared outputs: {stage['missing_recorded_outputs']}")
        print(f"  Unexpected changed outputs: {stage['unexpected_changed_outputs']}")
    return 0 if report["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
