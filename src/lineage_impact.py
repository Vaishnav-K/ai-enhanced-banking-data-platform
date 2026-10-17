"""Read-only downstream impact analysis based on declared lineage."""
import argparse
import json
from collections import deque
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = ROOT / "config" / "data_lineage.json"


def load_graph(config_path=DEFAULT_CONFIG):
    try:
        config = json.loads(Path(config_path).read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot load lineage configuration: {exc}") from exc

    if not isinstance(config, dict) or config.get("version") != 1:
        raise ValueError("Lineage configuration must be an object with version 1.")

    nodes = config.get("nodes")
    edges = config.get("edges")
    if not isinstance(nodes, list) or not isinstance(edges, list):
        raise ValueError("Lineage configuration requires nodes and edges lists.")

    by_id = {}
    for node in nodes:
        if not isinstance(node, dict) or not isinstance(node.get("id"), str):
            raise ValueError("Every node must be an object with a string id.")
        node_id = node["id"]
        if not node_id or node_id in by_id:
            raise ValueError(f"Empty or duplicate node id: {node_id!r}")
        if node.get("type") not in {"artifact", "stage"}:
            raise ValueError(f"Unsupported node type for {node_id!r}")
        if node["type"] == "artifact" and not isinstance(node.get("path"), str):
            raise ValueError(f"Artifact {node_id!r} requires a path.")
        if node["type"] == "stage" and not isinstance(node.get("stage_name"), str):
            raise ValueError(f"Stage {node_id!r} requires a stage_name.")
        by_id[node_id] = node

    downstream = {node_id: [] for node_id in by_id}
    seen_edges = set()
    for edge in edges:
        if not isinstance(edge, dict):
            raise ValueError("Every edge must be an object.")
        source, target = edge.get("from"), edge.get("to")
        if source not in by_id or target not in by_id:
            raise ValueError(f"Edge references unknown node: {source!r} -> {target!r}")
        if source == target:
            raise ValueError(f"Self-dependency is not allowed: {source!r}")
        pair = (source, target)
        if pair in seen_edges:
            raise ValueError(f"Duplicate edge: {source!r} -> {target!r}")
        seen_edges.add(pair)
        downstream[source].append(target)

    # A cycle would make downstream impact traversal misleading.
    indegree = {node_id: 0 for node_id in by_id}
    for source, targets in downstream.items():
        for target in targets:
            indegree[target] += 1
    queue = deque(node_id for node_id, degree in indegree.items() if degree == 0)
    visited = 0
    while queue:
        source = queue.popleft()
        visited += 1
        for target in downstream[source]:
            indegree[target] -= 1
            if indegree[target] == 0:
                queue.append(target)
    if visited != len(by_id):
        raise ValueError("Lineage graph contains a dependency cycle.")

    return by_id, downstream


def analyze_impact(target, config_path=DEFAULT_CONFIG):
    """Find the transitive downstream impact of a node id or artifact path."""
    nodes, downstream = load_graph(config_path)
    matches = [
        node_id for node_id, node in nodes.items()
        if node_id == target
        or (node["type"] == "artifact" and node["path"] == target)
        or (node["type"] == "stage" and node["stage_name"] == target)
    ]
    if not matches:
        raise ValueError(f"Unknown lineage target: {target}")

    impacted = set()
    queue = deque(matches)
    visited = set(matches)
    while queue:
        current = queue.popleft()
        for child in downstream[current]:
            if child not in visited:
                visited.add(child)
                impacted.add(child)
                queue.append(child)

    impacted_nodes = [nodes[node_id] for node_id in nodes if node_id in impacted]
    return {
        "target": target,
        "matched_nodes": matches,
        "impact_count": len(impacted_nodes),
        "impacted_stages": [
            node["stage_name"] for node in impacted_nodes if node["type"] == "stage"
        ],
        "impacted_artifacts": [
            node["path"] for node in impacted_nodes if node["type"] == "artifact"
        ],
        "impacted_nodes": impacted_nodes,
        "analysis_mode": "DECLARED_LINEAGE",
        "limitations": [
            "Impact is inferred from the declared dependency graph, not runtime proof.",
            "Review the graph when pipeline logic or artifact paths change.",
            "This command is read-only and does not rerun stages or modify data.",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description="Analyze downstream data pipeline impact.")
    parser.add_argument("target", help="Node id, stage name, or exact artifact path.")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--json", action="store_true", help="Print full JSON output.")
    args = parser.parse_args(argv)

    try:
        report = analyze_impact(args.target, args.config)
    except ValueError as exc:
        parser.error(str(exc))

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print("DATA LINEAGE IMPACT REPORT")
        print("=" * 60)
        print(f"Target: {report['target']}")
        print(f"Impacted nodes: {report['impact_count']}")
        print("\nImpacted stages:")
        for stage in report["impacted_stages"]:
            print(f"  - {stage}")
        print("\nImpacted artifacts:")
        for artifact in report["impacted_artifacts"]:
            print(f"  - {artifact}")
        print("\nNote: This is declared potential impact, not runtime proof.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
