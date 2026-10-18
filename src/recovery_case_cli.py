"""Command-line interface for human recovery decision audit history."""

import argparse
import json

import recovery_case_store as store


def build_parser():
    parser = argparse.ArgumentParser(
        description="Record or inspect human recovery decisions. "
                    "This tool never executes recovery actions."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    record = subparsers.add_parser(
        "record", help="Append a human recovery decision"
    )
    record.add_argument("--run-id", required=True)
    record.add_argument(
        "--decision",
        required=True,
        choices=sorted(store.ALLOWED_DECISIONS),
    )
    record.add_argument("--reason", required=True)
    record.add_argument("--recorded-by", default="operator")
    record.add_argument(
        "--evidence-json",
        default="{}",
        help="Optional JSON object containing the evidence snapshot",
    )

    listing = subparsers.add_parser(
        "list", help="List recovery decisions in insertion order"
    )
    listing.add_argument("--run-id")

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "record":
        try:
            evidence = json.loads(args.evidence_json)
        except json.JSONDecodeError as exc:
            parser.error(f"--evidence-json must contain valid JSON: {exc}")

        if not isinstance(evidence, dict):
            parser.error("--evidence-json must be a JSON object")

        try:
            event_id = store.record_recovery_decision(
                run_id=args.run_id,
                decision=args.decision,
                reason=args.reason,
                recorded_by=args.recorded_by,
                evidence_snapshot=evidence,
            )
        except ValueError as exc:
            parser.error(str(exc))

        print(json.dumps({
            "event_id": event_id,
            "run_id": args.run_id,
            "decision": args.decision,
            "recorded": True,
            "automatic_actions_taken": [],
        }, indent=2))
        return 0

    events = store.list_recovery_decisions(args.run_id)
    print(json.dumps({
        "count": len(events),
        "events": events,
        "automatic_actions_taken": [],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
