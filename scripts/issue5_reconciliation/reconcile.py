#!/usr/bin/env python3
"""Issue #5 reconciliation script — Phase 1 deliverable.

Merges session-summary rows from a source table into a destination table for
sessions whose `started_at` falls within the Phase 2 split-write cutover
window. Per v7 plan §"Reconciliation script spec":

  - Source: legacy table (e.g. picasso-session-summaries)
  - Destination: new table (e.g. picasso-session-summaries-prod)
  - Cutover window: [cutover_start_ms, cutover_end_ms]
  - Tenant scope: one tenant_hash per run (IAM LeadingKeys enforced)

Three-case merge logic (idempotent):
  - Case A — destination row absent → copy from source
  - Case B — both rows present → field-wise merge:
              MAX of counter fields, MIN of started_at, MAX of ended_at,
              "if not null" of single-value string fields (outcome,
              first_question, form_id, etc.).
  - Case C — destination present, source absent → DO NOT touch destination.

Validation step emits source-only / destination-only / both-merged counts so
operators inspect breakdowns before approving.

Usage:
  python reconcile.py \
    --source-table picasso-session-summaries \
    --dest-table picasso-session-summaries-prod \
    --tenant-hash my87674d777bf9 \
    --cutover-start-ms 1735689600000 \
    --cutover-end-ms 1735693200000 \
    --region us-east-1 \
    [--dry-run]

Exits 0 on clean completion; non-zero on any failure (operator must investigate).
"""
import argparse
import json
import logging
import sys
from typing import Any, Dict, List, Optional, Tuple

import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger("issue5_reconciliation")

# Counter fields are accumulators; merge by MAX (the higher value reflects
# the latest write, since both writers ADD to the same row during cutover).
COUNTER_FIELDS = (
    "message_count",
    "user_message_count",
    "bot_message_count",
    "response_count",
    "total_response_time_ms",
)

# Single-value string fields: prefer the non-null one. If both are non-null,
# keep destination's value (it's the more recent of the two write paths).
NULLABLE_STRING_FIELDS = (
    "outcome",
    "first_question",
    "form_id",
    "tenant_id",
    "session_id",
)


def emit(event: str, **fields: Any) -> None:
    """One structured log line for every action — operators audit by grep."""
    logger.info(json.dumps({"evt": event, **fields}))


def query_source_window(
    ddb,
    source_table: str,
    tenant_hash: str,
    cutover_start_iso: str,
    cutover_end_iso: str,
) -> List[Dict[str, Any]]:
    """Query source table for one tenant's rows whose started_at is inside the window.

    Uses Query (not Scan) so IAM LeadingKeys condition can enforce tenant scope.
    FilterExpression narrows the result set to the cutover window — applied after
    the partition is loaded, so this is best for short windows (< 1h typically).
    """
    table = ddb.Table(source_table)
    items: List[Dict[str, Any]] = []
    last_evaluated_key: Optional[Dict[str, Any]] = None
    while True:
        kwargs: Dict[str, Any] = {
            "KeyConditionExpression": Key("pk").eq(f"TENANT#{tenant_hash}"),
            "FilterExpression": "started_at BETWEEN :start AND :end",
            "ExpressionAttributeValues": {
                ":start": cutover_start_iso,
                ":end": cutover_end_iso,
            },
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
    return items


def get_dest_row(ddb, dest_table: str, pk: str, sk: str) -> Optional[Dict[str, Any]]:
    table = ddb.Table(dest_table)
    resp = table.get_item(Key={"pk": pk, "sk": sk})
    return resp.get("Item")


def merge_rows(source: Dict[str, Any], dest: Dict[str, Any]) -> Dict[str, Any]:
    """Case B field-wise merge. Returns the merged row to write to destination.

    Counters → MAX (both writers ADD; the higher value is the latest).
    started_at → MIN (earliest start wins).
    ended_at → MAX (latest end wins).
    Nullable strings → if-not-null preference, destination wins on tie
    (more recent write path).
    """
    merged = dict(dest)  # start with destination as base
    for field in COUNTER_FIELDS:
        s = source.get(field)
        d = dest.get(field)
        if s is None and d is None:
            continue
        if s is None:
            merged[field] = d
        elif d is None:
            merged[field] = s
        else:
            merged[field] = max(int(s), int(d))
    # started_at MIN
    s_start = source.get("started_at")
    d_start = dest.get("started_at")
    if s_start and d_start:
        merged["started_at"] = min(s_start, d_start)
    elif s_start:
        merged["started_at"] = s_start
    # ended_at MAX
    s_end = source.get("ended_at")
    d_end = dest.get("ended_at")
    if s_end and d_end:
        merged["ended_at"] = max(s_end, d_end)
    elif s_end:
        merged["ended_at"] = s_end
    # Nullable string fields — prefer destination's value if set; fall back to source
    for field in NULLABLE_STRING_FIELDS:
        if not merged.get(field) and source.get(field):
            merged[field] = source[field]
    return merged


def reconcile(
    ddb,
    source_table: str,
    dest_table: str,
    tenant_hash: str,
    cutover_start_iso: str,
    cutover_end_iso: str,
    dry_run: bool = False,
) -> Tuple[int, int, int]:
    """Run the reconciliation pass. Returns (case_a, case_b, case_c) counts.

    Case A: source-only rows we copied to destination (or would have, dry-run)
    Case B: both rows existed, we merged
    Case C: destination has rows in window with no source counterpart — left untouched

    Idempotency: re-running with same inputs produces identical destination state.
    Case B's merge is purely a function of the two row contents; A and C are no-ops
    on second run since destination then matches source.
    """
    source_rows = query_source_window(
        ddb, source_table, tenant_hash, cutover_start_iso, cutover_end_iso
    )
    emit(
        "reconcile_query_complete",
        source_table=source_table,
        tenant_hash=tenant_hash,
        cutover_start=cutover_start_iso,
        cutover_end=cutover_end_iso,
        source_rows=len(source_rows),
    )

    case_a = case_b = 0
    dest_table_resource = ddb.Table(dest_table)

    for source in source_rows:
        pk = source["pk"]
        sk = source["sk"]
        dest = get_dest_row(ddb, dest_table, pk, sk)

        if dest is None:
            # Case A — copy source into destination
            emit("reconcile_case_a_copy", pk=pk, sk=sk, dry_run=dry_run)
            if not dry_run:
                dest_table_resource.put_item(Item=source)
            case_a += 1
        else:
            # Case B — merge
            merged = merge_rows(source, dest)
            if merged != dest:
                emit("reconcile_case_b_merge", pk=pk, sk=sk, dry_run=dry_run)
                if not dry_run:
                    dest_table_resource.put_item(Item=merged)
            else:
                emit("reconcile_case_b_noop", pk=pk, sk=sk)
            case_b += 1

    # Case C is implicit: any destination rows in the window without a source
    # counterpart are LEFT UNTOUCHED. We surface the count by scanning the
    # destination's same window — operator inspects qualitatively, mismatch
    # may be the legitimate happy path for sessions started after cutover_end.
    case_c = _count_dest_only(
        ddb,
        dest_table,
        tenant_hash,
        cutover_start_iso,
        cutover_end_iso,
        source_keys={(r["pk"], r["sk"]) for r in source_rows},
    )

    emit(
        "reconcile_complete",
        case_a_source_only_copied=case_a,
        case_b_both_existed_merged=case_b,
        case_c_dest_only_untouched=case_c,
        dry_run=dry_run,
    )
    return case_a, case_b, case_c


def _count_dest_only(
    ddb,
    dest_table: str,
    tenant_hash: str,
    cutover_start_iso: str,
    cutover_end_iso: str,
    source_keys: set,
) -> int:
    """Count destination rows in the window that have no source counterpart."""
    table = ddb.Table(dest_table)
    count = 0
    last_evaluated_key = None
    while True:
        kwargs: Dict[str, Any] = {
            "KeyConditionExpression": Key("pk").eq(f"TENANT#{tenant_hash}"),
            "FilterExpression": "started_at BETWEEN :start AND :end",
            "ExpressionAttributeValues": {
                ":start": cutover_start_iso,
                ":end": cutover_end_iso,
            },
        }
        if last_evaluated_key:
            kwargs["ExclusiveStartKey"] = last_evaluated_key
        resp = table.query(**kwargs)
        for item in resp.get("Items", []):
            if (item["pk"], item["sk"]) not in source_keys:
                count += 1
        last_evaluated_key = resp.get("LastEvaluatedKey")
        if not last_evaluated_key:
            break
    return count


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-table", required=True)
    parser.add_argument("--dest-table", required=True)
    parser.add_argument("--tenant-hash", required=True)
    parser.add_argument("--cutover-start-iso", required=True,
                        help="ISO-8601 timestamp (UTC, with 'Z') for cutover window start")
    parser.add_argument("--cutover-end-iso", required=True,
                        help="ISO-8601 timestamp (UTC, with 'Z') for cutover window end")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would be written without modifying destination table")
    parser.add_argument("--profile", default=None,
                        help="AWS profile name (defaults to environment credentials)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    ddb = session.resource("dynamodb", region_name=args.region)

    try:
        case_a, case_b, case_c = reconcile(
            ddb,
            source_table=args.source_table,
            dest_table=args.dest_table,
            tenant_hash=args.tenant_hash,
            cutover_start_iso=args.cutover_start_iso,
            cutover_end_iso=args.cutover_end_iso,
            dry_run=args.dry_run,
        )
        # Print human summary too
        print(f"\nReconciliation summary (tenant={args.tenant_hash}, dry_run={args.dry_run}):")
        print(f"  Case A — source-only rows {'would-be ' if args.dry_run else ''}copied: {case_a}")
        print(f"  Case B — both-existed rows merged:                     {case_b}")
        print(f"  Case C — destination-only rows (untouched):            {case_c}")
        return 0
    except Exception as exc:  # noqa: BLE001
        emit("reconcile_failed", error=type(exc).__name__, message=str(exc))
        logger.error("Reconciliation failed: %s: %s", type(exc).__name__, exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
