"""7 test scenarios for the Issue #5 reconciliation script.

Per v7 plan §"Test plan (minimum 5 scenarios in dev account)" — extended to 7:
  1. Case A: source row exists, destination absent → destination matches source
  2. Case B: source counter > destination → destination = MAX after merge
  3. Case B: destination has nullable field set, source has same field unset → destination retains
  4. Case B inverse: source has outcome="qualified", dest has outcome=null → dest becomes "qualified"
  5. Case C: destination has rows in window with no source → dest untouched
  6. Idempotency: run twice → second run is a no-op
  7. Dry-run: nothing written; counts still reported

Uses moto mock_aws / mock_dynamodb to spin up real DDB tables in-memory.
"""
import os
import pytest
from moto import mock_dynamodb
import boto3

# Import sut after moto fixture activates so all SDK calls land on the mock
from reconcile import reconcile, merge_rows


SOURCE_TABLE = "src-session-summaries"
DEST_TABLE = "dest-session-summaries"
TENANT = "my87674d777bf9"
PK = f"TENANT#{TENANT}"
WINDOW_START = "2026-05-04T20:00:00.000Z"
WINDOW_END = "2026-05-04T20:05:00.000Z"
INSIDE_WINDOW = "2026-05-04T20:02:30.000Z"
AFTER_WINDOW = "2026-05-04T20:10:00.000Z"


@pytest.fixture
def ddb():
    """Spin up source + destination tables with the prod schema (pk hash + sk range)."""
    with mock_dynamodb():
        client = boto3.resource("dynamodb", region_name="us-east-1")
        for name in (SOURCE_TABLE, DEST_TABLE):
            client.create_table(
                TableName=name,
                KeySchema=[
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "sk", "KeyType": "RANGE"},
                ],
                AttributeDefinitions=[
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "sk", "AttributeType": "S"},
                ],
                BillingMode="PAY_PER_REQUEST",
            )
        yield client


def _put(ddb, table_name, **fields):
    """Helper: write a row to one of the tables."""
    item = {"pk": PK, **fields}
    ddb.Table(table_name).put_item(Item=item)
    return item


def _get(ddb, table_name, sk):
    return ddb.Table(table_name).get_item(Key={"pk": PK, "sk": sk}).get("Item")


# ── Scenario 1: Case A — source-only row copied ──────────────────────────────

def test_case_a_source_only_copied(ddb):
    src = _put(
        ddb, SOURCE_TABLE,
        sk="SESSION#sess_a", started_at=INSIDE_WINDOW, ended_at=INSIDE_WINDOW,
        message_count=3, outcome="conversation", session_id="sess_a",
    )
    case_a, case_b, case_c = reconcile(
        ddb, SOURCE_TABLE, DEST_TABLE, TENANT, WINDOW_START, WINDOW_END,
    )
    assert (case_a, case_b, case_c) == (1, 0, 0)
    dest_row = _get(ddb, DEST_TABLE, "SESSION#sess_a")
    assert dest_row == src


# ── Scenario 2: Case B — counter MAX ──────────────────────────────────────────

def test_case_b_source_counter_higher_dest_takes_max(ddb):
    _put(
        ddb, SOURCE_TABLE,
        sk="SESSION#sess_b", started_at=INSIDE_WINDOW, ended_at=INSIDE_WINDOW,
        message_count=8, response_count=4, total_response_time_ms=6000,
    )
    _put(
        ddb, DEST_TABLE,
        sk="SESSION#sess_b", started_at=INSIDE_WINDOW, ended_at=INSIDE_WINDOW,
        message_count=5, response_count=2, total_response_time_ms=3000,
    )
    case_a, case_b, case_c = reconcile(
        ddb, SOURCE_TABLE, DEST_TABLE, TENANT, WINDOW_START, WINDOW_END,
    )
    assert (case_a, case_b, case_c) == (0, 1, 0)
    merged = _get(ddb, DEST_TABLE, "SESSION#sess_b")
    assert merged["message_count"] == 8
    assert merged["response_count"] == 4
    assert merged["total_response_time_ms"] == 6000


# ── Scenario 3: Case B — destination retains its nullable when source unset ──

def test_case_b_dest_keeps_set_field_when_source_unset(ddb):
    _put(
        ddb, SOURCE_TABLE,
        sk="SESSION#sess_c", started_at=INSIDE_WINDOW, ended_at=INSIDE_WINDOW,
        message_count=3,
        # outcome NOT set on source
    )
    _put(
        ddb, DEST_TABLE,
        sk="SESSION#sess_c", started_at=INSIDE_WINDOW, ended_at=INSIDE_WINDOW,
        message_count=3, outcome="form_completed",
    )
    reconcile(ddb, SOURCE_TABLE, DEST_TABLE, TENANT, WINDOW_START, WINDOW_END)
    merged = _get(ddb, DEST_TABLE, "SESSION#sess_c")
    assert merged["outcome"] == "form_completed"


# ── Scenario 4: Case B inverse — dest gets source's nullable when dest unset ──

def test_case_b_inverse_source_outcome_fills_dest(ddb):
    _put(
        ddb, SOURCE_TABLE,
        sk="SESSION#sess_d", started_at=INSIDE_WINDOW, ended_at=INSIDE_WINDOW,
        message_count=3, outcome="qualified",
    )
    _put(
        ddb, DEST_TABLE,
        sk="SESSION#sess_d", started_at=INSIDE_WINDOW, ended_at=INSIDE_WINDOW,
        message_count=3,  # outcome is unset on dest
    )
    reconcile(ddb, SOURCE_TABLE, DEST_TABLE, TENANT, WINDOW_START, WINDOW_END)
    merged = _get(ddb, DEST_TABLE, "SESSION#sess_d")
    assert merged["outcome"] == "qualified"


# ── Scenario 5: Case C — dest-only row untouched ─────────────────────────────

def test_case_c_dest_only_in_window_untouched(ddb):
    """Destination has a row with no matching source counterpart in the window."""
    _put(
        ddb, DEST_TABLE,
        sk="SESSION#sess_e", started_at=INSIDE_WINDOW, ended_at=INSIDE_WINDOW,
        message_count=7, outcome="conversation",
    )
    case_a, case_b, case_c = reconcile(
        ddb, SOURCE_TABLE, DEST_TABLE, TENANT, WINDOW_START, WINDOW_END,
    )
    assert (case_a, case_b, case_c) == (0, 0, 1)
    dest_row = _get(ddb, DEST_TABLE, "SESSION#sess_e")
    assert dest_row["message_count"] == 7  # untouched
    assert dest_row["outcome"] == "conversation"


# ── Scenario 6: Idempotency — second run is a no-op ──────────────────────────

def test_idempotency_second_run_no_changes(ddb):
    _put(
        ddb, SOURCE_TABLE,
        sk="SESSION#sess_f", started_at=INSIDE_WINDOW, ended_at=INSIDE_WINDOW,
        message_count=4, outcome="qualified", session_id="sess_f",
    )
    # First pass
    reconcile(ddb, SOURCE_TABLE, DEST_TABLE, TENANT, WINDOW_START, WINDOW_END)
    after_first = _get(ddb, DEST_TABLE, "SESSION#sess_f")

    # Second pass — should be Case B no-op
    case_a, case_b, case_c = reconcile(
        ddb, SOURCE_TABLE, DEST_TABLE, TENANT, WINDOW_START, WINDOW_END,
    )
    assert (case_a, case_b, case_c) == (0, 1, 0)
    after_second = _get(ddb, DEST_TABLE, "SESSION#sess_f")
    assert after_second == after_first  # destination state identical


# ── Scenario 7: Dry-run — no destination writes ──────────────────────────────

def test_dry_run_does_not_modify_destination(ddb):
    _put(
        ddb, SOURCE_TABLE,
        sk="SESSION#sess_g", started_at=INSIDE_WINDOW, ended_at=INSIDE_WINDOW,
        message_count=2, outcome="conversation",
    )
    case_a, case_b, case_c = reconcile(
        ddb, SOURCE_TABLE, DEST_TABLE, TENANT, WINDOW_START, WINDOW_END,
        dry_run=True,
    )
    assert (case_a, case_b, case_c) == (1, 0, 0)
    # Crucially, destination should still be empty:
    assert _get(ddb, DEST_TABLE, "SESSION#sess_g") is None


# ── Bonus invariants ─────────────────────────────────────────────────────────

def test_started_at_min_ended_at_max_in_merge():
    """merge_rows: started_at takes MIN, ended_at takes MAX. Pure logic."""
    src = {
        "pk": PK, "sk": "SESSION#x",
        "started_at": "2026-05-04T20:00:00.000Z",
        "ended_at": "2026-05-04T20:01:00.000Z",
    }
    dest = {
        "pk": PK, "sk": "SESSION#x",
        "started_at": "2026-05-04T20:00:30.000Z",
        "ended_at": "2026-05-04T20:00:45.000Z",
    }
    merged = merge_rows(src, dest)
    assert merged["started_at"] == "2026-05-04T20:00:00.000Z"  # MIN
    assert merged["ended_at"] == "2026-05-04T20:01:00.000Z"   # MAX


def test_window_excludes_rows_outside(ddb):
    """A source row outside the cutover window must not be touched."""
    _put(
        ddb, SOURCE_TABLE,
        sk="SESSION#outside", started_at=AFTER_WINDOW, ended_at=AFTER_WINDOW,
        message_count=99,
    )
    case_a, case_b, case_c = reconcile(
        ddb, SOURCE_TABLE, DEST_TABLE, TENANT, WINDOW_START, WINDOW_END,
    )
    assert (case_a, case_b, case_c) == (0, 0, 0)
    assert _get(ddb, DEST_TABLE, "SESSION#outside") is None
