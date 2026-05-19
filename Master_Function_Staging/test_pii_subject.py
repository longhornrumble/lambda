"""Pytest tests for pii_subject.py (Consumer PII Remediation Path A, Phase 1).

Covers the deterministic normalization spec (PII Identity Contract §4 — Gmail-only
aliasing per audit 2026-05-18 #6/option A), email extraction incl. tie-breaks,
the get-or-create flow incl. the race + degenerate + throttle paths, the
best-effort never-fatal guarantee, the empty-index-row guard (#7), the lazy
``_table()`` path, and the forward-compatible reader (``read_subject_id``)
exercised against an old-shape record (CLAUDE.md schema discipline, #2).
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).parent))

import pii_subject  # noqa: E402
from pii_subject import (  # noqa: E402
    _MAX_INDEX_ATTEMPTS,
    extract_email,
    get_or_create_pii_subject_id,
    mint_pii_subject_id,
    normalize_email,
    read_subject_id,
)


def _ccf():
    return ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
    )


# ── normalization spec (#6: Gmail-only aliasing) ────────────────────────────
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Foo.Bar@Gmail.com", "foobar@gmail.com"),            # gmail: lower+dropdots
        ("foo.bar+tag@googlemail.com", "foobar@gmail.com"),   # googlemail alias+plus
        ("a.b.c@gmail.com", "abc@gmail.com"),
        ("x+promo@gmail.com", "x@gmail.com"),                 # gmail plus stripped
        ("CASE@Test.io", "case@test.io"),                     # non-gmail: lower only
        ("Keep.Dots@example.com", "keep.dots@example.com"),   # non-gmail keeps dots
        # #6 option A: non-Gmail +tag is NOT stripped (closes imposter vector)
        ("  User+promo@Example.COM  ", "user+promo@example.com"),
        ("alice+work@acme.com", "alice+work@acme.com"),
        ("bob+x@outlook.com", "bob+x@outlook.com"),
    ],
)
def test_normalize_email_deterministic(raw, expected):
    assert normalize_email(raw) == expected
    assert normalize_email(raw) == normalize_email(raw)  # idempotent / pure


@pytest.mark.parametrize(
    "bad",
    [None, "", "   ", "noatsign", "@nolocal.com", "local@", "a@b@gmail.com",
     "a b@gmail.com", "a\tb@example.com"],  # incl. internal-whitespace (R1)
)
def test_normalize_email_invalid_returns_none(bad):
    assert normalize_email(bad) is None


def test_normalize_email_non_gmail_plus_preserved():
    # #6 option A: non-Gmail '+' is preserved (not a provider-guaranteed alias).
    assert normalize_email("+@example.com") == "+@example.com"
    assert normalize_email("user+tag@example.com") == "user+tag@example.com"


def test_normalize_gmail_plus_strip_semantics():
    # Gmail strips from the first '+', so a LEADING '+' empties the local -> None.
    assert normalize_email("+tag@gmail.com") is None
    assert normalize_email("+@gmail.com") is None
    assert normalize_email("a+tag@gmail.com") == "a@gmail.com"


# ── email extraction ────────────────────────────────────────────────────────
def test_extract_email_prefers_named_key():
    assert extract_email({"Email Address": "x@y.com", "note": "z@w.com"}) == "x@y.com"


def test_extract_email_falls_back_to_value_scan():
    assert extract_email({"q1": "hello", "q2": "find@me.org"}) == "find@me.org"


def test_extract_email_none_when_absent():
    assert extract_email({"name": "Jane", "age": 30}) is None
    assert extract_email("not a dict") is None


def test_extract_email_tie_break_is_insertion_order():
    # #10 N1: documented contract — first email-shaped value in insertion order.
    assert extract_email({"a": "first@x.com", "b": "second@y.com"}) == "first@x.com"


def test_extract_email_substring_key_hint_matches():
    # #10 N2: documented — 'email' substring in any key triggers the named-key path.
    assert extract_email({"applicant_email_field": "hit@x.com"}) == "hit@x.com"


def test_extract_email_nested_dict_value_is_ignored():
    # #10 N3: documented scope decision — nested objects are not scanned.
    assert extract_email({"contact": {"email": "nested@z.com"}}) is None


# ── get-or-create flow ──────────────────────────────────────────────────────
def test_mint_format():
    sid = mint_pii_subject_id()
    assert sid.startswith("psub_") and len(sid) == 5 + 32


def test_get_or_create_no_email_mints_unindexed():
    tbl = MagicMock()
    sid = get_or_create_pii_subject_id("T1", {"name": "Jane"}, table=tbl)
    assert sid.startswith("psub_")
    tbl.get_item.assert_not_called()
    tbl.put_item.assert_not_called()


def test_get_or_create_reuses_existing():
    tbl = MagicMock()
    tbl.get_item.return_value = {"Item": {"pii_subject_id": "psub_existing"}}
    sid = get_or_create_pii_subject_id("T1", {"email": "a@b.com"}, table=tbl)
    assert sid == "psub_existing"
    tbl.put_item.assert_not_called()


def test_get_or_create_mints_and_indexes_new():
    tbl = MagicMock()
    tbl.get_item.return_value = {}
    sid = get_or_create_pii_subject_id("T1", {"email": "New@b.com"}, table=tbl)
    assert sid.startswith("psub_")
    args = tbl.put_item.call_args.kwargs
    assert args["Item"]["normalized_email"] == "new@b.com"
    assert args["Item"]["tenant_id"] == "T1"
    assert args["Item"]["pii_subject_id"] == sid
    assert "attribute_not_exists" in args["ConditionExpression"]
    # attempt-0 read is intentionally eventually-consistent
    assert tbl.get_item.call_args_list[0].kwargs.get("ConsistentRead") is False


def test_get_or_create_empty_index_value_not_reused(tmp_path):
    """#7: a corrupted/empty pii_subject_id in the index must NOT be reused and
    must NOT spin the loop forever — it falls to the unindexed best-effort id."""
    tbl = MagicMock()
    tbl.get_item.return_value = {"Item": {"pii_subject_id": ""}}  # corrupted row
    tbl.put_item.side_effect = _ccf()  # row exists, conditional put always fails
    sid = get_or_create_pii_subject_id("T1", {"email": "a@b.com"}, table=tbl)
    assert sid.startswith("psub_") and sid != ""
    assert tbl.get_item.call_count == _MAX_INDEX_ATTEMPTS  # bounded, no infinite loop
    assert tbl.put_item.call_count == _MAX_INDEX_ATTEMPTS  # every iteration tried a write


def test_get_or_create_non_ccf_clienterror_on_first_get_is_non_fatal():
    """R9: AccessDenied on the very FIRST index read -> best-effort usable id
    (the operationally-significant IAM-gap failure mode)."""
    tbl = MagicMock()
    tbl.get_item.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException"}}, "GetItem")
    sid = get_or_create_pii_subject_id("T1", {"email": "a@b.com"}, table=tbl)
    assert sid.startswith("psub_")


def test_get_or_create_race_returns_winner():
    """B1: lost race -> next iteration's STRONGLY-CONSISTENT read returns the
    winner's id. Never a divergent id for an already-indexed person."""
    tbl = MagicMock()
    tbl.get_item.side_effect = [
        {},                                               # attempt 0: absent (eventual)
        {"Item": {"pii_subject_id": "psub_winner"}},       # attempt 1: winner (consistent)
    ]
    tbl.put_item.side_effect = _ccf()
    sid = get_or_create_pii_subject_id("T1", {"email": "r@b.com"}, table=tbl)
    assert sid == "psub_winner"
    assert tbl.get_item.call_args_list[0].kwargs.get("ConsistentRead") is False
    assert tbl.get_item.call_args_list[1].kwargs.get("ConsistentRead") is True
    assert tbl.put_item.call_count == 1


def test_get_or_create_race_unresolved_is_unindexed_not_divergent():
    """B1 degenerate tail: bounded retry exhausts -> UNINDEXED best-effort id,
    never raises, never a silent divergent index entry. Retries are consistent."""
    tbl = MagicMock()
    tbl.get_item.return_value = {}                         # always absent
    tbl.put_item.side_effect = _ccf()
    sid = get_or_create_pii_subject_id("T1", {"email": "r@b.com"}, table=tbl)
    assert sid.startswith("psub_")
    assert tbl.put_item.call_count == _MAX_INDEX_ATTEMPTS
    assert tbl.get_item.call_count == _MAX_INDEX_ATTEMPTS
    # every retry after attempt 0 must be strongly consistent
    for call in tbl.get_item.call_args_list[1:]:
        assert call.kwargs.get("ConsistentRead") is True


@pytest.mark.parametrize("code", ["AccessDeniedException",
                                  "ProvisionedThroughputExceededException"])
def test_get_or_create_non_ccf_clienterror_on_put_is_non_fatal(code):
    """#10 S1: AccessDenied / throttle on put -> best-effort usable id, no crash.
    (Documents the deliberate tradeoff: under sustained throttle the row is
    UNINDEXED — incomplete-deletion risk closed only by the Phase-2 orphan
    sweep.)"""
    tbl = MagicMock()
    tbl.get_item.return_value = {}
    tbl.put_item.side_effect = ClientError({"Error": {"Code": code}}, "PutItem")
    sid = get_or_create_pii_subject_id("T1", {"email": "a@b.com"}, table=tbl)
    assert sid.startswith("psub_")


def test_get_or_create_throttle_on_consistent_get_is_non_fatal():
    """#10 S2: throttle on the post-race strongly-consistent get -> best-effort."""
    tbl = MagicMock()
    tbl.get_item.side_effect = [
        {},
        ClientError({"Error": {"Code": "ProvisionedThroughputExceededException"}},
                    "GetItem"),
    ]
    tbl.put_item.side_effect = _ccf()
    sid = get_or_create_pii_subject_id("T1", {"email": "a@b.com"}, table=tbl)
    assert sid.startswith("psub_")


def test_get_or_create_index_error_is_non_fatal():
    tbl = MagicMock()
    tbl.get_item.side_effect = RuntimeError("dynamo down")
    sid = get_or_create_pii_subject_id("T1", {"email": "a@b.com"}, table=tbl)
    assert sid.startswith("psub_")  # best-effort: still returns a usable id


def test_get_or_create_uses_lazy_table_when_not_injected():
    """#10 S4: the production path (no table= kwarg) goes through _table() ->
    boto3.resource. Patch it so the real lazy path is exercised, not bypassed."""
    pii_subject._dynamodb = None  # reset module singleton (N-1 hygiene)
    fake_tbl = MagicMock()
    fake_tbl.get_item.return_value = {}
    fake_resource = MagicMock()
    fake_resource.Table.return_value = fake_tbl
    with patch("pii_subject.boto3.resource", return_value=fake_resource) as res:
        sid = get_or_create_pii_subject_id("T1", {"email": "lazy@b.com"})
    assert sid.startswith("psub_")
    res.assert_called_once_with("dynamodb")
    fake_resource.Table.assert_called_once_with(pii_subject.PII_SUBJECT_INDEX_TABLE)
    pii_subject._dynamodb = None  # teardown: don't leak to other tests


# ── forward-compatible reader (#2 — exercises real code, not a dict literal) ─
def test_read_subject_id_old_shape_returns_none():
    """CLAUDE.md schema discipline: read_subject_id is the canonical reader.
    A pre-Phase-1 row (no pii_subject_id) must yield None, never raise."""
    old_shape = {
        "submission_id": "11111111-1111-1111-1111-111111111111",
        "tenant_id": "TENANT_A",
        "form_id": "volunteer_application",
        "form_data": {"email": "person@example.com", "name": "Jane"},
        "status": "pending_fulfillment",
        # no 'pii_subject_id' — the pre-migration shape
    }
    assert read_subject_id(old_shape) is None          # reader tolerates old shape


def test_read_subject_id_new_shape_returns_value():
    assert read_subject_id({"pii_subject_id": "psub_abc"}) == "psub_abc"


@pytest.mark.parametrize("bad_record", [None, "str", 123, [], {"pii_subject_id": ""},
                                        {"pii_subject_id": None},
                                        {"pii_subject_id": 42}])
def test_read_subject_id_robust_to_garbage(bad_record):
    # Never raises; empty/None/non-str/non-dict all collapse to None.
    assert read_subject_id(bad_record) is None
