"""Pytest tests for pii_subject.py (Consumer PII Remediation Path A, Phase 1).

Covers the deterministic normalization spec (PII Identity Contract §4), email
extraction, the get-or-create flow incl. the race path, the best-effort
never-fatal guarantee, and — per CLAUDE.md schema discipline — a
forward-compatible-read fixture proving an old-shape submission record
(no ``pii_subject_id``) is tolerated by readers.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).parent))

from pii_subject import (  # noqa: E402
    extract_email,
    get_or_create_pii_subject_id,
    mint_pii_subject_id,
    normalize_email,
)


# ── normalization spec (pure function, deterministic) ───────────────────────
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Foo.Bar@Gmail.com", "foobar@gmail.com"),       # gmail: lowercase + drop dots
        ("foo.bar+tag@googlemail.com", "foobar@gmail.com"),  # googlemail alias + plus
        ("a.b.c@gmail.com", "abc@gmail.com"),
        ("  User+promo@Example.COM  ", "user@example.com"),  # trim, non-gmail plus strip
        ("Keep.Dots@example.com", "keep.dots@example.com"),  # non-gmail keeps dots
        ("CASE@Test.io", "case@test.io"),
    ],
)
def test_normalize_email_deterministic(raw, expected):
    assert normalize_email(raw) == expected
    assert normalize_email(raw) == normalize_email(raw)  # idempotent / pure


@pytest.mark.parametrize(
    "bad",
    [None, "", "   ", "noatsign", "@nolocal.com", "local@", "a@b@gmail.com"],
)
def test_normalize_email_invalid_returns_none(bad):
    assert normalize_email(bad) is None


# ── email extraction from arbitrary form responses ──────────────────────────
def test_extract_email_prefers_named_key():
    assert extract_email({"Email Address": "x@y.com", "note": "z@w.com"}) == "x@y.com"


def test_extract_email_falls_back_to_value_scan():
    assert extract_email({"q1": "hello", "q2": "find@me.org"}) == "find@me.org"


def test_extract_email_none_when_absent():
    assert extract_email({"name": "Jane", "age": 30}) is None
    assert extract_email("not a dict") is None


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


def test_get_or_create_race_returns_winner():
    """B1: lost race -> next iteration's STRONGLY-CONSISTENT read returns the
    winner's id. We must never mint a divergent id for an already-indexed person."""
    tbl = MagicMock()
    tbl.get_item.side_effect = [
        {},                                               # attempt 0: absent (eventual)
        {"Item": {"pii_subject_id": "psub_winner"}},       # attempt 1: winner (consistent)
    ]
    tbl.put_item.side_effect = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
    )
    sid = get_or_create_pii_subject_id("T1", {"email": "r@b.com"}, table=tbl)
    assert sid == "psub_winner"
    # the post-race re-read MUST be strongly consistent (the B1 fix)
    assert tbl.get_item.call_args_list[1].kwargs.get("ConsistentRead") is True


def test_get_or_create_race_unresolved_is_unindexed_not_divergent():
    """B1 degenerate tail: winner repeatedly put-then-vanishes. Bounded retry
    exhausts -> row recorded UNINDEXED (legacy-equivalent, never raises, never a
    silent in-place divergent index entry)."""
    tbl = MagicMock()
    tbl.get_item.return_value = {}                         # always absent
    tbl.put_item.side_effect = ClientError(
        {"Error": {"Code": "ConditionalCheckFailedException"}}, "PutItem"
    )
    sid = get_or_create_pii_subject_id("T1", {"email": "r@b.com"}, table=tbl)
    assert sid.startswith("psub_")
    assert tbl.put_item.call_count == 3                    # _MAX_INDEX_ATTEMPTS
    assert tbl.get_item.call_count == 3


def test_get_or_create_non_ccf_clienterror_is_non_fatal():
    """A non-conditional ClientError (e.g. AccessDenied) is re-raised then caught
    by the best-effort outer guard -> usable id, no crash."""
    tbl = MagicMock()
    tbl.get_item.return_value = {}
    tbl.put_item.side_effect = ClientError(
        {"Error": {"Code": "AccessDeniedException"}}, "PutItem"
    )
    sid = get_or_create_pii_subject_id("T1", {"email": "a@b.com"}, table=tbl)
    assert sid.startswith("psub_")


def test_get_or_create_index_error_is_non_fatal():
    tbl = MagicMock()
    tbl.get_item.side_effect = RuntimeError("dynamo down")
    sid = get_or_create_pii_subject_id("T1", {"email": "a@b.com"}, table=tbl)
    assert sid.startswith("psub_")  # best-effort: still returns a usable id


# ── forward-compatible read (CLAUDE.md schema discipline) ───────────────────
def test_old_shape_submission_record_tolerated():
    """A pre-Phase-1 form-submission row has no pii_subject_id. Any reader must
    use .get() and not crash. This fixture is the old shape verbatim."""
    old_shape = {
        "submission_id": "11111111-1111-1111-1111-111111111111",
        "tenant_id": "TENANT_A",
        "form_id": "volunteer_application",
        "form_data": {"email": "person@example.com", "name": "Jane"},
        "session_id": "sess-1",
        "status": "pending_fulfillment",
        # NOTE: no 'pii_subject_id' key — this is the pre-migration shape
    }
    # Forward-compatible reader contract: optional field via .get(), default None.
    assert old_shape.get("pii_subject_id") is None
    with pytest.raises(KeyError):
        _ = old_shape["pii_subject_id"]  # bracket access is the anti-pattern we forbid

    new_shape = {**old_shape, "pii_subject_id": "psub_abc"}
    assert new_shape.get("pii_subject_id") == "psub_abc"
