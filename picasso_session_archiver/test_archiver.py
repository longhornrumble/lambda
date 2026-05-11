"""Unit tests for picasso-session-archiver.

Covers:
- TTL-style REMOVE (userIdentity = dynamodb service) → archived
- Manual delete-item REMOVE (no userIdentity) → archived (per archiver comment)
- INSERT / MODIFY → skipped
- REMOVE without OldImage → skipped + warning
- Decimal/set/bytes serialization
- Old-shape OldImage missing optional fields → archived using fallbacks
"""
import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("ARCHIVE_BUCKET", "picasso-archive-staging-test")

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


@pytest.fixture
def archiver(monkeypatch):
    mock_s3 = MagicMock()
    with patch("boto3.client", return_value=mock_s3):
        if "lambda_function" in sys.modules:
            del sys.modules["lambda_function"]
        import lambda_function as mod
        mod.s3 = mock_s3
        yield mod, mock_s3


def _ttl_remove_record(session_id="sess_test_123", extra=None, sequence_number=None):
    image = {
        "session_id": {"S": session_id},
        "tenant_id": {"S": "MYR384719"},
        "started_at": {"S": "2026-01-15T10:30:00Z"},
        "ttl": {"N": "1736947800"},
        "messages_count": {"N": "12"},
    }
    if extra:
        image.update(extra)
    return {
        "eventID": "ev_abc",
        "eventName": "REMOVE",
        "userIdentity": {"type": "Service", "principalId": "dynamodb.amazonaws.com"},
        "dynamodb": {
            "OldImage": image,
            "StreamViewType": "OLD_IMAGE",
            "SequenceNumber": sequence_number or f"seq_{session_id}",
        },
    }


def _manual_remove_record(session_id="sess_test_456"):
    rec = _ttl_remove_record(session_id)
    rec["userIdentity"] = {
        "type": "AssumedRole",
        "principalId": "AIDAEXAMPLE:chris",
    }
    return rec


def test_ttl_remove_is_archived(archiver):
    mod, s3 = archiver
    res = mod.lambda_handler({"Records": [_ttl_remove_record("sess_ttl_001")]}, None)
    assert res["archived"] == 1
    assert res["skipped_non_remove"] == 0
    assert res["batchItemFailures"] == []
    assert s3.put_object.call_count == 1
    call = s3.put_object.call_args.kwargs
    assert call["Bucket"] == "picasso-archive-staging-test"
    assert call["Key"].startswith("sessions/year=")
    assert call["Key"].endswith("sess_ttl_001.json")
    assert call["ContentType"] == "application/json"
    body = json.loads(call["Body"].decode("utf-8"))
    assert body["session_id"] == "sess_ttl_001"
    assert body["tenant_id"] == "MYR384719"


def test_manual_delete_is_also_archived(archiver):
    """Per archiver design comment: we don't filter on userIdentity so the
    Phase 2.7 delete-item verification mechanic works."""
    mod, s3 = archiver
    res = mod.lambda_handler({"Records": [_manual_remove_record("sess_manual_001")]}, None)
    assert res["archived"] == 1
    assert s3.put_object.call_count == 1


def test_insert_is_skipped(archiver):
    mod, s3 = archiver
    rec = _ttl_remove_record()
    rec["eventName"] = "INSERT"
    res = mod.lambda_handler({"Records": [rec]}, None)
    assert res["archived"] == 0
    assert res["skipped_non_remove"] == 1
    assert s3.put_object.call_count == 0


def test_modify_is_skipped(archiver):
    mod, s3 = archiver
    rec = _ttl_remove_record()
    rec["eventName"] = "MODIFY"
    res = mod.lambda_handler({"Records": [rec]}, None)
    assert res["archived"] == 0
    assert res["skipped_non_remove"] == 1


def test_remove_without_old_image_is_skipped(archiver):
    mod, s3 = archiver
    rec = _ttl_remove_record()
    rec["dynamodb"]["OldImage"] = None
    res = mod.lambda_handler({"Records": [rec]}, None)
    assert res["skipped_no_old_image"] == 1
    assert s3.put_object.call_count == 0


def test_old_shape_without_session_id_uses_unknown(archiver):
    """Forward-compatible-reads: a pre-existing row missing session_id key
    should still archive, partitioned under 'unknown' — not crash."""
    mod, s3 = archiver
    rec = _ttl_remove_record()
    del rec["dynamodb"]["OldImage"]["session_id"]
    res = mod.lambda_handler({"Records": [rec]}, None)
    assert res["archived"] == 1
    call = s3.put_object.call_args.kwargs
    assert call["Key"].endswith("unknown.json")


def test_camelcase_session_id_fallback(archiver):
    """Older records may use sessionId instead of session_id."""
    mod, s3 = archiver
    rec = _ttl_remove_record()
    del rec["dynamodb"]["OldImage"]["session_id"]
    rec["dynamodb"]["OldImage"]["sessionId"] = {"S": "camel_sess_001"}
    res = mod.lambda_handler({"Records": [rec]}, None)
    assert res["archived"] == 1
    call = s3.put_object.call_args.kwargs
    assert call["Key"].endswith("camel_sess_001.json")


def test_decimal_serialization(archiver):
    """DDB numeric attrs deserialize to Decimal — must JSON-encode cleanly."""
    mod, s3 = archiver
    rec = _ttl_remove_record("sess_decimals")
    rec["dynamodb"]["OldImage"]["score"] = {"N": "0.875"}
    rec["dynamodb"]["OldImage"]["count_int"] = {"N": "42"}
    res = mod.lambda_handler({"Records": [rec]}, None)
    assert res["archived"] == 1
    body = json.loads(s3.put_object.call_args.kwargs["Body"].decode("utf-8"))
    assert body["score"] == 0.875
    assert body["count_int"] == 42


def test_string_set_serialization(archiver):
    mod, s3 = archiver
    rec = _ttl_remove_record("sess_sets")
    rec["dynamodb"]["OldImage"]["tags"] = {"SS": ["a", "b", "c"]}
    res = mod.lambda_handler({"Records": [rec]}, None)
    body = json.loads(s3.put_object.call_args.kwargs["Body"].decode("utf-8"))
    assert sorted(body["tags"]) == ["a", "b", "c"]


def test_batch_mixed(archiver):
    mod, s3 = archiver
    insert = _ttl_remove_record("ins")
    insert["eventName"] = "INSERT"
    no_image = _ttl_remove_record("ni")
    no_image["dynamodb"]["OldImage"] = None
    res = mod.lambda_handler(
        {"Records": [_ttl_remove_record("a"), insert, _ttl_remove_record("b"), no_image]},
        None,
    )
    assert res["archived"] == 2
    assert res["skipped_non_remove"] == 1
    assert res["skipped_no_old_image"] == 1
    assert s3.put_object.call_count == 2


def test_empty_batch(archiver):
    mod, s3 = archiver
    res = mod.lambda_handler({"Records": []}, None)
    assert res["batchItemFailures"] == []
    assert res["archived"] == 0
    assert res["skipped_non_remove"] == 0
    assert res["skipped_no_old_image"] == 0
    assert s3.put_object.call_count == 0


def test_partitioning_is_utc_date(archiver):
    """Key format: sessions/year=YYYY/month=MM/day=DD/{sid}.json — UTC-based."""
    mod, s3 = archiver
    res = mod.lambda_handler({"Records": [_ttl_remove_record("sess_date")]}, None)
    assert res["archived"] == 1
    key = s3.put_object.call_args.kwargs["Key"]
    parts = key.split("/")
    assert parts[0] == "sessions"
    assert parts[1].startswith("year=") and len(parts[1]) == len("year=YYYY")
    assert parts[2].startswith("month=") and len(parts[2]) == len("month=MM")
    assert parts[3].startswith("day=") and len(parts[3]) == len("day=DD")
    assert parts[4].endswith(".json")


# ---------------------------------------------------------------------------
# Phase-audit B3/B4 — error path + ReportBatchItemFailures coverage
# ---------------------------------------------------------------------------

def test_s3_put_object_error_reported_via_batch_item_failures(archiver):
    """B3 + B4: when S3 PutObject raises, the record's SequenceNumber appears
    in batchItemFailures so the ESM retries just that record. The exception
    must NOT escape lambda_handler (was the pre-fix behavior — full-batch fail)."""
    from botocore.exceptions import ClientError
    mod, s3 = archiver
    s3.put_object.side_effect = ClientError(
        {"Error": {"Code": "ServiceUnavailable", "Message": "test"}},
        "PutObject",
    )

    rec = _ttl_remove_record("sess_fail_001", sequence_number="seq-001")
    res = mod.lambda_handler({"Records": [rec]}, None)

    assert res["archived"] == 0
    assert res["batchItemFailures"] == [{"itemIdentifier": "seq-001"}]


def test_type_error_in_deserialize_reported_via_batch_item_failures(archiver):
    """B3 + B4: malformed DDB type (TypeDeserializer raises TypeError) should
    not crash the handler — record goes to batchItemFailures, batch continues."""
    mod, s3 = archiver
    # An unknown DDB type code 'X' makes TypeDeserializer raise TypeError.
    rec = _ttl_remove_record("sess_malformed", sequence_number="seq-mal")
    rec["dynamodb"]["OldImage"]["weird_field"] = {"X": "unknown_type"}

    res = mod.lambda_handler({"Records": [rec]}, None)
    assert res["archived"] == 0
    assert res["batchItemFailures"] == [{"itemIdentifier": "seq-mal"}]
    # S3 was never called for this record
    assert s3.put_object.call_count == 0


def test_mixed_batch_partial_failure_reports_only_failed_records(archiver):
    """B4 critical invariant: good records succeed AND bad records are isolated
    in batchItemFailures. Without ReportBatchItemFailures, the whole batch
    would retry on any single failure."""
    from botocore.exceptions import ClientError
    mod, s3 = archiver

    # First put succeeds, second raises, third succeeds.
    side_effects = [
        None,
        ClientError({"Error": {"Code": "ThrottlingException"}}, "PutObject"),
        None,
    ]
    s3.put_object.side_effect = side_effects

    res = mod.lambda_handler({
        "Records": [
            _ttl_remove_record("good1", sequence_number="seq-1"),
            _ttl_remove_record("bad", sequence_number="seq-2"),
            _ttl_remove_record("good2", sequence_number="seq-3"),
        ]
    }, None)
    assert res["archived"] == 2
    assert res["batchItemFailures"] == [{"itemIdentifier": "seq-2"}]


def test_record_without_sequence_number_raises_last_resort(archiver):
    """If a record fails AND lacks a SequenceNumber (should not happen for real
    DDB Streams events) we re-raise rather than silently dropping the record."""
    from botocore.exceptions import ClientError
    mod, s3 = archiver
    s3.put_object.side_effect = ClientError({"Error": {"Code": "InternalError"}}, "PutObject")

    rec = _ttl_remove_record("sess_no_seq")
    rec["dynamodb"]["SequenceNumber"] = None

    with pytest.raises(ClientError):
        mod.lambda_handler({"Records": [rec]}, None)


# ---------------------------------------------------------------------------
# Coverage gaps from audit defer-list (R12, R13, R14)
# ---------------------------------------------------------------------------

def test_forward_compat_missing_tenant_id_started_at_does_not_crash(archiver):
    """R12: old-shape OldImage missing optional fields archives cleanly."""
    mod, s3 = archiver
    rec = _ttl_remove_record("sess_old")
    del rec["dynamodb"]["OldImage"]["tenant_id"]
    del rec["dynamodb"]["OldImage"]["started_at"]
    del rec["dynamodb"]["OldImage"]["ttl"]

    res = mod.lambda_handler({"Records": [rec]}, None)
    assert res["archived"] == 1
    body = json.loads(s3.put_object.call_args.kwargs["Body"].decode("utf-8"))
    assert body["session_id"] == "sess_old"
    assert "tenant_id" not in body
    assert "started_at" not in body


def test_idempotency_same_record_same_s3_key(archiver):
    """R13: re-processing the same record produces the same S3 key (deterministic
    from archive UTC date + session_id). Important for DDB Streams retry safety."""
    mod, s3 = archiver
    rec = _ttl_remove_record("sess_idempotent")
    mod.lambda_handler({"Records": [rec]}, None)
    first_key = s3.put_object.call_args.kwargs["Key"]
    mod.lambda_handler({"Records": [rec]}, None)
    second_key = s3.put_object.call_args.kwargs["Key"]
    assert first_key == second_key
    # Same body too (modulo time-of-day partitioning collision risk noted in README)
    assert s3.put_object.call_count == 2


def test_bytes_attribute_serialization(archiver):
    """R14: DDB Binary attrs deserialize to bytes; _json_default must decode."""
    mod, s3 = archiver
    rec = _ttl_remove_record("sess_bytes")
    rec["dynamodb"]["OldImage"]["payload_blob"] = {"B": b"hello"}
    res = mod.lambda_handler({"Records": [rec]}, None)
    assert res["archived"] == 1
    body = json.loads(s3.put_object.call_args.kwargs["Body"].decode("utf-8"))
    assert body["payload_blob"] == "hello"
