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


def _ttl_remove_record(session_id="sess_test_123", extra=None):
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
        "dynamodb": {"OldImage": image, "StreamViewType": "OLD_IMAGE"},
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
    assert res == {"archived": 0, "skipped_non_remove": 0, "skipped_no_old_image": 0}
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
