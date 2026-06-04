"""Pytest tests for analytics_writer.py + redact_pii.py.

Mirrors __tests__/analytics_writer.test.js. Both writers must satisfy the
same fixture in analytics_writer_contract.json. The contract test below
validates wire format identity per fixture; the JS suite validates the same
fixture from its side. CI runs both.
"""
import json
import os
import re
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Ensure module resolution works when pytest runs from project root
sys.path.insert(0, str(Path(__file__).parent))

# Set the env var BEFORE importing the writer module (it's read inside
# build_update_params, not at import — but match the JS fixture for clarity).
os.environ['SESSION_SUMMARIES_TABLE'] = 'picasso-session-summaries'

from analytics_writer import (  # noqa: E402
    build_update_params,
    write_session_summary,
    REASON_ENUM,
    ERROR_ENUM,
    SUPPORTED_EVENT_TYPES,
    FIRST_QUESTION_MAX_CHARS,
    TTL_DAYS,
)
from redact_pii import redact_pii  # noqa: E402


CONTRACT_PATH = Path(__file__).parent / 'analytics_writer_contract.json'
contract = json.loads(CONTRACT_PATH.read_text())


def _expected_ttl(iso):
    from datetime import datetime
    ts = iso.replace('Z', '+00:00') if iso.endswith('Z') else iso
    return int(datetime.fromisoformat(ts).timestamp()) + TTL_DAYS * 86400


@pytest.fixture
def base_input():
    return {
        'event_type': 'MESSAGE_SENT',
        'session_id': 'sess_abc123XYZ',
        'tenant_hash': 'my87674d777bf9',
        'tenant_id': 'MYR384719',
        'client_timestamp': '2026-05-04T20:00:00.000Z',
        'request_id': 'req-aaaa-1111',
        'event_payload': {'first_question': 'How do I apply?'},
    }


# ── redact_pii ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    'case',
    [c for c in contract['redact_pii_cases'] if 'expected' in c],
    ids=lambda c: c['input'][:30],
)
def test_redact_pii_matches_contract(case):
    assert redact_pii(case['input']) == case['expected']


def test_redact_pii_50_char_truncation_at_call_site():
    case = next(c for c in contract['redact_pii_cases'] if 'expected_truncated_50' in c)
    assert redact_pii(case['input']) == case['expected_redacted']
    assert redact_pii(case['input'])[:FIRST_QUESTION_MAX_CHARS] == case['expected_truncated_50']


@pytest.mark.parametrize('val', ['', None, 42, [], {}])
def test_redact_pii_non_string_returns_empty(val):
    assert redact_pii(val) == ''


# ── build_update_params: wire-format contract ───────────────────────────────

@pytest.fixture
def fixtures_by_name():
    return {f['name']: f for f in contract['fixtures']}


def test_message_sent_initial_matches_fixture(fixtures_by_name):
    f = fixtures_by_name['MESSAGE_SENT_initial']
    out = build_update_params(f['input'])
    assert 'params' in out
    p = out['params']
    assert p['Key'] == f['expected']['Key']
    assert p['UpdateExpression'] == f['expected']['UpdateExpression']
    assert p['ConditionExpression'] == f['expected']['ConditionExpression']
    assert p['ExpressionAttributeNames'] == f['expected']['ExpressionAttributeNames']
    expected_values = dict(f['expected']['ExpressionAttributeValues'])
    expected_values[':ttl'] = {'N': str(_expected_ttl(f['input']['client_timestamp']))}
    assert p['ExpressionAttributeValues'] == expected_values


def test_message_sent_no_first_question_omits_clause(fixtures_by_name):
    f = fixtures_by_name['MESSAGE_SENT_no_first_question']
    p = build_update_params(f['input'])['params']
    assert p['UpdateExpression'] == f['expected']['UpdateExpression']
    assert ':first_question' not in p['ExpressionAttributeValues']


def test_message_received_with_response_time(fixtures_by_name):
    f = fixtures_by_name['MESSAGE_RECEIVED_with_response_time']
    p = build_update_params(f['input'])['params']
    assert p['UpdateExpression'] == f['expected']['UpdateExpression']
    assert p['ConditionExpression'] == f['expected']['ConditionExpression']
    assert p['ExpressionAttributeValues'][':response_time'] == {'N': '850'}


def test_message_received_response_time_out_of_range_omits(fixtures_by_name):
    f = fixtures_by_name['MESSAGE_RECEIVED_response_time_out_of_range']
    p = build_update_params(f['input'])['params']
    assert p['UpdateExpression'] == f['expected']['UpdateExpression']
    assert ':response_time' not in p['ExpressionAttributeValues']


def test_form_completed_with_form_id(fixtures_by_name):
    f = fixtures_by_name['FORM_COMPLETED_with_form_id']
    p = build_update_params(f['input'])['params']
    assert p['UpdateExpression'] == f['expected']['UpdateExpression']
    assert p['ConditionExpression'] == f['expected']['ConditionExpression']
    assert p['ExpressionAttributeNames'] == f['expected']['ExpressionAttributeNames']
    assert p['ExpressionAttributeValues'][':outcome'] == {'S': 'form_completed'}
    assert p['ExpressionAttributeValues'][':form_id'] == {'S': 'volunteer_signup'}


def test_table_name_from_env_var(base_input):
    p = build_update_params(base_input)['params']
    assert p['TableName'] == 'picasso-session-summaries'


def test_placeholder_invariant_every_placeholder_in_values():
    """Every :placeholder in UpdateExpression OR ConditionExpression must be in ExpressionAttributeValues."""
    placeholder_re = re.compile(r':[A-Za-z_][A-Za-z0-9_]*')
    for fixture in contract['fixtures']:
        if fixture['input']['event_type'] not in SUPPORTED_EVENT_TYPES:
            continue
        out = build_update_params(fixture['input'])
        if 'error' in out:
            continue
        p = out['params']
        placeholders = placeholder_re.findall(p['UpdateExpression'])
        if p.get('ConditionExpression'):
            placeholders += placeholder_re.findall(p['ConditionExpression'])
        for ph in placeholders:
            assert ph in p['ExpressionAttributeValues'], '{} missing in fixture {}'.format(ph, fixture['name'])


def test_attribute_name_invariant_every_hash_in_names():
    name_re = re.compile(r'#[A-Za-z_][A-Za-z0-9_]*')
    for fixture in contract['fixtures']:
        if fixture['input']['event_type'] not in SUPPORTED_EVENT_TYPES:
            continue
        out = build_update_params(fixture['input'])
        if 'error' in out:
            continue
        p = out['params']
        for n in name_re.findall(p['UpdateExpression']):
            assert n in p['ExpressionAttributeNames'], '{} missing in fixture {}'.format(n, fixture['name'])


# ── write_session_summary: end-to-end ───────────────────────────────────────

@pytest.fixture
def mock_ddb_op():
    """Patch protected_dynamodb_operation; yields the mock."""
    with patch('aws_client_manager.protected_dynamodb_operation') as m:
        yield m


def test_happy_path_returns_true_one_call(base_input, mock_ddb_op):
    mock_ddb_op.return_value = {}
    assert write_session_summary(base_input) is True
    assert mock_ddb_op.call_count == 1
    args, kwargs = mock_ddb_op.call_args
    assert args[0] == 'update_item'
    assert kwargs['TableName'] == 'picasso-session-summaries'


def test_atomicity_one_call_per_event_type(base_input, mock_ddb_op):
    mock_ddb_op.return_value = {}
    for et in ['MESSAGE_SENT', 'MESSAGE_RECEIVED', 'FORM_COMPLETED']:
        mock_ddb_op.reset_mock()
        write_session_summary(dict(base_input, event_type=et, request_id='req-' + et))
        assert mock_ddb_op.call_count == 1


@pytest.mark.parametrize('field,bad,reason', [
    ('event_type', None, 'missing_event_type'),
    ('event_type', 'CTA_CLICKED', 'unknown_event_type'),
    ('session_id', '', 'missing_session_id'),
    ('session_id', 'sess abc', 'invalid_session_id_format'),
    ('session_id', 'a' * 129, 'invalid_session_id_format'),
    ('tenant_hash', '', 'missing_tenant_hash'),
    ('tenant_hash', 'abc', 'invalid_tenant_hash_format'),
    ('tenant_hash', 'tenant_with_us', 'invalid_tenant_hash_format'),
    ('request_id', '', 'request_id_missing'),
])
def test_validation_rejection_no_ddb_call(base_input, mock_ddb_op, capsys, field, bad, reason):
    bad_input = dict(base_input, **{field: bad})
    assert write_session_summary(bad_input) is False
    assert mock_ddb_op.call_count == 0
    captured = capsys.readouterr().out
    log_line = next(line for line in captured.splitlines() if 'analytics_write_invalid' in line)
    parsed = json.loads(log_line)
    assert parsed['reason'] == reason
    assert parsed['reason'] in REASON_ENUM


def test_runtime_ddb_error_logs_failure_no_reason(base_input, mock_ddb_op, capsys):
    class FakeBoto3Error(Exception):
        def __init__(self):
            self.response = {'Error': {'Code': 'ThrottlingException'}}
    mock_ddb_op.side_effect = FakeBoto3Error()
    assert write_session_summary(base_input) is False
    captured = capsys.readouterr().out
    log_line = next(line for line in captured.splitlines() if 'analytics_write_failure' in line)
    parsed = json.loads(log_line)
    assert parsed['error'] == 'ddb_throttle'
    assert parsed['error'] in ERROR_ENUM
    assert 'reason' not in parsed


def test_conditional_check_failed_logs_duplicate(base_input, mock_ddb_op, capsys):
    class FakeCondError(Exception):
        def __init__(self):
            self.response = {'Error': {'Code': 'ConditionalCheckFailedException'}}
    mock_ddb_op.side_effect = FakeCondError()
    assert write_session_summary(base_input) is False
    captured = capsys.readouterr().out
    log_line = next(line for line in captured.splitlines() if 'analytics_write_duplicate' in line)
    parsed = json.loads(log_line)
    assert parsed['error'] == 'ddb_validation'
    assert 'reason' not in parsed


def test_first_question_redacted_and_truncated(base_input, mock_ddb_op):
    long_pii = ('Email me at jane.doe@example.com — this is more than fifty characters '
                'total honestly')
    mock_ddb_op.return_value = {}
    write_session_summary(dict(base_input, event_payload={'first_question': long_pii}))
    args, kwargs = mock_ddb_op.call_args
    written = kwargs['ExpressionAttributeValues'][':first_question']['S']
    assert 'jane.doe@example.com' not in written
    assert '[EMAIL]' in written
    assert len(written) <= FIRST_QUESTION_MAX_CHARS


def test_started_at_from_client_timestamp_not_now(base_input, mock_ddb_op):
    past = '2025-01-01T00:00:00.000Z'
    mock_ddb_op.return_value = {}
    write_session_summary(dict(base_input, client_timestamp=past))
    args, kwargs = mock_ddb_op.call_args
    assert kwargs['ExpressionAttributeValues'][':started_at'] == {'S': past}
    assert kwargs['ExpressionAttributeValues'][':ended_at'] == {'S': past}


def test_forward_compat_old_shape_row(base_input, mock_ddb_op):
    """Writer succeeds against an old-shape row (ConditionExpression has attribute_not_exists branch)."""
    mock_ddb_op.return_value = {}
    write_session_summary(base_input)
    args, kwargs = mock_ddb_op.call_args
    assert 'attribute_not_exists' in kwargs['ConditionExpression']


# ── log_shapes contract enforcement (Python ↔ JS parity) ────────────────────

def test_log_shape_invalid_no_error_field(base_input, mock_ddb_op, capsys):
    write_session_summary(dict(base_input, session_id='bad space'))
    captured = capsys.readouterr().out
    log_line = next(line for line in captured.splitlines() if 'analytics_write_invalid' in line)
    parsed = json.loads(log_line)
    shape = contract['log_shapes']['analytics_write_invalid']
    for f in shape['required_fields']:
        assert f in parsed
    for f in shape['forbidden_fields']:
        assert f not in parsed


def test_log_shape_failure_no_reason_field(base_input, mock_ddb_op, capsys):
    class FakeBoto3Error(Exception):
        def __init__(self):
            self.response = {'Error': {'Code': 'ThrottlingException'}}
    mock_ddb_op.side_effect = FakeBoto3Error()
    write_session_summary(base_input)
    captured = capsys.readouterr().out
    log_line = next(line for line in captured.splitlines() if 'analytics_write_failure' in line)
    parsed = json.loads(log_line)
    shape = contract['log_shapes']['analytics_write_failure']
    for f in shape['required_fields']:
        assert f in parsed
    for f in shape['forbidden_fields']:
        assert f not in parsed


def test_log_shape_duplicate_error_equals_ddb_validation(base_input, mock_ddb_op, capsys):
    class FakeCondError(Exception):
        def __init__(self):
            self.response = {'Error': {'Code': 'ConditionalCheckFailedException'}}
    mock_ddb_op.side_effect = FakeCondError()
    write_session_summary(base_input)
    captured = capsys.readouterr().out
    log_line = next(line for line in captured.splitlines() if 'analytics_write_duplicate' in line)
    parsed = json.loads(log_line)
    shape = contract['log_shapes']['analytics_write_duplicate']
    for f in shape['forbidden_fields']:
        assert f not in parsed
    assert parsed['error'] == shape['error_must_equal']


# ---------------------------------------------------------------------------
# Cold-start assertion tests (phase-audit B10)
#
# These tests reload the module with and without SESSION_SUMMARIES_TABLE to
# exercise the cold-start emission path. Without these tests, the C1 fix's
# failure-mode branch had zero coverage and a rename would silently regress
# the only signal that surfaces a missing env var.
# ---------------------------------------------------------------------------

def _reload_analytics_writer():
    import importlib
    import analytics_writer
    importlib.reload(analytics_writer)
    return analytics_writer


def test_cold_start_missing_env_var_emits_misconfiguration(monkeypatch, capsys):
    """When SESSION_SUMMARIES_TABLE is unset at module import, emit a
    structured JSON line on stdout so CloudWatch Insights queries on
    `evt = analytics_write_misconfiguration` find it."""
    monkeypatch.delenv('SESSION_SUMMARIES_TABLE', raising=False)
    _reload_analytics_writer()
    captured = capsys.readouterr().out
    line = next(
        (ln for ln in captured.splitlines() if 'analytics_write_misconfiguration' in ln),
        None,
    )
    assert line is not None, (
        f"Expected analytics_write_misconfiguration emission on stdout, got: {captured!r}"
    )
    parsed = json.loads(line)
    assert parsed['evt'] == 'analytics_write_misconfiguration'
    assert parsed['reason'] == 'missing_env_var'
    assert parsed['env_var'] == 'SESSION_SUMMARIES_TABLE'


def test_cold_start_env_var_present_no_misconfiguration(monkeypatch, capsys):
    """When SESSION_SUMMARIES_TABLE IS set, the cold-start path is silent."""
    monkeypatch.setenv('SESSION_SUMMARIES_TABLE', 'picasso-session-summaries')
    _reload_analytics_writer()
    captured = capsys.readouterr().out
    assert 'analytics_write_misconfiguration' not in captured
