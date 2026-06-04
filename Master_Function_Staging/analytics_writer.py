"""Server-side analytics writer for picasso-session-summaries.

Python port of Bedrock_Streaming_Handler_Staging/analytics_writer.js. Both
writers must produce identical wire format (UpdateExpression, attribute
maps, ConditionExpression) for every fixture in analytics_writer_contract.json.
The contract test enforces this in CI.

Single atomic UpdateItem per call. Per-event-type ConditionExpression on
``last_request_id_<event>`` provides idempotency. Streaming chats are
fire-and-forget at the JS site; this Python writer is called from the
HTTP-fallback path (Master_Function handle_chat) where awaited writes are
acceptable. Uses protected_dynamodb_operation for circuit-breaker + the
shared boto3 client's existing connect_timeout=3 / read_timeout=5 config
(aws_client_manager.py:31-33). v7 plan suggested 2/2 explicit timeouts;
the existing 3/5 is close and shared with other DDB callers — tightening
it would touch unrelated code paths. Trade-off documented per v7 plan
review §"Python writer timeout".
"""
import json
import logging
import os
import re

logger = logging.getLogger(__name__)

SESSION_ID_RE = re.compile(r'^[a-zA-Z0-9_-]{1,128}$')
TENANT_HASH_RE = re.compile(r'^[a-zA-Z0-9]{10,20}$')
FIRST_QUESTION_MAX_CHARS = 50
TTL_DAYS = 365  # 12-month pseudonymized-summary retention (data-retention-strategy.md §2/§9; DDB-only)

REASON_ENUM = frozenset({
    'invalid_session_id_format',
    'invalid_tenant_hash_format',
    'missing_session_id',
    'missing_tenant_hash',
    'missing_event_type',
    'unknown_event_type',
    'ttl_calc_failed',
    'redact_pii_failed',
    'request_id_missing',
})

ERROR_ENUM = frozenset({
    'ddb_throttle',
    'ddb_validation',
    'ddb_resource_not_found',
    'ddb_unknown',
    'iam_access_denied',
    'network_timeout',
    'circuit_breaker_open',
    'internal_error',
})

SUPPORTED_EVENT_TYPES = frozenset({'MESSAGE_SENT', 'MESSAGE_RECEIVED', 'FORM_COMPLETED'})

# Cold-start assertion (Phase 1 C1 fix; phase-audit B9 fix). If
# SESSION_SUMMARIES_TABLE is unset, every write_session_summary call will silently
# no-op (DynamoDB rejects TableName=None as ValidationException; _classify_error
# maps it to 'ddb_validation' and returns False). Emit as JSON to stdout so
# CloudWatch Logs Insights queries on the `evt` key find it — matches the _log()
# pattern used by the rest of this module. Using logger.critical here would split
# the misconfiguration signal off into stderr where the Insights query
# `filter evt = "analytics_write_misconfiguration"` would miss it.
_SESSION_SUMMARIES_TABLE = os.environ.get('SESSION_SUMMARIES_TABLE')
if not _SESSION_SUMMARIES_TABLE:
    print(json.dumps({
        'evt': 'analytics_write_misconfiguration',
        'reason': 'missing_env_var',
        'env_var': 'SESSION_SUMMARIES_TABLE',
        'consequence': 'write_session_summary_will_silently_no_op',
    }))


def _log(state, **fields):
    """Emit one structured log line. Reason/error are enum members; raw user input
    is never interpolated. Matches log_shapes contract in analytics_writer_contract.json.
    """
    payload = {'evt': 'analytics_write_{}'.format(state)}
    payload.update(fields)
    print(json.dumps(payload))


def _classify_error(err):
    """Map a botocore.ClientError or other exception to ERROR_ENUM."""
    name = type(err).__name__
    code = ''
    try:
        code = err.response['Error']['Code']  # type: ignore[attr-defined]
    except (AttributeError, KeyError, TypeError):
        pass

    if code in ('ProvisionedThroughputExceededException', 'ThrottlingException'):
        return 'ddb_throttle'
    if code == 'ValidationException':
        return 'ddb_validation'
    if code == 'ResourceNotFoundException':
        return 'ddb_resource_not_found'
    if code == 'AccessDeniedException':
        return 'iam_access_denied'
    if code == 'ConditionalCheckFailedException':
        return 'ddb_validation'  # benign idempotency rejection
    if 'Timeout' in name or 'timeout' in name.lower():
        return 'network_timeout'
    if 'CircuitBreaker' in name:
        return 'circuit_breaker_open'
    if code:
        return 'ddb_unknown'
    return 'internal_error'


def _ttl_from_timestamp(iso_timestamp):
    """Compute Unix-seconds TTL = parsed(iso) + 90 days. Returns None on parse fail."""
    if not isinstance(iso_timestamp, str):
        return None
    # Accept the same ISO formats Date.parse() does. fromisoformat in Python 3.11+
    # parses 'Z' suffix; for older runtimes we strip it manually.
    try:
        from datetime import datetime
        ts = iso_timestamp.replace('Z', '+00:00') if iso_timestamp.endswith('Z') else iso_timestamp
        dt = datetime.fromisoformat(ts)
    except (ValueError, TypeError):
        return None
    return int(dt.timestamp()) + TTL_DAYS * 24 * 60 * 60


def build_update_params(input_data):
    """Build the UpdateItem kwargs dict matching the JS analytics_writer wire format.

    Returns ``{'params': {...}}`` on success or ``{'error': '<reason_enum>'}`` if
    the input cannot be transformed (e.g. ttl calc failed). Validation errors
    are NOT raised here — caller (write_session_summary) does the regex/required
    checks first.
    """
    event_type = input_data.get('event_type')
    session_id = input_data['session_id']
    tenant_hash = input_data['tenant_hash']
    tenant_id = input_data.get('tenant_id') or ''
    client_timestamp = input_data['client_timestamp']
    request_id = input_data['request_id']
    event_payload = input_data.get('event_payload') or {}

    ttl = _ttl_from_timestamp(client_timestamp)
    if ttl is None:
        return {'error': 'ttl_calc_failed'}

    set_parts = [
        'ended_at = :ended_at',
        'session_id = if_not_exists(session_id, :session_id)',
        'tenant_id = if_not_exists(tenant_id, :tenant_id)',
        'started_at = if_not_exists(started_at, :started_at)',
        '#ttl = :ttl',
    ]
    add_parts = []
    expression_values = {
        ':ended_at': {'S': client_timestamp},
        ':session_id': {'S': session_id},
        ':tenant_id': {'S': tenant_id},
        ':started_at': {'S': client_timestamp},
        ':ttl': {'N': str(ttl)},
        ':request_id': {'S': request_id},
    }
    expression_names = {'#ttl': 'ttl'}
    condition_expression = None

    if event_type == 'MESSAGE_SENT':
        expression_values[':one'] = {'N': '1'}
        add_parts.extend(['message_count :one', 'user_message_count :one'])
        set_parts.append('last_request_id_message_sent = :request_id')
        condition_expression = (
            'attribute_not_exists(last_request_id_message_sent) '
            'OR last_request_id_message_sent <> :request_id'
        )

        raw = event_payload.get('first_question')
        if isinstance(raw, str) and len(raw) > 0:
            try:
                from redact_pii import redact_pii
                redacted = redact_pii(raw)[:FIRST_QUESTION_MAX_CHARS]
            except Exception:
                return {'error': 'redact_pii_failed'}
            set_parts.append('first_question = if_not_exists(first_question, :first_question)')
            expression_values[':first_question'] = {'S': redacted}

    elif event_type == 'MESSAGE_RECEIVED':
        expression_values[':one'] = {'N': '1'}
        add_parts.extend(['message_count :one', 'bot_message_count :one'])
        set_parts.append('last_request_id_message_received = :request_id')
        condition_expression = (
            'attribute_not_exists(last_request_id_message_received) '
            'OR last_request_id_message_received <> :request_id'
        )

        rt = event_payload.get('response_time_ms')
        try:
            rt_num = float(rt) if rt is not None else None
        except (TypeError, ValueError):
            rt_num = None
        if rt_num is not None and 0 < rt_num < 60000:
            add_parts.extend(['total_response_time_ms :response_time', 'response_count :one'])
            expression_values[':response_time'] = {'N': str(int(rt_num))}

    elif event_type == 'FORM_COMPLETED':
        set_parts.extend(['#outcome = :outcome', 'last_request_id_form_completed = :request_id'])
        expression_names['#outcome'] = 'outcome'
        expression_values[':outcome'] = {'S': 'form_completed'}
        condition_expression = (
            'attribute_not_exists(last_request_id_form_completed) '
            'OR last_request_id_form_completed <> :request_id'
        )

        form_id = event_payload.get('form_id')
        if form_id:
            set_parts.append('form_id = :form_id')
            expression_values[':form_id'] = {'S': str(form_id)}
    else:
        return {'error': 'unknown_event_type'}

    update_expression = 'SET ' + ', '.join(set_parts)
    if add_parts:
        update_expression += ' ADD ' + ', '.join(add_parts)

    return {
        'params': {
            'TableName': _SESSION_SUMMARIES_TABLE,
            'Key': {
                'pk': {'S': 'TENANT#{}'.format(tenant_hash)},
                'sk': {'S': 'SESSION#{}'.format(session_id)},
            },
            'UpdateExpression': update_expression,
            'ConditionExpression': condition_expression,
            'ExpressionAttributeNames': expression_names,
            'ExpressionAttributeValues': expression_values,
        }
    }


def write_session_summary(input_data):
    """Validate input + write one analytics row. Returns True on success.

    Never raises — all errors are logged with ``error`` from ERROR_ENUM and the
    function returns False. Callers on the chat path can rely on this contract
    (no exception propagation across the analytics-writer boundary).

    Uses ``protected_dynamodb_operation`` for circuit-breaker semantics matching
    other Master_Function DDB writes.
    """
    input_data = input_data or {}
    event_type = input_data.get('event_type')
    session_id = input_data.get('session_id')
    tenant_hash = input_data.get('tenant_hash')
    request_id = input_data.get('request_id')

    if not event_type:
        _log('invalid', reason='missing_event_type')
        return False
    if event_type not in SUPPORTED_EVENT_TYPES:
        _log('invalid', reason='unknown_event_type', event_type=event_type)
        return False
    if not session_id:
        _log('invalid', reason='missing_session_id', event_type=event_type)
        return False
    if not SESSION_ID_RE.match(session_id):
        _log('invalid', reason='invalid_session_id_format', event_type=event_type)
        return False
    if not tenant_hash:
        _log('invalid', reason='missing_tenant_hash', event_type=event_type)
        return False
    if not TENANT_HASH_RE.match(tenant_hash):
        _log('invalid', reason='invalid_tenant_hash_format', event_type=event_type)
        return False
    if not request_id:
        _log('invalid', reason='request_id_missing', event_type=event_type)
        return False

    built = build_update_params(input_data)
    if 'error' in built:
        _log('invalid', reason=built['error'], event_type=event_type)
        return False

    try:
        # protected_dynamodb_operation wraps the boto3 client with the project's
        # circuit-breaker + retry semantics. Imported lazily so unit tests can
        # mock it without aws_client_manager being on the import path.
        from aws_client_manager import protected_dynamodb_operation
        protected_dynamodb_operation('update_item', **built['params'])
        return True
    except Exception as err:  # noqa: BLE001
        # ConditionalCheckFailedException = benign idempotency rejection.
        # Other errors = real failures. Both fill `error` from the enum;
        # `reason` is reserved for validation-time classifications and is
        # omitted on runtime-failure paths (matches log_shapes contract).
        try:
            err_code = err.response['Error']['Code']  # type: ignore[attr-defined]
        except (AttributeError, KeyError, TypeError):
            err_code = ''
        if err_code == 'ConditionalCheckFailedException':
            _log('duplicate', error='ddb_validation', event_type=event_type)
        else:
            _log('failure', error=_classify_error(err), event_type=event_type)
        return False
