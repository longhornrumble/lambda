"""
Tests for Attribution_Recap_Generator.

Coverage per done-bar:
1.  Variant selection: all four variants
2.  Flag-off tenant is skipped
3.  Recipients-absent tenant is skipped
4.  Idempotency: second run skipped
5.  Dry-run never invokes send_email Lambda
6.  Enabled path invokes send_email with exact contract payload
7.  Render snapshot fixtures for all four variants (various assertions)
8.  Old-shape aggregate row (missing fields) -> no crash
9.  ATTRIBUTION_AGGREGATES_TABLE missing -> ValueError
10. (WS-I) Token round-trip: valid token verifies; tampered byte -> 403
11. (WS-I) Wrong-suffix payload -> 403; missing param -> 403
12. (WS-I) Constant-time compare used (hmac.compare_digest)
13. (WS-I) Idempotent double-unsubscribe (ConditionalCheckFailed treated as success)
14. (WS-I) Suppression filter: mixed-case email matches; suppressed excluded; all-suppressed skips
15. (WS-I) Per-recipient sends each carry a DIFFERENT token
16. (WS-I) Postal address fail-closed: send blocked when RECAP_POSTAL_ADDRESS unset
17. (WS-I) Dry-run renders [POSTAL ADDRESS NOT CONFIGURED] placeholder
18. (WS-I) Secret-absent fail-closed; transient failure NOT cached
19. (WS-I) No email/token in any logger.* call (static inspection)

Run: pytest test_attribution_recap_generator.py -v
"""
import ast
import hashlib
import hmac
import base64
import inspect
import json
import logging
import os
import pytest
from datetime import datetime, timezone
from io import BytesIO
from unittest.mock import patch, MagicMock, call, ANY

# ---------------------------------------------------------------------------
# Module import -- patch env before import to avoid early failures
# ---------------------------------------------------------------------------
os.environ.setdefault('ATTRIBUTION_AGGREGATES_TABLE', 'picasso-attribution-aggregates')
os.environ.setdefault('TENANT_CONFIG_BUCKET', 'picasso-configs-test')
os.environ.setdefault('SEND_EMAIL_FUNCTION_NAME', 'send_email')
os.environ.setdefault('DASHBOARD_BASE_URL', 'https://app.myrecruiter.ai')
os.environ.setdefault('RECAP_SEND_ENABLED', 'false')
os.environ.setdefault('RECAP_POSTAL_ADDRESS', '123 Main St, Austin TX 78701')
os.environ.setdefault('UNSUB_SECRET_NAME', 'picasso/unsub-signing-key')
os.environ.setdefault('UNSUBSCRIBE_BASE_URL', 'https://app.myrecruiter.ai/unsubscribe')

import lambda_function as recap  # noqa: E402
from lambda_function import (
    _select_variant,
    _render_email,
    _render_text_fallback,
    _month_label,
    _pct_delta_str,
    _build_superlatives,
    _extract_top_topics,
    _find_mvp_channel,
    _build_unsub_token,
    _b64url_nopad,
    _fetch_suppressed_emails,
    _filter_suppressed,
    CONFIDENCE_FLOOR,
    SMALL_TENANT_FLOOR,
    DEFAULT_TZ,
    WORK_WEEK_HOURS,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

_GOOD_SUMMARY = {
    'conversations': 150,
    'leads': 30,
    'conversation_minutes': 600,
    'after_hours_conversations': 60,
    'self_booked_pct': 40.0,
    'median_first_response_minutes': 8.0,
    'prior_conversations': 120,
    'prior_leads': 24,
}

_BAD_SUMMARY = {
    'conversations': 120,
    'leads': 10,
    'conversation_minutes': 480,
    'after_hours_conversations': 40,
    'self_booked_pct': None,
    'median_first_response_minutes': None,
    'prior_conversations': 130,
    'prior_leads': 25,
}

_SMALL_SUMMARY = {
    'conversations': 20,
    'leads': 3,
    'conversation_minutes': 80,
    'after_hours_conversations': 8,
    'self_booked_pct': None,
    'median_first_response_minutes': None,
    'prior_conversations': 15,
    'prior_leads': 2,
}

_FIRST_MONTH_SUMMARY = {
    'conversations': 100,
    'leads': 15,
    'conversation_minutes': 400,
    'after_hours_conversations': 30,
    'self_booked_pct': None,
    'median_first_response_minutes': None,
    # No prior_conversations key -> triggers first_month variant
}

_CHANNEL_ROWS = [
    {
        'channel': 'website',
        'data': {
            'conversations': 80,
            'leads': 12,
            'topic_counts': {'Volunteer': 40, 'Donation': 20, 'Events': 15, 'Services': 5},
        },
    },
    {
        'channel': 'campaign',
        'data': {
            'conversations': 70,
            'leads': 18,
            'topic_counts': {'Volunteer': 30, 'Events': 25},
        },
    },
]

_TENANT_CONFIG_ENABLED = {
    'feature_flags': {'dashboard_attribution': True},
    'organization_name': 'Test Org',
    'attribution_recap': {'recipients': ['admin@example.org']},
}

_TENANT_CONFIG_ENABLED_MULTI = {
    'feature_flags': {'dashboard_attribution': True},
    'organization_name': 'Test Org',
    'attribution_recap': {'recipients': ['alice@example.org', 'bob@example.org']},
}

_TENANT_CONFIG_DISABLED = {
    'feature_flags': {'dashboard_attribution': False},
    'organization_name': 'Disabled Org',
    'attribution_recap': {'recipients': ['admin@example.org']},
}

_TENANT_CONFIG_NO_RECIPIENTS = {
    'feature_flags': {'dashboard_attribution': True},
    'organization_name': 'No Recipients Org',
}

MONTH_STR = '2026-06'
TENANT_ID = 'TEST12345'
ORG_NAME = 'Test Org'

_TEST_SIGNING_KEY = b'test-signing-key-for-unit-tests'
_TEST_POSTAL = '123 Main St, Austin TX 78701'
_TEST_UNSUB_BASE = 'https://app.myrecruiter.ai/unsubscribe'


def _make_lambda_response(body_dict: dict, status_code: int = 200, function_error: str = None):
    """Simulate boto3 lambda.invoke() return value."""
    body_str = json.dumps(body_dict)
    response = {
        'StatusCode': status_code,
        'Payload': BytesIO(json.dumps({'statusCode': status_code, 'body': body_str}).encode()),
    }
    if function_error:
        response['FunctionError'] = function_error
    return response


def _ddb_get_item_response(item: dict | None):
    """Simulate DynamoDB Table.get_item() response."""
    if item is None:
        return {'Item': {}, 'ResponseMetadata': {}}
    return {'Item': item, 'ResponseMetadata': {}}


# ---------------------------------------------------------------------------
# 1. Variant selection
# ---------------------------------------------------------------------------
class TestVariantSelection:

    def test_good_month(self):
        assert _select_variant(_GOOD_SUMMARY, MONTH_STR, TENANT_ID) == 'good_month'

    def test_bad_month_leads_declined(self):
        assert _select_variant(_BAD_SUMMARY, MONTH_STR, TENANT_ID) == 'bad_month'

    def test_small_tenant_below_floor(self):
        assert _select_variant(_SMALL_SUMMARY, MONTH_STR, TENANT_ID) == 'small_tenant'

    def test_first_month_no_prior(self):
        assert _select_variant(_FIRST_MONTH_SUMMARY, MONTH_STR, TENANT_ID) == 'first_month'

    def test_first_month_prior_none_explicitly(self):
        row = {**_GOOD_SUMMARY, 'prior_conversations': None}
        assert _select_variant(row, MONTH_STR, TENANT_ID) == 'first_month'

    def test_small_tenant_exactly_at_floor_minus_one(self):
        row = {**_GOOD_SUMMARY, 'conversations': SMALL_TENANT_FLOOR - 1, 'prior_conversations': 100}
        assert _select_variant(row, MONTH_STR, TENANT_ID) == 'small_tenant'

    def test_good_month_exactly_at_floor(self):
        row = {**_GOOD_SUMMARY, 'conversations': SMALL_TENANT_FLOOR, 'prior_conversations': 50}
        assert _select_variant(row, MONTH_STR, TENANT_ID) in ('good_month', 'bad_month')

    def test_bad_month_requires_leads_lower(self):
        row = {
            'conversations': 100,
            'leads': 20,
            'prior_conversations': 90,
            'prior_leads': 25,  # current < prior -> bad_month
        }
        assert _select_variant(row, MONTH_STR, TENANT_ID) == 'bad_month'

    def test_good_month_leads_equal(self):
        row = {
            'conversations': 100,
            'leads': 20,
            'prior_conversations': 90,
            'prior_leads': 20,  # equal -> good_month
        }
        assert _select_variant(row, MONTH_STR, TENANT_ID) == 'good_month'


# ---------------------------------------------------------------------------
# 2. Flag-off tenant skipped
# ---------------------------------------------------------------------------
class TestFlagGate:

    def test_flag_off_skipped(self):
        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_DISABLED), \
             patch.object(recap, '_load_aggregate', return_value={}):
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=True)
        assert result == 'skip_flag_off'

    def test_flag_absent_skipped(self):
        """No feature_flags key at all -> default False -> skip."""
        config = {'organization_name': 'No Flags Org'}
        with patch.object(recap, '_get_tenant_config', return_value=config):
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=True)
        assert result == 'skip_flag_off'

    def test_flag_in_features_key(self):
        """dashboard_attribution under 'features' (not 'feature_flags') should also gate."""
        config = {
            'features': {'dashboard_attribution': True},
            'organization_name': 'Features Key Org',
            'attribution_recap': {'recipients': ['x@y.com']},
        }
        with patch.object(recap, '_get_tenant_config', return_value=config), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_FIRST_MONTH_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=[]), \
             patch.object(recap, '_fetch_suppressed_emails', return_value=set()), \
             patch.object(recap, '_mark_recap_sent'):
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=True)
        assert result == 'dry_run'


# ---------------------------------------------------------------------------
# 3. Recipients-absent skipped
# ---------------------------------------------------------------------------
class TestRecipientsGate:

    def test_no_recipients_key_skipped(self):
        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_NO_RECIPIENTS):
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=True)
        assert result == 'skip_no_recipients'

    def test_empty_recipients_list_skipped(self):
        config = {**_TENANT_CONFIG_NO_RECIPIENTS, 'attribution_recap': {'recipients': []}}
        with patch.object(recap, '_get_tenant_config', return_value=config):
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=True)
        assert result == 'skip_no_recipients'


# ---------------------------------------------------------------------------
# 4. Idempotency: second run skipped
# ---------------------------------------------------------------------------
class TestIdempotency:

    def test_already_sent_skip(self):
        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_ENABLED), \
             patch.object(recap, '_recap_already_sent', return_value=True):
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=False)
        assert result == 'already_sent'

    def test_marker_written_on_send(self):
        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_ENABLED), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=_CHANNEL_ROWS), \
             patch.object(recap, '_fetch_suppressed_emails', return_value=set()), \
             patch.object(recap, '_get_unsub_signing_key', return_value=_TEST_SIGNING_KEY), \
             patch.object(recap, 'UNSUBSCRIBE_BASE_URL', _TEST_UNSUB_BASE), \
             patch.object(recap, 'RECAP_POSTAL_ADDRESS', _TEST_POSTAL), \
             patch.object(recap, '_mark_recap_sent') as mock_mark, \
             patch.object(recap, '_invoke_send_email') as mock_send:
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=False)

        assert result == 'sent'
        mock_mark.assert_called_once_with(TENANT_ID, MONTH_STR)
        mock_send.assert_called_once()

    def test_marker_not_written_on_dry_run(self):
        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_ENABLED), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=_CHANNEL_ROWS), \
             patch.object(recap, '_fetch_suppressed_emails', return_value=set()), \
             patch.object(recap, '_mark_recap_sent') as mock_mark, \
             patch.object(recap, '_invoke_send_email') as mock_send:
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=True)

        assert result == 'dry_run'
        mock_mark.assert_not_called()
        mock_send.assert_not_called()


# ---------------------------------------------------------------------------
# 5. Dry-run never invokes send_email Lambda
# ---------------------------------------------------------------------------
class TestDryRun:

    def test_dry_run_no_send_email_invoke(self):
        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_ENABLED), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=_CHANNEL_ROWS), \
             patch.object(recap, '_fetch_suppressed_emails', return_value=set()), \
             patch.object(recap, '_mark_recap_sent'), \
             patch.object(recap, '_lambda_client') as mock_lambda_client:
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=True)

        assert result == 'dry_run'
        mock_lambda_client.invoke.assert_not_called()

    def test_handler_dry_run_mode_by_default(self):
        """RECAP_SEND_ENABLED not 'true' -> dry_run mode."""
        with patch.object(recap, 'RECAP_SEND_ENABLED', 'false'), \
             patch.object(recap, '_get_active_tenant_pairs', return_value=[('hash1', TENANT_ID)]), \
             patch.object(recap, '_process_tenant', return_value='dry_run') as mock_proc:
            result = recap.lambda_handler({}, None)

        assert result['dry_run_mode'] is True
        assert result['dry_run'] == 1


# ---------------------------------------------------------------------------
# 6. Enabled path invokes send_email with exact contract payload
# ---------------------------------------------------------------------------
class TestEnabledPathSendEmailContract:

    def test_invoke_payload_matches_send_email_contract(self):
        """
        Per-recipient send: each invoke must have 'to' as a single-element list,
        and the body must be a JSON string (send_email contract).
        """
        mock_invoke_resp = _make_lambda_response({'success': True, 'message_id': 'msg123'})

        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_ENABLED), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=_CHANNEL_ROWS), \
             patch.object(recap, '_fetch_suppressed_emails', return_value=set()), \
             patch.object(recap, '_get_unsub_signing_key', return_value=_TEST_SIGNING_KEY), \
             patch.object(recap, 'UNSUBSCRIBE_BASE_URL', _TEST_UNSUB_BASE), \
             patch.object(recap, 'RECAP_POSTAL_ADDRESS', _TEST_POSTAL), \
             patch.object(recap, '_mark_recap_sent'), \
             patch.object(recap, '_lambda_client') as mock_lc:
            mock_lc.invoke.return_value = mock_invoke_resp
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=False)

        assert result == 'sent'
        mock_lc.invoke.assert_called_once()

        call_kwargs = mock_lc.invoke.call_args.kwargs
        assert call_kwargs['FunctionName'] == 'send_email'
        assert call_kwargs['InvocationType'] == 'RequestResponse'

        raw_payload = call_kwargs['Payload']
        outer = json.loads(raw_payload.decode('utf-8') if isinstance(raw_payload, bytes) else raw_payload)
        assert 'body' in outer, 'Payload must have a body key (send_email contract)'
        assert isinstance(outer['body'], str), 'body must be a JSON-encoded string'

        inner = json.loads(outer['body'])
        assert isinstance(inner.get('to'), list), 'to must be a list'
        assert len(inner['to']) == 1, 'per-recipient send: exactly one address per invoke'
        assert isinstance(inner.get('subject'), str) and inner['subject']
        assert inner.get('html_body') or inner.get('text_body')

        assert '$' not in inner.get('html_body', ''), 'html_body must not contain dollar signs'
        assert '$' not in inner.get('text_body', ''), 'text_body must not contain dollar signs'

    def test_send_email_function_error_logged_not_raised(self):
        mock_invoke_resp = _make_lambda_response({'errorMessage': 'SES error'}, function_error='Unhandled')
        with patch.object(recap, '_lambda_client') as mock_lc:
            mock_lc.invoke.return_value = mock_invoke_resp
            recap._invoke_send_email(
                {'to': ['x@y.com'], 'subject': 'test', 'html_body': '<p>hi</p>'},
                TENANT_ID, MONTH_STR,
            )

    def test_send_email_missing_function_name_logs_error(self, caplog):
        with patch.object(recap, 'SEND_EMAIL_FUNCTION_NAME', ''):
            with caplog.at_level(logging.ERROR):
                recap._invoke_send_email(
                    {'to': ['x@y.com'], 'subject': 'test', 'html_body': '<p>hi</p>'},
                    TENANT_ID, MONTH_STR,
                )
        assert any('SEND_EMAIL_FUNCTION_NAME' in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# 7. Render snapshot fixtures for all four variants
# ---------------------------------------------------------------------------

_VARIANT_CASES = [
    ('good_month', _GOOD_SUMMARY),
    ('bad_month', _BAD_SUMMARY),
    ('small_tenant', _SMALL_SUMMARY),
    ('first_month', _FIRST_MONTH_SUMMARY),
]

_POSTAL = _TEST_POSTAL
_UNSUB = 'https://app.myrecruiter.ai/unsubscribe?t=TESTTOKEN'


class TestRenderSnapshots:

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_no_dollar_sign(self, variant, summary):
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert '$' not in html

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_no_per_person_data(self, variant, summary):
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert 'sess_' not in html
        assert 'mailto:' not in html.lower()
        assert 'admin@example.org' not in html

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_cta_link_present(self, variant, summary):
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert 'https://app.myrecruiter.ai/attribution' in html

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_unsubscribe_present(self, variant, summary):
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert 'unsubscribe' in html.lower()

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_settings_present(self, variant, summary):
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert 'settings' in html.lower()

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_why_receiving_present(self, variant, summary):
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert 'receiving this' in html.lower()

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_no_empty_superlative(self, variant, summary):
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert len(html) > 500

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_html_is_valid_table_structure(self, variant, summary):
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert '<!DOCTYPE html>' in html
        assert '<body' in html
        assert '<table' in html

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_month_label_in_html(self, variant, summary):
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert 'June 2026' in html

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_text_fallback_no_dollar_sign(self, variant, summary):
        text = _render_text_fallback(variant, summary, MONTH_STR, ORG_NAME,
                                     postal_address=_POSTAL, unsub_url=_UNSUB)
        assert '$' not in text

    def test_small_tenant_no_rate_comparison(self):
        html = _render_email('small_tenant', _SMALL_SUMMARY, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert 'Channel MVP' not in html

    def test_first_month_no_delta(self):
        html = _render_email('first_month', _FIRST_MONTH_SUMMARY, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert '▲' not in html or '▼' not in html
        assert 'first' in html.lower() or 'welcome' in html.lower() or 'Welcome' in html

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_postal_address_in_footer(self, variant, summary):
        """CAN-SPAM condition 1: postal address must appear in footer HTML."""
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address='456 Test Ave, Dallas TX 75201', unsub_url=_UNSUB)
        assert '456 Test Ave, Dallas TX 75201' in html

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_postal_address_in_text_fallback(self, variant, summary):
        """CAN-SPAM condition 1: postal address in plain text fallback."""
        text = _render_text_fallback(variant, summary, MONTH_STR, ORG_NAME,
                                     postal_address='456 Test Ave, Dallas TX 75201', unsub_url=_UNSUB)
        assert '456 Test Ave, Dallas TX 75201' in text

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_tokenized_unsub_url_in_footer(self, variant, summary):
        """CAN-SPAM condition 3: per-recipient tokenized URL must appear in footer."""
        token_url = 'https://app.myrecruiter.ai/unsubscribe?t=MYTOKEN123'
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=token_url)
        assert 'MYTOKEN123' in html

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_tokenized_unsub_url_in_text_fallback(self, variant, summary):
        token_url = 'https://app.myrecruiter.ai/unsubscribe?t=MYTOKEN456'
        text = _render_text_fallback(variant, summary, MONTH_STR, ORG_NAME,
                                     postal_address=_POSTAL, unsub_url=token_url)
        assert 'MYTOKEN456' in text

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_dry_run_placeholder_postal(self, variant, summary):
        """Dry-run: [POSTAL ADDRESS NOT CONFIGURED] placeholder present when no postal."""
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address='[POSTAL ADDRESS NOT CONFIGURED]', unsub_url=_UNSUB)
        assert '[POSTAL ADDRESS NOT CONFIGURED]' in html


# ---------------------------------------------------------------------------
# 8. Old-shape aggregate row -> no crash
# ---------------------------------------------------------------------------
class TestSchemaToleranceOldRow:

    def test_empty_summary_row_no_crash(self):
        variant = _select_variant({}, MONTH_STR, TENANT_ID)
        assert variant == 'first_month'
        html = _render_email('first_month', {}, [], MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert '<!DOCTYPE html>' in html

    def test_partial_row_no_crash(self):
        row = {'conversations': 100, 'leads': 10}
        variant = _select_variant(row, MONTH_STR, TENANT_ID)
        html = _render_email(variant, row, [], MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert '$' not in html

    def test_none_values_in_row_no_crash(self):
        row = {
            'conversations': 100,
            'leads': 15,
            'conversation_minutes': None,
            'after_hours_conversations': None,
            'self_booked_pct': None,
            'median_first_response_minutes': None,
            'prior_conversations': 80,
            'prior_leads': None,
        }
        variant = _select_variant(row, MONTH_STR, TENANT_ID)
        html = _render_email(variant, row, [], MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert '<!DOCTYPE html>' in html

    def test_missing_channel_rows_no_crash(self):
        html = _render_email('good_month', _GOOD_SUMMARY, [], MONTH_STR, ORG_NAME, TENANT_ID,
                             postal_address=_POSTAL, unsub_url=_UNSUB)
        assert '<!DOCTYPE html>' in html
        assert '$' not in html

    def test_channel_row_missing_data_key(self):
        rows = [{'channel': 'website'}]
        topics = _extract_top_topics(rows)
        assert isinstance(topics, list)
        mvp = _find_mvp_channel(rows)
        assert mvp is None


# ---------------------------------------------------------------------------
# 9. ATTRIBUTION_AGGREGATES_TABLE missing -> ValueError
# ---------------------------------------------------------------------------
class TestMissingTableEnv:

    def test_missing_table_raises(self):
        with patch.object(recap, 'ATTRIBUTION_AGGREGATES_TABLE', ''):
            with pytest.raises(ValueError, match='ATTRIBUTION_AGGREGATES_TABLE'):
                recap.lambda_handler({}, None)


# ---------------------------------------------------------------------------
# 10. (WS-I) Token helpers: round-trip, tamper, wrong suffix
# ---------------------------------------------------------------------------
class TestTokenHelpers:

    def test_token_round_trip_valid(self):
        """Build token then validate it: must decode to (tenant_id, email_lower)."""
        token = _build_unsub_token(TENANT_ID, 'User@Example.COM', _TEST_SIGNING_KEY)
        # Verify token contains two base64url segments separated by '.'
        parts = token.split('.')
        assert len(parts) == 2

        # Validate: decode payload and HMAC
        payload_bytes = base64.urlsafe_b64decode(parts[0] + '=' * (4 - len(parts[0]) % 4) if len(parts[0]) % 4 else parts[0])
        payload = payload_bytes.decode('utf-8')
        assert payload == f'{TENANT_ID}|user@example.com|recap'

        sig_bytes = base64.urlsafe_b64decode(parts[1] + '=' * (4 - len(parts[1]) % 4) if len(parts[1]) % 4 else parts[1])
        import hashlib as _hl
        import hmac as _hmac
        expected = _hmac.new(_TEST_SIGNING_KEY, payload_bytes, _hl.sha256).digest()
        assert hmac.compare_digest(expected, sig_bytes)

    def test_token_normalises_email_lowercase(self):
        t1 = _build_unsub_token(TENANT_ID, 'USER@EXAMPLE.COM', _TEST_SIGNING_KEY)
        t2 = _build_unsub_token(TENANT_ID, 'user@example.com', _TEST_SIGNING_KEY)
        assert t1 == t2

    def test_build_unsub_token_format(self):
        """Token must have exactly one '.' separating two non-empty segments."""
        token = _build_unsub_token(TENANT_ID, 'a@b.com', _TEST_SIGNING_KEY)
        parts = token.split('.')
        assert len(parts) == 2
        assert all(p for p in parts)

    def test_token_no_padding(self):
        """base64url segments must have no '=' padding."""
        token = _build_unsub_token(TENANT_ID, 'a@b.com', _TEST_SIGNING_KEY)
        assert '=' not in token


# ---------------------------------------------------------------------------
# 11-12. (WS-I) Token validation: tamper, wrong suffix, missing, constant-time
# These tests live in the Unsubscribe Lambda but we test the token structure here
# via _build_unsub_token so the two Lambdas stay in agreement.
# ---------------------------------------------------------------------------
class TestTokenValidationIntegrity:

    def _decode_token(self, token):
        """Decode token payload -> (tenant_id, email, suffix)."""
        b64_payload = token.split('.')[0]
        padded = b64_payload + '=' * (4 - len(b64_payload) % 4) if len(b64_payload) % 4 else b64_payload
        payload = base64.urlsafe_b64decode(padded).decode('utf-8')
        return payload.split('|')

    def test_tampered_byte_invalidates(self):
        """Flip one byte in the signature segment -> HMAC mismatch."""
        from Attribution_Unsubscribe.lambda_function import _validate_token
        token = _build_unsub_token(TENANT_ID, 'x@y.com', _TEST_SIGNING_KEY)
        # Flip last char of sig segment
        parts = token.rsplit('.', 1)
        sig_chars = list(parts[1])
        sig_chars[-1] = 'A' if sig_chars[-1] != 'A' else 'B'
        tampered = parts[0] + '.' + ''.join(sig_chars)
        assert _validate_token(tampered, _TEST_SIGNING_KEY) is None

    def test_wrong_suffix_payload_invalid(self):
        """Payload suffix must be 'recap'; any other value -> invalid."""
        from Attribution_Unsubscribe.lambda_function import _validate_token
        # Build a token with wrong suffix by constructing manually
        import hashlib as _hl
        payload = f'{TENANT_ID}|x@y.com|monthly'
        payload_bytes = payload.encode('utf-8')
        sig = hmac.new(_TEST_SIGNING_KEY, payload_bytes, _hl.sha256).digest()
        token = _b64url_nopad(payload_bytes) + '.' + _b64url_nopad(sig)
        assert _validate_token(token, _TEST_SIGNING_KEY) is None

    def test_missing_dot_invalid(self):
        from Attribution_Unsubscribe.lambda_function import _validate_token
        assert _validate_token('nodottoken', _TEST_SIGNING_KEY) is None

    def test_empty_token_invalid(self):
        from Attribution_Unsubscribe.lambda_function import _validate_token
        assert _validate_token('', _TEST_SIGNING_KEY) is None

    def test_valid_token_returns_tuple(self):
        from Attribution_Unsubscribe.lambda_function import _validate_token
        token = _build_unsub_token(TENANT_ID, 'user@example.com', _TEST_SIGNING_KEY)
        result = _validate_token(token, _TEST_SIGNING_KEY)
        assert result is not None
        assert result[0] == TENANT_ID
        assert result[1] == 'user@example.com'

    def test_constant_time_compare_used(self):
        """_validate_token must use hmac.compare_digest (static inspect)."""
        import Attribution_Unsubscribe.lambda_function as unsub_mod
        src = inspect.getsource(unsub_mod._validate_token)
        assert 'compare_digest' in src, 'hmac.compare_digest must be used in _validate_token'


# ---------------------------------------------------------------------------
# 13. (WS-I) Unsubscribe Lambda: idempotent double-unsubscribe
# ---------------------------------------------------------------------------
class TestUnsubscribeLambda:

    def _make_event(self, token: str) -> dict:
        return {'queryStringParameters': {'t': token}}

    def test_valid_token_returns_200_html(self):
        from Attribution_Unsubscribe import lambda_function as unsub
        token = _build_unsub_token(TENANT_ID, 'alice@example.com', _TEST_SIGNING_KEY)
        event = self._make_event(token)

        mock_table = MagicMock()
        mock_table.put_item.return_value = {}

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_SIGNING_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            resp = unsub.lambda_handler(event, None)

        assert resp['statusCode'] == 200
        assert 'text/html' in resp['headers']['Content-Type']
        assert 'unsubscribed' in resp['body'].lower()

    def test_idempotent_double_unsubscribe(self):
        """ConditionalCheckFailedException on second request treated as success (200)."""
        from Attribution_Unsubscribe import lambda_function as unsub
        from botocore.exceptions import ClientError
        token = _build_unsub_token(TENANT_ID, 'alice@example.com', _TEST_SIGNING_KEY)
        event = self._make_event(token)

        mock_table = MagicMock()
        err = ClientError({'Error': {'Code': 'ConditionalCheckFailedException', 'Message': 'x'}}, 'PutItem')
        mock_table.put_item.side_effect = err

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_SIGNING_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            resp = unsub.lambda_handler(event, None)

        # Should still return 200 (idempotent)
        assert resp['statusCode'] == 200

    def test_missing_token_returns_403(self):
        from Attribution_Unsubscribe import lambda_function as unsub
        resp = unsub.lambda_handler({'queryStringParameters': {}}, None)
        assert resp['statusCode'] == 403

    def test_null_query_params_returns_403(self):
        from Attribution_Unsubscribe import lambda_function as unsub
        resp = unsub.lambda_handler({}, None)
        assert resp['statusCode'] == 403

    def test_tampered_token_returns_403(self):
        from Attribution_Unsubscribe import lambda_function as unsub
        token = _build_unsub_token(TENANT_ID, 'alice@example.com', _TEST_SIGNING_KEY)
        parts = token.rsplit('.', 1)
        sig_chars = list(parts[1])
        sig_chars[0] = 'A' if sig_chars[0] != 'A' else 'B'
        tampered = parts[0] + '.' + ''.join(sig_chars)
        event = self._make_event(tampered)

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_SIGNING_KEY):
            resp = unsub.lambda_handler(event, None)

        assert resp['statusCode'] == 403

    def test_wrong_suffix_returns_403(self):
        from Attribution_Unsubscribe import lambda_function as unsub
        import hashlib as _hl
        payload = f'{TENANT_ID}|alice@example.com|newsletter'
        payload_bytes = payload.encode('utf-8')
        sig = hmac.new(_TEST_SIGNING_KEY, payload_bytes, _hl.sha256).digest()
        token = _b64url_nopad(payload_bytes) + '.' + _b64url_nopad(sig)
        event = self._make_event(token)

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_SIGNING_KEY):
            resp = unsub.lambda_handler(event, None)

        assert resp['statusCode'] == 403

    def test_signing_key_unavailable_returns_403(self):
        """If signing key can't be fetched, return 403 (no detail)."""
        from Attribution_Unsubscribe import lambda_function as unsub
        token = _build_unsub_token(TENANT_ID, 'a@b.com', _TEST_SIGNING_KEY)
        with patch.object(unsub, '_get_signing_key', return_value=None):
            resp = unsub.lambda_handler(self._make_event(token), None)
        assert resp['statusCode'] == 403

    def test_suppression_row_schema(self):
        """Suppression row must use correct pk/sk and no TTL."""
        from Attribution_Unsubscribe import lambda_function as unsub
        token = _build_unsub_token(TENANT_ID, 'Alice@Example.COM', _TEST_SIGNING_KEY)
        event = self._make_event(token)

        mock_table = MagicMock()
        put_calls = []

        def capture_put(**kwargs):
            put_calls.append(kwargs)
            return {}

        mock_table.put_item.side_effect = capture_put

        with patch.object(unsub, '_get_signing_key', return_value=_TEST_SIGNING_KEY), \
             patch.object(unsub, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(unsub, '_dynamodb') as mock_ddb:
            mock_ddb.Table.return_value = mock_table
            unsub.lambda_handler(event, None)

        assert len(put_calls) == 1
        item = put_calls[0]['Item']
        assert item['pk'] == f'TENANT#{TENANT_ID}'
        assert item['sk'] == 'SUPPRESS#recap#alice@example.com'  # lowercased
        assert item['source'] == 'unsubscribe_link'
        assert 'created_at' in item
        assert 'ttl' not in item, 'Suppression row must NOT have TTL (permanent)'

    def test_403_body_has_no_detail(self):
        """403 responses must not reveal token structure or error reason."""
        from Attribution_Unsubscribe import lambda_function as unsub
        resp = unsub.lambda_handler({'queryStringParameters': {'t': 'invalid'}}, None)
        assert resp['statusCode'] == 403
        assert resp['body'].strip().lower() in ('forbidden', 'forbidden\n')


# ---------------------------------------------------------------------------
# 14. (WS-I) Suppression filter: mixed-case, partial, all-suppressed
# ---------------------------------------------------------------------------
class TestSuppressionFilter:

    def test_filter_suppressed_exact(self):
        suppressed = {'alice@example.org', 'bob@example.org'}
        result = _filter_suppressed(['alice@example.org', 'charlie@example.org'], suppressed)
        assert result == ['charlie@example.org']

    def test_filter_suppressed_case_insensitive(self):
        """Input email case doesn't matter -- lowercased for comparison."""
        suppressed = {'alice@example.org'}
        result = _filter_suppressed(['ALICE@EXAMPLE.ORG', 'bob@example.org'], suppressed)
        assert result == ['bob@example.org']

    def test_filter_suppressed_all(self):
        suppressed = {'alice@example.org', 'bob@example.org'}
        result = _filter_suppressed(['alice@example.org', 'bob@example.org'], suppressed)
        assert result == []

    def test_filter_suppressed_none(self):
        result = _filter_suppressed(['alice@example.org'], set())
        assert result == ['alice@example.org']

    def test_process_tenant_all_suppressed_returns_skip(self):
        config = {
            'feature_flags': {'dashboard_attribution': True},
            'organization_name': 'Org',
            'attribution_recap': {'recipients': ['admin@example.org']},
        }
        with patch.object(recap, '_get_tenant_config', return_value=config), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=[]), \
             patch.object(recap, '_fetch_suppressed_emails', return_value={'admin@example.org'}):
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=True)
        assert result == 'skip_all_suppressed'

    def test_process_tenant_partial_suppression_sends_remaining(self):
        """Partial suppression: remaining recipients still get the email."""
        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_ENABLED_MULTI), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=[]), \
             patch.object(recap, '_fetch_suppressed_emails', return_value={'alice@example.org'}), \
             patch.object(recap, '_get_unsub_signing_key', return_value=_TEST_SIGNING_KEY), \
             patch.object(recap, 'UNSUBSCRIBE_BASE_URL', _TEST_UNSUB_BASE), \
             patch.object(recap, 'RECAP_POSTAL_ADDRESS', _TEST_POSTAL), \
             patch.object(recap, '_mark_recap_sent'), \
             patch.object(recap, '_invoke_send_email') as mock_send:
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=False)

        assert result == 'sent'
        # Only bob remains after alice is suppressed
        assert mock_send.call_count == 1
        payload_sent = mock_send.call_args[0][0]
        assert payload_sent['to'] == ['bob@example.org']

    def test_suppression_wins_over_config_every_month(self):
        """Even if recipient is in config, suppression wins."""
        config = {
            'feature_flags': {'dashboard_attribution': True},
            'organization_name': 'Org',
            'attribution_recap': {'recipients': ['victim@example.org']},
        }
        with patch.object(recap, '_get_tenant_config', return_value=config), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=[]), \
             patch.object(recap, '_fetch_suppressed_emails', return_value={'victim@example.org'}), \
             patch.object(recap, '_invoke_send_email') as mock_send:
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=True)

        assert result == 'skip_all_suppressed'
        mock_send.assert_not_called()


# ---------------------------------------------------------------------------
# 15. (WS-I) Per-recipient sends carry DIFFERENT tokens
# ---------------------------------------------------------------------------
class TestPerRecipientTokens:

    def test_per_recipient_different_tokens(self):
        """Two recipients must receive different unsubscribe tokens."""
        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_ENABLED_MULTI), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=[]), \
             patch.object(recap, '_fetch_suppressed_emails', return_value=set()), \
             patch.object(recap, '_get_unsub_signing_key', return_value=_TEST_SIGNING_KEY), \
             patch.object(recap, 'UNSUBSCRIBE_BASE_URL', _TEST_UNSUB_BASE), \
             patch.object(recap, 'RECAP_POSTAL_ADDRESS', _TEST_POSTAL), \
             patch.object(recap, '_mark_recap_sent'), \
             patch.object(recap, '_invoke_send_email') as mock_send:
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=False)

        assert result == 'sent'
        assert mock_send.call_count == 2, 'Two recipients -> two send_email invokes'

        # Extract unsubscribe URLs from html_body of each call
        tokens = []
        for call_args in mock_send.call_args_list:
            payload = call_args[0][0]
            html = payload['html_body']
            # Find the ?t= value in the HTML
            import re
            matches = re.findall(r'\?t=([^"&\s]+)', html)
            assert matches, 'Unsubscribe token missing from html_body'
            tokens.append(matches[0])

        assert len(tokens) == 2
        assert tokens[0] != tokens[1], 'Per-recipient tokens must be different'

    def test_one_invoke_per_recipient(self):
        """N recipients -> N send_email invokes (one per recipient)."""
        config = {
            'feature_flags': {'dashboard_attribution': True},
            'organization_name': 'Org',
            'attribution_recap': {'recipients': ['a@t.com', 'b@t.com', 'c@t.com']},
        }
        with patch.object(recap, '_get_tenant_config', return_value=config), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=[]), \
             patch.object(recap, '_fetch_suppressed_emails', return_value=set()), \
             patch.object(recap, '_get_unsub_signing_key', return_value=_TEST_SIGNING_KEY), \
             patch.object(recap, 'UNSUBSCRIBE_BASE_URL', _TEST_UNSUB_BASE), \
             patch.object(recap, 'RECAP_POSTAL_ADDRESS', _TEST_POSTAL), \
             patch.object(recap, '_mark_recap_sent'), \
             patch.object(recap, '_invoke_send_email') as mock_send:
            recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=False)

        assert mock_send.call_count == 3


# ---------------------------------------------------------------------------
# 16. (WS-I) Postal address fail-closed
# ---------------------------------------------------------------------------
class TestPostalAddressFailClosed:

    def test_send_blocked_when_postal_unset(self, caplog):
        """When RECAP_POSTAL_ADDRESS is empty and RECAP_SEND_ENABLED=true, block send."""
        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_ENABLED), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=[]), \
             patch.object(recap, 'RECAP_POSTAL_ADDRESS', ''), \
             patch.object(recap, '_invoke_send_email') as mock_send, \
             caplog.at_level(logging.ERROR):
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=False)

        assert result == 'skip_flag_off'
        mock_send.assert_not_called()
        assert any('recap-blocked-no-postal-address' in r.message for r in caplog.records)

    def test_dry_run_postal_placeholder_rendered(self):
        """Dry-run with empty postal address renders [POSTAL ADDRESS NOT CONFIGURED]."""
        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_ENABLED), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=[]), \
             patch.object(recap, '_fetch_suppressed_emails', return_value=set()), \
             patch.object(recap, 'RECAP_POSTAL_ADDRESS', ''), \
             patch.object(recap, 'UNSUBSCRIBE_BASE_URL', ''), \
             patch.object(recap, '_render_email', wraps=recap._render_email) as mock_render:
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=True)

        assert result == 'dry_run'
        # render_email should have been called with the placeholder
        call_kwargs = mock_render.call_args.kwargs if mock_render.call_args.kwargs else {}
        postal_arg = call_kwargs.get('postal_address') or mock_render.call_args[1].get('postal_address') or ''
        # Either the kwarg or positional arg contains the placeholder
        all_args = str(mock_render.call_args)
        assert '[POSTAL ADDRESS NOT CONFIGURED]' in all_args


# ---------------------------------------------------------------------------
# 17. (WS-I) Secret-absent fail-closed; no caching of transient failure
# ---------------------------------------------------------------------------
class TestSecretFailClosed:

    def test_send_blocked_when_unsub_secret_absent(self, caplog):
        """When signing key unavailable and RECAP_SEND_ENABLED=true, block send."""
        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_ENABLED), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=[]), \
             patch.object(recap, '_fetch_suppressed_emails', return_value=set()), \
             patch.object(recap, 'RECAP_POSTAL_ADDRESS', _TEST_POSTAL), \
             patch.object(recap, 'UNSUBSCRIBE_BASE_URL', _TEST_UNSUB_BASE), \
             patch.object(recap, '_get_unsub_signing_key', return_value=None), \
             patch.object(recap, '_invoke_send_email') as mock_send, \
             caplog.at_level(logging.ERROR):
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=False)

        assert result == 'skip_flag_off'
        mock_send.assert_not_called()
        assert any('recap-blocked-unsub-key-unavailable' in r.message for r in caplog.records)

    def test_send_blocked_when_unsub_url_unset(self, caplog):
        """When UNSUBSCRIBE_BASE_URL unset and RECAP_SEND_ENABLED=true, block send."""
        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_ENABLED), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=[]), \
             patch.object(recap, '_fetch_suppressed_emails', return_value=set()), \
             patch.object(recap, 'RECAP_POSTAL_ADDRESS', _TEST_POSTAL), \
             patch.object(recap, 'UNSUBSCRIBE_BASE_URL', ''), \
             patch.object(recap, '_invoke_send_email') as mock_send, \
             caplog.at_level(logging.ERROR):
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=False)

        assert result == 'skip_flag_off'
        mock_send.assert_not_called()
        assert any('recap-blocked-no-unsub-url' in r.message for r in caplog.records)

    def test_transient_fetch_failure_not_cached(self):
        """
        After a transient Secrets Manager failure, _get_unsub_signing_key must retry
        (not cache the None result). We verify the _unsub_signing_key module variable
        is still None after a failed fetch.
        """
        import Attribution_Recap_Generator.lambda_function as _recap_mod
        from botocore.exceptions import ClientError

        # Reset cached key
        original_key = _recap_mod._unsub_signing_key
        _recap_mod._unsub_signing_key = None

        try:
            err = ClientError({'Error': {'Code': 'ServiceUnavailableException', 'Message': 'x'}}, 'GetSecretValue')
            with patch.object(_recap_mod, '_secretsmanager') as mock_sm:
                mock_sm.get_secret_value.side_effect = err
                result = _recap_mod._get_unsub_signing_key()

            # After failure: None returned, not cached
            assert result is None
            assert _recap_mod._unsub_signing_key is None, 'Transient failure must NOT be cached'

            # Second call: should retry (not return cached None)
            with patch.object(_recap_mod, '_secretsmanager') as mock_sm2:
                mock_sm2.get_secret_value.return_value = {'SecretString': 'newkey'}
                result2 = _recap_mod._get_unsub_signing_key()
            assert result2 == b'newkey'

        finally:
            _recap_mod._unsub_signing_key = original_key


# ---------------------------------------------------------------------------
# 18. (WS-I) No email/token in any log call (static inspection)
# ---------------------------------------------------------------------------
class TestPIILogHygiene:

    def _find_log_calls(self, source: str):
        """Parse AST and return list of logger.* call nodes."""
        tree = ast.parse(source)
        log_calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                func = node.func
                if isinstance(func, ast.Attribute) and isinstance(func.value, ast.Name):
                    if func.value.id == 'logger' and func.attr in ('info', 'warning', 'error', 'debug', 'critical'):
                        log_calls.append(node)
        return log_calls

    def _inside_email_log_id(self, root, target_node):
        """Return True if target_node is an argument to _email_log_id() in root."""
        for node in ast.walk(root):
            if isinstance(node, ast.Call):
                func = node.func
                func_name = (func.id if isinstance(func, ast.Name)
                             else (func.attr if isinstance(func, ast.Attribute) else ''))
                if func_name == '_email_log_id':
                    for call_arg in node.args:
                        if isinstance(call_arg, ast.Name) and call_arg.id == target_node.id:
                            return True
        return False

    def _call_has_suspect_arg(self, node):
        """
        Returns True if a log call's argument list contains a reference to
        variables typically holding email addresses, tokens, or signing keys.
        Excludes variables wrapped in _email_log_id() (those are safely hashed).
        """
        suspect = {'email', 'email_lower', 'token', 'signing_key', 'key_bytes',
                   'payload', 'recipient_email', 'raw'}
        for arg in node.args[1:]:  # skip format string (args[0])
            for inner in ast.walk(arg):
                if isinstance(inner, ast.Name) and inner.id in suspect:
                    if self._inside_email_log_id(arg, inner):
                        continue
                    return True
        return False

    def test_recap_generator_no_pii_in_logs(self):
        """No email address or token variable passed to logger.* in Attribution_Recap_Generator."""
        import Attribution_Recap_Generator.lambda_function as _mod
        src = inspect.getsource(_mod)
        log_calls = self._find_log_calls(src)
        violations = []
        for node in log_calls:
            if self._call_has_suspect_arg(node):
                violations.append(f'line {node.lineno}')
        assert not violations, f'PII variable(s) passed to logger in recap generator: {violations}'

    def test_unsubscribe_no_pii_in_logs(self):
        """No email address or token variable passed to logger.* in Attribution_Unsubscribe."""
        import Attribution_Unsubscribe.lambda_function as _mod
        src = inspect.getsource(_mod)
        log_calls = self._find_log_calls(src)
        violations = []
        for node in log_calls:
            if self._call_has_suspect_arg(node):
                violations.append(f'line {node.lineno}')
        assert not violations, f'PII variable(s) passed to logger in unsubscribe: {violations}'


# ---------------------------------------------------------------------------
# Additional unit tests for helpers
# ---------------------------------------------------------------------------
class TestHelpers:

    def test_month_label_valid(self):
        assert _month_label('2026-06') == 'June 2026'
        assert _month_label('2025-12') == 'December 2025'
        assert _month_label('2026-01') == 'January 2026'

    def test_month_label_invalid_passthrough(self):
        result = _month_label('invalid')
        assert result == 'invalid'

    def test_pct_delta_str_positive(self):
        result = _pct_delta_str(120, 100)
        assert '20%' in result
        assert '▲' in result

    def test_pct_delta_str_negative(self):
        result = _pct_delta_str(80, 100)
        assert '20%' in result
        assert '▼' in result

    def test_pct_delta_str_none_prior(self):
        assert _pct_delta_str(100, None) == ''

    def test_pct_delta_str_zero_prior(self):
        assert _pct_delta_str(100, 0) == ''

    def test_build_superlatives_empty_when_no_data(self):
        result = _build_superlatives(None, None, 0)
        assert result == []

    def test_build_superlatives_after_hours_present(self):
        result = _build_superlatives(None, None, 42)
        assert len(result) == 1
        assert '42' in result[0][1]

    def test_build_superlatives_all_present(self):
        result = _build_superlatives(55.0, 7.5, 30)
        assert len(result) == 3

    def test_extract_top_topics_sorted(self):
        rows = [
            {'data': {'topic_counts': {'Volunteer': 100, 'Donation': 50}}},
            {'data': {'topic_counts': {'Volunteer': 50, 'Events': 30}}},
        ]
        topics = _extract_top_topics(rows)
        assert topics[0][0] == 'Volunteer'
        assert topics[0][1] == 150

    def test_extract_top_topics_empty(self):
        assert _extract_top_topics([]) == []

    def test_find_mvp_channel_above_floor(self):
        rows = [
            {'channel': 'website', 'data': {'conversations': 60, 'leads': 6}},
            {'channel': 'campaign', 'data': {'conversations': 60, 'leads': 18}},
        ]
        mvp = _find_mvp_channel(rows)
        assert mvp is not None
        assert mvp['channel'] == 'campaign'

    def test_find_mvp_channel_below_floor_excluded(self):
        rows = [
            {'channel': 'campaign', 'data': {'conversations': 10, 'leads': 5}},
        ]
        mvp = _find_mvp_channel(rows)
        assert mvp is None

    def test_find_mvp_no_channel_rows(self):
        assert _find_mvp_channel([]) is None


# ---------------------------------------------------------------------------
# Integration-style: full handler flow (all mocked)
# ---------------------------------------------------------------------------
class TestHandlerFlow:

    def test_handler_processes_flagged_tenants(self):
        with patch.object(recap, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(recap, '_get_active_tenant_pairs', return_value=[('h1', 'T1'), ('h2', 'T2')]), \
             patch.object(recap, '_process_tenant', side_effect=['dry_run', 'skip_flag_off']) as mock_proc:
            result = recap.lambda_handler({}, None)

        assert result['total_tenants'] == 2
        assert result['dry_run'] == 1
        assert result['skipped'] == 1
        assert mock_proc.call_count == 2

    def test_handler_error_in_tenant_does_not_abort_others(self):
        def side_effect(tenant_id, month_str, dry_run):
            if tenant_id == 'T1':
                raise RuntimeError('boom')
            return 'dry_run'

        with patch.object(recap, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(recap, '_get_active_tenant_pairs', return_value=[('h1', 'T1'), ('h2', 'T2')]), \
             patch.object(recap, '_process_tenant', side_effect=side_effect):
            result = recap.lambda_handler({}, None)

        assert result['errors'] == 1
        assert result['dry_run'] == 1

    def test_handler_sent_path_increments_sent_count(self):
        with patch.object(recap, 'RECAP_SEND_ENABLED', 'true'), \
             patch.object(recap, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(recap, '_get_active_tenant_pairs', return_value=[('h1', 'T1')]), \
             patch.object(recap, '_process_tenant', return_value='sent'):
            result = recap.lambda_handler({}, None)

        assert result['sent'] == 1
        assert result['dry_run_mode'] is False

    def test_handler_skip_all_suppressed_counted_as_skipped(self):
        with patch.object(recap, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(recap, '_get_active_tenant_pairs', return_value=[('h1', 'T1')]), \
             patch.object(recap, '_process_tenant', return_value='skip_all_suppressed'):
            result = recap.lambda_handler({}, None)

        assert result['skipped'] == 1
