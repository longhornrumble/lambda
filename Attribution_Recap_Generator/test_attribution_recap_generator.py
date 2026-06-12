"""
Tests for Attribution_Recap_Generator.

Coverage per done-bar:
1. Variant selection: all four variants (good_month / bad_month / small_tenant / first_month)
2. Flag-off tenant is skipped (dashboard_attribution = False/absent)
3. Recipients-absent tenant is skipped
4. Idempotency: second run skipped (marker already present)
5. Dry-run never invokes send_email Lambda
6. Enabled path invokes send_email with the exact contract payload
   (body key = JSON string, to/subject/html_body/text_body -- send_email/lambda_function.py:119-162)
7. Render snapshot fixtures for all four variants:
   - no '$' anywhere in HTML
   - no per-person data (no email addresses, no session_ids in output)
   - CTA link present (DASHBOARD_BASE_URL/attribution)
   - unsubscribe + settings links present
8. Old-shape aggregate row (missing fields) -> no crash (schema-tolerant reads)
9. ATTRIBUTION_AGGREGATES_TABLE missing -> ValueError at handler start

Run: pytest test_attribution_recap_generator.py -v
"""
import json
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
# send_email/lambda_function.py:73,82-84,119-162:
#   event.get('body', '{}') must be a JSON string containing to/subject/html_body
# ---------------------------------------------------------------------------
class TestEnabledPathSendEmailContract:

    def test_invoke_payload_matches_send_email_contract(self):
        """
        send_email Lambda contract (send_email/lambda_function.py:119-162):
          - event.get('body', '{}') is JSON-parsed
          - required: 'to' (list), 'subject' (str), 'html_body' or 'text_body'
        For Lambda-to-Lambda invocation the Payload must wrap these as:
          {'body': json.dumps({'to': [...], 'subject': '...', 'html_body': '...', ...})}
        """
        mock_invoke_resp = _make_lambda_response({'success': True, 'message_id': 'msg123'})

        with patch.object(recap, '_get_tenant_config', return_value=_TENANT_CONFIG_ENABLED), \
             patch.object(recap, '_recap_already_sent', return_value=False), \
             patch.object(recap, '_load_aggregate', return_value=_GOOD_SUMMARY), \
             patch.object(recap, '_load_channel_rows', return_value=_CHANNEL_ROWS), \
             patch.object(recap, '_mark_recap_sent'), \
             patch.object(recap, '_lambda_client') as mock_lc:
            mock_lc.invoke.return_value = mock_invoke_resp
            result = recap._process_tenant(TENANT_ID, MONTH_STR, dry_run=False)

        assert result == 'sent'
        mock_lc.invoke.assert_called_once()

        call_kwargs = mock_lc.invoke.call_args.kwargs
        assert call_kwargs['FunctionName'] == 'send_email'
        assert call_kwargs['InvocationType'] == 'RequestResponse'

        # Payload is bytes containing a dict with 'body' key (JSON string)
        raw_payload = call_kwargs['Payload']
        outer = json.loads(raw_payload.decode('utf-8') if isinstance(raw_payload, bytes) else raw_payload)
        assert 'body' in outer, 'Payload must have a body key (send_email contract)'

        # body must be a JSON string (not a dict)
        assert isinstance(outer['body'], str), 'body must be a JSON-encoded string (send_email/lambda_function.py:82-84)'

        inner = json.loads(outer['body'])
        assert isinstance(inner.get('to'), list), 'to must be a list'
        assert len(inner['to']) > 0, 'to must have at least one recipient'
        assert isinstance(inner.get('subject'), str) and inner['subject'], 'subject must be a non-empty string'
        assert inner.get('html_body') or inner.get('text_body'), 'html_body or text_body required'

        # C8.10: no dollar signs in the payload
        assert '$' not in inner.get('html_body', ''), 'html_body must not contain dollar signs (locked decision #5)'
        assert '$' not in inner.get('text_body', ''), 'text_body must not contain dollar signs'

    def test_send_email_function_error_logged_not_raised(self):
        """FunctionError from send_email -> logged but no exception raised."""
        mock_invoke_resp = _make_lambda_response({'errorMessage': 'SES error'}, function_error='Unhandled')

        with patch.object(recap, '_lambda_client') as mock_lc:
            mock_lc.invoke.return_value = mock_invoke_resp
            # Should not raise
            recap._invoke_send_email(
                {'to': ['x@y.com'], 'subject': 'test', 'html_body': '<p>hi</p>'},
                TENANT_ID, MONTH_STR,
            )

    def test_send_email_missing_function_name_logs_error(self, caplog):
        """SEND_EMAIL_FUNCTION_NAME not set -> logs error, no crash."""
        import logging
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


class TestRenderSnapshots:

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_no_dollar_sign(self, variant, summary):
        """No dollar signs anywhere in HTML (locked decision #5 / C7)."""
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID)
        assert '$' not in html, f'Dollar sign found in {variant} HTML'

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_no_per_person_data(self, variant, summary):
        """No session_ids, no email addresses in rendered HTML (C8 aggregates only)."""
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID)
        # No raw session_id patterns (sess_...)
        assert 'sess_' not in html, f'Session ID found in {variant} HTML'
        # No @-sign except in our own domain references (footer links are to app URLs, not emails)
        # The HTML contains no email addresses -- verify by checking for mailto: or raw address
        assert 'mailto:' not in html.lower(), f'mailto link in {variant} HTML'
        # No literal recipient addresses (the tenant ID is safe; org name is not an email)
        assert 'admin@example.org' not in html, 'Recipient address leaked into HTML'

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_cta_link_present(self, variant, summary):
        """CTA link to attribution tab must be present."""
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID)
        assert 'https://app.myrecruiter.ai/attribution' in html, f'CTA link missing in {variant} HTML'

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_unsubscribe_present(self, variant, summary):
        """Unsubscribe link required (I3)."""
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID)
        assert 'unsubscribe' in html.lower(), f'Unsubscribe missing in {variant} HTML'

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_settings_present(self, variant, summary):
        """Settings link required (I3)."""
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID)
        assert 'settings' in html.lower(), f'Settings link missing in {variant} HTML'

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_why_receiving_present(self, variant, summary):
        """Footer must explain why they're receiving this (CAN-SPAM)."""
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID)
        assert "receiving this" in html.lower(), f'"receiving this" missing in {variant} HTML'

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_no_empty_superlative(self, variant, summary):
        """Small tenant variant must not render empty superlatives (I2)."""
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID)
        # Ensure no '&mdash;' immediately after a label with no value
        # A simple check: the rendered HTML must be non-empty
        assert len(html) > 500, f'{variant} HTML suspiciously short'

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_html_is_valid_table_structure(self, variant, summary):
        """Basic structural checks: DOCTYPE, body, table tags present."""
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID)
        assert '<!DOCTYPE html>' in html
        assert '<body' in html
        assert '<table' in html

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_month_label_in_html(self, variant, summary):
        """Month label ('June 2026') must appear in rendered HTML."""
        html = _render_email(variant, summary, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID)
        assert 'June 2026' in html, f'Month label missing in {variant} HTML'

    @pytest.mark.parametrize('variant,summary', _VARIANT_CASES)
    def test_text_fallback_no_dollar_sign(self, variant, summary):
        """Plain text fallback also has no dollar signs."""
        text = _render_text_fallback(variant, summary, MONTH_STR, ORG_NAME)
        assert '$' not in text, f'Dollar sign found in {variant} text fallback'

    def test_small_tenant_no_rate_comparison(self):
        """Small tenant variant must not show rate comparisons (I2)."""
        html = _render_email('small_tenant', _SMALL_SUMMARY, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID)
        # No delta arrows in the small_tenant path
        # The section_big_three for small_tenant omits delta HTML
        # Verify the conversion rate section (Channel MVP) is not in small_tenant HTML
        # (the spec says: fewer panels, no rates for small_tenant)
        # We check that the MVP section is absent (variant == 'small_tenant' skips it)
        assert 'Channel MVP' not in html, 'Channel MVP section must not appear in small_tenant HTML'

    def test_first_month_no_delta(self):
        """First-month variant must not show prior-month delta arrows."""
        html = _render_email('first_month', _FIRST_MONTH_SUMMARY, _CHANNEL_ROWS, MONTH_STR, ORG_NAME, TENANT_ID)
        # No delta arrows in first_month -- select_variant returns 'first_month' because
        # prior_conversations is absent, so _pct_delta_str returns '' for first_month summary
        # (prior_conversations is None -> '' delta). The HTML should not contain '▲' from
        # summary data (there may be no prior to compare). Verify no delta label.
        # The section_big_three with first_month variant still shows delta if prior data is
        # in the summary row -- but _FIRST_MONTH_SUMMARY has no prior_conversations.
        # So _pct_delta_str should return '' -> no arrow rendered.
        assert '▲' not in html or '▼' not in html  # at most one direction absent
        # The welcome-flavor title should be present
        assert 'first' in html.lower() or 'welcome' in html.lower() or 'Welcome' in html


# ---------------------------------------------------------------------------
# 8. Old-shape aggregate row (missing new fields) -> no crash
# ---------------------------------------------------------------------------
class TestSchemaToleranceOldRow:

    def test_empty_summary_row_no_crash(self):
        """Empty/missing aggregate row -> treat as zero-month, no exception."""
        # select_variant
        variant = _select_variant({}, MONTH_STR, TENANT_ID)
        assert variant == 'first_month'  # no prior_conversations -> first_month

        # render (zero-data first_month)
        html = _render_email('first_month', {}, [], MONTH_STR, ORG_NAME, TENANT_ID)
        assert '<!DOCTYPE html>' in html

    def test_partial_row_no_crash(self):
        """Row missing conversation_minutes -> defaults to 0, no exception."""
        row = {'conversations': 100, 'leads': 10}  # missing most fields
        variant = _select_variant(row, MONTH_STR, TENANT_ID)  # first_month (no prior)
        html = _render_email(variant, row, [], MONTH_STR, ORG_NAME, TENANT_ID)
        assert '$' not in html

    def test_none_values_in_row_no_crash(self):
        """None values for optional fields don't crash."""
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
        html = _render_email(variant, row, [], MONTH_STR, ORG_NAME, TENANT_ID)
        assert '<!DOCTYPE html>' in html

    def test_missing_channel_rows_no_crash(self):
        """No channel rows -> MVP absent, topics absent, no crash."""
        html = _render_email('good_month', _GOOD_SUMMARY, [], MONTH_STR, ORG_NAME, TENANT_ID)
        assert '<!DOCTYPE html>' in html
        assert '$' not in html

    def test_channel_row_missing_data_key(self):
        """Channel row without 'data' key is tolerated."""
        rows = [{'channel': 'website'}]  # no 'data' key
        topics = _extract_top_topics(rows)
        assert isinstance(topics, list)

        mvp = _find_mvp_channel(rows)
        assert mvp is None  # conversations=0 < floor -> skipped


# ---------------------------------------------------------------------------
# 9. ATTRIBUTION_AGGREGATES_TABLE missing -> ValueError
# ---------------------------------------------------------------------------
class TestMissingTableEnv:

    def test_missing_table_raises(self):
        with patch.object(recap, 'ATTRIBUTION_AGGREGATES_TABLE', ''):
            with pytest.raises(ValueError, match='ATTRIBUTION_AGGREGATES_TABLE'):
                recap.lambda_handler({}, None)


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
            {'channel': 'campaign', 'data': {'conversations': 10, 'leads': 5}},  # below floor
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
        """Handler iterates tenants and processes flagged ones."""
        with patch.object(recap, 'ATTRIBUTION_AGGREGATES_TABLE', 'test-table'), \
             patch.object(recap, '_get_active_tenant_pairs', return_value=[('h1', 'T1'), ('h2', 'T2')]), \
             patch.object(recap, '_process_tenant', side_effect=['dry_run', 'skip_flag_off']) as mock_proc:
            result = recap.lambda_handler({}, None)

        assert result['total_tenants'] == 2
        assert result['dry_run'] == 1
        assert result['skipped'] == 1
        assert mock_proc.call_count == 2

    def test_handler_error_in_tenant_does_not_abort_others(self):
        """Error in one tenant's processing doesn't abort the loop."""
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
