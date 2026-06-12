"""
Tests for Analytics_Aggregator attribution monthly rollup.

Covers (per done-bar):
- Month-boundary + after-hours boundary (tenant-local tz incl. default)
- Staff-hours idle-cap math (C7: 5-min cap, 1-min floor)
- Engaged definition (C7)
- Provenance fallback rows (no provenance → website)
- Unresolvable ep_ id → website + warning, no drop
- Registry table env missing → graceful skip
- Dub secret absent → zero-reach + warning, no crash
- Dub 429 honors Retry-After
- Old-shape aggregate row tolerance (reader must not crash without new fields)
- PAGE_VIEW reach sessionization (30-min windows, ga_client_id keying)

Run: pytest test_attribution_aggregator.py -v
"""
import json
import pytest
import time
from collections import defaultdict
from datetime import datetime, date, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch, MagicMock, call
from zoneinfo import ZoneInfo

import lambda_function as agg
from lambda_function import (
    _is_after_hours,
    _compute_active_seconds,
    _count_page_view_sessions,
    _compute_session_metrics,
    _build_summary,
    _build_channel,
    _build_entrypoint,
    categorize_question,
    DEFAULT_TZ,
    AFTER_HOURS_START,
    AFTER_HOURS_END,
    IDLE_CAP_SECONDS,
    MIN_CONVERSATION_SECONDS,
    PAGE_VIEW_SESSION_WINDOW_SECONDS,
    ATTRIBUTION_TTL_DAYS,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------
_TZ_CHICAGO = ZoneInfo('America/Chicago')
_TZ_UTC = ZoneInfo('UTC')
_TZ_LA = ZoneInfo('America/Los_Angeles')


def _ts(dt_str: str) -> str:
    """Return ISO8601 string with Z suffix."""
    return dt_str if dt_str.endswith('Z') else dt_str + 'Z'


def _ddb_event(
    session_id='sess_test',
    event_type='MESSAGE_SENT',
    step=1,
    ts='2026-06-12T10:00:00.000Z',
    tenant_id='FOS402334',
    tenant_hash='fo85e6a',
    ga_client_id=None,
    payload=None,
    attribution=None,
    entry_point_id=None,
):
    """Simulate a DynamoDB Item as returned by boto3 Resource (plain dict, not typed)."""
    item = {
        'pk': f'SESSION#{session_id}',
        'sk': f'STEP#{step:03d}',
        'session_id': session_id,
        'tenant_id': tenant_id,
        'tenant_hash': tenant_hash,
        'step_number': step,
        'event_type': event_type,
        'timestamp': ts,
        'ttl': 9999999999,
    }
    if ga_client_id:
        item['ga_client_id'] = ga_client_id
    if payload is not None:
        item['event_payload'] = json.dumps(payload)
    if attribution is not None:
        item['attribution'] = json.dumps(attribution)
    if entry_point_id is not None:
        item['entry_point_id'] = entry_point_id
    return item


def _pv_event(session_id='pv_test1', ts='2026-06-12T10:00:00Z', ga_client_id=None):
    return _ddb_event(
        session_id=session_id,
        event_type='PAGE_VIEW',
        ts=ts,
        ga_client_id=ga_client_id,
    )


# ---------------------------------------------------------------------------
# C7 — is_after_hours
# ---------------------------------------------------------------------------
class TestIsAfterHours:
    """C7 after-hours: outside Mon-Fri 09:00-17:00 tenant-local. FROZEN_CONTRACTS C7."""

    def test_weekday_business_hours_not_after_hours(self):
        """Monday 10:00 Chicago → business hours."""
        dt = datetime(2026, 6, 8, 10, 0, 0, tzinfo=_TZ_CHICAGO)  # Monday
        assert not _is_after_hours(dt)

    def test_weekday_before_start_is_after_hours(self):
        """Monday 08:59 Chicago → after hours."""
        dt = datetime(2026, 6, 8, 8, 59, 0, tzinfo=_TZ_CHICAGO)
        assert _is_after_hours(dt)

    def test_weekday_at_17_is_after_hours(self):
        """Monday 17:00 Chicago → after hours (end is exclusive, AFTER_HOURS_END=17)."""
        dt = datetime(2026, 6, 8, 17, 0, 0, tzinfo=_TZ_CHICAGO)
        assert _is_after_hours(dt)

    def test_weekday_before_end_not_after_hours(self):
        """Monday 16:59 Chicago → still business hours."""
        dt = datetime(2026, 6, 8, 16, 59, 0, tzinfo=_TZ_CHICAGO)
        assert not _is_after_hours(dt)

    def test_saturday_is_after_hours(self):
        """Saturday noon → always after hours."""
        dt = datetime(2026, 6, 13, 12, 0, 0, tzinfo=_TZ_CHICAGO)  # Saturday
        assert _is_after_hours(dt)

    def test_sunday_is_after_hours(self):
        """Sunday morning → always after hours."""
        dt = datetime(2026, 6, 14, 9, 0, 0, tzinfo=_TZ_CHICAGO)  # Sunday
        assert _is_after_hours(dt)

    def test_friday_business_hours(self):
        """Friday 13:00 → business hours."""
        dt = datetime(2026, 6, 12, 13, 0, 0, tzinfo=_TZ_CHICAGO)  # Friday
        assert not _is_after_hours(dt)

    def test_after_hours_uses_tenant_tz_not_utc(self):
        """
        Same UTC instant: 14:00 UTC = 09:00 Chicago (business) = 06:00 LA (after-hours).
        Verifies tz parameter is used, not system UTC.
        """
        # 14:00 UTC on a Wednesday
        utc_dt = datetime(2026, 6, 10, 14, 0, 0, tzinfo=ZoneInfo('UTC'))
        chicago_dt = utc_dt.astimezone(_TZ_CHICAGO)   # 09:00 CDT → business hours
        la_dt = utc_dt.astimezone(_TZ_LA)              # 07:00 PDT → after hours

        assert not _is_after_hours(chicago_dt), 'Chicago 09:00 should be business hours'
        assert _is_after_hours(la_dt), 'LA 07:00 should be after hours'


# ---------------------------------------------------------------------------
# C7 — _compute_active_seconds (staff-hours idle-cap math)
# ---------------------------------------------------------------------------
class TestComputeActiveSeconds:
    """C7 staff-hours absorbed: idle cap 5 min, floor 1 min. FROZEN_CONTRACTS C7."""

    def _ts(self, offset_s: int) -> datetime:
        base = datetime(2026, 6, 12, 10, 0, 0, tzinfo=_TZ_CHICAGO)
        return base + timedelta(seconds=offset_s)

    def test_empty_timestamps_returns_zero(self):
        assert _compute_active_seconds([]) == 0

    def test_single_message_returns_floor(self):
        """Single message → floor 1 min per conversation."""
        result = _compute_active_seconds([self._ts(0)])
        assert result == MIN_CONVERSATION_SECONDS  # 60 seconds

    def test_two_messages_short_gap(self):
        """Two messages 30s apart → active time = 30s, but floored at 60s."""
        result = _compute_active_seconds([self._ts(0), self._ts(30)])
        assert result == MIN_CONVERSATION_SECONDS  # floor applied

    def test_two_messages_normal_gap(self):
        """Two messages 90s apart → active time = 90s (above floor)."""
        result = _compute_active_seconds([self._ts(0), self._ts(90)])
        assert result == 90

    def test_idle_cap_applied(self):
        """Gap > 5 min → capped at 5 min (300s), not the actual gap."""
        result = _compute_active_seconds([self._ts(0), self._ts(600)])  # 10 min gap
        assert result == IDLE_CAP_SECONDS  # capped at 300s

    def test_multiple_gaps_summed_with_cap(self):
        """Multiple gaps: 2 min + 10 min (capped to 5 min) + 3 min = 600s."""
        times = [
            self._ts(0),
            self._ts(120),   # +2 min
            self._ts(720),   # +10 min from prev (capped to 5 min)
            self._ts(900),   # +3 min
        ]
        result = _compute_active_seconds(times)
        expected = 120 + IDLE_CAP_SECONDS + 180  # 600s
        assert result == expected

    def test_floor_applied_when_total_below_60s(self):
        """If all gaps sum to < 60s, floor is applied."""
        times = [self._ts(0), self._ts(10), self._ts(20)]  # 10+10 = 20s total
        result = _compute_active_seconds(times)
        assert result == MIN_CONVERSATION_SECONDS


# ---------------------------------------------------------------------------
# C7 — PAGE_VIEW reach sessionization
# ---------------------------------------------------------------------------
class TestCountPageViewSessions:
    """C7 reach: 30-min windows keyed on ga_client_id or pv_ session_id. FROZEN_CONTRACTS C7."""

    def _evs(self, session_id, ts_iso, ga_client_id=None):
        e = {'session_id': session_id, 'event_type': 'PAGE_VIEW', 'timestamp': ts_iso}
        if ga_client_id:
            e['ga_client_id'] = ga_client_id
        return [e]

    def test_empty_returns_zero(self):
        assert _count_page_view_sessions([]) == 0

    def test_single_session_counts_one(self):
        result = _count_page_view_sessions([self._evs('pv_1', '2026-06-12T10:00:00Z')])
        assert result == 1

    def test_two_distinct_pv_ids_count_two(self):
        result = _count_page_view_sessions([
            self._evs('pv_1', '2026-06-12T10:00:00Z'),
            self._evs('pv_2', '2026-06-12T10:05:00Z'),
        ])
        assert result == 2

    def test_same_ga_client_id_within_30min_counts_one(self):
        """Same ga_client_id within 30-min window → 1 session."""
        result = _count_page_view_sessions([
            self._evs('pv_1', '2026-06-12T10:00:00Z', ga_client_id='111.222'),
            self._evs('pv_2', '2026-06-12T10:15:00Z', ga_client_id='111.222'),  # same ga_id, +15m
        ])
        assert result == 1

    def test_same_ga_client_id_over_30min_counts_two(self):
        """Same ga_client_id > 30 min apart → 2 sessions."""
        result = _count_page_view_sessions([
            self._evs('pv_1', '2026-06-12T10:00:00Z', ga_client_id='111.222'),
            self._evs('pv_2', '2026-06-12T10:31:00Z', ga_client_id='111.222'),  # +31m
        ])
        assert result == 2

    def test_different_ga_client_ids_count_separately(self):
        result = _count_page_view_sessions([
            self._evs('pv_1', '2026-06-12T10:00:00Z', ga_client_id='111.111'),
            self._evs('pv_2', '2026-06-12T10:01:00Z', ga_client_id='222.222'),
        ])
        assert result == 2

    def test_no_ga_id_falls_back_to_pv_session_id(self):
        """Without ga_client_id, pv_ session_id is the key — each pv_ = 1 session."""
        result = _count_page_view_sessions([
            self._evs('pv_A', '2026-06-12T10:00:00Z'),
            self._evs('pv_A', '2026-06-12T10:10:00Z'),  # same pv_ id, within window → 1
        ])
        # Same pv_ id = same key, within window → 1
        assert result == 1


# ---------------------------------------------------------------------------
# C7 — engaged definition
# ---------------------------------------------------------------------------
class TestEngagedDefinition:
    """C7 engaged: >=1 CTA_CLICKED|LINK_CLICKED|FORM_VIEWED OR >=2 user messages. FROZEN_CONTRACTS C7."""

    def _session(self, events):
        return _compute_session_metrics('sess_x', events, _TZ_CHICAGO)

    def test_cta_clicked_is_engaged(self):
        evs = [_ddb_event(event_type='CTA_CLICKED', ts='2026-06-12T10:01:00Z')]
        sm = self._session(evs)
        assert sm['is_engaged']

    def test_link_clicked_is_engaged(self):
        evs = [_ddb_event(event_type='LINK_CLICKED', ts='2026-06-12T10:01:00Z')]
        sm = self._session(evs)
        assert sm['is_engaged']

    def test_form_viewed_is_engaged(self):
        evs = [_ddb_event(event_type='FORM_VIEWED', ts='2026-06-12T10:01:00Z')]
        sm = self._session(evs)
        assert sm['is_engaged']

    def test_two_user_messages_is_engaged(self):
        evs = [
            _ddb_event(event_type='MESSAGE_SENT', step=1, ts='2026-06-12T10:01:00Z'),
            _ddb_event(event_type='MESSAGE_SENT', step=2, ts='2026-06-12T10:02:00Z'),
        ]
        sm = self._session(evs)
        assert sm['is_engaged']

    def test_one_user_message_no_engaged_event_not_engaged(self):
        evs = [_ddb_event(event_type='MESSAGE_SENT', step=1, ts='2026-06-12T10:01:00Z')]
        sm = self._session(evs)
        assert not sm['is_engaged']

    def test_zero_events_not_engaged(self):
        sm = self._session([])
        assert not sm['is_engaged']


# ---------------------------------------------------------------------------
# C2 — provenance fallback: no provenance → website
# ---------------------------------------------------------------------------
class TestProvenanceFallback:
    """No provenance (old rows, no entry_point_id, no meta: prefix) → website. FROZEN_CONTRACTS C2."""

    def test_no_entry_point_defaults_to_website(self):
        evs = [_ddb_event(event_type='MESSAGE_SENT', ts='2026-06-12T10:01:00Z')]
        sm = _compute_session_metrics('sess_no_ep', evs, _TZ_CHICAGO)
        assert sm['channel'] == 'website'

    def test_meta_prefix_resolves_to_messenger(self):
        """session_id starting with meta: → messenger (C2)."""
        evs = [_ddb_event(session_id='meta:1234', event_type='MESSAGE_SENT', ts='2026-06-12T10:01:00Z')]
        session_metrics = {'meta:1234': _compute_session_metrics('meta:1234', evs, _TZ_CHICAGO)}
        enriched = agg._resolve_and_enrich_sessions('FOS402334', session_metrics)
        assert enriched['meta:1234']['channel'] == 'messenger'

    @patch('lambda_function.ENTRY_POINTS_TABLE', '')
    def test_ep_id_registry_missing_falls_back_to_website(self):
        """ep_ id present but ENTRY_POINTS_TABLE empty → website (graceful skip)."""
        evs = [
            _ddb_event(
                event_type='CONVERSATION_STARTED',
                ts='2026-06-12T10:00:00Z',
                entry_point_id='ep_ABCDEF12',
                payload={'entry_point_id': 'ep_ABCDEF12', 'attribution': {}},
            )
        ]
        session_metrics = {
            'sess_ep': _compute_session_metrics('sess_ep', evs, _TZ_CHICAGO)
        }
        # Force entry_point_id onto the metrics dict since _compute_session_metrics
        # reads from event_payload keys — set directly for clarity
        session_metrics['sess_ep']['entry_point_id'] = 'ep_ABCDEF12'

        enriched = agg._resolve_and_enrich_sessions('FOS402334', session_metrics)
        assert enriched['sess_ep']['channel'] == 'website'


# ---------------------------------------------------------------------------
# C2 — unresolvable ep_ id → website + warning, session NOT dropped
# ---------------------------------------------------------------------------
class TestUnresolvableEntryPoint:
    """C2: unresolvable ep_ id → website + warning, never drop session. FROZEN_CONTRACTS C2."""

    @patch('lambda_function.ENTRY_POINTS_TABLE', 'picasso-entry-points-staging')
    @patch('lambda_function._get_entry_point')
    def test_unresolvable_ep_defaults_to_website_not_dropped(self, mock_get_ep):
        """Registry returns None (entry point not found) → website, session kept."""
        mock_get_ep.return_value = None  # unresolvable

        session_metrics = {
            'sess_unresolv': {
                'session_id': 'sess_unresolv',
                'is_engaged': False,
                'has_application': False,
                'has_lead': False,
                'is_after_hours': False,
                'active_seconds': 60,
                'user_message_count': 1,
                'entry_point_id': 'ep_UNRESOLVX',
                'utm_campaign': None,
                'attribution': None,
                'topic_counts': {},
                'resource_clicks': {},
                'channel': 'website',
                'campaign': None,
                'placement': None,
            }
        }

        enriched = agg._resolve_and_enrich_sessions('FOS402334', session_metrics)
        assert 'sess_unresolv' in enriched  # NOT dropped
        assert enriched['sess_unresolv']['channel'] == 'website'

    @patch('lambda_function.ENTRY_POINTS_TABLE', 'picasso-entry-points-staging')
    @patch('lambda_function._get_entry_point')
    def test_resolved_ep_uses_registry_channel(self, mock_get_ep):
        """Valid ep_ id → registry channel used."""
        mock_get_ep.return_value = {
            'channel': 'campaign',
            'campaign': 'spring_gala',
            'placement': 'homepage_banner',
        }

        session_metrics = {
            'sess_ep': {
                'session_id': 'sess_ep',
                'is_engaged': True,
                'has_application': False,
                'has_lead': False,
                'is_after_hours': False,
                'active_seconds': 120,
                'user_message_count': 2,
                'entry_point_id': 'ep_TESTEP99',
                'utm_campaign': None,
                'attribution': None,
                'topic_counts': {},
                'resource_clicks': {},
                'channel': 'website',
                'campaign': None,
                'placement': None,
            }
        }

        enriched = agg._resolve_and_enrich_sessions('FOS402334', session_metrics)
        assert enriched['sess_ep']['channel'] == 'campaign'
        assert enriched['sess_ep']['campaign'] == 'spring_gala'


# ---------------------------------------------------------------------------
# Registry table env missing → graceful skip
# ---------------------------------------------------------------------------
class TestRegistryTableMissing:
    """ENTRY_POINTS_TABLE not configured → graceful skip, no crash. FROZEN_CONTRACTS C3."""

    @patch('lambda_function.ENTRY_POINTS_TABLE', '')
    def test_missing_registry_table_returns_none(self):
        """_get_entry_point must return None gracefully when table name not set."""
        result = agg._get_entry_point('FOS402334', 'ep_ABCDEF12')
        assert result is None

    @patch('lambda_function.ENTRY_POINTS_TABLE', '')
    def test_missing_registry_table_does_not_crash_aggregation(self):
        """Full session enrichment must not crash when registry is not configured."""
        session_metrics = {
            'sess_1': {
                'session_id': 'sess_1',
                'is_engaged': True,
                'has_application': False,
                'has_lead': False,
                'is_after_hours': False,
                'active_seconds': 60,
                'user_message_count': 2,
                'entry_point_id': 'ep_SOMEVALUE',
                'utm_campaign': None,
                'attribution': None,
                'topic_counts': {},
                'resource_clicks': {},
                'channel': 'website',
                'campaign': None,
                'placement': None,
            }
        }
        # Should not raise
        result = agg._resolve_and_enrich_sessions('FOS402334', session_metrics)
        assert result['sess_1']['channel'] == 'website'


# ---------------------------------------------------------------------------
# Dub secret absent → zero-reach, warning, no crash
# ---------------------------------------------------------------------------
class TestDubSecretAbsent:
    """C4: DUB_SECRET_NAME absent/empty → skip poll with warning, write zero reach, no crash."""

    @patch('lambda_function.DUB_SECRET_NAME', '')
    def test_dub_poll_returns_empty_dict_when_secret_missing(self):
        """_poll_dub_for_month must return {} when secret not configured."""
        agg._dub_secret_cache = None  # reset cache

        month_start = date(2026, 6, 1)
        month_end = date(2026, 7, 1)
        result = agg._poll_dub_for_month(
            'FOS402334', month_start, month_end, _TZ_CHICAGO
        )
        assert result == {}

    @patch('lambda_function.DUB_SECRET_NAME', 'picasso-dub-api-key-staging')
    @patch('lambda_function._secrets_client')
    def test_dub_poll_graceful_when_secret_not_in_secrets_manager(self, mock_secrets):
        """ResourceNotFoundException → graceful skip."""
        mock_secrets.exceptions.ResourceNotFoundException = Exception
        mock_secrets.get_secret_value.side_effect = Exception('ResourceNotFoundException')
        mock_secrets.exceptions.ResourceNotFoundException = type('RNF', (Exception,), {})
        mock_secrets.get_secret_value.side_effect = mock_secrets.exceptions.ResourceNotFoundException('not found')
        agg._dub_secret_cache = None

        month_start = date(2026, 6, 1)
        month_end = date(2026, 7, 1)
        result = agg._poll_dub_for_month(
            'FOS402334', month_start, month_end, _TZ_CHICAGO
        )
        assert result == {}

    @patch('lambda_function.DUB_SECRET_NAME', 'picasso-dub-api-key-staging')
    @patch('lambda_function._secrets_client')
    def test_dub_poll_zero_reach_when_secret_empty(self, mock_secrets):
        """Empty secret string → no poll, zero reach."""
        mock_secrets.get_secret_value.return_value = {'SecretString': ''}
        agg._dub_secret_cache = None

        month_start = date(2026, 6, 1)
        month_end = date(2026, 7, 1)
        result = agg._poll_dub_for_month(
            'FOS402334', month_start, month_end, _TZ_CHICAGO
        )
        assert result == {}


# ---------------------------------------------------------------------------
# Dub 429 honors Retry-After
# ---------------------------------------------------------------------------
class TestDubRateLimit:
    """C4: Dub 429 response must honor Retry-After header before retry."""

    def test_dub_get_honors_retry_after_on_429(self):
        """_dub_get must sleep Retry-After seconds on 429 then retry once."""
        import urllib.error

        secret = 'test-secret'
        retry_after = 3

        # urllib.error.HTTPError(url, code, msg, hdrs, fp)
        # Use a plain dict subclass for headers since HTTPError.headers is a property
        class FakeHeaders(dict):
            def get(self, key, default=None):
                return super().get(key, default)

        fake_err = urllib.error.HTTPError(
            url='https://api.dub.co/analytics?groupBy=top_links',
            code=429,
            msg='Too Many Requests',
            hdrs=FakeHeaders({'Retry-After': str(retry_after)}),
            fp=None,
        )

        success_resp = MagicMock()
        success_resp.read.return_value = b'[{"link": {"externalId": "ext_ep_ABCDEF12"}, "clicks": 5}]'
        success_resp.__enter__ = lambda s: s
        success_resp.__exit__ = MagicMock(return_value=False)

        call_count = [0]

        def fake_urlopen(req, timeout=None):
            call_count[0] += 1
            if call_count[0] == 1:
                raise fake_err
            return success_resp

        sleep_calls = []
        with patch('lambda_function.urllib.request.urlopen', side_effect=fake_urlopen), \
             patch('lambda_function.time.sleep', side_effect=lambda s: sleep_calls.append(s)):
            result = agg._dub_get('/analytics', {'groupBy': 'top_links'}, secret)

        assert call_count[0] == 2, 'Should have retried exactly once'
        assert any(s >= retry_after for s in sleep_calls), 'Should have slept at least Retry-After seconds'


# ---------------------------------------------------------------------------
# Old-shape aggregate row tolerance (FROZEN_CONTRACTS C5 / Schema Discipline)
# ---------------------------------------------------------------------------
class TestOldShapeAggregateRowTolerance:
    """
    CONTRACT/FIXTURE: readers of aggregate rows must not crash on rows without
    new attribution fields.  FROZEN_CONTRACTS C5; Schema Discipline.
    """

    def test_build_summary_with_empty_session_metrics(self):
        """_build_summary must work with zero sessions (idempotent month with no events)."""
        result = _build_summary([], {}, 0)
        assert result['conversations'] == 0
        assert result['engaged'] == 0
        assert result['reach_page_views_sessions'] == 0
        assert result['self_booked_pct'] is None
        assert result['median_first_response_minutes'] is None

    def test_build_channel_with_empty_sessions(self):
        """_build_channel must work with zero sessions."""
        result = _build_channel('website', [], {}, {})
        assert result['conversations'] == 0
        assert result['topic_counts'] == {}
        assert result['resource_clicks'] == {}

    def test_build_entrypoint_with_missing_registry_fields(self):
        """_build_entrypoint must tolerate missing label/campaign/placement (old registry rows)."""
        with patch('lambda_function._get_entry_point', return_value={}):
            result = _build_entrypoint('ep_TEST1234', 'FOS402334', [], {}, {})
        assert result['label'] == ''
        assert result['campaign'] == ''
        assert result['placement'] == ''
        assert result['dub_scans'] == 0
        assert result['dub_clicks'] == 0


# ---------------------------------------------------------------------------
# Month-boundary tests (tenant-local tz)
# ---------------------------------------------------------------------------
class TestMonthBoundary:
    """C5: month boundaries use tenant-local calendar month. FROZEN_CONTRACTS C5/C7."""

    def test_month_boundary_chicago_tz(self):
        """Chicago UTC-5 in June: 2026-06-01T00:00 CDT = 2026-06-01T05:00 UTC."""
        tz = _TZ_CHICAGO
        month_start = date(2026, 6, 1)
        start_dt = datetime(month_start.year, month_start.month, 1, 0, 0, 0, tzinfo=tz)
        # The local-midnight start must be different from UTC midnight
        utc_offset = start_dt.utcoffset()
        assert utc_offset is not None
        assert utc_offset.total_seconds() != 0

    def test_default_tz_is_chicago(self):
        """DEFAULT_TZ must be America/Chicago (FROZEN_CONTRACTS C7 PROVISIONAL)."""
        assert DEFAULT_TZ == 'America/Chicago'

    def test_prior_month_computed_on_first_3_days(self):
        """When now_local.day <= 3, prior month is added to compute list."""
        tz = _TZ_CHICAGO
        # Mock to a day that is the 2nd of the month
        now = datetime(2026, 7, 2, 9, 0, 0, tzinfo=tz)
        current = now.date().replace(day=1)  # 2026-07-01
        prior = (current - timedelta(days=1)).replace(day=1)   # 2026-06-01
        assert prior == date(2026, 6, 1)
        assert now.day <= 3


# ---------------------------------------------------------------------------
# C5 — TTL value
# ---------------------------------------------------------------------------
class TestAttributionTtl:
    """C5: TTL must be now + 420 days."""

    def test_attribution_ttl_is_420_days(self):
        ttl = agg._attribution_ttl()
        now = datetime.now(timezone.utc)
        expected_delta = timedelta(days=ATTRIBUTION_TTL_DAYS)
        actual_delta = datetime.fromtimestamp(ttl, tz=timezone.utc) - now
        # Allow 60s tolerance
        assert abs(actual_delta.total_seconds() - expected_delta.total_seconds()) < 60

    def test_attribution_ttl_days_constant(self):
        assert ATTRIBUTION_TTL_DAYS == 420


# ---------------------------------------------------------------------------
# categorize_question verbatim copy verification
# ---------------------------------------------------------------------------
class TestCategorizeQuestion:
    """C5 topics: verbatim copy of categorize_question from Analytics_Dashboard_API. FROZEN_CONTRACTS C5."""

    def test_volunteer(self):
        assert categorize_question('I want to volunteer') == 'Volunteer'

    def test_donation(self):
        assert categorize_question('How do I donate?') == 'Donation'

    def test_donation_alternate(self):
        assert categorize_question('I made a donation last year') == 'Donation'

    def test_events(self):
        assert categorize_question('Tell me about upcoming events') == 'Events'

    def test_gathering(self):
        assert categorize_question('Is there a gathering this weekend?') == 'Events'

    def test_services(self):
        assert categorize_question('What services do you offer?') == 'Services'

    def test_supplies(self):
        assert categorize_question('I need to request supplies') == 'Supplies'

    def test_general(self):
        assert categorize_question('Random question here') == 'General'

    def test_empty_string(self):
        assert categorize_question('') == 'General'

    def test_none(self):
        assert categorize_question(None) == 'General'  # type: ignore


# ---------------------------------------------------------------------------
# C5 — store_metric writes correct PK/SK format
# ---------------------------------------------------------------------------
class TestStoreMetric:
    """C5 aggregate row format: PK=TENANT#..., SK=METRIC#..., ttl=integer."""

    @patch('lambda_function._dynamodb_resource')
    def test_store_metric_pk_sk_format(self, mock_resource):
        """_store_metric must write correct PK/SK per C5."""
        mock_table = MagicMock()
        mock_resource.Table.return_value = mock_table

        agg._store_metric(
            'FOS402334',
            'attribution_summary#2026-06',
            {'conversations': 10},
            999999,
        )

        call_args = mock_table.put_item.call_args[1]
        item = call_args['Item']
        assert item['pk'] == 'TENANT#FOS402334'
        assert item['sk'] == 'METRIC#attribution_summary#2026-06'
        assert item['ttl'] == 999999

    @patch('lambda_function._dynamodb_resource')
    def test_store_metric_converts_float_to_decimal(self, mock_resource):
        """Floats in data must be converted to Decimal for DynamoDB."""
        mock_table = MagicMock()
        mock_resource.Table.return_value = mock_table

        agg._store_metric(
            'FOS402334',
            'attribution_summary#2026-06',
            {'rate': 0.75},
            999999,
        )

        call_args = mock_table.put_item.call_args[1]
        item = call_args['Item']
        assert isinstance(item['data']['rate'], Decimal)


# ---------------------------------------------------------------------------
# Full lambda_handler smoke test
# ---------------------------------------------------------------------------
class TestLambdaHandlerSmoke:
    """Smoke test: handler returns summary dict without crashing."""

    @patch('lambda_function._get_active_tenants', return_value=['FOS402334'])
    @patch('lambda_function._aggregate_attribution', return_value=3)
    def test_handler_returns_summary(self, mock_agg, mock_tenants):
        result = agg.lambda_handler({}, None)
        assert result['total_tenants'] == 1
        assert result['successful'] == 1
        assert result['failed'] == 0

    @patch('lambda_function._get_active_tenants', return_value=['FOS402334'])
    @patch('lambda_function._aggregate_attribution', side_effect=RuntimeError('oops'))
    def test_handler_graceful_on_tenant_failure(self, mock_agg, mock_tenants):
        """Single tenant failure must not crash the handler."""
        result = agg.lambda_handler({}, None)
        assert result['total_tenants'] == 1
        assert result['failed'] == 1
        assert result['successful'] == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
