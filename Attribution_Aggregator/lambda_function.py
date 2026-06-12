"""
Attribution Aggregator Lambda -- Attribution Monthly Rollups
============================================================

Re-homed from Analytics_Aggregator/ per FROZEN_CONTRACTS C5 change-log 2026-06-12.
Reason: legacy Analytics_Aggregator (zip-only, removal-slated, not twinned to staging)
must NOT be extended.  This is a NEW Lambda deployed as Attribution_Aggregator.

Computes attribution aggregate rows per FROZEN_CONTRACTS C5 and writes them to the
NEW picasso-attribution-aggregates table (env ATTRIBUTION_AGGREGATES_TABLE).

Key patterns (C5):
  pk  TENANT#{tenant_id}
  sk  METRIC#attribution_summary#{YYYY-MM}
  sk  METRIC#attribution_channel#{YYYY-MM}#{channel}
  sk  METRIC#attribution_entrypoint#{YYYY-MM}#{entry_point_id}

TTL attribute: ttl (FROZEN_CONTRACTS C5, lowercase matches session-events convention).
TTL value: now + 420 days.

Environment variables (FROZEN_CONTRACTS C5):
  ATTRIBUTION_AGGREGATES_TABLE  REQUIRED -- raise ValueError at handler start if unset
  SESSION_EVENTS_TABLE          REQUIRED (from Terraform; no hard default used at runtime)
  ENTRY_POINTS_TABLE            required at runtime; graceful skip if absent (staging soak)
  DUB_SECRET_NAME               required at runtime; graceful skip if absent (staging soak)
  TENANT_CONFIG_BUCKET          required for timezone; schema-tolerant (missing -> DEFAULT_TZ)
  ENVIRONMENT                   informational (default: staging)
"""

import json
import os
import logging
import re
import time
import urllib.request
import urllib.parse
import urllib.error
from collections import defaultdict
from datetime import datetime, timedelta, date, timezone
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Environment -- read at module load; ATTRIBUTION_AGGREGATES_TABLE validated in handler
# ---------------------------------------------------------------------------
ATTRIBUTION_AGGREGATES_TABLE = os.environ.get('ATTRIBUTION_AGGREGATES_TABLE', '')
SESSION_EVENTS_TABLE = os.environ.get('SESSION_EVENTS_TABLE', '')
ENTRY_POINTS_TABLE = os.environ.get('ENTRY_POINTS_TABLE', '')   # required at runtime
DUB_SECRET_NAME = os.environ.get('DUB_SECRET_NAME', '')         # required at runtime
TENANT_CONFIG_BUCKET = os.environ.get('TENANT_CONFIG_BUCKET', '')  # schema-tolerant fallback
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')

# ---------------------------------------------------------------------------
# C7 constants -- FROZEN_CONTRACTS C7; cite this section in comments below
# ---------------------------------------------------------------------------
# C7 -- single definition point for timezone default (PROVISIONAL per C7 note)
DEFAULT_TZ = 'America/Chicago'  # FROZEN_CONTRACTS C7
# C7 -- engaged threshold
ENGAGED_MIN_USER_MESSAGES = 2   # FROZEN_CONTRACTS C7
ENGAGED_EVENT_TYPES = {'CTA_CLICKED', 'LINK_CLICKED', 'FORM_VIEWED'}  # FROZEN_CONTRACTS C7
# C7 -- staff-hours idle cap (5 min per FROZEN_CONTRACTS C7)
IDLE_CAP_SECONDS = 5 * 60       # FROZEN_CONTRACTS C7
MIN_CONVERSATION_SECONDS = 60   # 1 min floor per conversation -- FROZEN_CONTRACTS C7
# C7 -- reach sessionization window
PAGE_VIEW_SESSION_WINDOW_SECONDS = 30 * 60  # 30-minute windows -- FROZEN_CONTRACTS C7
# C7 -- work-weeks divisor
WORK_WEEK_HOURS = 40            # FROZEN_CONTRACTS C7
# C7 -- after-hours definition
AFTER_HOURS_DAYS = {5, 6}       # Sat=5, Sun=6 (Python weekday) -- FROZEN_CONTRACTS C7
AFTER_HOURS_START = 9           # 09:00 -- FROZEN_CONTRACTS C7
AFTER_HOURS_END = 17            # 17:00 -- FROZEN_CONTRACTS C7

# C5 -- attribution aggregate TTL: 420 days
ATTRIBUTION_TTL_DAYS = 420

# C5 -- resource_clicks top-N cap
RESOURCE_CLICKS_TOP_N = 20

# C2 -- entry_point_id valid form
EP_ID_PATTERN = re.compile(r'^ep_[0-9A-Za-z]{8,64}$')  # FROZEN_CONTRACTS C2

# C4 -- Dub rate limits
DUB_ANALYTICS_RPS = 2
DUB_BASE_URL = 'https://api.dub.co'

# ---------------------------------------------------------------------------
# AWS clients
# ---------------------------------------------------------------------------
_dynamodb_client = boto3.client('dynamodb')
_dynamodb_resource = boto3.resource('dynamodb')
_s3_client = boto3.client('s3')
_secrets_client = boto3.client('secretsmanager')

# ---------------------------------------------------------------------------
# In-process caches (warm-invocation reuse)
# ---------------------------------------------------------------------------
_tenant_config_cache: Dict[str, Dict] = {}
_entry_point_cache: Dict[str, Dict] = {}  # key: f"{tenant_id}:{entry_point_id}"
_dub_secret_cache: Optional[str] = None


# ---------------------------------------------------------------------------
# C5 -- verbatim copy of categorize_question from Analytics_Dashboard_API
# lambda_function.py:5374 -- do NOT import across lambda dirs (per workstream
# instructions); do NOT edit Analytics_Dashboard_API.  Source cited here so a
# future dedup can find both copies.
# ---------------------------------------------------------------------------
def categorize_question(question: str) -> str:
    """
    Source: Analytics_Dashboard_API/lambda_function.py:5374 (verbatim copy).
    FROZEN_CONTRACTS C5 -- topics v1, 6 categories.
    """
    q = (question or '').lower()
    if 'volunteer' in q:
        return 'Volunteer'
    if 'donate' in q or 'donation' in q:
        return 'Donation'
    if 'event' in q or 'gathering' in q:
        return 'Events'
    if 'service' in q or 'help' in q:
        return 'Services'
    if 'supplies' in q or 'request' in q:
        return 'Supplies'
    return 'General'


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------
def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    EventBridge hourly trigger.  Optionally: {"tenant_id": "..."} for a
    single-tenant re-run (uses the decoded tenant_id; function derives the
    tenant_hash from the mappings bucket for GSI queries).
    C8.10: log IDs and counts only -- never full event payloads at INFO.

    FROZEN_CONTRACTS C5: ATTRIBUTION_AGGREGATES_TABLE MUST be set.
    Raises ValueError immediately if unset (fails fast before any DDB I/O).
    """
    if not ATTRIBUTION_AGGREGATES_TABLE:
        raise ValueError(
            'ATTRIBUTION_AGGREGATES_TABLE env var is required but not set. '
            'This Lambda writes to the NEW picasso-attribution-aggregates table '
            'only -- it must NEVER default to picasso-dashboard-aggregates. '
            'Configure the env var via Terraform (infra/modules/lambda-attribution-aggregator-staging).'
        )

    logger.info(
        'Attribution Aggregator started: target=%s',
        event.get('tenant_id', 'ALL'),
    )

    specific_tenant = event.get('tenant_id')
    if specific_tenant:
        # Single-tenant re-run: look up the hash for GSI query
        tenant_hash = _get_tenant_hash_for_id(specific_tenant)
        tenant_pairs = [(tenant_hash, specific_tenant)] if tenant_hash else []
    else:
        tenant_pairs = _get_active_tenant_pairs()

    logger.info('Processing %d tenants', len(tenant_pairs))

    results = []
    for tenant_hash, tenant_id in tenant_pairs:
        try:
            metrics_updated = _aggregate_attribution(tenant_hash, tenant_id)
            results.append({'tenant_id': tenant_id, 'status': 'success', 'metrics_updated': metrics_updated})
        except Exception as exc:
            logger.error('Attribution aggregation failed for tenant %s: %s', tenant_id[:8], exc)
            results.append({'tenant_id': tenant_id, 'status': 'error', 'error': str(exc)})

    summary = {
        'total_tenants': len(tenant_pairs),
        'successful': sum(1 for r in results if r['status'] == 'success'),
        'failed': sum(1 for r in results if r['status'] == 'error'),
    }
    logger.info('Attribution aggregation complete: %s/%s successful', summary['successful'], summary['total_tenants'])
    return summary


# ---------------------------------------------------------------------------
# Core: aggregate one tenant for the current (and optionally prior) month
# ---------------------------------------------------------------------------
def _aggregate_attribution(tenant_hash: str, tenant_id: str) -> int:
    """Return count of aggregate rows written."""
    config = _get_tenant_config(tenant_id)
    tz_name = config.get('timezone', DEFAULT_TZ)  # C7 schema-tolerant
    try:
        tz = ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, KeyError):
        logger.warning('Unknown timezone %s for tenant %s, falling back to %s', tz_name, tenant_id[:8], DEFAULT_TZ)
        tz = ZoneInfo(DEFAULT_TZ)

    now_local = datetime.now(tz)
    current_month = now_local.date().replace(day=1)

    months_to_compute = [current_month]
    # C5: recompute prior month on first 3 days of a new month
    if now_local.day <= 3:
        prior_month = (current_month - timedelta(days=1)).replace(day=1)
        months_to_compute.append(prior_month)

    metrics_written = 0
    for month_start in months_to_compute:
        metrics_written += _compute_month(tenant_hash, tenant_id, month_start, tz, config)

    return metrics_written


def _compute_month(
    tenant_hash: str,
    tenant_id: str,
    month_start: date,
    tz: ZoneInfo,
    config: Dict,
) -> int:
    """Compute and store all attribution aggregate rows for one calendar month."""
    # Month boundary in ISO8601 (tenant-local) -- C7 month definition
    month_end = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1)  # first of next month
    start_iso = datetime(month_start.year, month_start.month, 1, 0, 0, 0, tzinfo=tz).isoformat()
    end_iso = datetime(month_end.year, month_end.month, 1, 0, 0, 0, tzinfo=tz).isoformat()
    month_str = month_start.strftime('%Y-%m')

    logger.info('Computing attribution month=%s for tenant=%s', month_str, tenant_id[:8])

    # Fetch all session events for this tenant x month window.
    # GSI tenant-date-index: hash_key=tenant_hash, range_key=timestamp
    # (confirmed: infra/modules/ddb-session-events-staging/main.tf)
    events = _fetch_session_events(tenant_hash, start_iso, end_iso)
    logger.info('Fetched %d events for tenant=%s month=%s', len(events), tenant_id[:8], month_str)

    # Group events by session_id
    by_session: Dict[str, List[Dict]] = defaultdict(list)
    for ev in events:
        sid = ev.get('session_id', '')
        by_session[sid].append(ev)

    # Separate PAGE_VIEW sessions from conversation sessions
    page_view_sessions: List[List[Dict]] = []
    conversation_sessions: Dict[str, List[Dict]] = {}
    for sid, evs in by_session.items():
        if sid.startswith('pv_'):
            page_view_sessions.append(evs)
        else:
            conversation_sessions[sid] = evs

    # Compute per-session metrics for conversation sessions
    session_metrics = {
        sid: _compute_session_metrics(sid, evs, tz)
        for sid, evs in conversation_sessions.items()
    }

    # PAGE_VIEW reach sessionization (C7)
    reach_pv_sessions = _count_page_view_sessions(page_view_sessions)

    # C2 provenance resolution: mutates channel/campaign/placement on each session
    session_metrics = _resolve_and_enrich_sessions(tenant_id, session_metrics)

    # Poll Dub for entry-point reach data
    dub_data = _poll_dub_for_month(tenant_id, month_start, month_end, tz)

    # Build per-entry-point and per-channel structures
    by_channel: Dict[str, List[str]] = defaultdict(list)   # channel -> [session_ids]
    by_ep: Dict[str, List[str]] = defaultdict(list)        # entry_point_id -> [session_ids]

    for sid, sm in session_metrics.items():
        channel = sm['channel']
        ep_id = sm['entry_point_id']
        by_channel[channel].append(sid)
        if ep_id:
            by_ep[ep_id].append(sid)
        # Sessions without ep_id are counted under their channel only

    # Aggregate summary row
    all_session_ids = list(session_metrics.keys())
    summary_data = _build_summary(all_session_ids, session_metrics, reach_pv_sessions)

    # Aggregate per-channel rows
    channel_rows = {}
    for channel, sids in by_channel.items():
        channel_rows[channel] = _build_channel(channel, sids, session_metrics, dub_data)

    # Aggregate per-entry-point rows
    ep_rows = {}
    for ep_id, sids in by_ep.items():
        ep_rows[ep_id] = _build_entrypoint(ep_id, tenant_id, sids, session_metrics, dub_data)

    # Write to DynamoDB (ATTRIBUTION_AGGREGATES_TABLE -- validated at handler start)
    ttl = _attribution_ttl()
    written = 0

    _store_metric(tenant_id, f'attribution_summary#{month_str}', summary_data, ttl)
    written += 1

    for channel, data in channel_rows.items():
        _store_metric(tenant_id, f'attribution_channel#{month_str}#{channel}', data, ttl)
        written += 1

    for ep_id, data in ep_rows.items():
        _store_metric(tenant_id, f'attribution_entrypoint#{month_str}#{ep_id}', data, ttl)
        written += 1

    return written


# ---------------------------------------------------------------------------
# Per-session metrics computation
# ---------------------------------------------------------------------------
def _compute_session_metrics(sid: str, events: List[Dict], tz: ZoneInfo) -> Dict:
    """
    Compute all per-session metrics needed for C5 aggregation.
    C7 definitions implemented exactly; cite section in per-field comments.
    FROZEN_CONTRACTS C7.
    """
    # Sort by step_number for chronological ordering
    events_sorted = sorted(events, key=lambda e: int(e.get('step_number', 0)))

    user_message_count = 0
    has_engaged_event = False
    has_application = False
    has_lead = False
    is_after_hours = False
    first_user_message_ts: Optional[datetime] = None
    message_timestamps: List[datetime] = []
    topic_counts: Dict[str, int] = defaultdict(int)
    resource_clicks: Dict[str, int] = defaultdict(int)
    attribution: Optional[Dict] = None
    entry_point_id: Optional[str] = None
    channel = 'website'     # default per C2 step 3
    campaign: Optional[str] = None
    placement: Optional[str] = None

    for ev in events_sorted:
        event_type = ev.get('event_type', '')
        payload_raw = ev.get('event_payload', '{}')
        try:
            payload = json.loads(payload_raw) if isinstance(payload_raw, str) else (payload_raw or {})
        except (json.JSONDecodeError, TypeError):
            payload = {}

        ts_str = ev.get('timestamp') or ev.get('client_timestamp')
        ts: Optional[datetime] = None
        if ts_str:
            try:
                ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00')).astimezone(tz)
            except (ValueError, TypeError):
                pass

        # C1.1 / C7 -- CONVERSATION_STARTED carries attribution + entry_point_id
        if event_type == 'CONVERSATION_STARTED':
            ep_raw = ev.get('entry_point_id') or payload.get('entry_point_id')
            if ep_raw and EP_ID_PATTERN.match(str(ep_raw)):
                entry_point_id = str(ep_raw)
            attr_blob = ev.get('attribution')
            if attr_blob:
                try:
                    attribution = (
                        json.loads(attr_blob) if isinstance(attr_blob, str) else attr_blob
                    )
                except (json.JSONDecodeError, TypeError):
                    attribution = {}

        # C7 -- user message tracking (proxy: MESSAGE_SENT by user)
        if event_type in ('MESSAGE_SENT', 'USER_MESSAGE'):
            user_message_count += 1
            if ts:
                if first_user_message_ts is None:
                    first_user_message_ts = ts
                message_timestamps.append(ts)

            # C5 topics: categorize first user message
            text = payload.get('text', '') or payload.get('message', '') or ''
            if text:
                topic = categorize_question(text)
                topic_counts[topic] += 1

        # C7 -- engaged events
        if event_type in ENGAGED_EVENT_TYPES:
            has_engaged_event = True

        # C7 -- application started / lead delivered
        if event_type == 'FORM_STARTED':
            has_application = True
        if event_type == 'FORM_COMPLETED':
            has_lead = True

        # C1.2 -- LINK_CLICKED resource tracking (C5 resource_clicks)
        if event_type == 'LINK_CLICKED':
            has_engaged_event = True
            url = payload.get('url', '')
            if url:
                resource_clicks[url] += 1

    # C7 -- after-hours: first user message outside Mon-Fri 09:00-17:00 tenant-local
    if first_user_message_ts:
        is_after_hours = _is_after_hours(first_user_message_ts)

    # C7 -- engaged: >=1 engaged event OR >=2 user messages
    is_engaged = has_engaged_event or user_message_count >= ENGAGED_MIN_USER_MESSAGES

    # C7 -- staff-hours: sum consecutive message gaps, cap each at 5 min, floor 1 min
    active_seconds = _compute_active_seconds(message_timestamps)

    # C2 -- provenance resolution (registry lookup deferred to channel/ep aggregation)
    # Store raw data here; _resolve_provenance called at build time
    utm_campaign = None
    if attribution:
        utm_campaign = attribution.get('utm_campaign')
        if utm_campaign:
            utm_campaign = str(utm_campaign)[:128]  # C2 truncation

    return {
        'session_id': sid,
        'is_engaged': is_engaged,
        'has_application': has_application,
        'has_lead': has_lead,
        'is_after_hours': is_after_hours,
        'active_seconds': active_seconds,
        'user_message_count': user_message_count,
        'entry_point_id': entry_point_id,
        'utm_campaign': utm_campaign,
        'attribution': attribution,
        'topic_counts': dict(topic_counts),
        'resource_clicks': dict(resource_clicks),
        # channel/campaign/placement resolved below at aggregate build time
        'channel': 'website',  # placeholder; resolved in _resolve_and_enrich
        'campaign': utm_campaign,
        'placement': None,
    }


def _resolve_and_enrich_sessions(tenant_id: str, session_metrics: Dict[str, Dict]) -> Dict[str, Dict]:
    """
    C2 provenance resolution for all sessions.
    Mutates channel/campaign/placement in-place.
    """
    for sid, sm in session_metrics.items():
        ep_id = sm.get('entry_point_id')
        if ep_id and EP_ID_PATTERN.match(ep_id):
            registry_rec = _get_entry_point(tenant_id, ep_id)
            if registry_rec:
                sm['channel'] = registry_rec.get('channel', 'website')
                sm['campaign'] = registry_rec.get('campaign', sm.get('campaign'))
                sm['placement'] = registry_rec.get('placement')
            else:
                # C2: unresolvable ep_ -> website + warning metric
                logger.warning(
                    'Unresolvable entry_point_id=%s tenant=%s session=%s -- defaulting to website',
                    ep_id[:16], tenant_id[:8], sid[:16],
                )
                sm['channel'] = 'website'
        elif sid.startswith('meta:'):
            # C2: meta: prefix -> messenger
            sm['channel'] = 'messenger'
        # else: website (already default)
    return session_metrics


# ---------------------------------------------------------------------------
# C5 -- build aggregate row payloads
# ---------------------------------------------------------------------------
def _build_summary(
    session_ids: List[str],
    session_metrics: Dict[str, Dict],
    reach_pv_sessions: int,
) -> Dict:
    """Build attribution_summary row -- FROZEN_CONTRACTS C5."""
    conversations = len(session_ids)
    engaged = sum(1 for sid in session_ids if session_metrics[sid]['is_engaged'])
    applications = sum(1 for sid in session_ids if session_metrics[sid]['has_application'])
    leads = sum(1 for sid in session_ids if session_metrics[sid]['has_lead'])
    after_hours = sum(1 for sid in session_ids if session_metrics[sid]['is_after_hours'])
    total_active_s = sum(session_metrics[sid]['active_seconds'] for sid in session_ids)

    return {
        'conversations': conversations,
        'engaged': engaged,
        'applications': applications,
        'leads': leads,
        'after_hours_conversations': after_hours,
        'conversation_minutes': int(total_active_s // 60),
        'reach_page_views_sessions': reach_pv_sessions,
        'self_booked_pct': None,           # nullable v1 -- no scheduling source yet
        'median_first_response_minutes': None,  # nullable v1
    }


def _build_channel(
    channel: str,
    session_ids: List[str],
    session_metrics: Dict[str, Dict],
    dub_data: Dict,
) -> Dict:
    """Build attribution_channel row -- FROZEN_CONTRACTS C5."""
    conversations = len(session_ids)
    engaged = sum(1 for sid in session_ids if session_metrics[sid]['is_engaged'])
    applications = sum(1 for sid in session_ids if session_metrics[sid]['has_application'])
    leads = sum(1 for sid in session_ids if session_metrics[sid]['has_lead'])
    after_hours = sum(1 for sid in session_ids if session_metrics[sid]['is_after_hours'])
    total_active_s = sum(session_metrics[sid]['active_seconds'] for sid in session_ids)

    # Aggregate topic_counts across sessions
    merged_topics: Dict[str, int] = defaultdict(int)
    for sid in session_ids:
        for topic, cnt in session_metrics[sid].get('topic_counts', {}).items():
            merged_topics[topic] += cnt

    # Aggregate resource_clicks, capped at top-20 (C5)
    merged_clicks: Dict[str, int] = defaultdict(int)
    for sid in session_ids:
        for url, cnt in session_metrics[sid].get('resource_clicks', {}).items():
            merged_clicks[url] += cnt
    top_clicks = dict(
        sorted(merged_clicks.items(), key=lambda x: x[1], reverse=True)[:RESOURCE_CLICKS_TOP_N]
    )

    # C5 reach for channel: website = pv sessions (already in summary);
    # minted = Dub scans+clicks aggregated across all entry_points in this channel
    if channel == 'website':
        reach = None  # website reach is in summary row (page_view_sessions)
    else:
        # Sum dub scans/clicks across all eps in this channel
        ep_ids_in_channel = list({
            sm['entry_point_id']
            for sm in (session_metrics[sid] for sid in session_ids)
            if sm.get('entry_point_id')
        })
        scans = sum(dub_data.get(ep_id, {}).get('scans', 0) for ep_id in ep_ids_in_channel)
        clicks = sum(dub_data.get(ep_id, {}).get('clicks', 0) for ep_id in ep_ids_in_channel)
        reach = {'scans': scans, 'clicks': clicks}

    row = {
        'conversations': conversations,
        'engaged': engaged,
        'applications': applications,
        'leads': leads,
        'after_hours_conversations': after_hours,
        'conversation_minutes': int(total_active_s // 60),
        'self_booked_pct': None,
        'median_first_response_minutes': None,
        'topic_counts': dict(merged_topics),
        'resource_clicks': top_clicks,
    }
    if reach is not None:
        row['reach'] = reach
    return row


def _build_entrypoint(
    ep_id: str,
    tenant_id: str,
    session_ids: List[str],
    session_metrics: Dict[str, Dict],
    dub_data: Dict,
) -> Dict:
    """Build attribution_entrypoint row -- FROZEN_CONTRACTS C5."""
    conversations = len(session_ids)
    engaged = sum(1 for sid in session_ids if session_metrics[sid]['is_engaged'])
    applications = sum(1 for sid in session_ids if session_metrics[sid]['has_application'])
    leads = sum(1 for sid in session_ids if session_metrics[sid]['has_lead'])
    after_hours = sum(1 for sid in session_ids if session_metrics[sid]['is_after_hours'])
    total_active_s = sum(session_metrics[sid]['active_seconds'] for sid in session_ids)

    # Denormalized label/campaign/placement from registry snapshot (C5 -- contract-sanctioned exception)
    registry_rec = _get_entry_point(tenant_id, ep_id) or {}
    label = registry_rec.get('label', '')
    campaign = registry_rec.get('campaign', '')
    placement = registry_rec.get('placement', '')

    dub_ep = dub_data.get(ep_id, {})

    return {
        'conversations': conversations,
        'engaged': engaged,
        'applications': applications,
        'leads': leads,
        'after_hours_conversations': after_hours,
        'conversation_minutes': int(total_active_s // 60),
        'self_booked_pct': None,
        'median_first_response_minutes': None,
        'dub_scans': dub_ep.get('scans', 0),
        'dub_clicks': dub_ep.get('clicks', 0),
        'label': label,
        'campaign': campaign,
        'placement': placement,
    }


# ---------------------------------------------------------------------------
# C7 helper functions
# ---------------------------------------------------------------------------
def _is_after_hours(ts: datetime) -> bool:
    """
    C7 -- after hours: outside Mon-Fri 09:00-17:00 tenant-local.
    FROZEN_CONTRACTS C7.
    """
    weekday = ts.weekday()  # Mon=0, Sun=6
    if weekday in AFTER_HOURS_DAYS:
        return True
    return not (AFTER_HOURS_START <= ts.hour < AFTER_HOURS_END)


def _compute_active_seconds(timestamps: List[datetime]) -> int:
    """
    C7 -- staff-hours absorbed: sum of consecutive message gaps, each capped at
    IDLE_CAP_SECONDS (5 min), floor MIN_CONVERSATION_SECONDS (1 min) per session.
    FROZEN_CONTRACTS C7.
    """
    if not timestamps:
        return 0
    if len(timestamps) == 1:
        return MIN_CONVERSATION_SECONDS

    total = 0
    for i in range(1, len(timestamps)):
        gap = (timestamps[i] - timestamps[i - 1]).total_seconds()
        total += min(gap, IDLE_CAP_SECONDS)

    return max(int(total), MIN_CONVERSATION_SECONDS)


def _count_page_view_sessions(pv_event_groups: List[List[Dict]]) -> int:
    """
    C7 -- reach sessionization: PAGE_VIEW sessions = 30-min windows keyed on
    ga_client_id where present, else the pv_ session_id.
    Each pv_ session_id already represents one tab-session so count directly,
    but merge sessions sharing ga_client_id within the 30-min window.
    FROZEN_CONTRACTS C7.
    """
    if not pv_event_groups:
        return 0

    # Collect (ga_client_id_or_pv_id, timestamp) tuples
    anchors: List[Tuple[str, float]] = []
    for evs in pv_event_groups:
        ga_id = None
        ts_val: Optional[float] = None
        sid = ''
        for ev in evs:
            if not sid:
                sid = ev.get('session_id', '')
            if not ga_id:
                ga_id = ev.get('ga_client_id')
            ts_str = ev.get('timestamp') or ev.get('client_timestamp')
            if ts_str and ts_val is None:
                try:
                    ts_val = datetime.fromisoformat(
                        ts_str.replace('Z', '+00:00')
                    ).timestamp()
                except (ValueError, TypeError):
                    pass

        key = ga_id if ga_id else sid
        if key and ts_val is not None:
            anchors.append((key, ts_val))

    # Sessionize: group by key, then apply 30-min window
    by_key: Dict[str, List[float]] = defaultdict(list)
    for key, ts in anchors:
        by_key[key].append(ts)

    session_count = 0
    for key, times in by_key.items():
        times.sort()
        session_count += 1  # at least one session per unique key
        window_start = times[0]
        for t in times[1:]:
            if t - window_start > PAGE_VIEW_SESSION_WINDOW_SECONDS:
                session_count += 1
                window_start = t
            # else same session window -- no increment

    return session_count


# ---------------------------------------------------------------------------
# DynamoDB: fetch session events (C5 -- tenant-date-index GSI)
# ---------------------------------------------------------------------------
def _fetch_session_events(tenant_hash: str, start_iso: str, end_iso: str) -> List[Dict]:
    """
    Query picasso-session-events via tenant-date-index GSI.

    GSI definition (infra/modules/ddb-session-events-staging/main.tf):
      hash_key  = tenant_hash  (the original hash, e.g. "fo85e6a06dcdf4")
      range_key = timestamp

    Analytics_Event_Processor writes:
      tenant_hash = the hash (lambda_function.py:387)
      tenant_id   = decoded id (lambda_function.py:388)

    The GSI partition key is tenant_hash -- NOT tenant_id.
    Callers pass tenant_hash (not the decoded tenant_id) to this function.

    Schema-tolerant reads throughout (C2).
    C8.10: log only counts at INFO.
    """
    if not SESSION_EVENTS_TABLE:
        logger.warning('SESSION_EVENTS_TABLE not configured -- returning empty')
        return []

    table = _dynamodb_resource.Table(SESSION_EVENTS_TABLE)
    events = []
    last_key = None

    try:
        from boto3.dynamodb.conditions import Key
        while True:
            kwargs: Dict[str, Any] = {
                'IndexName': 'tenant-date-index',
                'KeyConditionExpression': (
                    Key('tenant_hash').eq(tenant_hash) &
                    Key('timestamp').between(start_iso, end_iso)
                ),
            }
            if last_key:
                kwargs['ExclusiveStartKey'] = last_key

            resp = table.query(**kwargs)
            events.extend(resp.get('Items', []))
            last_key = resp.get('LastEvaluatedKey')
            if not last_key:
                break
    except ClientError as exc:
        logger.error('DynamoDB query failed for tenant_hash=%s: %s', tenant_hash[:8], exc)
        # Graceful: return what we have (may be empty)

    return events


# ---------------------------------------------------------------------------
# DynamoDB: registry lookup (C3)
# ---------------------------------------------------------------------------
def _get_entry_point(tenant_id: str, entry_point_id: str) -> Optional[Dict]:
    """
    Schema-tolerant read of picasso-entry-points table (C3).
    Returns None if ENTRY_POINTS_TABLE not configured (staging soak).
    """
    if not ENTRY_POINTS_TABLE:
        logger.warning('ENTRY_POINTS_TABLE not configured -- skipping provenance lookup')
        return None

    cache_key = f'{tenant_id}:{entry_point_id}'
    if cache_key in _entry_point_cache:
        return _entry_point_cache[cache_key]

    try:
        table = _dynamodb_resource.Table(ENTRY_POINTS_TABLE)
        resp = table.get_item(Key={'tenant_id': tenant_id, 'entry_point_id': entry_point_id})
        rec = resp.get('Item')
        _entry_point_cache[cache_key] = rec  # cache miss (None) also cached
        return rec
    except ClientError as exc:
        logger.error(
            'Registry lookup failed ep=%s tenant=%s: %s',
            entry_point_id[:16], tenant_id[:8], exc,
        )
        return None


# ---------------------------------------------------------------------------
# DynamoDB: write aggregate (C5)
# ---------------------------------------------------------------------------
def _store_metric(tenant_id: str, metric_key: str, data: Dict, ttl: int) -> None:
    """
    Write to ATTRIBUTION_AGGREGATES_TABLE (picasso-attribution-aggregates).
    Physical key attribute names: pk / sk (lowercase -- FROZEN_CONTRACTS C5).
    pk = TENANT#{tenant_id}
    sk = METRIC#{metric_key}
    TTL attribute = 'ttl' (lowercase -- FROZEN_CONTRACTS C5).
    """
    table = _dynamodb_resource.Table(ATTRIBUTION_AGGREGATES_TABLE)
    item = {
        'pk': f'TENANT#{tenant_id}',
        'sk': f'METRIC#{metric_key}',
        'data': _convert_for_dynamo(data),
        'updated_at': datetime.now(timezone.utc).isoformat(),
        'ttl': ttl,
    }
    try:
        table.put_item(Item=item)
        logger.debug('Stored %s for tenant=%s', metric_key, tenant_id[:8])
    except ClientError as exc:
        logger.error('Failed to store metric %s for tenant=%s: %s', metric_key, tenant_id[:8], exc)
        raise


def _convert_for_dynamo(obj: Any) -> Any:
    """Recursively convert float to Decimal for DynamoDB."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    if isinstance(obj, dict):
        return {k: _convert_for_dynamo(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_convert_for_dynamo(v) for v in obj]
    return obj


def _attribution_ttl() -> int:
    """C5 -- TTL = now + 420 days. Attribute name: 'ttl'."""
    return int(
        (datetime.now(timezone.utc) + timedelta(days=ATTRIBUTION_TTL_DAYS)).timestamp()
    )


# ---------------------------------------------------------------------------
# Dub analytics poll (C4)
# ---------------------------------------------------------------------------
def _get_dub_secret() -> Optional[str]:
    """
    Fetch Dub API key from Secrets Manager (C4 / C8 Tier-4 key handling).
    Returns None when secret absent/empty -- callers MUST degrade gracefully.
    Never logged.
    """
    global _dub_secret_cache
    if _dub_secret_cache is not None:
        return _dub_secret_cache or None

    if not DUB_SECRET_NAME:
        logger.warning('DUB_SECRET_NAME not set -- Dub poll skipped')
        _dub_secret_cache = ''
        return None

    try:
        resp = _secrets_client.get_secret_value(SecretId=DUB_SECRET_NAME)
        secret = resp.get('SecretString', '')
        _dub_secret_cache = secret
        return secret or None
    except _secrets_client.exceptions.ResourceNotFoundException:
        logger.warning('Dub secret %s not found -- poll skipped (staging soak)', DUB_SECRET_NAME)
        _dub_secret_cache = ''
        return None
    except Exception as exc:
        logger.warning('Could not fetch Dub secret: %s -- poll skipped', exc)
        _dub_secret_cache = ''
        return None


def _poll_dub_for_month(
    tenant_id: str,
    month_start: date,
    month_end: date,
    tz: ZoneInfo,
) -> Dict[str, Dict]:
    """
    C4 analytics poll: returns {entry_point_id: {scans: N, clicks: N}}.
    Gracefully returns {} when secret absent or any error occurs.
    C4: 2 req/s, honor Retry-After on 429.
    C8.16-17: GET /analytics only; persist counts, country, device_class -- not referers.
    """
    secret = _get_dub_secret()
    if not secret:
        return {}

    start_str = datetime(month_start.year, month_start.month, 1, tzinfo=tz).isoformat()
    end_str = datetime(month_end.year, month_end.month, 1, tzinfo=tz).isoformat()
    tz_name = str(tz)

    results: Dict[str, Dict] = {}

    # One workspace sweep: groupBy=top_links filtered by tenantId
    params = {
        'groupBy': 'top_links',
        'tenantId': tenant_id,
        'timezone': tz_name,
        'start': start_str,
        'end': end_str,
        'interval': 'month',
    }
    try:
        top_links = _dub_get('/analytics', params, secret)
        # top_links is a list of {link: {externalId: ...}, clicks: N, ...}
        for item in (top_links or []):
            link = item.get('link', {}) or {}
            ext_id = link.get('externalId', '')
            # externalId stored without ext_ prefix in registry (C4 note)
            ep_id = ext_id.removeprefix('ext_') if ext_id.startswith('ext_') else ext_id
            if ep_id and EP_ID_PATTERN.match(ep_id):
                results.setdefault(ep_id, {'scans': 0, 'clicks': 0})
                results[ep_id]['clicks'] = item.get('clicks', 0)
    except Exception as exc:
        logger.warning('Dub top_links poll failed for tenant=%s: %s -- zero reach', tenant_id[:8], exc)

    # For known entry points, fetch scan (QR trigger) counts
    for ep_id in list(results.keys()):
        try:
            scan_params = {
                'groupBy': 'triggers',
                'externalId': f'ext_{ep_id}',
                'tenantId': tenant_id,
                'trigger': 'qr',
                'timezone': tz_name,
                'start': start_str,
                'end': end_str,
                'interval': 'month',
            }
            scan_data = _dub_get('/analytics', scan_params, secret)
            # scan_data: [{trigger: 'qr', count: N}, ...]
            for item in (scan_data or []):
                if item.get('trigger') == 'qr':
                    results[ep_id]['scans'] = item.get('count', 0)
            time.sleep(1.0 / DUB_ANALYTICS_RPS)  # C4 rate limit
        except Exception as exc:
            logger.warning('Dub scan poll failed ep=%s: %s', ep_id[:16], exc)

    return results


def _dub_get(path: str, params: Dict, secret: str) -> Any:
    """
    GET {DUB_BASE_URL}{path}?{params}.
    Honors Retry-After on 429 (C4).
    C8.16: only /analytics path permitted.
    C8.17: caller is responsible for filtering persisted output.
    """
    if not path.startswith('/analytics'):
        raise ValueError(f'Forbidden Dub path: {path}')

    query = urllib.parse.urlencode({k: v for k, v in params.items() if v is not None})
    url = f'{DUB_BASE_URL}{path}?{query}'

    req = urllib.request.Request(url, headers={'Authorization': f'Bearer {secret}'})

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except Exception as exc:
        # Check for 429 with Retry-After
        if hasattr(exc, 'code') and exc.code == 429:
            retry_after = int(exc.headers.get('Retry-After', '5'))
            logger.warning('Dub 429 -- sleeping %ss (Retry-After)', retry_after)
            time.sleep(min(retry_after, 60))
            # Single retry
            with urllib.request.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode('utf-8'))
        raise


# ---------------------------------------------------------------------------
# Tenant enumeration + config
#
# The session-events GSI (tenant-date-index) is keyed on tenant_hash.
# The tenant config and aggregate PK use tenant_id (decoded).
# _get_active_tenant_pairs() returns List[(tenant_hash, tenant_id)] so callers
# have both values without a second lookup.
#
# Mapping file per tenant: TENANT_CONFIG_BUCKET/mappings/{tenant_hash}.json
#   -> {"tenant_id": "FOS402334", "tenant_hash": "fo85e6a06dcdf4", ...}
# (same pattern as Analytics_Event_Processor.get_tenant_mapping)
# ---------------------------------------------------------------------------
def _get_active_tenant_pairs() -> List[Tuple[str, str]]:
    """
    Enumerate (tenant_hash, tenant_id) pairs from the config bucket.

    Reads: TENANT_CONFIG_BUCKET/mappings/ prefix (one JSON per tenant hash).
    Source of the GSI partition value: tenant_hash from each mapping file.
    (file:line anchor -- this function is the sole source; tests mock it.)

    Schema-tolerant: missing bucket env -> empty list (warning, no crash).
    C8.10: log counts, not payload details.
    """
    if not TENANT_CONFIG_BUCKET:
        logger.warning('TENANT_CONFIG_BUCKET not configured -- cannot enumerate tenants')
        return []

    pairs: List[Tuple[str, str]] = []
    try:
        paginator = _s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=TENANT_CONFIG_BUCKET, Prefix='mappings/', Delimiter='/'):
            for obj in page.get('Contents', []):
                key = obj.get('Key', '')
                if not key.endswith('.json'):
                    continue
                # key pattern: mappings/{tenant_hash}.json
                tenant_hash = key[len('mappings/'):-len('.json')]
                if len(tenant_hash) < 5:
                    continue
                mapping = _load_tenant_mapping(tenant_hash)
                if mapping:
                    tenant_id = mapping.get('tenant_id', '')
                    if tenant_id:
                        pairs.append((tenant_hash, tenant_id))
    except Exception as exc:
        logger.error('Could not enumerate active tenants: %s', exc)

    logger.info('Enumerated %d active tenants', len(pairs))
    return pairs


def _load_tenant_mapping(tenant_hash: str) -> Optional[Dict]:
    """
    Load a single tenant mapping file from S3.
    Schema-tolerant: missing file -> None (warning, no crash).
    """
    if not TENANT_CONFIG_BUCKET:
        return None
    try:
        resp = _s3_client.get_object(
            Bucket=TENANT_CONFIG_BUCKET,
            Key=f'mappings/{tenant_hash}.json',
        )
        return json.loads(resp['Body'].read().decode('utf-8'))
    except ClientError as exc:
        if exc.response['Error']['Code'] not in ('NoSuchKey', '404'):
            logger.error('Mapping load failed for hash=%s: %s', tenant_hash[:8], exc)
        return None
    except Exception as exc:
        logger.error('Mapping load failed for hash=%s: %s', tenant_hash[:8], exc)
        return None


def _get_tenant_hash_for_id(tenant_id: str) -> Optional[str]:
    """
    Reverse lookup: find tenant_hash for a given tenant_id.
    Used only for single-tenant re-run via handler event payload.
    Scans all mappings (acceptable: single re-run path only, not the hot hourly path).
    """
    for tenant_hash, tid in _get_active_tenant_pairs():
        if tid == tenant_id:
            return tenant_hash
    logger.warning('No tenant_hash found for tenant_id=%s', tenant_id[:8])
    return None


def _get_tenant_config(tenant_id: str) -> Dict:
    """
    Schema-tolerant read of tenant config from TENANT_CONFIG_BUCKET.
    Returns {} when config unavailable -- all readers use .get(key, default).
    Tenant timezone is read schema-tolerantly (C7 -- field may not exist yet).

    Three fallback layers (C5 / C7 requirement):
    1. TENANT_CONFIG_BUCKET env missing -> return {}
    2. Config object missing (NoSuchKey) -> return {}
    3. 'timezone' key absent in config -> caller falls back to DEFAULT_TZ
    """
    if tenant_id in _tenant_config_cache:
        return _tenant_config_cache[tenant_id]

    if not TENANT_CONFIG_BUCKET:
        logger.warning('TENANT_CONFIG_BUCKET not configured -- using defaults for tenant=%s', tenant_id[:8])
        _tenant_config_cache[tenant_id] = {}
        return {}

    config: Dict = {}
    try:
        resp = _s3_client.get_object(
            Bucket=TENANT_CONFIG_BUCKET,
            Key=f'tenants/{tenant_id}/config.json',
        )
        config = json.loads(resp['Body'].read().decode('utf-8'))
    except ClientError as exc:
        if exc.response['Error']['Code'] not in ('NoSuchKey', '404'):
            logger.error('Config fetch failed for tenant=%s: %s', tenant_id[:8], exc)
    except Exception as exc:
        logger.error('Config fetch failed for tenant=%s: %s', tenant_id[:8], exc)

    _tenant_config_cache[tenant_id] = config
    return config
