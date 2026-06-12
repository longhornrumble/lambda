"""
Attribution Recap Generator
===========================
Monthly infographic email for tenants with `dashboard_attribution` enabled.

Trigger: monthly EventBridge scheduled event (integrator Terraform glue).
On invoke: computes the PRIOR calendar month and iterates tenants.

Design spec: docs/roadmap/attribution-mockups/attribution-monthly-infographic-mockup.html
Locked decisions: docs/roadmap/ATTRIBUTION_SURFACE_INFOGRAPHIC.md (anatomy, I1/I2/I3)
Aggregate schema: FROZEN_CONTRACTS §C5 (pk/sk/data/ttl)
Definitions: FROZEN_CONTRACTS §C7 (staff_hours, work_weeks, after_hours, floors)
PII constraints: FROZEN_CONTRACTS §C8 (C8.10: IDs + counts only at INFO)
send_email contract: Lambdas/lambda/send_email/lambda_function.py (body key = JSON string)

CAN-SPAM compliance (WS-I conditions):
  Condition 1 - Postal address: RECAP_POSTAL_ADDRESS env rendered into footer HTML + text.
                When empty/unset and RECAP_SEND_ENABLED=true -> send BLOCKED (error log
                recap-blocked-no-postal-address, no send_email invoked).
                Dry-run renders [POSTAL ADDRESS NOT CONFIGURED] placeholder.
  Condition 2 - Suppression store: pk=TENANT#{id}, sk=SUPPRESS#recap#{email_lower},
                created_at (ISO), source. NO TTL (permanent). Filter applied before
                every send; suppression wins over tenant config every month.
  Condition 3 - Tokenized unsubscribe link: {UNSUBSCRIBE_BASE_URL}?t={token} per-recipient.
                Token = base64url(payload) + "." + base64url(hmac_sha256(key, payload)).
                Payload = "{tenant_id}|{email_lower}|recap". Signing key from Secrets Manager
                (UNSUB_SECRET_NAME). FAIL-CLOSED when secret absent or UNSUBSCRIBE_BASE_URL
                unset while RECAP_SEND_ENABLED=true.

Environment variables:
  ATTRIBUTION_AGGREGATES_TABLE  DynamoDB table for aggregates (C5) + suppression rows
  TENANT_CONFIG_BUCKET          S3 bucket for tenant configs + mappings
  SEND_EMAIL_FUNCTION_NAME      Lambda function name for send_email
  DASHBOARD_BASE_URL            Base URL for the "Read your full briefing" CTA
  RECAP_SEND_ENABLED            'true' to actually send; anything else = dry-run (default)
  RECAP_POSTAL_ADDRESS          Physical postal address for CAN-SPAM footer (condition 1)
  UNSUB_SECRET_NAME             Secrets Manager secret name holding the HMAC signing key
  UNSUBSCRIBE_BASE_URL          Base URL for the unsubscribe endpoint (no trailing slash)
"""

import base64
import hashlib
import hmac
import json
import os
import logging
import re
from calendar import month_name
from datetime import datetime, timedelta, timezone, date
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
ATTRIBUTION_AGGREGATES_TABLE = os.environ.get('ATTRIBUTION_AGGREGATES_TABLE', '')
TENANT_CONFIG_BUCKET = os.environ.get('TENANT_CONFIG_BUCKET', '')
SEND_EMAIL_FUNCTION_NAME = os.environ.get('SEND_EMAIL_FUNCTION_NAME', '')
DASHBOARD_BASE_URL = os.environ.get('DASHBOARD_BASE_URL', 'https://app.myrecruiter.ai')
RECAP_SEND_ENABLED = os.environ.get('RECAP_SEND_ENABLED', 'false')
RECAP_POSTAL_ADDRESS = os.environ.get('RECAP_POSTAL_ADDRESS', '')
UNSUB_SECRET_NAME = os.environ.get('UNSUB_SECRET_NAME', '')
UNSUBSCRIBE_BASE_URL = os.environ.get('UNSUBSCRIBE_BASE_URL', '')

# ---------------------------------------------------------------------------
# C7 constants (FROZEN_CONTRACTS C7)
# ---------------------------------------------------------------------------
DEFAULT_TZ = 'America/Chicago'   # FROZEN_CONTRACTS C7 PROVISIONAL
WORK_WEEK_HOURS = 40              # FROZEN_CONTRACTS C7
# C7 confidence floor: n >= 50 before any rate/channel comparison
CONFIDENCE_FLOOR = 50             # FROZEN_CONTRACTS C7
# Small-tenant floor: below this many conversations -> small variant
SMALL_TENANT_FLOOR = 50           # C7 floor; same as confidence floor
# Top-N for topic bars in email (design spec: 3)
TOPICS_TOP_N = 3

# ---------------------------------------------------------------------------
# Idempotency / TTL
# ---------------------------------------------------------------------------
RECAP_SENT_TTL_DAYS = 420         # match C5 aggregate TTL

# ---------------------------------------------------------------------------
# AWS clients (module-level; re-used across warm invocations)
# ---------------------------------------------------------------------------
_dynamodb = boto3.resource('dynamodb')
_s3 = boto3.client('s3')
_lambda_client = boto3.client('lambda')
_secretsmanager = boto3.client('secretsmanager')

# ---------------------------------------------------------------------------
# In-process caches
# ---------------------------------------------------------------------------
_tenant_config_cache: Dict[str, Dict] = {}

# UNSUB signing key cache: None = not yet fetched. False is never stored --
# transient failures must not be cached; we retry on next invocation.
_unsub_signing_key: Optional[bytes] = None


# ---------------------------------------------------------------------------
# HMAC signing key (Secrets Manager) -- condition 3
# ---------------------------------------------------------------------------

def _get_unsub_signing_key() -> Optional[bytes]:
    """
    Fetch the HMAC signing key from Secrets Manager.
    Returns bytes on success, None on any error.
    Successful fetches are cached for the lifetime of the container.
    Transient failures are NOT cached -- we retry on next invocation.
    SECURITY: key value is never logged.
    """
    global _unsub_signing_key
    if _unsub_signing_key is not None:
        return _unsub_signing_key

    if not UNSUB_SECRET_NAME:
        logger.error('UNSUB_SECRET_NAME env var is not set -- cannot fetch signing key')
        return None

    try:
        resp = _secretsmanager.get_secret_value(SecretId=UNSUB_SECRET_NAME)
    except ClientError as exc:
        # Do NOT cache the failure -- next invocation will retry.
        logger.error(
            'Failed to fetch unsub signing key from Secrets Manager secret=%s: %s',
            UNSUB_SECRET_NAME, exc.response['Error']['Code'],
        )
        return None
    except Exception as exc:
        logger.error('Unexpected error fetching unsub signing key: %s', type(exc).__name__)
        return None

    raw = resp.get('SecretString') or ''
    if not raw:
        logger.error('Unsub signing key secret is empty (secret=%s)', UNSUB_SECRET_NAME)
        return None

    key_bytes = raw.strip().encode('utf-8')
    _unsub_signing_key = key_bytes
    logger.info('Unsub signing key loaded (secret=%s)', UNSUB_SECRET_NAME)
    return _unsub_signing_key


# ---------------------------------------------------------------------------
# Token helpers (condition 3)
# Token format: base64url(payload) + "." + base64url(hmac_sha256(key, payload))
# payload = "{tenant_id}|{email_lower}|recap"  (LOCKED -- must match Unsubscribe Lambda)
# ---------------------------------------------------------------------------

def _b64url_nopad(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode('ascii')


def _build_unsub_token(tenant_id: str, email: str, key: bytes) -> str:
    """Build a per-recipient HMAC token. NEVER log this value."""
    payload = f'{tenant_id}|{email.lower()}|recap'
    payload_bytes = payload.encode('utf-8')
    sig = hmac.new(key, payload_bytes, hashlib.sha256).digest()
    return _b64url_nopad(payload_bytes) + '.' + _b64url_nopad(sig)


# ---------------------------------------------------------------------------
# Suppression helpers (condition 2)
# Row schema: pk=TENANT#{tenant_id}, sk=SUPPRESS#recap#{email_lower}
#             created_at (ISO), source ("unsubscribe_link"|"manual")
#             NO TTL -- permanent.
# ---------------------------------------------------------------------------

def _suppression_sk(email: str) -> str:
    return f'SUPPRESS#recap#{email.lower()}'


def _fetch_suppressed_emails(tenant_id: str) -> set:
    """
    Query all SUPPRESS#recap# rows for a tenant.
    Returns a set of lowercased email strings.
    Returns empty set on any error.
    """
    if not ATTRIBUTION_AGGREGATES_TABLE:
        return set()

    try:
        from boto3.dynamodb.conditions import Key
        table = _dynamodb.Table(ATTRIBUTION_AGGREGATES_TABLE)
        resp = table.query(
            KeyConditionExpression=(
                Key('pk').eq(f'TENANT#{tenant_id}') &
                Key('sk').begins_with('SUPPRESS#recap#')
            )
        )
        suppressed = set()
        prefix = 'SUPPRESS#recap#'
        for item in resp.get('Items', []):
            sk = item.get('sk', '')
            if sk.startswith(prefix):
                suppressed.add(sk[len(prefix):])
        return suppressed
    except ClientError as exc:
        logger.error(
            'Suppression query failed for tenant=%s: %s',
            tenant_id[:8], exc.response['Error']['Code'],
        )
        return set()
    except Exception as exc:
        logger.error('Unexpected error querying suppressions for tenant=%s: %s', tenant_id[:8], type(exc).__name__)
        return set()


def _filter_suppressed(recipients: List[str], suppressed: set) -> List[str]:
    """Filter out suppressed emails (case-insensitive)."""
    return [r for r in recipients if r.lower() not in suppressed]


# ---------------------------------------------------------------------------
# Lambda handler
# ---------------------------------------------------------------------------
def lambda_handler(event: Dict, context: Any) -> Dict:
    """
    EventBridge monthly trigger.
    Iterates all tenants; generates + sends (or dry-runs) recap emails.
    C8.10: log IDs and counts only at INFO -- never email bodies or recipients.
    """
    if not ATTRIBUTION_AGGREGATES_TABLE:
        raise ValueError(
            'ATTRIBUTION_AGGREGATES_TABLE env var is required but not set. '
            'This Lambda reads from picasso-attribution-aggregates. '
            'Configure via Terraform (integrator glue).'
        )

    dry_run = RECAP_SEND_ENABLED.strip().lower() != 'true'
    if dry_run:
        logger.info(
            'Attribution Recap Generator running in DRY-RUN mode '
            '(set RECAP_SEND_ENABLED=true to enable sending). '
            'First real send is gated on communications-consent advisory + operator enablement.'
        )

    now_utc = datetime.now(timezone.utc)
    first_of_current = now_utc.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    last_of_prior = first_of_current - timedelta(days=1)
    prior_month_str = last_of_prior.strftime('%Y-%m')

    logger.info(
        'Attribution Recap Generator started: prior_month=%s dry_run=%s',
        prior_month_str, dry_run,
    )

    tenant_pairs = _get_active_tenant_pairs()
    logger.info('Enumerated %d total tenants', len(tenant_pairs))

    results = []
    for tenant_hash, tenant_id in tenant_pairs:  # noqa: B007
        try:
            outcome = _process_tenant(tenant_id, prior_month_str, dry_run)
            results.append({'tenant_id': tenant_id, 'outcome': outcome})
        except Exception as exc:
            logger.error(
                'Recap generation failed for tenant=%s: %s',
                tenant_id[:8], exc,
            )
            results.append({'tenant_id': tenant_id, 'outcome': 'error', 'error': str(exc)})

    sent_count = sum(1 for r in results if r['outcome'] == 'sent')
    dry_run_count = sum(1 for r in results if r['outcome'] == 'dry_run')
    skipped_count = sum(1 for r in results if r['outcome'].startswith('skip'))
    error_count = sum(1 for r in results if r['outcome'] == 'error')
    already_sent_count = sum(1 for r in results if r['outcome'] == 'already_sent')

    logger.info(
        'Attribution Recap complete: month=%s total=%d sent=%d dry_run=%d '
        'skipped=%d already_sent=%d errors=%d',
        prior_month_str, len(results), sent_count, dry_run_count,
        skipped_count, already_sent_count, error_count,
    )

    return {
        'month': prior_month_str,
        'total_tenants': len(tenant_pairs),
        'sent': sent_count,
        'dry_run': dry_run_count,
        'skipped': skipped_count,
        'already_sent': already_sent_count,
        'errors': error_count,
        'dry_run_mode': dry_run,
    }


# ---------------------------------------------------------------------------
# Per-tenant processing
# ---------------------------------------------------------------------------
def _process_tenant(tenant_id: str, month_str: str, dry_run: bool) -> str:
    """
    Returns one of: 'skip_flag_off', 'skip_no_recipients',
                    'skip_all_suppressed', 'already_sent',
                    'sent', 'dry_run'.

    CAN-SPAM conditions (fail-closed when RECAP_SEND_ENABLED=true):
      - Postal address (condition 1): blocks send if RECAP_POSTAL_ADDRESS unset.
      - Suppression filter (condition 2): applied before every send; suppressed
        recipients removed; all-suppressed -> skip_all_suppressed.
      - Tokenized unsubscribe link (condition 3): blocks send if signing key
        unavailable or UNSUBSCRIBE_BASE_URL unset.
    """
    config = _get_tenant_config(tenant_id)

    # HARD GATE (locked decision #10): only tenants with dashboard_attribution truthy.
    features = config.get('features', {})
    feature_flags = config.get('feature_flags', {})
    attribution_enabled = (
        features.get('dashboard_attribution', False)
        or feature_flags.get('dashboard_attribution', False)
    )
    if not attribution_enabled:
        return 'skip_flag_off'

    # Recipients: tenant config key attribution_recap.recipients (schema-tolerant).
    recap_config = config.get('attribution_recap', {})
    recipients_raw = recap_config.get('recipients', [])
    if not recipients_raw:
        logger.info(
            'Tenant %s has no recap recipients configured -- skipping (attribution_recap.recipients)',
            tenant_id[:8],
        )
        return 'skip_no_recipients'

    # Idempotency check.
    if _recap_already_sent(tenant_id, month_str):
        logger.info('Recap already sent for tenant=%s month=%s -- skipping', tenant_id[:8], month_str)
        return 'already_sent'

    # Load aggregate data.
    summary_row = _load_aggregate(tenant_id, f'attribution_summary#{month_str}')
    channel_rows = _load_channel_rows(tenant_id, month_str)

    # Variant selection.
    variant = _select_variant(summary_row, month_str, tenant_id)

    # Org display name.
    org_name = (
        config.get('organization_name')
        or config.get('chat_title')
        or tenant_id
    )

    # -----------------------------------------------------------------------
    # CAN-SPAM condition 1: postal address
    # -----------------------------------------------------------------------
    postal_address = RECAP_POSTAL_ADDRESS.strip()
    if not postal_address and not dry_run:
        logger.error(
            'recap-blocked-no-postal-address: tenant=%s month=%s -- '
            'RECAP_POSTAL_ADDRESS env var is not set; blocking send.',
            tenant_id[:8], month_str,
        )
        return 'skip_flag_off'

    # Dry-run uses a visible placeholder so fixtures keep working.
    postal_display = postal_address if postal_address else '[POSTAL ADDRESS NOT CONFIGURED]'

    # -----------------------------------------------------------------------
    # CAN-SPAM condition 3: signing key + base URL (preflight before suppression)
    # -----------------------------------------------------------------------
    signing_key: Optional[bytes] = None
    if not dry_run:
        if not UNSUBSCRIBE_BASE_URL:
            logger.error(
                'recap-blocked-no-unsub-url: tenant=%s month=%s -- '
                'UNSUBSCRIBE_BASE_URL env var is not set; blocking send.',
                tenant_id[:8], month_str,
            )
            return 'skip_flag_off'
        signing_key = _get_unsub_signing_key()
        if signing_key is None:
            logger.error(
                'recap-blocked-unsub-key-unavailable: tenant=%s month=%s -- '
                'could not fetch signing key from Secrets Manager; blocking send.',
                tenant_id[:8], month_str,
            )
            return 'skip_flag_off'

    # -----------------------------------------------------------------------
    # CAN-SPAM condition 2: suppression filter
    # -----------------------------------------------------------------------
    suppressed = _fetch_suppressed_emails(tenant_id)
    recipients = _filter_suppressed(recipients_raw, suppressed)

    if not recipients:
        logger.info(
            'All %d recipient(s) suppressed for tenant=%s month=%s -- skipping',
            len(recipients_raw), tenant_id[:8], month_str,
        )
        return 'skip_all_suppressed'

    if len(recipients) < len(recipients_raw):
        logger.info(
            'Suppression filtered %d of %d recipient(s) for tenant=%s month=%s',
            len(recipients_raw) - len(recipients), len(recipients_raw),
            tenant_id[:8], month_str,
        )

    recipients_count = len(recipients)

    if dry_run:
        # Render once to confirm no crash; placeholder token; no send.
        html = _render_email(
            variant, summary_row, channel_rows, month_str, org_name, tenant_id,
            postal_address=postal_display,
            unsub_url=f'{UNSUBSCRIBE_BASE_URL or "https://placeholder.invalid"}?t=PLACEHOLDER_TOKEN',
        )
        html_bytes = len(html.encode('utf-8'))
        logger.info(
            'Recap rendered (dry-run): tenant=%s month=%s variant=%s '
            'recipients_count=%d html_bytes=%d',
            tenant_id[:8], month_str, variant, recipients_count, html_bytes,
        )
        return 'dry_run'

    # Write idempotency marker BEFORE invoking send_email.
    _mark_recap_sent(tenant_id, month_str)

    # One send_email invoke per recipient, each with its own tokenized unsubscribe link.
    month_label = _month_label(month_str)
    subject = f'Your {month_label} — by the numbers ({org_name})'

    for recipient_email in recipients:
        token = _build_unsub_token(tenant_id, recipient_email, signing_key)
        unsub_url = f'{UNSUBSCRIBE_BASE_URL}?t={token}'

        html = _render_email(
            variant, summary_row, channel_rows, month_str, org_name, tenant_id,
            postal_address=postal_display,
            unsub_url=unsub_url,
        )
        text = _render_text_fallback(
            variant, summary_row, month_str, org_name,
            postal_address=postal_display,
            unsub_url=unsub_url,
        )

        email_payload = {
            'to': [recipient_email],
            'subject': subject,
            'html_body': html,
            'text_body': text,
            'tags': {
                'tenant_id': tenant_id[:50],
                'email_type': 'attribution_recap',
                'month': month_str,
            },
        }
        _invoke_send_email(email_payload, tenant_id, month_str)

    logger.info(
        'Recap rendered+sent: tenant=%s month=%s variant=%s recipients_count=%d',
        tenant_id[:8], month_str, variant, recipients_count,
    )
    return 'sent'


# ---------------------------------------------------------------------------
# Variant selection (pure function; I2 locked)
# ---------------------------------------------------------------------------
def _select_variant(
    summary_row: Dict,
    month_str: str,
    tenant_id: str,
) -> str:
    """
    Returns one of: 'first_month', 'small_tenant', 'bad_month', 'good_month'.
    Pure function: no I/O, no side-effects.
    """
    conversations = int(summary_row.get('conversations', 0))
    leads = int(summary_row.get('leads', 0))
    prior_conversations = summary_row.get('prior_conversations')
    prior_leads = summary_row.get('prior_leads')

    if prior_conversations is None:
        return 'first_month'

    if conversations < SMALL_TENANT_FLOOR:
        return 'small_tenant'

    if prior_leads is not None and leads < int(prior_leads):
        return 'bad_month'

    return 'good_month'


# ---------------------------------------------------------------------------
# Email rendering -- table-based, inline styles, 620px max-width
# ---------------------------------------------------------------------------

_EMERALD_50 = '#ecfdf5'
_EMERALD_100 = '#d1fae5'
_EMERALD_200 = '#a7f3d0'
_EMERALD_400 = '#34d399'
_EMERALD_500 = '#50C878'
_EMERALD_600 = '#059669'
_EMERALD_700 = '#047857'
_EMERALD_800 = '#065f46'
_EMERALD_900 = '#064e3b'
_SLATE_50 = '#f8fafc'
_SLATE_100 = '#f1f5f9'
_SLATE_200 = '#e2e8f0'
_SLATE_400 = '#94a3b8'
_SLATE_500 = '#64748b'
_SLATE_600 = '#475569'
_SLATE_900 = '#0f172a'
_AMBER_400 = '#fbbf24'
_WHITE = '#ffffff'


def _render_email(
    variant: str,
    summary_row: Dict,
    channel_rows: List[Dict],
    month_str: str,
    org_name: str,
    tenant_id: str,
    postal_address: str = '',
    unsub_url: str = '',
) -> str:
    """
    Render the full email HTML string.
    Table-based layout, inline styles, max-width 620px.
    No dollar signs anywhere (locked decision #5 / C7).
    No per-person data (C8 -- aggregates only).
    postal_address: rendered in footer (CAN-SPAM condition 1).
    unsub_url: per-recipient tokenized unsubscribe URL (CAN-SPAM condition 3).
    """
    month_label = _month_label(month_str)

    conversations = int(summary_row.get('conversations') or 0)
    leads = int(summary_row.get('leads') or 0)
    conversation_minutes = int(summary_row.get('conversation_minutes') or 0)
    after_hours = int(summary_row.get('after_hours_conversations') or 0)
    after_hours_pct = round(after_hours / conversations * 100) if conversations > 0 else 0

    staff_hours = conversation_minutes / 60.0
    work_weeks = staff_hours / WORK_WEEK_HOURS

    prior_conv = summary_row.get('prior_conversations')
    prior_leads_val = summary_row.get('prior_leads')
    conv_delta_str = _pct_delta_str(conversations, prior_conv)
    leads_delta_str = _pct_delta_str(leads, prior_leads_val)

    topics = _extract_top_topics(channel_rows)
    mvp_channel = _find_mvp_channel(channel_rows)

    cta_url = f'{DASHBOARD_BASE_URL}/attribution'

    self_booked_pct = summary_row.get('self_booked_pct')
    median_first_response = summary_row.get('median_first_response_minutes')

    body_sections = []
    body_sections.append(_section_brand(month_label))
    body_sections.append(_section_title(variant, month_label, org_name))

    if variant == 'first_month':
        body_sections.append(_section_hero_first(after_hours, conversations, month_label))
    elif variant == 'small_tenant':
        body_sections.append(_section_hero_small(conversations, after_hours))
    else:
        body_sections.append(_section_hero(after_hours, after_hours_pct))

    body_sections.append(_section_big_three(
        variant, conversations, leads, staff_hours, work_weeks,
        conv_delta_str, leads_delta_str,
    ))

    if variant not in ('first_month', 'small_tenant') and mvp_channel:
        body_sections.append(_section_channel_mvp(mvp_channel))

    if topics:
        body_sections.append(_section_topics(topics, variant))

    superlatives = _build_superlatives(self_booked_pct, median_first_response, after_hours)
    if superlatives and variant not in ('small_tenant',):
        body_sections.append(_section_superlatives(superlatives))

    body_sections.append(_section_cta(month_label, cta_url, variant))
    body_sections.append(_section_footer(org_name, DASHBOARD_BASE_URL, postal_address, unsub_url))

    inner_html = '\n'.join(body_sections)
    preheader_text = _preheader_text(variant, month_label, conversations)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Your {month_label}, by the numbers</title>
</head>
<body style="margin:0;padding:0;background-color:{_SLATE_100};font-family:'Helvetica Neue',Helvetica,Arial,sans-serif;">
<!-- preheader -->
<span style="display:none;font-size:1px;color:{_SLATE_100};line-height:1px;max-height:0px;max-width:0px;opacity:0;overflow:hidden;">{preheader_text}</span>
<!-- outer wrapper -->
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:{_SLATE_100};padding:36px 16px 56px;">
<tr><td align="center">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="620" style="max-width:620px;background-color:{_WHITE};border-radius:20px;overflow:hidden;border:1px solid {_SLATE_200};">
{inner_html}
</table>
</td></tr>
</table>
</body>
</html>"""


def _preheader_text(variant: str, month_label: str, conversations: int) -> str:
    if variant == 'first_month':
        return f'Welcome to your first Mission Intelligence recap for {month_label}.'
    if variant == 'small_tenant':
        return f'Your AI team member answered {conversations} conversations in {month_label}.'
    if variant == 'bad_month':
        return f'Your {month_label} recap — every conversation counts.'
    return "Your mission worked nights this month. Here's the proof in one scroll."


# ---------------------------------------------------------------------------
# HTML section builders
# ---------------------------------------------------------------------------

def _section_brand(month_label: str) -> str:
    return f"""<tr><td style="padding:26px 36px 0;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
<tr>
  <td width="34" valign="middle">
    <table role="presentation" cellpadding="0" cellspacing="0" border="0">
    <tr><td style="background-color:{_EMERALD_500};border-radius:8px;width:30px;height:30px;text-align:center;vertical-align:middle;">
      <span style="color:{_WHITE};font-size:14px;font-weight:800;">&#x2197;</span>
    </td></tr>
    </table>
  </td>
  <td valign="middle" style="padding-left:10px;">
    <span style="font-size:12px;font-weight:800;color:{_SLATE_900};">MyRecruiter</span>
  </td>
  <td align="right" valign="middle">
    <span style="font-size:11px;font-weight:600;color:{_SLATE_400};">Monthly recap &middot; {month_label}</span>
  </td>
</tr>
</table>
</td></tr>"""


def _section_title(variant: str, month_label: str, org_name: str) -> str:
    org_safe = _html_escape(org_name)
    if variant == 'first_month':
        subtitle = 'Welcome to Mission Intelligence. Your AI team member was on the clock all month.'
    elif variant == 'bad_month':
        subtitle = "Some months are quieter. Here's what your AI team member still delivered."
    elif variant == 'small_tenant':
        subtitle = f"Every conversation counts. Here's what happened in {month_label}."
    else:
        subtitle = 'While your team was doing the mission, your AI team member was answering the door. Here\'s what happened.'

    return f"""<tr><td style="padding:22px 36px 6px;">
  <div style="font-size:11px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:{_EMERALD_600};">Prepared for {org_safe}</div>
  <h1 style="font-size:30px;font-weight:800;letter-spacing:-.025em;line-height:1.12;margin:6px 0 0;color:{_SLATE_900};">Your {month_label},<br>by the numbers.</h1>
  <p style="font-size:14px;color:{_SLATE_500};margin:8px 0 0;line-height:1.5;">{subtitle}</p>
</td></tr>"""


def _section_hero(after_hours: int, after_hours_pct: int) -> str:
    night_label = f'{after_hours:,}' if after_hours > 0 else '0'
    chip_pct = f'<span style="background:rgba(255,255,255,.12);border:1px solid rgba(255,255,255,.18);border-radius:999px;padding:5px 12px;font-size:11px;font-weight:700;color:{_WHITE};display:inline-block;margin:4px 4px 0 0;"><b style="color:{_EMERALD_200};">{after_hours_pct}%</b> of all engagement</span>'

    return f"""<tr><td style="padding:22px 36px 0;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
<tr><td style="background:linear-gradient(150deg,{_SLATE_900} 0%,{_EMERALD_900} 70%,{_EMERALD_800} 100%);border-radius:18px;padding:30px 28px 26px;">
  <div style="font-size:10px;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:{_EMERALD_200};">The night shift you never had to staff</div>
  <div style="font-size:60px;font-weight:800;letter-spacing:-.04em;line-height:1;margin:10px 0 4px;color:{_WHITE};">{night_label}</div>
  <div style="font-size:16px;font-weight:600;color:{_EMERALD_100};line-height:1.45;max-width:380px;">conversations happened <strong>after your office closed</strong> &mdash; nights, weekends, and more.</div>
  <div style="margin-top:18px;">{chip_pct}</div>
</td></tr>
</table>
</td></tr>"""


def _section_hero_first(after_hours: int, total: int, month_label: str) -> str:
    night_label = f'{after_hours:,}' if after_hours else '0'
    total_label = f'{total:,}' if total else '0'
    return f"""<tr><td style="padding:22px 36px 0;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
<tr><td style="background:linear-gradient(150deg,{_SLATE_900} 0%,{_EMERALD_900} 70%,{_EMERALD_800} 100%);border-radius:18px;padding:30px 28px 26px;">
  <div style="font-size:10px;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:{_EMERALD_200};">Your first month on the clock</div>
  <div style="font-size:60px;font-weight:800;letter-spacing:-.04em;line-height:1;margin:10px 0 4px;color:{_WHITE};">{total_label}</div>
  <div style="font-size:16px;font-weight:600;color:{_EMERALD_100};line-height:1.45;max-width:380px;">conversations handled in {month_label}, including <strong>{night_label} after hours</strong>.</div>
</td></tr>
</table>
</td></tr>"""


def _section_hero_small(total: int, after_hours: int) -> str:
    total_label = f'{total:,}'
    return f"""<tr><td style="padding:22px 36px 0;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
<tr><td style="background:linear-gradient(150deg,{_SLATE_900} 0%,{_EMERALD_900} 70%,{_EMERALD_800} 100%);border-radius:18px;padding:30px 28px 26px;">
  <div style="font-size:10px;font-weight:700;letter-spacing:.14em;text-transform:uppercase;color:{_EMERALD_200};">Every conversation answered</div>
  <div style="font-size:60px;font-weight:800;letter-spacing:-.04em;line-height:1;margin:10px 0 4px;color:{_WHITE};">{total_label}</div>
  <div style="font-size:16px;font-weight:600;color:{_EMERALD_100};line-height:1.45;max-width:380px;">conversations &mdash; every one answered, {after_hours} of them after hours.</div>
</td></tr>
</table>
</td></tr>"""


def _section_big_three(
    variant: str,
    conversations: int,
    leads: int,
    staff_hours: float,
    work_weeks: float,
    conv_delta_str: str,
    leads_delta_str: str,
) -> str:
    if staff_hours >= 10:
        hours_label = f'~{int(round(staff_hours))} hrs'
    else:
        hours_label = f'~{staff_hours:.1f} hrs'

    ww_label = f'{work_weeks:.1f} work-wks'

    if variant in ('small_tenant',):
        conv_delta_html = ''
        leads_delta_html = ''
    else:
        conv_delta_html = f'<div style="font-size:11px;font-weight:800;color:{_EMERALD_600};">{conv_delta_str}</div>' if conv_delta_str else ''
        leads_delta_html = f'<div style="font-size:11px;font-weight:800;color:{_EMERALD_600};">{leads_delta_str}</div>' if leads_delta_str else ''

    conv_cell = f"""<td width="33%" style="border:1px solid {_SLATE_200};border-radius:14px;padding:16px 14px 14px;text-align:center;vertical-align:top;">
  <div style="font-size:28px;font-weight:800;letter-spacing:-.02em;color:{_SLATE_900};">{conversations:,}</div>
  {conv_delta_html}
  <div style="font-size:11px;font-weight:600;color:{_SLATE_500};margin-top:4px;line-height:1.4;">conversations</div>
</td>"""

    leads_cell = f"""<td width="33%" style="border:1px solid {_SLATE_200};border-radius:14px;padding:16px 14px 14px;text-align:center;vertical-align:top;">
  <div style="font-size:28px;font-weight:800;letter-spacing:-.02em;color:{_SLATE_900};">{leads:,}</div>
  {leads_delta_html}
  <div style="font-size:11px;font-weight:600;color:{_SLATE_500};margin-top:4px;line-height:1.4;">leads delivered</div>
</td>"""

    hours_cell = f"""<td width="33%" style="background-color:{_EMERALD_50};border:1px solid {_EMERALD_200};border-radius:14px;padding:16px 14px 14px;text-align:center;vertical-align:top;">
  <div style="font-size:28px;font-weight:800;letter-spacing:-.02em;color:{_EMERALD_800};">{hours_label}</div>
  <div style="font-size:11px;font-weight:800;color:{_EMERALD_600};">&asymp; {ww_label}</div>
  <div style="font-size:11px;font-weight:600;color:{_SLATE_500};margin-top:4px;line-height:1.4;">of staff time handled</div>
</td>"""

    return f"""<tr><td style="padding:22px 36px 0;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
<tr>
  <td width="33%" style="padding-right:6px;">{conv_cell}</td>
  <td width="33%" style="padding-right:6px;">{leads_cell}</td>
  <td width="33%">{hours_cell}</td>
</tr>
</table>
</td></tr>"""


def _section_channel_mvp(mvp: Dict) -> str:
    channel_safe = _html_escape(mvp.get('channel', '').replace('_', ' ').title())
    leads = int(mvp.get('leads', 0))
    conversations = int(mvp.get('conversations', 0))
    if conversations > 0 and leads > 0:
        rate_pct = round(leads / conversations * 100)
        evidence = f'<strong style="color:{_EMERALD_700};">{rate_pct}% lead rate</strong> from {conversations:,} conversations.'
    else:
        evidence = f'{leads:,} leads from {conversations:,} conversations.'

    return f"""<tr><td style="padding:22px 36px 0;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
<tr><td style="border:1px solid {_EMERALD_200};border-radius:16px;background:linear-gradient(180deg,{_WHITE} 30%,{_EMERALD_50});padding:20px 22px;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
<tr>
  <td width="50" valign="top">
    <div style="background-color:{_EMERALD_100};border-radius:12px;width:46px;height:46px;text-align:center;line-height:46px;font-size:22px;">&#x1F3C6;</div>
  </td>
  <td valign="top" style="padding-left:16px;">
    <div style="font-size:10px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:{_EMERALD_700};">Channel MVP</div>
    <div style="font-size:16px;font-weight:800;letter-spacing:-.01em;margin:3px 0 2px;color:{_SLATE_900};">{channel_safe}</div>
    <p style="font-size:12px;color:{_SLATE_500};line-height:1.5;margin:0;">{evidence}</p>
  </td>
</tr>
</table>
</td></tr>
</table>
</td></tr>"""


def _section_topics(topics: List[Tuple[str, int]], variant: str) -> str:
    if not topics:
        return ''

    max_count = max(t[1] for t in topics) if topics else 1
    bars = []
    emerald_shades = [_EMERALD_500, _EMERALD_400, _EMERALD_200]
    for i, (topic, count) in enumerate(topics[:3]):
        shade = emerald_shades[min(i, 2)]
        pct = round(count / max_count * 100) if max_count > 0 else 0
        bars.append(
            f'<tr><td style="padding-bottom:12px;">'
            f'<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">'
            f'<tr>'
            f'<td><span style="font-size:12px;font-weight:700;color:{_SLATE_900};">{_html_escape(topic)}</span></td>'
            f'<td align="right"><span style="font-size:11px;font-weight:600;color:{_SLATE_400};">{count:,} conversations</span></td>'
            f'</tr>'
            f'<tr><td colspan="2" style="padding-top:5px;">'
            f'<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background-color:{_SLATE_100};border-radius:999px;height:10px;">'
            f'<tr><td width="{pct}%" style="background-color:{shade};border-radius:999px;height:10px;"></td><td></td></tr>'
            f'</table>'
            f'</td></tr>'
            f'</table>'
            f'</td></tr>'
        )

    bars_html = '\n'.join(bars)
    note_html = ''
    if len(topics) > 3 and variant not in ('small_tenant',):
        overachiever = topics[3][0] if len(topics) > 3 else None
        if overachiever:
            note_html = f'<tr><td style="padding-top:10px;"><span style="font-size:12px;color:{_SLATE_500};">Quiet overachiever: <strong style="color:{_EMERALD_700};">{_html_escape(overachiever)}</strong> conversations kept the team informed.</span></td></tr>'

    return f"""<tr><td style="padding:26px 36px 0;">
  <div style="font-size:10px;font-weight:800;letter-spacing:.12em;text-transform:uppercase;color:{_SLATE_400};margin-bottom:12px;">What people asked about</div>
  <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%">
  {bars_html}
  {note_html}
  </table>
</td></tr>"""


def _section_superlatives(superlatives: List[Tuple[str, str, str]]) -> str:
    cells = []
    border_style = f'1px solid {_SLATE_100}'
    for i, (emoji, label, desc) in enumerate(superlatives):
        border_right = f'border-right:{border_style};' if i < len(superlatives) - 1 else ''
        cells.append(
            f'<td style="padding:0 14px;{border_right}text-align:left;vertical-align:top;">'
            f'<div style="font-size:16px;">{emoji}</div>'
            f'<div style="font-size:13px;font-weight:800;margin-top:4px;color:{_SLATE_900};">{_html_escape(label)}</div>'
            f'<div style="font-size:11px;color:{_SLATE_400};line-height:1.4;margin-top:2px;">{_html_escape(desc)}</div>'
            f'</td>'
        )

    cells_html = '\n'.join(cells)
    return f"""<tr><td style="padding:24px 36px 0;">
<table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border:1px dashed {_SLATE_200};border-radius:14px;padding:16px 6px;">
<tr>
{cells_html}
</tr>
</table>
</td></tr>"""


def _section_cta(month_label: str, cta_url: str, variant: str) -> str:
    nudge = ''
    if variant == 'good_month':
        nudge = f'<p style="font-size:12px;color:{_SLATE_400};margin-top:12px;"><strong style="color:{_SLATE_600};">Board meeting coming up?</strong> The briefing exports as a board-ready PDF in one click.</p>'

    return f"""<tr><td style="text-align:center;padding:30px 36px 8px;">
  <a href="{cta_url}" style="display:inline-block;background-color:{_EMERALD_500};color:{_WHITE};font-weight:800;font-size:14px;border-radius:999px;padding:14px 34px;text-decoration:none;">Read your full {month_label} briefing &rarr;</a>
  {nudge}
</td></tr>"""


def _section_footer(
    org_name: str,
    dashboard_base: str,
    postal_address: str = '',
    unsub_url: str = '',
) -> str:
    """
    Footer: why-receiving + settings + unsubscribe + postal address.
    CAN-SPAM condition 1: postal_address rendered inline.
    CAN-SPAM condition 3: unsub_url is the per-recipient tokenized link.
    C8.10 compliant: no PII; aggregate counts only in body above.
    """
    org_safe = _html_escape(org_name)
    settings_url = f'{dashboard_base}/settings/notifications'
    postal_html = f'<br>{_html_escape(postal_address)}' if postal_address else ''
    actual_unsub_url = unsub_url if unsub_url else f'{dashboard_base}/unsubscribe'

    return f"""<tr><td style="text-align:center;font-size:10px;color:{_SLATE_400};padding:22px 36px 26px;line-height:1.6;border-top:1px solid {_SLATE_100};margin-top:22px;">
  You&rsquo;re receiving this because Mission Intelligence is enabled for {org_safe}.<br>
  Every number is something MyRecruiter directly witnessed.{postal_html}<br>
  <a href="{settings_url}" style="color:{_SLATE_400};">Monthly recap settings</a> &middot;
  <a href="{actual_unsub_url}" style="color:{_SLATE_400};">Unsubscribe</a>
</td></tr>"""


# ---------------------------------------------------------------------------
# Text fallback
# ---------------------------------------------------------------------------
def _render_text_fallback(
    variant: str,
    summary_row: Dict,
    month_str: str,
    org_name: str,
    postal_address: str = '',
    unsub_url: str = '',
) -> str:
    """Plain-text fallback. No dollar signs. No per-person data.
    CAN-SPAM condition 1: postal_address appended.
    CAN-SPAM condition 3: unsub_url rendered as plain text link.
    """
    month_label = _month_label(month_str)
    conversations = int(summary_row.get('conversations', 0))
    leads = int(summary_row.get('leads', 0))
    conversation_minutes = int(summary_row.get('conversation_minutes', 0))
    staff_hours = conversation_minutes / 60.0
    work_weeks = staff_hours / WORK_WEEK_HOURS
    after_hours = int(summary_row.get('after_hours_conversations', 0))

    cta_url = f'{DASHBOARD_BASE_URL}/attribution'
    actual_unsub_url = unsub_url if unsub_url else f'{DASHBOARD_BASE_URL}/unsubscribe'

    lines = [
        f'YOUR {month_label.upper()}, BY THE NUMBERS',
        f'Prepared for {org_name}',
        '',
        f'Conversations: {conversations:,}',
        f'Leads delivered: {leads:,}',
        f'Staff time handled: ~{staff_hours:.1f} hrs (approx. {work_weeks:.1f} work-weeks)',
        f'After-hours conversations: {after_hours:,}',
        '',
        f'Read your full {month_label} briefing: {cta_url}',
        '',
        f'You are receiving this because Mission Intelligence is enabled for {org_name}.',
        f'Settings: {DASHBOARD_BASE_URL}/settings/notifications',
        f'Unsubscribe: {actual_unsub_url}',
    ]
    if postal_address:
        lines.append(postal_address)
    return '\n'.join(lines)


# ---------------------------------------------------------------------------
# Data helpers
# ---------------------------------------------------------------------------

def _month_label(month_str: str) -> str:
    """'2026-06' -> 'June 2026'"""
    try:
        parts = month_str.split('-')
        year = int(parts[0])
        mon = int(parts[1])
        return f'{month_name[mon]} {year}'
    except (IndexError, ValueError):
        return month_str


def _pct_delta_str(current: int, prior) -> str:
    if prior is None:
        return ''
    try:
        prior_int = int(prior)
    except (TypeError, ValueError):
        return ''
    if prior_int == 0:
        return ''
    pct = round((current - prior_int) / prior_int * 100)
    if pct >= 0:
        return f'▲ {pct}% vs prior month'
    return f'▼ {abs(pct)}% vs prior month'


def _extract_top_topics(channel_rows: List[Dict]) -> List[Tuple[str, int]]:
    merged: Dict[str, int] = {}
    for row in channel_rows:
        data = row.get('data', row)
        tc = data.get('topic_counts', {}) or {}
        for topic, count in tc.items():
            try:
                merged[topic] = merged.get(topic, 0) + int(count)
            except (TypeError, ValueError):
                pass

    if not merged:
        return []

    sorted_topics = sorted(merged.items(), key=lambda x: x[1], reverse=True)
    return sorted_topics[:TOPICS_TOP_N + 1]


def _find_mvp_channel(channel_rows: List[Dict]) -> Optional[Dict]:
    best = None
    best_rate = -1.0

    for row in channel_rows:
        data = row.get('data', row)
        conversations = int(data.get('conversations', 0) or 0)
        leads = int(data.get('leads', 0) or 0)
        channel = row.get('channel', data.get('channel', ''))

        if conversations < CONFIDENCE_FLOOR:
            continue

        rate = leads / conversations if conversations > 0 else 0.0
        if rate > best_rate:
            best_rate = rate
            best = {
                'channel': channel,
                'leads': leads,
                'conversations': conversations,
                'rate': rate,
            }

    return best


def _build_superlatives(
    self_booked_pct,
    median_first_response,
    after_hours: int,
) -> List[Tuple[str, str, str]]:
    items = []

    if after_hours and after_hours > 0:
        items.append(('&#x1F989;', f'{after_hours:,} after-hours', 'conversations handled outside business hours'))

    if self_booked_pct is not None:
        try:
            pct_val = float(self_booked_pct)
            if pct_val > 0:
                items.append(('&#x1F4C5;', f'{round(pct_val)}% self-booked', 'leads who scheduled themselves &mdash; zero waiting'))
        except (TypeError, ValueError):
            pass

    if median_first_response is not None:
        try:
            minutes = float(median_first_response)
            if minutes > 0:
                items.append(('&#x26A1;', f'{round(minutes)} min response', 'median time to first answer'))
        except (TypeError, ValueError):
            pass

    return items


def _html_escape(text: str) -> str:
    return (
        str(text)
        .replace('&', '&amp;')
        .replace('<', '&lt;')
        .replace('>', '&gt;')
        .replace('"', '&quot;')
        .replace("'", '&#x27;')
    )


# ---------------------------------------------------------------------------
# DynamoDB: aggregate reads (FROZEN_CONTRACTS C5)
# ---------------------------------------------------------------------------

def _load_aggregate(tenant_id: str, metric_key: str) -> Dict:
    if not ATTRIBUTION_AGGREGATES_TABLE:
        return {}

    try:
        table = _dynamodb.Table(ATTRIBUTION_AGGREGATES_TABLE)
        resp = table.get_item(Key={
            'pk': f'TENANT#{tenant_id}',
            'sk': f'METRIC#{metric_key}',
        })
        item = resp.get('Item', {})
        return item.get('data', {}) or {}
    except ClientError as exc:
        logger.error(
            'Aggregate read failed for tenant=%s key=%s: %s',
            tenant_id[:8], metric_key, exc,
        )
        return {}
    except Exception as exc:
        logger.error(
            'Unexpected error reading aggregate for tenant=%s key=%s: %s',
            tenant_id[:8], metric_key, exc,
        )
        return {}


def _load_channel_rows(tenant_id: str, month_str: str) -> List[Dict]:
    if not ATTRIBUTION_AGGREGATES_TABLE:
        return []

    try:
        from boto3.dynamodb.conditions import Key, Attr
        table = _dynamodb.Table(ATTRIBUTION_AGGREGATES_TABLE)
        prefix = f'METRIC#attribution_channel#{month_str}#'
        resp = table.query(
            KeyConditionExpression=(
                Key('pk').eq(f'TENANT#{tenant_id}') &
                Key('sk').begins_with(prefix)
            )
        )
        rows = []
        for item in resp.get('Items', []):
            sk = item.get('sk', '')
            suffix = sk[len(prefix):]
            rows.append({
                'channel': suffix,
                'data': item.get('data', {}) or {},
            })
        return rows
    except ClientError as exc:
        logger.error(
            'Channel rows query failed for tenant=%s month=%s: %s',
            tenant_id[:8], month_str, exc,
        )
        return []
    except Exception as exc:
        logger.error(
            'Unexpected error loading channel rows for tenant=%s month=%s: %s',
            tenant_id[:8], month_str, exc,
        )
        return []


# ---------------------------------------------------------------------------
# Idempotency marker
# ---------------------------------------------------------------------------

def _recap_sent_sk(month_str: str) -> str:
    return f'METRIC#recap_sent#{month_str}'


def _recap_already_sent(tenant_id: str, month_str: str) -> bool:
    if not ATTRIBUTION_AGGREGATES_TABLE:
        return False

    try:
        table = _dynamodb.Table(ATTRIBUTION_AGGREGATES_TABLE)
        resp = table.get_item(Key={
            'pk': f'TENANT#{tenant_id}',
            'sk': _recap_sent_sk(month_str),
        })
        return bool(resp.get('Item'))
    except ClientError as exc:
        logger.warning(
            'Idempotency check failed for tenant=%s month=%s (will proceed): %s',
            tenant_id[:8], month_str, exc,
        )
        return False


def _mark_recap_sent(tenant_id: str, month_str: str) -> None:
    if not ATTRIBUTION_AGGREGATES_TABLE:
        return

    ttl = int(
        (datetime.now(timezone.utc) + timedelta(days=RECAP_SENT_TTL_DAYS)).timestamp()
    )
    try:
        table = _dynamodb.Table(ATTRIBUTION_AGGREGATES_TABLE)
        table.put_item(
            Item={
                'pk': f'TENANT#{tenant_id}',
                'sk': _recap_sent_sk(month_str),
                'sent_at': datetime.now(timezone.utc).isoformat(),
                'ttl': ttl,
            },
            ConditionExpression='attribute_not_exists(sk)',
        )
    except ClientError as exc:
        if exc.response['Error']['Code'] == 'ConditionalCheckFailedException':
            logger.info(
                'Recap marker already exists (race condition) for tenant=%s month=%s',
                tenant_id[:8], month_str,
            )
        else:
            logger.error(
                'Failed to write recap marker for tenant=%s month=%s: %s',
                tenant_id[:8], month_str, exc,
            )


# ---------------------------------------------------------------------------
# send_email Lambda invocation
# ---------------------------------------------------------------------------

def _invoke_send_email(payload: Dict, tenant_id: str, month_str: str) -> None:
    """
    Invoke the send_email Lambda per its Lambda-to-Lambda contract.
    C8.10: do NOT log recipient addresses or HTML body at INFO level.
    """
    if not SEND_EMAIL_FUNCTION_NAME:
        logger.error(
            'SEND_EMAIL_FUNCTION_NAME not configured -- cannot send recap for tenant=%s month=%s',
            tenant_id[:8], month_str,
        )
        return

    invoke_payload = {'body': json.dumps(payload)}

    try:
        resp = _lambda_client.invoke(
            FunctionName=SEND_EMAIL_FUNCTION_NAME,
            InvocationType='RequestResponse',
            Payload=json.dumps(invoke_payload).encode('utf-8'),
        )
        raw = resp['Payload'].read()
        result = json.loads(raw) if raw else {}

        if resp.get('FunctionError'):
            logger.error(
                'send_email FunctionError for tenant=%s month=%s: %s',
                tenant_id[:8], month_str, result,
            )
            return

        response_body = {}
        body_str = result.get('body', '{}')
        if isinstance(body_str, str):
            try:
                response_body = json.loads(body_str)
            except json.JSONDecodeError:
                response_body = {}
        elif isinstance(body_str, dict):
            response_body = body_str

        if response_body.get('success'):
            logger.info(
                'Recap email sent: tenant=%s month=%s message_id=%s',
                tenant_id[:8], month_str, response_body.get('message_id', '?'),
            )
        else:
            logger.error(
                'send_email reported failure for tenant=%s month=%s: %s',
                tenant_id[:8], month_str, response_body.get('error', '?'),
            )

    except ClientError as exc:
        logger.error(
            'Lambda invoke failed for send_email tenant=%s month=%s: %s',
            tenant_id[:8], month_str, exc,
        )
    except Exception as exc:
        logger.error(
            'Unexpected error invoking send_email for tenant=%s month=%s: %s',
            tenant_id[:8], month_str, exc,
        )


# ---------------------------------------------------------------------------
# Tenant enumeration + config
# ---------------------------------------------------------------------------

def _get_active_tenant_pairs() -> List[Tuple[str, str]]:
    if not TENANT_CONFIG_BUCKET:
        logger.warning('TENANT_CONFIG_BUCKET not configured -- cannot enumerate tenants')
        return []

    pairs: List[Tuple[str, str]] = []
    try:
        paginator = _s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=TENANT_CONFIG_BUCKET, Prefix='mappings/', Delimiter='/'):
            for obj in page.get('Contents', []):
                key = obj.get('Key', '')
                if not key.endswith('.json'):
                    continue
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
    if not TENANT_CONFIG_BUCKET:
        return None
    try:
        resp = _s3.get_object(
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


def _get_tenant_config(tenant_id: str) -> Dict:
    if tenant_id in _tenant_config_cache:
        return _tenant_config_cache[tenant_id]

    if not TENANT_CONFIG_BUCKET:
        logger.warning('TENANT_CONFIG_BUCKET not configured for tenant=%s', tenant_id[:8])
        _tenant_config_cache[tenant_id] = {}
        return {}

    config: Dict = {}
    for key in [
        f'tenants/{tenant_id}/{tenant_id}-config.json',
        f'tenants/{tenant_id}/config.json',
    ]:
        try:
            resp = _s3.get_object(Bucket=TENANT_CONFIG_BUCKET, Key=key)
            config = json.loads(resp['Body'].read().decode('utf-8'))
            break
        except ClientError as exc:
            if exc.response['Error']['Code'] not in ('NoSuchKey', '404'):
                logger.error('Config fetch failed for tenant=%s key=%s: %s', tenant_id[:8], key, exc)
        except Exception as exc:
            logger.error('Config fetch failed for tenant=%s key=%s: %s', tenant_id[:8], key, exc)

    _tenant_config_cache[tenant_id] = config
    return config
