"""
Attribution API route handlers — WS-D
======================================
Four routes wired into Analytics_Dashboard_API/lambda_function.py.

  GET  /attribution/summary
  GET  /attribution/channels/{channel}
  GET  /attribution/entry-points
  POST /attribution/entry-points

All routes:
  - JWT auth (handled by lambda_function.py before dispatch)
  - Server-side flag check: dashboard_attribution -> 403 when off (C6, locked decision #10)
  - Responses via cors_response(status, body); body always includes tenant_id, month, source
  - Aggregates only — no per-person data (C8)
  - Never log full payloads at info level — IDs and counts only (C8.10)

Env vars (declare in Lambda config; integrator glue adds grants):
  ATTRIBUTION_AGGREGATES_TABLE — picasso-attribution-aggregates (C5 rows, NEW table per C5 re-home)
  ENTRY_POINTS_TABLE           — picasso-entry-points (C3 registry)
  MINT_FUNCTION_NAME           — WS-B's mint Lambda (C4b proxy)

Cite: FROZEN_CONTRACTS.md C4b, C5, C6, C7, C8
"""

from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse, parse_qs
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError

from attribution_rules import (
    CONFIDENCE_FLOOR,
    channel_rate_held,
    channel_read,
    channel_suggested_move,
    entry_point_rate_held,
    summary_insight,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Env vars — read at module import (Lambda lifetime cache)
# ---------------------------------------------------------------------------
# C5 re-home (2026-06-12): new table + new env var; legacy AGGREGATES_TABLE/
# picasso-dashboard-aggregates are dead — using them yields silent all-zero responses.
AGGREGATES_TABLE = os.environ.get("ATTRIBUTION_AGGREGATES_TABLE", "picasso-attribution-aggregates")
ENTRY_POINTS_TABLE = os.environ.get("ENTRY_POINTS_TABLE", "")
MINT_FUNCTION_NAME = os.environ.get("MINT_FUNCTION_NAME", "")

# C7 — tenant timezone (PROVISIONAL default; cite FROZEN_CONTRACTS C7)
DEFAULT_TZ = "America/Chicago"

# Valid channel enum (C8.12)
VALID_CHANNELS = {"website", "messenger", "standalone", "campaign"}

# ---------------------------------------------------------------------------
# Boto3 clients — module-level; Lambda reuse across invocations
# ---------------------------------------------------------------------------
_dynamodb = boto3.client("dynamodb")
_lambda_client = boto3.client("lambda")

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_MONTH_RE = re.compile(r"^\d{4}-(?:0[1-9]|1[0-2])$")


def _current_month_tenant_local(config: dict[str, Any]) -> str:
    """Return the current calendar month (YYYY-MM) in the tenant's timezone."""
    tz_name = config.get("timezone") or DEFAULT_TZ
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo(DEFAULT_TZ)
    return datetime.now(tz).strftime("%Y-%m")


def _next_month(month: str) -> str:
    """Return the YYYY-MM of the month after the given YYYY-MM string."""
    year, mon = int(month[:4]), int(month[5:7])
    if mon == 12:
        return f"{year + 1}-01"
    return f"{year}-{mon + 1:02d}"


def _validate_month(month_param: Optional[str], config: dict[str, Any]) -> tuple[str, Optional[dict]]:
    """
    Validate and normalise the ?month= parameter.
    Returns (month_str, None) on success or (None, error_response) on failure.
    error_response is a pre-built dict suitable for cors_response(400, ...).
    Rejects months more than 1 calendar month in the future (Fix 7 — month sanity ceiling).
    """
    if not month_param:
        return _current_month_tenant_local(config), None
    if not _MONTH_RE.match(month_param):
        return "", {"error": "Invalid month parameter", "message": "month must be YYYY-MM"}
    # Ceiling: reject months more than 1 calendar month ahead of current tenant-local month
    current = _current_month_tenant_local(config)
    ceiling = _next_month(current)
    if month_param > ceiling:
        return "", {
            "error": "month out of range",
            "message": f"month may not be more than 1 calendar month in the future (ceiling: {ceiling})",
        }
    return month_param, None


def _deserialize_item(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Minimal DynamoDB low-level type deserialiser for the attribute shapes we use.
    Handles S (string), N (number->int/float), BOOL, NULL, M (map S->N), L (list).
    Schema-tolerant: unknown type descriptors are passed through as-is.
    """
    out: dict[str, Any] = {}
    for k, v in raw.items():
        if "S" in v:
            out[k] = v["S"]
        elif "N" in v:
            n = v["N"]
            out[k] = int(n) if "." not in n else float(n)
        elif "BOOL" in v:
            out[k] = v["BOOL"]
        elif "NULL" in v:
            out[k] = None
        elif "M" in v:
            out[k] = {mk: (int(mv["N"]) if "." not in mv["N"] else float(mv["N"]))
                      if "N" in mv else mv.get("S", "")
                      for mk, mv in v["M"].items()}
        elif "L" in v:
            out[k] = [_deserialize_item({"_": li}).get("_") for li in v["L"]]
        else:
            out[k] = v  # pass-through unknown type descriptors
    return out


def _hoist_data(raw: dict[str, Any]) -> dict[str, Any]:
    """The Attribution_Aggregator writes the C5 metrics nested under a `data`
    Map; older/seeded rows also carry them at the top level. Merge the `data`
    members up to the top level (raw AttributeValue level, so nested Maps like
    topic_counts/reach deserialize correctly) so the reader is tolerant of both
    shapes. `data` members win on conflict — the aggregator is authoritative.
    Without this, aggregator-written rows read as all-zeros (the reader accesses
    metrics top-level) and the dashboard shows the empty state.
    """
    data = raw.get("data")
    if isinstance(data, dict) and isinstance(data.get("M"), dict):
        merged = {k: v for k, v in raw.items() if k != "data"}
        merged.update(data["M"])
        return merged
    return raw


def _get_agg_item(tenant_id: str, sk: str) -> Optional[dict[str, Any]]:
    """
    Fetch a single C5 aggregate row by pk=TENANT#{tenant_id} sk=METRIC#{sk}.
    C5 key attributes are lowercase pk/sk (matches session-events convention).
    Returns a schema-tolerant deserialized dict or None if not found.
    """
    try:
        resp = _dynamodb.get_item(
            TableName=AGGREGATES_TABLE,
            Key={
                "pk": {"S": f"TENANT#{tenant_id}"},
                "sk": {"S": f"METRIC#{sk}"},
            },
        )
    except ClientError as e:
        logger.error("[attribution] DDB get_item error sk=%s code=%s", sk,
                     e.response["Error"]["Code"])
        raise
    raw = resp.get("Item")
    if not raw:
        return None
    return _deserialize_item(_hoist_data(raw))


def _query_agg_prefix(tenant_id: str, sk_prefix: str) -> list[dict[str, Any]]:
    """
    Query C5 rows by pk=TENANT#{tenant_id} sk begins_with sk_prefix.
    C5 key attributes are lowercase pk/sk (matches session-events convention).
    Returns a list of deserialized dicts (may be empty).
    """
    results: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = dict(
        TableName=AGGREGATES_TABLE,
        KeyConditionExpression="pk = :pk AND begins_with(sk, :sk_prefix)",
        ExpressionAttributeValues={
            ":pk": {"S": f"TENANT#{tenant_id}"},
            ":sk_prefix": {"S": f"METRIC#{sk_prefix}"},
        },
    )
    while True:
        try:
            resp = _dynamodb.query(**kwargs)
        except ClientError as e:
            logger.error("[attribution] DDB query error prefix=%s code=%s", sk_prefix,
                         e.response["Error"]["Code"])
            raise
        for raw in resp.get("Items", []):
            results.append(_deserialize_item(_hoist_data(raw)))
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
        kwargs["ExclusiveStartKey"] = last
    return results


def _query_entry_points_registry(tenant_id: str) -> list[dict[str, Any]]:
    """
    Direct DDB Query on the C3 registry table (PK = tenant_id).
    Schema-tolerant reads — old rows may lack new attributes.
    """
    if not ENTRY_POINTS_TABLE:
        logger.warning("[attribution] ENTRY_POINTS_TABLE not set; returning empty list")
        return []
    results: list[dict[str, Any]] = []
    kwargs: dict[str, Any] = dict(
        TableName=ENTRY_POINTS_TABLE,
        KeyConditionExpression="tenant_id = :tid",
        ExpressionAttributeValues={":tid": {"S": tenant_id}},
    )
    while True:
        try:
            resp = _dynamodb.query(**kwargs)
        except ClientError as e:
            logger.error("[attribution] registry query error code=%s",
                         e.response["Error"]["Code"])
            raise
        for raw in resp.get("Items", []):
            results.append(_deserialize_item(raw))
        last = resp.get("LastEvaluatedKey")
        if not last:
            break
        kwargs["ExclusiveStartKey"] = last
    return results


def _invoke_mint(payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    """
    Invoke WS-B's mint Lambda (C4b).
    Returns (http_status, response_body_dict).

    Error code mapping (C6):
      SUFFIX_TAKEN  -> 409
      DUB_ERROR     -> 502
      VALIDATION    -> 400
      CONFLICT      -> 409
    If MINT_FUNCTION_NAME unset -> 503.
    """
    if not MINT_FUNCTION_NAME:
        logger.warning("[attribution] MINT_FUNCTION_NAME not configured")
        return 503, {"error": "Mint service not configured"}

    try:
        resp = _lambda_client.invoke(
            FunctionName=MINT_FUNCTION_NAME,
            InvocationType="RequestResponse",
            Payload=json.dumps(payload).encode(),
        )
    except ClientError as e:
        logger.error("[attribution] Lambda invoke error code=%s",
                     e.response["Error"]["Code"])
        return 502, {"error": "Mint service unavailable"}

    raw = resp["Payload"].read()
    try:
        body = json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        logger.error("[attribution] Mint Lambda returned non-JSON payload")
        return 502, {"error": "Mint service returned invalid response"}

    # WS-B returns {ok: true, entry_point: {...}} or {ok: false, error: {code, message}}
    if body.get("ok"):
        return 201, body

    error_code = (body.get("error") or {}).get("code", "")
    status_map = {
        "SUFFIX_TAKEN": 409,
        "DUB_ERROR": 502,
        "VALIDATION": 400,
        "CONFLICT": 409,
    }
    http_status = status_map.get(error_code, 502)
    return http_status, body


def _rate_pct(leads: int, conversations: int) -> float:
    """Lead conversion rate as a float 0.0-1.0."""
    if conversations <= 0:
        return 0.0
    return leads / conversations


def _prior_month(month: str) -> str:
    """Return the YYYY-MM of the month before the given YYYY-MM string."""
    year, mon = int(month[:4]), int(month[5:7])
    if mon == 1:
        return f"{year - 1}-12"
    return f"{year}-{mon - 1:02d}"


# ---------------------------------------------------------------------------
# Mint request validation (C4b + C8)
# ---------------------------------------------------------------------------

_LABEL_RE = re.compile(r"^[^@]{1,128}$")
_CAMPAIGN_RE = re.compile(r"^[^@]{1,128}$")
_PLACEMENT_RE = re.compile(r"^[^@]{1,128}$")
_URL_RE = re.compile(r"^https://[^/\s]+")
_EP_ID_RE = re.compile(r"^ep_[0-9A-Za-z]{8,64}$")
_SUFFIX_RE = re.compile(r"^[A-Za-z0-9._-]+$")
_SUFFIX_MAX = 190


def _validate_target_url(url: str) -> Optional[str]:
    """
    Validate target.url per C8.15:
    - must be https://
    - no userinfo (@ in netloc)
    - query params: every key must start with utm_ ('ep' is appended by the service, never client-supplied)
    Returns an error string or None on success.
    """
    if not url or not _URL_RE.match(url):
        return "target.url must be an https:// URL"
    parsed = urlparse(url)
    if parsed.username or (parsed.netloc and "@" in parsed.netloc):
        return "target.url must not contain userinfo"
    if parsed.query:
        qs = parse_qs(parsed.query, keep_blank_values=True)
        bad_keys = [k for k in qs if not k.startswith("utm_")]
        if bad_keys:
            return (
                f"target.url query parameters must start with utm_; "
                f"rejected: {', '.join(sorted(bad_keys))}"
            )
    return None


def _validate_mint_body(body: dict[str, Any]) -> Optional[str]:
    """
    Validate the POST /attribution/entry-points request body per C4b.
    Returns an error message string on failure, or None on success.
    C8.13: length caps, reject @; C8.14: no person fields accepted.
    C8.15: target.url query-param restriction (only utm_* allowed).
    """
    required = ("label", "channel", "campaign", "placement", "target")
    for f in required:
        if not body.get(f):
            return f"Missing required field: {f}"

    label = body.get("label", "")
    if not _LABEL_RE.match(label):
        return "label must be 1-128 chars and must not contain @"

    channel = body.get("channel", "")
    if channel not in ("standalone", "campaign"):
        return "channel must be standalone or campaign"

    campaign = body.get("campaign", "")
    if not _CAMPAIGN_RE.match(campaign):
        return "campaign must be 1-128 chars and must not contain @"

    placement = body.get("placement", "")
    if not _PLACEMENT_RE.match(placement):
        return "placement must be 1-128 chars and must not contain @"

    target = body.get("target", {})
    if not isinstance(target, dict):
        return "target must be an object"
    target_type = target.get("type", "")
    if target_type not in ("standalone_chat", "site_url"):
        return "target.type must be standalone_chat or site_url"
    url_err = _validate_target_url(target.get("url", ""))
    if url_err:
        return url_err

    # Optional suffix: when present must be <=190 chars and match safe charset
    suffix = body.get("suffix")
    if suffix is not None:
        if not isinstance(suffix, str) or len(suffix) == 0:
            return "suffix must be a non-empty string when provided"
        if len(suffix) > _SUFFIX_MAX:
            return f"suffix must be at most {_SUFFIX_MAX} characters"
        if not _SUFFIX_RE.match(suffix):
            return "suffix may only contain letters, digits, dots, underscores, and hyphens"

    return None


# ---------------------------------------------------------------------------
# Route handlers (called from lambda_function.py after auth + flag check)
# ---------------------------------------------------------------------------

def check_attribution_flag(get_tenant_features_fn, validate_feature_access_fn,
                            tenant_id: str, user_role: Optional[str]) -> Optional[dict]:
    """
    Shared attribution flag gate.  Returns None (access granted) or a 403 dict.
    Delegates to the existing validate_feature_access pattern.
    """
    return validate_feature_access_fn(tenant_id, "dashboard_attribution", user_role)


def handle_attribution_summary(
    tenant_id: str,
    params: dict[str, str],
    config: dict[str, Any],
    cors_response_fn,
    user_role: Optional[str],
    validate_feature_access_fn,
) -> dict[str, Any]:
    """
    GET /attribution/summary?month=YYYY-MM

    Body always includes tenant_id, month, source.
    Never echoes per-person data (C8.7).
    """
    # Flag gate
    err = check_attribution_flag(None, validate_feature_access_fn, tenant_id, user_role)
    if err:
        return err

    month_param = params.get("month")
    month, val_err = _validate_month(month_param, config)
    if val_err:
        return cors_response_fn(400, dict(tenant_id=tenant_id, month=month_param or "", source="validation", **val_err))

    # C5: fetch summary row — wrap DDB calls so operation details never leak to callers (Fix 6)
    try:
        summary_row = _get_agg_item(tenant_id, f"attribution_summary#{month}") or {}
        prior = _get_agg_item(tenant_id, f"attribution_summary#{_prior_month(month)}") or {}
        channel_rows_raw = _query_agg_prefix(tenant_id, f"attribution_channel#{month}#")
    except ClientError:
        return cors_response_fn(502, {
            "tenant_id": tenant_id, "month": month, "source": "dynamodb",
            "error": "Upstream data unavailable",
        })

    # ---- ecosystem ----
    total_convs = int(summary_row.get("conversations", 0))
    after_hours = int(summary_row.get("after_hours_conversations", 0))
    after_hours_pct = round(after_hours / total_convs * 100, 1) if total_convs > 0 else 0.0

    channels_out = []
    for ch_row in channel_rows_raw:
        ch = ch_row.get("channel", "")
        c = int(ch_row.get("conversations", 0))
        l = int(ch_row.get("leads", 0))
        share = round(c / total_convs * 100, 1) if total_convs > 0 else 0.0
        held = channel_rate_held(c)
        rate = _rate_pct(l, c)
        channels_out.append({
            "channel": ch,
            "share_pct": share,
            "conversations": c,
            "leads": l,
            "rate": round(rate, 4),
            "rate_held": held,
        })

    # ---- funnel ----
    convs = int(summary_row.get("conversations", 0))
    engaged = int(summary_row.get("engaged", 0))
    applications = int(summary_row.get("applications", 0))
    leads = int(summary_row.get("leads", 0))
    reach_pv = int(summary_row.get("reach_page_views_sessions", 0))

    # reached = reach_page_views_sessions + sum of minted (scans+clicks)
    minted_reach = 0
    for ch_row in channel_rows_raw:
        ch_reach = ch_row.get("reach", {})
        if isinstance(ch_reach, dict):
            minted_reach += int(ch_reach.get("scans", 0)) + int(ch_reach.get("clicks", 0))
    reached = reach_pv + minted_reach

    funnel_rate = _rate_pct(leads, convs)

    # ---- time ----
    conv_min = int(summary_row.get("conversation_minutes", 0))
    staff_hours = round(conv_min / 60, 1)  # C6: conversation_minutes/60
    work_weeks = round(staff_hours / 40, 1)  # C7
    self_booked = summary_row.get("self_booked_pct")   # nullable
    median_resp = summary_row.get("median_first_response_minutes")  # nullable

    # ---- deltas vs prior month ----
    def _delta(key: str, cur_row: dict, prev_row: dict) -> dict[str, Any]:
        cur = int(cur_row.get(key, 0))
        prev = int(prev_row.get(key, 0))
        abs_d = cur - prev
        pct_d = round(abs_d / prev * 100, 1) if prev != 0 else None
        return {"abs": abs_d, "pct": pct_d}

    deltas = {
        "conversations": _delta("conversations", summary_row, prior),
        "leads": _delta("leads", summary_row, prior),
        "engaged": _delta("engaged", summary_row, prior),
    }

    # ---- insight ----
    insight = summary_insight(channel_rows_raw)

    logger.info("[attribution/summary] tenant=%s month=%s convs=%d",
                tenant_id[:8], month, convs)

    return cors_response_fn(200, {
        "tenant_id": tenant_id,
        "month": month,
        "source": "dynamodb",
        "ecosystem": {
            "total_conversations": total_convs,
            "after_hours_pct": after_hours_pct,
            "channels": channels_out,
        },
        "funnel": {
            "reached": reached,
            "conversations": convs,
            "engaged": engaged,
            "applications": applications,
            "leads": leads,
            "rate": round(funnel_rate, 4),
        },
        "time": {
            "after_hours_conversations": after_hours,
            "staff_hours": staff_hours,
            "work_weeks": work_weeks,
            "self_booked_pct": self_booked,
            "median_first_response_minutes": median_resp,
        },
        "deltas": deltas,
        "insight": insight,
    })


def handle_attribution_channel(
    tenant_id: str,
    channel: str,
    params: dict[str, str],
    config: dict[str, Any],
    cors_response_fn,
    user_role: Optional[str],
    validate_feature_access_fn,
) -> dict[str, Any]:
    """
    GET /attribution/channels/{channel}?month=YYYY-MM

    channel must be one of website|messenger|standalone|campaign (C8.12).
    """
    # Flag gate
    err = check_attribution_flag(None, validate_feature_access_fn, tenant_id, user_role)
    if err:
        return err

    if channel not in VALID_CHANNELS:
        return cors_response_fn(400, {
            "tenant_id": tenant_id, "month": "", "source": "validation",
            "error": "Invalid channel",
            "message": f"channel must be one of {sorted(VALID_CHANNELS)}",
        })

    month_param = params.get("month")
    month, val_err = _validate_month(month_param, config)
    if val_err:
        return cors_response_fn(400, dict(tenant_id=tenant_id, month=month_param or "", source="validation", **val_err))

    # C5: channel row — wrap DDB calls so operation details never leak to callers (Fix 6)
    try:
        ch_row = _get_agg_item(tenant_id, f"attribution_channel#{month}#{channel}") or {}
        ep_rows = _query_agg_prefix(tenant_id, f"attribution_entrypoint#{month}#")
        registry_records = _query_entry_points_registry(tenant_id)
    except ClientError:
        return cors_response_fn(502, {
            "tenant_id": tenant_id, "month": month, "source": "dynamodb",
            "error": "Upstream data unavailable",
        })

    # Filter to this channel (ep rows hold denormalised channel from registry)
    # For website-channel, ep rows are not applicable; include all for others
    ch_ep_rows = [r for r in ep_rows if r.get("channel", "") == channel]

    # C3 registry: get created_at and short_link for each entry point
    registry_map = {r.get("entry_point_id", ""): r for r in registry_records if r.get("channel") == channel}

    # Requested month's start (for is_new check)
    month_start = month + "-01"

    entry_points_out = []
    for ep in ch_ep_rows:
        ep_id = ep.get("entry_point_id") or ep.get("sk", "").split("#")[-1]
        ep_c = int(ep.get("conversations", 0))
        ep_l = int(ep.get("leads", 0))
        ep_rate = _rate_pct(ep_l, ep_c)
        ep_held = entry_point_rate_held(ep_c)
        reg = registry_map.get(ep_id, {})
        created_at = reg.get("created_at") or ep.get("created_at", "")
        is_new = created_at[:7] == month if created_at else False
        entry_points_out.append({
            "entry_point_id": ep_id,
            "label": ep.get("label") or reg.get("label", ""),
            "campaign": ep.get("campaign") or reg.get("campaign", ""),
            "placement": ep.get("placement") or reg.get("placement", ""),
            "created_at": created_at,
            "short_link": reg.get("dub_short_link", ""),
            "scans": int(ep.get("dub_scans", 0)),
            "clicks": int(ep.get("dub_clicks", 0)),
            "conversations": ep_c,
            "leads": ep_l,
            "rate": round(ep_rate, 4),
            "rate_held": ep_held,
            "is_new": is_new,
        })

    # topics
    topic_counts: dict[str, int] = ch_row.get("topic_counts") or {}
    topics_out = sorted(
        [{"topic": t, "count": int(c)} for t, c in topic_counts.items()],
        key=lambda x: -x["count"],
    )

    # resources
    resource_clicks: dict[str, int] = ch_row.get("resource_clicks") or {}
    resources_out = sorted(
        [{"url": u, "clicks": int(c)} for u, c in resource_clicks.items()],
        key=lambda x: -x["clicks"],
    )

    # 6-month trend + website comparison row — also wrapped to prevent DDB detail leakage
    try:
        trend_out = []
        for i in range(5, -1, -1):
            m = month
            for _ in range(i):
                m = _prior_month(m)
            t_row = _get_agg_item(tenant_id, f"attribution_channel#{m}#{channel}") or {}
            trend_out.append({
                "month": m,
                "conversations": int(t_row.get("conversations", 0)),
                "leads": int(t_row.get("leads", 0)),
            })
        website_row = _get_agg_item(tenant_id, f"attribution_channel#{month}#website") or {}
    except ClientError:
        return cors_response_fn(502, {
            "tenant_id": tenant_id, "month": month, "source": "dynamodb",
            "error": "Upstream data unavailable",
        })

    # channel funnel
    ch_c = int(ch_row.get("conversations", 0))
    ch_l = int(ch_row.get("leads", 0))
    ch_engaged = int(ch_row.get("engaged", 0))
    ch_apps = int(ch_row.get("applications", 0))
    ch_rate = _rate_pct(ch_l, ch_c)

    # reach for channel
    ch_reach = ch_row.get("reach", {})
    if isinstance(ch_reach, int):
        reached = ch_reach  # website is int page-view sessions
    elif isinstance(ch_reach, dict):
        reached = int(ch_reach.get("scans", 0)) + int(ch_reach.get("clicks", 0))
    else:
        reached = 0

    w_c = int(website_row.get("conversations", 0))
    w_l = int(website_row.get("leads", 0))

    read = channel_read(channel, ch_c, ch_l, len(entry_points_out))
    sug_move = channel_suggested_move(channel, ch_c, ch_l, w_c, w_l, len(entry_points_out))

    logger.info("[attribution/channel] tenant=%s channel=%s month=%s convs=%d",
                tenant_id[:8], channel, month, ch_c)

    return cors_response_fn(200, {
        "tenant_id": tenant_id,
        "month": month,
        "source": "dynamodb",
        "funnel": {
            "reached": reached,
            "conversations": ch_c,
            "engaged": ch_engaged,
            "applications": ch_apps,
            "leads": ch_l,
            "rate": round(ch_rate, 4),
        },
        "entry_points": entry_points_out,
        "topics": topics_out,
        "resources": resources_out,
        "trend": trend_out,
        "read": read,
        "suggested_move": sug_move,
    })


def handle_attribution_entry_points_list(
    tenant_id: str,
    cors_response_fn,
    user_role: Optional[str],
    validate_feature_access_fn,
) -> dict[str, Any]:
    """
    GET /attribution/entry-points
    Direct DDB Query on C3 registry (read-only).
    """
    err = check_attribution_flag(None, validate_feature_access_fn, tenant_id, user_role)
    if err:
        return err

    try:
        records = _query_entry_points_registry(tenant_id)
    except ClientError:
        return cors_response_fn(502, {
            "tenant_id": tenant_id, "month": "", "source": "dynamodb",
            "error": "Failed to query entry points registry",
        })

    logger.info("[attribution/entry-points] tenant=%s count=%d", tenant_id[:8], len(records))

    return cors_response_fn(200, {
        "tenant_id": tenant_id,
        "month": "",
        "source": "dynamodb",
        "entry_points": records,
    })


def handle_attribution_mint(
    tenant_id: str,
    event: dict[str, Any],
    cors_response_fn,
    user_role: Optional[str],
    validate_feature_access_fn,
) -> dict[str, Any]:
    """
    POST /attribution/entry-points
    Validate body per C4b, inject tenant_id from JWT (NEVER from client body),
    proxy to WS-B's mint Lambda, map error codes.
    C8.14: never add person fields to mint payload.
    """
    err = check_attribution_flag(None, validate_feature_access_fn, tenant_id, user_role)
    if err:
        return err

    try:
        body = json.loads(event.get("body", "{}") or "{}")
    except (json.JSONDecodeError, ValueError):
        return cors_response_fn(400, {
            "tenant_id": tenant_id, "month": "", "source": "validation",
            "error": "Invalid JSON body",
        })

    # C8.14: tenant_id always comes from JWT — ignore any client-supplied tenant_id
    body.pop("tenant_id", None)

    val_err = _validate_mint_body(body)
    if val_err:
        return cors_response_fn(400, {
            "tenant_id": tenant_id, "month": "", "source": "validation",
            "error": "Validation failed", "message": val_err,
        })

    # Build mint payload (C4b) — inject tenant_id from JWT.
    # target is allow-listed to {type, url} only — never forward the verbatim
    # client dict (prevents mass-assignment via extra keys).
    client_target = body["target"]
    safe_target = {"type": client_target["type"], "url": client_target["url"]}
    mint_payload = {
        "action": "mint",
        "tenant_id": tenant_id,
        "label": body["label"],
        "channel": body["channel"],
        "campaign": body["campaign"],
        "placement": body["placement"],
        "target": safe_target,
    }
    if body.get("suffix"):
        mint_payload["suffix"] = body["suffix"]

    # C8.10: never log full payloads; log IDs/counts only
    logger.info("[attribution/mint] tenant=%s channel=%s label_len=%d",
                tenant_id[:8], body.get("channel", ""), len(body.get("label", "")))

    status, resp_body = _invoke_mint(mint_payload)

    # Attach standard envelope fields
    resp_body.setdefault("tenant_id", tenant_id)
    resp_body.setdefault("month", "")
    resp_body.setdefault("source", "mint")

    return cors_response_fn(status, resp_body)
