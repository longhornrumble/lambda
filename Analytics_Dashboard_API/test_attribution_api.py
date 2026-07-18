"""
Tests for WS-D attribution API endpoints and rule pack.

Coverage:
- Each endpoint's exact response shape (happy path)
- 403 when dashboard_attribution flag is off
- Below-floor channel -> rate_held: True + too_early insight
- Old-shape C5 row (missing new attributes) -> no crash (contract/fixture test, REQUIRED)
- Empty tenant (no aggregate rows) -> graceful empty response
- Month param: malformed -> 400; absent -> current month
- Month param: more than 1 calendar month in the future -> 400 (Fix 7 month ceiling)
- Mint proxy: success (201), SUFFIX_TAKEN (409), DUB_ERROR (502),
              VALIDATION (400), CONFLICT (409), MINT_FUNCTION_NAME unset (503)
- Rule pack pure-function tests: best-rate, worth_a_look, too_early, no_data
- target allow-list: extra keys on target not forwarded to mint (Fix 3)
- suffix validation: too long, bad charset, ok value (Fix 4)
- target.url query-param restriction: non-utm_ params rejected (Fix 5)
- DDB ClientError in handle_attribution_summary -> 502 no detail (Fix 6)
- DDB ClientError in handle_attribution_channel -> 502 no detail (Fix 6)

Cite: FROZEN_CONTRACTS.md C5, C6, C7, C8
"""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Make the Lambda dir importable without a full Lambda runtime
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.dirname(__file__))

# Stub heavy deps that aren't available in the test env.
# IMPORTANT: only stub a name if it genuinely cannot be imported — unconditional
# stubs inserted at collection time shadow real same-dir modules (e.g.
# tenant_registry_ops) for every subsequent test file collected by pytest,
# causing "module has no attribute …" failures in unrelated test files.
import importlib
import types

for _stub in ("jwt", "tenant_registry_ops"):
    if _stub not in sys.modules:
        try:
            importlib.import_module(_stub)
            # Real module imported successfully — do nothing; let it win.
        except ImportError:
            sys.modules[_stub] = types.ModuleType(_stub)


# Pre-set env vars before any module-level reads.
# Fix 1: use ATTRIBUTION_AGGREGATES_TABLE (C5 re-home); old AGGREGATES_TABLE is dead.
os.environ.setdefault("ATTRIBUTION_AGGREGATES_TABLE", "picasso-attribution-aggregates-test")
os.environ.setdefault("ENTRY_POINTS_TABLE", "picasso-entry-points-test")
os.environ["MINT_FUNCTION_NAME"] = "mint-function-test"

# Patch boto3 at the top level before importing attribution_api
import boto3  # noqa: E402  (must be after env vars)

import attribution_rules  # noqa: E402
import attribution_api  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cors(status: int, body: dict) -> dict:
    """Minimal cors_response stand-in matching lambda_function.cors_response."""
    return {"statusCode": status, "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body)}


def _feat_ok(_tid, feat, role):
    """validate_feature_access stub: always grants."""
    return None


def _feat_deny(_tid, feat, role):
    """validate_feature_access stub: always denies (flag off)."""
    return _cors(403, {"error": "Feature not available", "feature": feat})


_CONFIG_CHICAGO = {"timezone": "America/Chicago"}
_CONFIG_EMPTY = {}


# ---------------------------------------------------------------------------
# C5 DDB row builder helpers
# ---------------------------------------------------------------------------

def _summary_row(
    tenant_id="T1",
    month="2026-05",
    conversations=200,
    engaged=80,
    applications=30,
    leads=20,
    after_hours_conversations=40,
    conversation_minutes=600,
    reach_page_views_sessions=500,
    self_booked_pct=None,
    median_first_response_minutes=None,
):
    """Build a C5 attribution_summary aggregate dict (deserialized form).
    Uses lowercase pk/sk per C5 key convention (Fix 2).
    """
    row = {
        "pk": f"TENANT#{tenant_id}",
        "sk": f"METRIC#attribution_summary#{month}",
        "conversations": conversations,
        "engaged": engaged,
        "applications": applications,
        "leads": leads,
        "after_hours_conversations": after_hours_conversations,
        "conversation_minutes": conversation_minutes,
        "reach_page_views_sessions": reach_page_views_sessions,
    }
    if self_booked_pct is not None:
        row["self_booked_pct"] = self_booked_pct
    if median_first_response_minutes is not None:
        row["median_first_response_minutes"] = median_first_response_minutes
    return row


def _channel_row(
    tenant_id="T1",
    month="2026-05",
    channel="website",
    conversations=200,
    engaged=80,
    applications=30,
    leads=20,
    topic_counts=None,
    resource_clicks=None,
    reach=500,
):
    """Build a C5 attribution_channel aggregate dict (deserialized form).
    Uses lowercase pk/sk per C5 key convention (Fix 2).
    """
    return {
        "pk": f"TENANT#{tenant_id}",
        "sk": f"METRIC#attribution_channel#{month}#{channel}",
        "channel": channel,
        "conversations": conversations,
        "engaged": engaged,
        "applications": applications,
        "leads": leads,
        "topic_counts": topic_counts or {},
        "resource_clicks": resource_clicks or {},
        "reach": reach,
    }


def _old_shape_summary_row():
    """
    Old-shape C5 row: missing ALL new attribution attributes.
    Contract/fixture test: readers must not crash.  Cite: FROZEN_CONTRACTS C5, Schema Discipline.
    Uses lowercase pk/sk per C5 key convention (Fix 2).
    """
    return {
        "pk": "TENANT#T1",
        "sk": "METRIC#attribution_summary#2025-01",
        # Intentionally absent: conversations, engaged, applications, leads,
        # after_hours_conversations, conversation_minutes, reach_page_views_sessions,
        # self_booked_pct, median_first_response_minutes
    }


def _old_shape_channel_row():
    """Old-shape C5 channel row: missing all new fields.
    Uses lowercase pk/sk per C5 key convention (Fix 2).
    """
    return {
        "pk": "TENANT#T1",
        "sk": "METRIC#attribution_channel#2025-01#website",
        "channel": "website",
        # Intentionally absent: conversations, leads, reach, topic_counts, resource_clicks
    }


# ---------------------------------------------------------------------------
# Rule pack unit tests (pure functions — no I/O)
# ---------------------------------------------------------------------------

class TestAttributionRules:
    def test_summary_insight_no_data(self):
        result = attribution_rules.summary_insight([])
        assert result["held"] is True
        assert result["rule_id"] == "no_data"

    def test_summary_insight_all_below_floor(self):
        rows = [
            {"channel": "website", "conversations": 10, "leads": 2},
            {"channel": "standalone", "conversations": 5, "leads": 1},
        ]
        result = attribution_rules.summary_insight(rows)
        assert result["rule_id"] == "too_early"
        assert result["held"] is True

    def test_summary_insight_double_down(self):
        rows = [
            {"channel": "website", "conversations": 100, "leads": 10},
        ]
        result = attribution_rules.summary_insight(rows)
        assert result["rule_id"] == "double_down"
        assert result["held"] is False
        assert "website" in result["text"].lower()

    def test_summary_insight_worth_a_look(self):
        rows = [
            {"channel": "website", "conversations": 100, "leads": 10},     # 10% rate
            {"channel": "standalone", "conversations": 60, "leads": 30},   # 50% rate -> 5x
        ]
        result = attribution_rules.summary_insight(rows)
        assert result["rule_id"] == "worth_a_look"
        assert "standalone" in result["text"].lower()
        assert result["held"] is False

    def test_channel_rate_held_below_floor(self):
        assert attribution_rules.channel_rate_held(49) is True

    def test_channel_rate_held_at_floor(self):
        assert attribution_rules.channel_rate_held(50) is False

    def test_entry_point_rate_held_below_floor(self):
        assert attribution_rules.entry_point_rate_held(0) is True

    def test_channel_read_below_floor(self):
        result = attribution_rules.channel_read("standalone", 10, 2, 1)
        assert result["rule_id"] == "too_early"
        assert "not enough" in result["text"].lower()

    def test_channel_read_zero_entry_points(self):
        result = attribution_rules.channel_read("campaign", 60, 15, 0)
        assert result["rule_id"] == "mint_prompt"

    def test_channel_read_normal(self):
        result = attribution_rules.channel_read("website", 100, 20, 3)
        assert result["rule_id"] == "channel_summary"
        assert "20" in result["text"]

    def test_channel_suggested_move_too_early(self):
        result = attribution_rules.channel_suggested_move(
            "standalone", 5, 1, 100, 10, 2)
        assert result["tier"] == "too_early"

    def test_channel_suggested_move_mint_prompt(self):
        result = attribution_rules.channel_suggested_move(
            "standalone", 60, 10, 100, 10, 0)
        assert result["rule_id"] == "mint_prompt"

    def test_channel_suggested_move_worth_a_look(self):
        # standalone: 50/60 = 83%; website: 10/100 = 10%; multiple = 8.3x
        result = attribution_rules.channel_suggested_move(
            "standalone", 60, 50, 100, 10, 3)
        assert result["tier"] == "worth_a_look"

    def test_channel_suggested_move_double_down(self):
        # standalone: 5/60 = 8%; website: 10/100 = 10% (below 1.5x threshold)
        result = attribution_rules.channel_suggested_move(
            "standalone", 60, 5, 100, 10, 3)
        assert result["tier"] == "double_down"


# ---------------------------------------------------------------------------
# Old-shape / contract fixture tests (REQUIRED by done-bar)
# ---------------------------------------------------------------------------

class TestOldShapeContractFixture:
    """
    Old-shape C5 rows (missing new attributes) must not crash any reader.
    Cite: FROZEN_CONTRACTS C5 'ALL attributes optional on read (item.get(…))',
    CLAUDE.md Schema Discipline.
    """

    def test_summary_route_old_shape_row_no_crash(self):
        """
        Old-shape summary row must yield a valid 200 response, not an exception.
        """
        old_row = _old_shape_summary_row()
        with (
            patch.object(attribution_api, "_get_agg_item") as mock_get,
            patch.object(attribution_api, "_query_agg_prefix") as mock_query,
        ):
            # First call: current month summary; second call: prior month
            mock_get.side_effect = [old_row, None]
            mock_query.return_value = []

            resp = attribution_api.handle_attribution_summary(
                "T1", {}, _CONFIG_CHICAGO, _cors, "super_admin", _feat_ok,
            )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        # Must have required envelope fields
        assert body["tenant_id"] == "T1"
        assert "funnel" in body
        assert body["funnel"]["conversations"] == 0

    def test_channel_route_old_shape_row_no_crash(self):
        """Old-shape channel row must not crash the channel handler."""
        old_ch = _old_shape_channel_row()
        with (
            patch.object(attribution_api, "_get_agg_item") as mock_get,
            patch.object(attribution_api, "_query_agg_prefix") as mock_query,
            patch.object(attribution_api, "_query_entry_points_registry") as mock_reg,
        ):
            # Main ch row, then 6 trend rows, then website row
            mock_get.side_effect = [old_ch] + [None] * 6 + [None]
            mock_query.return_value = []
            mock_reg.return_value = []

            resp = attribution_api.handle_attribution_channel(
                "T1", "website", {}, _CONFIG_CHICAGO, _cors, "super_admin", _feat_ok,
            )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["tenant_id"] == "T1"
        assert "funnel" in body
        assert body["funnel"]["conversations"] == 0


# ---------------------------------------------------------------------------
# GET /attribution/summary
# ---------------------------------------------------------------------------

class TestSummaryEndpoint:
    def _run(self, params=None, get_side=None, query_side=None,
             feat_fn=_feat_ok, user_role="super_admin", config=None):
        get_side = get_side or []
        query_side = query_side or []
        with (
            patch.object(attribution_api, "_get_agg_item") as mock_get,
            patch.object(attribution_api, "_query_agg_prefix") as mock_query,
        ):
            mock_get.side_effect = get_side
            mock_query.return_value = query_side
            return attribution_api.handle_attribution_summary(
                "T1", params or {}, config or _CONFIG_CHICAGO,
                _cors, user_role, feat_fn,
            )

    def test_flag_off_returns_403(self):
        resp = self._run(feat_fn=_feat_deny)
        assert resp["statusCode"] == 403

    def test_malformed_month_returns_400(self):
        resp = self._run(params={"month": "2026-13"})
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert "month" in body.get("error", "").lower() or "month" in body.get("message", "").lower()

    def test_absent_month_defaults_to_current(self):
        # No month param -> uses current month; just verify 200 returned
        resp = self._run(
            params={},
            get_side=[_summary_row(), None],  # current + prior
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        # month field must be present and be a valid YYYY-MM
        import re
        assert re.match(r"^\d{4}-(?:0[1-9]|1[0-2])$", body["month"])

    def test_response_shape_complete(self):
        resp = self._run(
            params={"month": "2026-05"},
            get_side=[_summary_row(), _summary_row(conversations=150, leads=15)],
            query_side=[_channel_row()],
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])

        # Envelope fields
        assert body["tenant_id"] == "T1"
        assert body["month"] == "2026-05"
        assert body["source"] == "dynamodb"

        # ecosystem
        eco = body["ecosystem"]
        assert "total_conversations" in eco
        assert "after_hours_pct" in eco
        assert isinstance(eco["channels"], list)

        # funnel
        fn = body["funnel"]
        for k in ("reached", "conversations", "engaged", "applications", "leads", "rate"):
            assert k in fn, f"missing funnel key: {k}"

        # time
        tm = body["time"]
        for k in ("after_hours_conversations", "staff_hours", "work_weeks",
                  "self_booked_pct", "median_first_response_minutes"):
            assert k in tm, f"missing time key: {k}"

        # deltas
        assert "deltas" in body and isinstance(body["deltas"], dict)

        # insight
        ins = body["insight"]
        assert "text" in ins and "rule_id" in ins and "held" in ins

    def test_staff_hours_calculation(self):
        """staff_hours = conversation_minutes / 60; work_weeks = staff_hours / 40."""
        resp = self._run(
            params={"month": "2026-05"},
            get_side=[_summary_row(conversation_minutes=2400), None],
        )
        body = json.loads(resp["body"])
        assert body["time"]["staff_hours"] == 40.0  # 2400/60
        assert body["time"]["work_weeks"] == 1.0    # 40/40

    def test_empty_tenant_returns_200_zeros(self):
        """Empty tenant (no rows) -> graceful 200 with all-zero counts."""
        resp = self._run(
            params={"month": "2026-05"},
            get_side=[None, None],
            query_side=[],
        )
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["funnel"]["conversations"] == 0
        assert body["ecosystem"]["total_conversations"] == 0
        # insight held when no channel data
        assert body["insight"]["held"] is True

    def test_below_floor_channel_rate_held_true(self):
        """Channel with < 50 conversations -> rate_held: True in ecosystem.channels."""
        below_floor_ch = _channel_row(conversations=10, leads=2)
        resp = self._run(
            params={"month": "2026-05"},
            get_side=[_summary_row(conversations=10), None],
            query_side=[below_floor_ch],
        )
        body = json.loads(resp["body"])
        ch = body["ecosystem"]["channels"][0]
        assert ch["rate_held"] is True

    def test_channel_above_floor_rate_held_false(self):
        """Channel with >= 50 conversations -> rate_held: False."""
        above_floor_ch = _channel_row(conversations=100, leads=20)
        resp = self._run(
            params={"month": "2026-05"},
            get_side=[_summary_row(), None],
            query_side=[above_floor_ch],
        )
        body = json.loads(resp["body"])
        ch = body["ecosystem"]["channels"][0]
        assert ch["rate_held"] is False

    def test_below_floor_insight_too_early(self):
        """When all channels below floor, insight.rule_id == too_early."""
        below_ch = _channel_row(conversations=5, leads=1)
        resp = self._run(
            params={"month": "2026-05"},
            get_side=[_summary_row(conversations=5), None],
            query_side=[below_ch],
        )
        body = json.loads(resp["body"])
        assert body["insight"]["rule_id"] == "too_early"
        assert body["insight"]["held"] is True


# ---------------------------------------------------------------------------
# GET /attribution/channels/{channel}
# ---------------------------------------------------------------------------

class TestChannelEndpoint:
    def _run(self, channel="website", params=None, get_side=None, query_side=None,
             reg_side=None, feat_fn=_feat_ok, user_role="super_admin"):
        get_side = get_side or []
        with (
            patch.object(attribution_api, "_get_agg_item") as mock_get,
            patch.object(attribution_api, "_query_agg_prefix") as mock_query,
            patch.object(attribution_api, "_query_entry_points_registry") as mock_reg,
        ):
            mock_get.side_effect = get_side
            mock_query.return_value = query_side or []
            mock_reg.return_value = reg_side or []
            return attribution_api.handle_attribution_channel(
                "T1", channel, params or {"month": "2026-05"},
                _CONFIG_CHICAGO, _cors, user_role, feat_fn,
            )

    def test_flag_off_returns_403(self):
        resp = self._run(feat_fn=_feat_deny)
        assert resp["statusCode"] == 403

    def test_invalid_channel_returns_400(self):
        resp = self._run(channel="bogus")
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert "channel" in body.get("error", "").lower()

    def test_malformed_month_returns_400(self):
        resp = self._run(params={"month": "2026-00"})
        assert resp["statusCode"] == 400

    def test_response_shape_complete(self):
        ch = _channel_row(
            channel="website", conversations=100, leads=20,
            topic_counts={"Volunteer": 30, "Events": 20},
            resource_clicks={"https://example.com": 5},
            reach=300,
        )
        # get_agg_item calls: [main ch_row, 6 trend months, website website comparison row]
        trend_rows = [_channel_row(conversations=50, leads=5)] * 6
        website_comp = _channel_row(channel="website", conversations=100, leads=10)
        get_side = [ch] + trend_rows + [website_comp]
        resp = self._run(get_side=get_side)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])

        assert body["tenant_id"] == "T1"
        assert body["month"] == "2026-05"
        assert body["source"] == "dynamodb"

        fn = body["funnel"]
        for k in ("reached", "conversations", "engaged", "applications", "leads", "rate"):
            assert k in fn, f"missing funnel key: {k}"

        assert isinstance(body["entry_points"], list)
        assert isinstance(body["topics"], list)
        assert isinstance(body["resources"], list)
        assert len(body["trend"]) == 6
        for t in body["trend"]:
            assert "month" in t and "conversations" in t and "leads" in t

        assert "text" in body["read"] and "rule_id" in body["read"]
        sm = body["suggested_move"]
        assert "text" in sm and "rule_id" in sm and "tier" in sm

    def test_below_floor_read_too_early(self):
        ch = _channel_row(channel="standalone", conversations=5, leads=1)
        get_side = [ch] + [None] * 7
        resp = self._run(channel="standalone", get_side=get_side)
        body = json.loads(resp["body"])
        assert body["read"]["rule_id"] == "too_early"

    def test_empty_tenant_200_zeros(self):
        get_side = [None] * 8
        resp = self._run(get_side=get_side)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["funnel"]["conversations"] == 0


# ---------------------------------------------------------------------------
# GET /attribution/entry-points
# ---------------------------------------------------------------------------

class TestEntryPointsListEndpoint:
    def _run(self, records=None, feat_fn=_feat_ok, user_role="super_admin"):
        with patch.object(attribution_api, "_query_entry_points_registry") as mock_reg:
            mock_reg.return_value = records or []
            return attribution_api.handle_attribution_entry_points_list(
                "T1", _cors, user_role, feat_fn,
            )

    def test_flag_off_returns_403(self):
        resp = self._run(feat_fn=_feat_deny)
        assert resp["statusCode"] == 403

    def test_empty_returns_200_empty_list(self):
        resp = self._run()
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["entry_points"] == []

    def test_returns_c3_records_verbatim(self):
        records = [
            {"tenant_id": "T1", "entry_point_id": "ep_ABC123456789",
             "label": "Gala Tent", "channel": "standalone",
             "dub_short_link": "https://myrctr.link/gala-tents",
             "status": "active", "created_at": "2026-05-01T00:00:00Z"},
        ]
        resp = self._run(records=records)
        body = json.loads(resp["body"])
        assert body["tenant_id"] == "T1"
        assert body["source"] == "dynamodb"
        assert len(body["entry_points"]) == 1
        assert body["entry_points"][0]["label"] == "Gala Tent"


# ---------------------------------------------------------------------------
# POST /attribution/entry-points (mint proxy)
# ---------------------------------------------------------------------------

class TestMintEndpoint:
    _VALID_BODY = {
        "label": "Gala Tent",
        "channel": "standalone",
        "campaign": "Summer2026",
        "placement": "lobby",
        "target": {"type": "standalone_chat", "url": "https://chat.myrecruiter.ai"},
    }

    def _event(self, body_dict):
        return {"body": json.dumps(body_dict)}

    def _run(self, body_dict=None, invoke_return=None, feat_fn=_feat_ok,
             user_role="super_admin"):
        with patch.object(attribution_api, "_invoke_mint") as mock_inv:
            mock_inv.return_value = invoke_return or (201, {
                "ok": True,
                "entry_point": {
                    "entry_point_id": "ep_01HZABC12345678901234567",
                    "short_link": "https://myrctr.link/gala-tents",
                    "qr_url": "https://api.dub.co/qr?url=https://myrctr.link/gala-tents&size=1000&level=H",
                    "destination_url": "https://chat.myrecruiter.ai?ep=ep_01HZABC12345678901234567",
                    "dub_link_id": "dub123",
                    "created_at": "2026-06-01T00:00:00Z",
                },
            })
            return attribution_api.handle_attribution_mint(
                "T1", self._event(body_dict or self._VALID_BODY),
                _cors, user_role, feat_fn,
            )

    def test_flag_off_returns_403(self):
        resp = self._run(feat_fn=_feat_deny)
        assert resp["statusCode"] == 403

    def test_success_returns_201_with_entry_point(self):
        resp = self._run()
        assert resp["statusCode"] == 201
        body = json.loads(resp["body"])
        assert body["ok"] is True
        assert "entry_point" in body

    def test_suffix_taken_returns_409(self):
        resp = self._run(invoke_return=(409, {"ok": False, "error": {"code": "SUFFIX_TAKEN", "message": "taken"}}))
        assert resp["statusCode"] == 409

    def test_dub_error_returns_502(self):
        resp = self._run(invoke_return=(502, {"ok": False, "error": {"code": "DUB_ERROR", "message": "dub error"}}))
        assert resp["statusCode"] == 502

    def test_validation_error_returns_400_from_downstream(self):
        resp = self._run(invoke_return=(400, {"ok": False, "error": {"code": "VALIDATION", "message": "bad"}}))
        assert resp["statusCode"] == 400

    def test_conflict_returns_409(self):
        resp = self._run(invoke_return=(409, {"ok": False, "error": {"code": "CONFLICT", "message": "conflict"}}))
        assert resp["statusCode"] == 409

    def test_mint_function_name_unset_returns_503(self):
        orig = os.environ.get("MINT_FUNCTION_NAME", "")
        try:
            os.environ["MINT_FUNCTION_NAME"] = ""
            # Reset module-level cache
            attribution_api.MINT_FUNCTION_NAME = ""
            # Don't patch _invoke_mint — let it run with empty name
            resp = attribution_api.handle_attribution_mint(
                "T1", self._event(self._VALID_BODY),
                _cors, "super_admin", _feat_ok,
            )
            assert resp["statusCode"] == 503
        finally:
            os.environ["MINT_FUNCTION_NAME"] = orig
            attribution_api.MINT_FUNCTION_NAME = orig

    def test_invalid_json_body_returns_400(self):
        event = {"body": "not-json"}
        resp = attribution_api.handle_attribution_mint(
            "T1", event, _cors, "super_admin", _feat_ok,
        )
        assert resp["statusCode"] == 400

    def test_missing_required_field_returns_400(self):
        bad_body = dict(self._VALID_BODY)
        del bad_body["label"]
        resp = self._run(body_dict=bad_body)
        # _validate_mint_body returns error -> 400 before invoke
        assert resp["statusCode"] == 400

    def test_at_sign_in_label_returns_400(self):
        bad_body = dict(self._VALID_BODY)
        bad_body["label"] = "user@example.com"
        resp = self._run(body_dict=bad_body)
        assert resp["statusCode"] == 400

    def test_invalid_channel_returns_400(self):
        bad_body = dict(self._VALID_BODY)
        bad_body["channel"] = "website"  # not allowed in mint (standalone|campaign only)
        resp = self._run(body_dict=bad_body)
        assert resp["statusCode"] == 400

    def test_http_url_rejected(self):
        bad_body = dict(self._VALID_BODY)
        bad_body["target"] = {"type": "site_url", "url": "http://example.com"}
        resp = self._run(body_dict=bad_body)
        assert resp["statusCode"] == 400

    def test_tenant_id_from_jwt_not_client(self):
        """C8.14: tenant_id injected from JWT; any client-supplied value is stripped."""
        body_with_tenant = dict(self._VALID_BODY)
        body_with_tenant["tenant_id"] = "EVIL_TENANT"
        resp = self._run(body_dict=body_with_tenant)
        # Should succeed (200/201); the evil tenant_id is stripped
        assert resp["statusCode"] == 201
        body = json.loads(resp["body"])
        assert body.get("tenant_id") == "T1"  # from JWT, not client

    # Fix 3: target allow-list
    def test_extra_target_keys_not_forwarded(self):
        """Fix 3: extra keys in target dict must not be forwarded to WS-B (mass-assignment guard)."""
        import unittest.mock
        body_with_extra = dict(self._VALID_BODY)
        body_with_extra["target"] = {
            "type": "standalone_chat",
            "url": "https://chat.myrecruiter.ai",
            "extra_key": "evil_value",
            "admin": True,
        }
        captured = {}
        original_invoke = attribution_api._invoke_mint

        def capture_payload(payload):
            captured["payload"] = payload
            return 201, {
                "ok": True,
                "entry_point": {
                    "entry_point_id": "ep_01HZABC12345678901234567",
                    "short_link": "https://myrctr.link/t",
                    "qr_url": "https://api.dub.co/qr?url=https://myrctr.link/t&size=1000&level=H",
                    "destination_url": "https://chat.myrecruiter.ai?ep=ep_01HZABC12345678901234567",
                    "dub_link_id": "d1",
                    "created_at": "2026-06-01T00:00:00Z",
                },
            }

        with unittest.mock.patch.object(attribution_api, "_invoke_mint", side_effect=capture_payload):
            resp = attribution_api.handle_attribution_mint(
                "T1", {"body": json.dumps(body_with_extra)},
                _cors, "super_admin", _feat_ok,
            )
        assert resp["statusCode"] == 201
        forwarded_target = captured["payload"]["target"]
        assert set(forwarded_target.keys()) == {"type", "url"}, (
            f"target forwarded unexpected keys: {set(forwarded_target.keys()) - {'type','url'}}"
        )

    # Fix 4: suffix validation
    def test_suffix_too_long_returns_400(self):
        """Fix 4: suffix > 190 chars must be rejected with 400."""
        bad_body = dict(self._VALID_BODY)
        bad_body["suffix"] = "a" * 191
        resp = self._run(body_dict=bad_body)
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert "suffix" in body.get("message", "").lower()

    def test_suffix_bad_charset_returns_400(self):
        """Fix 4: suffix with disallowed chars must be rejected with 400."""
        bad_body = dict(self._VALID_BODY)
        bad_body["suffix"] = "bad suffix!"  # space + ! not allowed
        resp = self._run(body_dict=bad_body)
        assert resp["statusCode"] == 400

    def test_suffix_valid_accepted(self):
        """Fix 4: valid suffix (letters, digits, dots, underscores, hyphens) passes."""
        body_with_suffix = dict(self._VALID_BODY)
        body_with_suffix["suffix"] = "gala-tents_2026.v2"
        resp = self._run(body_dict=body_with_suffix)
        assert resp["statusCode"] == 201

    def test_suffix_at_190_chars_accepted(self):
        """Fix 4: suffix exactly 190 chars is accepted."""
        body_with_suffix = dict(self._VALID_BODY)
        body_with_suffix["suffix"] = "a" * 190
        resp = self._run(body_dict=body_with_suffix)
        assert resp["statusCode"] == 201

    # Fix 5: target.url query-param restriction
    def test_target_url_with_non_utm_param_rejected(self):
        """Fix 5 / C8.15: target.url with a non-utm_ query param must be rejected."""
        bad_body = dict(self._VALID_BODY)
        bad_body["target"] = {"type": "site_url", "url": "https://example.com?email=user@test.com"}
        resp = self._run(body_dict=bad_body)
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert "utm_" in body.get("message", "").lower() or "query" in body.get("message", "").lower()

    def test_target_url_with_utm_param_accepted(self):
        """Fix 5: target.url with only utm_ params is accepted."""
        body_with_utm = dict(self._VALID_BODY)
        body_with_utm["target"] = {
            "type": "site_url",
            "url": "https://example.com?utm_source=qr&utm_campaign=gala",
        }
        resp = self._run(body_dict=body_with_utm)
        assert resp["statusCode"] == 201

    def test_target_url_with_ep_param_rejected(self):
        """Fix 5: ep= param is appended by the service, never client-supplied — must be rejected."""
        bad_body = dict(self._VALID_BODY)
        bad_body["target"] = {"type": "site_url", "url": "https://example.com?ep=ep_ABCDEFGH12345678"}
        resp = self._run(body_dict=bad_body)
        assert resp["statusCode"] == 400


# ---------------------------------------------------------------------------
# Fix 6: DDB ClientError -> clean 502 (no operation detail leak)
# ---------------------------------------------------------------------------

class TestDDBErrorHandling:
    """Fix 6: DDB ClientErrors in handle_attribution_summary and handle_attribution_channel
    must yield clean 502 responses without leaking DDB operation details."""

    def test_summary_ddb_error_returns_502_no_detail(self):
        """ClientError in _get_agg_item during summary handler -> 502, no str(e) in body."""
        from botocore.exceptions import ClientError as BotoClientError
        err = BotoClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "too much"}},
            "GetItem",
        )
        with patch.object(attribution_api, "_get_agg_item", side_effect=err):
            resp = attribution_api.handle_attribution_summary(
                "T1", {"month": "2026-05"}, _CONFIG_CHICAGO, _cors, "super_admin", _feat_ok,
            )
        assert resp["statusCode"] == 502
        body = json.loads(resp["body"])
        # Must NOT echo DDB error message or operation name
        body_str = json.dumps(body)
        assert "ProvisionedThroughputExceededException" not in body_str
        assert "GetItem" not in body_str
        assert "too much" not in body_str
        # Must have the standard error key
        assert "error" in body

    def test_channel_ddb_error_returns_502_no_detail(self):
        """ClientError in _get_agg_item during channel handler -> 502, no str(e) in body."""
        from botocore.exceptions import ClientError as BotoClientError
        err = BotoClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException", "Message": "too much"}},
            "GetItem",
        )
        with patch.object(attribution_api, "_get_agg_item", side_effect=err):
            resp = attribution_api.handle_attribution_channel(
                "T1", "website", {"month": "2026-05"}, _CONFIG_CHICAGO,
                _cors, "super_admin", _feat_ok,
            )
        assert resp["statusCode"] == 502
        body = json.loads(resp["body"])
        body_str = json.dumps(body)
        assert "ProvisionedThroughputExceededException" not in body_str
        assert "GetItem" not in body_str
        assert "error" in body

    def test_summary_query_ddb_error_returns_502(self):
        """ClientError in _query_agg_prefix during summary handler -> 502."""
        from botocore.exceptions import ClientError as BotoClientError
        err = BotoClientError(
            {"Error": {"Code": "ResourceNotFoundException", "Message": "table not found"}},
            "Query",
        )
        with (
            patch.object(attribution_api, "_get_agg_item") as mock_get,
            patch.object(attribution_api, "_query_agg_prefix", side_effect=err),
        ):
            mock_get.return_value = None
            resp = attribution_api.handle_attribution_summary(
                "T1", {"month": "2026-05"}, _CONFIG_CHICAGO, _cors, "super_admin", _feat_ok,
            )
        assert resp["statusCode"] == 502
        body = json.loads(resp["body"])
        assert "ResourceNotFoundException" not in json.dumps(body)


# ---------------------------------------------------------------------------
# Fix 7: Month sanity ceiling
# ---------------------------------------------------------------------------

class TestMonthCeiling:
    """Fix 7: months more than 1 calendar month in the future are rejected with 400."""

    def _run_summary(self, month: str):
        """Run summary handler with given month param; no DDB calls needed."""
        with (
            patch.object(attribution_api, "_get_agg_item", return_value=None),
            patch.object(attribution_api, "_query_agg_prefix", return_value=[]),
        ):
            return attribution_api.handle_attribution_summary(
                "T1", {"month": month}, _CONFIG_CHICAGO, _cors, "super_admin", _feat_ok,
            )

    def test_far_future_month_rejected(self):
        """A month 2 years in the future must be rejected."""
        resp = self._run_summary("2099-01")
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert "range" in body.get("error", "").lower() or "future" in body.get("message", "").lower()

    def test_far_future_channel_rejected(self):
        """Channel endpoint: far-future month rejected."""
        with (
            patch.object(attribution_api, "_get_agg_item", return_value=None),
            patch.object(attribution_api, "_query_agg_prefix", return_value=[]),
            patch.object(attribution_api, "_query_entry_points_registry", return_value=[]),
        ):
            resp = attribution_api.handle_attribution_channel(
                "T1", "website", {"month": "2099-12"}, _CONFIG_CHICAGO,
                _cors, "super_admin", _feat_ok,
            )
        assert resp["statusCode"] == 400

    def test_current_month_accepted(self):
        """Current month must always be accepted."""
        from attribution_api import _current_month_tenant_local
        current = _current_month_tenant_local(_CONFIG_CHICAGO)
        resp = self._run_summary(current)
        assert resp["statusCode"] == 200

    def test_next_month_accepted(self):
        """One month in the future (the ceiling) must be accepted."""
        from attribution_api import _current_month_tenant_local, _next_month
        current = _current_month_tenant_local(_CONFIG_CHICAGO)
        ceiling = _next_month(current)
        resp = self._run_summary(ceiling)
        assert resp["statusCode"] == 200

    def test_two_months_ahead_rejected(self):
        """Two months in the future must be rejected."""
        from attribution_api import _current_month_tenant_local, _next_month
        current = _current_month_tenant_local(_CONFIG_CHICAGO)
        two_ahead = _next_month(_next_month(current))
        resp = self._run_summary(two_ahead)
        assert resp["statusCode"] == 400


# ---------------------------------------------------------------------------
# _deserialize_item unit test
# ---------------------------------------------------------------------------

class TestDeserializeItem:
    def test_string_and_number(self):
        raw = {"label": {"S": "test"}, "count": {"N": "42"}}
        out = attribution_api._deserialize_item(raw)
        assert out["label"] == "test"
        assert out["count"] == 42

    def test_float_number(self):
        raw = {"rate": {"N": "0.25"}}
        out = attribution_api._deserialize_item(raw)
        assert out["rate"] == 0.25

    def test_bool_and_null(self):
        raw = {"active": {"BOOL": True}, "deleted": {"NULL": True}}
        out = attribution_api._deserialize_item(raw)
        assert out["active"] is True
        assert out["deleted"] is None

    def test_map(self):
        raw = {"topics": {"M": {"Volunteer": {"N": "10"}, "Events": {"N": "5"}}}}
        out = attribution_api._deserialize_item(raw)
        assert out["topics"] == {"Volunteer": 10, "Events": 5}

    def test_empty_dict(self):
        assert attribution_api._deserialize_item({}) == {}


# ---------------------------------------------------------------------------
# _prior_month helper
# ---------------------------------------------------------------------------

class TestPriorMonth:
    def test_normal_month(self):
        assert attribution_api._prior_month("2026-05") == "2026-04"

    def test_january_wraps_to_december(self):
        assert attribution_api._prior_month("2026-01") == "2025-12"


# --- Regression: aggregator writes metrics under a `data` Map; the reader must
# hoist them to top level or the dashboard shows the empty state (2026-07-18). ---

def test_hoist_data_lifts_nested_metrics_to_top_level():
    raw = {
        "pk": {"S": "TENANT#T1"},
        "sk": {"S": "METRIC#attribution_summary#2026-07"},
        "updated_at": {"S": "2026-07-18T00:00:00Z"},
        "ttl": {"N": "123"},
        "data": {"M": {
            "conversations": {"N": "42"},
            "leads": {"N": "7"},
            "self_booked_pct": {"NULL": True},
            "topic_counts": {"M": {"Volunteer": {"N": "10"}, "Donation": {"N": "5"}}},
            "reach": {"M": {"scans": {"N": "3"}, "clicks": {"N": "0"}}},
        }},
    }
    item = attribution_api._deserialize_item(attribution_api._hoist_data(raw))
    assert int(item.get("conversations", 0)) == 42          # top-level readable
    assert int(item.get("leads", 0)) == 7
    assert item.get("self_booked_pct") is None
    assert item.get("topic_counts") == {"Volunteer": 10, "Donation": 5}  # nested Map intact
    assert item.get("reach") == {"scans": 3, "clicks": 0}


def test_hoist_data_noop_on_top_level_rows():
    raw = {"pk": {"S": "TENANT#T1"}, "sk": {"S": "x"}, "conversations": {"N": "9"}}
    item = attribution_api._deserialize_item(attribution_api._hoist_data(raw))
    assert int(item.get("conversations", 0)) == 9


def test_hoist_data_prefers_data_on_conflict():
    raw = {"conversations": {"N": "1"}, "data": {"M": {"conversations": {"N": "99"}}}}
    item = attribution_api._deserialize_item(attribution_api._hoist_data(raw))
    assert int(item.get("conversations", 0)) == 99
