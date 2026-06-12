"""
Attribution Recommendations Rule Pack — WS-D
==============================================
Pure functions over C5 aggregate rows.  No I/O; no global state.
Single source for both /attribution/summary insight and
/attribution/channels/{channel} read + suggested_move fields.

Cite: FROZEN_CONTRACTS.md C6 rule pack v1, C7 definitions.

C7 confidence floor
-------------------
n >= 50 conversations per channel / per entry point before ANY rate
comparison or insight.  Below floor -> rate_held: True, rules emit
too_early only.

Tiers
-----
  double_down   — best-rate channel above floor
  worth_a_look  — channel rate >= 1.5x website rate (both above floor)
  too_early     — entry point or channel below floor
  (no tier)     — mint prompt (channel with zero entry points)
"""

from __future__ import annotations

from typing import Any, Optional

# C7 — embed verbatim; cite FROZEN_CONTRACTS C7
CONFIDENCE_FLOOR = 50  # conversations


def _rate(leads: int, conversations: int) -> float:
    """Lead conversion rate (0.0–1.0). Zero when no conversations."""
    if conversations <= 0:
        return 0.0
    return leads / conversations


def _above_floor(conversations: int) -> bool:
    return conversations >= CONFIDENCE_FLOOR


# ---------------------------------------------------------------------------
# Per-channel helpers used by summary insight + channel read/suggested_move
# ---------------------------------------------------------------------------

def channel_rate_held(conversations: int) -> bool:
    """True when channel is below the C7 confidence floor."""
    return not _above_floor(conversations)


def entry_point_rate_held(conversations: int) -> bool:
    """True when an entry-point row is below the C7 confidence floor."""
    return not _above_floor(conversations)


# ---------------------------------------------------------------------------
# Summary-level: single best insight across all channels
# ---------------------------------------------------------------------------

def summary_insight(
    channel_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Given a list of C5 attribution_channel rows (current month), return the
    single highest-priority insight for the /attribution/summary response.

    Each row must have: channel, conversations (int), leads (int).
    Returns: {text, rule_id, held}

    Priority (first match wins):
    1. A channel with rate >= 1.5x website rate (both above floor) -> worth_a_look
    2. Best-rate channel above floor -> double_down
    3. Any channel below floor -> too_early
    4. Fallback (no data) -> held=True
    """
    if not channel_rows:
        return {"text": "No channel data available yet.", "rule_id": "no_data", "held": True}

    website_row = next(
        (r for r in channel_rows if r.get("channel") == "website"), None
    )
    website_convs = int((website_row or {}).get("conversations", 0) if website_row else 0)
    website_leads = int((website_row or {}).get("leads", 0) if website_row else 0)
    website_rate = _rate(website_leads, website_convs)
    website_above = _above_floor(website_convs)

    # Collect non-website channels above floor
    above_floor_non_website = [
        r for r in channel_rows
        if r.get("channel") != "website"
        and _above_floor(int(r.get("conversations", 0)))
    ]

    # Rule: channel rate >= 1.5x website (both above floor)
    if website_above and above_floor_non_website:
        best_multiple: Optional[float] = None
        best_row: Optional[dict[str, Any]] = None
        for r in above_floor_non_website:
            c = int(r.get("conversations", 0))
            l = int(r.get("leads", 0))
            r_rate = _rate(l, c)
            if website_rate > 0:
                multiple = r_rate / website_rate
                if multiple >= 1.5 and (best_multiple is None or multiple > best_multiple):
                    best_multiple = multiple
                    best_row = r
        if best_row is not None and best_multiple is not None:
            ch = best_row.get("channel", "")
            return {
                "text": (
                    f"{ch.title()} converts at {best_multiple:.1f}x your website rate — "
                    "consider expanding this channel."
                ),
                "rule_id": "worth_a_look",
                "held": False,
            }

    # Rule: best-rate channel above floor -> double_down
    all_above = [
        r for r in channel_rows
        if _above_floor(int(r.get("conversations", 0)))
    ]
    if all_above:
        best = max(
            all_above,
            key=lambda r: _rate(int(r.get("leads", 0)), int(r.get("conversations", 0))),
        )
        c = int(best.get("conversations", 0))
        l = int(best.get("leads", 0))
        ch = best.get("channel", "")
        rate_pct = round(_rate(l, c) * 100, 1)
        return {
            "text": (
                f"{ch.title()} has your best conversion rate ({rate_pct}%) — "
                "double down here."
            ),
            "rule_id": "double_down",
            "held": False,
        }

    # Rule: all channels below floor -> too_early
    return {
        "text": "Not enough data yet — keep running and check back next month.",
        "rule_id": "too_early",
        "held": True,
    }


# ---------------------------------------------------------------------------
# Channel-level: read (narrative) + suggested_move
# ---------------------------------------------------------------------------

def channel_read(
    channel: str,
    conversations: int,
    leads: int,
    entry_point_count: int,
) -> dict[str, str]:
    """
    Narrative read for a single channel, used in GET /attribution/channels/{channel}.
    Returns {text, rule_id}.
    """
    if not _above_floor(conversations):
        return {
            "text": "Leave them running — not enough data yet to draw conclusions.",
            "rule_id": "too_early",
        }
    rate_pct = round(_rate(leads, conversations) * 100, 1)
    if entry_point_count == 0:
        return {
            "text": (
                f"{channel.title()} has a {rate_pct}% conversion rate but no minted entry points. "
                "Create a short link to start tracking reach."
            ),
            "rule_id": "mint_prompt",
        }
    return {
        "text": (
            f"{channel.title()} converted {leads} of {conversations} conversations "
            f"({rate_pct}%) this month."
        ),
        "rule_id": "channel_summary",
    }


def channel_suggested_move(
    channel: str,
    conversations: int,
    leads: int,
    website_conversations: int,
    website_leads: int,
    entry_point_count: int,
) -> dict[str, Any]:
    """
    Suggested move for a single channel, used in GET /attribution/channels/{channel}.
    Returns {text, rule_id, tier}.
    """
    # Below floor
    if not _above_floor(conversations):
        return {
            "text": "Leave them running — not enough data to suggest a move yet.",
            "rule_id": "too_early",
            "tier": "too_early",
        }

    # Zero entry points -> mint prompt
    if entry_point_count == 0:
        return {
            "text": (
                f"Mint a short link for {channel.title()} to start measuring reach "
                "and connect scans/clicks to conversations."
            ),
            "rule_id": "mint_prompt",
            "tier": "worth_a_look",
        }

    r_channel = _rate(leads, conversations)
    r_website = _rate(website_leads, website_conversations)
    website_above = _above_floor(website_conversations)

    # worth_a_look: channel rate >= 1.5x website (both above floor)
    if website_above and r_website > 0 and r_channel >= 1.5 * r_website:
        multiple = r_channel / r_website
        return {
            "text": (
                f"{channel.title()} converts at {multiple:.1f}x your website rate. "
                "Scale up placements here."
            ),
            "rule_id": "worth_a_look",
            "tier": "worth_a_look",
        }

    # double_down: best effort for above-floor channels
    rate_pct = round(r_channel * 100, 1)
    return {
        "text": (
            f"This channel is performing at {rate_pct}%. "
            "Keep current placements and review entry point copy."
        ),
        "rule_id": "double_down",
        "tier": "double_down",
    }
