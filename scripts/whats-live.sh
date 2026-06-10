#!/bin/bash
# whats-live.sh — one command answering "what code is live where" for the
# CI-managed Lambdas (CI-modernization Phase 1.5, minimal read-side form —
# no manifest infra: the publish-version description already encodes
# "PROD: <commit msg> (<sha>)" for CI deploys, and the MFS `live` alias is
# the prod traffic pointer).
#
# Usage:
#   ./scripts/whats-live.sh            # prod (614) + staging (525)
#   ./scripts/whats-live.sh prod      # prod only
#   ./scripts/whats-live.sh staging   # staging only
#
# Profiles: override via PROD_PROFILE / STAGING_PROFILE env vars.
set -euo pipefail

PROD_PROFILE="${PROD_PROFILE:-chris-admin}"
STAGING_PROFILE="${STAGING_PROFILE:-myrecruiter-staging}"
SCOPE="${1:-all}"

# Stale exported creds override AWS_PROFILE (root CLAUDE.md hard rule).
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN

show_fn() {
  local profile="$1" fn="$2" alias_name="$3"
  local cfg sha mod
  cfg=$(aws lambda get-function-configuration --function-name "$fn" \
        --profile "$profile" \
        --query '{Sha:CodeSha256,Mod:LastModified}' --output text 2>/dev/null) || {
    echo "  $fn: (not found / no access)"; return; }
  sha=$(cut -f2 <<<"$cfg"); mod=$(cut -f1 <<<"$cfg")
  # Latest published version's description carries the deploy note.
  local note
  note=$(aws lambda list-versions-by-function --function-name "$fn" \
         --profile "$profile" \
         --query 'Versions[-1].{V:Version,D:Description}' --output text 2>/dev/null | tr '\t' ' ')
  echo "  $fn"
  echo "    \$LATEST: sha=${sha:0:12}… modified=$mod"
  echo "    newest version: $note"
  if [ -n "$alias_name" ]; then
    local av asha
    av=$(aws lambda get-alias --function-name "$fn" --name "$alias_name" \
         --profile "$profile" --query FunctionVersion --output text 2>/dev/null) || av="(none)"
    if [ "$av" != "(none)" ]; then
      asha=$(aws lambda get-function-configuration --function-name "$fn:$av" \
             --profile "$profile" --query CodeSha256 --output text 2>/dev/null)
      local match="≠ \$LATEST ⚠️ (alias traffic is NOT running latest code)"
      [ "$asha" = "$sha" ] && match="= \$LATEST ✓"
      echo "    TRAFFIC → alias '$alias_name' = v$av sha=${asha:0:12}… $match"
    else
      echo "    alias '$alias_name': not found"
    fi
  fi
}

if [ "$SCOPE" = "all" ] || [ "$SCOPE" = "prod" ]; then
  echo "PRODUCTION (614, profile=$PROD_PROFILE)"
  show_fn "$PROD_PROFILE" Analytics_Dashboard_API ""
  show_fn "$PROD_PROFILE" Master_Function "live"
  show_fn "$PROD_PROFILE" Bedrock_Streaming_Handler ""
fi

if [ "$SCOPE" = "all" ] || [ "$SCOPE" = "staging" ]; then
  echo "STAGING (525, profile=$STAGING_PROFILE)"
  show_fn "$STAGING_PROFILE" Analytics_Dashboard_API ""
  show_fn "$STAGING_PROFILE" Master_Function_Staging ""
  show_fn "$STAGING_PROFILE" Bedrock_Streaming_Handler_Staging ""
fi
