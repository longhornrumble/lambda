#!/usr/bin/env python3
"""
Operator re-push script — Messenger welcome surfaces (M5).

The Config Builder UI can't trigger a welcome-surface push yet (that's a
separate future project); until then, an operator who edits
`messenger_behavior.welcome` in a tenant's S3 config after the channel was
already connected re-pushes it manually with this script.

Resolves the tenant's channel-mapping row (PAGE#<id> / CHANNEL#<type>) via
the TenantIndex GSI, decrypts the stored Page access token with KMS, loads
the tenant config from S3, and pushes ice breakers + persistent menu to the
Messenger Profile API — reusing `push_welcome_surfaces()` from
lambda_function.py (the exact same code path the OAuth callback uses), so
behavior never drifts between connect-time and re-push.

Dry-run by default: prints the exact profile payload without calling Graph.
Pass --execute to actually push.

Requires operator AWS credentials (SSO) with read access to the channel
mappings table, the KMS key, and the config bucket — same account as the
tenant's channel mapping (staging 525 or prod 614).

Usage:
    python repush_welcome_surfaces.py TENANT_ID [--channel messenger|instagram] [--execute]

Dependencies: boto3, PyJWT (lambda_function.py imports jwt at module load
even though this script's code path never uses it).
"""

import argparse
import json
import os
import sys

_HERE = os.path.dirname(os.path.abspath(__file__))
_HANDLER_DIR = os.path.dirname(_HERE)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("tenant_id", help="Tenant ID whose channel mapping to re-push")
    parser.add_argument(
        "--channel", choices=["messenger", "instagram"], default="messenger",
        help="Channel row to resolve the page/IG-account id + token from (default: messenger)",
    )
    parser.add_argument(
        "--execute", action="store_true",
        help="Actually push to the Messenger Profile API (default: dry-run, payload only)",
    )
    parser.add_argument(
        "--bucket", default=os.environ.get("CONFIG_BUCKET", ""),
        help="S3 config bucket (default: CONFIG_BUCKET env var)",
    )
    parser.add_argument(
        "--table", default=os.environ.get("CHANNEL_MAPPINGS_TABLE", "picasso-channel-mappings"),
        help="Channel mappings DynamoDB table (default: CHANNEL_MAPPINGS_TABLE env var)",
    )
    parser.add_argument(
        "--kms-key-id", default=os.environ.get("KMS_KEY_ID", "alias/picasso-channel-tokens"),
        help="KMS key alias/ARN used to decrypt the stored Page access token",
    )
    args = parser.parse_args()

    if not args.bucket:
        print("[ERROR] --bucket or CONFIG_BUCKET env var is required", file=sys.stderr)
        return 1

    # lambda_function.py reads these into module-level constants at import
    # time — set them before importing.
    os.environ["CONFIG_BUCKET"] = args.bucket
    os.environ["CHANNEL_MAPPINGS_TABLE"] = args.table
    os.environ["KMS_KEY_ID"] = args.kms_key_id

    sys.path.insert(0, _HANDLER_DIR)
    import lambda_function as lf  # noqa: E402  (path must be set first)

    channels = lf._query_channels_by_tenant(args.tenant_id)
    matches = [c for c in channels if c.get("channelType") == args.channel]
    if not matches:
        print(
            f"[ERROR] No {args.channel} channel mapping found for tenant_id={args.tenant_id} "
            f"in table {args.table}",
            file=sys.stderr,
        )
        return 1

    channel = matches[0]
    account_id = channel.get("pageId") or channel.get("igAccountId", "")
    encrypted_token = channel.get("encryptedPageToken", "")
    if not encrypted_token:
        print(f"[ERROR] Channel mapping has no encryptedPageToken for tenant_id={args.tenant_id}", file=sys.stderr)
        return 1

    page_token = lf._decrypt_token(encrypted_token)

    # Preview: build the exact payload without calling Graph (dry-run default).
    config = lf._load_tenant_config_for_welcome(args.tenant_id)
    if config is None:
        print(f"[ERROR] Could not load tenant config for tenant_id={args.tenant_id} from s3://{args.bucket}", file=sys.stderr)
        return 1

    if (config.get("feature_flags") or {}).get("MESSENGER_CHANNEL") is not True:
        print(f"[WARN] MESSENGER_CHANNEL flag is not enabled for tenant_id={args.tenant_id} — push_welcome_surfaces() will skip.")

    welcome = (config.get("messenger_behavior") or {}).get("welcome") or {}
    payload, ice_count, menu_count = lf._build_welcome_profile_payload(welcome, args.tenant_id)

    print(f"--- {args.channel} channel: account_id={account_id}, tenant_id={args.tenant_id} ---")
    print(json.dumps(payload, indent=2))
    print(f"\n{ice_count} ice breaker(s), {menu_count} persistent menu item(s).")

    if not args.execute:
        print("\nDry run — no Graph call made. Re-run with --execute to push.")
        return 0

    result = lf.push_welcome_surfaces(page_token, args.tenant_id)
    print(f"\n[RESULT] {result}")
    return 1 if "error" in result else 0


if __name__ == "__main__":
    sys.exit(main())
