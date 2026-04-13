#!/usr/bin/env python3
"""
One-time backfill script for tenant registry.
Populates picasso-tenant-registry-{env} from existing S3 configs and known Clerk org mappings.

Usage:
    python backfill_tenant_registry.py                  # Dry run (default)
    python backfill_tenant_registry.py --execute        # Actually write to DynamoDB
    python backfill_tenant_registry.py --verify         # Verify registry matches S3

Environment variables:
    ENVIRONMENT  - staging (default) or production
    AWS_REGION   - us-east-1 (default)
    AWS_PROFILE  - AWS profile to use (default: chris-admin)
"""

import boto3
import json
import os
import sys
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# Configuration
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
S3_BUCKET = 'myrecruiter-picasso'
TABLE_NAME = f"picasso-tenant-registry-{ENVIRONMENT}"

# Known tenant data — 3 active clients
# Clerk org IDs from existing Clerk setup (see memory: project_tenant_user_directory.md)
TENANTS = [
    {
        'tenantId': 'ATL642715',
        'companyName': 'Atlanta Angels',
        'subscriptionTier': 'standard',
        'clerkOrgId': '',  # Not yet created in Clerk
        'stripeCustomerId': '',  # Capture from Bubble before cancellation
        'networkId': 'NATANGELS',
        'networkName': 'National Angels',
    },
    {
        'tenantId': 'AUS123957',
        'companyName': 'Austin Angels',
        'subscriptionTier': 'standard',
        'clerkOrgId': 'org_3C87BBQLABjOKmIr0syXmYOPT4u',  # Known Clerk org
        'stripeCustomerId': '',  # Capture from Bubble before cancellation
        'networkId': 'NATANGELS',
        'networkName': 'National Angels',
    },
    {
        'tenantId': 'FOS402334',
        'companyName': 'Foster Village',
        'subscriptionTier': 'standard',
        'clerkOrgId': '',  # Not yet created in Clerk
        'stripeCustomerId': '',  # Capture from Bubble before cancellation
        'networkId': None,
        'networkName': None,
    },
]


def get_clients():
    """Initialize AWS clients."""
    session = boto3.Session(
        region_name=AWS_REGION,
        profile_name=os.environ.get('AWS_PROFILE', 'chris-admin'),
    )
    return session.client('s3'), session.client('dynamodb')


def load_tenant_from_s3(s3, tenant_id):
    """Load tenant config and mapping from S3. Returns (config, mapping, onboarded_at)."""
    config_key = f"tenants/{tenant_id}/{tenant_id}-config.json"

    try:
        # Load config
        obj = s3.get_object(Bucket=S3_BUCKET, Key=config_key)
        config = json.loads(obj['Body'].read())
        onboarded_at = obj['LastModified'].isoformat()

        # Get tenant hash from config
        tenant_hash = config.get('tenant_hash', '')

        # Verify mapping file exists
        if tenant_hash:
            mapping_key = f"mappings/{tenant_hash}.json"
            try:
                mapping_obj = s3.get_object(Bucket=S3_BUCKET, Key=mapping_key)
                mapping = json.loads(mapping_obj['Body'].read())
                assert mapping.get('tenant_id') == tenant_id, f"Mapping tenant_id mismatch: {mapping.get('tenant_id')} != {tenant_id}"
            except ClientError:
                print(f"  WARNING: Mapping file not found for hash {tenant_hash}")
                mapping = None
        else:
            print(f"  WARNING: No tenant_hash in config")
            mapping = None

        return config, mapping, onboarded_at

    except ClientError as e:
        print(f"  ERROR: Failed to load config: {e.response['Error']['Message']}")
        return None, None, None


def build_registry_record(tenant_data, config, onboarded_at):
    """Build a DynamoDB-formatted registry record."""
    tenant_hash = config.get('tenant_hash', '')
    now = datetime.now(timezone.utc).isoformat()

    item = {
        'tenantId': {'S': tenant_data['tenantId']},
        'tenantHash': {'S': tenant_hash},
        'companyName': {'S': tenant_data['companyName']},
        's3ConfigPath': {'S': f"tenants/{tenant_data['tenantId']}/{tenant_data['tenantId']}-config.json"},
        'subscriptionTier': {'S': tenant_data['subscriptionTier']},
        'status': {'S': 'active'},
        'onboardedAt': {'S': onboarded_at},
        'updatedAt': {'S': now},
    }

    # GSI key attributes — omit when empty (DynamoDB rejects empty string GSI keys;
    # omitting means the item won't appear in that GSI, which is correct)
    if tenant_data.get('clerkOrgId'):
        item['clerkOrgId'] = {'S': tenant_data['clerkOrgId']}
    if tenant_data.get('stripeCustomerId'):
        item['stripeCustomerId'] = {'S': tenant_data['stripeCustomerId']}

    # Nullable fields
    if tenant_data.get('networkId'):
        item['networkId'] = {'S': tenant_data['networkId']}
        item['networkName'] = {'S': tenant_data.get('networkName', '')}
    else:
        item['networkId'] = {'NULL': True}
        item['networkName'] = {'NULL': True}

    return item


def backfill(execute=False):
    """Run the backfill. Dry run by default."""
    s3, dynamodb = get_clients()

    print(f"Tenant Registry Backfill")
    print(f"  Table: {TABLE_NAME}")
    print(f"  Mode: {'EXECUTE' if execute else 'DRY RUN'}")
    print(f"  Tenants: {len(TENANTS)}")
    print("=" * 50)

    results = {'success': 0, 'failed': 0, 'skipped': 0}

    for tenant_data in TENANTS:
        tenant_id = tenant_data['tenantId']
        print(f"\n--- {tenant_id} ({tenant_data['companyName']}) ---")

        # Load from S3
        config, mapping, onboarded_at = load_tenant_from_s3(s3, tenant_id)
        if not config:
            print(f"  SKIP: No config found in S3")
            results['skipped'] += 1
            continue

        tenant_hash = config.get('tenant_hash', '')
        print(f"  Hash: {tenant_hash}")
        print(f"  Onboarded: {onboarded_at}")
        print(f"  Clerk Org: {tenant_data['clerkOrgId'] or '(not linked)'}")
        print(f"  Stripe: {tenant_data['stripeCustomerId'] or '(not linked)'}")
        print(f"  Network: {tenant_data.get('networkId', '(independent)')}")

        # Build record
        record = build_registry_record(tenant_data, config, onboarded_at)

        if execute:
            try:
                dynamodb.put_item(TableName=TABLE_NAME, Item=record)
                print(f"  WRITTEN to {TABLE_NAME}")
                results['success'] += 1
            except ClientError as e:
                print(f"  ERROR: {e.response['Error']['Message']}")
                results['failed'] += 1
        else:
            display = {}
            for k, v in record.items():
                if 'NULL' in v:
                    display[k] = None
                else:
                    display[k] = list(v.values())[0]
            print(f"  WOULD WRITE: {json.dumps(display, indent=4)}")
            results['success'] += 1

    print(f"\n{'=' * 50}")
    print(f"Results: {results['success']} success, {results['failed']} failed, {results['skipped']} skipped")

    if not execute:
        print("\nThis was a DRY RUN. Re-run with --execute to write to DynamoDB.")


def verify():
    """Verify registry records match S3 configs."""
    s3, dynamodb = get_clients()

    print(f"Verifying {TABLE_NAME} against S3...")
    print("=" * 50)

    for tenant_data in TENANTS:
        tenant_id = tenant_data['tenantId']
        print(f"\n--- {tenant_id} ---")

        # Check S3
        config, _, _ = load_tenant_from_s3(s3, tenant_id)
        if not config:
            print(f"  S3: NOT FOUND")
            continue
        print(f"  S3: OK (hash={config.get('tenant_hash', 'N/A')})")

        # Check DynamoDB
        try:
            response = dynamodb.get_item(
                TableName=TABLE_NAME,
                Key={'tenantId': {'S': tenant_id}}
            )
            item = response.get('Item')
            if item:
                db_hash = item.get('tenantHash', {}).get('S', '')
                db_status = item.get('status', {}).get('S', '')
                s3_hash = config.get('tenant_hash', '')
                hash_match = db_hash == s3_hash
                print(f"  Registry: OK (hash={db_hash}, status={db_status}, hash_match={hash_match})")
                if not hash_match:
                    print(f"  WARNING: Hash mismatch! S3={s3_hash}, Registry={db_hash}")
            else:
                print(f"  Registry: NOT FOUND")
        except ClientError as e:
            print(f"  Registry: ERROR - {e.response['Error']['Message']}")


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ('-h', '--help'):
        print("Usage:")
        print(f"  {sys.argv[0]}              # Dry run")
        print(f"  {sys.argv[0]} --execute    # Write to DynamoDB")
        print(f"  {sys.argv[0]} --verify     # Verify registry vs S3")
        sys.exit(0)

    if '--verify' in sys.argv:
        verify()
    elif '--execute' in sys.argv:
        backfill(execute=True)
    else:
        backfill(execute=False)


if __name__ == '__main__':
    main()
