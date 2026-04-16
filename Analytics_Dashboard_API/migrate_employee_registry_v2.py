#!/usr/bin/env python3
"""
One-time migration: v1 employee registry -> v2 (UUID-based employeeId sort key).

v1 schema: PK=tenantId, SK=clerkUserId
v2 schema: PK=tenantId, SK=employeeId (UUID), clerkUserId as nullable non-key field

Usage:
    AWS_PROFILE=chris-admin python3 migrate_employee_registry_v2.py

    # Override environment:
    ENVIRONMENT=production AWS_PROFILE=chris-admin python3 migrate_employee_registry_v2.py

Safe to re-run (idempotent -- PutItem overwrites by composite key).
"""

import os
import sys
import uuid
import logging
from decimal import Decimal
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')
V1_TABLE_NAME = f'picasso-employee-registry-{ENVIRONMENT}'
V2_TABLE_NAME = f'picasso-employee-registry-v2-{ENVIRONMENT}'
REGION = os.environ.get('AWS_REGION', 'us-east-1')

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def new_timestamp():
    """Return current UTC ISO 8601 timestamp."""
    return datetime.now(timezone.utc).isoformat()


def is_corrupt(record):
    """
    A v1 record is corrupt when clerkUserId is missing or empty.

    handle_admin_employee_invite has a bug where it writes an empty SK,
    which produces an invalid DynamoDB item.  Skip these rows.
    """
    clerk_user_id = record.get('clerkUserId', '')
    # DynamoDB high-level resource returns strings as-is.
    # Guard against None or non-string values just in case.
    return not clerk_user_id or not str(clerk_user_id).strip()


def scan_all(table):
    """
    Full table scan with pagination.

    Returns a list of plain dicts (boto3 resource already unmarshals
    DynamoDB types into Python types, including Decimal for numbers).
    """
    items = []
    params = {}

    while True:
        response = table.scan(**params)
        items.extend(response.get('Items', []))

        last_key = response.get('LastEvaluatedKey')
        if not last_key:
            break
        params['ExclusiveStartKey'] = last_key

    return items


def build_v2_record(v1_record, employee_id):
    """
    Map a v1 record dict to a v2 record dict.

    Key changes:
    - employeeId (new UUID) becomes the sort key
    - clerkUserId moves from SK to a regular nullable field
    - type='clerk_user' is added for all migrated records
    - All other fields (email, name, role, status, createdAt, updatedAt)
      are copied as-is; updatedAt is refreshed to the migration timestamp.

    DynamoDB resource handles Decimal fields transparently, so no manual
    marshalling is needed here.
    """
    now = new_timestamp()

    record = {
        'tenantId':     v1_record['tenantId'],
        'employeeId':   employee_id,
        'clerkUserId':  v1_record.get('clerkUserId') or None,
        'type':         'clerk_user',
        'email':        v1_record.get('email', ''),
        'name':         v1_record.get('name', ''),
        'role':         v1_record.get('role', 'member'),
        'status':       v1_record.get('status', 'active'),
        'createdAt':    v1_record.get('createdAt', now),
        'updatedAt':    now,
    }

    # Drop None-valued non-required fields to keep the item clean,
    # except clerkUserId which is intentionally nullable in v2.
    return record


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print()
    print('=' * 60)
    print('Employee Registry v1 -> v2 Migration')
    print('=' * 60)
    print(f'Environment  : {ENVIRONMENT}')
    print(f'Source table : {V1_TABLE_NAME}')
    print(f'Target table : {V2_TABLE_NAME}')
    print('=' * 60)
    print()

    dynamodb = boto3.resource('dynamodb', region_name=REGION)
    v1_table = dynamodb.Table(V1_TABLE_NAME)
    v2_table = dynamodb.Table(V2_TABLE_NAME)

    # --- Step 1: Verify tables exist ---
    for table_obj, label in [(v1_table, 'Source'), (v2_table, 'Target')]:
        try:
            table_obj.load()
            logger.info('%s table exists: %s', label, table_obj.table_name)
        except ClientError as exc:
            code = exc.response['Error']['Code']
            if code == 'ResourceNotFoundException':
                logger.error('%s table not found: %s', label, table_obj.table_name)
                sys.exit(1)
            raise

    print()

    # --- Step 2: Scan v1 ---
    logger.info('Scanning v1 table...')
    try:
        v1_items = scan_all(v1_table)
    except ClientError as exc:
        logger.error('Failed to scan v1 table: %s', exc)
        sys.exit(1)

    logger.info('Found %d record(s) in v1 table', len(v1_items))
    print()

    # --- Step 3: Migrate ---
    migrated = 0
    skipped_corrupt = 0
    errors = 0

    for item in v1_items:
        tenant_id    = item.get('tenantId', '(unknown)')
        email        = item.get('email', '')
        clerk_user_id = item.get('clerkUserId', '')

        if is_corrupt(item):
            logger.warning(
                'SKIP (corrupt) tenant=%-20s  email=%-40s  clerkUserId=%r',
                tenant_id, email, clerk_user_id,
            )
            skipped_corrupt += 1
            continue

        employee_id = str(uuid.uuid4())
        v2_record   = build_v2_record(item, employee_id)

        try:
            v2_table.put_item(Item=v2_record)
            logger.info(
                'OK   tenant=%-20s  email=%-40s  clerkUserId=%-30s  -> employeeId=%s',
                tenant_id, email, clerk_user_id, employee_id,
            )
            migrated += 1
        except ClientError as exc:
            logger.error(
                'ERROR tenant=%-20s  email=%-40s  clerkUserId=%s  exc=%s',
                tenant_id, email, clerk_user_id, exc,
            )
            errors += 1

    # --- Summary ---
    print()
    print('=' * 60)
    print(f'Migration complete')
    print(f'  Migrated : {migrated}')
    print(f'  Skipped  : {skipped_corrupt}  (corrupt — empty clerkUserId)')
    print(f'  Errors   : {errors}')
    print('=' * 60)
    print()

    if errors:
        sys.exit(1)


if __name__ == '__main__':
    main()
