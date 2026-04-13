"""
Tenant Registry Operations — Full CRUD for portal super admin views.

Used by Analytics_Dashboard_API for /admin/tenants and /admin/tenants/{id}/employees endpoints.
Uses boto3 low-level client (consistent with existing Analytics Dashboard API patterns).
"""

import os
import logging
import time
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal

logger = logging.getLogger()

REGION = os.environ.get('AWS_REGION', 'us-east-1')
TENANT_TABLE = os.environ.get('TENANT_REGISTRY_TABLE', f"picasso-tenant-registry-{os.environ.get('ENVIRONMENT', 'staging')}")
EMPLOYEE_TABLE = os.environ.get('EMPLOYEE_REGISTRY_TABLE', f"picasso-employee-registry-{os.environ.get('ENVIRONMENT', 'staging')}")

dynamodb = boto3.client('dynamodb', region_name=REGION)


def _unmarshall(item):
    """Convert DynamoDB item format to plain dict."""
    result = {}
    for key, value in item.items():
        if 'S' in value:
            result[key] = value['S']
        elif 'N' in value:
            result[key] = value['N']
        elif 'BOOL' in value:
            result[key] = value['BOOL']
        elif 'NULL' in value:
            result[key] = None
        elif 'L' in value:
            result[key] = [_unmarshall_value(v) for v in value['L']]
        elif 'M' in value:
            result[key] = _unmarshall(value['M'])
    return result


def _unmarshall_value(value):
    """Convert a single DynamoDB value."""
    if 'S' in value:
        return value['S']
    elif 'N' in value:
        return value['N']
    elif 'BOOL' in value:
        return value['BOOL']
    elif 'NULL' in value:
        return None
    elif 'M' in value:
        return _unmarshall(value['M'])
    elif 'L' in value:
        return [_unmarshall_value(v) for v in value['L']]
    return None


def _marshall_value(value):
    """Convert a Python value to DynamoDB format."""
    if value is None:
        return {'NULL': True}
    elif isinstance(value, str):
        return {'S': value}
    elif isinstance(value, bool):
        return {'BOOL': value}
    elif isinstance(value, (int, float, Decimal)):
        return {'N': str(value)}
    elif isinstance(value, list):
        return {'L': [_marshall_value(v) for v in value]}
    elif isinstance(value, dict):
        return {'M': {k: _marshall_value(v) for k, v in value.items()}}
    return {'S': str(value)}


# --- Tenant Operations ---

def put_tenant(record):
    """Write a full tenant record."""
    item = {
        'tenantId': {'S': record['tenantId']},
        'tenantHash': {'S': record['tenantHash']},
        'companyName': {'S': record.get('companyName', '')},
        's3ConfigPath': {'S': record.get('s3ConfigPath', '')},
        'subscriptionTier': {'S': record.get('subscriptionTier', 'free')},
        'status': {'S': record.get('status', 'active')},
        'onboardedAt': {'S': record.get('onboardedAt', '')},
        'updatedAt': {'S': record.get('updatedAt', new_timestamp())},
    }

    # GSI key attributes — omit when empty (DynamoDB rejects empty string GSI keys)
    if record.get('clerkOrgId'):
        item['clerkOrgId'] = {'S': record['clerkOrgId']}
    if record.get('stripeCustomerId'):
        item['stripeCustomerId'] = {'S': record['stripeCustomerId']}

    # Nullable fields
    if record.get('networkId'):
        item['networkId'] = {'S': record['networkId']}
    else:
        item['networkId'] = {'NULL': True}

    if record.get('networkName'):
        item['networkName'] = {'S': record['networkName']}
    else:
        item['networkName'] = {'NULL': True}

    dynamodb.put_item(TableName=TENANT_TABLE, Item=item)
    return _unmarshall(item)


def get_tenant(tenant_id):
    """Get a tenant by ID. Returns dict or None."""
    response = dynamodb.get_item(
        TableName=TENANT_TABLE,
        Key={'tenantId': {'S': tenant_id}}
    )
    item = response.get('Item')
    return _unmarshall(item) if item else None


def list_all_tenants():
    """Scan all tenants. Acceptable at current scale (< 50 tenants)."""
    items = []
    params = {'TableName': TENANT_TABLE}

    while True:
        response = dynamodb.scan(**params)
        items.extend([_unmarshall(item) for item in response.get('Items', [])])

        if 'LastEvaluatedKey' not in response:
            break
        params['ExclusiveStartKey'] = response['LastEvaluatedKey']

    return items


def update_tenant(tenant_id, fields):
    """
    Update specific fields on a tenant record.
    fields: dict of field_name → new_value
    Always sets updatedAt.
    """
    if not fields:
        return

    fields['updatedAt'] = new_timestamp()

    update_parts = []
    names = {}
    values = {}

    for i, (key, value) in enumerate(fields.items()):
        alias = f"#f{i}"
        val_alias = f":v{i}"
        update_parts.append(f"{alias} = {val_alias}")
        names[alias] = key
        values[val_alias] = _marshall_value(value)

    dynamodb.update_item(
        TableName=TENANT_TABLE,
        Key={'tenantId': {'S': tenant_id}},
        UpdateExpression='SET ' + ', '.join(update_parts),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


# --- Employee Operations ---

def put_employee(tenant_id, clerk_user_id, record):
    """Write a full employee record."""
    item = {
        'tenantId': {'S': tenant_id},
        'clerkUserId': {'S': clerk_user_id},
        'email': {'S': record.get('email', '')},
        'name': {'S': record.get('name', '')},
        'role': {'S': record.get('role', 'member')},
        'status': {'S': record.get('status', 'active')},
        'createdAt': {'S': record.get('createdAt', new_timestamp())},
        'updatedAt': {'S': record.get('updatedAt', new_timestamp())},
    }

    dynamodb.put_item(TableName=EMPLOYEE_TABLE, Item=item)
    return _unmarshall(item)


def list_employees(tenant_id):
    """List all employees for a tenant."""
    items = []
    params = {
        'TableName': EMPLOYEE_TABLE,
        'KeyConditionExpression': 'tenantId = :tid',
        'ExpressionAttributeValues': {':tid': {'S': tenant_id}},
    }

    while True:
        response = dynamodb.query(**params)
        items.extend([_unmarshall(item) for item in response.get('Items', [])])

        if 'LastEvaluatedKey' not in response:
            break
        params['ExclusiveStartKey'] = response['LastEvaluatedKey']

    return items


def get_employee(tenant_id, clerk_user_id):
    """Get a specific employee. Returns dict or None."""
    response = dynamodb.get_item(
        TableName=EMPLOYEE_TABLE,
        Key={
            'tenantId': {'S': tenant_id},
            'clerkUserId': {'S': clerk_user_id},
        }
    )
    item = response.get('Item')
    return _unmarshall(item) if item else None


def get_employee_by_clerk_user_id(clerk_user_id):
    """Look up an employee by Clerk user ID (across all tenants). Returns dict or None."""
    response = dynamodb.query(
        TableName=EMPLOYEE_TABLE,
        IndexName='ClerkUserIdIndex',
        KeyConditionExpression='clerkUserId = :uid',
        ExpressionAttributeValues={':uid': {'S': clerk_user_id}},
        Limit=1,
    )
    items = response.get('Items', [])
    return _unmarshall(items[0]) if items else None


def get_employee_by_email(email):
    """Look up an employee by email (across all tenants). Returns dict or None."""
    response = dynamodb.query(
        TableName=EMPLOYEE_TABLE,
        IndexName='EmailIndex',
        KeyConditionExpression='email = :email',
        ExpressionAttributeValues={':email': {'S': email}},
        Limit=1,
    )
    items = response.get('Items', [])
    return _unmarshall(items[0]) if items else None


def update_employee(tenant_id, clerk_user_id, fields):
    """Update specific fields on an employee record. Always sets updatedAt."""
    if not fields:
        return

    fields['updatedAt'] = new_timestamp()

    update_parts = []
    names = {}
    values = {}

    for i, (key, value) in enumerate(fields.items()):
        alias = f"#f{i}"
        val_alias = f":v{i}"
        update_parts.append(f"{alias} = {val_alias}")
        names[alias] = key
        values[val_alias] = _marshall_value(value)

    dynamodb.update_item(
        TableName=EMPLOYEE_TABLE,
        Key={
            'tenantId': {'S': tenant_id},
            'clerkUserId': {'S': clerk_user_id},
        },
        UpdateExpression='SET ' + ', '.join(update_parts),
        ExpressionAttributeNames=names,
        ExpressionAttributeValues=values,
    )


def new_timestamp():
    """Return current ISO timestamp."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
