"""
Tenant Registry — Read-only operations for Master Function hot path.
Provides DynamoDB-based hash resolution as an alternative to S3 mapping files.

Uses aws_client_manager's protected_dynamodb_operation for circuit breaker protection.
Table name from TENANT_REGISTRY_TABLE env var.
"""

import os
import time
import logging

logger = logging.getLogger()

TABLE_NAME = os.environ.get('TENANT_REGISTRY_TABLE', f"picasso-tenant-registry-{os.environ.get('ENVIRONMENT', 'staging')}")

# Module-level cache for registry lookups (matches hash_to_tenant_cache pattern in tenant_config_loader.py)
_registry_cache = {}
_CACHE_TTL = 300  # 5 minutes


def get_tenant_by_hash(tenant_hash):
    """
    Query TenantHashIndex to resolve hash → tenant record.
    Returns the full tenant record dict, or None if not found.
    Uses circuit-breaker-protected DynamoDB operations.
    Results cached for 5 minutes (module-level, survives across invocations in warm Lambda).
    """
    # Check cache first
    cached = _registry_cache.get(tenant_hash)
    if cached:
        cache_age = time.time() - cached.get('_cached_at', 0)
        if cache_age < _CACHE_TTL:
            logger.debug(f"[{tenant_hash[:8]}...] Registry cache hit")
            return cached.get('record')

    try:
        from aws_client_manager import protected_dynamodb_operation

        response = protected_dynamodb_operation(
            'query',
            TableName=TABLE_NAME,
            IndexName='TenantHashIndex',
            KeyConditionExpression='tenantHash = :hash',
            ExpressionAttributeValues={
                ':hash': {'S': tenant_hash}
            },
            Limit=1
        )

        items = response.get('Items', [])
        if not items:
            logger.info(f"[{tenant_hash[:8]}...] No registry record for hash")
            return None

        # Convert DynamoDB format to plain dict
        item = items[0]
        record = {
            'tenantId': item.get('tenantId', {}).get('S', ''),
            'tenantHash': item.get('tenantHash', {}).get('S', ''),
            'companyName': item.get('companyName', {}).get('S', ''),
            'status': item.get('status', {}).get('S', ''),
            's3ConfigPath': item.get('s3ConfigPath', {}).get('S', ''),
            'clerkOrgId': item.get('clerkOrgId', {}).get('S', ''),
            'stripeCustomerId': item.get('stripeCustomerId', {}).get('S', ''),
            'networkId': item.get('networkId', {}).get('S') if item.get('networkId', {}).get('S') else None,
            'networkName': item.get('networkName', {}).get('S') if item.get('networkName', {}).get('S') else None,
            'subscriptionTier': item.get('subscriptionTier', {}).get('S', ''),
            'onboardedAt': item.get('onboardedAt', {}).get('S', ''),
            'updatedAt': item.get('updatedAt', {}).get('S', ''),
        }

        # Cache the result
        _registry_cache[tenant_hash] = {
            'record': record,
            '_cached_at': time.time()
        }

        logger.info(f"[{tenant_hash[:8]}...] Registry lookup successful: {record['tenantId']}")
        return record

    except Exception as e:
        logger.warning(f"[{tenant_hash[:8]}...] Registry lookup failed: {str(e)}")
        return None


def get_tenant_by_id(tenant_id):
    """
    GetItem by primary key (tenantId).
    Returns the full tenant record dict, or None if not found.
    """
    try:
        from aws_client_manager import protected_dynamodb_operation

        response = protected_dynamodb_operation(
            'get_item',
            TableName=TABLE_NAME,
            Key={
                'tenantId': {'S': tenant_id}
            }
        )

        item = response.get('Item')
        if not item:
            return None

        return {
            'tenantId': item.get('tenantId', {}).get('S', ''),
            'tenantHash': item.get('tenantHash', {}).get('S', ''),
            'companyName': item.get('companyName', {}).get('S', ''),
            'status': item.get('status', {}).get('S', ''),
            's3ConfigPath': item.get('s3ConfigPath', {}).get('S', ''),
            'clerkOrgId': item.get('clerkOrgId', {}).get('S', ''),
            'stripeCustomerId': item.get('stripeCustomerId', {}).get('S', ''),
            'networkId': item.get('networkId', {}).get('S') if item.get('networkId', {}).get('S') else None,
            'networkName': item.get('networkName', {}).get('S') if item.get('networkName', {}).get('S') else None,
            'subscriptionTier': item.get('subscriptionTier', {}).get('S', ''),
            'onboardedAt': item.get('onboardedAt', {}).get('S', ''),
            'updatedAt': item.get('updatedAt', {}).get('S', ''),
        }

    except Exception as e:
        logger.warning(f"[{tenant_id}] Registry get_item failed: {str(e)}")
        return None
