import json
import logging
import boto3
from typing import Optional, Dict, Any, List
from functools import lru_cache
import time

from config import CONFIG_BUCKET, CACHE_TTL_SECONDS

logger = logging.getLogger()

class TenantResolver:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self._cache = {}
        self._cache_timestamps = {}
        self._reverse_cache = {}  
    
    def resolve_tenant_hash(self, tenant_hash: str) -> Optional[str]:
        """
        Resolve tenant_hash to tenant_id using S3 mapping
        """
        if tenant_hash in self._cache:
            cache_age = time.time() - self._cache_timestamps.get(tenant_hash, 0)
            if cache_age < CACHE_TTL_SECONDS:
                logger.info(f"Returning cached tenant_id for {tenant_hash[:8]}... (age: {cache_age:.0f}s)")
                return self._cache[tenant_hash]
        
        try:
            logger.info(f"Fetching tenant mapping from S3 for hash: {tenant_hash[:8]}...")
            
            response = self.s3_client.get_object(
                Bucket=CONFIG_BUCKET,
                Key=f'mappings/{tenant_hash}.json'
            )
            
            mapping = json.loads(response['Body'].read())
            tenant_id = mapping.get('tenant_id')
            
            if tenant_id:
                self._cache[tenant_hash] = tenant_id
                self._cache_timestamps[tenant_hash] = time.time()
                self._reverse_cache[tenant_id] = tenant_hash
                
                if len(self._cache) > 100:
                    oldest_key = min(self._cache_timestamps.keys(), key=lambda k: self._cache_timestamps[k])
                    old_tenant_id = self._cache[oldest_key]
                    del self._cache[oldest_key]
                    del self._cache_timestamps[oldest_key]
                    if old_tenant_id in self._reverse_cache:
                        del self._reverse_cache[old_tenant_id]
                
                logger.info(f"Resolved {tenant_hash[:8]}... to tenant_id: {tenant_id}")
                return tenant_id
            else:
                logger.warning(f"No tenant_id found in mapping for {tenant_hash[:8]}...")
                return None
                
        except self.s3_client.exceptions.NoSuchKey:
            logger.error(f"No mapping found for tenant_hash: {tenant_hash[:8]}...")
            return None
        except Exception as e:
            logger.error(f"Error resolving tenant_hash {tenant_hash[:8]}...: {str(e)}")
            return None
    
    def get_tenant_hash_by_id(self, tenant_id: str) -> Optional[str]:
        """
        Get tenant_hash from tenant_id (reverse lookup)
        This requires listing all mappings or maintaining a reverse index
        """
        if tenant_id in self._reverse_cache:
            logger.info(f"Returning cached tenant_hash for tenant_id: {tenant_id}")
            return self._reverse_cache[tenant_id]
        
        try:
            logger.info(f"Searching for tenant_hash by tenant_id: {tenant_id}")
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=CONFIG_BUCKET,
                Prefix='mappings/',
                MaxKeys=1000
            )
            
            for page in page_iterator:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    key = obj['Key']
                    if not key.endswith('.json'):
                        continue
                    
                    tenant_hash = key.replace('mappings/', '').replace('.json', '')
                    
                    if tenant_hash in self._cache:
                        cached_id = self._cache[tenant_hash]
                        if cached_id == tenant_id:
                            logger.info(f"Found cached match: {tenant_hash[:8]}... for tenant_id: {tenant_id}")
                            self._reverse_cache[tenant_id] = tenant_hash
                            return tenant_hash
                    else:
                        try:
                            response = self.s3_client.get_object(
                                Bucket=CONFIG_BUCKET,
                                Key=key
                            )
                            mapping = json.loads(response['Body'].read())
                            
                            if mapping.get('tenant_id') == tenant_id:
                                logger.info(f"Found tenant_hash: {tenant_hash[:8]}... for tenant_id: {tenant_id}")
                                
                                self._cache[tenant_hash] = tenant_id
                                self._cache_timestamps[tenant_hash] = time.time()
                                self._reverse_cache[tenant_id] = tenant_hash
                                
                                return tenant_hash
                        except Exception as e:
                            logger.warning(f"Error reading mapping {key}: {str(e)}")
                            continue
            
            logger.error(f"No tenant_hash found for tenant_id: {tenant_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error searching for tenant_id {tenant_id}: {str(e)}")
            return None
    
    def get_all_tenant_ids(self) -> List[str]:
        """
        Get all available tenant_ids from S3 mappings
        """
        tenant_ids = []
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=CONFIG_BUCKET,
                Prefix='mappings/',
                MaxKeys=1000
            )
            
            for page in page_iterator:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    key = obj['Key']
                    if not key.endswith('.json'):
                        continue
                    
                    try:
                        response = self.s3_client.get_object(
                            Bucket=CONFIG_BUCKET,
                            Key=key
                        )
                        mapping = json.loads(response['Body'].read())
                        tenant_id = mapping.get('tenant_id')
                        
                        if tenant_id:
                            tenant_ids.append(tenant_id)
                            
                    except Exception as e:
                        logger.warning(f"Error reading mapping {key}: {str(e)}")
                        continue
            
            logger.info(f"Found {len(tenant_ids)} tenant_ids in S3")
            return tenant_ids
            
        except Exception as e:
            logger.error(f"Error listing tenant_ids: {str(e)}")
            return []