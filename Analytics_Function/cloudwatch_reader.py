import json
import logging
import boto3
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import time
from functools import lru_cache

from config import LOG_GROUP_STREAMING, LOG_GROUP_MASTER, CACHE_TTL_SECONDS, MAX_QUERY_RESULTS

logger = logging.getLogger()

class CloudWatchReader:
    def __init__(self):
        self.logs_client = boto3.client('logs')
        self._cache = {}
        self._cache_timestamps = {}
    
    def get_qa_complete_logs(self, tenant_hash: str, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """
        Query CloudWatch Insights for QA_COMPLETE logs from both Lambda functions
        """
        cache_key = f"{tenant_hash}:{start_time.isoformat()}:{end_time.isoformat()}"
        
        if cache_key in self._cache:
            cache_age = time.time() - self._cache_timestamps.get(cache_key, 0)
            if cache_age < CACHE_TTL_SECONDS:
                logger.info(f"Returning cached results for {tenant_hash[:8]}... (age: {cache_age:.0f}s)")
                return self._cache[cache_key]
        
        logger.info(f"Querying CloudWatch for tenant_hash: {tenant_hash[:8]}... from {start_time.isoformat()} to {end_time.isoformat()}")
        
        query = f"""
        fields @timestamp, @message
        | filter @message like /QA_COMPLETE/
        | filter @message like /{tenant_hash}/
        | sort @timestamp desc
        | limit {MAX_QUERY_RESULTS}
        """
        
        all_results = []
        
        for log_group in [LOG_GROUP_STREAMING, LOG_GROUP_MASTER]:
            try:
                logger.info(f"Querying log group: {log_group}")
                
                response = self.logs_client.start_query(
                    logGroupName=log_group,
                    startTime=int(start_time.timestamp()),
                    endTime=int(end_time.timestamp()),
                    queryString=query
                )
                
                query_id = response['queryId']
                
                status = 'Running'
                max_wait = 30  
                wait_time = 0
                
                while status == 'Running' and wait_time < max_wait:
                    time.sleep(1)
                    wait_time += 1
                    
                    result = self.logs_client.get_query_results(queryId=query_id)
                    status = result['status']
                
                if status == 'Complete':
                    results = result.get('results', [])
                    logger.info(f"Found {len(results)} QA_COMPLETE logs in {log_group}")
                    
                    for log_entry in results:
                        parsed_log = self._parse_log_entry(log_entry)
                        if parsed_log:
                            all_results.append(parsed_log)
                else:
                    logger.warning(f"Query did not complete in time. Status: {status}")
                    
            except Exception as e:
                logger.error(f"Error querying {log_group}: {str(e)}")
                continue
        
        logger.info(f"Total QA_COMPLETE logs found: {len(all_results)}")
        
        self._cache[cache_key] = all_results
        self._cache_timestamps[cache_key] = time.time()
        
        if len(self._cache) > 100:
            oldest_key = min(self._cache_timestamps.keys(), key=lambda k: self._cache_timestamps[k])
            del self._cache[oldest_key]
            del self._cache_timestamps[oldest_key]
        
        return all_results
    
    def _parse_log_entry(self, log_entry: List[Dict[str, str]]) -> Optional[Dict[str, Any]]:
        """Parse a CloudWatch Insights log entry"""
        try:
            message_field = next((field for field in log_entry if field.get('field') == '@message'), None)
            timestamp_field = next((field for field in log_entry if field.get('field') == '@timestamp'), None)
            
            if not message_field:
                return None
            
            message = message_field.get('value', '')
            timestamp = timestamp_field.get('value', '') if timestamp_field else ''
            
            # Try to parse as JSON directly
            try:
                # Handle potential log prefix (like timestamp and request ID)
                if '\t' in message:
                    # Split by tab and get the last part (the actual JSON)
                    parts = message.split('\t')
                    if len(parts) > 1:
                        message = parts[-1]
                
                log_data = json.loads(message)
            except json.JSONDecodeError:
                # Try to extract JSON from the message
                start_idx = message.find('{')
                end_idx = message.rfind('}') + 1
                if start_idx != -1 and end_idx > start_idx:
                    try:
                        log_data = json.loads(message[start_idx:end_idx])
                    except json.JSONDecodeError:
                        logger.debug(f"Could not parse JSON from message: {message[:200]}")
                        return None
                else:
                    return None
            
            # Check if this is a QA_COMPLETE log
            if log_data.get('type') != 'QA_COMPLETE':
                return None
            
            return {
                'timestamp': log_data.get('timestamp', timestamp),
                'tenant_hash': log_data.get('tenant_hash'),
                'tenant_id': log_data.get('tenant_id'),
                'session_id': log_data.get('session_id'),
                'conversation_id': log_data.get('conversation_id'),
                'question': log_data.get('question', ''),
                'answer': log_data.get('answer', ''),
                'metrics': log_data.get('metrics', {})
            }
            
        except Exception as e:
            logger.warning(f"Error parsing log entry: {str(e)}")
            return None
    
    def get_query_status(self, query_id: str) -> str:
        """Check the status of a CloudWatch Insights query"""
        try:
            response = self.logs_client.describe_queries(queryIds=[query_id])
            if response.get('queries'):
                return response['queries'][0].get('status', 'Unknown')
        except Exception as e:
            logger.error(f"Error checking query status: {str(e)}")
        return 'Unknown'