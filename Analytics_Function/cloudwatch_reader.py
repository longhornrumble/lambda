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

        logger.info(f"\n=== CloudWatch Query Debug ===")
        logger.info(f"Tenant hash: {tenant_hash}")
        logger.info(f"Start time: {start_time.isoformat()}")
        logger.info(f"End time: {end_time.isoformat()}")
        logger.info(f"Time range: {(end_time - start_time).days} days")
        
        # Use separate filters - this matches what worked in direct query
        query = f"""
        fields @timestamp, @message
        | filter @message like /QA_COMPLETE/
        | filter @message like /{tenant_hash}/
        | sort @timestamp asc
        | limit {MAX_QUERY_RESULTS}
        """

        logger.info(f"Query string:\n{query}")
        logger.info(f"Max results limit: {MAX_QUERY_RESULTS}")
        
        all_results = []
        
        for log_group in [LOG_GROUP_STREAMING, LOG_GROUP_MASTER]:
            try:
                logger.info(f"\n--- Querying log group: {log_group} ---")
                
                response = self.logs_client.start_query(
                    logGroupName=log_group,
                    startTime=int(start_time.timestamp()),
                    endTime=int(end_time.timestamp()),
                    queryString=query
                )
                
                query_id = response['queryId']
                
                status = 'Running'
                max_wait = 60  # Increased from 30 to 60 seconds
                wait_time = 0
                poll_count = 0

                while status == 'Running' and wait_time < max_wait:
                    time.sleep(2)  # Poll every 2 seconds instead of 1
                    wait_time += 2
                    poll_count += 1
                    
                    result = self.logs_client.get_query_results(queryId=query_id)
                    status = result['status']

                    # Log progress every 10 seconds
                    if poll_count % 5 == 0:
                        logger.info(f"Query still running... ({wait_time}s elapsed)")
                
                if status == 'Complete':
                    results = result.get('results', [])
                    logger.info(f"Raw results count: {len(results)} from {log_group}")

                    parsed_count = 0
                    failed_count = 0
                    seen_sessions = set()
                    seen_questions = set()

                    for idx, log_entry in enumerate(results):
                        parsed_log = self._parse_log_entry(log_entry)
                        if parsed_log:
                            all_results.append(parsed_log)
                            parsed_count += 1

                            # Track unique sessions and questions
                            session_id = parsed_log.get('session_id', '')
                            question = parsed_log.get('question', '')
                            if session_id:
                                seen_sessions.add(session_id)
                            if question:
                                seen_questions.add(question[:50])  # First 50 chars

                            # Log first few successful parses
                            if parsed_count <= 3:
                                logger.info(f"Parsed log {parsed_count}: session={session_id[:8]}..., question={question[:30]}...")
                        else:
                            failed_count += 1
                            # Log all failures for debugging
                            logger.warning(f"Failed to parse log entry {idx+1}/{len(results)}")
                            # Always log failed messages for Foster Village debug
                            if tenant_hash == "fo85e6a06dcdf4" or failed_count <= 5:
                                # Get the message field for debugging
                                msg_field = next((f for f in log_entry if f.get('field') == '@message'), None)
                                if msg_field:
                                    msg_value = msg_field.get('value', '')
                                    # Check if it contains QA_COMPLETE
                                    if 'QA_COMPLETE' in msg_value:
                                        logger.warning(f"Failed to parse QA_COMPLETE log! Message preview: {msg_value[:300]}")
                                    else:
                                        logger.warning(f"Non-QA_COMPLETE message: {msg_value[:100]}")

                    logger.info(f"Parse summary for {log_group}:")
                    logger.info(f"  - Successfully parsed: {parsed_count}")
                    logger.info(f"  - Failed to parse: {failed_count}")
                    logger.info(f"  - Unique sessions: {len(seen_sessions)}")
                    logger.info(f"  - Unique questions: {len(seen_questions)}")
                else:
                    logger.warning(f"Query did not complete. Status: {status}")
                    if status == 'Running':
                        logger.warning(f"Query still running after {max_wait} seconds timeout")
                    
            except Exception as e:
                logger.error(f"Error querying {log_group}: {str(e)}")
                continue
        
        # Final summary
        unique_sessions = set()
        unique_questions = set()
        for log in all_results:
            if log.get('session_id'):
                unique_sessions.add(log['session_id'])
            if log.get('question'):
                unique_questions.add(log['question'][:50])

        logger.info(f"\n=== Final Query Results ===")
        logger.info(f"Total QA_COMPLETE logs found: {len(all_results)}")
        logger.info(f"Unique sessions: {len(unique_sessions)}")
        logger.info(f"Unique questions: {len(unique_questions)}")

        # Log all found logs for Foster Village to debug
        if tenant_hash == "fo85e6a06dcdf4":
            logger.info(f"\n=== Foster Village Debug - All {len(all_results)} logs ===")
            for i, log in enumerate(all_results, 1):
                logger.info(f"Log {i}: session={log.get('session_id', 'N/A')[:12]}, "
                          f"timestamp={log.get('timestamp', 'N/A')[:19]}, "
                          f"question={log.get('question', 'N/A')[:40]}...")
        
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
                # Handle Lambda log format: "timestamp request-id INFO {json}"
                if ' INFO ' in message:
                    # Split by INFO and take everything after it
                    parts = message.split(' INFO ', 1)
                    if len(parts) > 1:
                        message = parts[1].strip()
                elif '\t' in message:
                    # Handle tab-separated format (legacy)
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
                # Log why we're skipping this
                logger.debug(f"Skipping non-QA_COMPLETE log: type={log_data.get('type')}")
                return None

            # Validate tenant_hash matches what we're looking for
            log_tenant_hash = log_data.get('tenant_hash', '')
            if not log_tenant_hash:
                logger.debug(f"Skipping log with no tenant_hash")
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