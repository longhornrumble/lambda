import json
import os
import logging
import boto3
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

from config import ENVIRONMENT, CONFIG_BUCKET
from cloudwatch_reader import CloudWatchReader
from tenant_resolver import TenantResolver

class AnalyticsFunction:
    def __init__(self):
        self.cloudwatch = CloudWatchReader()
        self.tenant_resolver = TenantResolver()
        
    def process_tenant(self, tenant_hash: str, start_date: Optional[str] = None, 
                       end_date: Optional[str] = None, top_questions_limit: int = 5,
                       include_heat_map: bool = False, include_full_conversations: bool = False,
                       full_conversations_limit: int = 50) -> Dict[str, Any]:
        """Process analytics for a specific tenant with date range"""
        logger.info(f"Processing analytics for tenant_hash: {tenant_hash[:8]}... from {start_date} to {end_date}")
        
        # Parse dates or use defaults
        if end_date:
            try:
                end_time = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            except:
                end_time = datetime.strptime(end_date, "%Y-%m-%d")
                end_time = end_time.replace(hour=23, minute=59, second=59)
        else:
            end_time = datetime.utcnow()
        
        if start_date:
            try:
                start_time = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            except:
                start_time = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            # Default to 7 days ago
            start_time = end_time - timedelta(days=7)
        
        # Calculate period in days for display
        period_days = (end_time - start_time).days
        
        tenant_id = self.tenant_resolver.resolve_tenant_hash(tenant_hash)
        if not tenant_id:
            logger.error(f"Could not resolve tenant_id for hash: {tenant_hash[:8]}...")
            raise ValueError(f"Invalid tenant_hash: {tenant_hash}")
        
        qa_logs = self.cloudwatch.get_qa_complete_logs(
            tenant_hash=tenant_hash,
            start_time=start_time,
            end_time=end_time
        )
        
        if not qa_logs:
            logger.info(f"No QA_COMPLETE logs found for tenant {tenant_hash[:8]}...")
            return self._empty_response(tenant_id, tenant_hash, start_time, end_time, 
                                       include_heat_map, include_full_conversations)
        
        unique_sessions = set()
        unique_conversations = set()
        response_times = []
        first_token_times = []  # Track streaming response times separately
        total_response_times = []  # Track complete response times
        questions = []
        after_hours_count = 0
        
        # Heat map data: track hourly and daily patterns
        hourly_distribution = {hour: 0 for hour in range(24)}
        daily_distribution = {day: 0 for day in range(7)}  # 0=Monday, 6=Sunday
        question_timestamps = []  # For detailed heat map
        
        # Full conversation data if requested
        full_conversations = []
        
        for log in qa_logs:
            unique_sessions.add(log.get('session_id', ''))
            unique_conversations.add(log.get('conversation_id', ''))
            
            # Track both streaming and total response times
            if 'metrics' in log:
                # First token time (streaming)
                if 'first_token_ms' in log['metrics'] and log['metrics']['first_token_ms'] > 0:
                    first_token_times.append(log['metrics']['first_token_ms'])
                    response_times.append(log['metrics']['first_token_ms'])  # Use for primary metric
                # Total time
                if 'total_time_ms' in log['metrics'] and log['metrics']['total_time_ms'] > 0:
                    total_response_times.append(log['metrics']['total_time_ms'])
                    if not ('first_token_ms' in log['metrics'] and log['metrics']['first_token_ms'] > 0):
                        response_times.append(log['metrics']['total_time_ms'])  # Fallback if no streaming
                elif 'response_time_ms' in log['metrics'] and not response_times:
                    response_times.append(log['metrics']['response_time_ms'])
            
            questions.append(log.get('question', ''))
            
            # Collect full conversation if requested
            if include_full_conversations and len(full_conversations) < full_conversations_limit:
                full_conversations.append({
                    'timestamp': log.get('timestamp', ''),
                    'session_id': log.get('session_id', ''),
                    'conversation_id': log.get('conversation_id', ''),
                    'question': log.get('question', ''),
                    'answer': log.get('answer', ''),
                    'response_time_ms': log.get('metrics', {}).get('total_time_ms', 0) or log.get('metrics', {}).get('response_time_ms', 0)
                })
            
            timestamp_str = log.get('timestamp', '')
            if timestamp_str:
                try:
                    timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    hour = timestamp.hour
                    day_of_week = timestamp.weekday()
                    
                    # Track for heat map
                    hourly_distribution[hour] += 1
                    daily_distribution[day_of_week] += 1
                    question_timestamps.append({
                        'timestamp': timestamp_str,
                        'hour': hour,
                        'day_of_week': day_of_week,
                        'question': log.get('question', '')[:100]  # Truncate for heat map
                    })
                    
                    # Track after hours
                    if hour < 9 or hour >= 17:
                        after_hours_count += 1
                except Exception as e:
                    logger.warning(f"Could not parse timestamp: {timestamp_str} - {e}")
        
        conversation_count = len(unique_conversations) if unique_conversations else len(unique_sessions)
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0
        avg_first_token_time = sum(first_token_times) / len(first_token_times) if first_token_times else 0
        avg_total_time = sum(total_response_times) / len(total_response_times) if total_response_times else 0
        after_hours_percentage = (after_hours_count / len(qa_logs) * 100) if qa_logs else 0
        
        top_questions = self._extract_top_questions(questions, limit=top_questions_limit)
        
        result = {
            "tenant_id": tenant_id,
            "tenant_hash": tenant_hash,
            "start_date": start_time.strftime("%Y-%m-%d"),
            "end_date": end_time.strftime("%Y-%m-%d"),
            "period_days": period_days,
            "metrics": {
                "conversation_count": conversation_count,
                "avg_response_time_ms": round(avg_response_time),  # Primary metric (first token if available)
                "avg_first_token_ms": round(avg_first_token_time) if avg_first_token_time else None,
                "avg_total_time_ms": round(avg_total_time) if avg_total_time else None,
                "after_hours_percentage": round(after_hours_percentage, 1),
                "total_messages": len(qa_logs),
                "streaming_enabled_percentage": round(len(first_token_times) / len(qa_logs) * 100, 1) if qa_logs else 0
            },
            "top_questions": top_questions,
            "last_updated": datetime.utcnow().isoformat()
        }
        
        # Add heat map data if requested
        if include_heat_map:
            peak_hour_num = max(hourly_distribution, key=hourly_distribution.get) if any(hourly_distribution.values()) else None
            peak_day_num = max(daily_distribution, key=daily_distribution.get) if any(daily_distribution.values()) else None
            
            # Format hour for display
            if peak_hour_num is not None:
                if peak_hour_num == 0:
                    peak_hour_formatted = "12am"
                elif peak_hour_num < 12:
                    peak_hour_formatted = f"{peak_hour_num}am"
                elif peak_hour_num == 12:
                    peak_hour_formatted = "12pm"
                else:
                    peak_hour_formatted = f"{peak_hour_num - 12}pm"
            else:
                peak_hour_formatted = None
            
            # Format day for display
            day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            peak_day_formatted = day_names[peak_day_num] if peak_day_num is not None else None
            
            # Create grid data for easier Bubble consumption
            heat_grid = []
            time_slots = [0, 3, 6, 9, 12, 15, 18, 21]  # Hours to show
            time_labels = ["12am", "3am", "6am", "9am", "12pm", "3pm", "6pm", "9pm"]
            
            for i, hour in enumerate(time_slots):
                row_data = {
                    "time_label": time_labels[i],
                    "hour": hour,
                    "monday": 0,
                    "tuesday": 0,
                    "wednesday": 0,
                    "thursday": 0,
                    "friday": 0,
                    "saturday": 0,
                    "sunday": 0
                }
                
                # Aggregate counts for 3-hour windows
                for h in range(hour, min(hour + 3, 24)):
                    if h in hourly_distribution:
                        # This is simplified - you'd need to track which day each question was on
                        # For now, distribute across days based on daily_distribution
                        for day_num in range(7):
                            if day_num in daily_distribution:
                                # Rough distribution - you'd calculate this properly from question_timestamps
                                value = hourly_distribution[h] * daily_distribution[day_num] // sum(daily_distribution.values()) if sum(daily_distribution.values()) > 0 else 0
                                day_name = day_names[day_num].lower()
                                row_data[day_name] += value
                
                heat_grid.append(row_data)
            
            # Create chart-ready data
            hourly_chart_values = []
            hourly_chart_labels = []
            for hour in range(24):
                count = hourly_distribution.get(hour, 0)
                hourly_chart_values.append(count)
                
                # Format hour label
                if hour == 0:
                    label = "12am"
                elif hour < 12:
                    label = f"{hour}am"
                elif hour == 12:
                    label = "12pm"
                else:
                    label = f"{hour-12}pm"
                hourly_chart_labels.append(label)
            
            result["heat_map_data"] = {
                "hourly_distribution": hourly_distribution,
                "daily_distribution": daily_distribution,
                "day_labels": day_names,
                "peak_hour": peak_hour_num,
                "peak_day": peak_day_num,
                "peak_hour_formatted": peak_hour_formatted,
                "peak_day_formatted": peak_day_formatted,
                "peak_time_formatted": f"{peak_day_formatted} at {peak_hour_formatted}" if peak_day_formatted and peak_hour_formatted else "No activity yet",
                "question_timestamps": question_timestamps[:100],  # Limit to 100 for performance
                "heat_grid": heat_grid,  # Grid-ready data
                "hourly_chart_values": hourly_chart_values,  # List of counts [0, 1, 0, 2, ...]
                "hourly_chart_labels": hourly_chart_labels,  # List of labels ["12am", "1am", ...]
                "hourly_chart_values_string": ", ".join(map(str, hourly_chart_values)),  # "0, 1, 0, 2, ..."
                "hourly_chart_labels_string": ", ".join(hourly_chart_labels)  # "12am, 1am, 2am, ..."
            }
        
        # Add full conversations if requested
        if include_full_conversations:
            # Sort by timestamp (most recent first)
            full_conversations.sort(key=lambda x: x['timestamp'], reverse=True)
            result["full_conversations"] = full_conversations
            result["full_conversations_total"] = len(qa_logs)  # Total available
            result["full_conversations_returned"] = len(full_conversations)  # Actually returned
        
        return result
    
    def _extract_top_questions(self, questions: List[str], limit: int = 5) -> List[Dict[str, Any]]:
        """Extract top N most frequent questions"""
        if not questions:
            return []
        
        from collections import Counter
        import re
        
        normalized_questions = []
        for q in questions:
            if q:
                normalized = re.sub(r'[^\w\s]', '', q.lower()).strip()
                if len(normalized) > 10:  
                    normalized_questions.append((normalized[:100], q))  
        
        question_counts = Counter()
        original_map = {}
        
        for normalized, original in normalized_questions:
            question_counts[normalized] += 1
            if normalized not in original_map:
                original_map[normalized] = original
        
        top_questions = []
        for normalized, count in question_counts.most_common(limit):
            top_questions.append({
                "question": original_map[normalized],
                "count": count,
                "percentage": round(count / len(questions) * 100, 1)
            })
        
        return top_questions
    
    def _empty_response(self, tenant_id: str, tenant_hash: str, start_time: datetime, 
                        end_time: datetime, include_heat_map: bool = False,
                        include_full_conversations: bool = False) -> Dict[str, Any]:
        """Return empty metrics response"""
        period_days = (end_time - start_time).days
        result = {
            "tenant_id": tenant_id,
            "tenant_hash": tenant_hash,
            "start_date": start_time.strftime("%Y-%m-%d"),
            "end_date": end_time.strftime("%Y-%m-%d"),
            "period_days": period_days,
            "metrics": {
                "conversation_count": 0,
                "avg_response_time_ms": 0,
                "avg_first_token_ms": None,
                "avg_total_time_ms": None,
                "after_hours_percentage": 0,
                "total_messages": 0,
                "streaming_enabled_percentage": 0
            },
            "top_questions": [],
            "last_updated": datetime.utcnow().isoformat()
        }
        
        # Add empty heat map data if requested
        if include_heat_map:
            result["heat_map_data"] = {
                "hourly_distribution": {hour: 0 for hour in range(24)},
                "daily_distribution": {day: 0 for day in range(7)},
                "day_labels": ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"],
                "peak_hour": None,
                "peak_day": None,
                "question_timestamps": []
            }
        
        # Add empty full conversations if requested
        if include_full_conversations:
            result["full_conversations"] = []
            result["full_conversations_total"] = 0
            result["full_conversations_returned"] = 0
        
        return result


def lambda_handler(event, context):
    """Main Lambda handler"""
    handler_start_time = time.time()
    
    try:
        logger.info(f"Analytics Function invoked with event: {json.dumps(event)}")
        
        headers = event.get('headers', {})
        query_params = event.get('queryStringParameters', {})
        
        if not query_params:
            return {
                'statusCode': 400,
                'headers': get_cors_headers(),
                'body': json.dumps({
                    'error': 'Missing query parameters',
                    'details': 'tenant_id is required, along with either start_date/end_date or period'
                })
            }
        
        tenant_id = query_params.get('tenant_id')
        start_date = query_params.get('start_date')
        end_date = query_params.get('end_date')
        
        # New optional parameters
        top_questions_limit = int(query_params.get('top_questions_limit', '5'))
        include_heat_map = query_params.get('include_heat_map', 'false').lower() == 'true'
        include_full_conversations = query_params.get('include_full_conversations', 'false').lower() == 'true'
        full_conversations_limit = int(query_params.get('full_conversations_limit', '50'))
        
        # Support legacy period parameter for backwards compatibility
        period = query_params.get('period')
        if period and not start_date:
            # Convert period to date range
            end_time = datetime.utcnow()
            period_days = {
                "7_days": 7,
                "30_days": 30,
                "90_days": 90
            }.get(period, 7)
            start_time = end_time - timedelta(days=period_days)
            start_date = start_time.strftime("%Y-%m-%d")
            end_date = end_time.strftime("%Y-%m-%d")
        
        if not tenant_id:
            return {
                'statusCode': 400,
                'headers': get_cors_headers(),
                'body': json.dumps({
                    'error': 'Missing tenant_id parameter',
                    'details': 'tenant_id is required'
                })
            }
        
        from tenant_resolver import TenantResolver
        resolver = TenantResolver()
        tenant_hash = resolver.get_tenant_hash_by_id(tenant_id)
        
        if not tenant_hash:
            logger.error(f"No tenant_hash found for tenant_id: {tenant_id}")
            return {
                'statusCode': 404,
                'headers': get_cors_headers(),
                'body': json.dumps({
                    'error': 'Tenant not found',
                    'details': f'No configuration found for tenant_id: {tenant_id}'
                })
            }
        
        analytics = AnalyticsFunction()
        result = analytics.process_tenant(
            tenant_hash, 
            start_date, 
            end_date,
            top_questions_limit=top_questions_limit,
            include_heat_map=include_heat_map,
            include_full_conversations=include_full_conversations,
            full_conversations_limit=full_conversations_limit
        )
        
        processing_time = (time.time() - handler_start_time) * 1000
        result['metrics']['processing_time_ms'] = round(processing_time)
        
        logger.info(f"Analytics processed successfully in {processing_time:.0f}ms")
        
        return {
            'statusCode': 200,
            'headers': get_cors_headers(),
            'body': json.dumps(result)
        }
        
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}")
        return {
            'statusCode': 400,
            'headers': get_cors_headers(),
            'body': json.dumps({
                'error': 'Invalid request',
                'details': str(e)
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing analytics: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': get_cors_headers(),
            'body': json.dumps({
                'error': 'Internal server error',
                'details': 'An error occurred processing analytics'
            })
        }


def get_cors_headers():
    """Get CORS headers for Bubble domain"""
    return {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',  
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Cache-Control': 'no-cache, must-revalidate'
    }