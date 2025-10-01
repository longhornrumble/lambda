#!/usr/bin/env python3
"""
Comprehensive tests for DynamoDB operations and schema validation
Tests form submissions, SMS usage tracking, and audit logging
"""

import unittest
from unittest.mock import Mock, patch
import json
import boto3
from moto import mock_dynamodb
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import uuid

# Import modules under test
from form_handler import FormHandler


class TestDynamoDBSchemas(unittest.TestCase):
    """Test DynamoDB table schemas and operations"""

    @mock_dynamodb
    def setUp(self):
        """Set up DynamoDB tables with proper schemas"""
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        self.dynamodb_client = boto3.client('dynamodb', region_name='us-east-1')

        # Create form submissions table
        self.submissions_table = self.dynamodb.create_table(
            TableName='picasso_form_submissions',
            KeySchema=[
                {'AttributeName': 'submission_id', 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'submission_id', 'AttributeType': 'S'}
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'tenant-timestamp-index',
                    'KeySchema': [
                        {'AttributeName': 'tenant_id', 'KeyType': 'HASH'},
                        {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
                    ],
                    'Projection': {'ProjectionType': 'ALL'},
                    'ProvisionedThroughput': {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
                }
            ],
            AttributeDefinitions=[
                {'AttributeName': 'submission_id', 'AttributeType': 'S'},
                {'AttributeName': 'tenant_id', 'AttributeType': 'S'},
                {'AttributeName': 'timestamp', 'AttributeType': 'S'}
            ],
            BillingMode='PROVISIONED',
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )

        # Create SMS usage table
        self.sms_usage_table = self.dynamodb.create_table(
            TableName='picasso_sms_usage',
            KeySchema=[
                {'AttributeName': 'tenant_id', 'KeyType': 'HASH'},
                {'AttributeName': 'month', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'tenant_id', 'AttributeType': 'S'},
                {'AttributeName': 'month', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )

        # Create audit logs table
        self.audit_table = self.dynamodb.create_table(
            TableName='picasso_audit_logs',
            KeySchema=[
                {'AttributeName': 'tenant_id', 'KeyType': 'HASH'},
                {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'tenant_id', 'AttributeType': 'S'},
                {'AttributeName': 'timestamp', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )

        # Create form templates table (future use)
        self.templates_table = self.dynamodb.create_table(
            TableName='picasso_form_templates',
            KeySchema=[
                {'AttributeName': 'tenant_id', 'KeyType': 'HASH'},
                {'AttributeName': 'template_id', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'tenant_id', 'AttributeType': 'S'},
                {'AttributeName': 'template_id', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )

        # Sample tenant config
        self.tenant_config = {
            'tenant_id': 'test_tenant_123',
            'tenant_hash': 'hash_abc123',
            'organization_name': 'Test Organization'
        }

    def test_form_submissions_table_schema(self):
        """Test form submissions table schema and required fields"""
        # Verify table exists
        table_description = self.dynamodb_client.describe_table(TableName='picasso_form_submissions')
        table = table_description['Table']

        # Check key schema
        self.assertEqual(len(table['KeySchema']), 1)
        self.assertEqual(table['KeySchema'][0]['AttributeName'], 'submission_id')
        self.assertEqual(table['KeySchema'][0]['KeyType'], 'HASH')

        # Check GSI for tenant queries
        gsi = table['GlobalSecondaryIndexes'][0]
        self.assertEqual(gsi['IndexName'], 'tenant-timestamp-index')
        self.assertEqual(len(gsi['KeySchema']), 2)

    def test_sms_usage_table_schema(self):
        """Test SMS usage table schema for rate limiting"""
        table_description = self.dynamodb_client.describe_table(TableName='picasso_sms_usage')
        table = table_description['Table']

        # Check composite key for tenant and month
        self.assertEqual(len(table['KeySchema']), 2)
        hash_key = next(k for k in table['KeySchema'] if k['KeyType'] == 'HASH')
        range_key = next(k for k in table['KeySchema'] if k['KeyType'] == 'RANGE')

        self.assertEqual(hash_key['AttributeName'], 'tenant_id')
        self.assertEqual(range_key['AttributeName'], 'month')

    def test_audit_logs_table_schema(self):
        """Test audit logs table schema for tenant-based querying"""
        table_description = self.dynamodb_client.describe_table(TableName='picasso_audit_logs')
        table = table_description['Table']

        # Check composite key for tenant and timestamp
        self.assertEqual(len(table['KeySchema']), 2)
        hash_key = next(k for k in table['KeySchema'] if k['KeyType'] == 'HASH')
        range_key = next(k for k in table['KeySchema'] if k['KeyType'] == 'RANGE')

        self.assertEqual(hash_key['AttributeName'], 'tenant_id')
        self.assertEqual(range_key['AttributeName'], 'timestamp')

    def test_form_templates_table_schema(self):
        """Test form templates table schema for future template management"""
        table_description = self.dynamodb_client.describe_table(TableName='picasso_form_templates')
        table = table_description['Table']

        # Check composite key for tenant and template
        self.assertEqual(len(table['KeySchema']), 2)
        hash_key = next(k for k in table['KeySchema'] if k['KeyType'] == 'HASH')
        range_key = next(k for k in table['KeySchema'] if k['KeyType'] == 'RANGE')

        self.assertEqual(hash_key['AttributeName'], 'tenant_id')
        self.assertEqual(range_key['AttributeName'], 'template_id')

    def test_store_form_submission_complete_record(self):
        """Test storing complete form submission record"""
        handler = FormHandler(self.tenant_config)

        submission_id = handler._store_submission(
            form_type='volunteer_signup',
            responses={
                'first_name': 'John',
                'last_name': 'Doe',
                'email': 'john@example.com',
                'phone': '+15551234567',
                'availability': 'Weekends',
                'skills': ['cooking', 'organizing'],
                'emergency_contact': {
                    'name': 'Jane Doe',
                    'phone': '+15551234568'
                }
            },
            session_id='session_12345',
            conversation_id='conv_67890'
        )

        # Verify record was stored
        response = self.submissions_table.get_item(Key={'submission_id': submission_id})
        self.assertIn('Item', response)

        item = response['Item']

        # Verify all required fields
        self.assertEqual(item['submission_id'], submission_id)
        self.assertEqual(item['tenant_id'], 'test_tenant_123')
        self.assertEqual(item['tenant_hash'], 'hash_abc123')
        self.assertEqual(item['form_type'], 'volunteer_signup')
        self.assertEqual(item['session_id'], 'session_12345')
        self.assertEqual(item['conversation_id'], 'conv_67890')
        self.assertEqual(item['status'], 'submitted')
        self.assertIn('timestamp', item)

        # Verify nested responses structure
        responses = item['responses']
        self.assertEqual(responses['first_name'], 'John')
        self.assertEqual(responses['email'], 'john@example.com')
        self.assertIn('cooking', responses['skills'])
        self.assertEqual(responses['emergency_contact']['name'], 'Jane Doe')

    def test_store_form_submission_minimal_record(self):
        """Test storing minimal form submission record"""
        handler = FormHandler(self.tenant_config)

        submission_id = handler._store_submission(
            form_type='newsletter_signup',
            responses={'email': 'subscriber@example.com'},
            session_id='session_minimal',
            conversation_id='conv_minimal'
        )

        # Verify minimal record was stored
        response = self.submissions_table.get_item(Key={'submission_id': submission_id})
        item = response['Item']

        self.assertEqual(item['form_type'], 'newsletter_signup')
        self.assertEqual(len(item['responses']), 1)
        self.assertEqual(item['responses']['email'], 'subscriber@example.com')

    def test_store_form_submission_unicode_content(self):
        """Test storing form submission with Unicode content"""
        handler = FormHandler(self.tenant_config)

        submission_id = handler._store_submission(
            form_type='contact',
            responses={
                'first_name': 'José',
                'last_name': 'García',
                'message': 'Necesito ayuda con vivienda. ¡Gracias! 🏠',
                'preferred_language': 'Español'
            },
            session_id='session_unicode',
            conversation_id='conv_unicode'
        )

        # Verify Unicode content was stored correctly
        response = self.submissions_table.get_item(Key={'submission_id': submission_id})
        item = response['Item']

        self.assertEqual(item['responses']['first_name'], 'José')
        self.assertEqual(item['responses']['last_name'], 'García')
        self.assertIn('🏠', item['responses']['message'])
        self.assertEqual(item['responses']['preferred_language'], 'Español')

    def test_sms_usage_tracking_new_month(self):
        """Test SMS usage tracking for new month"""
        handler = FormHandler(self.tenant_config)

        # Should start with 0 usage for new month
        usage = handler._get_monthly_sms_usage()
        self.assertEqual(usage, 0)

        # Increment usage
        handler._increment_sms_usage()

        # Should now be 1
        usage = handler._get_monthly_sms_usage()
        self.assertEqual(usage, 1)

    def test_sms_usage_tracking_existing_month(self):
        """Test SMS usage tracking for existing month"""
        current_month = datetime.now().strftime('%Y-%m')

        # Pre-populate usage
        self.sms_usage_table.put_item(Item={
            'tenant_id': 'test_tenant_123',
            'month': current_month,
            'count': 25
        })

        handler = FormHandler(self.tenant_config)

        # Should return existing usage
        usage = handler._get_monthly_sms_usage()
        self.assertEqual(usage, 25)

        # Increment usage
        handler._increment_sms_usage()

        # Should now be 26
        usage = handler._get_monthly_sms_usage()
        self.assertEqual(usage, 26)

    def test_sms_usage_multiple_increments(self):
        """Test multiple SMS usage increments"""
        handler = FormHandler(self.tenant_config)

        # Increment multiple times
        for i in range(5):
            handler._increment_sms_usage()

        # Should be 5
        usage = handler._get_monthly_sms_usage()
        self.assertEqual(usage, 5)

    def test_sms_usage_different_tenants(self):
        """Test SMS usage isolation between tenants"""
        tenant1_config = {'tenant_id': 'tenant_1', 'tenant_hash': 'hash_1'}
        tenant2_config = {'tenant_id': 'tenant_2', 'tenant_hash': 'hash_2'}

        handler1 = FormHandler(tenant1_config)
        handler2 = FormHandler(tenant2_config)

        # Increment for tenant 1
        handler1._increment_sms_usage()
        handler1._increment_sms_usage()

        # Increment for tenant 2
        handler2._increment_sms_usage()

        # Verify isolation
        self.assertEqual(handler1._get_monthly_sms_usage(), 2)
        self.assertEqual(handler2._get_monthly_sms_usage(), 1)

    def test_audit_logging_form_submission(self):
        """Test audit logging for form submissions"""
        handler = FormHandler(self.tenant_config)

        handler._audit_submission(
            submission_id='sub_123',
            form_type='volunteer_signup',
            notification_results=['email:admin@test.com', 'sms:+15551234567'],
            fulfillment_result={'type': 'email', 'status': 'sent'}
        )

        # Verify audit log was created
        # Query by tenant_id (note: timestamp will vary, so we scan)
        response = self.audit_table.query(
            KeyConditionExpression='tenant_id = :tid',
            ExpressionAttributeValues={':tid': 'test_tenant_123'}
        )

        self.assertEqual(len(response['Items']), 1)
        item = response['Items'][0]

        self.assertEqual(item['event_type'], 'form_submission')
        self.assertEqual(item['submission_id'], 'sub_123')
        self.assertEqual(item['form_type'], 'volunteer_signup')
        self.assertIn('email:admin@test.com', item['notifications'])
        self.assertEqual(item['fulfillment']['type'], 'email')

    def test_audit_logging_multiple_events(self):
        """Test multiple audit log entries"""
        handler = FormHandler(self.tenant_config)

        # Create multiple audit entries
        for i in range(3):
            handler._audit_submission(
                submission_id=f'sub_{i}',
                form_type='contact',
                notification_results=[f'email:user{i}@test.com'],
                fulfillment_result={'type': 'none'}
            )

        # Verify all entries were created
        response = self.audit_table.query(
            KeyConditionExpression='tenant_id = :tid',
            ExpressionAttributeValues={':tid': 'test_tenant_123'}
        )

        self.assertEqual(len(response['Items']), 3)

        # Verify they're sorted by timestamp (most recent first)
        timestamps = [item['timestamp'] for item in response['Items']]
        self.assertEqual(timestamps, sorted(timestamps, reverse=True))

    def test_error_handling_dynamodb_failures(self):
        """Test error handling when DynamoDB operations fail"""
        # Create handler with invalid config to trigger potential errors
        invalid_config = {'tenant_id': None, 'tenant_hash': None}
        handler = FormHandler(invalid_config)

        # Test SMS usage with None tenant_id
        usage = handler._get_monthly_sms_usage()
        self.assertEqual(usage, 0)  # Should return 0 on error

        # Test increment with None tenant_id
        # Should not raise exception
        handler._increment_sms_usage()

    def test_query_submissions_by_tenant(self):
        """Test querying submissions by tenant using GSI"""
        handler = FormHandler(self.tenant_config)

        # Store multiple submissions
        submission_ids = []
        for i in range(3):
            sub_id = handler._store_submission(
                form_type=f'form_type_{i}',
                responses={'field': f'value_{i}'},
                session_id=f'session_{i}',
                conversation_id=f'conv_{i}'
            )
            submission_ids.append(sub_id)

        # Wait for eventual consistency (not needed in moto, but good practice)
        import time
        time.sleep(0.1)

        # Query by tenant using GSI (if implemented)
        # This would be used by admin interfaces to view tenant submissions
        response = self.dynamodb_client.query(
            TableName='picasso_form_submissions',
            IndexName='tenant-timestamp-index',
            KeyConditionExpression='tenant_id = :tid',
            ExpressionAttributeValues={':tid': {'S': 'test_tenant_123'}}
        )

        # Should return all 3 submissions
        self.assertEqual(len(response['Items']), 3)

    def test_data_types_and_limits(self):
        """Test various data types and size limits"""
        handler = FormHandler(self.tenant_config)

        # Test with large text data
        large_message = 'x' * 1000  # 1KB message

        submission_id = handler._store_submission(
            form_type='large_data_test',
            responses={
                'large_text': large_message,
                'number_value': 42,
                'boolean_value': True,
                'list_value': ['item1', 'item2', 'item3'],
                'nested_object': {
                    'level1': {
                        'level2': 'deep_value'
                    }
                }
            },
            session_id='session_large',
            conversation_id='conv_large'
        )

        # Verify large data was stored
        response = self.submissions_table.get_item(Key={'submission_id': submission_id})
        item = response['Item']

        self.assertEqual(len(item['responses']['large_text']), 1000)
        self.assertEqual(item['responses']['number_value'], 42)
        self.assertTrue(item['responses']['boolean_value'])
        self.assertEqual(len(item['responses']['list_value']), 3)
        self.assertEqual(item['responses']['nested_object']['level1']['level2'], 'deep_value')

    def test_timestamp_formats(self):
        """Test timestamp format consistency"""
        handler = FormHandler(self.tenant_config)

        # Store submission and audit log
        submission_id = handler._store_submission(
            form_type='timestamp_test',
            responses={'test': 'data'},
            session_id='session_timestamp',
            conversation_id='conv_timestamp'
        )

        handler._audit_submission(
            submission_id=submission_id,
            form_type='timestamp_test',
            notification_results=[],
            fulfillment_result={}
        )

        # Verify timestamp formats are ISO 8601 with timezone
        submission = self.submissions_table.get_item(Key={'submission_id': submission_id})['Item']

        # Should be able to parse the timestamp
        timestamp_str = submission['timestamp']
        parsed_timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        self.assertIsInstance(parsed_timestamp, datetime)

    def test_concurrent_sms_usage_updates(self):
        """Test concurrent SMS usage updates (atomic operations)"""
        handler = FormHandler(self.tenant_config)

        # Simulate concurrent increments
        # DynamoDB's ADD operation is atomic
        import threading
        import time

        def increment_usage():
            handler._increment_sms_usage()

        # Start multiple threads
        threads = []
        for i in range(5):
            thread = threading.Thread(target=increment_usage)
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Final usage should be exactly 5
        final_usage = handler._get_monthly_sms_usage()
        self.assertEqual(final_usage, 5)


if __name__ == '__main__':
    unittest.main()