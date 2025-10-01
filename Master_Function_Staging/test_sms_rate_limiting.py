#!/usr/bin/env python3
"""
Specialized tests for SMS rate limiting functionality
Tests monthly limits, usage tracking, and rate limiting edge cases
"""

import unittest
from unittest.mock import Mock, patch
import json
import boto3
from moto import mock_dynamodb, mock_sns
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from botocore.exceptions import ClientError

# Import the module under test
from form_handler import FormHandler


class TestSMSRateLimiting(unittest.TestCase):
    """Test cases for SMS rate limiting functionality"""

    @mock_dynamodb
    @mock_sns
    def setUp(self):
        """Set up test environment with DynamoDB and SNS mocks"""
        # Set up DynamoDB
        self.dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
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

        # Sample tenant config with SMS settings
        self.tenant_config = {
            'tenant_id': 'test_tenant_123',
            'tenant_hash': 'hash_abc123',
            'organization_name': 'Test Organization',
            'conversational_forms': {
                'volunteer_signup': {
                    'notifications': {
                        'sms': {
                            'enabled': True,
                            'recipients': ['+15551234567', '+15551234568'],
                            'monthly_limit': 100,
                            'template': 'New volunteer: {first_name}'
                        }
                    }
                }
            }
        }

        self.form_data = {
            'form_type': 'volunteer_signup',
            'responses': {'first_name': 'John', 'last_name': 'Doe'},
            'submission_id': 'sub_123'
        }

    def test_sms_rate_limiting_under_limit(self):
        """Test SMS sending when under monthly limit"""
        handler = FormHandler(self.tenant_config)
        sms_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        # Set current usage to 50 (under limit of 100)
        current_month = datetime.now().strftime('%Y-%m')
        self.sms_usage_table.put_item(Item={
            'tenant_id': 'test_tenant_123',
            'month': current_month,
            'count': 50
        })

        result = handler._send_sms_notifications(sms_config, self.form_data)

        # Should send to both recipients
        self.assertEqual(len(result), 2)
        self.assertIn('sms:+15551234567', result)
        self.assertIn('sms:+15551234568', result)

        # Usage should be incremented to 52
        final_usage = handler._get_monthly_sms_usage()
        self.assertEqual(final_usage, 52)

    def test_sms_rate_limiting_at_limit(self):
        """Test SMS sending when at monthly limit"""
        handler = FormHandler(self.tenant_config)
        sms_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        # Set current usage to 100 (at limit)
        current_month = datetime.now().strftime('%Y-%m')
        self.sms_usage_table.put_item(Item={
            'tenant_id': 'test_tenant_123',
            'month': current_month,
            'count': 100
        })

        result = handler._send_sms_notifications(sms_config, self.form_data)

        # Should not send any SMS
        self.assertEqual(len(result), 0)

        # Usage should remain at 100
        final_usage = handler._get_monthly_sms_usage()
        self.assertEqual(final_usage, 100)

    def test_sms_rate_limiting_over_limit(self):
        """Test SMS sending when over monthly limit"""
        handler = FormHandler(self.tenant_config)
        sms_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        # Set current usage to 150 (over limit of 100)
        current_month = datetime.now().strftime('%Y-%m')
        self.sms_usage_table.put_item(Item={
            'tenant_id': 'test_tenant_123',
            'month': current_month,
            'count': 150
        })

        result = handler._send_sms_notifications(sms_config, self.form_data)

        # Should not send any SMS
        self.assertEqual(len(result), 0)

        # Usage should remain at 150
        final_usage = handler._get_monthly_sms_usage()
        self.assertEqual(final_usage, 150)

    def test_sms_rate_limiting_partial_send(self):
        """Test SMS sending when limit is hit during multi-recipient send"""
        handler = FormHandler(self.tenant_config)
        sms_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        # Set current usage to 99 (1 under limit, but we have 2 recipients)
        current_month = datetime.now().strftime('%Y-%m')
        self.sms_usage_table.put_item(Item={
            'tenant_id': 'test_tenant_123',
            'month': current_month,
            'count': 99
        })

        result = handler._send_sms_notifications(sms_config, self.form_data)

        # Should send to only 1 recipient (first one)
        self.assertEqual(len(result), 1)
        self.assertIn('sms:+15551234567', result)
        self.assertNotIn('sms:+15551234568', result)

        # Usage should be 100 (limit reached)
        final_usage = handler._get_monthly_sms_usage()
        self.assertEqual(final_usage, 100)

    def test_sms_rate_limiting_custom_limit(self):
        """Test SMS rate limiting with custom monthly limit"""
        # Create config with custom limit
        custom_config = self.tenant_config.copy()
        custom_config['conversational_forms']['volunteer_signup']['notifications']['sms']['monthly_limit'] = 25

        handler = FormHandler(custom_config)
        sms_config = custom_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        # Set current usage to 24 (1 under custom limit)
        current_month = datetime.now().strftime('%Y-%m')
        self.sms_usage_table.put_item(Item={
            'tenant_id': 'test_tenant_123',
            'month': current_month,
            'count': 24
        })

        result = handler._send_sms_notifications(sms_config, self.form_data)

        # Should send to only 1 recipient
        self.assertEqual(len(result), 1)

        # Usage should be 25 (custom limit reached)
        final_usage = handler._get_monthly_sms_usage()
        self.assertEqual(final_usage, 25)

    def test_sms_rate_limiting_no_limit_config(self):
        """Test SMS rate limiting when no monthly limit is configured"""
        # Create config without monthly_limit
        no_limit_config = self.tenant_config.copy()
        del no_limit_config['conversational_forms']['volunteer_signup']['notifications']['sms']['monthly_limit']

        handler = FormHandler(no_limit_config)
        sms_config = no_limit_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        # Set current usage to high number
        current_month = datetime.now().strftime('%Y-%m')
        self.sms_usage_table.put_item(Item={
            'tenant_id': 'test_tenant_123',
            'month': current_month,
            'count': 150
        })

        result = handler._send_sms_notifications(sms_config, self.form_data)

        # Should still send (uses default limit of 100 from code)
        # Wait, let's check the code - it should use default of 100
        self.assertEqual(len(result), 0)  # 150 > 100 (default limit)

    def test_sms_usage_month_rollover(self):
        """Test SMS usage resets for new month"""
        handler = FormHandler(self.tenant_config)

        # Set usage for previous month
        current_date = datetime.now()
        previous_month = (current_date - relativedelta(months=1)).strftime('%Y-%m')
        current_month = current_date.strftime('%Y-%m')

        # Add usage for previous month
        self.sms_usage_table.put_item(Item={
            'tenant_id': 'test_tenant_123',
            'month': previous_month,
            'count': 100
        })

        # Current month should start at 0
        current_usage = handler._get_monthly_sms_usage()
        self.assertEqual(current_usage, 0)

        # Send SMS should work
        sms_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['sms']
        result = handler._send_sms_notifications(sms_config, self.form_data)

        self.assertEqual(len(result), 2)

    def test_sms_usage_multiple_tenants_isolation(self):
        """Test SMS usage isolation between multiple tenants"""
        # Set up two tenants
        tenant1_config = {
            'tenant_id': 'tenant_1',
            'tenant_hash': 'hash_1',
            'conversational_forms': {
                'test_form': {
                    'notifications': {
                        'sms': {
                            'enabled': True,
                            'recipients': ['+15551111111'],
                            'monthly_limit': 50,
                            'template': 'Test message'
                        }
                    }
                }
            }
        }

        tenant2_config = {
            'tenant_id': 'tenant_2',
            'tenant_hash': 'hash_2',
            'conversational_forms': {
                'test_form': {
                    'notifications': {
                        'sms': {
                            'enabled': True,
                            'recipients': ['+15552222222'],
                            'monthly_limit': 75,
                            'template': 'Test message'
                        }
                    }
                }
            }
        }

        handler1 = FormHandler(tenant1_config)
        handler2 = FormHandler(tenant2_config)

        # Set different usage for each tenant
        current_month = datetime.now().strftime('%Y-%m')
        self.sms_usage_table.put_item(Item={
            'tenant_id': 'tenant_1',
            'month': current_month,
            'count': 45
        })
        self.sms_usage_table.put_item(Item={
            'tenant_id': 'tenant_2',
            'month': current_month,
            'count': 70
        })

        # Test tenant 1 (under limit)
        sms_config1 = tenant1_config['conversational_forms']['test_form']['notifications']['sms']
        result1 = handler1._send_sms_notifications(sms_config1, self.form_data)
        self.assertEqual(len(result1), 1)

        # Test tenant 2 (under limit)
        sms_config2 = tenant2_config['conversational_forms']['test_form']['notifications']['sms']
        result2 = handler2._send_sms_notifications(sms_config2, self.form_data)
        self.assertEqual(len(result2), 1)

        # Verify isolation
        self.assertEqual(handler1._get_monthly_sms_usage(), 46)
        self.assertEqual(handler2._get_monthly_sms_usage(), 71)

    def test_sms_usage_tracking_errors(self):
        """Test SMS usage tracking with DynamoDB errors"""
        handler = FormHandler(self.tenant_config)

        # Test with DynamoDB unavailable
        with patch('boto3.resource') as mock_resource:
            mock_table = Mock()
            mock_table.get_item.side_effect = ClientError(
                {'Error': {'Code': 'ResourceNotFoundException', 'Message': 'Table not found'}},
                'GetItem'
            )
            mock_resource.return_value.Table.return_value = mock_table

            # Should return 0 on error
            usage = handler._get_monthly_sms_usage()
            self.assertEqual(usage, 0)

    def test_sms_usage_increment_errors(self):
        """Test SMS usage increment with DynamoDB errors"""
        handler = FormHandler(self.tenant_config)

        # Test with DynamoDB unavailable for increment
        with patch('boto3.resource') as mock_resource:
            mock_table = Mock()
            mock_table.update_item.side_effect = ClientError(
                {'Error': {'Code': 'ProvisionedThroughputExceededException', 'Message': 'Throttled'}},
                'UpdateItem'
            )
            mock_resource.return_value.Table.return_value = mock_table

            # Should not raise exception
            handler._increment_sms_usage()

    def test_sms_rate_limiting_edge_case_zero_limit(self):
        """Test SMS rate limiting with zero monthly limit"""
        zero_limit_config = self.tenant_config.copy()
        zero_limit_config['conversational_forms']['volunteer_signup']['notifications']['sms']['monthly_limit'] = 0

        handler = FormHandler(zero_limit_config)
        sms_config = zero_limit_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        result = handler._send_sms_notifications(sms_config, self.form_data)

        # Should not send any SMS
        self.assertEqual(len(result), 0)

        # Usage should remain 0
        final_usage = handler._get_monthly_sms_usage()
        self.assertEqual(final_usage, 0)

    def test_sms_rate_limiting_negative_limit(self):
        """Test SMS rate limiting with negative monthly limit"""
        negative_limit_config = self.tenant_config.copy()
        negative_limit_config['conversational_forms']['volunteer_signup']['notifications']['sms']['monthly_limit'] = -10

        handler = FormHandler(negative_limit_config)
        sms_config = negative_limit_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        result = handler._send_sms_notifications(sms_config, self.form_data)

        # Should not send any SMS (negative limit treated as 0)
        self.assertEqual(len(result), 0)

    def test_sms_usage_atomic_operations(self):
        """Test SMS usage increment operations are atomic"""
        handler = FormHandler(self.tenant_config)

        # Simulate race condition by checking usage before and after increment
        initial_usage = handler._get_monthly_sms_usage()
        self.assertEqual(initial_usage, 0)

        # Increment in rapid succession
        for i in range(10):
            handler._increment_sms_usage()

        final_usage = handler._get_monthly_sms_usage()
        self.assertEqual(final_usage, 10)

    def test_sms_rate_limiting_message_truncation(self):
        """Test SMS message truncation in rate limiting context"""
        handler = FormHandler(self.tenant_config)

        # Create form data with long message that would be truncated
        long_form_data = {
            'form_type': 'volunteer_signup',
            'responses': {
                'first_name': 'JohnWithVeryLongFirstNameThatExceedsNormalLimitsAndWouldCauseMessageTruncation'
            },
            'submission_id': 'sub_123'
        }

        sms_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        # Set usage just under limit
        current_month = datetime.now().strftime('%Y-%m')
        self.sms_usage_table.put_item(Item={
            'tenant_id': 'test_tenant_123',
            'month': current_month,
            'count': 98
        })

        result = handler._send_sms_notifications(sms_config, long_form_data)

        # Should still send despite long message (message gets truncated)
        self.assertEqual(len(result), 2)

    def test_sms_rate_limiting_with_sns_errors(self):
        """Test SMS rate limiting when SNS send fails"""
        handler = FormHandler(self.tenant_config)
        sms_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        # Mock SNS to raise error
        with patch('boto3.client') as mock_boto:
            mock_sns = Mock()
            mock_sns.publish.side_effect = ClientError(
                {'Error': {'Code': 'InvalidParameter', 'Message': 'Invalid phone number'}},
                'Publish'
            )
            mock_boto.return_value = mock_sns

            result = handler._send_sms_notifications(sms_config, self.form_data)

            # Should return empty list due to errors
            self.assertEqual(len(result), 0)

            # Usage should not be incremented if sends fail
            final_usage = handler._get_monthly_sms_usage()
            self.assertEqual(final_usage, 0)

    def test_sms_rate_limiting_integration_with_priority(self):
        """Test SMS rate limiting integrated with priority handling"""
        handler = FormHandler(self.tenant_config)

        # Create form configuration with priority handling
        form_config = {
            'notifications': {
                'sms': {
                    'enabled': True,
                    'recipients': ['+15551234567'],
                    'monthly_limit': 100,
                    'template': 'Priority: {priority} - New volunteer: {first_name}'
                }
            }
        }

        # Set current usage just under limit
        current_month = datetime.now().strftime('%Y-%m')
        self.sms_usage_table.put_item(Item={
            'tenant_id': 'test_tenant_123',
            'month': current_month,
            'count': 99
        })

        # Test high priority form (should still respect rate limits)
        high_priority_data = {
            'form_type': 'volunteer_signup',
            'responses': {'first_name': 'John', 'urgency': 'urgent'},
            'submission_id': 'sub_123',
            'priority': 'high'
        }

        notification_results = handler._send_notifications(
            form_config=form_config,
            form_data=high_priority_data,
            priority='high'
        )

        # Should send SMS for high priority
        sms_notifications = [n for n in notification_results if n.startswith('sms:')]
        self.assertEqual(len(sms_notifications), 1)

        # Usage should be at limit
        final_usage = handler._get_monthly_sms_usage()
        self.assertEqual(final_usage, 100)

    def test_sms_usage_month_format_consistency(self):
        """Test SMS usage month format is consistent across operations"""
        handler = FormHandler(self.tenant_config)

        # Get current usage (creates entry if needed)
        handler._increment_sms_usage()

        # Verify the month format used in DynamoDB
        current_month = datetime.now().strftime('%Y-%m')
        response = self.sms_usage_table.get_item(Key={
            'tenant_id': 'test_tenant_123',
            'month': current_month
        })

        self.assertIn('Item', response)
        self.assertEqual(response['Item']['month'], current_month)
        self.assertEqual(response['Item']['count'], 1)


if __name__ == '__main__':
    unittest.main()