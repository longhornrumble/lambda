#!/usr/bin/env python3
"""
Comprehensive tests for notification sending with mocked AWS services
Tests email (SES), SMS (SNS), and webhook integrations
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import boto3
from moto import mock_ses, mock_sns, mock_dynamodb
from botocore.exceptions import ClientError
import requests
from datetime import datetime

# Import the module under test
from form_handler import FormHandler


class TestNotificationServices(unittest.TestCase):
    """Test cases for multi-channel notification sending"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        self.tenant_config = {
            'tenant_id': 'test_tenant_123',
            'tenant_hash': 'hash_abc123',
            'organization_name': 'Test Community Center',
            'from_email': 'noreply@testcenter.org',
            'conversational_forms': {
                'volunteer_signup': {
                    'notifications': {
                        'email': {
                            'enabled': True,
                            'recipients': ['volunteer@testcenter.org', 'admin@testcenter.org'],
                            'sender': 'noreply@testcenter.org',
                            'subject': 'New Volunteer: {first_name} {last_name}',
                            'template': 'volunteer_notification',
                            'high_template': 'urgent_volunteer_notification'
                        },
                        'sms': {
                            'enabled': True,
                            'recipients': ['+15551234567', '+15559876543'],
                            'monthly_limit': 100,
                            'template': 'New volunteer: {first_name}. Call {emergency_phone}'
                        },
                        'webhook': {
                            'enabled': True,
                            'url': 'https://api.testcenter.org/webhook/volunteers',
                            'headers': {
                                'Authorization': 'Bearer secret123',
                                'X-Source': 'Picasso-Forms'
                            }
                        }
                    }
                }
            }
        }

        self.form_data = {
            'form_type': 'volunteer_signup',
            'responses': {
                'first_name': 'John',
                'last_name': 'Doe',
                'email': 'john.doe@example.com',
                'phone': '+15551234567',
                'availability': 'Weekends',
                'emergency_contact': 'Jane Doe (+15559999999)'
            },
            'submission_id': 'sub_12345',
            'priority': 'normal'
        }

    @mock_ses
    def test_send_email_notifications_success(self):
        """Test successful email notification sending via SES"""
        handler = FormHandler(self.tenant_config)
        email_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['email']

        result = handler._send_email_notifications(email_config, self.form_data, 'normal')

        # Should send to both recipients
        self.assertEqual(len(result), 2)
        self.assertIn('email:volunteer@testcenter.org', result)
        self.assertIn('email:admin@testcenter.org', result)

    @mock_ses
    def test_send_email_notifications_high_priority(self):
        """Test email notifications with high priority template"""
        handler = FormHandler(self.tenant_config)
        email_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['email']

        result = handler._send_email_notifications(email_config, self.form_data, 'high')

        # Should use high priority template and send to both recipients
        self.assertEqual(len(result), 2)

    @mock_ses
    def test_send_email_notifications_custom_sender(self):
        """Test email notifications with custom sender address"""
        # Modify config to use custom sender
        custom_config = self.tenant_config.copy()
        custom_config['conversational_forms']['volunteer_signup']['notifications']['email']['sender'] = 'custom@testcenter.org'

        handler = FormHandler(custom_config)
        email_config = custom_config['conversational_forms']['volunteer_signup']['notifications']['email']

        with patch('boto3.client') as mock_boto:
            mock_ses = Mock()
            mock_ses.send_email.return_value = {'MessageId': 'test-message-id'}
            mock_boto.return_value = mock_ses

            result = handler._send_email_notifications(email_config, self.form_data, 'normal')

            # Verify custom sender was used
            calls = mock_ses.send_email.call_args_list
            for call in calls:
                self.assertEqual(call[1]['Source'], 'custom@testcenter.org')

    @mock_ses
    def test_send_email_notifications_ses_error(self):
        """Test email notification handling when SES returns error"""
        handler = FormHandler(self.tenant_config)
        email_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['email']

        with patch('boto3.client') as mock_boto:
            mock_ses = Mock()
            mock_ses.send_email.side_effect = ClientError(
                {'Error': {'Code': 'MessageRejected', 'Message': 'Email address not verified'}},
                'SendEmail'
            )
            mock_boto.return_value = mock_ses

            result = handler._send_email_notifications(email_config, self.form_data, 'normal')

            # Should return empty list due to errors
            self.assertEqual(len(result), 0)

    @mock_ses
    def test_send_email_notifications_partial_failure(self):
        """Test email notification with partial failures"""
        handler = FormHandler(self.tenant_config)
        email_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['email']

        with patch('boto3.client') as mock_boto:
            mock_ses = Mock()

            # First call succeeds, second fails
            mock_ses.send_email.side_effect = [
                {'MessageId': 'success-message-id'},
                ClientError(
                    {'Error': {'Code': 'MessageRejected', 'Message': 'Invalid recipient'}},
                    'SendEmail'
                )
            ]
            mock_boto.return_value = mock_ses

            result = handler._send_email_notifications(email_config, self.form_data, 'normal')

            # Should have one success
            self.assertEqual(len(result), 1)
            self.assertIn('email:volunteer@testcenter.org', result)

    @mock_sns
    @mock_dynamodb
    def test_send_sms_notifications_success(self):
        """Test successful SMS notification sending via SNS"""
        # Set up SMS usage table
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb.create_table(
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

        handler = FormHandler(self.tenant_config)
        sms_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        result = handler._send_sms_notifications(sms_config, self.form_data)

        # Should send to both recipients
        self.assertEqual(len(result), 2)
        self.assertIn('sms:+15551234567', result)
        self.assertIn('sms:+15559876543', result)

    @mock_sns
    @mock_dynamodb
    def test_send_sms_notifications_message_formatting(self):
        """Test SMS notification message formatting with template variables"""
        # Set up SMS usage table
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb.create_table(
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

        handler = FormHandler(self.tenant_config)
        sms_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        with patch('boto3.client') as mock_boto:
            mock_sns = Mock()
            mock_sns.publish.return_value = {'MessageId': 'test-sms-id'}
            mock_boto.return_value = mock_sns

            result = handler._send_sms_notifications(sms_config, self.form_data)

            # Verify message was formatted correctly
            calls = mock_sns.publish.call_args_list
            for call in calls:
                message = call[1]['Message']
                self.assertIn('John', message)  # first_name variable
                # Note: emergency_phone might not be in form_data, so template will leave placeholder

    @mock_sns
    @mock_dynamodb
    def test_send_sms_notifications_sns_error(self):
        """Test SMS notification handling when SNS returns error"""
        # Set up SMS usage table
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb.create_table(
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

        handler = FormHandler(self.tenant_config)
        sms_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['sms']

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

    @patch('requests.post')
    def test_send_webhook_notifications_success(self):
        """Test successful webhook notification sending"""
        handler = FormHandler(self.tenant_config)
        webhook_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['webhook']

        # Mock successful HTTP response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests_post = Mock(return_value=mock_response)

        with patch('requests.post', mock_requests_post):
            result = handler._send_webhook_notifications(webhook_config, self.form_data)

            # Should indicate successful webhook
            self.assertEqual(len(result), 1)
            self.assertIn('webhook:200', result)

            # Verify request was made correctly
            mock_requests_post.assert_called_once()
            call_args = mock_requests_post.call_args

            # Check URL
            self.assertEqual(call_args[1]['url'], 'https://api.testcenter.org/webhook/volunteers')

            # Check headers
            expected_headers = {
                'Authorization': 'Bearer secret123',
                'X-Source': 'Picasso-Forms',
                'Content-Type': 'application/json'
            }
            self.assertEqual(call_args[1]['headers'], expected_headers)

            # Check payload
            self.assertEqual(call_args[1]['json'], self.form_data)
            self.assertEqual(call_args[1]['timeout'], 10)

    @patch('requests.post')
    def test_send_webhook_notifications_http_error(self):
        """Test webhook notification with HTTP error response"""
        handler = FormHandler(self.tenant_config)
        webhook_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['webhook']

        # Mock HTTP error response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_requests_post = Mock(return_value=mock_response)

        with patch('requests.post', mock_requests_post):
            result = handler._send_webhook_notifications(webhook_config, self.form_data)

            # Should return empty list due to error
            self.assertEqual(len(result), 0)

    @patch('requests.post')
    def test_send_webhook_notifications_connection_error(self):
        """Test webhook notification with connection error"""
        handler = FormHandler(self.tenant_config)
        webhook_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['webhook']

        # Mock connection error
        mock_requests_post = Mock(side_effect=requests.ConnectionError('Connection failed'))

        with patch('requests.post', mock_requests_post):
            result = handler._send_webhook_notifications(webhook_config, self.form_data)

            # Should return empty list due to exception
            self.assertEqual(len(result), 0)

    @patch('requests.post')
    def test_send_webhook_notifications_timeout(self):
        """Test webhook notification with timeout error"""
        handler = FormHandler(self.tenant_config)
        webhook_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['webhook']

        # Mock timeout error
        mock_requests_post = Mock(side_effect=requests.Timeout('Request timed out'))

        with patch('requests.post', mock_requests_post):
            result = handler._send_webhook_notifications(webhook_config, self.form_data)

            # Should return empty list due to timeout
            self.assertEqual(len(result), 0)

    @patch('requests.post')
    def test_send_webhook_notifications_custom_headers(self):
        """Test webhook notification with custom headers preserved"""
        # Modify config to include additional custom headers
        custom_config = self.tenant_config.copy()
        custom_config['conversational_forms']['volunteer_signup']['notifications']['webhook']['headers'].update({
            'X-API-Version': 'v2',
            'X-Custom-Field': 'test-value'
        })

        handler = FormHandler(custom_config)
        webhook_config = custom_config['conversational_forms']['volunteer_signup']['notifications']['webhook']

        mock_response = Mock()
        mock_response.status_code = 202
        mock_requests_post = Mock(return_value=mock_response)

        with patch('requests.post', mock_requests_post):
            result = handler._send_webhook_notifications(webhook_config, self.form_data)

            # Verify custom headers were included
            call_args = mock_requests_post.call_args
            headers = call_args[1]['headers']

            self.assertEqual(headers['X-API-Version'], 'v2')
            self.assertEqual(headers['X-Custom-Field'], 'test-value')
            self.assertEqual(headers['Authorization'], 'Bearer secret123')
            self.assertEqual(headers['Content-Type'], 'application/json')

    @mock_ses
    @mock_sns
    @mock_dynamodb
    @patch('requests.post')
    def test_send_notifications_integration_all_channels(self, mock_requests):
        """Test integrated notification sending across all channels"""
        # Set up DynamoDB for SMS usage
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb.create_table(
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

        # Mock webhook response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests.return_value = mock_response

        handler = FormHandler(self.tenant_config)
        form_config = self.tenant_config['conversational_forms']['volunteer_signup']

        result = handler._send_notifications(form_config, self.form_data, 'normal')

        # Should have notifications from all channels
        email_notifications = [n for n in result if n.startswith('email:')]
        sms_notifications = [n for n in result if n.startswith('sms:')]
        webhook_notifications = [n for n in result if n.startswith('webhook:')]

        self.assertEqual(len(email_notifications), 2)  # 2 email recipients
        self.assertEqual(len(sms_notifications), 2)     # 2 SMS recipients
        self.assertEqual(len(webhook_notifications), 1) # 1 webhook

    @mock_ses
    @mock_sns
    @mock_dynamodb
    @patch('requests.post')
    def test_send_notifications_high_priority_sms_only(self, mock_requests):
        """Test high priority notifications trigger SMS"""
        # Set up DynamoDB for SMS usage
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb.create_table(
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

        # Mock webhook response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests.return_value = mock_response

        handler = FormHandler(self.tenant_config)
        form_config = self.tenant_config['conversational_forms']['volunteer_signup']

        # Test high priority
        high_priority_result = handler._send_notifications(form_config, self.form_data, 'high')

        # Should include SMS for high priority
        sms_notifications = [n for n in high_priority_result if n.startswith('sms:')]
        self.assertEqual(len(sms_notifications), 2)

        # Test normal priority
        normal_priority_result = handler._send_notifications(form_config, self.form_data, 'normal')

        # Should not include SMS for normal priority in current implementation
        # (The code shows SMS is only sent for high priority)
        sms_notifications_normal = [n for n in normal_priority_result if n.startswith('sms:')]
        self.assertEqual(len(sms_notifications_normal), 0)

    def test_send_notifications_disabled_channels(self):
        """Test notification sending with disabled channels"""
        # Create config with disabled email and SMS
        disabled_config = self.tenant_config.copy()
        disabled_config['conversational_forms']['volunteer_signup']['notifications']['email']['enabled'] = False
        disabled_config['conversational_forms']['volunteer_signup']['notifications']['sms']['enabled'] = False

        handler = FormHandler(disabled_config)
        form_config = disabled_config['conversational_forms']['volunteer_signup']

        with patch('requests.post') as mock_requests:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_requests.return_value = mock_response

            result = handler._send_notifications(form_config, self.form_data, 'high')

            # Should only have webhook notifications
            email_notifications = [n for n in result if n.startswith('email:')]
            sms_notifications = [n for n in result if n.startswith('sms:')]
            webhook_notifications = [n for n in result if n.startswith('webhook:')]

            self.assertEqual(len(email_notifications), 0)
            self.assertEqual(len(sms_notifications), 0)
            self.assertEqual(len(webhook_notifications), 1)

    @mock_ses
    def test_build_email_body_html_formatting(self):
        """Test HTML email body building with proper formatting"""
        handler = FormHandler(self.tenant_config)

        html_body = handler._build_email_body(self.form_data, 'volunteer_notification')

        # Verify HTML structure
        self.assertIn('<html>', html_body)
        self.assertIn('<head>', html_body)
        self.assertIn('<style>', html_body)
        self.assertIn('<table>', html_body)
        self.assertIn('<tr>', html_body)
        self.assertIn('<td>', html_body)

        # Verify content
        self.assertIn('John', html_body)
        self.assertIn('Doe', html_body)
        self.assertIn('john.doe@example.com', html_body)
        self.assertIn('Weekends', html_body)
        self.assertIn('sub_12345', html_body)
        self.assertIn('test_tenant_123', html_body)

        # Verify field labels are formatted properly
        self.assertIn('First Name', html_body)  # first_name -> First Name
        self.assertIn('Last Name', html_body)   # last_name -> Last Name

    @mock_ses
    def test_send_fulfillment_email(self):
        """Test sending fulfillment email to form submitter"""
        handler = FormHandler(self.tenant_config)

        with patch('boto3.client') as mock_boto:
            mock_ses = Mock()
            mock_ses.send_email.return_value = {'MessageId': 'fulfillment-message-id'}
            mock_boto.return_value = mock_ses

            handler._send_fulfillment_email(
                recipient='john.doe@example.com',
                template='volunteer_welcome',
                responses=self.form_data['responses']
            )

            # Verify fulfillment email was sent
            mock_ses.send_email.assert_called_once()
            call_args = mock_ses.send_email.call_args

            # Check recipient
            self.assertEqual(call_args[1]['Destination']['ToAddresses'], ['john.doe@example.com'])

            # Check sender
            self.assertEqual(call_args[1]['Source'], 'noreply@testcenter.org')

    @mock_ses
    def test_send_fulfillment_email_template_substitution(self):
        """Test fulfillment email template variable substitution"""
        # Add email templates to config
        template_config = self.tenant_config.copy()
        template_config['email_templates'] = {
            'volunteer_welcome': {
                'subject': 'Welcome {first_name}!',
                'body': 'Thank you {first_name} {last_name} for volunteering!'
            }
        }

        handler = FormHandler(template_config)

        with patch('boto3.client') as mock_boto:
            mock_ses = Mock()
            mock_ses.send_email.return_value = {'MessageId': 'fulfillment-message-id'}
            mock_boto.return_value = mock_ses

            handler._send_fulfillment_email(
                recipient='john.doe@example.com',
                template='volunteer_welcome',
                responses=self.form_data['responses']
            )

            # Verify template variables were substituted
            call_args = mock_ses.send_email.call_args
            subject = call_args[1]['Message']['Subject']['Data']
            body = call_args[1]['Message']['Body']['Html']['Data']

            self.assertEqual(subject, 'Welcome John!')
            self.assertEqual(body, 'Thank you John Doe for volunteering!')

    @mock_ses
    def test_send_fulfillment_email_ses_error(self):
        """Test fulfillment email handling when SES returns error"""
        handler = FormHandler(self.tenant_config)

        with patch('boto3.client') as mock_boto:
            mock_ses = Mock()
            mock_ses.send_email.side_effect = ClientError(
                {'Error': {'Code': 'MessageRejected', 'Message': 'Invalid recipient'}},
                'SendEmail'
            )
            mock_boto.return_value = mock_ses

            # Should not raise exception
            handler._send_fulfillment_email(
                recipient='invalid@example.com',
                template='volunteer_welcome',
                responses=self.form_data['responses']
            )

    def test_format_template_with_form_data(self):
        """Test template formatting with complex form data"""
        handler = FormHandler(self.tenant_config)

        template = 'New {form_type} from {responses_first_name} {responses_last_name} at {responses_email}'
        flattened_data = handler._flatten_dict(self.form_data)

        result = handler._format_template(template, flattened_data)

        expected = 'New volunteer_signup from John Doe at john.doe@example.com'
        self.assertEqual(result, expected)


if __name__ == '__main__':
    unittest.main()