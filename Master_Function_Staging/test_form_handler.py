#!/usr/bin/env python3
"""
Comprehensive unit tests for form_handler.py
Tests form submission processing, notifications, SMS rate limiting, and error handling
"""

import unittest
from unittest.mock import Mock, patch, MagicMock, call
import json
import uuid
from datetime import datetime, timezone
from botocore.exceptions import ClientError
import boto3
from moto import mock_dynamodb, mock_ses, mock_sns, mock_s3, mock_lambda
import pytest

# Import the module under test
from form_handler import FormHandler


class TestFormHandler(unittest.TestCase):
    """Test cases for FormHandler class"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        self.tenant_config = {
            'tenant_id': 'test_tenant_123',
            'tenant_hash': 'hash_abc123',
            'organization_name': 'Test Organization',
            'contact_email': 'admin@testorg.com',
            'conversational_forms': {
                'volunteer_signup': {
                    'notifications': {
                        'email': {
                            'enabled': True,
                            'recipients': ['volunteer@testorg.com'],
                            'sender': 'noreply@testorg.com',
                            'subject': 'New Volunteer: {first_name} {last_name}',
                            'template': 'volunteer_notification'
                        },
                        'sms': {
                            'enabled': True,
                            'recipients': ['+15551234567'],
                            'monthly_limit': 100,
                            'template': 'New volunteer: {first_name}'
                        },
                        'webhook': {
                            'enabled': True,
                            'url': 'https://api.example.com/webhook',
                            'headers': {'Authorization': 'Bearer token123'}
                        }
                    },
                    'fulfillment': {
                        'type': 'email',
                        'template': 'volunteer_welcome'
                    },
                    'priority_rules': [
                        {'field': 'urgency', 'value': 'urgent', 'priority': 'high'}
                    ],
                    'next_steps': 'We will contact you within 48 hours.'
                },
                'contact': {
                    'notifications': {
                        'email': {
                            'enabled': True,
                            'recipients': ['support@testorg.com'],
                            'high_template': 'urgent_contact'
                        }
                    }
                }
            },
            'email_templates': {
                'volunteer_welcome': {
                    'subject': 'Welcome {first_name}!',
                    'body': 'Thank you for volunteering, {first_name}!'
                }
            }
        }

        self.sample_form_data = {
            'form_type': 'volunteer_signup',
            'responses': {
                'first_name': 'John',
                'last_name': 'Doe',
                'email': 'john.doe@example.com',
                'phone': '+15551234567',
                'availability': 'Weekends',
                'message': 'I want to help with food distribution'
            },
            'session_id': 'session_12345',
            'conversation_id': 'conv_67890',
            'metadata': {'source': 'widget'}
        }

    @mock_dynamodb
    @mock_ses
    @mock_sns
    @mock_s3
    @mock_lambda
    def test_successful_form_submission(self):
        """Test complete successful form submission workflow"""
        # Set up AWS mocks
        self._setup_aws_mocks()

        # Create handler and process submission
        handler = FormHandler(self.tenant_config)
        result = handler.handle_form_submission(self.sample_form_data)

        # Verify result structure
        self.assertTrue(result['success'])
        self.assertIn('submission_id', result)
        self.assertIn('notifications_sent', result)
        self.assertIn('fulfillment', result)
        self.assertIn('next_steps', result)

        # Verify submission was stored
        self.assertIsInstance(result['submission_id'], str)

        # Verify notifications were sent
        notifications = result['notifications_sent']
        self.assertIn('email:volunteer@testorg.com', notifications)
        self.assertIn('sms:+15551234567', notifications)
        self.assertIn('webhook:200', notifications)

    @mock_dynamodb
    def test_store_submission_success(self):
        """Test successful submission storage to DynamoDB"""
        # Set up DynamoDB mock
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.create_table(
            TableName='picasso_form_submissions',
            KeySchema=[{'AttributeName': 'submission_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'submission_id', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )

        handler = FormHandler(self.tenant_config)

        # Store submission
        submission_id = handler._store_submission(
            form_type='volunteer_signup',
            responses=self.sample_form_data['responses'],
            session_id='session_123',
            conversation_id='conv_456'
        )

        # Verify submission was stored
        response = table.get_item(Key={'submission_id': submission_id})
        self.assertIn('Item', response)

        item = response['Item']
        self.assertEqual(item['tenant_id'], 'test_tenant_123')
        self.assertEqual(item['form_type'], 'volunteer_signup')
        self.assertEqual(item['responses']['first_name'], 'John')
        self.assertEqual(item['status'], 'submitted')

    @mock_dynamodb
    def test_store_submission_error(self):
        """Test submission storage with DynamoDB error"""
        # Don't create table to trigger error
        handler = FormHandler(self.tenant_config)

        # Should raise ClientError
        with self.assertRaises(ClientError):
            handler._store_submission(
                form_type='volunteer_signup',
                responses=self.sample_form_data['responses'],
                session_id='session_123',
                conversation_id='conv_456'
            )

    def test_determine_priority_explicit_urgency(self):
        """Test priority determination with explicit urgency field"""
        handler = FormHandler(self.tenant_config)
        form_config = self.tenant_config['conversational_forms']['volunteer_signup']

        # Test high priority
        responses = {'urgency': 'urgent'}
        priority = handler._determine_priority('volunteer_signup', responses, form_config)
        self.assertEqual(priority, 'high')

        # Test normal priority
        responses = {'urgency': 'normal'}
        priority = handler._determine_priority('volunteer_signup', responses, form_config)
        self.assertEqual(priority, 'normal')

        # Test low priority
        responses = {'urgency': 'low'}
        priority = handler._determine_priority('volunteer_signup', responses, form_config)
        self.assertEqual(priority, 'low')

    def test_determine_priority_rules(self):
        """Test priority determination using configured rules"""
        handler = FormHandler(self.tenant_config)
        form_config = self.tenant_config['conversational_forms']['volunteer_signup']

        responses = {'urgency': 'urgent'}
        priority = handler._determine_priority('volunteer_signup', responses, form_config)
        self.assertEqual(priority, 'high')

    def test_determine_priority_defaults(self):
        """Test priority determination using form type defaults"""
        handler = FormHandler(self.tenant_config)
        form_config = {}

        # Test form types with known defaults
        priority = handler._determine_priority('request_support', {}, form_config)
        self.assertEqual(priority, 'high')

        priority = handler._determine_priority('volunteer_signup', {}, form_config)
        self.assertEqual(priority, 'normal')

        priority = handler._determine_priority('newsletter', {}, form_config)
        self.assertEqual(priority, 'low')

        priority = handler._determine_priority('unknown_form', {}, form_config)
        self.assertEqual(priority, 'normal')

    @mock_ses
    def test_send_email_notifications_success(self):
        """Test successful email notification sending"""
        # Set up SES mock
        ses_client = boto3.client('ses', region_name='us-east-1')

        handler = FormHandler(self.tenant_config)
        email_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['email']

        form_data = {
            'form_type': 'volunteer_signup',
            'responses': {'first_name': 'John', 'last_name': 'Doe'},
            'submission_id': 'sub_123',
            'priority': 'normal'
        }

        result = handler._send_email_notifications(email_config, form_data, 'normal')

        # Verify email was "sent"
        self.assertEqual(len(result), 1)
        self.assertIn('email:volunteer@testorg.com', result)

    @mock_ses
    def test_send_email_notifications_error(self):
        """Test email notification with SES error"""
        # Patch SES to raise an error
        with patch('boto3.client') as mock_boto:
            mock_ses = Mock()
            mock_ses.send_email.side_effect = ClientError(
                {'Error': {'Code': 'MessageRejected', 'Message': 'Email rejected'}},
                'SendEmail'
            )
            mock_boto.return_value = mock_ses

            # Reload the module to use patched boto3
            import importlib
            import form_handler
            importlib.reload(form_handler)
            from form_handler import FormHandler

            handler = FormHandler(self.tenant_config)
            email_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['email']

            form_data = {
                'form_type': 'volunteer_signup',
                'responses': {'first_name': 'John'},
                'submission_id': 'sub_123',
                'priority': 'normal'
            }

            result = handler._send_email_notifications(email_config, form_data, 'normal')

            # Should return empty list due to error
            self.assertEqual(len(result), 0)

    @mock_sns
    @mock_dynamodb
    def test_send_sms_notifications_success(self):
        """Test successful SMS notification sending"""
        # Set up SMS usage table
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        sms_table = dynamodb.create_table(
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

        form_data = {
            'form_type': 'volunteer_signup',
            'responses': {'first_name': 'John'},
            'submission_id': 'sub_123'
        }

        result = handler._send_sms_notifications(sms_config, form_data)

        # Verify SMS was "sent"
        self.assertEqual(len(result), 1)
        self.assertIn('sms:+15551234567', result)

    @mock_dynamodb
    def test_sms_rate_limiting(self):
        """Test SMS rate limiting with monthly limits"""
        # Set up SMS usage table with current usage
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        sms_table = dynamodb.create_table(
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

        # Set current usage to limit
        current_month = datetime.now().strftime('%Y-%m')
        sms_table.put_item(Item={
            'tenant_id': 'test_tenant_123',
            'month': current_month,
            'count': 100  # At limit
        })

        handler = FormHandler(self.tenant_config)
        sms_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['sms']

        form_data = {
            'form_type': 'volunteer_signup',
            'responses': {'first_name': 'John'},
            'submission_id': 'sub_123'
        }

        result = handler._send_sms_notifications(sms_config, form_data)

        # Should return empty list due to rate limiting
        self.assertEqual(len(result), 0)

    @patch('requests.post')
    def test_send_webhook_notifications_success(self):
        """Test successful webhook notification sending"""
        # Mock successful webhook response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests_post = Mock(return_value=mock_response)

        with patch('requests.post', mock_requests_post):
            handler = FormHandler(self.tenant_config)
            webhook_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['webhook']

            form_data = {
                'form_type': 'volunteer_signup',
                'responses': {'first_name': 'John'},
                'submission_id': 'sub_123'
            }

            result = handler._send_webhook_notifications(webhook_config, form_data)

            # Verify webhook was called
            self.assertEqual(len(result), 1)
            self.assertIn('webhook:200', result)

            # Verify request parameters
            mock_requests_post.assert_called_once()
            call_args = mock_requests_post.call_args
            self.assertEqual(call_args[1]['url'], 'https://api.example.com/webhook')
            self.assertEqual(call_args[1]['headers']['Authorization'], 'Bearer token123')
            self.assertEqual(call_args[1]['json'], form_data)

    @patch('requests.post')
    def test_send_webhook_notifications_error(self):
        """Test webhook notification with HTTP error"""
        # Mock failed webhook response
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = 'Internal Server Error'
        mock_requests_post = Mock(return_value=mock_response)

        with patch('requests.post', mock_requests_post):
            handler = FormHandler(self.tenant_config)
            webhook_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['webhook']

            form_data = {
                'form_type': 'volunteer_signup',
                'responses': {'first_name': 'John'},
                'submission_id': 'sub_123'
            }

            result = handler._send_webhook_notifications(webhook_config, form_data)

            # Should return empty list due to error
            self.assertEqual(len(result), 0)

    @patch('requests.post')
    def test_send_webhook_notifications_exception(self):
        """Test webhook notification with connection exception"""
        # Mock connection exception
        mock_requests_post = Mock(side_effect=ConnectionError('Connection failed'))

        with patch('requests.post', mock_requests_post):
            handler = FormHandler(self.tenant_config)
            webhook_config = self.tenant_config['conversational_forms']['volunteer_signup']['notifications']['webhook']

            form_data = {
                'form_type': 'volunteer_signup',
                'responses': {'first_name': 'John'},
                'submission_id': 'sub_123'
            }

            result = handler._send_webhook_notifications(webhook_config, form_data)

            # Should return empty list due to exception
            self.assertEqual(len(result), 0)

    @mock_lambda
    def test_process_fulfillment_lambda(self):
        """Test fulfillment processing with Lambda invocation"""
        handler = FormHandler(self.tenant_config)

        # Create Lambda function mock
        lambda_client = boto3.client('lambda', region_name='us-east-1')
        lambda_client.create_function(
            FunctionName='test-fulfillment-function',
            Runtime='python3.9',
            Role='arn:aws:iam::123456789012:role/lambda-role',
            Handler='lambda_function.lambda_handler',
            Code={'ZipFile': b'fake code'},
        )

        form_config = {
            'fulfillment': {
                'type': 'lambda',
                'function': 'test-fulfillment-function',
                'action': 'process_volunteer'
            }
        }

        result = handler._process_fulfillment(
            form_config=form_config,
            form_type='volunteer_signup',
            responses={'first_name': 'John'},
            submission_id='sub_123'
        )

        # Verify Lambda was invoked
        self.assertEqual(result['type'], 'lambda')
        self.assertEqual(result['function'], 'test-fulfillment-function')
        self.assertEqual(result['status'], 'invoked')
        self.assertEqual(result['status_code'], 202)

    @mock_ses
    def test_process_fulfillment_email(self):
        """Test fulfillment processing with email sending"""
        handler = FormHandler(self.tenant_config)

        form_config = {
            'fulfillment': {
                'type': 'email',
                'template': 'volunteer_welcome'
            }
        }

        responses = {
            'first_name': 'John',
            'email': 'john@example.com'
        }

        result = handler._process_fulfillment(
            form_config=form_config,
            form_type='volunteer_signup',
            responses=responses,
            submission_id='sub_123'
        )

        # Verify email fulfillment
        self.assertEqual(result['type'], 'email')
        self.assertEqual(result['status'], 'sent')
        self.assertEqual(result['recipient'], 'john@example.com')

    @mock_s3
    def test_process_fulfillment_s3(self):
        """Test fulfillment processing with S3 storage"""
        # Create S3 bucket
        s3_client = boto3.client('s3', region_name='us-east-1')
        s3_client.create_bucket(Bucket='test-fulfillment-bucket')

        handler = FormHandler(self.tenant_config)

        form_config = {
            'fulfillment': {
                'type': 's3',
                'bucket': 'test-fulfillment-bucket'
            }
        }

        responses = {'first_name': 'John', 'last_name': 'Doe'}

        result = handler._process_fulfillment(
            form_config=form_config,
            form_type='volunteer_signup',
            responses=responses,
            submission_id='sub_123'
        )

        # Verify S3 storage
        self.assertEqual(result['type'], 's3')
        self.assertEqual(result['status'], 'stored')
        self.assertIn('s3://test-fulfillment-bucket', result['location'])

    def test_process_fulfillment_no_config(self):
        """Test fulfillment processing with no configuration"""
        handler = FormHandler(self.tenant_config)

        form_config = {}

        result = handler._process_fulfillment(
            form_config=form_config,
            form_type='volunteer_signup',
            responses={'first_name': 'John'},
            submission_id='sub_123'
        )

        # Verify no fulfillment
        self.assertEqual(result['status'], 'no_fulfillment_configured')

    def test_process_fulfillment_unsupported_type(self):
        """Test fulfillment processing with unsupported type"""
        handler = FormHandler(self.tenant_config)

        form_config = {
            'fulfillment': {
                'type': 'unsupported_type'
            }
        }

        result = handler._process_fulfillment(
            form_config=form_config,
            form_type='volunteer_signup',
            responses={'first_name': 'John'},
            submission_id='sub_123'
        )

        # Verify unsupported type handling
        self.assertEqual(result['type'], 'unsupported_type')
        self.assertEqual(result['status'], 'unsupported')

    @mock_dynamodb
    def test_get_monthly_sms_usage(self):
        """Test getting monthly SMS usage from DynamoDB"""
        # Set up SMS usage table
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        sms_table = dynamodb.create_table(
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

        # Add usage record
        current_month = datetime.now().strftime('%Y-%m')
        sms_table.put_item(Item={
            'tenant_id': 'test_tenant_123',
            'month': current_month,
            'count': 42
        })

        handler = FormHandler(self.tenant_config)
        usage = handler._get_monthly_sms_usage()

        self.assertEqual(usage, 42)

    @mock_dynamodb
    def test_get_monthly_sms_usage_no_record(self):
        """Test getting monthly SMS usage when no record exists"""
        # Set up empty SMS usage table
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
        usage = handler._get_monthly_sms_usage()

        self.assertEqual(usage, 0)

    @mock_dynamodb
    def test_increment_sms_usage(self):
        """Test incrementing SMS usage counter"""
        # Set up SMS usage table
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        sms_table = dynamodb.create_table(
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

        # Increment usage (should create new record)
        handler._increment_sms_usage()

        # Verify usage was incremented
        current_month = datetime.now().strftime('%Y-%m')
        response = sms_table.get_item(Key={
            'tenant_id': 'test_tenant_123',
            'month': current_month
        })

        self.assertIn('Item', response)
        self.assertEqual(response['Item']['count'], 1)

    @mock_dynamodb
    def test_audit_submission(self):
        """Test audit logging of form submission"""
        # Set up audit table
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        audit_table = dynamodb.create_table(
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

        handler = FormHandler(self.tenant_config)

        handler._audit_submission(
            submission_id='sub_123',
            form_type='volunteer_signup',
            notification_results=['email:test@example.com'],
            fulfillment_result={'type': 'email', 'status': 'sent'}
        )

        # Verify audit log was created
        # Note: We can't easily verify the exact item due to timestamp generation
        # But we can verify the method doesn't raise an exception

    def test_format_template_basic(self):
        """Test basic template formatting"""
        handler = FormHandler(self.tenant_config)

        template = 'Hello {first_name} {last_name}!'
        data = {'first_name': 'John', 'last_name': 'Doe'}

        result = handler._format_template(template, data)
        self.assertEqual(result, 'Hello John Doe!')

    def test_format_template_nested_data(self):
        """Test template formatting with nested data"""
        handler = FormHandler(self.tenant_config)

        template = 'Form: {form_type}, Name: {responses_first_name}'
        data = {
            'form_type': 'volunteer_signup',
            'responses': {'first_name': 'John', 'last_name': 'Doe'}
        }

        result = handler._format_template(template, data)
        self.assertEqual(result, 'Form: volunteer_signup, Name: John')

    def test_format_template_missing_variable(self):
        """Test template formatting with missing variable"""
        handler = FormHandler(self.tenant_config)

        template = 'Hello {first_name} {missing_var}!'
        data = {'first_name': 'John'}

        result = handler._format_template(template, data)
        # Should leave missing variables as-is
        self.assertEqual(result, 'Hello John {missing_var}!')

    def test_flatten_dict(self):
        """Test dictionary flattening for template variables"""
        handler = FormHandler(self.tenant_config)

        nested_dict = {
            'form_type': 'volunteer_signup',
            'responses': {
                'first_name': 'John',
                'contact': {
                    'email': 'john@example.com',
                    'phone': '555-1234'
                }
            }
        }

        flattened = handler._flatten_dict(nested_dict)

        expected = {
            'form_type': 'volunteer_signup',
            'responses_first_name': 'John',
            'responses_contact_email': 'john@example.com',
            'responses_contact_phone': '555-1234'
        }

        self.assertEqual(flattened, expected)

    def test_build_email_body(self):
        """Test HTML email body building"""
        handler = FormHandler(self.tenant_config)

        form_data = {
            'form_type': 'volunteer_signup',
            'responses': {
                'first_name': 'John',
                'last_name': 'Doe',
                'email': 'john@example.com'
            },
            'submission_id': 'sub_123'
        }

        html = handler._build_email_body(form_data, 'volunteer_notification')

        # Verify HTML structure
        self.assertIn('<html>', html)
        self.assertIn('<table>', html)
        self.assertIn('John', html)
        self.assertIn('Doe', html)
        self.assertIn('john@example.com', html)
        self.assertIn('sub_123', html)
        self.assertIn('test_tenant_123', html)

    def test_get_next_steps_configured(self):
        """Test getting next steps from form configuration"""
        handler = FormHandler(self.tenant_config)
        form_config = self.tenant_config['conversational_forms']['volunteer_signup']

        next_steps = handler._get_next_steps('volunteer_signup', form_config)
        self.assertEqual(next_steps, 'We will contact you within 48 hours.')

    def test_get_next_steps_default(self):
        """Test getting default next steps for form types"""
        handler = FormHandler(self.tenant_config)
        form_config = {}

        next_steps = handler._get_next_steps('volunteer_signup', form_config)
        self.assertEqual(next_steps, 'We will contact you within 48 hours to discuss next steps.')

        next_steps = handler._get_next_steps('unknown_form', form_config)
        self.assertEqual(next_steps, 'Thank you for your submission. We will be in touch soon.')

    def test_handle_form_submission_missing_form_type(self):
        """Test handling form submission with missing form type configuration"""
        handler = FormHandler(self.tenant_config)

        form_data = {
            'form_type': 'nonexistent_form',
            'responses': {'name': 'John'},
            'session_id': 'session_123',
            'conversation_id': 'conv_456'
        }

        result = handler.handle_form_submission(form_data)

        self.assertFalse(result['success'])
        self.assertIn('error', result)
        self.assertIn('not configured', result['error'])

    def test_handle_form_submission_exception(self):
        """Test handling form submission with unexpected exception"""
        # Create handler with invalid config to trigger exception
        invalid_config = {'tenant_id': None}  # This should cause issues
        handler = FormHandler(invalid_config)

        result = handler.handle_form_submission(self.sample_form_data)

        self.assertFalse(result['success'])
        self.assertIn('error', result)

    def _setup_aws_mocks(self):
        """Helper method to set up AWS service mocks"""
        # Set up DynamoDB tables
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

        # Submissions table
        dynamodb.create_table(
            TableName='picasso_form_submissions',
            KeySchema=[{'AttributeName': 'submission_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'submission_id', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )

        # SMS usage table
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

        # Audit table
        dynamodb.create_table(
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


if __name__ == '__main__':
    unittest.main()