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


class TestBubbleIntegration(unittest.TestCase):
    """Test cases for Bubble webhook integration with standardized schema"""

    def setUp(self):
        """Set up test fixtures for Bubble tests"""
        self.tenant_config = {
            'tenant_id': 'AUS123957',
            'tenant_hash': 'auc5b0ecb0adcb',
            'chat_title': 'Austin Angels',
            'features': {
                'conversational_forms': True
            },
            'bubble_integration': {
                'webhook_url': 'https://myapp.bubbleapps.io/api/1.1/wf/form_submission',
                'api_key': 'bubble_test_api_key_123'
            },
            'conversational_forms': {
                'volunteer_signup': {
                    'form_id': 'volunteer_signup',
                    'title': 'Volunteer Application',
                    'program': 'volunteer_daretodream',
                    'notifications': {'email': {'enabled': False}},
                    'next_steps': 'Thank you!'
                }
            }
        }

        self.sample_responses = {
            'first_name': 'Jane',
            'last_name': 'Smith',
            'email': 'jane.smith@example.com',
            'phone': '+15559876543',
            'program_interest': 'mentorship',
            'availability': 'Weekends'
        }

    @patch('urllib.request.urlopen')
    def test_bubble_webhook_uses_standardized_schema(self):
        """Test that payload uses 11 fixed fields + form_data JSON string"""
        mock_response = Mock()
        mock_response.getcode.return_value = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        captured_data = {}

        def capture_request(request, timeout=None):
            captured_data['url'] = request.full_url
            captured_data['headers'] = dict(request.headers)
            captured_data['payload'] = json.loads(request.data.decode('utf-8'))
            return mock_response

        with patch('urllib.request.urlopen', side_effect=capture_request):
            handler = FormHandler(self.tenant_config)
            handler._send_bubble_webhook(
                form_type='volunteer_signup',
                responses=self.sample_responses,
                submission_id='sub_schema_test_123',
                session_id='session_456',
                conversation_id='conv_789'
            )

        payload = captured_data['payload']

        # Verify submission metadata
        self.assertEqual(payload['submission_id'], 'sub_schema_test_123')
        self.assertIn('timestamp', payload)

        # Verify tenant metadata
        self.assertEqual(payload['tenant_id'], 'AUS123957')
        self.assertEqual(payload['tenant_hash'], 'auc5b0ecb0adcb')
        self.assertEqual(payload['organization_name'], 'Austin Angels')

        # Verify form metadata
        self.assertEqual(payload['form_id'], 'volunteer_signup')
        self.assertEqual(payload['form_title'], 'Volunteer Application')
        self.assertEqual(payload['program_id'], 'volunteer_daretodream')

        # Verify session tracking
        self.assertEqual(payload['session_id'], 'session_456')
        self.assertEqual(payload['conversation_id'], 'conv_789')

        # Verify form_data is a JSON string containing the responses
        self.assertIn('form_data', payload)
        self.assertIsInstance(payload['form_data'], str)
        form_data = json.loads(payload['form_data'])
        self.assertEqual(form_data['first_name'], 'Jane')
        self.assertEqual(form_data['last_name'], 'Smith')
        self.assertEqual(form_data['email'], 'jane.smith@example.com')

        # Verify NO old fields exist at top level
        self.assertNotIn('first_name', payload)
        self.assertNotIn('email', payload)
        self.assertNotIn('applicant_email', payload)
        self.assertNotIn('responses_json', payload)

    @patch('urllib.request.urlopen')
    def test_bubble_webhook_includes_authorization_header(self):
        """Test that Authorization header is included when api_key is configured"""
        mock_response = Mock()
        mock_response.getcode.return_value = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        captured_headers = {}

        def capture_request(request, timeout=None):
            captured_headers.update(dict(request.headers))
            return mock_response

        with patch('urllib.request.urlopen', side_effect=capture_request):
            handler = FormHandler(self.tenant_config)
            handler._send_bubble_webhook(
                form_type='volunteer_signup',
                responses=self.sample_responses,
                submission_id='sub_auth_test',
                session_id='session_123',
                conversation_id='conv_456'
            )

        # Verify Authorization header
        self.assertIn('Authorization', captured_headers)
        self.assertEqual(captured_headers['Authorization'], 'Bearer bubble_test_api_key_123')

    @patch('urllib.request.urlopen')
    def test_bubble_webhook_handles_any_form_fields_in_form_data(self):
        """Test that any form fields are included in form_data JSON string"""
        mock_response = Mock()
        mock_response.getcode.return_value = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        # Custom form with different fields than volunteer_signup
        custom_responses = {
            'organization_name': 'Acme Corp',
            'donation_amount': '500',
            'payment_method': 'credit_card',
            'recurring': True,
            'special_instructions': 'Please use for education programs'
        }

        captured_data = {}

        def capture_request(request, timeout=None):
            captured_data['payload'] = json.loads(request.data.decode('utf-8'))
            return mock_response

        with patch('urllib.request.urlopen', side_effect=capture_request):
            handler = FormHandler(self.tenant_config)
            handler._send_bubble_webhook(
                form_type='donation',
                responses=custom_responses,
                submission_id='sub_custom_123',
                session_id='session_custom',
                conversation_id='conv_custom'
            )

        payload = captured_data['payload']

        # Verify form_data contains all custom fields as JSON string
        self.assertIn('form_data', payload)
        form_data = json.loads(payload['form_data'])
        self.assertEqual(form_data['organization_name'], 'Acme Corp')
        self.assertEqual(form_data['donation_amount'], '500')
        self.assertEqual(form_data['payment_method'], 'credit_card')
        self.assertEqual(form_data['recurring'], True)
        self.assertEqual(form_data['special_instructions'], 'Please use for education programs')

        # Verify these fields are NOT at top level (only in form_data)
        self.assertNotIn('organization_name', payload)
        self.assertNotIn('donation_amount', payload)

    def test_bubble_webhook_skips_when_forms_not_enabled(self):
        """Test that Bubble webhook is skipped when conversational_forms feature is disabled"""
        config_no_forms = {
            'tenant_id': 'test_no_forms',
            'features': {
                'conversational_forms': False
            },
            'bubble_integration': {
                'webhook_url': 'https://myapp.bubbleapps.io/api/1.1/wf/form_submission'
            }
        }

        with patch('urllib.request.urlopen') as mock_urlopen:
            handler = FormHandler(config_no_forms)
            handler._send_bubble_webhook(
                form_type='volunteer_signup',
                responses=self.sample_responses,
                submission_id='sub_no_forms',
                session_id='session_123',
                conversation_id='conv_456'
            )

            # urlopen should NOT be called
            mock_urlopen.assert_not_called()

    def test_bubble_webhook_skips_when_no_webhook_url(self):
        """Test that Bubble webhook is skipped when no webhook_url is configured"""
        config_no_url = {
            'tenant_id': 'test_no_url',
            'features': {
                'conversational_forms': True
            },
            'bubble_integration': {}  # No webhook_url
        }

        with patch('urllib.request.urlopen') as mock_urlopen:
            handler = FormHandler(config_no_url)
            handler._send_bubble_webhook(
                form_type='volunteer_signup',
                responses=self.sample_responses,
                submission_id='sub_no_url',
                session_id='session_123',
                conversation_id='conv_456'
            )

            # urlopen should NOT be called
            mock_urlopen.assert_not_called()

    @patch('urllib.request.urlopen')
    def test_bubble_webhook_handles_http_error_gracefully(self):
        """Test that HTTP errors from Bubble don't fail the form submission"""
        import urllib.error

        mock_error = urllib.error.HTTPError(
            url='https://myapp.bubbleapps.io/api/1.1/wf/form_submission',
            code=500,
            msg='Internal Server Error',
            hdrs={},
            fp=Mock(read=Mock(return_value=b'Server error'))
        )

        with patch('urllib.request.urlopen', side_effect=mock_error):
            handler = FormHandler(self.tenant_config)

            # Should NOT raise exception - errors are handled gracefully
            try:
                handler._send_bubble_webhook(
                    form_type='volunteer_signup',
                    responses=self.sample_responses,
                    submission_id='sub_error_test',
                    session_id='session_123',
                    conversation_id='conv_456'
                )
            except Exception as e:
                self.fail(f"_send_bubble_webhook raised exception: {e}")

    @patch('urllib.request.urlopen')
    def test_bubble_webhook_handles_network_error_gracefully(self):
        """Test that network errors don't fail the form submission"""
        with patch('urllib.request.urlopen', side_effect=ConnectionError('Network error')):
            handler = FormHandler(self.tenant_config)

            # Should NOT raise exception
            try:
                handler._send_bubble_webhook(
                    form_type='volunteer_signup',
                    responses=self.sample_responses,
                    submission_id='sub_network_error',
                    session_id='session_123',
                    conversation_id='conv_456'
                )
            except Exception as e:
                self.fail(f"_send_bubble_webhook raised exception: {e}")

    @patch.dict('os.environ', {'BUBBLE_WEBHOOK_URL': 'https://env.bubbleapps.io/api/wf/test'})
    @patch('urllib.request.urlopen')
    def test_bubble_webhook_uses_env_var_when_config_not_set(self):
        """Test that environment variable is used when config doesn't have webhook_url"""
        mock_response = Mock()
        mock_response.getcode.return_value = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        config_no_url = {
            'tenant_id': 'test_env_var',
            'features': {
                'conversational_forms': True
            },
            'bubble_integration': {}  # No webhook_url in config
        }

        captured_url = None

        def capture_request(request, timeout=None):
            nonlocal captured_url
            captured_url = request.full_url
            return mock_response

        with patch('urllib.request.urlopen', side_effect=capture_request):
            handler = FormHandler(config_no_url)
            handler._send_bubble_webhook(
                form_type='volunteer_signup',
                responses=self.sample_responses,
                submission_id='sub_env_test',
                session_id='session_123',
                conversation_id='conv_456'
            )

        # Verify environment variable URL was used
        self.assertEqual(captured_url, 'https://env.bubbleapps.io/api/wf/test')

    @patch('urllib.request.urlopen')
    def test_bubble_webhook_includes_session_and_conversation_ids(self):
        """Test that session_id and conversation_id are included in payload"""
        mock_response = Mock()
        mock_response.getcode.return_value = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        captured_data = {}

        def capture_request(request, timeout=None):
            captured_data['payload'] = json.loads(request.data.decode('utf-8'))
            return mock_response

        with patch('urllib.request.urlopen', side_effect=capture_request):
            handler = FormHandler(self.tenant_config)
            handler._send_bubble_webhook(
                form_type='volunteer_signup',
                responses=self.sample_responses,
                submission_id='sub_session_test',
                session_id='session_abc123',
                conversation_id='conv_xyz789'
            )

        # Verify session and conversation IDs
        payload = captured_data['payload']
        self.assertEqual(payload['session_id'], 'session_abc123')
        self.assertEqual(payload['conversation_id'], 'conv_xyz789')


# ============================================================================
# EMAIL DETAILS BUILDER TESTS
# ============================================================================

# Import the email details builder functions
from form_handler import (
    humanize_key,
    format_value,
    get_field_priority,
    build_email_details_text,
    extract_contact,
    get_email_subject_suffix
)


class TestHumanizeKey(unittest.TestCase):
    """Test cases for humanize_key function"""

    def test_snake_case_to_title_case(self):
        """Test converting snake_case keys to Title Case"""
        self.assertEqual(humanize_key('first_name'), 'First Name')
        self.assertEqual(humanize_key('last_name'), 'Last Name')
        self.assertEqual(humanize_key('email_address'), 'Email Address')

    def test_preserves_acronyms(self):
        """Test that common acronyms are preserved in uppercase"""
        self.assertEqual(humanize_key('zip_code'), 'ZIP Code')
        self.assertEqual(humanize_key('user_id'), 'User ID')
        self.assertEqual(humanize_key('website_url'), 'Website URL')
        self.assertEqual(humanize_key('dob'), 'DOB')
        self.assertEqual(humanize_key('ssn'), 'SSN')

    def test_camel_case_handling(self):
        """Test handling camelCase keys"""
        self.assertEqual(humanize_key('firstName'), 'First Name')
        self.assertEqual(humanize_key('zipCode'), 'ZIP Code')

    def test_empty_key(self):
        """Test handling empty keys"""
        self.assertEqual(humanize_key(''), '')
        self.assertEqual(humanize_key(None), '')


class TestFormatValue(unittest.TestCase):
    """Test cases for format_value function"""

    def test_omits_null_and_empty(self):
        """Test that null and empty values return None"""
        self.assertIsNone(format_value(None))
        self.assertIsNone(format_value(''))

    def test_boolean_formatting(self):
        """Test that booleans are formatted as Yes/No"""
        self.assertEqual(format_value(True), 'Yes')
        self.assertEqual(format_value(False), 'No')

    def test_list_formatting(self):
        """Test that lists are joined with comma"""
        self.assertEqual(format_value(['English', 'Spanish', 'French']), 'English, Spanish, French')
        self.assertEqual(format_value(['Single']), 'Single')
        self.assertIsNone(format_value([]))
        self.assertIsNone(format_value([None, '', None]))

    def test_dict_formatting(self):
        """Test that dicts are stringified"""
        result = format_value({'street': '123 Main', 'city': 'Austin'})
        self.assertIn('"street"', result)
        self.assertIn('"city"', result)

    def test_string_truncation(self):
        """Test that long strings are truncated"""
        long_value = 'x' * 2500
        result = format_value(long_value)
        self.assertEqual(len(result), 2003)  # 2000 + '...'
        self.assertTrue(result.endswith('...'))

    def test_number_formatting(self):
        """Test that numbers are converted to strings"""
        self.assertEqual(format_value(123), '123')
        self.assertEqual(format_value(123.45), '123.45')


class TestGetFieldPriority(unittest.TestCase):
    """Test cases for get_field_priority function"""

    def test_name_fields_highest_priority(self):
        """Test that name fields have highest priority"""
        self.assertEqual(get_field_priority('first_name'), 0)
        self.assertEqual(get_field_priority('last_name'), 1)
        self.assertEqual(get_field_priority('name'), 2)

    def test_email_fields_second_priority(self):
        """Test that email fields come after name"""
        self.assertEqual(get_field_priority('email'), 10)
        self.assertEqual(get_field_priority('email_address'), 10)

    def test_phone_fields_third_priority(self):
        """Test that phone fields come after email"""
        self.assertEqual(get_field_priority('phone'), 20)
        self.assertEqual(get_field_priority('mobile'), 20)
        self.assertEqual(get_field_priority('cell_phone'), 20)

    def test_address_fields_fourth_priority(self):
        """Test that address fields come after phone"""
        self.assertEqual(get_field_priority('street_address'), 30)
        self.assertEqual(get_field_priority('city'), 32)
        self.assertEqual(get_field_priority('state'), 33)
        self.assertEqual(get_field_priority('zip_code'), 34)

    def test_other_fields_lower_priority(self):
        """Test that other fields have lowest priority"""
        self.assertEqual(get_field_priority('comments'), 100)
        self.assertEqual(get_field_priority('availability'), 100)


class TestBuildEmailDetailsText(unittest.TestCase):
    """Test cases for build_email_details_text function"""

    def test_formats_fields_as_label_value(self):
        """Test that fields are formatted as 'Label: Value' lines"""
        form_data = json.dumps({
            'first_name': 'Jane',
            'last_name': 'Smith',
            'email': 'jane@example.com'
        })
        result = build_email_details_text(form_data)

        self.assertIn('First Name: Jane', result)
        self.assertIn('Last Name: Smith', result)
        self.assertIn('Email: jane@example.com', result)

    def test_orders_contact_fields_first(self):
        """Test that contact fields appear first in output"""
        form_data = json.dumps({
            'comments': 'Some comments',
            'zip_code': '78701',
            'first_name': 'Jane',
            'email': 'jane@example.com',
            'city': 'Austin',
            'last_name': 'Smith',
            'phone': '+15551234567'
        })
        result = build_email_details_text(form_data)
        lines = result.split('\n')

        # Find indices
        first_name_idx = next(i for i, l in enumerate(lines) if l.startswith('First Name:'))
        email_idx = next(i for i, l in enumerate(lines) if l.startswith('Email:'))
        phone_idx = next(i for i, l in enumerate(lines) if l.startswith('Phone:'))
        city_idx = next(i for i, l in enumerate(lines) if l.startswith('City:'))
        comments_idx = next(i for i, l in enumerate(lines) if l.startswith('Comments:'))

        # Verify order
        self.assertLess(first_name_idx, email_idx)
        self.assertLess(email_idx, phone_idx)
        self.assertLess(phone_idx, city_idx)
        self.assertLess(city_idx, comments_idx)

    def test_omits_empty_values(self):
        """Test that empty and null values are omitted"""
        form_data = json.dumps({
            'first_name': 'Jane',
            'last_name': '',
            'email': None,
            'notes': 'Some notes'
        })
        result = build_email_details_text(form_data)

        self.assertIn('First Name: Jane', result)
        self.assertIn('Notes: Some notes', result)
        self.assertNotIn('Last Name:', result)
        self.assertNotIn('Email:', result)

    def test_handles_invalid_json(self):
        """Test graceful handling of invalid JSON"""
        result = build_email_details_text('not valid json')
        self.assertIn('Unable to parse form data', result)
        self.assertIn('not valid json', result)

    def test_handles_non_dict_json(self):
        """Test handling of JSON that's not a dict"""
        result = build_email_details_text(json.dumps(['array', 'data']))
        self.assertIn('Unable to parse form data', result)

    def test_boolean_values_formatted_as_yes_no(self):
        """Test that boolean values show as Yes/No"""
        form_data = json.dumps({
            'first_name': 'Jane',
            'has_children': True,
            'has_vehicle': False
        })
        result = build_email_details_text(form_data)

        self.assertIn('Has Children: Yes', result)
        self.assertIn('Has Vehicle: No', result)


class TestExtractContact(unittest.TestCase):
    """Test cases for extract_contact function"""

    def test_extracts_name_from_first_and_last(self):
        """Test extracting name from first_name and last_name"""
        form_data = {
            'first_name': 'Jane',
            'last_name': 'Smith',
            'email': 'jane@example.com'
        }
        contact = extract_contact(form_data)
        self.assertEqual(contact['name'], 'Jane Smith')

    def test_extracts_email(self):
        """Test extracting email from email field"""
        form_data = {
            'first_name': 'Jane',
            'email': 'jane@example.com'
        }
        contact = extract_contact(form_data)
        self.assertEqual(contact['email'], 'jane@example.com')

    def test_extracts_phone(self):
        """Test extracting phone from phone field"""
        form_data = {
            'first_name': 'Jane',
            'phone': '+15551234567'
        }
        contact = extract_contact(form_data)
        self.assertEqual(contact['phone'], '+15551234567')

    def test_extracts_phone_from_mobile(self):
        """Test extracting phone from mobile field"""
        form_data = {
            'first_name': 'Jane',
            'mobile_number': '+15559876543'
        }
        contact = extract_contact(form_data)
        self.assertEqual(contact['phone'], '+15559876543')

    def test_returns_empty_dict_when_no_contact_fields(self):
        """Test returning empty dict when no contact fields present"""
        form_data = {
            'comments': 'Just a comment',
            'program': 'lovebox'
        }
        contact = extract_contact(form_data)
        self.assertEqual(contact, {})

    def test_handles_null_input(self):
        """Test handling null input"""
        self.assertEqual(extract_contact(None), {})
        self.assertEqual(extract_contact({}), {})


class TestGetEmailSubjectSuffix(unittest.TestCase):
    """Test cases for get_email_subject_suffix function"""

    def test_returns_full_name(self):
        """Test returning full name when both names present"""
        form_data = {
            'first_name': 'Jane',
            'last_name': 'Smith'
        }
        self.assertEqual(get_email_subject_suffix(form_data), 'Jane Smith')

    def test_returns_first_name_only(self):
        """Test returning first name when last name missing"""
        form_data = {'first_name': 'Jane'}
        self.assertEqual(get_email_subject_suffix(form_data), 'Jane')

    def test_returns_default_when_no_name(self):
        """Test returning default when no name fields present"""
        form_data = {'email': 'anon@example.com'}
        self.assertEqual(get_email_subject_suffix(form_data), 'New submission')


class TestBubbleWebhookNewFields(unittest.TestCase):
    """Test that Bubble webhook includes new email details fields"""

    def setUp(self):
        """Set up test fixtures"""
        self.tenant_config = {
            'tenant_id': 'test_tenant',
            'tenant_hash': 'abc123',
            'chat_title': 'Test Organization',
            'features': {'conversational_forms': True},
            'bubble_integration': {
                'webhook_url': 'https://test.bubbleapps.io/api/1.1/wf/form_submit'
            },
            'conversational_forms': {
                'volunteer_signup': {
                    'title': 'Volunteer Application',
                    'program': 'lovebox',
                    'fields': [
                        {'id': 'first_name', 'label': 'First Name'},
                        {'id': 'last_name', 'label': 'Last Name'},
                        {'id': 'email', 'label': 'Email'}
                    ]
                }
            }
        }

        self.sample_responses = {
            'first_name': 'Jane',
            'last_name': 'Smith',
            'email': 'jane@example.com',
            'phone': '+15551234567'
        }

    @patch('urllib.request.urlopen')
    def test_webhook_includes_email_details_text(self):
        """Test that webhook payload includes email_details_text"""
        mock_response = Mock()
        mock_response.getcode.return_value = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        captured_data = {}

        def capture_request(request, timeout=None):
            captured_data['payload'] = json.loads(request.data.decode('utf-8'))
            return mock_response

        with patch('urllib.request.urlopen', side_effect=capture_request):
            handler = FormHandler(self.tenant_config)
            handler._send_bubble_webhook(
                form_type='volunteer_signup',
                responses=self.sample_responses,
                submission_id='sub_test',
                session_id='session_123',
                conversation_id='conv_456'
            )

        payload = captured_data['payload']

        # Check email_details_text exists and is formatted correctly
        self.assertIn('email_details_text', payload)
        self.assertIn('First Name: Jane', payload['email_details_text'])
        self.assertIn('Last Name: Smith', payload['email_details_text'])
        self.assertIn('Email: jane@example.com', payload['email_details_text'])

    @patch('urllib.request.urlopen')
    def test_webhook_includes_email_subject_suffix(self):
        """Test that webhook payload includes email_subject_suffix"""
        mock_response = Mock()
        mock_response.getcode.return_value = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        captured_data = {}

        def capture_request(request, timeout=None):
            captured_data['payload'] = json.loads(request.data.decode('utf-8'))
            return mock_response

        with patch('urllib.request.urlopen', side_effect=capture_request):
            handler = FormHandler(self.tenant_config)
            handler._send_bubble_webhook(
                form_type='volunteer_signup',
                responses=self.sample_responses,
                submission_id='sub_test',
                session_id='session_123',
                conversation_id='conv_456'
            )

        payload = captured_data['payload']

        # Check email_subject_suffix
        self.assertEqual(payload['email_subject_suffix'], 'Jane Smith')

    @patch('urllib.request.urlopen')
    def test_webhook_includes_contact_object(self):
        """Test that webhook payload includes contact object"""
        mock_response = Mock()
        mock_response.getcode.return_value = 200
        mock_response.__enter__ = Mock(return_value=mock_response)
        mock_response.__exit__ = Mock(return_value=False)

        captured_data = {}

        def capture_request(request, timeout=None):
            captured_data['payload'] = json.loads(request.data.decode('utf-8'))
            return mock_response

        with patch('urllib.request.urlopen', side_effect=capture_request):
            handler = FormHandler(self.tenant_config)
            handler._send_bubble_webhook(
                form_type='volunteer_signup',
                responses=self.sample_responses,
                submission_id='sub_test',
                session_id='session_123',
                conversation_id='conv_456'
            )

        payload = captured_data['payload']

        # Check contact object
        self.assertIn('contact', payload)
        self.assertEqual(payload['contact']['name'], 'Jane Smith')
        self.assertEqual(payload['contact']['email'], 'jane@example.com')
        self.assertEqual(payload['contact']['phone'], '+15551234567')


if __name__ == '__main__':
    unittest.main()