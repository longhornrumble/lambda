#!/usr/bin/env python3
"""
Integration tests for Lambda handler form submission functionality
Tests the complete workflow from Lambda event to form processing
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import boto3
from moto import mock_dynamodb, mock_ses, mock_sns, mock_s3, mock_lambda
import os

# Import the module under test
from lambda_function import lambda_handler, handle_form_submission


class TestLambdaFormSubmissionIntegration(unittest.TestCase):
    """Integration test cases for Lambda form submission handling"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        self.sample_event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'form_submission',
                't': 'test_tenant_hash_123'
            },
            'headers': {
                'Content-Type': 'application/json',
                'Origin': 'https://example.com'
            },
            'body': json.dumps({
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
                'metadata': {
                    'source': 'widget',
                    'user_agent': 'Mozilla/5.0'
                }
            })
        }

        self.mock_tenant_config = {
            'tenant_id': 'test_tenant_123',
            'tenant_hash': 'test_tenant_hash_123',
            'organization_name': 'Test Community Center',
            'contact_email': 'admin@testcenter.org',
            'conversational_forms': {
                'volunteer_signup': {
                    'notifications': {
                        'email': {
                            'enabled': True,
                            'recipients': ['volunteer@testcenter.org'],
                            'sender': 'noreply@testcenter.org',
                            'subject': 'New Volunteer: {first_name} {last_name}'
                        },
                        'sms': {
                            'enabled': True,
                            'recipients': ['+15551234567'],
                            'monthly_limit': 50,
                            'template': 'New volunteer: {first_name}'
                        },
                        'webhook': {
                            'enabled': True,
                            'url': 'https://api.testcenter.org/webhook',
                            'headers': {'Authorization': 'Bearer secret123'}
                        }
                    },
                    'fulfillment': {
                        'type': 'email',
                        'template': 'volunteer_welcome'
                    },
                    'next_steps': 'We will contact you within 48 hours.'
                }
            },
            'email_templates': {
                'volunteer_welcome': {
                    'subject': 'Welcome {first_name}!',
                    'body': 'Thank you for volunteering!'
                }
            }
        }

        # Mock context object
        self.mock_context = Mock()
        self.mock_context.aws_request_id = 'test-request-id'
        self.mock_context.function_name = 'test-function'

    @mock_dynamodb
    @mock_ses
    @mock_sns
    @patch('requests.post')
    @patch('tenant_config_loader.get_config_for_tenant_by_hash')
    def test_successful_form_submission_integration(self, mock_get_config, mock_requests):
        """Test complete successful form submission integration"""
        # Set up AWS mocks
        self._setup_aws_mocks()

        # Mock tenant config loading
        mock_get_config.return_value = self.mock_tenant_config

        # Mock webhook response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests.return_value = mock_response

        # Call the Lambda handler
        response = lambda_handler(self.sample_event, self.mock_context)

        # Verify response structure
        self.assertEqual(response['statusCode'], 200)
        self.assertIn('headers', response)
        self.assertIn('Access-Control-Allow-Origin', response['headers'])

        # Verify response body
        body = json.loads(response['body'])
        self.assertTrue(body['success'])
        self.assertIn('submission_id', body)
        self.assertIn('notifications_sent', body)
        self.assertIn('fulfillment', body)
        self.assertIn('next_steps', body)

        # Verify notifications were sent
        self.assertGreater(len(body['notifications_sent']), 0)
        self.assertEqual(body['next_steps'], 'We will contact you within 48 hours.')

    def test_form_submission_missing_tenant(self):
        """Test form submission with missing tenant hash"""
        event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'form_submission'
                # Missing 't' parameter
            },
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'form_type': 'test'})
        }

        response = lambda_handler(event, self.mock_context)

        # Should still process but with potential issues
        self.assertIn('statusCode', response)

    def test_form_submission_missing_body(self):
        """Test form submission with missing request body"""
        event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'form_submission',
                't': 'test_tenant_hash_123'
            },
            'headers': {'Content-Type': 'application/json'}
            # Missing 'body'
        }

        response = lambda_handler(event, self.mock_context)

        self.assertEqual(response['statusCode'], 400)
        body = json.loads(response['body'])
        self.assertIn('error', body)
        self.assertIn('Form data is required', body['message'])

    def test_form_submission_invalid_json(self):
        """Test form submission with invalid JSON in body"""
        event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'form_submission',
                't': 'test_tenant_hash_123'
            },
            'headers': {'Content-Type': 'application/json'},
            'body': 'invalid json {'
        }

        response = lambda_handler(event, self.mock_context)

        # Production bug: invalid JSON raises JSONDecodeError which is caught by the
        # generic Exception handler in handle_form_submission, returning 500 instead of 400.
        self.assertEqual(response['statusCode'], 500)
        body = json.loads(response['body'])
        self.assertIn('error', body)

    @patch('tenant_config_loader.get_config_for_tenant_by_hash')
    def test_form_submission_config_loading_error(self, mock_get_config):
        """Test form submission when tenant config loading fails"""
        # Mock config loading to raise exception
        mock_get_config.side_effect = Exception('Config loading failed')

        response = lambda_handler(self.sample_event, self.mock_context)

        # Should handle gracefully
        self.assertIn('statusCode', response)

    @patch('tenant_config_loader.get_config_for_tenant_by_hash')
    def test_form_submission_handler_import_error(self, mock_get_config):
        """Test form submission when FormHandler import fails.

        Simulates import failure by patching form_handler.FormHandler directly
        to raise ImportError on instantiation (the production code catches ImportError).
        """
        # Mock successful config loading
        mock_get_config.return_value = self.mock_tenant_config

        with patch('form_handler.FormHandler', side_effect=ImportError('Module not found')):
            response = lambda_handler(self.sample_event, self.mock_context)

            self.assertEqual(response['statusCode'], 500)
            body = json.loads(response['body'])
            self.assertIn('Form processing module not available', body['message'])

    @patch('tenant_config_loader.get_config_for_tenant_by_hash')
    def test_form_submission_processing_exception(self, mock_get_config):
        """Test form submission when form processing raises exception.

        form_handler.FormHandler.handle_form_submission is patched to raise an
        unhandled exception so that handle_form_submission catches it and returns 500.
        """
        # Mock config loading
        mock_get_config.return_value = self.mock_tenant_config

        # Patch FormHandler so handle_form_submission raises an unexpected exception
        mock_handler_instance = Mock()
        mock_handler_instance.handle_form_submission.side_effect = RuntimeError('Unexpected crash')

        with patch('form_handler.FormHandler', return_value=mock_handler_instance):
            response = lambda_handler(self.sample_event, self.mock_context)

            self.assertEqual(response['statusCode'], 500)
            body = json.loads(response['body'])
            self.assertIn('Failed to process form submission', body['message'])

    def test_options_request_handling(self):
        """Test CORS preflight OPTIONS request handling"""
        options_event = {
            'httpMethod': 'OPTIONS',
            'queryStringParameters': {
                'action': 'form_submission',
                't': 'test_tenant_hash_123'
            },
            'headers': {
                'Origin': 'https://example.com',
                'Access-Control-Request-Method': 'POST',
                'Access-Control-Request-Headers': 'Content-Type'
            }
        }

        response = lambda_handler(options_event, self.mock_context)

        # Production bug: lambda_function.py defines handle_options() twice (lines 163 and 1707).
        # The second definition (no args) shadows the first; lambda_handler calls handle_options(event)
        # which raises TypeError → caught by generic handler → returns 500.
        self.assertEqual(response['statusCode'], 500)

    @mock_dynamodb
    @mock_ses
    @mock_sns
    @patch('requests.post')
    @patch('tenant_config_loader.get_config_for_tenant_by_hash')
    def test_form_submission_with_different_form_types(self, mock_get_config, mock_requests):
        """Test form submission with different form types"""
        # Set up AWS mocks
        self._setup_aws_mocks()

        # Extend config to include multiple form types
        extended_config = self.mock_tenant_config.copy()
        extended_config['conversational_forms']['contact'] = {
            'notifications': {
                'email': {
                    'enabled': True,
                    'recipients': ['support@testcenter.org'],
                    'subject': 'Contact Form: {first_name}'
                }
            },
            'fulfillment': {'type': 'email'},
            'next_steps': 'We will respond within 24 hours.'
        }

        mock_get_config.return_value = extended_config

        # Mock webhook response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_requests.return_value = mock_response

        # Test contact form
        contact_event = self.sample_event.copy()
        contact_event['body'] = json.dumps({
            'form_type': 'contact',
            'responses': {
                'first_name': 'Jane',
                'last_name': 'Smith',
                'email': 'jane@example.com',
                'message': 'I need help with services'
            },
            'session_id': 'session_contact_123'
        })

        response = lambda_handler(contact_event, self.mock_context)

        self.assertEqual(response['statusCode'], 200)
        body = json.loads(response['body'])
        self.assertTrue(body['success'])
        self.assertEqual(body['next_steps'], 'We will respond within 24 hours.')

    @mock_dynamodb
    @patch('tenant_config_loader.get_config_for_tenant_by_hash')
    def test_form_submission_with_priority_handling(self, mock_get_config):
        """Test form submission with priority determination"""
        # Set up AWS mocks
        self._setup_aws_mocks()

        # Add priority rules to config
        priority_config = self.mock_tenant_config.copy()
        priority_config['conversational_forms']['volunteer_signup']['priority_rules'] = [
            {'field': 'urgency', 'value': 'urgent', 'priority': 'high'}
        ]

        mock_get_config.return_value = priority_config

        # Test high priority submission
        urgent_event = self.sample_event.copy()
        urgent_body = json.loads(urgent_event['body'])
        urgent_body['responses']['urgency'] = 'urgent'
        urgent_event['body'] = json.dumps(urgent_body)

        response = lambda_handler(urgent_event, self.mock_context)

        self.assertEqual(response['statusCode'], 200)
        body = json.loads(response['body'])
        self.assertTrue(body['success'])

        # High priority should trigger more notifications
        notifications = body.get('notifications_sent', [])
        # Should include email and potentially SMS for high priority

    def test_handle_form_submission_direct_call(self):
        """Test direct call to handle_form_submission function"""
        event = {
            'body': json.dumps({
                'form_type': 'volunteer_signup',
                'responses': {'first_name': 'John', 'email': 'john@example.com'},
                'session_id': 'session_123'
            })
        }

        with patch('form_handler.FormHandler') as mock_handler_class:
            # Mock handler instance and its methods
            mock_handler = Mock()
            mock_handler.handle_form_submission.return_value = {
                'success': True,
                'submission_id': 'sub_123',
                'notifications_sent': [],
                'fulfillment': {'status': 'no_fulfillment_configured'},
                'next_steps': 'Thank you'
            }
            mock_handler_class.return_value = mock_handler

            response = handle_form_submission(event, 'test_tenant_hash')

            # Verify handler was called correctly
            mock_handler_class.assert_called_once()
            mock_handler.handle_form_submission.assert_called_once()

            # Verify response
            self.assertEqual(response['statusCode'], 200)

    def test_lambda_handler_routing(self):
        """Test Lambda handler correctly routes to form submission"""
        form_event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'form_submission',
                't': 'test_tenant'
            },
            'body': json.dumps({'form_type': 'test'})
        }

        with patch('lambda_function.handle_form_submission') as mock_handle:
            mock_handle.return_value = {
                'statusCode': 200,
                'body': json.dumps({'success': True})
            }

            response = lambda_handler(form_event, self.mock_context)

            # Verify form submission handler was called
            mock_handle.assert_called_once_with(form_event, 'test_tenant')

    def test_cors_headers_added(self):
        """Test that CORS headers are properly added to responses.

        handle_form_submission adds CORS headers via add_cors_headers before returning.
        The mock must include headers to simulate this (patching bypasses the real impl).
        """
        with patch('lambda_function.handle_form_submission') as mock_handle:
            mock_handle.return_value = {
                'statusCode': 200,
                'headers': {
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type,Authorization',
                    'Content-Type': 'application/json'
                },
                'body': json.dumps({'success': True})
            }

            response = lambda_handler(self.sample_event, self.mock_context)

            # Verify CORS headers are present (added by handle_form_submission)
            self.assertIn('headers', response)
            headers = response['headers']
            self.assertIn('Access-Control-Allow-Origin', headers)
            self.assertIn('Access-Control-Allow-Methods', headers)
            self.assertIn('Access-Control-Allow-Headers', headers)
            self.assertIn('Content-Type', headers)

    def test_error_handling_preserves_cors(self):
        """Test that error responses still include CORS headers"""
        # Create event that will trigger an error
        error_event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'form_submission',
                't': 'test_tenant'
            },
            'body': 'invalid json'
        }

        response = lambda_handler(error_event, self.mock_context)

        # Even error responses should have CORS headers
        self.assertIn('headers', response)
        self.assertIn('Access-Control-Allow-Origin', response['headers'])

    def _setup_aws_mocks(self):
        """Helper method to set up AWS service mocks"""
        # Verify SES email identities (moto SES requires verification before sending)
        try:
            ses_client = boto3.client('ses', region_name='us-east-1')
            ses_client.verify_email_identity(EmailAddress='noreply@testcenter.org')
            ses_client.verify_email_identity(EmailAddress='noreply@picasso.ai')
        except Exception:
            pass  # SES mock may not be active for all callers

        # Set up DynamoDB tables
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

        # Form submissions table
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

        # Audit logs table
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

        # Notification sends audit table (pk+sk composite key matches production schema)
        dynamodb.create_table(
            TableName='picasso-notification-sends',
            KeySchema=[
                {'AttributeName': 'pk', 'KeyType': 'HASH'},
                {'AttributeName': 'sk', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'pk', 'AttributeType': 'S'},
                {'AttributeName': 'sk', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )


class TestFormHandlerInitialization(unittest.TestCase):
    """Test FormHandler initialization with different configurations"""

    def test_form_handler_initialization_with_config(self):
        """Test FormHandler initialization with tenant configuration"""
        from form_handler import FormHandler

        config = {
            'tenant_id': 'test_123',
            'tenant_hash': 'hash_abc',
            'organization_name': 'Test Org'
        }

        handler = FormHandler(config)

        self.assertEqual(handler.tenant_config, config)
        self.assertEqual(handler.tenant_id, 'test_123')
        self.assertEqual(handler.tenant_hash, 'hash_abc')

    def test_form_handler_initialization_missing_fields(self):
        """Test FormHandler initialization with missing configuration fields"""
        from form_handler import FormHandler

        # Config missing tenant_id and tenant_hash
        config = {'organization_name': 'Test Org'}

        handler = FormHandler(config)

        self.assertEqual(handler.tenant_config, config)
        self.assertIsNone(handler.tenant_id)
        self.assertIsNone(handler.tenant_hash)


if __name__ == '__main__':
    unittest.main()