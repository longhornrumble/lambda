#!/usr/bin/env python3
"""
Comprehensive error handling and edge case tests
Tests exception handling, malformed data, security, and boundary conditions
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import json
import boto3
from moto import mock_dynamodb, mock_ses, mock_sns, mock_s3, mock_lambda
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError
import tempfile
import os
from datetime import datetime

# Import modules under test
from form_handler import FormHandler
from lambda_function import lambda_handler, handle_form_submission


class TestErrorHandling(unittest.TestCase):
    """Test cases for error handling and edge cases"""

    def setUp(self):
        """Set up test fixtures"""
        self.valid_tenant_config = {
            'tenant_id': 'test_tenant_123',
            'tenant_hash': 'hash_abc123',
            'organization_name': 'Test Organization',
            'conversational_forms': {
                'test_form': {
                    'notifications': {
                        'email': {
                            'enabled': True,
                            'recipients': ['test@example.com']
                        }
                    }
                }
            }
        }

        self.valid_form_data = {
            'form_type': 'test_form',
            'responses': {'name': 'John Doe'},
            'session_id': 'session_123',
            'conversation_id': 'conv_456'
        }

        self.mock_context = Mock()
        self.mock_context.aws_request_id = 'test-request-id'

    def test_form_handler_none_config(self):
        """Test FormHandler initialization with None config

        Production bug: FormHandler(None) crashes with AttributeError because
        __init__ calls tenant_config.get('tenant_id') on a NoneType.
        """
        with self.assertRaises(AttributeError):
            FormHandler(None)

    def test_form_handler_empty_config(self):
        """Test FormHandler initialization with empty config"""
        handler = FormHandler({})

        self.assertEqual(handler.tenant_config, {})
        self.assertIsNone(handler.tenant_id)
        self.assertIsNone(handler.tenant_hash)

    def test_form_handler_malformed_config(self):
        """Test FormHandler with malformed configuration"""
        malformed_configs = [
            {'tenant_id': None, 'tenant_hash': 'valid_hash'},
            {'tenant_id': 'valid_id', 'tenant_hash': None},
            {'tenant_id': '', 'tenant_hash': ''},
            {'tenant_id': 123, 'tenant_hash': 456},  # Wrong types
        ]

        for config in malformed_configs:
            handler = FormHandler(config)
            # Should not crash, but handle gracefully
            self.assertIsNotNone(handler.tenant_config)

    def test_handle_form_submission_missing_form_type(self):
        """Test form submission with missing form_type"""
        handler = FormHandler(self.valid_tenant_config)

        invalid_data = {
            'responses': {'name': 'John'},
            'session_id': 'session_123'
            # Missing 'form_type'
        }

        result = handler.handle_form_submission(invalid_data)

        self.assertFalse(result['success'])
        self.assertIn('error', result)

    @mock_dynamodb
    def test_handle_form_submission_empty_responses(self):
        """Test form submission with empty responses"""
        self._create_form_tables()
        handler = FormHandler(self.valid_tenant_config)

        empty_data = {
            'form_type': 'test_form',
            'responses': {},  # Empty responses
            'session_id': 'session_123'
        }

        result = handler.handle_form_submission(empty_data)

        # Should still process successfully
        self.assertTrue(result['success'])

    def test_handle_form_submission_none_responses(self):
        """Test form submission with None responses"""
        handler = FormHandler(self.valid_tenant_config)

        none_data = {
            'form_type': 'test_form',
            'responses': None,  # None responses
            'session_id': 'session_123'
        }

        result = handler.handle_form_submission(none_data)

        # Should handle gracefully
        self.assertIn('success', result)

    def test_handle_form_submission_malformed_responses(self):
        """Test form submission with malformed responses"""
        handler = FormHandler(self.valid_tenant_config)

        malformed_data = {
            'form_type': 'test_form',
            'responses': 'not_a_dict',  # String instead of dict
            'session_id': 'session_123'
        }

        result = handler.handle_form_submission(malformed_data)

        # Should handle the error gracefully
        self.assertFalse(result['success'])
        self.assertIn('error', result)

    def test_handle_form_submission_extremely_large_data(self):
        """Test form submission with extremely large data"""
        handler = FormHandler(self.valid_tenant_config)

        # Create large responses (1MB of text)
        large_text = 'x' * (1024 * 1024)
        large_data = {
            'form_type': 'test_form',
            'responses': {
                'large_field': large_text,
                'normal_field': 'normal value'
            },
            'session_id': 'session_123'
        }

        # Should handle large data (might succeed or fail gracefully)
        result = handler.handle_form_submission(large_data)

        # Either succeeds or fails gracefully
        self.assertIn('success', result)

    @mock_dynamodb
    def test_handle_form_submission_unicode_and_special_chars(self):
        """Test form submission with Unicode and special characters"""
        self._create_form_tables()
        handler = FormHandler(self.valid_tenant_config)

        unicode_data = {
            'form_type': 'test_form',
            'responses': {
                'name': 'José García-Müller',
                'message': '¡Hola! 🌟 Testing unicode: αβγ δεζ ηθι',
                'email': 'josé@müller.com',
                'special_chars': '<script>alert("xss")</script>',
                'emoji': '😊 👍 💯',
                'currency': '€100 £50 ¥1000'
            },
            'session_id': 'session_unicode_test'
        }

        result = handler.handle_form_submission(unicode_data)

        # Should handle Unicode correctly
        self.assertTrue(result['success'])

    @mock_dynamodb
    def test_store_submission_dynamodb_unavailable(self):
        """Test form submission storage when DynamoDB is unavailable"""
        handler = FormHandler(self.valid_tenant_config)

        # Don't create the table to simulate DynamoDB unavailable
        with self.assertRaises(ClientError):
            handler._store_submission(
                form_type='test_form',
                responses={'name': 'John'},
                session_id='session_123',
                conversation_id='conv_456'
            )

    @mock_dynamodb
    def test_store_submission_invalid_data_types(self):
        """Test storing submission with invalid data types"""
        # Set up DynamoDB table
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb.create_table(
            TableName='picasso_form_submissions',
            KeySchema=[{'AttributeName': 'submission_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'submission_id', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )

        handler = FormHandler(self.valid_tenant_config)

        # Test with complex nested objects that might cause serialization issues
        complex_responses = {
            'nested_object': {
                'level1': {
                    'level2': {
                        'level3': 'deep_value'
                    }
                }
            },
            'list_with_objects': [
                {'item': 1, 'data': 'value1'},
                {'item': 2, 'data': 'value2'}
            ],
            'circular_ref': None  # Can't test actual circular refs easily
        }

        # Should handle complex data
        submission_id = handler._store_submission(
            form_type='test_form',
            responses=complex_responses,
            session_id='session_complex',
            conversation_id='conv_complex'
        )

        self.assertIsInstance(submission_id, str)

    @mock_dynamodb
    def test_aws_credentials_error(self):
        """Test handling of AWS credentials errors"""
        self._create_form_tables()
        handler = FormHandler(self.valid_tenant_config)

        # Patch the module-level ses client (created at import time)
        mock_ses_client = Mock()
        mock_ses_client.send_email.side_effect = NoCredentialsError()

        with patch('form_handler.ses', mock_ses_client):
            # _send_email_notifications catches ClientError but not NoCredentialsError
            with self.assertRaises(NoCredentialsError):
                handler._send_email_notifications(
                    {'enabled': True, 'recipients': ['test@example.com']},
                    {'form_type': 'test'},
                    'normal'
                )

    @mock_dynamodb
    def test_aws_partial_credentials_error(self):
        """Test handling of partial AWS credentials"""
        self._create_form_tables()
        handler = FormHandler(self.valid_tenant_config)

        # Patch the module-level sns client (created at import time)
        mock_sns_client = Mock()
        mock_sns_client.publish.side_effect = PartialCredentialsError(
            provider='env',
            cred_var='AWS_SECRET_ACCESS_KEY'
        )

        with patch('form_handler.sns', mock_sns_client):
            # _send_sms_notifications should propagate PartialCredentialsError
            with self.assertRaises(PartialCredentialsError):
                handler._send_sms_notifications(
                    {'enabled': True, 'recipients': ['+15551234567']},
                    {'form_type': 'test'}
                )

    def test_lambda_handler_malformed_event(self):
        """Test Lambda handler with malformed event structure"""
        malformed_events = [
            None,  # None event
            {},    # Empty event
            {'httpMethod': None},  # None httpMethod
            {'queryStringParameters': None},  # None query params
            {'headers': None},  # None headers
            {'body': None},  # None body
        ]

        for event in malformed_events:
            try:
                response = lambda_handler(event, self.mock_context)

                # Should return a response (might be error response)
                self.assertIn('statusCode', response)
                self.assertIn('headers', response)

            except Exception as e:
                # If exception occurs, it should be a handled exception
                self.assertIsInstance(e, (ValueError, TypeError, KeyError))

    def test_lambda_handler_missing_action(self):
        """Test Lambda handler with missing action parameter"""
        event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                # Missing 'action'
                't': 'test_tenant'
            },
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'test': 'data'})
        }

        response = lambda_handler(event, self.mock_context)

        # Should default to health check
        self.assertEqual(response['statusCode'], 200)

    def test_lambda_handler_unknown_action(self):
        """Test Lambda handler with unknown action"""
        event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'unknown_action',
                't': 'test_tenant'
            },
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'test': 'data'})
        }

        response = lambda_handler(event, self.mock_context)

        self.assertEqual(response['statusCode'], 404)
        body = json.loads(response['body'])
        self.assertIn('Action unknown_action not found', body['message'])

    def test_lambda_handler_exception_handling(self):
        """Test Lambda handler exception handling"""
        # Create event that will cause an exception
        event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'form_submission',
                't': 'test_tenant'
            },
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({'test': 'data'})
        }

        # Mock to raise an unexpected exception
        with patch('lambda_function.handle_form_submission') as mock_handle:
            mock_handle.side_effect = Exception('Unexpected error')

            response = lambda_handler(event, self.mock_context)

            self.assertEqual(response['statusCode'], 500)
            body = json.loads(response['body'])
            self.assertEqual(body['error'], 'Internal Server Error')

    @mock_dynamodb
    def test_form_submission_sql_injection_attempts(self):
        """Test form submission with SQL injection attempts"""
        self._create_form_tables()
        handler = FormHandler(self.valid_tenant_config)

        sql_injection_data = {
            'form_type': 'test_form',
            'responses': {
                'name': "'; DROP TABLE users; --",
                'email': "admin'/**/OR/**/1=1#",
                'comment': "1' UNION SELECT * FROM passwords--",
                'field': "1; DELETE FROM forms WHERE 1=1; --"
            },
            'session_id': 'session_sql_test'
        }

        result = handler.handle_form_submission(sql_injection_data)

        # Should handle malicious input safely (no SQL is executed)
        self.assertTrue(result['success'])

    @mock_dynamodb
    def test_form_submission_xss_attempts(self):
        """Test form submission with XSS attempts"""
        self._create_form_tables()
        handler = FormHandler(self.valid_tenant_config)

        xss_data = {
            'form_type': 'test_form',
            'responses': {
                'name': '<script>alert("xss")</script>',
                'message': '<img src="x" onerror="alert(1)">',
                'comment': 'javascript:alert("xss")',
                'field': '<svg onload="alert(1)">',
                'style': '<style>body{background:url("javascript:alert(1)")}</style>'
            },
            'session_id': 'session_xss_test'
        }

        result = handler.handle_form_submission(xss_data)

        # Should store data safely (no script execution)
        self.assertTrue(result['success'])

    @mock_dynamodb
    def test_form_submission_path_traversal_attempts(self):
        """Test form submission with path traversal attempts"""
        self._create_form_tables()
        handler = FormHandler(self.valid_tenant_config)

        path_traversal_data = {
            'form_type': 'test_form',
            'responses': {
                'filename': '../../../etc/passwd',
                'path': '..\\..\\windows\\system32\\config\\sam',
                'upload': '../../../../var/log/auth.log',
                'file': '/proc/self/environ'
            },
            'session_id': 'session_path_test'
        }

        result = handler.handle_form_submission(path_traversal_data)

        # Should handle path traversal attempts safely
        self.assertTrue(result['success'])

    def test_extremely_long_field_values(self):
        """Test form submission with extremely long field values"""
        handler = FormHandler(self.valid_tenant_config)

        long_data = {
            'form_type': 'test_form',
            'responses': {
                'short_field': 'normal',
                'long_field': 'x' * 1000000,  # 1MB field
                'medium_field': 'y' * 10000,   # 10KB field
            },
            'session_id': 'session_long_test'
        }

        result = handler.handle_form_submission(long_data)

        # Should handle long values (may succeed or fail gracefully)
        self.assertIn('success', result)

    @mock_dynamodb
    def test_null_byte_injection(self):
        """Test form submission with null byte injection"""
        self._create_form_tables()
        handler = FormHandler(self.valid_tenant_config)

        null_byte_data = {
            'form_type': 'test_form',
            'responses': {
                'filename': 'file.txt\x00.exe',
                'path': '/safe/path\x00/../../../etc/passwd',
                'content': 'safe content\x00malicious content'
            },
            'session_id': 'session_null_test'
        }

        result = handler.handle_form_submission(null_byte_data)

        # Should handle null bytes safely
        self.assertTrue(result['success'])

    def test_concurrent_form_submissions(self):
        """Test handling of concurrent form submissions"""
        import threading
        import time

        handler = FormHandler(self.valid_tenant_config)
        results = []
        errors = []

        def submit_form(form_id):
            try:
                data = {
                    'form_type': 'test_form',
                    'responses': {'id': form_id, 'name': f'User {form_id}'},
                    'session_id': f'session_{form_id}'
                }
                result = handler.handle_form_submission(data)
                results.append(result)
            except Exception as e:
                errors.append(e)

        # Start multiple threads
        threads = []
        for i in range(10):
            thread = threading.Thread(target=submit_form, args=(i,))
            threads.append(thread)

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for completion
        for thread in threads:
            thread.join()

        # Should handle concurrent requests
        self.assertEqual(len(errors), 0)  # No exceptions
        self.assertEqual(len(results), 10)  # All requests processed

    def test_memory_usage_with_large_forms(self):
        """Test memory usage with large form submissions"""
        handler = FormHandler(self.valid_tenant_config)

        # Create form with many fields
        large_responses = {}
        for i in range(1000):  # 1000 fields
            large_responses[f'field_{i}'] = f'value_{i}' * 100  # Each value 600 chars

        large_form_data = {
            'form_type': 'test_form',
            'responses': large_responses,
            'session_id': 'session_memory_test'
        }

        result = handler.handle_form_submission(large_form_data)

        # Should handle large forms without memory issues
        self.assertIn('success', result)

    def _create_form_tables(self):
        """Helper: create DynamoDB tables required by FormHandler"""
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb.create_table(
            TableName='picasso_form_submissions',
            KeySchema=[{'AttributeName': 'submission_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'submission_id', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
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


@mock_dynamodb
class TestEdgeCases(unittest.TestCase):
    """Test edge cases and boundary conditions"""

    def setUp(self):
        """Create DynamoDB tables required by FormHandler"""
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        dynamodb.create_table(
            TableName='picasso_form_submissions',
            KeySchema=[{'AttributeName': 'submission_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'submission_id', 'AttributeType': 'S'}],
            BillingMode='PAY_PER_REQUEST'
        )
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

    def test_zero_length_strings(self):
        """Test handling of zero-length strings"""
        handler = FormHandler({
            'tenant_id': '',
            'tenant_hash': '',
            'conversational_forms': {
                '': {  # Empty form type
                    'notifications': {}
                }
            }
        })

        result = handler.handle_form_submission({
            'form_type': '',
            'responses': {'': ''},  # Empty field name and value
            'session_id': '',
            'conversation_id': ''
        })

        # Should handle empty strings gracefully
        self.assertIn('success', result)

    def test_numeric_field_names(self):
        """Test handling of numeric field names"""
        config = {
            'tenant_id': 'test',
            'conversational_forms': {
                'numeric_test': {
                    'notifications': {'email': {'enabled': False}}
                }
            }
        }

        handler = FormHandler(config)

        numeric_data = {
            'form_type': 'numeric_test',
            'responses': {
                '123': 'numeric field name',
                '0': 'zero field',
                '-1': 'negative field',
                '3.14': 'decimal field'
            },
            'session_id': 'session_numeric'
        }

        result = handler.handle_form_submission(numeric_data)
        self.assertTrue(result['success'])

    def test_boolean_and_none_values(self):
        """Test handling of boolean and None values in responses"""
        config = {
            'tenant_id': 'test',
            'conversational_forms': {
                'bool_test': {
                    'notifications': {'email': {'enabled': False}}
                }
            }
        }

        handler = FormHandler(config)

        bool_data = {
            'form_type': 'bool_test',
            'responses': {
                'is_true': True,
                'is_false': False,
                'is_none': None,
                'number_zero': 0,
                'empty_list': [],
                'empty_dict': {}
            },
            'session_id': 'session_bool'
        }

        result = handler.handle_form_submission(bool_data)
        self.assertTrue(result['success'])


if __name__ == '__main__':
    unittest.main()