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
from moto import mock_dynamodb, mock_ses, mock_sns, mock_s3, mock_lambda, mock_iam
import pytest

# Import the module under test
from form_handler import FormHandler
import pii_subject  # PII Path A Phase 1 — for index-table integration assertions


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
                'message': 'I want to help with food distribution',
                'urgency': 'urgent'  # triggers high priority so SMS fires
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

        # Mock the webhook call (api.example.com won't resolve in tests)
        mock_webhook_response = Mock()
        mock_webhook_response.status_code = 200

        # Create handler and process submission
        handler = FormHandler(self.tenant_config)
        with patch('requests.post', return_value=mock_webhook_response):
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
        self.assertEqual(item['status'], 'pending_fulfillment')

    def test_store_submission_writes_ttl(self):
        """M4 done-bar #2 (master plan v0.3 §M4): _store_submission writes a `ttl`
        attribute on every form-submission row so the existing table-level TTL config
        (infra/modules/ddb-form-submissions-staging/main.tf attribute_name='ttl',
        enabled=true) actually fires. Without this, rows persist indefinitely despite
        the IaC saying otherwise (the writer half of D5 G-A widget claim falsehood).

        Moto-independent (same pattern as test_store_submission_writes_and_indexes_pii_subject_id)
        — the @mock_dynamodb harness for form_handler is pre-existing broken (audit C2);
        this MagicMock approach captures the put_item call directly so the assertion
        is deterministic.
        """
        import form_handler as fh
        import time as _time

        class _FakeIndex:
            def __init__(self):
                self.store = {}

            def get_item(self, Key, ConsistentRead=False):
                v = self.store.get((Key['tenant_id'], Key['normalized_email']))
                return {'Item': v} if v else {}

            def put_item(self, Item, ConditionExpression=None):
                k = (Item['tenant_id'], Item['normalized_email'])
                if ConditionExpression and k in self.store:
                    raise ClientError(
                        {'Error': {'Code': 'ConditionalCheckFailedException'}},
                        'PutItem')
                self.store[k] = Item

        fake_index = _FakeIndex()
        fake_res = MagicMock()
        fake_res.Table.return_value = fake_index
        pii_subject._dynamodb = fake_res
        self.addCleanup(setattr, pii_subject, '_dynamodb', None)

        stored = {}
        subs_table = MagicMock()
        subs_table.put_item.side_effect = (
            lambda Item: stored.__setitem__(Item['submission_id'], Item))

        handler = FormHandler(self.tenant_config)
        responses = {'first_name': 'John', 'email': 'jane@example.com'}

        write_start = int(_time.time())
        with patch.object(fh, 'dynamodb') as md:
            md.Table.return_value = subs_table
            sid = handler._store_submission(
                'volunteer_signup', responses, 'sess_1', 'conv_1',
            )
        write_end = int(_time.time())

        item = stored[sid]
        # ttl is present + integer + ~365 days from write time (1s tolerance)
        self.assertIn('ttl', item,
                      "M4 G-A writer fix: form-submission rows MUST carry a ttl attribute")
        self.assertIsInstance(item['ttl'], int)
        expected_min = write_start + (365 * 24 * 3600) - 1
        expected_max = write_end + (365 * 24 * 3600) + 1
        self.assertGreaterEqual(item['ttl'], expected_min)
        self.assertLessEqual(item['ttl'], expected_max)

    def test_store_submission_writes_and_indexes_pii_subject_id(self):
        """SC4 (audit #1): _store_submission writes an ADDITIVE pii_subject_id and
        a repeat submission from the same email REUSES it (the real indexed path).

        Moto-independent on purpose: the @mock_dynamodb form_handler suite is a
        PRE-EXISTING harness break (form_handler.py builds its boto3 resource at
        import, before moto activates — see audit C2). This deterministic mock
        proves SC4 without depending on that broken harness.
        """
        import form_handler as fh

        class _FakeIndex:
            def __init__(self):
                self.store = {}

            def get_item(self, Key, ConsistentRead=False):
                v = self.store.get((Key['tenant_id'], Key['normalized_email']))
                return {'Item': v} if v else {}

            def put_item(self, Item, ConditionExpression=None):
                k = (Item['tenant_id'], Item['normalized_email'])
                if ConditionExpression and k in self.store:
                    raise ClientError(
                        {'Error': {'Code': 'ConditionalCheckFailedException'}},
                        'PutItem')
                self.store[k] = Item

        fake_index = _FakeIndex()
        fake_res = MagicMock()
        fake_res.Table.return_value = fake_index
        pii_subject._dynamodb = fake_res
        self.addCleanup(setattr, pii_subject, '_dynamodb', None)

        stored = {}
        subs_table = MagicMock()
        subs_table.put_item.side_effect = (
            lambda Item: stored.__setitem__(Item['submission_id'], Item))

        handler = FormHandler(self.tenant_config)
        responses = {'first_name': 'John', 'email': 'John.Doe@Example.com'}

        with patch.object(fh, 'dynamodb') as md:
            md.Table.return_value = subs_table
            sid1 = handler._store_submission('volunteer_signup', responses,
                                             'sess_1', 'conv_1')
            sid2 = handler._store_submission('volunteer_signup', responses,
                                             'sess_2', 'conv_2')

        item1, item2 = stored[sid1], stored[sid2]
        # additive field present + opaque-prefixed on every row
        self.assertTrue(item1['pii_subject_id'].startswith('psub_'))
        self.assertTrue(item2['pii_subject_id'].startswith('psub_'))
        # R9: a stored Phase-1 row is readable via the canonical Phase-2 reader
        self.assertEqual(pii_subject.read_subject_id(item1), item1['pii_subject_id'])
        self.assertEqual(pii_subject.read_subject_id(item2), item2['pii_subject_id'])
        # same person -> same subject id (the indexed reuse path actually ran)
        self.assertEqual(item1['pii_subject_id'], item2['pii_subject_id'])
        # the index really holds the entry (non-gmail: lowercased, dots/+ kept)
        self.assertEqual(
            fake_index.store[('test_tenant_123', 'john.doe@example.com')]
            ['pii_subject_id'],
            item1['pii_subject_id'])

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

    @mock_dynamodb
    @mock_ses
    def test_send_email_notifications_success(self):
        """Test successful email notification sending"""
        # Set up SES mock - verify sender email identity
        ses_client = boto3.client('ses', region_name='us-east-1')
        ses_client.verify_email_identity(EmailAddress='noreply@testorg.com')

        # Create notification-sends table (pk+sk composite key matches production schema)
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
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

    @mock_dynamodb
    def test_send_email_notifications_error(self):
        """Test email notification with SES error"""
        # Create notification-sends table so the failure audit write doesn't crash
        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
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

        mock_ses_client = Mock()
        mock_ses_client.send_email.side_effect = ClientError(
            {'Error': {'Code': 'MessageRejected', 'Message': 'Email rejected'}},
            'SendEmail'
        )

        with patch('form_handler.ses', mock_ses_client):
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

    def test_process_fulfillment_lambda(self):
        """Test fulfillment processing with Lambda invocation.

        Patches the module-level `lambda_client.invoke` directly rather than
        spinning up moto's Lambda mock — moto's `create_function` requires the
        `docker` Python module + a Docker daemon to validate the function
        package, which CI runners don't have. The production code path under
        test is FormHandler._process_fulfillment's routing + invoke args, not
        the runtime Lambda execution.
        """
        handler = FormHandler(self.tenant_config)

        form_config = {
            'fulfillment': {
                'type': 'lambda',
                'function': 'test-fulfillment-function',
                'action': 'process_volunteer'
            }
        }

        with patch('form_handler.lambda_client.invoke') as mock_invoke:
            mock_invoke.return_value = {'StatusCode': 202}
            result = handler._process_fulfillment(
                form_config=form_config,
                form_type='volunteer_signup',
                responses={'first_name': 'John'},
                submission_id='sub_123'
            )

        # Verify Lambda was invoked correctly
        mock_invoke.assert_called_once()
        invoke_kwargs = mock_invoke.call_args.kwargs
        self.assertEqual(invoke_kwargs['FunctionName'], 'test-fulfillment-function')
        self.assertEqual(invoke_kwargs['InvocationType'], 'Event')
        payload = json.loads(invoke_kwargs['Payload'])
        self.assertEqual(payload['action'], 'process_volunteer')
        self.assertEqual(payload['form_type'], 'volunteer_signup')
        self.assertEqual(payload['submission_id'], 'sub_123')
        self.assertEqual(payload['responses'], {'first_name': 'John'})

        # Verify the routing return shape
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

    # --- Sprint D writer extension: _persist_fulfillment_path ---
    # Moto-independent (same pattern as test_store_submission_writes_ttl): the
    # MagicMock approach captures update_item calls directly. This proves the
    # writer half of the PII DSAR fulfillment walker contract — the walker
    # already reads `fulfillment_path` per-row + this writer is what fills it.

    def _patch_submissions_table(self, fh_module):
        """Return (subs_table, update_calls) and patch fh_module.dynamodb."""
        update_calls = []
        subs_table = MagicMock()
        subs_table.update_item.side_effect = (
            lambda **kwargs: update_calls.append(kwargs))
        patcher = patch.object(fh_module, 'dynamodb')
        md = patcher.start()
        self.addCleanup(patcher.stop)
        md.Table.return_value = subs_table
        return subs_table, update_calls

    def test_persist_fulfillment_path_writes_s3_uri_on_stored_result(self):
        """Sprint D writer extension: when fulfillment is type=s3 status=stored,
        UpdateItem fires with the s3:// location on the composite key
        (tenant_id, submission_id) under attribute `fulfillment_path`.
        """
        import form_handler as fh
        _, update_calls = self._patch_submissions_table(fh)

        handler = FormHandler(self.tenant_config)
        handler._persist_fulfillment_path(
            'sub_abc',
            {'type': 's3', 'status': 'stored',
             'location': 's3://bkt/submissions/test_tenant_123/f/sub_abc.json'},
        )

        self.assertEqual(len(update_calls), 1)
        call_kwargs = update_calls[0]
        self.assertEqual(call_kwargs['Key'],
                         {'tenant_id': 'test_tenant_123',
                          'submission_id': 'sub_abc'})
        self.assertEqual(call_kwargs['UpdateExpression'],
                         'SET fulfillment_path = :fp')
        self.assertEqual(
            call_kwargs['ExpressionAttributeValues'],
            {':fp': 's3://bkt/submissions/test_tenant_123/f/sub_abc.json'})

    def test_persist_fulfillment_path_skips_non_s3_fulfillment(self):
        """email/lambda/webhook/no_config results MUST NOT trigger UpdateItem."""
        import form_handler as fh
        _, update_calls = self._patch_submissions_table(fh)

        handler = FormHandler(self.tenant_config)
        for result in [
            None,
            {},
            {'status': 'no_fulfillment_configured'},
            {'type': 'email', 'status': 'sent', 'recipient': 'a@b.com'},
            {'type': 'lambda', 'status': 'invoked'},
            {'type': 'unsupported_type', 'status': 'unsupported'},
        ]:
            handler._persist_fulfillment_path('sub_x', result)
        self.assertEqual(update_calls, [])

    def test_persist_fulfillment_path_skips_s3_error_result(self):
        """type=s3 status=error MUST NOT persist (S3 PutObject failed → no key
        exists; DSAR walker would chase a phantom path)."""
        import form_handler as fh
        _, update_calls = self._patch_submissions_table(fh)

        handler = FormHandler(self.tenant_config)
        handler._persist_fulfillment_path(
            'sub_err',
            {'type': 's3', 'status': 'error', 'error': 'AccessDenied'},
        )
        self.assertEqual(update_calls, [])

    def test_persist_fulfillment_path_skips_s3_stored_without_location(self):
        """Defensive: a stored result without a 'location' key MUST NOT
        persist (avoid writing an empty fulfillment_path that would later
        trigger a parse-failure in the DSAR walker)."""
        import form_handler as fh
        _, update_calls = self._patch_submissions_table(fh)

        handler = FormHandler(self.tenant_config)
        handler._persist_fulfillment_path(
            'sub_noloc',
            {'type': 's3', 'status': 'stored'},
        )
        self.assertEqual(update_calls, [])

    def test_persist_fulfillment_path_swallows_client_error(self):
        """UpdateItem ClientError MUST NOT raise (walker manual-followup
        branch is the documented fallback)."""
        import form_handler as fh
        update_calls = []
        subs_table = MagicMock()
        def _raise(**kwargs):
            update_calls.append(kwargs)
            raise ClientError(
                {'Error': {'Code': 'AccessDeniedException',
                           'Message': 'no grant'}},
                'UpdateItem')
        subs_table.update_item.side_effect = _raise
        with patch.object(fh, 'dynamodb') as md:
            md.Table.return_value = subs_table
            handler = FormHandler(self.tenant_config)
            try:
                handler._persist_fulfillment_path(
                    'sub_err',
                    {'type': 's3', 'status': 'stored',
                     'location': 's3://bkt/submissions/t/f/sub_err.json'},
                )
            except Exception as e:  # noqa: BLE001
                self.fail(f"unexpected raise from _persist_fulfillment_path: {e}")
        self.assertEqual(len(update_calls), 1)

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
        # All-or-nothing: any missing variable returns the original template unchanged
        self.assertEqual(result, 'Hello {first_name} {missing_var}!')

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
        # Verify SES email identities
        ses_client = boto3.client('ses', region_name='us-east-1')
        ses_client.verify_email_identity(EmailAddress='noreply@testorg.com')
        ses_client.verify_email_identity(EmailAddress='noreply@picasso.ai')

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


# TestBubbleIntegration (8 tests) deleted — _send_bubble_webhook() was removed
# from form_handler.py in commit 21b3a02 (Bubble deplatforming). These tests
# validated behaviour that no longer exists in production.


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
        """Test handling camelCase keys — capitalize() lowercases subsequent words (known production behaviour)"""
        self.assertEqual(humanize_key('firstName'), 'First name')
        self.assertEqual(humanize_key('zipCode'), 'Zip code')

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


# TestBubbleWebhookNewFields (3 tests) deleted — _send_bubble_webhook() was removed
# from form_handler.py in commit 21b3a02 (Bubble deplatforming). These tests
# validated behaviour that no longer exists in production.


if __name__ == '__main__':
    unittest.main()