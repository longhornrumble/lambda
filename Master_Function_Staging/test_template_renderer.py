#!/usr/bin/env python3
"""
Comprehensive unit tests for template_renderer.py
Tests template loading, rendering, variable substitution, and validation
"""

import unittest
from unittest.mock import Mock, patch, mock_open
import json
import tempfile
import os
from pathlib import Path

# Import the module under test
from template_renderer import TemplateRenderer


class TestTemplateRenderer(unittest.TestCase):
    """Test cases for TemplateRenderer class"""

    def setUp(self):
        """Set up test fixtures before each test method"""
        self.sample_templates = {
            'email_templates': {
                'volunteer_signup_confirmation': {
                    'subject': 'Welcome to {{organization_name}} Volunteer Program!',
                    'body_html': '''
                        <h1>Welcome {{first_name}}!</h1>
                        <p>Thank you for signing up to volunteer with {{organization_name}}.</p>
                        <p>We will contact you at {{email}} within 48 hours.</p>
                        <p>Your availability: {{availability}}</p>
                        <p>Emergency contact: {{emergency_phone}}</p>
                        <p>Date: {{date}}</p>
                    ''',
                    'body_text': '''
                        Welcome {{first_name}}!

                        Thank you for signing up to volunteer with {{organization_name}}.
                        We will contact you at {{email}} within 48 hours.

                        Your availability: {{availability}}
                        Emergency contact: {{emergency_phone}}
                        Date: {{date}}
                    '''
                },
                'contact_us_acknowledgment': {
                    'subject': 'We received your message - {{organization_name}}',
                    'body_html': '<p>Thank you {{first_name}}, we will respond within 24 hours.</p>',
                    'body_text': 'Thank you {{first_name}}, we will respond within 24 hours.'
                },
                'support_request_received': {
                    'subject': 'URGENT: Support Request from {{first_name}}',
                    'body_html': '<p>Urgent support needed: {{message}}</p>',
                    'body_text': 'Urgent support needed: {{message}}'
                },
                'default': {
                    'subject': 'Form Submission Received',
                    'body_html': '<p>Thank you for your submission.</p>',
                    'body_text': 'Thank you for your submission.'
                }
            },
            'sms_templates': {
                'volunteer_signup_confirmation': {
                    'message': '{{organization_name}}: Welcome {{first_name}}! We\'ll contact you within 48h. {{emergency_phone}}'
                },
                'contact_us_acknowledgment': {
                    'message': '{{organization_name}}: Thanks {{first_name}}! We received your message and will respond within 24h.'
                },
                'support_request_received': {
                    'message': 'URGENT: {{organization_name}} support request from {{first_name}}. Call {{emergency_phone}}'
                },
                'default': {
                    'message': 'Thank you for your submission. We will contact you soon.'
                }
            },
            'webhook_templates': {
                'volunteer_signup': {
                    'headers': {
                        'Content-Type': 'application/json',
                        'X-Event-Type': 'volunteer_signup',
                        'X-Tenant-ID': '{{tenant_id}}'
                    },
                    'body': {
                        'event': 'volunteer_signup',
                        'timestamp': '{{timestamp}}',
                        'submission_id': '{{submission_id}}',
                        'tenant_id': '{{tenant_id}}',
                        'data': {
                            'form_type': '{{form_type}}',
                            'volunteer': {
                                'name': '{{first_name}} {{last_name}}',
                                'email': '{{email}}',
                                'phone': '{{phone}}',
                                'availability': '{{availability}}'
                            },
                            'responses': '{{responses}}'
                        }
                    }
                },
                'support_request': {
                    'headers': {
                        'Content-Type': 'application/json',
                        'X-Event-Type': 'support_request',
                        'X-Priority': 'high'
                    },
                    'body': {
                        'event': 'support_request',
                        'priority': 'high',
                        'message': '{{message}}',
                        'contact': '{{email}}'
                    }
                },
                'form_submission': {
                    'headers': {
                        'Content-Type': 'application/json',
                        'X-Event-Type': 'form_submission'
                    },
                    'body': {
                        'event': 'form_submission',
                        'form_type': '{{form_type}}',
                        'submission_id': '{{submission_id}}',
                        'data': '{{responses}}'
                    }
                },
                'default': {
                    'headers': {'Content-Type': 'application/json'},
                    'body': {'event': 'form_submission'}
                }
            }
        }

        self.sample_responses = {
            'first_name': 'John',
            'last_name': 'Doe',
            'email': 'john.doe@example.com',
            'phone': '+15551234567',
            'availability': 'Weekends',
            'message': 'I need urgent help with housing assistance'
        }

        self.sample_tenant_config = {
            'tenant_id': 'tenant_test_123',
            'organization_name': 'Community Helpers',
            'contact_email': 'info@communityhelpers.org',
            'emergency_phone': '1-800-HELP-NOW',
            'mission_statement': 'help our community thrive',
            'ein_number': '12-3456789',
            'sms_sender_name': 'CommHelp'
        }

    def test_init_with_templates_file(self):
        """Test initialization with valid templates file"""
        # Create temporary templates file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            # Verify templates were loaded
            self.assertEqual(len(renderer.templates['email_templates']), 4)
            self.assertEqual(len(renderer.templates['sms_templates']), 4)
            self.assertEqual(len(renderer.templates['webhook_templates']), 4)

        finally:
            os.unlink(temp_file_path)

    def test_init_with_missing_file(self):
        """Test initialization with missing templates file"""
        renderer = TemplateRenderer('/nonexistent/path/templates.json')

        # Should fall back to default templates
        self.assertIn('email_templates', renderer.templates)
        self.assertIn('default', renderer.templates['email_templates'])

    def test_init_with_invalid_json(self):
        """Test initialization with invalid JSON file"""
        # Create temporary file with invalid JSON
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('invalid json content {')
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            # Should fall back to default templates
            self.assertIn('email_templates', renderer.templates)
            self.assertIn('default', renderer.templates['email_templates'])

        finally:
            os.unlink(temp_file_path)

    def test_render_template_basic(self):
        """Test basic template rendering with simple variables"""
        renderer = TemplateRenderer()

        template = 'Hello {{first_name}} {{last_name}}!'
        variables = {'first_name': 'John', 'last_name': 'Doe'}

        result = renderer.render_template(template, variables)
        self.assertEqual(result, 'Hello John Doe!')

    def test_render_template_missing_variable(self):
        """Test template rendering with missing variables"""
        renderer = TemplateRenderer()

        template = 'Hello {{first_name}} {{missing_var}}!'
        variables = {'first_name': 'John'}

        result = renderer.render_template(template, variables)
        self.assertEqual(result, 'Hello John {{missing_var}}!')

    def test_render_template_none_values(self):
        """Test template rendering with None values"""
        renderer = TemplateRenderer()

        template = 'Hello {{first_name}} {{last_name}}!'
        variables = {'first_name': 'John', 'last_name': None}

        result = renderer.render_template(template, variables)
        self.assertEqual(result, 'Hello John !')

    def test_render_template_complex_variables(self):
        """Test template rendering with complex variable names"""
        renderer = TemplateRenderer()

        template = 'User: {{user_name}}, ID: {{user_id}}, Status: {{is_active}}'
        variables = {'user_name': 'john_doe', 'user_id': '12345', 'is_active': True}

        result = renderer.render_template(template, variables)
        self.assertEqual(result, 'User: john_doe, ID: 12345, Status: True')

    def test_render_email_template_volunteer_signup(self):
        """Test rendering email template for volunteer signup"""
        # Create renderer with sample templates
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            result = renderer.render_email_template(
                'volunteer_signup',
                self.sample_responses,
                self.sample_tenant_config
            )

            # Verify all components are rendered
            self.assertIn('subject', result)
            self.assertIn('body_html', result)
            self.assertIn('body_text', result)

            # Verify variable substitution
            self.assertIn('Community Helpers', result['subject'])
            self.assertIn('John', result['body_html'])
            self.assertIn('john.doe@example.com', result['body_html'])
            self.assertIn('Weekends', result['body_html'])
            self.assertIn('1-800-HELP-NOW', result['body_html'])

            # Verify computed variables
            self.assertIn('2025', result['body_html'])  # Year from date

        finally:
            os.unlink(temp_file_path)

    def test_render_email_template_contact_form(self):
        """Test rendering email template for contact form"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            result = renderer.render_email_template(
                'contact',
                self.sample_responses,
                self.sample_tenant_config
            )

            # Should use contact_us_acknowledgment template
            self.assertIn('We received your message', result['subject'])
            self.assertIn('John', result['body_html'])
            self.assertIn('24 hours', result['body_html'])

        finally:
            os.unlink(temp_file_path)

    def test_render_email_template_support_request(self):
        """Test rendering email template for support request"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            result = renderer.render_email_template(
                'support',
                self.sample_responses,
                self.sample_tenant_config
            )

            # Should use support_request_received template
            self.assertIn('URGENT', result['subject'])
            self.assertIn('John', result['subject'])
            self.assertIn('housing assistance', result['body_html'])

        finally:
            os.unlink(temp_file_path)

    def test_render_email_template_default(self):
        """Test rendering email template with fallback to default"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            result = renderer.render_email_template(
                'unknown_form_type',
                self.sample_responses,
                self.sample_tenant_config
            )

            # Should use default template
            self.assertEqual(result['subject'], 'Form Submission Received')
            self.assertIn('Thank you for your submission', result['body_html'])

        finally:
            os.unlink(temp_file_path)

    def test_render_email_template_no_tenant_config(self):
        """Test rendering email template without tenant configuration"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            result = renderer.render_email_template(
                'volunteer_signup',
                self.sample_responses,
                None
            )

            # Should use default organization values
            self.assertIn('Our Organization', result['body_html'])
            self.assertIn('info@organization.org', result['body_html'])

        finally:
            os.unlink(temp_file_path)

    def test_render_sms_template_volunteer_signup(self):
        """Test rendering SMS template for volunteer signup"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            result = renderer.render_sms_template(
                'volunteer_signup',
                self.sample_responses,
                self.sample_tenant_config
            )

            # Verify SMS content
            self.assertIn('CommHelp', result)  # SMS sender name
            self.assertIn('John', result)
            self.assertIn('48h', result)
            self.assertIn('1-800-HELP-NOW', result)

            # Verify length is reasonable for SMS
            self.assertLessEqual(len(result), 180)  # Allow some buffer over 160

        finally:
            os.unlink(temp_file_path)

    def test_render_sms_template_length_warning(self):
        """Test SMS template rendering with length warning for long messages"""
        # Create template with very long message
        long_templates = {
            'sms_templates': {
                'test_long': {
                    'message': 'This is a very long SMS message that exceeds the recommended 160 character limit for SMS messages and should trigger a warning in the logs {{first_name}} {{last_name}} {{organization_name}} {{emergency_phone}} {{availability}}'
                }
            }
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(long_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            with self.assertLogs(level='WARNING') as log:
                result = renderer.render_sms_template(
                    'test_long',
                    self.sample_responses,
                    self.sample_tenant_config
                )

                # Should log warning about message length
                self.assertTrue(any('chars (recommended max 160)' in msg for msg in log.output))

            # Should still return the message
            self.assertGreater(len(result), 160)

        finally:
            os.unlink(temp_file_path)

    def test_render_sms_template_default(self):
        """Test rendering SMS template with fallback to default"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            result = renderer.render_sms_template(
                'unknown_form_type',
                self.sample_responses,
                self.sample_tenant_config
            )

            # Should use default SMS template
            self.assertEqual(result, 'Thank you for your submission. We will contact you soon.')

        finally:
            os.unlink(temp_file_path)

    def test_render_webhook_template_volunteer_signup(self):
        """Test rendering webhook template for volunteer signup"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            result = renderer.render_webhook_template(
                'volunteer_signup',
                'submission_12345',
                self.sample_responses,
                self.sample_tenant_config
            )

            # Verify structure
            self.assertIn('headers', result)
            self.assertIn('body', result)

            # Verify headers
            headers = result['headers']
            self.assertEqual(headers['Content-Type'], 'application/json')
            self.assertEqual(headers['X-Event-Type'], 'volunteer_signup')
            self.assertEqual(headers['X-Tenant-ID'], 'tenant_test_123')

            # Verify body structure
            body = result['body']
            self.assertEqual(body['event'], 'volunteer_signup')
            self.assertEqual(body['submission_id'], 'submission_12345')
            self.assertEqual(body['tenant_id'], 'tenant_test_123')

            # Verify nested data
            self.assertEqual(body['data']['volunteer']['name'], 'John Doe')
            self.assertEqual(body['data']['volunteer']['email'], 'john.doe@example.com')
            self.assertEqual(body['data']['volunteer']['availability'], 'Weekends')

            # Verify responses are converted back to dict
            self.assertIsInstance(body['data']['responses'], dict)
            self.assertEqual(body['data']['responses']['first_name'], 'John')

        finally:
            os.unlink(temp_file_path)

    def test_render_webhook_template_support_request(self):
        """Test rendering webhook template for support request"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            result = renderer.render_webhook_template(
                'support_request',
                'submission_67890',
                self.sample_responses,
                self.sample_tenant_config
            )

            # Verify high priority headers
            self.assertEqual(result['headers']['X-Priority'], 'high')

            # Verify body content
            body = result['body']
            self.assertEqual(body['priority'], 'high')
            self.assertIn('housing assistance', body['message'])
            self.assertEqual(body['contact'], 'john.doe@example.com')

        finally:
            os.unlink(temp_file_path)

    def test_render_webhook_template_default(self):
        """Test rendering webhook template with fallback to default"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            result = renderer.render_webhook_template(
                'unknown_form_type',
                'submission_99999',
                self.sample_responses,
                self.sample_tenant_config
            )

            # Should use form_submission template (default fallback)
            self.assertEqual(result['headers']['X-Event-Type'], 'form_submission')
            self.assertEqual(result['body']['event'], 'form_submission')

        finally:
            os.unlink(temp_file_path)

    def test_deep_render_nested_dict(self):
        """Test deep rendering of nested dictionary structures"""
        renderer = TemplateRenderer()

        nested_obj = {
            'level1': {
                'level2': {
                    'message': 'Hello {{name}}!'
                },
                'simple': '{{greeting}}'
            }
        }

        variables = {'name': 'John', 'greeting': 'Hi there'}

        result = renderer._deep_render(nested_obj, variables)

        self.assertEqual(result['level1']['level2']['message'], 'Hello John!')
        self.assertEqual(result['level1']['simple'], 'Hi there')

    def test_deep_render_list(self):
        """Test deep rendering of list structures"""
        renderer = TemplateRenderer()

        obj_with_list = {
            'messages': [
                'Hello {{name}}!',
                'Welcome to {{organization}}',
                {'nested': '{{status}}'}
            ]
        }

        variables = {'name': 'John', 'organization': 'Acme Corp', 'status': 'active'}

        result = renderer._deep_render(obj_with_list, variables)

        self.assertEqual(result['messages'][0], 'Hello John!')
        self.assertEqual(result['messages'][1], 'Welcome to Acme Corp')
        self.assertEqual(result['messages'][2]['nested'], 'active')

    def test_deep_render_non_string_types(self):
        """Test deep rendering preserves non-string types"""
        renderer = TemplateRenderer()

        mixed_obj = {
            'string': 'Hello {{name}}',
            'number': 42,
            'boolean': True,
            'null': None,
            'list': [1, 2, 3]
        }

        variables = {'name': 'John'}

        result = renderer._deep_render(mixed_obj, variables)

        self.assertEqual(result['string'], 'Hello John')
        self.assertEqual(result['number'], 42)
        self.assertEqual(result['boolean'], True)
        self.assertIsNone(result['null'])
        self.assertEqual(result['list'], [1, 2, 3])

    def test_get_available_templates(self):
        """Test getting list of available templates"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            available = renderer.get_available_templates()

            # Verify structure
            self.assertIn('email', available)
            self.assertIn('sms', available)
            self.assertIn('webhook', available)

            # Verify counts
            self.assertEqual(len(available['email']), 4)
            self.assertEqual(len(available['sms']), 4)
            self.assertEqual(len(available['webhook']), 4)

            # Verify specific templates
            self.assertIn('volunteer_signup_confirmation', available['email'])
            self.assertIn('default', available['email'])

        finally:
            os.unlink(temp_file_path)

    def test_validate_template_variables_email(self):
        """Test template variable validation for email templates"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            # Test with all required variables
            provided_vars = {
                'organization_name': 'Test Org',
                'first_name': 'John',
                'email': 'john@example.com',
                'availability': 'Weekends',
                'emergency_phone': '1-800-HELP',
                'date': '2025-01-01'
            }

            result = renderer.validate_template_variables(
                'email',
                'volunteer_signup_confirmation',
                provided_vars
            )

            # Should have no missing variables
            self.assertEqual(len(result['missing']), 0)

            # Test with missing variables
            incomplete_vars = {'first_name': 'John'}

            result = renderer.validate_template_variables(
                'email',
                'volunteer_signup_confirmation',
                incomplete_vars
            )

            # Should identify missing variables
            self.assertGreater(len(result['missing']), 0)
            self.assertIn('organization_name', result['missing'])
            self.assertIn('email', result['missing'])

        finally:
            os.unlink(temp_file_path)

    def test_validate_template_variables_webhook(self):
        """Test template variable validation for webhook templates"""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(self.sample_templates, f)
            temp_file_path = f.name

        try:
            renderer = TemplateRenderer(temp_file_path)

            provided_vars = {
                'tenant_id': 'test_123',
                'timestamp': '2025-01-01',
                'submission_id': 'sub_456',
                'form_type': 'volunteer_signup',
                'first_name': 'John',
                'last_name': 'Doe',
                'email': 'john@example.com',
                'phone': '+15551234567',
                'availability': 'Weekends',
                'responses': '{"test": "data"}'
            }

            result = renderer.validate_template_variables(
                'webhook',
                'volunteer_signup',
                provided_vars
            )

            # Should have no missing variables
            self.assertEqual(len(result['missing']), 0)

        finally:
            os.unlink(temp_file_path)

    def test_validate_template_variables_nonexistent_template(self):
        """Test template variable validation for nonexistent template"""
        renderer = TemplateRenderer()

        result = renderer.validate_template_variables(
            'email',
            'nonexistent_template',
            {'some': 'variables'}
        )

        # Should return empty missing and all provided as extra
        self.assertEqual(len(result['missing']), 0)
        self.assertEqual(result['extra'], ['some'])

    def test_render_template_edge_cases(self):
        """Test template rendering edge cases"""
        renderer = TemplateRenderer()

        # Empty template
        result = renderer.render_template('', {})
        self.assertEqual(result, '')

        # Template with no variables
        result = renderer.render_template('No variables here', {'unused': 'var'})
        self.assertEqual(result, 'No variables here')

        # Variables with special characters
        result = renderer.render_template('{{var_name}}', {'var_name': 'Value with "quotes" & symbols'})
        self.assertEqual(result, 'Value with "quotes" & symbols')

        # Numeric variables
        result = renderer.render_template('Count: {{count}}', {'count': 42})
        self.assertEqual(result, 'Count: 42')

    def test_render_template_regex_patterns(self):
        """Test template rendering with various regex patterns"""
        renderer = TemplateRenderer()

        # Multiple variables in one line
        template = '{{first}} {{second}} {{first}} {{third}}'
        variables = {'first': 'A', 'second': 'B', 'third': 'C'}
        result = renderer.render_template(template, variables)
        self.assertEqual(result, 'A B A C')

        # Variables with underscores and numbers
        template = '{{var_1}} {{var_name_2}} {{var3}}'
        variables = {'var_1': 'one', 'var_name_2': 'two', 'var3': 'three'}
        result = renderer.render_template(template, variables)
        self.assertEqual(result, 'one two three')

        # Case sensitivity
        template = '{{Name}} {{name}} {{NAME}}'
        variables = {'Name': 'Title', 'name': 'lower', 'NAME': 'UPPER'}
        result = renderer.render_template(template, variables)
        self.assertEqual(result, 'Title lower UPPER')


if __name__ == '__main__':
    unittest.main()