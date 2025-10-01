"""
Template Renderer Module
Loads and renders notification templates for emails, SMS, and webhooks
"""

import json
import re
from typing import Dict, Any, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class TemplateRenderer:
    """Renders notification templates with variable substitution"""

    def __init__(self, templates_path: Optional[str] = None):
        """
        Initialize template renderer

        Args:
            templates_path: Path to templates JSON file
        """
        self.templates = {}
        self.templates_path = templates_path or Path(__file__).parent / 'notification_templates.json'
        self._load_templates()

    def _load_templates(self):
        """Load templates from JSON file"""
        try:
            with open(self.templates_path, 'r') as f:
                self.templates = json.load(f)
            logger.info(f"Loaded {len(self.templates.get('email_templates', {}))} email templates")
            logger.info(f"Loaded {len(self.templates.get('sms_templates', {}))} SMS templates")
            logger.info(f"Loaded {len(self.templates.get('webhook_templates', {}))} webhook templates")
        except FileNotFoundError:
            logger.warning(f"Template file not found: {self.templates_path}")
            self.templates = self._get_default_templates()
        except Exception as e:
            logger.error(f"Error loading templates: {e}")
            self.templates = self._get_default_templates()

    def _get_default_templates(self) -> Dict[str, Any]:
        """Return default templates if file not found"""
        return {
            'email_templates': {
                'default': {
                    'subject': 'Form Submission Received',
                    'body_html': '<p>Thank you for your submission.</p>',
                    'body_text': 'Thank you for your submission.'
                }
            },
            'sms_templates': {
                'default': {
                    'message': 'Thank you for your submission. We will contact you soon.'
                }
            },
            'webhook_templates': {
                'default': {
                    'headers': {'Content-Type': 'application/json'},
                    'body': {'event': 'form_submission'}
                }
            }
        }

    def render_template(self, template_string: str, variables: Dict[str, Any]) -> str:
        """
        Render a template string with variable substitution

        Args:
            template_string: Template with {{variable}} placeholders
            variables: Dictionary of variable values

        Returns:
            Rendered string
        """
        # Handle None values
        safe_vars = {k: (v if v is not None else '') for k, v in variables.items()}

        # Find all {{variable}} patterns
        pattern = r'\{\{(\w+)\}\}'

        def replace_var(match):
            var_name = match.group(1)
            return str(safe_vars.get(var_name, match.group(0)))

        return re.sub(pattern, replace_var, template_string)

    def render_email_template(
        self,
        form_type: str,
        responses: Dict[str, Any],
        tenant_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, str]:
        """
        Render email template for a form submission

        Args:
            form_type: Type of form (volunteer_signup, contact_us, etc.)
            responses: Form responses
            tenant_config: Tenant configuration for organization details

        Returns:
            Dictionary with subject, body_html, and body_text
        """
        # Build template key
        template_key = f"{form_type}_confirmation"
        if form_type == 'contact':
            template_key = 'contact_us_acknowledgment'
        elif form_type == 'support':
            template_key = 'support_request_received'

        # Get template
        template = self.templates.get('email_templates', {}).get(
            template_key,
            self.templates.get('email_templates', {}).get('default', {})
        )

        # Build variables
        variables = {**responses}
        if tenant_config:
            variables['organization_name'] = tenant_config.get('organization_name', 'Our Organization')
            variables['contact_email'] = tenant_config.get('contact_email', 'info@organization.org')
            variables['emergency_phone'] = tenant_config.get('emergency_phone', '1-800-HELP')
            variables['mission_statement'] = tenant_config.get('mission_statement', 'make a difference')
            variables['ein_number'] = tenant_config.get('ein_number', 'XX-XXXXXXX')

        # Add computed variables
        from datetime import datetime
        variables['date'] = datetime.utcnow().strftime('%B %d, %Y')
        variables['timestamp'] = datetime.utcnow().isoformat()

        # Render template
        return {
            'subject': self.render_template(template.get('subject', ''), variables),
            'body_html': self.render_template(template.get('body_html', ''), variables),
            'body_text': self.render_template(template.get('body_text', ''), variables)
        }

    def render_sms_template(
        self,
        form_type: str,
        responses: Dict[str, Any],
        tenant_config: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Render SMS template for a form submission

        Args:
            form_type: Type of form
            responses: Form responses
            tenant_config: Tenant configuration

        Returns:
            Rendered SMS message (max 160 chars recommended)
        """
        # Get template key
        template_key = f"{form_type}_confirmation"
        if form_type == 'contact':
            template_key = 'contact_us_acknowledgment'
        elif form_type == 'support':
            template_key = 'support_request_received'

        # Get template
        template = self.templates.get('sms_templates', {}).get(
            template_key,
            self.templates.get('sms_templates', {}).get('default', {})
        )

        # Build variables
        variables = {**responses}
        if tenant_config:
            variables['organization_name'] = tenant_config.get('sms_sender_name',
                                                              tenant_config.get('organization_name', 'Org'))
            variables['emergency_phone'] = tenant_config.get('emergency_phone', '911')

        # Add computed variables
        from datetime import datetime
        variables['date'] = datetime.utcnow().strftime('%m/%d')

        # Render and truncate if needed
        message = self.render_template(template.get('message', ''), variables)

        # Warn if message is too long
        if len(message) > 160:
            logger.warning(f"SMS message is {len(message)} chars (recommended max 160)")

        return message

    def render_webhook_template(
        self,
        form_type: str,
        submission_id: str,
        responses: Dict[str, Any],
        tenant_config: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Render webhook payload for a form submission

        Args:
            form_type: Type of form
            submission_id: Unique submission ID
            responses: Form responses
            tenant_config: Tenant configuration

        Returns:
            Dictionary with headers and body for webhook
        """
        # Get template
        template_key = form_type
        if form_type == 'volunteer_signup':
            template_key = 'volunteer_signup'
        elif form_type == 'support_request':
            template_key = 'support_request'
        else:
            template_key = 'form_submission'

        template = self.templates.get('webhook_templates', {}).get(
            template_key,
            self.templates.get('webhook_templates', {}).get('default', {})
        )

        # Build variables
        from datetime import datetime
        variables = {
            **responses,
            'submission_id': submission_id,
            'form_type': form_type,
            'timestamp': datetime.utcnow().isoformat(),
            'tenant_id': tenant_config.get('tenant_id', '') if tenant_config else ''
        }

        # Special handling for responses field (keep as dict)
        variables['responses'] = json.dumps(responses)

        # Render headers
        headers = {}
        for key, value in template.get('headers', {}).items():
            headers[key] = self.render_template(value, variables)

        # Render body (deep render for nested structures)
        body = self._deep_render(template.get('body', {}), variables)

        # Parse responses back to dict if it was stringified
        if isinstance(body.get('data', {}).get('responses'), str):
            try:
                body['data']['responses'] = json.loads(body['data']['responses'])
            except:
                body['data']['responses'] = responses

        return {
            'headers': headers,
            'body': body
        }

    def _deep_render(self, obj: Any, variables: Dict[str, Any]) -> Any:
        """
        Recursively render templates in nested structures

        Args:
            obj: Object to render (dict, list, or string)
            variables: Template variables

        Returns:
            Rendered object
        """
        if isinstance(obj, str):
            return self.render_template(obj, variables)
        elif isinstance(obj, dict):
            return {k: self._deep_render(v, variables) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._deep_render(item, variables) for item in obj]
        else:
            return obj

    def get_available_templates(self) -> Dict[str, list]:
        """
        Get list of available templates by type

        Returns:
            Dictionary with email, sms, and webhook template names
        """
        return {
            'email': list(self.templates.get('email_templates', {}).keys()),
            'sms': list(self.templates.get('sms_templates', {}).keys()),
            'webhook': list(self.templates.get('webhook_templates', {}).keys())
        }

    def validate_template_variables(
        self,
        template_type: str,
        template_name: str,
        variables: Dict[str, Any]
    ) -> Dict[str, list]:
        """
        Validate that required variables are provided for a template

        Args:
            template_type: 'email', 'sms', or 'webhook'
            template_name: Name of the template
            variables: Provided variables

        Returns:
            Dictionary with 'missing' and 'extra' variable lists
        """
        # Get template
        templates_key = f"{template_type}_templates"
        template = self.templates.get(templates_key, {}).get(template_name, {})

        if not template:
            return {'missing': [], 'extra': list(variables.keys())}

        # Extract variable names from template
        required_vars = set()

        def extract_vars(obj):
            if isinstance(obj, str):
                pattern = r'\{\{(\w+)\}\}'
                required_vars.update(re.findall(pattern, obj))
            elif isinstance(obj, dict):
                for value in obj.values():
                    extract_vars(value)
            elif isinstance(obj, list):
                for item in obj:
                    extract_vars(item)

        extract_vars(template)

        # Compare with provided
        provided_vars = set(variables.keys())

        return {
            'missing': list(required_vars - provided_vars),
            'extra': list(provided_vars - required_vars)
        }


# Example usage
if __name__ == '__main__':
    renderer = TemplateRenderer()

    # Example form responses
    responses = {
        'first_name': 'John',
        'last_name': 'Doe',
        'email': 'john@example.com',
        'phone': '555-1234',
        'availability': 'Weekends',
        'message': 'I would like to volunteer for your food distribution program.'
    }

    # Example tenant config
    tenant_config = {
        'tenant_id': 'tenant_abc123',
        'organization_name': 'Community Helpers',
        'contact_email': 'volunteer@communityhelpers.org',
        'emergency_phone': '1-800-HELP-NOW',
        'sms_sender_name': 'CommHelp'
    }

    # Render email
    email = renderer.render_email_template('volunteer_signup', responses, tenant_config)
    print("Email Subject:", email['subject'])
    print("Email Body (text):", email['body_text'][:200], "...")

    # Render SMS
    sms = renderer.render_sms_template('volunteer_signup', responses, tenant_config)
    print("\nSMS Message:", sms)

    # Render webhook
    webhook = renderer.render_webhook_template(
        'volunteer_signup',
        'form_volunteer_123456_abcd',
        responses,
        tenant_config
    )
    print("\nWebhook Headers:", webhook['headers'])
    print("Webhook Body:", json.dumps(webhook['body'], indent=2))