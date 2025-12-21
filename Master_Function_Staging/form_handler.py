"""
Conversational Form Handler for Master Function
Processes form submissions from Picasso chat widget
Handles notifications, storage, and fulfillment
"""

import json
import uuid
import logging
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')
sns = boto3.client('sns')
s3 = boto3.client('s3')
lambda_client = boto3.client('lambda')

# Initialize logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# DynamoDB tables
SUBMISSIONS_TABLE = 'picasso_form_submissions'
SMS_USAGE_TABLE = 'picasso_sms_usage'
AUDIT_TABLE = 'picasso_audit_logs'


def normalize_label(label: str) -> str:
    """
    Normalize a label to snake_case
    "Caregiver's Phone Number" -> "caregivers_phone_number"
    """
    import re
    if not label:
        return ''
    result = label.lower()
    result = re.sub(r"[''']", '', result)          # Remove apostrophes
    result = re.sub(r'[^a-z0-9]+', '_', result)    # Replace non-alphanumeric with underscore
    result = re.sub(r'^_+|_+$', '', result)        # Trim leading/trailing underscores
    result = re.sub(r'_+', '_', result)            # Collapse multiple underscores
    return result


def transform_form_data_to_labels(form_data: Dict[str, Any], form_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform form data field IDs to human-readable labels
    Converts keys like "field_1761666576305.first_name" to "first_name"

    Args:
        form_data: Raw form responses with field IDs as keys
        form_config: Form configuration with field definitions

    Returns:
        Transformed form data with human-readable keys
    """
    if not form_data or not form_config.get('fields'):
        return form_data or {}

    transformed = {}
    fields = form_config.get('fields', [])

    # Build a lookup map for field IDs to labels
    field_map = {}

    for field in fields:
        field_id = field.get('id', '')
        # Normalize label to snake_case
        normalized_label = normalize_label(field.get('label', ''))

        # For composite fields with subfields (name, address)
        subfields = field.get('subfields', [])
        if subfields:
            for subfield in subfields:
                # Subfield ID format: "field_123.first_name"
                subfield_key = subfield.get('id', '')
                # Use just the subfield label (e.g., "first_name", "last_name")
                subfield_label = normalize_label(subfield.get('label', ''))
                field_map[subfield_key] = subfield_label
        else:
            # Simple field - use the field's label
            field_map[field_id] = normalized_label

    # Transform the form data keys
    for key, value in form_data.items():
        if key in field_map:
            # Use the mapped label
            transformed[field_map[key]] = value
        else:
            # Fallback: try to extract a readable name from the key
            # e.g., "field_123.first_name" -> "first_name"
            parts = key.split('.')
            if len(parts) > 1:
                transformed[parts[-1]] = value
            else:
                # Keep original key if no mapping found
                transformed[key] = value

    return transformed


# ============================================================================
# EMAIL DETAILS BUILDER - Human-readable formatting for Bubble email templates
# ============================================================================

# Acronyms to preserve in title case (kept uppercase)
PRESERVED_ACRONYMS = {'ZIP', 'ID', 'URL', 'DOB', 'SSN', 'EIN', 'PO', 'APT', 'LLC', 'INC'}

# Contact field patterns for ordering (processed first in emails)
CONTACT_FIELD_PATTERNS = {
    'name': ['name', 'first_name', 'last_name', 'full_name', 'firstname', 'lastname'],
    'email': ['email', 'e_mail', 'email_address'],
    'phone': ['phone', 'mobile', 'cell', 'telephone', 'tel'],
    'address': ['street', 'address', 'city', 'state', 'zip', 'postal', 'country', 'apt', 'unit', 'suite']
}


def humanize_key(key: str) -> str:
    """
    Humanize a snake_case or kebab-case key into Title Case.
    Preserves common acronyms like ZIP, ID, URL, DOB.

    Args:
        key: The field key (e.g., "zip_code", "user_id")

    Returns:
        Human-readable label (e.g., "ZIP Code", "User ID")
    """
    import re
    if not key:
        return ''

    # Split on underscores, hyphens, or camelCase boundaries
    # First handle camelCase
    key_spaced = re.sub(r'([a-z])([A-Z])', r'\1 \2', key)
    # Then split on underscores/hyphens
    words = re.split(r'[_\-]+', key_spaced)

    result_words = []
    for word in words:
        word = word.strip()
        if not word:
            continue
        upper_word = word.upper()
        # Check if it's a preserved acronym
        if upper_word in PRESERVED_ACRONYMS:
            result_words.append(upper_word)
        else:
            # Title case: first letter upper, rest lower
            result_words.append(word.capitalize())

    return ' '.join(result_words)


def format_value(value: Any, max_length: int = 2000) -> Optional[str]:
    """
    Format a value for display in plain text email.
    Handles booleans, arrays, objects, and long strings.

    Args:
        value: The value to format
        max_length: Maximum length before truncation (default 2000)

    Returns:
        Formatted string, or None if value should be omitted
    """
    # Omit None or empty strings
    if value is None or value == '':
        return None

    # Boolean: Yes/No
    if isinstance(value, bool):
        return 'Yes' if value else 'No'

    # List: join with comma
    if isinstance(value, list):
        filtered = [str(v) for v in value if v is not None and v != '']
        if not filtered:
            return None
        joined = ', '.join(filtered)
        if len(joined) > max_length:
            return joined[:max_length] + '...'
        return joined

    # Dict: stringify as single line
    if isinstance(value, dict):
        try:
            str_val = json.dumps(value)
            if len(str_val) > max_length:
                return str_val[:max_length] + '...'
            return str_val
        except (TypeError, ValueError):
            return '[Object]'

    # String or number: convert to string and truncate if needed
    str_val = str(value)
    if len(str_val) > max_length:
        return str_val[:max_length] + '...'
    return str_val


def get_field_priority(key: str) -> int:
    """
    Get the priority score for field ordering.
    Lower score = appears earlier in email.

    Args:
        key: Field key

    Returns:
        Priority score (0-999)
    """
    lower_key = key.lower()

    # Name fields: highest priority (0-9)
    for pattern in CONTACT_FIELD_PATTERNS['name']:
        if lower_key == pattern or pattern in lower_key:
            if lower_key == 'first_name' or lower_key == 'firstname':
                return 0
            if lower_key == 'last_name' or lower_key == 'lastname':
                return 1
            if lower_key == 'name' or lower_key == 'full_name':
                return 2
            return 9

    # Email fields: second priority (10-19)
    for pattern in CONTACT_FIELD_PATTERNS['email']:
        if lower_key == pattern or pattern in lower_key:
            return 10

    # Phone fields: third priority (20-29)
    for pattern in CONTACT_FIELD_PATTERNS['phone']:
        if lower_key == pattern or pattern in lower_key:
            return 20

    # Address fields: fourth priority (30-49)
    for pattern in CONTACT_FIELD_PATTERNS['address']:
        if lower_key == pattern or pattern in lower_key:
            if 'street' in lower_key or 'address' in lower_key:
                return 30
            if 'apt' in lower_key or 'unit' in lower_key or 'suite' in lower_key:
                return 31
            if 'city' in lower_key:
                return 32
            if 'state' in lower_key:
                return 33
            if 'zip' in lower_key or 'postal' in lower_key:
                return 34
            if 'country' in lower_key:
                return 35
            return 39

    # All other fields: alphabetical (100+)
    return 100


def build_email_details_text(form_data_string: str) -> str:
    """
    Build human-readable email details text from form data.

    Args:
        form_data_string: JSON string of form data

    Returns:
        Formatted plain text with one "Label: Value" per line
    """
    # Parse JSON, handle failure gracefully
    try:
        form_data = json.loads(form_data_string)
    except (json.JSONDecodeError, TypeError):
        return f"Unable to parse form data. Raw:\n{form_data_string}"

    if not form_data or not isinstance(form_data, dict):
        return f"Unable to parse form data. Raw:\n{form_data_string}"

    # Build list of (key, label, value, priority)
    fields = []
    for key, value in form_data.items():
        formatted_value = format_value(value)
        if formatted_value is not None:
            fields.append({
                'key': key,
                'label': humanize_key(key),
                'value': formatted_value,
                'priority': get_field_priority(key)
            })

    # Sort: by priority first, then alphabetically by key
    fields.sort(key=lambda f: (f['priority'], f['key']))

    # Build output lines
    lines = [f"{f['label']}: {f['value']}" for f in fields]
    return '\n'.join(lines)


def extract_contact(form_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Extract contact information from form data (best-effort).

    Args:
        form_data: Parsed form data object

    Returns:
        Contact object { name?, email?, phone? }
    """
    if not form_data or not isinstance(form_data, dict):
        return {}

    contact = {}
    lower_entries = [(k.lower(), v, k) for k, v in form_data.items()]

    # Extract name
    first_name = None
    last_name = None
    for lower_key, value, original_key in lower_entries:
        if (lower_key == 'first_name' or lower_key == 'firstname') and value:
            first_name = str(value).strip()
        if (lower_key == 'last_name' or lower_key == 'lastname') and value:
            last_name = str(value).strip()
        if (lower_key == 'name' or lower_key == 'full_name') and value and not first_name:
            contact['name'] = str(value).strip()

    if first_name or last_name:
        contact['name'] = ' '.join(filter(None, [first_name, last_name]))

    # Extract email (first field containing 'email')
    for lower_key, value, original_key in lower_entries:
        if 'email' in lower_key and value and isinstance(value, str) and '@' in value:
            contact['email'] = value.strip()
            break

    # Extract phone (first field containing 'phone', 'mobile', 'cell')
    for lower_key, value, original_key in lower_entries:
        if ('phone' in lower_key or 'mobile' in lower_key or 'cell' in lower_key) and value:
            contact['phone'] = str(value).strip()
            break

    return contact


def get_email_subject_suffix(form_data: Dict[str, Any]) -> str:
    """
    Generate email subject suffix from form data.
    Returns person's name if available, otherwise "New submission".

    Args:
        form_data: Parsed form data object

    Returns:
        Subject suffix (e.g., "Jane Smith" or "New submission")
    """
    contact = extract_contact(form_data)
    if contact.get('name'):
        return contact['name']
    return 'New submission'


class FormHandler:
    """Handles conversational form submissions and notifications"""

    def __init__(self, tenant_config: Dict[str, Any]):
        self.tenant_config = tenant_config
        self.tenant_id = tenant_config.get('tenant_id')
        self.tenant_hash = tenant_config.get('tenant_hash')

    def handle_form_submission(self, form_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Main entry point for processing form submissions

        Args:
            form_data: {
                'form_type': str,
                'responses': dict,
                'session_id': str,
                'conversation_id': str,
                'metadata': dict
            }

        Returns:
            Response with submission_id and next_steps
        """
        try:
            form_type = form_data.get('form_type')
            responses = form_data.get('responses', {})
            session_id = form_data.get('session_id')
            conversation_id = form_data.get('conversation_id')

            logger.info(f"Processing form submission: {form_type} for tenant: {self.tenant_id}")

            # Get form configuration
            forms_config = self.tenant_config.get('conversational_forms', {})
            form_config = forms_config.get(form_type, {})

            if not form_config:
                raise ValueError(f"Form type '{form_type}' not configured for tenant")

            # Store submission
            submission_id = self._store_submission(
                form_type=form_type,
                responses=responses,
                session_id=session_id,
                conversation_id=conversation_id
            )

            # Send to Bubble webhook if configured
            self._send_bubble_webhook(
                form_type=form_type,
                responses=responses,
                submission_id=submission_id,
                session_id=session_id,
                conversation_id=conversation_id
            )

            # Determine priority
            priority = self._determine_priority(form_type, responses, form_config)

            # Send notifications
            notification_results = self._send_notifications(
                form_config=form_config,
                form_data={
                    'form_type': form_type,
                    'responses': responses,
                    'submission_id': submission_id,
                    'priority': priority
                },
                priority=priority
            )

            # Handle fulfillment
            fulfillment_result = self._process_fulfillment(
                form_config=form_config,
                form_type=form_type,
                responses=responses,
                submission_id=submission_id
            )

            # Audit log
            self._audit_submission(
                submission_id=submission_id,
                form_type=form_type,
                notification_results=notification_results,
                fulfillment_result=fulfillment_result
            )

            return {
                'success': True,
                'submission_id': submission_id,
                'notifications_sent': notification_results,
                'fulfillment': fulfillment_result,
                'next_steps': self._get_next_steps(form_type, form_config)
            }

        except Exception as e:
            logger.error(f"Form submission error: {str(e)}")
            return {
                'success': False,
                'error': str(e)
            }

    def _store_submission(self, form_type: str, responses: Dict[str, Any],
                         session_id: str, conversation_id: str) -> str:
        """Store form submission in DynamoDB"""
        submission_id = str(uuid.uuid4())
        timestamp = datetime.now(timezone.utc).isoformat()

        try:
            table = dynamodb.Table(SUBMISSIONS_TABLE)
            table.put_item(
                Item={
                    'submission_id': submission_id,
                    'tenant_id': self.tenant_id,
                    'tenant_hash': self.tenant_hash,
                    'form_type': form_type,
                    'responses': responses,
                    'session_id': session_id,
                    'conversation_id': conversation_id,
                    'timestamp': timestamp,
                    'status': 'submitted'
                }
            )
            logger.info(f"Stored submission: {submission_id}")
            return submission_id

        except ClientError as e:
            logger.error(f"Error storing submission: {str(e)}")
            raise

    def _determine_priority(self, form_type: str, responses: Dict[str, Any],
                          form_config: Dict[str, Any]) -> str:
        """Determine notification priority based on form data"""

        # Check for explicit priority fields
        if 'urgency' in responses:
            urgency = responses['urgency'].lower()
            if urgency in ['immediate', 'urgent', 'high']:
                return 'high'
            elif urgency in ['normal', 'this week']:
                return 'normal'
            else:
                return 'low'

        # Check priority rules in config
        priority_rules = form_config.get('priority_rules', [])
        for rule in priority_rules:
            field = rule.get('field')
            value = rule.get('value')
            priority = rule.get('priority')

            if field in responses and responses[field] == value:
                return priority

        # Form-type based defaults
        priority_defaults = {
            'request_support': 'high',
            'volunteer_signup': 'normal',
            'donation': 'normal',
            'contact': 'normal',
            'newsletter': 'low'
        }

        return priority_defaults.get(form_type, 'normal')

    def _send_notifications(self, form_config: Dict[str, Any],
                          form_data: Dict[str, Any], priority: str) -> List[str]:
        """Send multi-channel notifications"""
        notifications_sent = []
        notification_config = form_config.get('notifications', {})

        # Email notifications
        if notification_config.get('email', {}).get('enabled'):
            email_results = self._send_email_notifications(
                notification_config['email'],
                form_data,
                priority
            )
            notifications_sent.extend(email_results)

        # SMS notifications (high priority only)
        if priority == 'high' and notification_config.get('sms', {}).get('enabled'):
            sms_results = self._send_sms_notifications(
                notification_config['sms'],
                form_data
            )
            notifications_sent.extend(sms_results)

        # Webhook notifications
        if notification_config.get('webhook', {}).get('enabled'):
            webhook_results = self._send_webhook_notifications(
                notification_config['webhook'],
                form_data
            )
            notifications_sent.extend(webhook_results)

        logger.info(f"Notifications sent: {notifications_sent}")
        return notifications_sent

    def _send_email_notifications(self, email_config: Dict[str, Any],
                                 form_data: Dict[str, Any], priority: str) -> List[str]:
        """Send email notifications via SES"""
        sent = []
        recipients = email_config.get('recipients', [])

        # Build email content
        subject_template = email_config.get('subject', 'New Form Submission: {form_type}')
        subject = self._format_template(subject_template, form_data)

        # Select template based on priority
        template_name = email_config.get(f'{priority}_template') or email_config.get('template')

        for recipient in recipients:
            try:
                # Build email body
                body = self._build_email_body(form_data, template_name)

                # Send via SES
                response = ses.send_email(
                    Source=email_config.get('sender', 'noreply@picasso.ai'),
                    Destination={'ToAddresses': [recipient]},
                    Message={
                        'Subject': {'Data': subject},
                        'Body': {
                            'Html': {'Data': body}
                        }
                    }
                )

                sent.append(f"email:{recipient}")
                logger.info(f"Email sent to {recipient}: {response['MessageId']}")

            except ClientError as e:
                logger.error(f"Email send error to {recipient}: {str(e)}")

        return sent

    def _send_sms_notifications(self, sms_config: Dict[str, Any],
                               form_data: Dict[str, Any]) -> List[str]:
        """Send SMS notifications with rate limiting"""
        sent = []

        # Check monthly usage limit
        monthly_limit = sms_config.get('monthly_limit', 100)
        current_usage = self._get_monthly_sms_usage()

        if current_usage >= monthly_limit:
            logger.warning(f"SMS monthly limit reached: {current_usage}/{monthly_limit}")
            return sent

        recipients = sms_config.get('recipients', [])
        message_template = sms_config.get('template', 'New {form_type} submission')
        message = self._format_template(message_template, form_data)[:160]

        for phone in recipients:
            if current_usage >= monthly_limit:
                break

            try:
                response = sns.publish(
                    PhoneNumber=phone,
                    Message=message
                )

                sent.append(f"sms:{phone}")
                self._increment_sms_usage()
                current_usage += 1
                logger.info(f"SMS sent to {phone}: {response['MessageId']}")

            except ClientError as e:
                logger.error(f"SMS send error to {phone}: {str(e)}")

        return sent

    def _send_webhook_notifications(self, webhook_config: Dict[str, Any],
                                   form_data: Dict[str, Any]) -> List[str]:
        """Send webhook notifications for integrations"""
        import requests
        sent = []

        url = webhook_config.get('url')
        headers = webhook_config.get('headers', {})

        # Add content type if not specified
        if 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'

        try:
            response = requests.post(
                url=url,
                headers=headers,
                json=form_data,
                timeout=10
            )

            if response.status_code < 300:
                sent.append(f"webhook:{response.status_code}")
                logger.info(f"Webhook sent to {url}: {response.status_code}")
            else:
                logger.error(f"Webhook error: {response.status_code} - {response.text}")

        except Exception as e:
            logger.error(f"Webhook send error: {str(e)}")

        return sent

    def _send_bubble_webhook(self, form_type: str, responses: Dict[str, Any],
                            submission_id: str, session_id: str, conversation_id: str):
        """Send form submission to Bubble via Workflow API

        Uses a standardized schema for multi-tenant SaaS scalability:
        - Fixed metadata fields at top level (14 fields)
        - form_data as JSON string for dynamic form fields
        - email_details_text for human-readable email templates
        - email_subject_suffix for personalized email subjects
        - contact object for extracted contact info

        Bubble initializes once with these fields, then parses form_data JSON.
        """
        import urllib.request
        import urllib.error
        import os

        # Check if forms are enabled for this tenant
        features = self.tenant_config.get('features', {})
        if not features.get('conversational_forms'):
            logger.debug("Conversational forms not enabled for tenant, skipping webhook")
            return

        # Get webhook URL and API key (tenant config overrides env vars, with default fallback)
        bubble_config = self.tenant_config.get('bubble_integration', {})
        default_webhook_url = 'https://hrfx.bubbleapps.io/api/1.1/wf/form_submission'
        webhook_url = bubble_config.get('webhook_url') or os.environ.get('BUBBLE_WEBHOOK_URL') or default_webhook_url
        api_key = bubble_config.get('api_key') or os.environ.get('BUBBLE_API_KEY')

        if not webhook_url:
            logger.debug("Bubble webhook URL not configured, skipping")
            return

        # Get form-specific configuration
        forms_config = self.tenant_config.get('conversational_forms', {})
        form_config = forms_config.get(form_type, {})

        # Transform form data to human-readable labels
        transformed_form_data = transform_form_data_to_labels(responses, form_config)
        form_data_json_string = json.dumps(transformed_form_data)

        # Build payload with standardized schema for multi-tenant scalability
        # Bubble initializes once with these 14 fields, then parses form_data JSON
        payload = {
            # Submission metadata
            'submission_id': submission_id,
            'timestamp': datetime.now(timezone.utc).isoformat(),

            # Tenant metadata (from config root)
            'tenant_id': self.tenant_id,
            'tenant_hash': self.tenant_hash or '',
            'organization_name': self.tenant_config.get('chat_title') or self.tenant_config.get('organization_name') or '',

            # Form metadata (from form definition)
            'form_id': form_type,
            'form_title': form_config.get('title') or form_type,
            'program_id': form_config.get('program') or '',

            # Session tracking
            'session_id': session_id,
            'conversation_id': conversation_id,

            # All form responses as JSON string with human-readable labels
            'form_data': form_data_json_string,

            # NEW: Human-readable fields for Bubble email templates
            'email_details_text': build_email_details_text(form_data_json_string),
            'email_subject_suffix': get_email_subject_suffix(transformed_form_data),
            'contact': extract_contact(transformed_form_data)
        }

        headers = {
            'Content-Type': 'application/json',
        }

        # Add API key if configured
        if api_key:
            headers['Authorization'] = f'Bearer {api_key}'

        try:
            # Convert payload to JSON bytes
            json_data = json.dumps(payload).encode('utf-8')

            # Create request
            req = urllib.request.Request(
                webhook_url,
                data=json_data,
                headers=headers,
                method='POST'
            )

            # Send request
            with urllib.request.urlopen(req, timeout=10) as response:
                status_code = response.getcode()
                if status_code in [200, 201]:
                    logger.info(f"Sent form submission to Bubble: {submission_id}")
                else:
                    response_text = response.read().decode('utf-8')
                    logger.error(f"Bubble webhook error: {status_code} - {response_text}")

        except urllib.error.HTTPError as e:
            logger.error(f"Bubble webhook HTTP error: {e.code} - {e.read().decode('utf-8')}")
            # Don't fail the form submission if Bubble webhook fails
        except Exception as e:
            logger.error(f"Error sending to Bubble: {str(e)}")
            # Don't fail the form submission if Bubble webhook fails

    def _process_fulfillment(self, form_config: Dict[str, Any], form_type: str,
                           responses: Dict[str, Any], submission_id: str) -> Dict[str, Any]:
        """Process form fulfillment actions"""
        fulfillment = form_config.get('fulfillment', {})
        fulfillment_type = fulfillment.get('type')

        if not fulfillment_type:
            return {'status': 'no_fulfillment_configured'}

        if fulfillment_type == 'lambda':
            # Invoke another Lambda function
            function_name = fulfillment.get('function')
            action = fulfillment.get('action', 'process_form')

            try:
                response = lambda_client.invoke(
                    FunctionName=function_name,
                    InvocationType='Event',  # Async
                    Payload=json.dumps({
                        'action': action,
                        'form_type': form_type,
                        'submission_id': submission_id,
                        'responses': responses,
                        'tenant_id': self.tenant_id
                    })
                )

                return {
                    'type': 'lambda',
                    'function': function_name,
                    'status': 'invoked',
                    'status_code': response['StatusCode']
                }

            except ClientError as e:
                logger.error(f"Lambda invocation error: {str(e)}")
                return {'type': 'lambda', 'status': 'error', 'error': str(e)}

        elif fulfillment_type == 'email':
            # Send fulfillment email to user
            user_email = responses.get('email')
            if user_email:
                template = fulfillment.get('template', 'thank_you')
                self._send_fulfillment_email(user_email, template, responses)
                return {'type': 'email', 'status': 'sent', 'recipient': user_email}

        elif fulfillment_type == 's3':
            # Store in S3
            bucket = fulfillment.get('bucket')
            key = f"submissions/{self.tenant_id}/{form_type}/{submission_id}.json"

            try:
                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=json.dumps(responses),
                    ContentType='application/json'
                )
                return {'type': 's3', 'status': 'stored', 'location': f"s3://{bucket}/{key}"}
            except ClientError as e:
                logger.error(f"S3 storage error: {str(e)}")
                return {'type': 's3', 'status': 'error', 'error': str(e)}

        return {'type': fulfillment_type, 'status': 'unsupported'}

    def _get_monthly_sms_usage(self) -> int:
        """Get current month's SMS usage count"""
        try:
            table = dynamodb.Table(SMS_USAGE_TABLE)
            current_month = datetime.now().strftime('%Y-%m')

            response = table.get_item(
                Key={
                    'tenant_id': self.tenant_id,
                    'month': current_month
                }
            )

            if 'Item' in response:
                return response['Item'].get('count', 0)
            return 0

        except ClientError:
            return 0

    def _increment_sms_usage(self):
        """Increment SMS usage counter"""
        try:
            table = dynamodb.Table(SMS_USAGE_TABLE)
            current_month = datetime.now().strftime('%Y-%m')

            table.update_item(
                Key={
                    'tenant_id': self.tenant_id,
                    'month': current_month
                },
                UpdateExpression='ADD #count :inc',
                ExpressionAttributeNames={'#count': 'count'},
                ExpressionAttributeValues={':inc': 1}
            )
        except ClientError as e:
            logger.error(f"Error incrementing SMS usage: {str(e)}")

    def _audit_submission(self, submission_id: str, form_type: str,
                        notification_results: List[str], fulfillment_result: Dict[str, Any]):
        """Log submission to audit table"""
        try:
            table = dynamodb.Table(AUDIT_TABLE)
            table.put_item(
                Item={
                    'tenant_id': self.tenant_id,
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                    'event_type': 'form_submission',
                    'submission_id': submission_id,
                    'form_type': form_type,
                    'notifications': notification_results,
                    'fulfillment': fulfillment_result
                }
            )
        except ClientError as e:
            logger.error(f"Audit log error: {str(e)}")

    def _format_template(self, template: str, data: Dict[str, Any]) -> str:
        """Format template string with data values"""
        try:
            # Flatten nested data for easier templating
            flat_data = self._flatten_dict(data)
            return template.format(**flat_data)
        except Exception:
            return template

    def _flatten_dict(self, d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """Flatten nested dictionary for template formatting"""
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def _build_email_body(self, form_data: Dict[str, Any], template_name: Optional[str]) -> str:
        """Build HTML email body"""
        responses = form_data.get('responses', {})
        form_type = form_data.get('form_type', 'Unknown')
        submission_id = form_data.get('submission_id', '')

        # Build HTML table of responses
        rows = []
        for field, value in responses.items():
            field_label = field.replace('_', ' ').title()
            rows.append(f"""
                <tr>
                    <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">{field_label}</td>
                    <td style="padding: 8px; border: 1px solid #ddd;">{value}</td>
                </tr>
            """)

        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: Arial, sans-serif; }}
                h2 {{ color: #333; }}
                table {{ border-collapse: collapse; width: 100%; max-width: 600px; }}
                th, td {{ text-align: left; padding: 8px; }}
                .metadata {{ color: #666; font-size: 12px; margin-top: 20px; }}
            </style>
        </head>
        <body>
            <h2>New Form Submission: {form_type.replace('_', ' ').title()}</h2>
            <table>
                {''.join(rows)}
            </table>
            <div class="metadata">
                <p>Submission ID: {submission_id}</p>
                <p>Tenant: {self.tenant_id}</p>
                <p>Timestamp: {datetime.now().isoformat()}</p>
            </div>
        </body>
        </html>
        """

        return html

    def _send_fulfillment_email(self, recipient: str, template: str, responses: Dict[str, Any]):
        """Send fulfillment email to form submitter"""
        try:
            # Get template content from config or use default
            templates = self.tenant_config.get('email_templates', {})
            template_content = templates.get(template, self._get_default_template(template))

            # Format with responses
            subject = template_content.get('subject', 'Thank you for your submission')
            body = template_content.get('body', 'We have received your form submission.')

            # Replace placeholders
            for key, value in responses.items():
                placeholder = f"{{{key}}}"
                subject = subject.replace(placeholder, str(value))
                body = body.replace(placeholder, str(value))

            # Send email
            ses.send_email(
                Source=self.tenant_config.get('from_email', 'noreply@picasso.ai'),
                Destination={'ToAddresses': [recipient]},
                Message={
                    'Subject': {'Data': subject},
                    'Body': {
                        'Html': {'Data': body}
                    }
                }
            )

            logger.info(f"Fulfillment email sent to {recipient}")

        except ClientError as e:
            logger.error(f"Fulfillment email error: {str(e)}")

    def _get_default_template(self, template_type: str) -> Dict[str, str]:
        """Get default email template"""
        templates = {
            'thank_you': {
                'subject': 'Thank you for your submission',
                'body': """
                    <h2>Thank you!</h2>
                    <p>We have received your submission and will be in touch soon.</p>
                    <p>Best regards,<br>The Team</p>
                """
            },
            'volunteer_welcome': {
                'subject': 'Welcome to our volunteer program!',
                'body': """
                    <h2>Welcome {first_name}!</h2>
                    <p>Thank you for your interest in volunteering with us.</p>
                    <p>We will review your application and contact you at {email} within 48 hours.</p>
                    <p>Best regards,<br>The Volunteer Team</p>
                """
            },
            'donation_receipt': {
                'subject': 'Thank you for your donation',
                'body': """
                    <h2>Thank you for your generous donation!</h2>
                    <p>Your donation of {amount} has been received.</p>
                    <p>A tax receipt will be sent to {email}.</p>
                    <p>Thank you for your support!</p>
                """
            }
        }

        return templates.get(template_type, templates['thank_you'])

    def _get_next_steps(self, form_type: str, form_config: Dict[str, Any]) -> str:
        """Get next steps message for user"""
        next_steps = form_config.get('next_steps')

        if next_steps:
            return next_steps

        # Default next steps by form type
        defaults = {
            'volunteer_signup': 'We will contact you within 48 hours to discuss next steps.',
            'donation': 'Thank you for your donation. A receipt will be sent to your email.',
            'request_support': 'We have received your request and will respond within 24 hours.',
            'contact': 'Thank you for contacting us. We will respond within 1-2 business days.',
            'newsletter': 'You have been subscribed to our newsletter.'
        }

        return defaults.get(form_type, 'Thank you for your submission. We will be in touch soon.')