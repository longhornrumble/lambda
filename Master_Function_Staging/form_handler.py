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