"""
SES Email Sending Lambda Function

This Lambda receives email send requests from Bubble via API Gateway
and sends emails using AWS SES with support for HTML, text, and attachments.
"""

import json
import boto3
import logging
import os
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import base64
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize SES client
ses = boto3.client('ses')

# Configuration
DEFAULT_SENDER = os.environ.get('DEFAULT_SENDER', 'notify@myrecruiter.ai')
CONFIGURATION_SET = os.environ.get('CONFIGURATION_SET', 'picasso-emails')


def lambda_handler(event, context):
    """
    Handle email send requests from Bubble.

    Expected body format:
    {
        "to": ["recipient@example.com"],
        "subject": "Your subject line",
        "html_body": "<html>...</html>",
        "text_body": "Plain text version (optional)",
        "from": "sender@myrecruiter.ai (optional)",
        "cc": ["cc@example.com"] (optional),
        "bcc": ["bcc@example.com"] (optional),
        "reply_to": ["reply@example.com"] (optional),
        "attachments": [
            {
                "filename": "report.pdf",
                "content_base64": "JVBERi0xLjQK...",
                "content_type": "application/pdf"
            }
        ] (optional),
        "tags": {
            "tenant_id": "FOS402334",
            "email_type": "form_notification"
        } (optional)
    }
    """
    # Handle CORS preflight
    if event.get('httpMethod') == 'OPTIONS' or event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return cors_response(200, {'message': 'OK'})

    try:
        # Debug: Log raw body to see what Bubble is sending
        raw_body = event.get('body', '{}')
        logger.info(f"Raw body received: {raw_body[:500] if raw_body else 'None'}")

        # Parse request body
        body = json.loads(raw_body)

        # Validate required fields
        to_addresses = body.get('to', [])
        if not to_addresses:
            return cors_response(400, {
                'success': False,
                'error': 'Missing required field: to'
            })

        subject = body.get('subject', '')
        if not subject:
            return cors_response(400, {
                'success': False,
                'error': 'Missing required field: subject'
            })

        html_body = body.get('html_body', '')
        text_body = body.get('text_body', '')

        if not html_body and not text_body:
            return cors_response(400, {
                'success': False,
                'error': 'At least one of html_body or text_body is required'
            })

        # Optional fields with defaults
        sender = body.get('from', DEFAULT_SENDER)
        cc = body.get('cc', [])
        bcc = body.get('bcc', [])
        reply_to = body.get('reply_to', [])
        attachments = body.get('attachments', [])
        tags = body.get('tags', {})

        # Build and send email
        message_id = send_email(
            sender=sender,
            to=to_addresses,
            cc=cc,
            bcc=bcc,
            reply_to=reply_to,
            subject=subject,
            html_body=html_body,
            text_body=text_body,
            attachments=attachments,
            tags=tags
        )

        logger.info(f"Email sent successfully: {message_id}")

        return cors_response(200, {
            'success': True,
            'message_id': message_id
        })

    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in request body: {str(e)}")
        return cors_response(400, {
            'success': False,
            'error': 'Invalid JSON in request body'
        })
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"SES error ({error_code}): {error_message}")
        return cors_response(500, {
            'success': False,
            'error': f"Email service error: {error_message}",
            'error_code': error_code
        })
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        return cors_response(500, {
            'success': False,
            'error': str(e)
        })


def send_email(sender, to, cc, bcc, reply_to, subject, html_body, text_body, attachments, tags):
    """
    Build MIME message and send via SES SendRawEmail.

    Returns the SES MessageId.
    """
    # Create the root MIME message
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(to)

    if cc:
        msg['Cc'] = ', '.join(cc)
    if reply_to:
        msg['Reply-To'] = ', '.join(reply_to)

    # Create body part (multipart/alternative for HTML + text)
    body_part = MIMEMultipart('alternative')

    # Add text body first (email clients show the last part by default)
    if text_body:
        text_mime = MIMEText(text_body, 'plain', 'utf-8')
        body_part.attach(text_mime)

    # Add HTML body
    if html_body:
        html_mime = MIMEText(html_body, 'html', 'utf-8')
        body_part.attach(html_mime)

    msg.attach(body_part)

    # Add attachments
    for att in attachments:
        try:
            filename = att.get('filename', 'attachment')
            content_base64 = att.get('content_base64', '')
            content_type = att.get('content_type', 'application/octet-stream')

            # Decode base64 content
            content = base64.b64decode(content_base64)

            # Create attachment part
            attachment_part = MIMEApplication(content)
            attachment_part.add_header(
                'Content-Disposition',
                'attachment',
                filename=filename
            )
            attachment_part.add_header('Content-Type', content_type)

            msg.attach(attachment_part)
            logger.info(f"Added attachment: {filename} ({len(content)} bytes)")

        except Exception as e:
            logger.error(f"Error processing attachment {att.get('filename', 'unknown')}: {str(e)}")
            raise ValueError(f"Invalid attachment: {str(e)}")

    # Build destination list
    destinations = list(to)
    if cc:
        destinations.extend(cc)
    if bcc:
        destinations.extend(bcc)

    # Build message tags for SES tracking
    message_tags = [{'Name': k, 'Value': str(v)[:256]} for k, v in tags.items()]

    # Send via SES
    response = ses.send_raw_email(
        Source=sender,
        Destinations=destinations,
        RawMessage={'Data': msg.as_string()},
        ConfigurationSetName=CONFIGURATION_SET,
        Tags=message_tags
    )

    message_id = response['MessageId']
    logger.info(f"Email sent: {message_id} to {destinations} with {len(attachments)} attachment(s)")

    return message_id


def cors_response(status_code, body):
    """Return response with CORS headers."""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type, x-api-key'
        },
        'body': json.dumps(body)
    }
