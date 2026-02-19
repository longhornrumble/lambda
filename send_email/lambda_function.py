"""
SES Email Sending Lambda Function

This Lambda receives email send requests from Bubble via API Gateway
and sends emails using AWS SES with support for HTML, text, and attachments.
"""

import json
import boto3
import logging
import os
import re
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


def fix_bubble_json(raw_body):
    """
    Fix common JSON issues from Bubble's :formatted as JSON-safe.

    Bubble's JSON-safe adds quotes around values, but when inserted into
    a template that already has quotes, we get double-quoted strings like:
    "html_body": ""<html>content</html>""

    This function fixes those patterns.
    """
    if not raw_body:
        return raw_body

    # Log the problematic area for debugging
    logger.info(f"Raw body length: {len(raw_body)}")

    fixed = raw_body

    # Handle escaped newlines that might not be properly escaped
    # Bubble might send actual newlines instead of \n
    fixed = fixed.replace('\r\n', '\\n').replace('\r', '\\n')

    # Fix double-quoted strings from Bubble
    # Pattern: "key": ""content"" -> "key": "content"
    # The content can contain quotes, so we can't use a simple regex
    # Instead, we look for the specific pattern: ": "" followed later by "",

    # Step 1: Fix opening double-quotes after colon
    # Pattern: ": "" -> ": "
    fixed = re.sub(r':\s*""(?!")', ': "', fixed)

    # Step 2: Fix closing double-quotes before comma, closing brace, or end
    # Pattern: "", -> ",  and ""} -> "}  and ""\n -> "\n
    fixed = re.sub(r'""(\s*[,}\]])', r'"\1', fixed)

    # Step 3: Handle case where the value ends with "" at the very end of JSON
    # Pattern: ""\s*$ -> "
    fixed = re.sub(r'""\s*$', '"', fixed)

    return fixed


def lambda_handler(event, context):
    """
    Handle email send requests from Bubble.
    """
    # Handle CORS preflight
    if event.get('httpMethod') == 'OPTIONS' or event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return cors_response(200, {'message': 'OK'})

    try:
        # Debug: Log raw body - log more for debugging
        raw_body = event.get('body', '{}')
        logger.info(f"Raw body received (first 1000 chars): {raw_body[:1000] if raw_body else 'None'}")
        
        # Log around the error position
        if len(raw_body) > 500:
            logger.info(f"Raw body chars 500-800: {repr(raw_body[500:800])}")

        # Try to parse request body
        body = None
        parse_error = None

        # First attempt: parse as-is
        try:
            body = json.loads(raw_body)
        except json.JSONDecodeError as e:
            parse_error = e
            logger.warning(f"Initial JSON parse failed at position {e.pos}: {str(e)}")
            logger.info(f"Context around error: {repr(raw_body[max(0,e.pos-50):e.pos+50])}")

            # Second attempt: fix Bubble's double-quoted strings
            try:
                fixed_body = fix_bubble_json(raw_body)
                if fixed_body != raw_body:
                    logger.info("Applied Bubble JSON fix")
                    logger.info(f"Fixed body chars 500-800: {repr(fixed_body[500:800])}")
                body = json.loads(fixed_body)
                parse_error = None
            except json.JSONDecodeError as e2:
                parse_error = e2
                logger.error(f"JSON parse still failed at position {e2.pos}: {str(e2)}")
                logger.info(f"Context around error: {repr(fixed_body[max(0,e2.pos-50):e2.pos+50])}")

        if parse_error:
            raise parse_error

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
    """Build MIME message and send via SES SendRawEmail."""
    msg = MIMEMultipart('mixed')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = ', '.join(to)

    if cc:
        msg['Cc'] = ', '.join(cc)
    if reply_to:
        msg['Reply-To'] = ', '.join(reply_to)

    body_part = MIMEMultipart('alternative')

    if text_body:
        text_mime = MIMEText(text_body, 'plain', 'utf-8')
        body_part.attach(text_mime)

    if html_body:
        html_mime = MIMEText(html_body, 'html', 'utf-8')
        body_part.attach(html_mime)

    msg.attach(body_part)

    for att in attachments:
        try:
            filename = att.get('filename', 'attachment')
            content_base64 = att.get('content_base64', '')
            content_type = att.get('content_type', 'application/octet-stream')
            content = base64.b64decode(content_base64)
            attachment_part = MIMEApplication(content)
            attachment_part.add_header('Content-Disposition', 'attachment', filename=filename)
            attachment_part.add_header('Content-Type', content_type)
            msg.attach(attachment_part)
            logger.info(f"Added attachment: {filename} ({len(content)} bytes)")
        except Exception as e:
            logger.error(f"Error processing attachment {att.get('filename', 'unknown')}: {str(e)}")
            raise ValueError(f"Invalid attachment: {str(e)}")

    destinations = list(to)
    if cc:
        destinations.extend(cc)
    if bcc:
        destinations.extend(bcc)

    message_tags = [{'Name': k, 'Value': str(v)[:256]} for k, v in tags.items()]

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
