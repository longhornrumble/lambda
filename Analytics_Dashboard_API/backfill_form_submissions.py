#!/usr/bin/env python3
"""
Backfill script to add contact, comments, and form_data_display fields
to existing form submissions in DynamoDB.

Usage:
    AWS_PROFILE=chris-admin python3 backfill_form_submissions.py [--dry-run]

Options:
    --dry-run    Preview changes without writing to DynamoDB
"""

import boto3
import json
import sys
from typing import Dict, Any, Optional, Tuple

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb', region_name='us-east-1')
TABLE_NAME = 'picasso_form_submissions'

# Stats tracking
stats = {
    'scanned': 0,
    'updated': 0,
    'skipped': 0,
    'errors': 0
}


def extract_contact_from_form_data_labeled(form_data_labeled: Dict) -> Tuple[Dict, str]:
    """
    Extract canonical contact and comments from form_data_labeled structure.
    Returns (contact_dict, comments_string)
    """
    contact = {
        'first_name': None,
        'middle_name': None,
        'last_name': None,
        'full_name': None,
        'email': None,
        'phone': None,
        'address': {
            'street': None,
            'unit': None,
            'city': None,
            'state': None,
            'zip': None,
            'formatted': None
        }
    }
    comments = None

    data_map = form_data_labeled.get('M', {})

    for field_label, field_wrapper in data_map.items():
        if not isinstance(field_wrapper, dict) or 'M' not in field_wrapper:
            continue

        field_obj = field_wrapper['M']
        field_type = field_obj.get('type', {}).get('S', 'text')
        value_obj = field_obj.get('value', {})

        label_lower = field_label.lower()

        # Email
        if 'email' in label_lower or field_type == 'email':
            if 'S' in value_obj:
                contact['email'] = value_obj['S']

        # Phone
        elif 'phone' in label_lower or 'mobile' in label_lower or 'cell' in label_lower or field_type == 'phone':
            if 'S' in value_obj:
                contact['phone'] = value_obj['S']

        # Comments/About
        elif any(kw in label_lower for kw in ['comment', 'message', 'note', 'question', 'additional', 'tell us', 'about']):
            if 'S' in value_obj:
                comments = value_obj['S']

        # Name (composite or simple)
        elif 'name' in label_lower:
            if 'S' in value_obj:
                # Simple string name
                contact['full_name'] = value_obj['S']
            elif 'M' in value_obj:
                # Composite name
                nested = value_obj['M']
                for sub_key, sub_val in nested.items():
                    if isinstance(sub_val, dict) and 'S' in sub_val:
                        val = sub_val['S']
                        sub_key_lower = sub_key.lower()
                        if 'first' in sub_key_lower:
                            contact['first_name'] = val
                        elif 'middle' in sub_key_lower:
                            contact['middle_name'] = val
                        elif 'last' in sub_key_lower:
                            contact['last_name'] = val

        # Address (composite)
        elif 'address' in label_lower or field_type == 'address':
            if 'M' in value_obj:
                nested = value_obj['M']
                addr_parts = []
                for sub_key, sub_val in nested.items():
                    if isinstance(sub_val, dict) and 'S' in sub_val:
                        val = sub_val['S']
                        sub_key_lower = sub_key.lower()
                        if 'street' in sub_key_lower:
                            contact['address']['street'] = val
                            addr_parts.insert(0, val)
                        elif 'unit' in sub_key_lower or 'apt' in sub_key_lower:
                            contact['address']['unit'] = val
                        elif 'city' in sub_key_lower:
                            contact['address']['city'] = val
                            addr_parts.append(val)
                        elif 'state' in sub_key_lower:
                            contact['address']['state'] = val
                            addr_parts.append(val)
                        elif 'zip' in sub_key_lower:
                            contact['address']['zip'] = val
                            addr_parts.append(val)
                contact['address']['formatted'] = ' '.join(addr_parts)

    # Build full_name if not set
    if not contact['full_name']:
        name_parts = [contact['first_name'], contact['middle_name'], contact['last_name']]
        contact['full_name'] = ' '.join(filter(None, name_parts)) or None

    return contact, comments


def build_form_data_display(form_data_labeled: Dict) -> Dict[str, str]:
    """
    Build flat key-value display format from form_data_labeled.
    Returns { "Name": "John Doe", "Email": "john@example.com", ... }
    """
    display = {}
    data_map = form_data_labeled.get('M', {})

    for field_label, field_wrapper in data_map.items():
        if not isinstance(field_wrapper, dict) or 'M' not in field_wrapper:
            continue

        field_obj = field_wrapper['M']
        field_type = field_obj.get('type', {}).get('S', 'text')
        value_obj = field_obj.get('value', {})

        # Handle simple string values
        if 'S' in value_obj:
            display[field_label] = value_obj['S']

        # Handle composite values (name, address)
        elif 'M' in value_obj:
            nested = value_obj['M']

            if field_type == 'name':
                # Combine name parts
                parts = []
                for key in ['First Name', 'Middle Name', 'Last Name']:
                    for sub_key, sub_val in nested.items():
                        if key.lower() in sub_key.lower() and isinstance(sub_val, dict) and 'S' in sub_val:
                            if sub_val['S']:
                                parts.append(sub_val['S'])
                            break
                display[field_label] = ' '.join(parts)

            elif field_type == 'address':
                # Format address as single line
                addr_parts = []
                order = ['street', 'unit', 'city', 'state', 'zip']
                for key in order:
                    for sub_key, sub_val in nested.items():
                        if key in sub_key.lower() and isinstance(sub_val, dict) and 'S' in sub_val:
                            if sub_val['S']:
                                addr_parts.append(sub_val['S'])
                            break
                display[field_label] = ' '.join(addr_parts)

            else:
                # Generic composite - join all values
                parts = []
                for sub_val in nested.values():
                    if isinstance(sub_val, dict) and 'S' in sub_val and sub_val['S']:
                        parts.append(sub_val['S'])
                display[field_label] = ' '.join(parts)

    return display


def convert_to_dynamodb_format(obj: Any) -> Dict:
    """Convert Python object to DynamoDB attribute format."""
    if obj is None:
        return {'NULL': True}
    elif isinstance(obj, str):
        return {'S': obj}
    elif isinstance(obj, bool):
        return {'BOOL': obj}
    elif isinstance(obj, (int, float)):
        return {'N': str(obj)}
    elif isinstance(obj, dict):
        return {'M': {k: convert_to_dynamodb_format(v) for k, v in obj.items()}}
    elif isinstance(obj, list):
        return {'L': [convert_to_dynamodb_format(item) for item in obj]}
    else:
        return {'S': str(obj)}


def process_item(item: Dict, dry_run: bool = False) -> bool:
    """
    Process a single DynamoDB item, adding missing fields.
    Returns True if item was updated.
    """
    submission_id = item.get('submission_id', {}).get('S', 'unknown')

    # Check if already has the new fields
    has_contact = 'contact' in item and item['contact'].get('M')
    has_display = 'form_data_display' in item and item['form_data_display'].get('M')

    if has_contact and has_display:
        return False  # Already backfilled

    # Get form_data_labeled
    form_data_labeled = item.get('form_data_labeled', {})
    if not form_data_labeled.get('M'):
        print(f"  ⚠️  {submission_id}: No form_data_labeled, skipping")
        return False

    # Extract contact and comments
    contact, comments = extract_contact_from_form_data_labeled(form_data_labeled)

    # Build display format
    form_data_display = build_form_data_display(form_data_labeled)

    if dry_run:
        print(f"  📋 {submission_id}:")
        print(f"      contact.email: {contact.get('email')}")
        print(f"      contact.phone: {contact.get('phone')}")
        print(f"      comments: {comments[:50] + '...' if comments and len(comments) > 50 else comments}")
        print(f"      display fields: {len(form_data_display)}")
        return True

    # Update DynamoDB
    try:
        update_expression_parts = []
        expression_values = {}
        expression_names = {}

        if not has_contact:
            update_expression_parts.append('#contact = :contact')
            expression_names['#contact'] = 'contact'
            expression_values[':contact'] = convert_to_dynamodb_format(contact)

        if comments:
            update_expression_parts.append('#comments = :comments')
            expression_names['#comments'] = 'comments'
            expression_values[':comments'] = {'S': comments}

        if not has_display and form_data_display:
            update_expression_parts.append('#display = :display')
            expression_names['#display'] = 'form_data_display'
            expression_values[':display'] = convert_to_dynamodb_format(form_data_display)

        if not update_expression_parts:
            return False

        dynamodb.update_item(
            TableName=TABLE_NAME,
            Key={'submission_id': {'S': submission_id}},
            UpdateExpression='SET ' + ', '.join(update_expression_parts),
            ExpressionAttributeNames=expression_names,
            ExpressionAttributeValues=expression_values
        )

        print(f"  ✅ {submission_id}: Updated ({len(form_data_display)} display fields)")
        return True

    except Exception as e:
        print(f"  ❌ {submission_id}: Error - {e}")
        stats['errors'] += 1
        return False


def main():
    dry_run = '--dry-run' in sys.argv

    print(f"\n{'=' * 60}")
    print(f"Form Submissions Backfill Script")
    print(f"{'=' * 60}")
    print(f"Table: {TABLE_NAME}")
    print(f"Mode: {'DRY RUN (no changes)' if dry_run else 'LIVE (writing to DynamoDB)'}")
    print(f"{'=' * 60}\n")

    if not dry_run:
        confirm = input("This will modify DynamoDB records. Continue? (yes/no): ")
        if confirm.lower() != 'yes':
            print("Aborted.")
            return

    # Scan all items
    paginator = dynamodb.get_paginator('scan')

    for page in paginator.paginate(TableName=TABLE_NAME):
        items = page.get('Items', [])

        for item in items:
            stats['scanned'] += 1

            if process_item(item, dry_run):
                stats['updated'] += 1
            else:
                stats['skipped'] += 1

    # Summary
    print(f"\n{'=' * 60}")
    print(f"Summary")
    print(f"{'=' * 60}")
    print(f"  Scanned: {stats['scanned']}")
    print(f"  Updated: {stats['updated']}")
    print(f"  Skipped: {stats['skipped']}")
    print(f"  Errors:  {stats['errors']}")
    print(f"{'=' * 60}\n")


if __name__ == '__main__':
    main()
