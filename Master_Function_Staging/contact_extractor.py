"""
Contact Extractor Module

Extracts canonical structured contact information from form data.
Provides a stable, documented schema for downstream integrations (Bubble, etc.)

Schema Version: 2
"""

from typing import Dict, List, Any, Optional, Tuple

# ============================================================================
# CONFIGURABLE KEY MAPPINGS - Extend these to support new form field names
# ============================================================================

# First name field keys (in priority order)
FIRST_NAME_KEYS = [
    'first_name',
    'firstname',
    'applicant_first_name',
    'contact_first_name',
    'given_name'
]

# Middle name field keys (in priority order)
MIDDLE_NAME_KEYS = [
    'middle_name',
    'middlename',
    'applicant_middle_name',
    'contact_middle_name'
]

# Last name field keys (in priority order)
LAST_NAME_KEYS = [
    'last_name',
    'lastname',
    'applicant_last_name',
    'contact_last_name',
    'surname',
    'family_name'
]

# Street address field keys (in priority order)
STREET_KEYS = [
    'street_address',
    'address_street',
    'street',
    'address_line_1',
    'address1',
    'address'
]

# Unit/apartment field keys (in priority order)
UNIT_KEYS = [
    'apt_suite_unit',
    'unit',
    'apt',
    'suite',
    'apartment',
    'address_line_2',
    'address2'
]

# City field keys (in priority order)
CITY_KEYS = [
    'city',
    'address_city',
    'locality'
]

# State field keys (in priority order)
STATE_KEYS = [
    'state',
    'address_state',
    'province',
    'region'
]

# ZIP/postal code field keys (in priority order)
ZIP_KEYS = [
    'zip_code',
    'zip',
    'postal_code',
    'postcode',
    'zipcode'
]

# Email field keys (in priority order)
EMAIL_KEYS = [
    'email',
    'email_address',
    'e_mail',
    'contact_email',
    'applicant_email'
]

# Phone field keys (in priority order)
PHONE_KEYS = [
    'phone',
    'phone_number',
    'mobile',
    'cell',
    'telephone',
    'contact_phone',
    'applicant_phone',
    'caregivers_phone_number'
]

# Comments/notes field keys (in priority order)
COMMENTS_KEYS = [
    'comments',
    'comment',
    'message',
    'notes',
    'note',
    'description_of_needs',
    'how_can_we_help',
    'reason_for_inquiry',
    'additional_info',
    'additional_information',
    'details',
    'description'
]

# Sensitive field patterns to mask in email_details_text
SENSITIVE_FIELD_PATTERNS = [
    'ssn',
    'social_security',
    'dob',
    'date_of_birth',
    'birth_date',
    'password',
    'secret',
    'pin'
]

# Maximum length for comments field before truncation
COMMENTS_MAX_LENGTH = 500

# Current schema version for the contact payload
SCHEMA_VERSION = 2


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def find_first_match(form_data: Dict[str, Any], key_list: List[str]) -> Optional[str]:
    """
    Find the first matching value from form data using priority key list.

    Args:
        form_data: Parsed form data object
        key_list: Priority list of keys to search

    Returns:
        First non-empty value found, or None
    """
    if not form_data or not isinstance(form_data, dict):
        return None

    # Create lowercase key map for case-insensitive matching
    lower_key_map = {}
    for key, value in form_data.items():
        lower_key_map[key.lower()] = {'original_key': key, 'value': value}

    # Search through priority list
    for search_key in key_list:
        lower_search_key = search_key.lower()

        # Exact match first
        if lower_search_key in lower_key_map:
            val = lower_key_map[lower_search_key]['value']
            if val is not None and val != '':
                return str(val).strip()

    return None


def find_first_match_partial(form_data: Dict[str, Any], patterns: List[str]) -> Optional[str]:
    """
    Find the first matching value using partial key matching (contains).

    Args:
        form_data: Parsed form data object
        patterns: Patterns to search for in keys

    Returns:
        First non-empty value found, or None
    """
    if not form_data or not isinstance(form_data, dict):
        return None

    for pattern in patterns:
        for key, value in form_data.items():
            if pattern.lower() in key.lower():
                if value is not None and value != '':
                    return str(value).strip()

    return None


def is_sensitive_field(key: str) -> bool:
    """
    Check if a field key contains a sensitive pattern.

    Args:
        key: Field key to check

    Returns:
        True if key matches a sensitive pattern
    """
    lower_key = key.lower()
    return any(pattern in lower_key for pattern in SENSITIVE_FIELD_PATTERNS)


def truncate_text(text: Optional[str], max_length: int) -> Optional[str]:
    """
    Truncate a string to max length with ellipsis.

    Args:
        text: Text to truncate
        max_length: Maximum length

    Returns:
        Truncated text
    """
    if not text or len(text) <= max_length:
        return text
    return text[:max_length] + '...'


# ============================================================================
# MAIN EXTRACTION FUNCTIONS
# ============================================================================

def extract_canonical_contact(form_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract canonical contact information and comments from form data.

    Args:
        form_data: Parsed form data object

    Returns:
        Dictionary with 'contact' and 'comments' keys
    """
    if not form_data or not isinstance(form_data, dict):
        return {
            'contact': create_empty_contact(),
            'comments': None
        }

    # Extract name parts
    first_name = find_first_match(form_data, FIRST_NAME_KEYS)
    middle_name = find_first_match(form_data, MIDDLE_NAME_KEYS)
    last_name = find_first_match(form_data, LAST_NAME_KEYS)

    # Compute full name from parts
    name_parts = [p for p in [first_name, middle_name, last_name] if p]
    name_full = ' '.join(name_parts) if name_parts else None

    # Extract address parts
    street = find_first_match(form_data, STREET_KEYS)
    unit = find_first_match(form_data, UNIT_KEYS)
    city = find_first_match(form_data, CITY_KEYS)
    state = find_first_match(form_data, STATE_KEYS)
    zip_code = find_first_match(form_data, ZIP_KEYS)

    # Compute address text
    address_text = format_address_text(street, unit, city, state, zip_code)

    # Extract email - try exact keys first, then partial match
    email = find_first_match(form_data, EMAIL_KEYS)
    if not email:
        # Fallback: find any field containing 'email' with @ in value
        for key, value in form_data.items():
            if 'email' in key.lower() and value and '@' in str(value):
                email = str(value).strip()
                break

    # Extract phone - try exact keys first, then partial match
    phone = find_first_match(form_data, PHONE_KEYS)
    if not phone:
        phone = find_first_match_partial(form_data, ['phone', 'mobile', 'cell'])

    # Extract comments
    comments = find_first_match(form_data, COMMENTS_KEYS)
    if comments:
        comments = truncate_text(comments, COMMENTS_MAX_LENGTH)

    # Build contact object
    contact = {
        'first_name': first_name,
        'middle_name': middle_name,
        'last_name': last_name,
        'name_full': name_full,
        'email': email,
        'phone': phone,
        'address': {
            'street': street,
            'unit': unit,
            'city': city,
            'state': state,
            'zip': zip_code
        },
        'address_text': address_text
    }

    return {
        'contact': contact,
        'comments': comments
    }


def create_empty_contact() -> Dict[str, Any]:
    """
    Create an empty contact object with all None values.

    Returns:
        Empty contact object
    """
    return {
        'first_name': None,
        'middle_name': None,
        'last_name': None,
        'name_full': None,
        'email': None,
        'phone': None,
        'address': {
            'street': None,
            'unit': None,
            'city': None,
            'state': None,
            'zip': None
        },
        'address_text': None
    }


def format_address_text(
    street: Optional[str],
    unit: Optional[str],
    city: Optional[str],
    state: Optional[str],
    zip_code: Optional[str]
) -> Optional[str]:
    """
    Format address parts into a single text string.

    Args:
        street: Street address
        unit: Unit/apartment
        city: City
        state: State
        zip_code: ZIP code

    Returns:
        Formatted address or None if no parts
    """
    parts = []

    # Street + Unit
    if street:
        if unit:
            parts.append(f"{street}, {unit}")
        else:
            parts.append(street)
    elif unit:
        parts.append(unit)

    # City, State ZIP
    city_state_zip = []
    if city:
        city_state_zip.append(city)
    if state:
        if city_state_zip:
            city_state_zip[-1] += ','
        city_state_zip.append(state)
    if zip_code:
        city_state_zip.append(zip_code)

    if city_state_zip:
        parts.append(' '.join(city_state_zip))

    return ', '.join(parts) if parts else None


def filter_sensitive_fields(form_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filter sensitive fields from form data for email display.

    Args:
        form_data: Parsed form data object

    Returns:
        Form data with sensitive fields removed
    """
    if not form_data or not isinstance(form_data, dict):
        return form_data

    return {
        key: value
        for key, value in form_data.items()
        if not is_sensitive_field(key)
    }


def get_schema_version() -> int:
    """
    Get the current schema version.

    Returns:
        Schema version number
    """
    return SCHEMA_VERSION
