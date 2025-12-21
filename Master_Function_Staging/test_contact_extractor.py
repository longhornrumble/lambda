"""
Unit tests for contact_extractor.py
"""

import pytest
from contact_extractor import (
    extract_canonical_contact,
    create_empty_contact,
    format_address_text,
    filter_sensitive_fields,
    find_first_match,
    is_sensitive_field,
    truncate_text,
    get_schema_version,
    FIRST_NAME_KEYS,
    LAST_NAME_KEYS,
    STREET_KEYS,
    EMAIL_KEYS,
    PHONE_KEYS,
    COMMENTS_KEYS,
    SENSITIVE_FIELD_PATTERNS,
    COMMENTS_MAX_LENGTH,
    SCHEMA_VERSION
)


class TestExtractCanonicalContact:
    """Tests for extract_canonical_contact function"""

    def test_extracts_complete_contact(self):
        """Test extraction from typical complete form data"""
        form_data = {
            'first_name': 'Jane',
            'last_name': 'Smith',
            'email': 'jane.smith@example.com',
            'phone': '+15125551234',
            'street_address': '123 Main Street',
            'apt_suite_unit': 'Apt 4B',
            'city': 'Austin',
            'state': 'TX',
            'zip_code': '78701',
            'comments': 'Looking for volunteer opportunities'
        }

        result = extract_canonical_contact(form_data)

        assert result['contact']['first_name'] == 'Jane'
        assert result['contact']['last_name'] == 'Smith'
        assert result['contact']['name_full'] == 'Jane Smith'
        assert result['contact']['email'] == 'jane.smith@example.com'
        assert result['contact']['phone'] == '+15125551234'
        assert result['contact']['address']['street'] == '123 Main Street'
        assert result['contact']['address']['unit'] == 'Apt 4B'
        assert result['contact']['address']['city'] == 'Austin'
        assert result['contact']['address']['state'] == 'TX'
        assert result['contact']['address']['zip'] == '78701'
        assert result['contact']['address_text'] == '123 Main Street, Apt 4B, Austin, TX 78701'
        assert result['comments'] == 'Looking for volunteer opportunities'

    def test_handles_missing_optional_fields(self):
        """Test extraction when optional fields are missing"""
        form_data = {
            'first_name': 'John',
            'email': 'john@example.com'
        }

        result = extract_canonical_contact(form_data)

        assert result['contact']['first_name'] == 'John'
        assert result['contact']['middle_name'] is None
        assert result['contact']['last_name'] is None
        assert result['contact']['name_full'] == 'John'
        assert result['contact']['email'] == 'john@example.com'
        assert result['contact']['phone'] is None
        assert result['contact']['address']['street'] is None
        assert result['contact']['address_text'] is None
        assert result['comments'] is None

    def test_extracts_middle_name(self):
        """Test middle name extraction"""
        form_data = {
            'first_name': 'Mary',
            'middle_name': 'Jane',
            'last_name': 'Doe'
        }

        result = extract_canonical_contact(form_data)

        assert result['contact']['first_name'] == 'Mary'
        assert result['contact']['middle_name'] == 'Jane'
        assert result['contact']['last_name'] == 'Doe'
        assert result['contact']['name_full'] == 'Mary Jane Doe'

    def test_alternate_first_name_keys(self):
        """Test alternate key names for first name"""
        test_cases = [
            {'applicant_first_name': 'Alice'},
            {'contact_first_name': 'Bob'},
            {'given_name': 'Charlie'},
            {'firstname': 'Diana'}
        ]

        for form_data in test_cases:
            result = extract_canonical_contact(form_data)
            assert result['contact']['first_name'] is not None

    def test_alternate_last_name_keys(self):
        """Test alternate key names for last name"""
        test_cases = [
            {'applicant_last_name': 'Johnson'},
            {'contact_last_name': 'Williams'},
            {'surname': 'Brown'},
            {'family_name': 'Davis'},
            {'lastname': 'Miller'}
        ]

        for form_data in test_cases:
            result = extract_canonical_contact(form_data)
            assert result['contact']['last_name'] is not None

    def test_alternate_street_keys(self):
        """Test alternate key names for street address"""
        test_cases = [
            {'address_street': '456 Oak Ave'},
            {'street': '789 Pine Blvd'},
            {'address_line_1': '101 Elm St'},
            {'address1': '202 Maple Dr'}
        ]

        for form_data in test_cases:
            result = extract_canonical_contact(form_data)
            assert result['contact']['address']['street'] is not None

    def test_alternate_email_keys(self):
        """Test alternate key names for email"""
        test_cases = [
            {'email_address': 'test1@example.com'},
            {'e_mail': 'test2@example.com'},
            {'contact_email': 'test3@example.com'},
            {'applicant_email': 'test4@example.com'}
        ]

        for form_data in test_cases:
            result = extract_canonical_contact(form_data)
            assert result['contact']['email'] is not None
            assert '@' in result['contact']['email']

    def test_alternate_phone_keys(self):
        """Test alternate key names for phone"""
        test_cases = [
            {'phone_number': '555-1234'},
            {'mobile': '555-5678'},
            {'cell': '555-9012'},
            {'telephone': '555-3456'},
            {'caregivers_phone_number': '555-7890'}
        ]

        for form_data in test_cases:
            result = extract_canonical_contact(form_data)
            assert result['contact']['phone'] is not None

    def test_comments_from_various_fields(self):
        """Test comments extraction from various field names"""
        test_cases = [
            {'comments': 'Test comment 1'},
            {'comment': 'Test comment 2'},
            {'message': 'Test message'},
            {'notes': 'Test notes'},
            {'note': 'Test note'},
            {'description_of_needs': 'Need help with groceries'},
            {'how_can_we_help': 'Looking for support'},
            {'reason_for_inquiry': 'General inquiry'},
            {'additional_info': 'Extra information'}
        ]

        for form_data in test_cases:
            result = extract_canonical_contact(form_data)
            assert result['comments'] is not None

    def test_truncates_long_comments(self):
        """Test that long comments are truncated at 500 characters"""
        long_comment = 'A' * 600
        form_data = {'comments': long_comment}

        result = extract_canonical_contact(form_data)

        assert len(result['comments']) == 503  # 500 + '...'
        assert result['comments'].endswith('...')

    def test_returns_empty_for_none_input(self):
        """Test returns empty contact for None input"""
        result = extract_canonical_contact(None)

        assert result['contact']['first_name'] is None
        assert result['contact']['last_name'] is None
        assert result['contact']['email'] is None
        assert result['comments'] is None

    def test_returns_empty_for_invalid_input(self):
        """Test returns empty contact for invalid input"""
        result = extract_canonical_contact('not a dict')

        assert result['contact']['first_name'] is None
        assert result['comments'] is None

    def test_returns_empty_for_empty_object(self):
        """Test returns empty contact for empty dict"""
        result = extract_canonical_contact({})

        assert result['contact']['first_name'] is None
        assert result['contact']['name_full'] is None
        assert result['comments'] is None

    def test_case_insensitive_matching(self):
        """Test case-insensitive key matching"""
        form_data = {
            'FIRST_NAME': 'Test',
            'Last_Name': 'User',
            'EMAIL': 'test@example.com'
        }

        result = extract_canonical_contact(form_data)

        assert result['contact']['first_name'] == 'Test'
        assert result['contact']['last_name'] == 'User'
        assert result['contact']['email'] == 'test@example.com'

    def test_trims_whitespace(self):
        """Test whitespace is trimmed from values"""
        form_data = {
            'first_name': '  Jane  ',
            'email': '  jane@example.com  '
        }

        result = extract_canonical_contact(form_data)

        assert result['contact']['first_name'] == 'Jane'
        assert result['contact']['email'] == 'jane@example.com'

    def test_skips_empty_string_values(self):
        """Test empty string values are treated as missing"""
        form_data = {
            'first_name': '',
            'last_name': 'Smith',
            'email': ''
        }

        result = extract_canonical_contact(form_data)

        assert result['contact']['first_name'] is None
        assert result['contact']['last_name'] == 'Smith'
        assert result['contact']['name_full'] == 'Smith'
        assert result['contact']['email'] is None


class TestFormatAddressText:
    """Tests for format_address_text function"""

    def test_formats_complete_address(self):
        """Test formatting complete address"""
        result = format_address_text('123 Main St', 'Apt 4B', 'Austin', 'TX', '78701')
        assert result == '123 Main St, Apt 4B, Austin, TX 78701'

    def test_formats_address_without_unit(self):
        """Test formatting address without unit"""
        result = format_address_text('123 Main St', None, 'Austin', 'TX', '78701')
        assert result == '123 Main St, Austin, TX 78701'

    def test_formats_address_street_and_city_only(self):
        """Test formatting with only street and city"""
        result = format_address_text('123 Main St', None, 'Austin', None, None)
        assert result == '123 Main St, Austin'

    def test_formats_city_state_zip_only(self):
        """Test formatting with only city, state, zip"""
        result = format_address_text(None, None, 'Austin', 'TX', '78701')
        assert result == 'Austin, TX 78701'

    def test_returns_none_for_no_parts(self):
        """Test returns None when no address parts"""
        result = format_address_text(None, None, None, None, None)
        assert result is None

    def test_handles_unit_without_street(self):
        """Test handling unit without street"""
        result = format_address_text(None, 'Suite 100', 'Austin', 'TX', '78701')
        assert result == 'Suite 100, Austin, TX 78701'


class TestFilterSensitiveFields:
    """Tests for filter_sensitive_fields function"""

    def test_removes_ssn_fields(self):
        """Test removal of SSN-related fields"""
        form_data = {
            'first_name': 'Jane',
            'ssn': '123-45-6789',
            'social_security_number': '987-65-4321'
        }

        result = filter_sensitive_fields(form_data)

        assert result['first_name'] == 'Jane'
        assert 'ssn' not in result
        assert 'social_security_number' not in result

    def test_removes_dob_fields(self):
        """Test removal of DOB-related fields"""
        form_data = {
            'first_name': 'Jane',
            'dob': '1990-01-15',
            'date_of_birth': '1990-01-15',
            'birth_date': '1990-01-15'
        }

        result = filter_sensitive_fields(form_data)

        assert result['first_name'] == 'Jane'
        assert 'dob' not in result
        assert 'date_of_birth' not in result
        assert 'birth_date' not in result

    def test_removes_password_fields(self):
        """Test removal of password-related fields"""
        form_data = {
            'first_name': 'Jane',
            'password': 'secret123',
            'secret_key': 'abc123'
        }

        result = filter_sensitive_fields(form_data)

        assert result['first_name'] == 'Jane'
        assert 'password' not in result
        assert 'secret_key' not in result

    def test_handles_none_input(self):
        """Test handling None input"""
        result = filter_sensitive_fields(None)
        assert result is None

    def test_handles_empty_dict(self):
        """Test handling empty dict"""
        result = filter_sensitive_fields({})
        assert result == {}


class TestIsSensitiveField:
    """Tests for is_sensitive_field function"""

    def test_identifies_ssn_fields(self):
        """Test identification of SSN fields"""
        assert is_sensitive_field('ssn') is True
        assert is_sensitive_field('social_security') is True
        assert is_sensitive_field('SSN') is True

    def test_identifies_dob_fields(self):
        """Test identification of DOB fields"""
        assert is_sensitive_field('dob') is True
        assert is_sensitive_field('date_of_birth') is True
        assert is_sensitive_field('birth_date') is True

    def test_identifies_password_fields(self):
        """Test identification of password fields"""
        assert is_sensitive_field('password') is True
        assert is_sensitive_field('user_password') is True

    def test_non_sensitive_fields(self):
        """Test non-sensitive fields return False"""
        assert is_sensitive_field('first_name') is False
        assert is_sensitive_field('email') is False
        assert is_sensitive_field('phone') is False


class TestTruncateText:
    """Tests for truncate_text function"""

    def test_no_truncation_for_short_text(self):
        """Test no truncation for text under limit"""
        result = truncate_text('Hello', 100)
        assert result == 'Hello'

    def test_truncates_long_text(self):
        """Test truncation with ellipsis"""
        result = truncate_text('Hello World', 5)
        assert result == 'Hello...'

    def test_handles_none_input(self):
        """Test handling None input"""
        result = truncate_text(None, 100)
        assert result is None

    def test_exact_length_match(self):
        """Test text at exact length limit"""
        result = truncate_text('Hello', 5)
        assert result == 'Hello'


class TestFindFirstMatch:
    """Tests for find_first_match function"""

    def test_finds_exact_match(self):
        """Test finding exact key match"""
        form_data = {'first_name': 'Jane', 'last_name': 'Doe'}
        result = find_first_match(form_data, ['first_name', 'given_name'])
        assert result == 'Jane'

    def test_uses_priority_order(self):
        """Test priority order is respected"""
        form_data = {'given_name': 'Alice', 'first_name': 'Jane'}
        result = find_first_match(form_data, ['first_name', 'given_name'])
        assert result == 'Jane'

    def test_skips_empty_values(self):
        """Test skipping empty values"""
        form_data = {'first_name': '', 'given_name': 'Alice'}
        result = find_first_match(form_data, ['first_name', 'given_name'])
        assert result == 'Alice'

    def test_returns_none_when_no_match(self):
        """Test returns None when no match found"""
        form_data = {'other_field': 'value'}
        result = find_first_match(form_data, ['first_name', 'given_name'])
        assert result is None

    def test_handles_none_input(self):
        """Test handling None input"""
        result = find_first_match(None, ['first_name'])
        assert result is None


class TestCreateEmptyContact:
    """Tests for create_empty_contact function"""

    def test_all_fields_are_none(self):
        """Test all fields are None"""
        contact = create_empty_contact()

        assert contact['first_name'] is None
        assert contact['middle_name'] is None
        assert contact['last_name'] is None
        assert contact['name_full'] is None
        assert contact['email'] is None
        assert contact['phone'] is None
        assert contact['address']['street'] is None
        assert contact['address']['unit'] is None
        assert contact['address']['city'] is None
        assert contact['address']['state'] is None
        assert contact['address']['zip'] is None
        assert contact['address_text'] is None


class TestGetSchemaVersion:
    """Tests for get_schema_version function"""

    def test_returns_current_version(self):
        """Test returns current schema version"""
        assert get_schema_version() == 2
        assert get_schema_version() == SCHEMA_VERSION


class TestConstants:
    """Tests for module constants"""

    def test_comments_max_length(self):
        """Test COMMENTS_MAX_LENGTH is 500"""
        assert COMMENTS_MAX_LENGTH == 500

    def test_schema_version(self):
        """Test SCHEMA_VERSION is 2"""
        assert SCHEMA_VERSION == 2

    def test_key_lists_not_empty(self):
        """Test key lists are non-empty"""
        assert len(FIRST_NAME_KEYS) > 0
        assert len(LAST_NAME_KEYS) > 0
        assert len(STREET_KEYS) > 0
        assert len(EMAIL_KEYS) > 0
        assert len(PHONE_KEYS) > 0
        assert len(COMMENTS_KEYS) > 0

    def test_sensitive_patterns_include_expected(self):
        """Test sensitive patterns include expected values"""
        assert 'ssn' in SENSITIVE_FIELD_PATTERNS
        assert 'dob' in SENSITIVE_FIELD_PATTERNS
        assert 'password' in SENSITIVE_FIELD_PATTERNS


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
