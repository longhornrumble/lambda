/**
 * Unit tests for contact_extractor.js
 */

const {
  extractCanonicalContact,
  createEmptyContact,
  formatAddressText,
  filterSensitiveFields,
  findFirstMatch,
  isSensitiveField,
  truncateText,
  getSchemaVersion,
  FIRST_NAME_KEYS,
  LAST_NAME_KEYS,
  STREET_KEYS,
  EMAIL_KEYS,
  PHONE_KEYS,
  COMMENTS_KEYS,
  SENSITIVE_FIELD_PATTERNS,
  COMMENTS_MAX_LENGTH,
  SCHEMA_VERSION
} = require('../contact_extractor');

describe('Contact Extractor', () => {
  describe('extractCanonicalContact', () => {
    test('extracts complete contact from typical form data', () => {
      const formData = {
        first_name: 'Jane',
        last_name: 'Smith',
        email: 'jane.smith@example.com',
        phone: '+15125551234',
        street_address: '123 Main Street',
        apt_suite_unit: 'Apt 4B',
        city: 'Austin',
        state: 'TX',
        zip_code: '78701',
        comments: 'Looking for volunteer opportunities'
      };

      const result = extractCanonicalContact(formData);

      expect(result.contact.first_name).toBe('Jane');
      expect(result.contact.last_name).toBe('Smith');
      expect(result.contact.name_full).toBe('Jane Smith');
      expect(result.contact.email).toBe('jane.smith@example.com');
      expect(result.contact.phone).toBe('+15125551234');
      expect(result.contact.address.street).toBe('123 Main Street');
      expect(result.contact.address.unit).toBe('Apt 4B');
      expect(result.contact.address.city).toBe('Austin');
      expect(result.contact.address.state).toBe('TX');
      expect(result.contact.address.zip).toBe('78701');
      expect(result.contact.address_text).toBe('123 Main Street, Apt 4B, Austin, TX 78701');
      expect(result.comments).toBe('Looking for volunteer opportunities');
    });

    test('handles missing optional fields', () => {
      const formData = {
        first_name: 'John',
        email: 'john@example.com'
      };

      const result = extractCanonicalContact(formData);

      expect(result.contact.first_name).toBe('John');
      expect(result.contact.middle_name).toBeNull();
      expect(result.contact.last_name).toBeNull();
      expect(result.contact.name_full).toBe('John');
      expect(result.contact.email).toBe('john@example.com');
      expect(result.contact.phone).toBeNull();
      expect(result.contact.address.street).toBeNull();
      expect(result.contact.address_text).toBeNull();
      expect(result.comments).toBeNull();
    });

    test('extracts middle name when present', () => {
      const formData = {
        first_name: 'Mary',
        middle_name: 'Jane',
        last_name: 'Doe'
      };

      const result = extractCanonicalContact(formData);

      expect(result.contact.first_name).toBe('Mary');
      expect(result.contact.middle_name).toBe('Jane');
      expect(result.contact.last_name).toBe('Doe');
      expect(result.contact.name_full).toBe('Mary Jane Doe');
    });

    test('handles alternate key names for first name', () => {
      const testCases = [
        { applicant_first_name: 'Alice' },
        { contact_first_name: 'Bob' },
        { given_name: 'Charlie' },
        { firstname: 'Diana' }
      ];

      for (const formData of testCases) {
        const result = extractCanonicalContact(formData);
        expect(result.contact.first_name).not.toBeNull();
      }
    });

    test('handles alternate key names for last name', () => {
      const testCases = [
        { applicant_last_name: 'Johnson' },
        { contact_last_name: 'Williams' },
        { surname: 'Brown' },
        { family_name: 'Davis' },
        { lastname: 'Miller' }
      ];

      for (const formData of testCases) {
        const result = extractCanonicalContact(formData);
        expect(result.contact.last_name).not.toBeNull();
      }
    });

    test('handles alternate key names for street address', () => {
      const testCases = [
        { address_street: '456 Oak Ave' },
        { street: '789 Pine Blvd' },
        { address_line_1: '101 Elm St' },
        { address1: '202 Maple Dr' }
      ];

      for (const formData of testCases) {
        const result = extractCanonicalContact(formData);
        expect(result.contact.address.street).not.toBeNull();
      }
    });

    test('handles alternate key names for email', () => {
      const testCases = [
        { email_address: 'test1@example.com' },
        { e_mail: 'test2@example.com' },
        { contact_email: 'test3@example.com' },
        { applicant_email: 'test4@example.com' }
      ];

      for (const formData of testCases) {
        const result = extractCanonicalContact(formData);
        expect(result.contact.email).not.toBeNull();
        expect(result.contact.email).toContain('@');
      }
    });

    test('handles alternate key names for phone', () => {
      const testCases = [
        { phone_number: '555-1234' },
        { mobile: '555-5678' },
        { cell: '555-9012' },
        { telephone: '555-3456' },
        { caregivers_phone_number: '555-7890' }
      ];

      for (const formData of testCases) {
        const result = extractCanonicalContact(formData);
        expect(result.contact.phone).not.toBeNull();
      }
    });

    test('extracts comments from various field names', () => {
      const testCases = [
        { comments: 'Test comment 1' },
        { comment: 'Test comment 2' },
        { message: 'Test message' },
        { notes: 'Test notes' },
        { note: 'Test note' },
        { description_of_needs: 'Need help with groceries' },
        { how_can_we_help: 'Looking for support' },
        { reason_for_inquiry: 'General inquiry' },
        { additional_info: 'Extra information' }
      ];

      for (const formData of testCases) {
        const result = extractCanonicalContact(formData);
        expect(result.comments).not.toBeNull();
      }
    });

    test('truncates long comments at 500 characters', () => {
      const longComment = 'A'.repeat(600);
      const formData = {
        comments: longComment
      };

      const result = extractCanonicalContact(formData);

      expect(result.comments.length).toBe(503); // 500 + '...'
      expect(result.comments.endsWith('...')).toBe(true);
    });

    test('returns empty contact for null input', () => {
      const result = extractCanonicalContact(null);

      expect(result.contact.first_name).toBeNull();
      expect(result.contact.last_name).toBeNull();
      expect(result.contact.email).toBeNull();
      expect(result.comments).toBeNull();
    });

    test('returns empty contact for invalid input', () => {
      const result = extractCanonicalContact('not an object');

      expect(result.contact.first_name).toBeNull();
      expect(result.comments).toBeNull();
    });

    test('returns empty contact for empty object', () => {
      const result = extractCanonicalContact({});

      expect(result.contact.first_name).toBeNull();
      expect(result.contact.name_full).toBeNull();
      expect(result.comments).toBeNull();
    });

    test('handles case-insensitive key matching', () => {
      const formData = {
        FIRST_NAME: 'Test',
        Last_Name: 'User',
        EMAIL: 'test@example.com'
      };

      const result = extractCanonicalContact(formData);

      expect(result.contact.first_name).toBe('Test');
      expect(result.contact.last_name).toBe('User');
      expect(result.contact.email).toBe('test@example.com');
    });

    test('trims whitespace from values', () => {
      const formData = {
        first_name: '  Jane  ',
        email: '  jane@example.com  '
      };

      const result = extractCanonicalContact(formData);

      expect(result.contact.first_name).toBe('Jane');
      expect(result.contact.email).toBe('jane@example.com');
    });

    test('skips empty string values', () => {
      const formData = {
        first_name: '',
        last_name: 'Smith',
        email: ''
      };

      const result = extractCanonicalContact(formData);

      expect(result.contact.first_name).toBeNull();
      expect(result.contact.last_name).toBe('Smith');
      expect(result.contact.name_full).toBe('Smith');
      expect(result.contact.email).toBeNull();
    });
  });

  describe('formatAddressText', () => {
    test('formats complete address', () => {
      const result = formatAddressText('123 Main St', 'Apt 4B', 'Austin', 'TX', '78701');
      expect(result).toBe('123 Main St, Apt 4B, Austin, TX 78701');
    });

    test('formats address without unit', () => {
      const result = formatAddressText('123 Main St', null, 'Austin', 'TX', '78701');
      expect(result).toBe('123 Main St, Austin, TX 78701');
    });

    test('formats address with only street and city', () => {
      const result = formatAddressText('123 Main St', null, 'Austin', null, null);
      expect(result).toBe('123 Main St, Austin');
    });

    test('formats address with only city state zip', () => {
      const result = formatAddressText(null, null, 'Austin', 'TX', '78701');
      expect(result).toBe('Austin, TX 78701');
    });

    test('returns null for no address parts', () => {
      const result = formatAddressText(null, null, null, null, null);
      expect(result).toBeNull();
    });

    test('handles unit without street', () => {
      const result = formatAddressText(null, 'Suite 100', 'Austin', 'TX', '78701');
      expect(result).toBe('Suite 100, Austin, TX 78701');
    });
  });

  describe('filterSensitiveFields', () => {
    test('removes fields with SSN pattern', () => {
      const formData = {
        first_name: 'Jane',
        ssn: '123-45-6789',
        social_security_number: '987-65-4321'
      };

      const result = filterSensitiveFields(formData);

      expect(result.first_name).toBe('Jane');
      expect(result.ssn).toBeUndefined();
      expect(result.social_security_number).toBeUndefined();
    });

    test('removes fields with DOB pattern', () => {
      const formData = {
        first_name: 'Jane',
        dob: '1990-01-15',
        date_of_birth: '1990-01-15',
        birth_date: '1990-01-15'
      };

      const result = filterSensitiveFields(formData);

      expect(result.first_name).toBe('Jane');
      expect(result.dob).toBeUndefined();
      expect(result.date_of_birth).toBeUndefined();
      expect(result.birth_date).toBeUndefined();
    });

    test('removes fields with password pattern', () => {
      const formData = {
        first_name: 'Jane',
        password: 'secret123',
        secret_key: 'abc123'
      };

      const result = filterSensitiveFields(formData);

      expect(result.first_name).toBe('Jane');
      expect(result.password).toBeUndefined();
      expect(result.secret_key).toBeUndefined();
    });

    test('handles null input', () => {
      const result = filterSensitiveFields(null);
      expect(result).toBeNull();
    });

    test('handles empty object', () => {
      const result = filterSensitiveFields({});
      expect(result).toEqual({});
    });
  });

  describe('isSensitiveField', () => {
    test('identifies SSN fields', () => {
      expect(isSensitiveField('ssn')).toBe(true);
      expect(isSensitiveField('social_security')).toBe(true);
      expect(isSensitiveField('SSN')).toBe(true);
    });

    test('identifies DOB fields', () => {
      expect(isSensitiveField('dob')).toBe(true);
      expect(isSensitiveField('date_of_birth')).toBe(true);
      expect(isSensitiveField('birth_date')).toBe(true);
    });

    test('identifies password fields', () => {
      expect(isSensitiveField('password')).toBe(true);
      expect(isSensitiveField('user_password')).toBe(true);
    });

    test('returns false for non-sensitive fields', () => {
      expect(isSensitiveField('first_name')).toBe(false);
      expect(isSensitiveField('email')).toBe(false);
      expect(isSensitiveField('phone')).toBe(false);
    });
  });

  describe('truncateText', () => {
    test('does not truncate short text', () => {
      const result = truncateText('Hello', 100);
      expect(result).toBe('Hello');
    });

    test('truncates long text with ellipsis', () => {
      const result = truncateText('Hello World', 5);
      expect(result).toBe('Hello...');
    });

    test('handles null input', () => {
      const result = truncateText(null, 100);
      expect(result).toBeNull();
    });

    test('handles exact length match', () => {
      const result = truncateText('Hello', 5);
      expect(result).toBe('Hello');
    });
  });

  describe('findFirstMatch', () => {
    test('finds exact key match', () => {
      const formData = { first_name: 'Jane', last_name: 'Doe' };
      const result = findFirstMatch(formData, ['first_name', 'given_name']);
      expect(result).toBe('Jane');
    });

    test('uses priority order', () => {
      const formData = { given_name: 'Alice', first_name: 'Jane' };
      const result = findFirstMatch(formData, ['first_name', 'given_name']);
      expect(result).toBe('Jane');
    });

    test('skips empty values', () => {
      const formData = { first_name: '', given_name: 'Alice' };
      const result = findFirstMatch(formData, ['first_name', 'given_name']);
      expect(result).toBe('Alice');
    });

    test('returns null when no match', () => {
      const formData = { other_field: 'value' };
      const result = findFirstMatch(formData, ['first_name', 'given_name']);
      expect(result).toBeNull();
    });

    test('handles null input', () => {
      const result = findFirstMatch(null, ['first_name']);
      expect(result).toBeNull();
    });
  });

  describe('createEmptyContact', () => {
    test('creates object with all null values', () => {
      const contact = createEmptyContact();

      expect(contact.first_name).toBeNull();
      expect(contact.middle_name).toBeNull();
      expect(contact.last_name).toBeNull();
      expect(contact.name_full).toBeNull();
      expect(contact.email).toBeNull();
      expect(contact.phone).toBeNull();
      expect(contact.address.street).toBeNull();
      expect(contact.address.unit).toBeNull();
      expect(contact.address.city).toBeNull();
      expect(contact.address.state).toBeNull();
      expect(contact.address.zip).toBeNull();
      expect(contact.address_text).toBeNull();
    });
  });

  describe('getSchemaVersion', () => {
    test('returns current schema version', () => {
      expect(getSchemaVersion()).toBe(2);
      expect(getSchemaVersion()).toBe(SCHEMA_VERSION);
    });
  });

  describe('Constants', () => {
    test('COMMENTS_MAX_LENGTH is 500', () => {
      expect(COMMENTS_MAX_LENGTH).toBe(500);
    });

    test('SCHEMA_VERSION is 2', () => {
      expect(SCHEMA_VERSION).toBe(2);
    });

    test('key lists are non-empty arrays', () => {
      expect(FIRST_NAME_KEYS.length).toBeGreaterThan(0);
      expect(LAST_NAME_KEYS.length).toBeGreaterThan(0);
      expect(STREET_KEYS.length).toBeGreaterThan(0);
      expect(EMAIL_KEYS.length).toBeGreaterThan(0);
      expect(PHONE_KEYS.length).toBeGreaterThan(0);
      expect(COMMENTS_KEYS.length).toBeGreaterThan(0);
    });

    test('sensitive patterns include expected values', () => {
      expect(SENSITIVE_FIELD_PATTERNS).toContain('ssn');
      expect(SENSITIVE_FIELD_PATTERNS).toContain('dob');
      expect(SENSITIVE_FIELD_PATTERNS).toContain('password');
    });
  });
});
