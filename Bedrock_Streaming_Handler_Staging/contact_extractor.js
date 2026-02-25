/**
 * Contact Extractor Module
 *
 * Extracts canonical structured contact information from form data.
 * Provides a stable, documented schema for downstream integrations (Bubble, etc.)
 *
 * Schema Version: 2
 */

// ============================================================================
// CONFIGURABLE KEY MAPPINGS - Extend these to support new form field names
// ============================================================================

/**
 * First name field keys (in priority order)
 */
const FIRST_NAME_KEYS = [
  'first_name',
  'firstname',
  'applicant_first_name',
  'contact_first_name',
  'given_name'
];

/**
 * Middle name field keys (in priority order)
 */
const MIDDLE_NAME_KEYS = [
  'middle_name',
  'middlename',
  'applicant_middle_name',
  'contact_middle_name'
];

/**
 * Last name field keys (in priority order)
 */
const LAST_NAME_KEYS = [
  'last_name',
  'lastname',
  'applicant_last_name',
  'contact_last_name',
  'surname',
  'family_name'
];

/**
 * Street address field keys (in priority order)
 */
const STREET_KEYS = [
  'street_address',
  'address_street',
  'street',
  'address_line_1',
  'address1',
  'address'
];

/**
 * Unit/apartment field keys (in priority order)
 */
const UNIT_KEYS = [
  'apt_suite_unit',
  'unit',
  'apt',
  'suite',
  'apartment',
  'address_line_2',
  'address2'
];

/**
 * City field keys (in priority order)
 */
const CITY_KEYS = [
  'city',
  'address_city',
  'locality'
];

/**
 * State field keys (in priority order)
 */
const STATE_KEYS = [
  'state',
  'address_state',
  'province',
  'region'
];

/**
 * ZIP/postal code field keys (in priority order)
 */
const ZIP_KEYS = [
  'zip_code',
  'zip',
  'postal_code',
  'postcode',
  'zipcode'
];

/**
 * Email field keys (in priority order)
 */
const EMAIL_KEYS = [
  'email',
  'email_address',
  'e_mail',
  'contact_email',
  'applicant_email'
];

/**
 * Phone field keys (in priority order)
 */
const PHONE_KEYS = [
  'phone',
  'phone_number',
  'mobile',
  'cell',
  'telephone',
  'contact_phone',
  'applicant_phone',
  'caregivers_phone_number'
];

/**
 * Comments/notes field keys (in priority order)
 */
const COMMENTS_KEYS = [
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
];

/**
 * Sensitive field patterns to mask in email_details_text
 */
const SENSITIVE_FIELD_PATTERNS = [
  'ssn',
  'social_security',
  'dob',
  'date_of_birth',
  'birth_date',
  'password',
  'secret',
  'pin'
];

/**
 * Maximum length for comments field before truncation
 */
const COMMENTS_MAX_LENGTH = 500;

/**
 * Current schema version for the contact payload
 */
const SCHEMA_VERSION = 2;

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * Find the first matching value from form data using priority key list
 *
 * @param {Object} formData - Parsed form data object
 * @param {string[]} keyList - Priority list of keys to search
 * @returns {string|null} First non-empty value found, or null
 */
function findFirstMatch(formData, keyList) {
  if (!formData || typeof formData !== 'object') {
    return null;
  }

  // Create lowercase key map for case-insensitive matching
  const lowerKeyMap = {};
  for (const [key, value] of Object.entries(formData)) {
    lowerKeyMap[key.toLowerCase()] = { originalKey: key, value };
  }

  // Search through priority list
  for (const searchKey of keyList) {
    const lowerSearchKey = searchKey.toLowerCase();

    // Exact match first
    if (lowerKeyMap[lowerSearchKey]) {
      const val = lowerKeyMap[lowerSearchKey].value;
      if (val !== null && val !== undefined && val !== '') {
        return String(val).trim();
      }
    }
  }

  return null;
}

/**
 * Find the first matching value using partial key matching (contains)
 *
 * @param {Object} formData - Parsed form data object
 * @param {string[]} patterns - Patterns to search for in keys
 * @returns {string|null} First non-empty value found, or null
 */
function findFirstMatchPartial(formData, patterns) {
  if (!formData || typeof formData !== 'object') {
    return null;
  }

  for (const pattern of patterns) {
    for (const [key, value] of Object.entries(formData)) {
      if (key.toLowerCase().includes(pattern.toLowerCase())) {
        if (value !== null && value !== undefined && value !== '') {
          return String(value).trim();
        }
      }
    }
  }

  return null;
}

/**
 * Check if a field key contains a sensitive pattern
 *
 * @param {string} key - Field key to check
 * @returns {boolean} True if key matches a sensitive pattern
 */
function isSensitiveField(key) {
  const lowerKey = key.toLowerCase();
  return SENSITIVE_FIELD_PATTERNS.some(pattern => lowerKey.includes(pattern));
}

/**
 * Truncate a string to max length with ellipsis
 *
 * @param {string} text - Text to truncate
 * @param {number} maxLength - Maximum length
 * @returns {string} Truncated text
 */
function truncateText(text, maxLength) {
  if (!text || text.length <= maxLength) {
    return text;
  }
  return text.substring(0, maxLength) + '...';
}

// ============================================================================
// MAIN EXTRACTION FUNCTIONS
// ============================================================================

/**
 * Extract canonical contact information and comments from form data
 *
 * @param {Object} formData - Parsed form data object
 * @returns {Object} { contact: CanonicalContact, comments: string|null }
 */
function extractCanonicalContact(formData) {
  if (!formData || typeof formData !== 'object') {
    return {
      contact: createEmptyContact(),
      comments: null
    };
  }

  // Extract name parts
  const firstName = findFirstMatch(formData, FIRST_NAME_KEYS);
  const middleName = findFirstMatch(formData, MIDDLE_NAME_KEYS);
  const lastName = findFirstMatch(formData, LAST_NAME_KEYS);

  // Compute full name from parts
  const nameParts = [firstName, middleName, lastName].filter(Boolean);
  const nameFull = nameParts.length > 0 ? nameParts.join(' ') : null;

  // Extract address parts
  const street = findFirstMatch(formData, STREET_KEYS);
  const unit = findFirstMatch(formData, UNIT_KEYS);
  const city = findFirstMatch(formData, CITY_KEYS);
  const state = findFirstMatch(formData, STATE_KEYS);
  const zip = findFirstMatch(formData, ZIP_KEYS);

  // Compute address text
  const addressText = formatAddressText(street, unit, city, state, zip);

  // Extract email - try exact keys first, then partial match
  let email = findFirstMatch(formData, EMAIL_KEYS);
  if (!email) {
    // Fallback: find any field containing 'email' with @ in value
    for (const [key, value] of Object.entries(formData)) {
      if (key.toLowerCase().includes('email') && value && String(value).includes('@')) {
        email = String(value).trim();
        break;
      }
    }
  }

  // Extract phone - try exact keys first, then partial match
  let phone = findFirstMatch(formData, PHONE_KEYS);
  if (!phone) {
    phone = findFirstMatchPartial(formData, ['phone', 'mobile', 'cell']);
  }

  // Extract comments
  let comments = findFirstMatch(formData, COMMENTS_KEYS);
  if (comments) {
    comments = truncateText(comments, COMMENTS_MAX_LENGTH);
  }

  // Build contact object
  const contact = {
    first_name: firstName,
    middle_name: middleName,
    last_name: lastName,
    name_full: nameFull,
    email: email,
    phone: phone,
    address: {
      street: street,
      unit: unit,
      city: city,
      state: state,
      zip: zip
    },
    address_text: addressText
  };

  return {
    contact,
    comments
  };
}

/**
 * Create an empty contact object with all null values
 *
 * @returns {Object} Empty contact object
 */
function createEmptyContact() {
  return {
    first_name: null,
    middle_name: null,
    last_name: null,
    name_full: null,
    email: null,
    phone: null,
    address: {
      street: null,
      unit: null,
      city: null,
      state: null,
      zip: null
    },
    address_text: null
  };
}

/**
 * Format address parts into a single text string
 *
 * @param {string|null} street - Street address
 * @param {string|null} unit - Unit/apartment
 * @param {string|null} city - City
 * @param {string|null} state - State
 * @param {string|null} zip - ZIP code
 * @returns {string|null} Formatted address or null if no parts
 */
function formatAddressText(street, unit, city, state, zip) {
  const parts = [];

  // Street + Unit
  if (street) {
    if (unit) {
      parts.push(`${street}, ${unit}`);
    } else {
      parts.push(street);
    }
  } else if (unit) {
    parts.push(unit);
  }

  // City, State ZIP
  const cityStateZip = [];
  if (city) cityStateZip.push(city);
  if (state) {
    if (cityStateZip.length > 0) {
      cityStateZip[cityStateZip.length - 1] += ',';
    }
    cityStateZip.push(state);
  }
  if (zip) {
    cityStateZip.push(zip);
  }

  if (cityStateZip.length > 0) {
    parts.push(cityStateZip.join(' '));
  }

  return parts.length > 0 ? parts.join(', ') : null;
}

/**
 * Filter sensitive fields from form data for email display
 *
 * @param {Object} formData - Parsed form data object
 * @returns {Object} Form data with sensitive fields removed
 */
function filterSensitiveFields(formData) {
  if (!formData || typeof formData !== 'object') {
    return formData;
  }

  const filtered = {};
  for (const [key, value] of Object.entries(formData)) {
    if (!isSensitiveField(key)) {
      filtered[key] = value;
    }
  }
  return filtered;
}

/**
 * Get the current schema version
 *
 * @returns {number} Schema version number
 */
function getSchemaVersion() {
  return SCHEMA_VERSION;
}

// ============================================================================
// EXPORTS
// ============================================================================

module.exports = {
  // Main extraction function
  extractCanonicalContact,

  // Helper functions
  createEmptyContact,
  formatAddressText,
  filterSensitiveFields,
  findFirstMatch,
  isSensitiveField,
  truncateText,
  getSchemaVersion,

  // Constants (for testing/extension)
  FIRST_NAME_KEYS,
  MIDDLE_NAME_KEYS,
  LAST_NAME_KEYS,
  STREET_KEYS,
  UNIT_KEYS,
  CITY_KEYS,
  STATE_KEYS,
  ZIP_KEYS,
  EMAIL_KEYS,
  PHONE_KEYS,
  COMMENTS_KEYS,
  SENSITIVE_FIELD_PATTERNS,
  COMMENTS_MAX_LENGTH,
  SCHEMA_VERSION
};
