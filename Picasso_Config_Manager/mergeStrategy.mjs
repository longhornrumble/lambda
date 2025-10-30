/**
 * Config Merge Strategy Module
 * Implements section-based editing to preserve read-only sections
 */

/**
 * Editable sections that can be modified through the config builder
 */
const EDITABLE_SECTIONS = [
  'programs',
  'conversational_forms',
  'cta_definitions',
  'conversation_branches',
  'content_showcase',
  'cta_settings',
];

/**
 * Read-only sections that should be preserved during merge
 */
const READ_ONLY_SECTIONS = [
  'branding',
  'features',
  'quick_help',
  'action_chips',
  'widget_behavior',
  'aws',
  'card_inventory',
];

/**
 * Metadata fields that should be updated during merge
 */
const METADATA_FIELDS = [
  'tenant_id',
  'version',
  'chat_title',
  'company_name',
  'last_updated',
];

/**
 * Merge edited sections into the base configuration
 * Preserves read-only sections and updates metadata
 *
 * @param {Object} baseConfig - The current full configuration from S3
 * @param {Object} editedSections - Object containing only the edited sections
 * @returns {Object} The merged configuration
 */
export function mergeConfigSections(baseConfig, editedSections) {
  // Start with the base config
  const merged = { ...baseConfig };

  // Update editable sections if provided
  EDITABLE_SECTIONS.forEach(section => {
    if (editedSections.hasOwnProperty(section)) {
      merged[section] = editedSections[section];
    }
  });

  // Update metadata fields if provided
  METADATA_FIELDS.forEach(field => {
    if (editedSections.hasOwnProperty(field)) {
      merged[field] = editedSections[field];
    }
  });

  // Ensure tenant_id and version are preserved from base
  merged.tenant_id = baseConfig.tenant_id;
  if (!merged.version) {
    merged.version = baseConfig.version || '1.3';
  }

  // Add last_updated timestamp
  merged.last_updated = new Date().toISOString();

  return merged;
}

/**
 * Extract only editable sections from a full config
 * Useful for sending only the editable parts to the frontend
 *
 * @param {Object} fullConfig - The full configuration object
 * @returns {Object} Object containing only editable sections and metadata
 */
export function extractEditableSections(fullConfig) {
  const editable = {};

  // Include metadata
  METADATA_FIELDS.forEach(field => {
    if (fullConfig.hasOwnProperty(field)) {
      editable[field] = fullConfig[field];
    }
  });

  // Include editable sections
  EDITABLE_SECTIONS.forEach(section => {
    if (fullConfig.hasOwnProperty(section)) {
      editable[section] = fullConfig[section];
    }
  });

  return editable;
}

/**
 * Validate that edited sections only contain allowed sections
 *
 * @param {Object} editedSections - Object containing edited sections
 * @returns {Object} Validation result with isValid and errors
 */
export function validateEditedSections(editedSections) {
  const errors = [];
  const allowedKeys = [...EDITABLE_SECTIONS, ...METADATA_FIELDS];

  // Check for disallowed sections
  const editedKeys = Object.keys(editedSections);
  const disallowedKeys = editedKeys.filter(
    key => !allowedKeys.includes(key) && !READ_ONLY_SECTIONS.includes(key)
  );

  if (disallowedKeys.length > 0) {
    errors.push(`Disallowed sections in edited config: ${disallowedKeys.join(', ')}`);
  }

  // Check for attempts to edit read-only sections
  const readOnlyAttempts = editedKeys.filter(key => READ_ONLY_SECTIONS.includes(key));
  if (readOnlyAttempts.length > 0) {
    errors.push(`Cannot edit read-only sections: ${readOnlyAttempts.join(', ')}`);
  }

  return {
    isValid: errors.length === 0,
    errors,
  };
}

/**
 * Get information about section structure
 *
 * @returns {Object} Object containing section categorization
 */
export function getSectionInfo() {
  return {
    editable: EDITABLE_SECTIONS,
    readOnly: READ_ONLY_SECTIONS,
    metadata: METADATA_FIELDS,
  };
}

/**
 * Deep clone an object (simple implementation for config objects)
 *
 * @param {Object} obj - Object to clone
 * @returns {Object} Cloned object
 */
export function deepClone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

/**
 * Merge multiple section updates into a single config
 * Useful when applying multiple changes in sequence
 *
 * @param {Object} baseConfig - The current full configuration
 * @param {Array<Object>} sectionUpdates - Array of section update objects
 * @returns {Object} The merged configuration
 */
export function mergeMultipleSectionUpdates(baseConfig, sectionUpdates) {
  let merged = deepClone(baseConfig);

  sectionUpdates.forEach(update => {
    merged = mergeConfigSections(merged, update);
  });

  return merged;
}

/**
 * Check if a section is editable
 *
 * @param {string} sectionName - Name of the section to check
 * @returns {boolean} True if the section is editable
 */
export function isEditableSection(sectionName) {
  return EDITABLE_SECTIONS.includes(sectionName);
}

/**
 * Check if a section is read-only
 *
 * @param {string} sectionName - Name of the section to check
 * @returns {boolean} True if the section is read-only
 */
export function isReadOnlySection(sectionName) {
  return READ_ONLY_SECTIONS.includes(sectionName);
}

/**
 * Generate a diff between two configs showing what changed
 *
 * @param {Object} oldConfig - The old configuration
 * @param {Object} newConfig - The new configuration
 * @returns {Object} Object describing the changes
 */
export function generateConfigDiff(oldConfig, newConfig) {
  const diff = {
    metadata_changes: {},
    section_changes: {},
    has_changes: false,
  };

  // Check metadata changes
  METADATA_FIELDS.forEach(field => {
    if (oldConfig[field] !== newConfig[field]) {
      diff.metadata_changes[field] = {
        old: oldConfig[field],
        new: newConfig[field],
      };
      diff.has_changes = true;
    }
  });

  // Check editable section changes
  EDITABLE_SECTIONS.forEach(section => {
    const oldSection = oldConfig[section] || {};
    const newSection = newConfig[section] || {};

    const oldKeys = Object.keys(oldSection);
    const newKeys = Object.keys(newSection);

    if (oldKeys.length !== newKeys.length ||
        JSON.stringify(oldSection) !== JSON.stringify(newSection)) {
      diff.section_changes[section] = {
        old_count: oldKeys.length,
        new_count: newKeys.length,
        added: newKeys.filter(k => !oldKeys.includes(k)),
        removed: oldKeys.filter(k => !newKeys.includes(k)),
        modified: newKeys.filter(k =>
          oldKeys.includes(k) &&
          JSON.stringify(oldSection[k]) !== JSON.stringify(newSection[k])
        ),
      };
      diff.has_changes = true;
    }
  });

  return diff;
}
