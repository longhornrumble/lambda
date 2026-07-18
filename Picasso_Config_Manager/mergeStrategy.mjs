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
  'branding',
  'features',
  'quick_help',
  'action_chips',
  'widget_behavior',
  'aws',
  'bedrock_instructions',
  'feature_flags',
  'intent_definitions',
  'topic_definitions',
  'form_settings',
  'monitor',
  'notification_settings',
  'messenger_behavior',
];

/**
 * Read-only sections that should be preserved during merge
 */
const READ_ONLY_SECTIONS = [
  'card_inventory',
];

/**
 * Metadata fields that should be updated during merge
 */
const METADATA_FIELDS = [
  'tenant_id',
  'tenant_hash',
  'active',
  'version',
  'chat_title',
  'organization_name',
  'company_name',
  'last_updated',
  'chat_subtitle',
  'welcome_message',
  'subscription_tier',
  'tone_prompt',
  'model_id',
  'callout_text',
  'generated_at',
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
 * Form field types the Picasso widget can render (FormFieldPrompt.jsx).
 * The Config Builder authors a subset of these (it does not offer
 * phone_with_consent), so builder-authored configs always pass. Anything
 * outside this set reaches the widget as a prompt with NO input control —
 * a dead-end form (the BRI071351 'boolean' incident, 2026-07-18). This
 * Lambda is the only write path to the config bucket, so this is the last
 * gate before a bad shape can reach live forms.
 */
const WIDGET_FIELD_TYPES = [
  'text', 'email', 'phone', 'select', 'textarea', 'number', 'date',
  'name', 'address', 'phone_with_consent',
];

// Composite types render via CompositeFieldGroup, which requires subfields.
const COMPOSITE_FIELD_TYPES = ['name', 'address', 'phone_with_consent'];

/**
 * Validate that every form field is a shape the widget can render.
 * Appends human-readable messages to `errors`. Tolerates missing/odd
 * structure (forward-compatible reads) — only flags fields that would
 * concretely break rendering.
 */
function validateFormShapes(forms, errors) {
  for (const [formId, form] of Object.entries(forms || {})) {
    for (const field of (form && form.fields) || []) {
      const t = field?.type;
      const id = field?.id ?? '(no id)';
      if (!WIDGET_FIELD_TYPES.includes(t)) {
        errors.push(
          `Form "${formId}" field "${id}": unsupported type "${t}" — the widget cannot render it. Supported: ${WIDGET_FIELD_TYPES.join(', ')}`
        );
      }
      if (COMPOSITE_FIELD_TYPES.includes(t) && !(Array.isArray(field.subfields) && field.subfields.length > 0)) {
        errors.push(
          `Form "${formId}" field "${id}": composite type "${t}" requires a non-empty subfields array`
        );
      }
      if (t === 'select' && !(Array.isArray(field.options) && field.options.length > 0)) {
        errors.push(
          `Form "${formId}" field "${id}": select requires a non-empty options array`
        );
      }
    }
  }
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

  // Check for unknown sections (warn but don't block — frontend may send full config)
  const editedKeys = Object.keys(editedSections);
  const unknownKeys = editedKeys.filter(
    key => !allowedKeys.includes(key) && !READ_ONLY_SECTIONS.includes(key)
  );

  if (unknownKeys.length > 0) {
    console.warn(`Unknown sections in edited config (will be ignored during merge): ${unknownKeys.join(', ')}`);
  }

  // Only block attempts to edit read-only sections
  const readOnlyAttempts = editedKeys.filter(key => READ_ONLY_SECTIONS.includes(key));
  if (readOnlyAttempts.length > 0) {
    errors.push(`Cannot edit read-only sections: ${readOnlyAttempts.join(', ')}`);
  }

  // Block form field shapes the widget cannot render (only when the write
  // touches conversational_forms — untouched sections are never re-validated).
  if (editedSections.conversational_forms) {
    validateFormShapes(editedSections.conversational_forms, errors);
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
