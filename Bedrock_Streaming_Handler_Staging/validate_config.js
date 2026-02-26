/**
 * Config Validation for Dynamic CTA Selection
 *
 * Validates tenant config has sufficient CTA coverage for all slots,
 * topic tags reference the vocabulary, and forms are properly linked.
 *
 * Usage:
 *   node validate_config.js <path-to-config.json>
 *   node validate_config.js  (reads from stdin)
 */

function validateDynamicCTAConfig(config) {
  const errors = [];
  const warnings = [];

  const ctaDefs = config.cta_definitions || {};
  const topicVocab = config.topic_vocabulary || {};
  const forms = config.conversational_forms || {};
  const flags = config.feature_flags || {};

  // Check feature flag
  if (!flags.DYNAMIC_CTA_SELECTION) {
    warnings.push('DYNAMIC_CTA_SELECTION flag is not true — dynamic selection will not activate');
  }

  // Check topic vocabulary exists and has minimum entries
  const vocabKeys = Object.keys(topicVocab);
  if (vocabKeys.length === 0) {
    errors.push('topic_vocabulary is missing or empty');
  } else if (vocabKeys.length < 5) {
    warnings.push(`topic_vocabulary has only ${vocabKeys.length} topics (recommend 5+)`);
  }

  // Check each vocabulary entry has keyword variants
  for (const [topicId, variants] of Object.entries(topicVocab)) {
    if (!Array.isArray(variants) || variants.length === 0) {
      errors.push(`topic_vocabulary["${topicId}"] has no keyword variants`);
    }
  }

  // Analyze CTA coverage
  const aiAvailable = [];
  const slotCoverage = { action: [], info: [], lateral: [] };
  const lateralEligible = [];
  const vocabSet = new Set(vocabKeys);

  for (const [id, cta] of Object.entries(ctaDefs)) {
    if (!cta.ai_available) continue;
    aiAvailable.push(id);

    const meta = cta.selection_metadata;
    if (!meta) {
      errors.push(`CTA "${id}" has ai_available=true but no selection_metadata`);
      continue;
    }

    // Check topic_tags reference vocabulary
    for (const tag of (meta.topic_tags || [])) {
      if (!vocabSet.has(tag)) {
        errors.push(`CTA "${id}" has topic_tag "${tag}" not in topic_vocabulary`);
      }
    }

    // Check slot eligibility
    const eligibility = meta.slot_eligibility || [];
    if (eligibility.length === 0) {
      warnings.push(`CTA "${id}" has no slot_eligibility — will never be selected`);
    }
    for (const slot of eligibility) {
      if (slotCoverage[slot]) {
        slotCoverage[slot].push(id);
      } else {
        errors.push(`CTA "${id}" has invalid slot_eligibility value "${slot}"`);
      }
    }

    // Track lateral eligible
    if (meta.lateral_eligible || eligibility.includes('lateral')) {
      lateralEligible.push(id);
    }

    // Check form CTAs have matching form definitions
    if (cta.action === 'start_form' && cta.formId) {
      if (!forms[cta.formId]) {
        errors.push(`CTA "${id}" references formId "${cta.formId}" but no matching form definition exists`);
      }
    }
  }

  // Check minimum slot coverage
  if (aiAvailable.length === 0) {
    errors.push('No CTAs have ai_available=true — dynamic selection will return empty');
  }

  for (const [slot, ctas] of Object.entries(slotCoverage)) {
    if (ctas.length === 0) {
      errors.push(`No CTAs eligible for "${slot}" slot`);
    } else if (ctas.length < 2) {
      warnings.push(`Only ${ctas.length} CTA eligible for "${slot}" slot (recommend 2+)`);
    }
  }

  // Check lateral escape coverage
  if (lateralEligible.length < 2) {
    warnings.push(`Only ${lateralEligible.length} lateral-eligible CTAs (recommend 2+ for escape routes)`);
  }

  return {
    valid: errors.length === 0,
    errors,
    warnings,
    summary: {
      total_ctas: Object.keys(ctaDefs).length,
      ai_available: aiAvailable.length,
      topic_count: vocabKeys.length,
      slot_coverage: {
        action: slotCoverage.action.length,
        info: slotCoverage.info.length,
        lateral: slotCoverage.lateral.length
      },
      lateral_eligible: lateralEligible.length,
      form_ctas: Object.values(ctaDefs).filter(c => c.action === 'start_form').length
    }
  };
}

// CLI usage
if (require.main === module) {
  const fs = require('fs');
  const path = process.argv[2];

  let configJson;
  if (path) {
    configJson = fs.readFileSync(path, 'utf8');
  } else {
    // Read from stdin
    configJson = fs.readFileSync(0, 'utf8');
  }

  const config = JSON.parse(configJson);
  const result = validateDynamicCTAConfig(config);

  console.log('\n=== Dynamic CTA Config Validation ===\n');
  console.log('Summary:');
  console.log(`  Total CTAs: ${result.summary.total_ctas}`);
  console.log(`  AI-available: ${result.summary.ai_available}`);
  console.log(`  Topic vocabulary: ${result.summary.topic_count} topics`);
  console.log(`  Slot coverage: action=${result.summary.slot_coverage.action}, info=${result.summary.slot_coverage.info}, lateral=${result.summary.slot_coverage.lateral}`);
  console.log(`  Lateral-eligible: ${result.summary.lateral_eligible}`);

  if (result.errors.length > 0) {
    console.log(`\n❌ ERRORS (${result.errors.length}):`);
    result.errors.forEach(e => console.log(`  - ${e}`));
  }

  if (result.warnings.length > 0) {
    console.log(`\n⚠️  WARNINGS (${result.warnings.length}):`);
    result.warnings.forEach(w => console.log(`  - ${w}`));
  }

  if (result.valid) {
    console.log('\n✅ Config is valid for dynamic CTA selection');
  } else {
    console.log('\n❌ Config has errors that must be fixed');
    process.exit(1);
  }
}

module.exports = { validateDynamicCTAConfig };
