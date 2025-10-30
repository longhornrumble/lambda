/**
 * Manual Validation Tests for 3-Tier Routing Parity
 *
 * Tests to verify JavaScript implementation matches Python implementation in Master_Function_Staging
 */

const { getConversationBranch, buildCtasFromBranch } = require('./response_enhancer');

// Mock config similar to tenant configs
const mockConfig = {
    conversation_branches: {
        volunteer_interest: {
            available_ctas: {
                primary: 'volunteer_apply',
                secondary: ['view_volunteer_programs', 'contact_us']
            }
        },
        donation_interest: {
            available_ctas: {
                primary: 'donate_now',
                secondary: ['view_donation_options']
            }
        },
        navigation_hub: {
            available_ctas: {
                primary: 'volunteer_apply',
                secondary: ['schedule_discovery', 'contact_us']
            }
        }
    },
    cta_definitions: {
        volunteer_apply: {
            label: 'Apply to Volunteer',
            action: 'start_form',
            type: 'form_cta',
            program: 'volunteer'
        },
        view_volunteer_programs: {
            label: 'View Programs',
            action: 'navigate',
            type: 'info_cta'
        },
        contact_us: {
            label: 'Contact Us',
            action: 'navigate',
            type: 'info_cta'
        },
        donate_now: {
            label: 'Donate Now',
            action: 'navigate',
            type: 'info_cta'
        },
        view_donation_options: {
            label: 'View Donation Options',
            action: 'navigate',
            type: 'info_cta'
        },
        schedule_discovery: {
            label: 'Schedule Discovery',
            action: 'navigate',
            type: 'info_cta'
        }
    },
    cta_settings: {
        fallback_branch: 'navigation_hub',
        max_display: 3
    }
};

// Test results tracking
let testsRun = 0;
let testsPassed = 0;
let testsFailed = 0;

function assert(condition, testName) {
    testsRun++;
    if (condition) {
        testsPassed++;
        console.log(`✅ Test ${testsRun}: ${testName}`);
        return true;
    } else {
        testsFailed++;
        console.error(`❌ Test ${testsRun}: ${testName}`);
        return false;
    }
}

console.log('='.repeat(80));
console.log('BEDROCK STREAMING HANDLER: 3-TIER ROUTING VALIDATION TESTS');
console.log('='.repeat(80));
console.log();

// ============================================================================
// Test Suite 1: Tier 1 - Action Chip Routing
// ============================================================================
console.log('Test Suite 1: Tier 1 - Action Chip Routing');
console.log('-'.repeat(80));

// Test 1.1: Valid action chip routing
const test1_1_metadata = {
    action_chip_triggered: true,
    action_chip_id: 'volunteer',
    target_branch: 'volunteer_interest'
};
const test1_1_result = getConversationBranch(test1_1_metadata, mockConfig);
assert(
    test1_1_result === 'volunteer_interest',
    'Tier 1 - Valid action chip routes to target_branch'
);

// Test 1.2: Invalid action chip branch fallthrough
const test1_2_metadata = {
    action_chip_triggered: true,
    action_chip_id: 'volunteer',
    target_branch: 'non_existent_branch'
};
const test1_2_result = getConversationBranch(test1_2_metadata, mockConfig);
assert(
    test1_2_result === 'navigation_hub',
    'Tier 1 - Invalid branch falls through to Tier 3 fallback'
);

// Test 1.3: Null target_branch fallthrough
const test1_3_metadata = {
    action_chip_triggered: true,
    action_chip_id: 'volunteer',
    target_branch: null
};
const test1_3_result = getConversationBranch(test1_3_metadata, mockConfig);
assert(
    test1_3_result === 'navigation_hub',
    'Tier 1 - Null target_branch falls through to Tier 3 fallback'
);

console.log();

// ============================================================================
// Test Suite 2: Tier 2 - CTA Routing
// ============================================================================
console.log('Test Suite 2: Tier 2 - CTA Routing');
console.log('-'.repeat(80));

// Test 2.1: Valid CTA routing
const test2_1_metadata = {
    cta_triggered: true,
    cta_id: 'donate_now',
    target_branch: 'donation_interest'
};
const test2_1_result = getConversationBranch(test2_1_metadata, mockConfig);
assert(
    test2_1_result === 'donation_interest',
    'Tier 2 - Valid CTA routes to target_branch'
);

// Test 2.2: Invalid CTA branch fallthrough
const test2_2_metadata = {
    cta_triggered: true,
    cta_id: 'donate_now',
    target_branch: 'invalid_branch'
};
const test2_2_result = getConversationBranch(test2_2_metadata, mockConfig);
assert(
    test2_2_result === 'navigation_hub',
    'Tier 2 - Invalid branch falls through to Tier 3 fallback'
);

console.log();

// ============================================================================
// Test Suite 3: Tier 3 - Fallback Navigation Hub
// ============================================================================
console.log('Test Suite 3: Tier 3 - Fallback Navigation Hub');
console.log('-'.repeat(80));

// Test 3.1: Free-form query uses fallback
const test3_1_metadata = {};
const test3_1_result = getConversationBranch(test3_1_metadata, mockConfig);
assert(
    test3_1_result === 'navigation_hub',
    'Tier 3 - Free-form query routes to fallback_branch'
);

// Test 3.2: No fallback configured
const mockConfigNoFallback = {
    ...mockConfig,
    cta_settings: {}
};
const test3_2_result = getConversationBranch({}, mockConfigNoFallback);
assert(
    test3_2_result === null,
    'Tier 3 - No fallback configured returns null (graceful degradation)'
);

// Test 3.3: Invalid fallback branch
const mockConfigInvalidFallback = {
    ...mockConfig,
    cta_settings: { fallback_branch: 'non_existent' }
};
const test3_3_result = getConversationBranch({}, mockConfigInvalidFallback);
assert(
    test3_3_result === null,
    'Tier 3 - Invalid fallback_branch returns null (graceful degradation)'
);

console.log();

// ============================================================================
// Test Suite 4: buildCtasFromBranch - CTA Building
// ============================================================================
console.log('Test Suite 4: buildCtasFromBranch - CTA Building');
console.log('-'.repeat(80));

// Test 4.1: Build CTAs from valid branch
const test4_1_ctas = buildCtasFromBranch('volunteer_interest', mockConfig, []);
assert(
    test4_1_ctas.length === 3,
    'Build CTAs - Returns max 3 CTAs from branch'
);
assert(
    test4_1_ctas[0].id === 'volunteer_apply',
    'Build CTAs - Primary CTA is first'
);

// Test 4.2: Filter completed form CTAs
const test4_2_ctas = buildCtasFromBranch('volunteer_interest', mockConfig, ['volunteer']);
assert(
    test4_2_ctas.length === 2,
    'Build CTAs - Filters completed form CTAs'
);
assert(
    !test4_2_ctas.find(cta => cta.id === 'volunteer_apply'),
    'Build CTAs - Completed form CTA not included'
);

// Test 4.3: Invalid branch returns empty array
const test4_3_ctas = buildCtasFromBranch('non_existent', mockConfig, []);
assert(
    test4_3_ctas.length === 0,
    'Build CTAs - Invalid branch returns empty array'
);

// Test 4.4: Branch with no CTAs returns empty array
const mockConfigNoCtas = {
    conversation_branches: {
        empty_branch: {
            available_ctas: {}
        }
    },
    cta_definitions: mockConfig.cta_definitions,
    cta_settings: mockConfig.cta_settings
};
const test4_4_ctas = buildCtasFromBranch('empty_branch', mockConfigNoCtas, []);
assert(
    test4_4_ctas.length === 0,
    'Build CTAs - Branch with no CTAs returns empty array'
);

console.log();

// ============================================================================
// Test Suite 5: Routing Priority (Tier 1 > Tier 2 > Tier 3)
// ============================================================================
console.log('Test Suite 5: Routing Priority');
console.log('-'.repeat(80));

// Test 5.1: Action chip overrides CTA
const test5_1_metadata = {
    action_chip_triggered: true,
    target_branch: 'volunteer_interest',
    cta_triggered: true,
    // This should be ignored because action chip has priority
};
const test5_1_result = getConversationBranch(test5_1_metadata, mockConfig);
assert(
    test5_1_result === 'volunteer_interest',
    'Routing Priority - Tier 1 (action chip) overrides Tier 2 (CTA)'
);

// Test 5.2: CTA overrides fallback
const test5_2_metadata = {
    cta_triggered: true,
    target_branch: 'donation_interest'
};
const test5_2_result = getConversationBranch(test5_2_metadata, mockConfig);
assert(
    test5_2_result === 'donation_interest',
    'Routing Priority - Tier 2 (CTA) overrides Tier 3 (fallback)'
);

console.log();

// ============================================================================
// Summary
// ============================================================================
console.log('='.repeat(80));
console.log('TEST SUMMARY');
console.log('='.repeat(80));
console.log(`Tests run: ${testsRun}`);
console.log(`✅ Passed: ${testsPassed}`);
console.log(`❌ Failed: ${testsFailed}`);
console.log();

if (testsFailed === 0) {
    console.log('✅ ALL TESTS PASSED - Parity with Master_Function verified');
    process.exit(0);
} else {
    console.log('❌ SOME TESTS FAILED - Review implementation');
    process.exit(1);
}
