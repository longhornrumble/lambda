/**
 * Response Enhancer for Conversational Form CTAs
 *
 * Simple context bridge that detects conversation topics and injects
 * appropriate CTAs based on configuration. No complex scoring or strategies.
 */

const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');

// Initialize S3 client
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

// Simple cache for tenant configs (5 minute TTL)
const configCache = {};
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

/**
 * Resolve tenant hash to tenant ID via S3 mapping
 */
async function resolveTenantHash(tenantHash) {
    try {
        const mappingKey = `mappings/${tenantHash}.json`;
        const command = new GetObjectCommand({
            Bucket: process.env.CONFIG_BUCKET || process.env.S3_CONFIG_BUCKET || 'myrecruiter-picasso',
            Key: mappingKey
        });

        const response = await s3Client.send(command);
        const mappingData = JSON.parse(await response.Body.transformToString());

        return mappingData.tenant_id;
    } catch (error) {
        console.error(`Error resolving tenant hash ${tenantHash}:`, error);
        return null;
    }
}

/**
 * Load tenant configuration from S3
 */
async function loadTenantConfig(tenantHash) {
    // Check cache first
    const cacheKey = `config_${tenantHash}`;
    if (configCache[cacheKey]) {
        const { data, timestamp } = configCache[cacheKey];
        if (Date.now() - timestamp < CACHE_TTL) {
            console.log(`Using cached config for ${tenantHash}`);
            return data;
        }
    }

    try {
        // Resolve hash to ID
        const tenantId = await resolveTenantHash(tenantHash);
        if (!tenantId) {
            console.error('Could not resolve tenant hash to ID');
            return { conversation_branches: {}, cta_definitions: {} };
        }

        // Load config from S3
        const configKey = `tenants/${tenantId}/${tenantId}-config.json`;
        const command = new GetObjectCommand({
            Bucket: process.env.CONFIG_BUCKET || process.env.S3_CONFIG_BUCKET || 'myrecruiter-picasso',
            Key: configKey
        });

        const response = await s3Client.send(command);
        const config = JSON.parse(await response.Body.transformToString());

        // Extract relevant sections
        const result = {
            conversation_branches: config.conversation_branches || {},
            cta_definitions: config.cta_definitions || {},
            conversational_forms: config.conversational_forms || {},
            cta_settings: config.cta_settings || {}  // Required for Tier 3 fallback routing
        };

        // Cache the config
        configCache[cacheKey] = {
            data: result,
            timestamp: Date.now()
        };

        console.log(`Loaded config for ${tenantHash} (${tenantId})`);
        return result;
    } catch (error) {
        console.error('Error loading tenant config:', error);
        return { conversation_branches: {}, cta_definitions: {} };
    }
}

/**
 * Get conversation branch using 3-tier routing hierarchy (PRD: Action Chips Explicit Routing)
 *
 * This implements explicit routing metadata, eliminating keyword matching for action chips and CTAs.
 *
 * Tier 1: Explicit action chip routing (chip.target_branch)
 * Tier 2: Explicit CTA routing (cta.target_branch)
 * Tier 3: Fallback navigation hub (cta_settings.fallback_branch)
 *
 * @param {Object} routingMetadata - Routing metadata from frontend
 * @param {Object} config - Tenant configuration
 * @returns {string|null} - Branch name to use for CTA selection, or null if no match
 */
function getConversationBranch(routingMetadata, config) {
    const branches = config.conversation_branches || {};
    const ctaSettings = config.cta_settings || {};

    // TIER 1: Explicit action chip routing
    if (routingMetadata.action_chip_triggered) {
        const targetBranch = routingMetadata.target_branch;
        if (targetBranch && branches[targetBranch]) {
            console.log(`[Tier 1] Routing via action chip to branch: ${targetBranch}`);
            return targetBranch;
        }
        if (targetBranch) {
            console.log(`[Tier 1] Invalid target_branch: ${targetBranch}, falling back to next tier`);
        }
    }

    // TIER 2: Explicit CTA routing
    if (routingMetadata.cta_triggered) {
        const targetBranch = routingMetadata.target_branch;
        if (targetBranch && branches[targetBranch]) {
            console.log(`[Tier 2] Routing via CTA to branch: ${targetBranch}`);
            return targetBranch;
        }
        if (targetBranch) {
            console.log(`[Tier 2] Invalid target_branch: ${targetBranch}, falling back to next tier`);
        }
    }

    // TIER 3: Fallback navigation hub
    const fallbackBranch = ctaSettings.fallback_branch;
    if (fallbackBranch && branches[fallbackBranch]) {
        console.log(`[Tier 3] Routing to fallback branch: ${fallbackBranch}`);
        return fallbackBranch;
    }

    // No routing match - graceful degradation (backward compatibility)
    if (fallbackBranch) {
        console.log(`[Tier 3] Fallback branch '${fallbackBranch}' not found in conversation_branches`);
    } else {
        console.log('[Tier 3] No fallback_branch configured - no CTAs will be shown');
    }

    return null;
}

/**
 * Build CTA cards for a specific conversation branch (no keyword matching)
 *
 * This function implements explicit CTA selection based on a pre-determined branch,
 * bypassing the keyword detection logic.
 *
 * @param {string} branchName - Name of the conversation branch to use
 * @param {Object} config - Tenant configuration
 * @param {Array} completedForms - List of completed form IDs to filter out
 * @returns {Array} - CTA cards to display (max 3)
 */
function buildCtasFromBranch(branchName, config, completedForms = []) {
    const branches = config.conversation_branches || {};
    const ctaDefinitions = config.cta_definitions || {};

    if (!branchName || !branches[branchName]) {
        console.log(`[CTA Builder] Branch '${branchName}' not found`);
        return [];
    }

    const branch = branches[branchName];
    const availableCtas = branch.available_ctas || {};
    const ctas = [];

    // Add primary CTA if defined
    const primaryCtaId = availableCtas.primary;
    if (primaryCtaId && ctaDefinitions[primaryCtaId]) {
        const primaryCta = ctaDefinitions[primaryCtaId];

        // Check if this is a form CTA
        const isFormCta = (
            primaryCta.action === 'start_form' ||
            primaryCta.action === 'form_trigger' ||
            primaryCta.type === 'form_cta'
        );

        if (isFormCta) {
            // Extract program from CTA
            let program = primaryCta.program || primaryCta.program_id;
            const formId = primaryCta.formId || primaryCta.form_id;

            // Map formIds to programs if needed
            if (!program && formId) {
                if (formId === 'lb_apply') {
                    program = 'lovebox';
                } else if (formId === 'dd_apply') {
                    program = 'daretodream';
                }
            }

            // Filter if completed
            if (program && completedForms.includes(program)) {
                console.log(`[CTA Builder] Filtering completed program: ${program}`);
            } else {
                ctas.push({ ...primaryCta, id: primaryCtaId });
            }
        } else {
            // Not a form CTA, always show
            ctas.push({ ...primaryCta, id: primaryCtaId });
        }
    }

    // Add secondary CTAs if defined
    const secondaryCtas = availableCtas.secondary || [];
    for (const ctaId of secondaryCtas) {
        if (!ctaDefinitions[ctaId]) {
            continue;
        }

        const cta = ctaDefinitions[ctaId];

        // Check if this is a form CTA
        const isFormCta = (
            cta.action === 'start_form' ||
            cta.action === 'form_trigger' ||
            cta.type === 'form_cta'
        );

        if (isFormCta) {
            // Extract program from CTA
            let program = cta.program || cta.program_id;
            const formId = cta.formId || cta.form_id;

            // Map formIds to programs if needed
            if (!program && formId) {
                if (formId === 'lb_apply') {
                    program = 'lovebox';
                } else if (formId === 'dd_apply') {
                    program = 'daretodream';
                }
            }

            // Filter if completed
            if (program && completedForms.includes(program)) {
                console.log(`[CTA Builder] Filtering completed program: ${program}`);
                continue;
            }
        }

        ctas.push({ ...cta, id: ctaId });
    }

    // Return max 3 CTAs
    const finalCtas = ctas.slice(0, 3);
    console.log(`[CTA Builder] Built ${finalCtas.length} CTAs for branch '${branchName}'`);
    return finalCtas;
}

/**
 * Detect conversation branch based on Bedrock response content
 *
 * DEPRECATED: This function uses keyword matching and should be replaced with explicit routing.
 * Kept for backward compatibility only.
 *
 * This is the "context bridge" - matches response to configuration
 */
function detectConversationBranch(bedrockResponse, userQuery, config, completedForms = []) {
    const { conversation_branches, cta_definitions } = config;

    // Check if user is engaged/interested
    const userEngaged = /\b(tell me|more|interested|how|what|when|where|apply|volunteer|help|can i|do you|does)\b/i.test(userQuery);
    if (!userEngaged) {
        console.log('User not engaged enough for CTAs');
        return null;
    }

    // Priority order for branch detection (broader topics first)
    const branchPriority = [
        'program_exploration',
        'volunteer_interest',
        'requirements_discussion',
        'lovebox_discussion',
        'daretodream_discussion'
    ];

    // Check branches in priority order
    for (const branchName of branchPriority) {
        const branch = conversation_branches?.[branchName];
        if (!branch || !branch.detection_keywords || !Array.isArray(branch.detection_keywords)) {
            continue;
        }

        // Check if any keywords match the response
        const matches = branch.detection_keywords.some(keyword =>
            bedrockResponse.toLowerCase().includes(keyword.toLowerCase())
        );

        if (matches) {
            console.log(`Detected branch: ${branchName}`);

            // Build CTA array from branch configuration
            const ctas = [];

            // Add primary CTA if defined and not completed
            if (branch.available_ctas?.primary) {
                const primaryCta = cta_definitions[branch.available_ctas.primary];
                if (primaryCta) {
                    // Check if this is a form CTA that's already been completed
                    const isFormCta = primaryCta.action === 'start_form' || primaryCta.action === 'form_trigger' || primaryCta.type === 'form_cta';

                    if (isFormCta) {
                        // Extract program from CTA - check multiple possible fields
                        // The program could be in: program, program_id, or derived from formId
                        let program = primaryCta.program || primaryCta.program_id;

                        if (!program) {
                            // Map formIds to programs
                            const formId = primaryCta.formId || primaryCta.form_id;
                            if (formId === 'lb_apply') program = 'lovebox';
                            else if (formId === 'dd_apply') program = 'daretodream';
                            else if (formId === 'volunteer_apply' || formId === 'volunteer_general') {
                                // Generic volunteer form - check branch context AND response content
                                if (branchName === 'lovebox_discussion' || bedrockResponse.toLowerCase().includes('love box')) {
                                    program = 'lovebox';
                                } else if (branchName === 'daretodream_discussion' || bedrockResponse.toLowerCase().includes('dare to dream')) {
                                    program = 'daretodream';
                                }
                            }
                        }

                        // Filter if user has completed this program
                        if (program && completedForms.includes(program)) {
                            console.log(`🚫 Filtering primary CTA for completed program: ${program} (branch: ${branchName}, formId: ${primaryCta.formId || primaryCta.form_id})`);
                        } else {
                            console.log(`✅ Adding primary CTA - program: ${program || 'none'}, completed: [${completedForms.join(',')}], formId: ${primaryCta.formId || primaryCta.form_id}`);
                            ctas.push({
                                ...primaryCta,
                                id: branch.available_ctas.primary
                            });
                        }
                    } else {
                        // Not a form CTA, always show
                        ctas.push({
                            ...primaryCta,
                            id: branch.available_ctas.primary
                        });
                    }
                }
            }

            // Add secondary CTAs if defined (user is engaged if branch was detected)
            if (branch.available_ctas?.secondary) {
                for (const ctaId of branch.available_ctas.secondary) {
                    const cta = cta_definitions[ctaId];
                    if (cta) {
                        // Check if this is a form CTA that's already been completed
                        const isFormCta = cta.action === 'start_form' || cta.action === 'form_trigger' || cta.type === 'form_cta';

                        if (isFormCta) {
                            // Extract program from CTA
                            let program = cta.program || cta.program_id;

                            if (!program) {
                                // Map formIds to programs
                                const formId = cta.formId || cta.form_id;
                                if (formId === 'lb_apply') program = 'lovebox';
                                else if (formId === 'dd_apply') program = 'daretodream';
                                else if (formId === 'volunteer_apply' || formId === 'volunteer_general') {
                                    // Generic volunteer form - check branch context AND response content
                                    if (branchName === 'lovebox_discussion' || bedrockResponse.toLowerCase().includes('love box')) {
                                        program = 'lovebox';
                                    } else if (branchName === 'daretodream_discussion' || bedrockResponse.toLowerCase().includes('dare to dream')) {
                                        program = 'daretodream';
                                    }
                                }
                            }

                            // Filter if user has completed this program
                            if (program && completedForms.includes(program)) {
                                console.log(`🚫 Filtering secondary CTA for completed program: ${program} (CTA: ${ctaId})`);
                            } else {
                                ctas.push({
                                    ...cta,
                                    id: ctaId
                                });
                            }
                        } else {
                            // Not a form CTA, always show
                            ctas.push({
                                ...cta,
                                id: ctaId
                            });
                        }
                    }
                }
            }

            // Return max 3 CTAs for clarity
            return {
                branch: branchName,
                ctas: ctas.slice(0, 3)
            };
        }
    }

    console.log('No matching branch found');
    return null;
}

/**
 * Check if a conversational form should be triggered
 * Based on v5 plan - forms triggered by specific phrases
 */
function checkFormTriggers(bedrockResponse, userQuery, config) {
    const { conversational_forms } = config;

    if (!conversational_forms) return null;

    // Check each form for trigger phrases
    for (const [formId, form] of Object.entries(conversational_forms)) {
        if (!form.enabled || !form.trigger_phrases) continue;

        // Check if user message contains trigger phrases
        const triggered = form.trigger_phrases.some(phrase =>
            userQuery.toLowerCase().includes(phrase.toLowerCase())
        );

        if (triggered) {
            console.log(`Form trigger detected: ${formId}`);
            return {
                formId: form.form_id || formId,
                title: form.title,
                description: form.description,
                ctaText: form.cta_text,
                fields: form.fields
            };
        }
    }

    return null;
}

/**
 * Main enhancement function - adds CTAs to Bedrock response
 */
async function enhanceResponse(bedrockResponse, userMessage, tenantHash, sessionContext = {}, routingMetadata = {}) {
    console.log('🔍 enhanceResponse called with:', {
        responseLength: bedrockResponse?.length,
        userMessage,
        tenantHash,
        sessionContext,
        routingMetadata,
        completedForms: sessionContext.completed_forms || [],
        suspendedForms: sessionContext.suspended_forms || [],
        programInterest: sessionContext.program_interest,
        responseSnippet: bedrockResponse?.substring(0, 100)
    });

    try {
        // Load tenant configuration
        const config = await loadTenantConfig(tenantHash);

        // Extract completed forms and suspended forms from session context
        const completedForms = sessionContext.completed_forms || [];
        const suspendedForms = sessionContext.suspended_forms || [];

        // ============================================================================
        // TIER 1-3: Explicit Routing (PRD: Action Chips Explicit Routing)
        // ============================================================================
        // Check for explicit routing metadata BEFORE keyword detection or form triggers
        const explicitBranch = getConversationBranch(routingMetadata, config);
        if (explicitBranch) {
            console.log(`[Explicit Routing] Using branch: ${explicitBranch}`);
            const ctas = buildCtasFromBranch(explicitBranch, config, completedForms);

            if (ctas.length > 0) {
                console.log(`[Explicit Routing] Returning ${ctas.length} CTAs from branch '${explicitBranch}'`);
                return {
                    message: bedrockResponse,
                    ctaButtons: ctas,
                    metadata: {
                        enhanced: true,
                        branch: explicitBranch,
                        routing_tier: 'explicit',
                        routing_method: routingMetadata.action_chip_triggered ? 'action_chip' :
                                       routingMetadata.cta_triggered ? 'cta' : 'fallback'
                    }
                };
            } else {
                console.log(`[Explicit Routing] Branch '${explicitBranch}' has no CTAs to display`);
            }
        }
        // ============================================================================
        // End of Explicit Routing - Continue to existing logic for backward compatibility
        // ============================================================================

        // PHASE 1B: If there are suspended forms, check if user is asking about a DIFFERENT program
        // This enables intelligent form switching UX
        if (suspendedForms.length > 0) {
            console.log(`[Phase 1B] 🔄 Suspended form detected: ${suspendedForms[0]}`);

            const conversationalForms = config.conversational_forms || {};

            // Check if user's message would trigger a different form
            const triggeredForm = checkFormTriggers(bedrockResponse, userMessage, config);

            if (triggeredForm) {
                const newFormId = triggeredForm.formId;
                const suspendedFormId = suspendedForms[0];

                // If user is asking about a DIFFERENT program, offer to switch
                if (newFormId !== suspendedFormId) {
                    console.log(`[Phase 1B] 🔀 Program switch detected! Suspended: ${suspendedFormId}, Interested in: ${newFormId}`);

                    // Get program names from form titles in config
                    const newProgramName = (triggeredForm.title || 'this program').replace(' Application', '');

                    // Get suspended form's title - need to find the config by matching form_id
                    let suspendedFormConfig = null;
                    for (const [configKey, formConfig] of Object.entries(conversationalForms)) {
                        if (formConfig.form_id === suspendedFormId || configKey === suspendedFormId) {
                            suspendedFormConfig = formConfig;
                            break;
                        }
                    }

                    if (!suspendedFormConfig) {
                        suspendedFormConfig = {};
                    }

                    let suspendedProgramName = (suspendedFormConfig.title || 'your application').replace(' Application', '');

                    // If user selected a program_interest in the volunteer form, use that instead of "Volunteer"
                    const programInterest = sessionContext.program_interest;
                    if (programInterest) {
                        const programMap = {
                            'lovebox': 'Love Box',
                            'daretodream': 'Dare to Dream',
                            'both': 'both programs',
                            'unsure': 'Volunteer'
                        };
                        suspendedProgramName = programMap[programInterest.toLowerCase()] || suspendedProgramName;
                        console.log(`[Phase 1B] 📝 User selected program_interest='${programInterest}', showing as '${suspendedProgramName}'`);
                    }

                    return {
                        message: bedrockResponse,
                        ctaButtons: [],  // No automatic CTAs - frontend will show switch options
                        metadata: {
                            enhanced: true,
                            program_switch_detected: true,
                            suspended_form: {
                                form_id: suspendedFormId,
                                program_name: suspendedProgramName
                            },
                            new_form_of_interest: {
                                form_id: newFormId,
                                program_name: newProgramName,
                                cta_text: triggeredForm.ctaText || `Apply to ${newProgramName}`,
                                fields: triggeredForm.fields || []
                            }
                        }
                    };
                }
            }

            // No different program detected - just skip CTAs as before
            console.log(`[Phase 1B] 🚫 Skipping form CTAs - suspended form active, no program switch detected`);
            return {
                message: bedrockResponse,
                ctaButtons: [],  // No CTAs when form is suspended
                metadata: {
                    enhanced: false,
                    suspended_forms_detected: suspendedForms
                }
            };
        }

        // Check for form triggers first (highest priority)
        const formTrigger = checkFormTriggers(bedrockResponse, userMessage, config);
        if (formTrigger) {
            // Map formId to program for comparison with completed_forms
            let program = formTrigger.formId; // Default to formId
            if (formTrigger.formId === 'lb_apply') program = 'lovebox';
            else if (formTrigger.formId === 'dd_apply') program = 'daretodream';

            // Check if this program has already been completed
            if (completedForms.includes(program)) {
                console.log(`🚫 Program "${program}" already completed (formId: ${formTrigger.formId}), skipping CTA`);
                // Don't show this CTA - continue to branch detection
            } else {
                return {
                    message: bedrockResponse,
                    ctaButtons: [{
                        type: 'form_cta',
                        label: formTrigger.ctaText || 'Start Application',
                        action: 'start_form',
                        formId: formTrigger.formId,
                        fields: formTrigger.fields
                    }],
                    metadata: {
                        enhanced: true,
                        form_triggered: formTrigger.formId,
                        program: program
                    }
                };
            }
        }

        // ============================================================================
        // DEPRECATED: Keyword Detection (Backward Compatibility Only)
        // ============================================================================
        // This is kept for tenants that haven't migrated to v1.4.1 explicit routing yet
        console.log('[DEPRECATED] Using keyword detection - consider configuring explicit routing via target_branch');
        const branchResult = detectConversationBranch(bedrockResponse, userMessage, config, completedForms);

        console.log('🌿 Branch detection result (keyword-based):', {
            branchFound: !!branchResult,
            branch: branchResult?.branch,
            ctaCount: branchResult?.ctas?.length || 0,
            ctas: branchResult?.ctas?.map(c => c.label || c.text)
        });

        if (branchResult && branchResult.ctas.length > 0) {
            // Convert CTAs to button format and filter out completed programs
            const ctaButtons = branchResult.ctas
                .filter(cta => {
                    // Check if this CTA is for a program that's already been completed
                    if (cta.action === 'start_form' || cta.action === 'form_trigger' || cta.type === 'form_cta') {
                        // Extract program from CTA
                        let program = cta.program || cta.program_id;

                        if (!program) {
                            // Map formIds to programs
                            const formId = cta.formId || cta.form_id;
                            if (formId === 'lb_apply') program = 'lovebox';
                            else if (formId === 'dd_apply') program = 'daretodream';
                            else if (formId === 'volunteer_apply' || formId === 'volunteer_general') {
                                // Generic volunteer form - check branch context
                                if (branchResult.branch === 'lovebox_discussion') program = 'lovebox';
                                else if (branchResult.branch === 'daretodream_discussion') program = 'daretodream';
                            }
                        }

                        // Filter if user has completed this program
                        if (program && completedForms.includes(program)) {
                            console.log(`🚫 Filtering out CTA for completed program: ${program} (formId: ${cta.formId || cta.form_id})`);
                            return false;
                        }
                    }
                    return true;
                })
                .map(cta => ({
                    label: cta.text || cta.label,
                    action: cta.action || cta.type,
                    ...cta
                }));

            return {
                message: bedrockResponse,
                ctaButtons: ctaButtons,
                metadata: {
                    enhanced: true,
                    branch_detected: branchResult.branch,
                    filtered_forms: completedForms
                }
            };
        }

        // No enhancements needed
        return {
            message: bedrockResponse,
            ctaButtons: [],
            metadata: { enhanced: false }
        };

    } catch (error) {
        console.error('Error enhancing response:', error);
        // Return unenhanced response on error
        return {
            message: bedrockResponse,
            ctaButtons: [],
            metadata: { enhanced: false, error: error.message }
        };
    }
}

// Export for use in main handler
module.exports = {
    enhanceResponse,
    loadTenantConfig,
    detectConversationBranch,  // DEPRECATED - kept for backward compatibility
    getConversationBranch,      // NEW - 3-tier routing
    buildCtasFromBranch         // NEW - explicit CTA building
};