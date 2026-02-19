/**
 * Response Enhancer for Conversational CTAs
 *
 * v3.0 Evolution: Supports AI-generated dynamic actions alongside
 * legacy explicit routing (Tier 1-2 action chips preserved).
 *
 * New: parseAiActions(), parseChips() for extracting AI-generated
 * hidden tags from Bedrock responses.
 */

const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');

// Initialize S3 client
const s3Client = new S3Client({ region: process.env.AWS_REGION || 'us-east-1' });

// Simple cache for tenant configs (5 minute TTL)
const configCache = {};
const CACHE_TTL = 5 * 60 * 1000; // 5 minutes

// Track if we've warned about S3 bucket fallback (only warn once per Lambda instance)
let s3BucketWarningLogged = false;

// ═══════════════════════════════════════════════════════════════
// EVOLUTION v3.0: AI-Generated Action & Chip Parsers
// ═══════════════════════════════════════════════════════════════

/**
 * Parse AI-generated actions from Bedrock response.
 * Extracts <!-- ACTIONS: [...] --> hidden tag and returns parsed actions.
 *
 * @param {string} response - Bedrock response text (may include hidden tags)
 * @returns {{ actions: Array, cleanedResponse: string }}
 */
function parseAiActions(response) {
    if (!response || typeof response !== 'string') {
        return { actions: [], cleanedResponse: response || '' };
    }

    const pattern = /<!--\s*ACTIONS:\s*(\[[\s\S]*?\])\s*-->/i;
    const match = response.match(pattern);

    if (!match) {
        return { actions: [], cleanedResponse: response };
    }

    try {
        const actions = JSON.parse(match[1]);
        const cleanedResponse = response.replace(pattern, '').trim();
        console.log(`[v3.0] Parsed ${actions.length} AI-generated actions`);
        return { actions: Array.isArray(actions) ? actions : [], cleanedResponse };
    } catch (parseError) {
        console.error('[v3.0] Failed to parse AI-generated actions:', parseError.message);
        // Strip the malformed tag but return no actions
        const cleanedResponse = response.replace(pattern, '').trim();
        return { actions: [], cleanedResponse };
    }
}

/**
 * Parse suggested chips from Bedrock response.
 * Extracts <!-- CHIPS: [...] --> hidden tag and returns chip strings.
 *
 * @param {string} response - Bedrock response text (may include hidden tags)
 * @returns {{ chips: Array<string>, cleanedResponse: string }}
 */
function parseChips(response) {
    if (!response || typeof response !== 'string') {
        return { chips: [], cleanedResponse: response || '' };
    }

    const pattern = /<!--\s*CHIPS:\s*(\[[\s\S]*?\])\s*-->/i;
    const match = response.match(pattern);

    if (!match) {
        return { chips: [], cleanedResponse: response };
    }

    try {
        const chips = JSON.parse(match[1]);
        const cleanedResponse = response.replace(pattern, '').trim();
        // Validate: must be array of strings, max 3, max 50 chars each
        const validChips = (Array.isArray(chips) ? chips : [])
            .filter(c => typeof c === 'string' && c.trim().length > 0)
            .map(c => c.trim().slice(0, 50))
            .slice(0, 3);
        console.log(`[v3.0] Parsed ${validChips.length} suggested chips`);
        return { chips: validChips, cleanedResponse };
    } catch (parseError) {
        console.error('[v3.0] Failed to parse suggested chips:', parseError.message);
        const cleanedResponse = response.replace(pattern, '').trim();
        return { chips: [], cleanedResponse };
    }
}

/**
 * Validate and filter AI-generated actions against tenant config.
 * Ensures formIds exist, URLs are valid, and completed forms are excluded.
 *
 * @param {Array} actions - Raw AI-generated actions
 * @param {Object} config - Tenant configuration
 * @param {Array} completedForms - List of completed form program IDs
 * @returns {Array} - Validated CTA buttons ready for frontend
 */
function validateAiActions(actions, config, completedForms = []) {
    if (!actions || !Array.isArray(actions) || actions.length === 0) {
        return [];
    }

    const conversationalForms = config.conversational_forms || {};
    const availableActions = config.available_actions || {};
    const availableForms = availableActions.forms || {};
    const availableLinks = availableActions.links || {};
    const validated = [];

    for (const action of actions.slice(0, 3)) {
        // Must have label and action type
        if (!action.label || !action.action) {
            console.log(`[v3.0] Skipping action without label/action:`, action);
            continue;
        }

        if (action.action === 'start_form') {
            // Validate formId exists in config
            const formId = action.formId || action.form_id;
            if (!formId) {
                console.log(`[v3.0] Skipping start_form action without formId`);
                continue;
            }

            // Check available_actions.forms first, then conversational_forms
            let formFound = false;
            let program = formId;

            if (availableForms[formId]) {
                formFound = true;
                program = formId;
            } else {
                for (const [key, formConfig] of Object.entries(conversationalForms)) {
                    if (formConfig.form_id === formId || key === formId) {
                        formFound = true;
                        program = formConfig.program || formConfig.form_id || key;
                        break;
                    }
                }
            }

            if (!formFound) {
                console.log(`[v3.0] Skipping start_form - formId "${formId}" not in config`);
                continue;
            }

            // Filter completed forms
            let programKey = program;
            if (formId === 'lb_apply') programKey = 'lovebox';
            else if (formId === 'dd_apply') programKey = 'daretodream';

            if (completedForms.includes(programKey)) {
                console.log(`[v3.0] Filtering completed program: ${programKey} (formId: ${formId})`);
                continue;
            }

            validated.push({
                type: 'form_cta',
                label: action.label,
                action: 'start_form',
                formId: formId
            });

        } else if (action.action === 'external_link') {
            // Validate URL exists and looks legitimate
            if (!action.url || typeof action.url !== 'string') {
                console.log(`[v3.0] Skipping external_link without url`);
                continue;
            }

            // Basic URL validation — must start with http
            if (!action.url.startsWith('http')) {
                console.log(`[v3.0] Skipping external_link with invalid url: "${action.url}"`);
                continue;
            }

            validated.push({
                label: action.label,
                action: 'external_link',
                url: action.url
            });

        } else if (action.action === 'send_query') {
            // Validate query exists
            if (!action.query || typeof action.query !== 'string') {
                console.log(`[v3.0] Skipping send_query without query`);
                continue;
            }
            validated.push({
                label: action.label,
                action: 'send_query',
                query: action.query
            });

        } else {
            console.log(`[v3.0] Skipping unknown action type: ${action.action}`);
        }
    }

    console.log(`[v3.0] Validated ${validated.length}/${actions.length} AI-generated actions`);
    return validated;
}

/**
 * Get config bucket name with warning if using fallback
 * @returns {string} - S3 bucket name
 */
function getConfigBucket() {
  const bucket = process.env.CONFIG_BUCKET || process.env.S3_CONFIG_BUCKET;
  if (!bucket) {
    if (!s3BucketWarningLogged) {
      console.warn('⚠️ CONFIG_BUCKET/S3_CONFIG_BUCKET not set - using fallback myrecruiter-picasso');
      s3BucketWarningLogged = true;
    }
    return 'myrecruiter-picasso';
  }
  return bucket;
}

/**
 * Resolve tenant hash to tenant ID via S3 mapping
 */
async function resolveTenantHash(tenantHash) {
    try {
        const mappingKey = `mappings/${tenantHash}.json`;
        const command = new GetObjectCommand({
            Bucket: getConfigBucket(),
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
            Bucket: getConfigBucket(),
            Key: configKey
        });

        const response = await s3Client.send(command);
        const config = JSON.parse(await response.Body.transformToString());

        // Extract relevant sections
        const result = {
            conversation_branches: config.conversation_branches || {},
            cta_definitions: config.cta_definitions || {},
            conversational_forms: config.conversational_forms || {},
            cta_settings: config.cta_settings || {},  // Required for Tier 3 fallback routing
            content_showcase: config.content_showcase || [],  // Required for showcase items
            available_actions: config.available_actions || {}  // Required for v3.5 NEXT tag validation
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
        // Try target_branch from metadata first, then look up from cta_definitions by cta_id
        let targetBranch = routingMetadata.target_branch;
        if (!targetBranch && routingMetadata.cta_id) {
            const ctaDefs = config.cta_definitions || {};
            targetBranch = ctaDefs[routingMetadata.cta_id]?.target_branch;
            if (targetBranch) {
                console.log(`[Tier 2] Resolved target_branch from cta_definitions[${routingMetadata.cta_id}]: ${targetBranch}`);
            }
        }
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
                // Strip legacy style field and add position metadata
                const { style, ...cleanCta } = primaryCta;
                ctas.push({ ...cleanCta, id: primaryCtaId, _position: 'primary' });
            }
        } else {
            // Not a form CTA, always show
            // Strip legacy style field and add position metadata
            const { style, ...cleanCta } = primaryCta;
            ctas.push({ ...cleanCta, id: primaryCtaId, _position: 'primary' });
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

        // Strip legacy style field and add position metadata
        const { style, ...cleanCta } = cta;
        ctas.push({ ...cleanCta, id: ctaId, _position: 'secondary' });
    }

    // Return max 3 CTAs
    const finalCtas = ctas.slice(0, 3);
    console.log(`[CTA Builder] Built ${finalCtas.length} CTAs for branch '${branchName}'`);
    return finalCtas;
}

/**
 * Parse branch hint from Bedrock response (Tier 4 AI-suggested routing)
 *
 * Extracts the <!-- BRANCH: xxx --> tag from the model's response and strips it
 * from the visible text.
 *
 * @param {string} response - Raw Bedrock response text
 * @returns {Object} - { branch: string|null, cleanedResponse: string }
 */
function parseBranchHint(response) {
    if (!response || typeof response !== 'string') {
        return { branch: null, cleanedResponse: response || '' };
    }

    // Match <!-- BRANCH: branch_name --> pattern (case insensitive, allows whitespace)
    const branchPattern = /<!--\s*BRANCH:\s*([a-zA-Z0-9_-]+)\s*-->/i;
    const match = response.match(branchPattern);

    if (match) {
        const branch = match[1].toLowerCase();
        // Strip the branch tag from visible response
        const cleanedResponse = response.replace(branchPattern, '').trim();
        console.log(`[Tier 4] Parsed branch hint: "${branch}"`);
        return { branch, cleanedResponse };
    }

    return { branch: null, cleanedResponse: response };
}

/**
 * Get showcase item for a conversation branch
 *
 * Looks up a branch by name and returns the associated showcase item if one exists.
 * Showcase items act as "digital flyers" with rich media and CTAs.
 *
 * @param {string} branchName - Name of the conversation branch
 * @param {Object} config - Tenant configuration with conversation_branches and content_showcase
 * @returns {Object|null} - Showcase item object or null if not found
 */
function getShowcaseForBranch(branchName, config) {
    const branches = config.conversation_branches || {};
    const contentShowcase = config.content_showcase || [];

    // Check if branch exists
    if (!branchName || !branches[branchName]) {
        console.log(`[Showcase] Branch '${branchName}' not found`);
        return null;
    }

    const branch = branches[branchName];

    // Check if branch has a showcase_item_id
    if (!branch.showcase_item_id) {
        console.log(`[Showcase] Branch '${branchName}' has no showcase_item_id`);
        return null;
    }

    // Find matching showcase item in content_showcase array
    const showcaseItem = contentShowcase.find(item => item.id === branch.showcase_item_id);

    if (!showcaseItem) {
        console.log(`[Showcase] Showcase item '${branch.showcase_item_id}' not found in content_showcase`);
        return null;
    }

    // Check if showcase item is enabled (if the field exists)
    if (showcaseItem.hasOwnProperty('enabled') && !showcaseItem.enabled) {
        console.log(`[Showcase] Showcase item '${showcaseItem.id}' is disabled`);
        return null;
    }

    console.log(`[Showcase] Found showcase item '${showcaseItem.id}' for branch '${branchName}'`);
    return showcaseItem;
}

/**
 * Resolve showcase item CTAs to full CTA definitions
 *
 * Takes the CTA IDs from a showcase item's available_ctas and resolves them
 * to full CTA definitions from the tenant configuration.
 *
 * @param {Object} showcaseItem - Showcase item with available_ctas
 * @param {Object} config - Tenant configuration with cta_definitions
 * @returns {Object} - { primary: CTA|null, secondary: CTA[] }
 */
function resolveShowcaseCTAs(showcaseItem, config) {
    const ctaDefinitions = config.cta_definitions || {};
    const availableCtas = showcaseItem.available_ctas || {};

    const resolvedCtas = {
        primary: null,
        secondary: []
    };

    // Resolve primary CTA
    if (availableCtas.primary) {
        const primaryCtaId = availableCtas.primary;
        const primaryCta = ctaDefinitions[primaryCtaId];

        if (primaryCta) {
            // Strip legacy style field and include ID
            const { style, ...cleanCta } = primaryCta;
            resolvedCtas.primary = { ...cleanCta, id: primaryCtaId };
            console.log(`[Showcase CTAs] Resolved primary CTA: ${primaryCtaId}`);
        } else {
            console.log(`[Showcase CTAs] Primary CTA '${primaryCtaId}' not found in cta_definitions`);
        }
    }

    // Resolve secondary CTAs
    if (Array.isArray(availableCtas.secondary)) {
        for (const ctaId of availableCtas.secondary) {
            const cta = ctaDefinitions[ctaId];

            if (cta) {
                // Strip legacy style field and include ID
                const { style, ...cleanCta } = cta;
                resolvedCtas.secondary.push({ ...cleanCta, id: ctaId });
                console.log(`[Showcase CTAs] Resolved secondary CTA: ${ctaId}`);
            } else {
                console.log(`[Showcase CTAs] Secondary CTA '${ctaId}' not found in cta_definitions`);
            }
        }
    }

    console.log(`[Showcase CTAs] Resolved ${resolvedCtas.primary ? 1 : 0} primary + ${resolvedCtas.secondary.length} secondary CTAs`);
    return resolvedCtas;
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
                            // Strip legacy style field and add position metadata
                            const { style, ...cleanCta } = primaryCta;
                            ctas.push({
                                ...cleanCta,
                                id: branch.available_ctas.primary,
                                _position: 'primary'
                            });
                        }
                    } else {
                        // Not a form CTA, always show
                        // Strip legacy style field and add position metadata
                        const { style, ...cleanCta } = primaryCta;
                        ctas.push({
                            ...cleanCta,
                            id: branch.available_ctas.primary,
                            _position: 'primary'
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
                                // Strip legacy style field and add position metadata
                                const { style, ...cleanCta } = cta;
                                ctas.push({
                                    ...cleanCta,
                                    id: ctaId,
                                    _position: 'secondary'
                                });
                            }
                        } else {
                            // Not a form CTA, always show
                            // Strip legacy style field and add position metadata
                            const { style, ...cleanCta } = cta;
                            ctas.push({
                                ...cleanCta,
                                id: ctaId,
                                _position: 'secondary'
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
async function enhanceResponse(bedrockResponse, userMessage, tenantHash, sessionContext = {}, routingMetadata = {}, parsedActions = null) {
    console.log('🔍 enhanceResponse called with:', {
        responseLength: bedrockResponse?.length,
        userMessage,
        tenantHash,
        sessionContext,
        routingMetadata,
        completedForms: sessionContext.completed_forms || [],
        suspendedForms: sessionContext.suspended_forms || [],
        programInterest: sessionContext.program_interest,
        hasParsedActions: parsedActions && parsedActions.length > 0,
        parsedActionCount: parsedActions?.length || 0,
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

            // Check if this branch has an associated showcase item
            const showcaseItem = getShowcaseForBranch(explicitBranch, config);
            let showcaseCard = null;

            if (showcaseItem) {
                // Build showcase card with resolved CTAs
                const resolvedCtas = resolveShowcaseCTAs(showcaseItem, config);

                showcaseCard = {
                    id: showcaseItem.id,
                    type: showcaseItem.type,
                    name: showcaseItem.name,
                    tagline: showcaseItem.tagline,
                    description: showcaseItem.description,
                    image_url: showcaseItem.image_url,
                    highlights: showcaseItem.highlights,
                    ctaButtons: resolvedCtas
                };

                console.log(`[Explicit Routing] Including showcase card: ${showcaseItem.id}`);
            }

            if (ctas.length > 0 || showcaseCard) {
                const response = {
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

                // Add showcase card if present
                if (showcaseCard) {
                    response.showcaseCard = showcaseCard;
                    response.metadata.has_showcase = true;
                }

                console.log(`[Explicit Routing] Returning ${ctas.length} CTAs${showcaseCard ? ' + showcase card' : ''} from branch '${explicitBranch}'`);
                return response;
            } else {
                console.log(`[Explicit Routing] Branch '${explicitBranch}' has no CTAs or showcase items to display`);
            }
        }
        // ============================================================================
        // End of Explicit Routing - Continue to existing logic for backward compatibility
        // ============================================================================

        // ============================================================================
        // TIER 4: AI-Suggested Branch Routing (Free-flow conversations)
        // ============================================================================
        // Skip Tier 4 if no fallback_branch configured - user wants explicit routing only
        // This respects the Config Builder's "None (no CTAs shown when no match)" setting
        const ctaSettings = config.cta_settings || {};
        const tier4Enabled = !!ctaSettings.fallback_branch;

        if (!tier4Enabled) {
            console.log('[Tier 4] Skipped - no fallback_branch configured (explicit routing only)');
        }

        // Parse branch hint from model response (<!-- BRANCH: xxx -->)
        const { branch: suggestedBranch, cleanedResponse } = parseBranchHint(bedrockResponse);

        if (tier4Enabled && suggestedBranch) {
            const branches = config.conversation_branches || {};

            // Validate suggested branch exists
            if (branches[suggestedBranch]) {
                console.log(`[Tier 4] AI suggested valid branch: ${suggestedBranch}`);
                const ctas = buildCtasFromBranch(suggestedBranch, config, completedForms);

                // Check if this branch has an associated showcase item
                const showcaseItem = getShowcaseForBranch(suggestedBranch, config);
                let showcaseCard = null;

                if (showcaseItem) {
                    // Build showcase card with resolved CTAs
                    const resolvedCtas = resolveShowcaseCTAs(showcaseItem, config);

                    showcaseCard = {
                        id: showcaseItem.id,
                        type: showcaseItem.type,
                        name: showcaseItem.name,
                        tagline: showcaseItem.tagline,
                        description: showcaseItem.description,
                        image_url: showcaseItem.image_url,
                        highlights: showcaseItem.highlights,
                        ctaButtons: resolvedCtas
                    };

                    console.log(`[Tier 4] Including showcase card: ${showcaseItem.id}`);
                }

                if (ctas.length > 0 || showcaseCard) {
                    const response = {
                        message: cleanedResponse,  // Use cleaned response (tag stripped)
                        ctaButtons: ctas,
                        metadata: {
                            enhanced: true,
                            branch: suggestedBranch,
                            routing_tier: 'tier4_ai_suggested',
                            routing_method: 'model_branch_hint'
                        }
                    };

                    // Add showcase card if present
                    if (showcaseCard) {
                        response.showcaseCard = showcaseCard;
                        response.metadata.has_showcase = true;
                    }

                    console.log(`[Tier 4] Returning ${ctas.length} CTAs${showcaseCard ? ' + showcase card' : ''} from AI-suggested branch '${suggestedBranch}'`);
                    return response;
                }
            } else {
                // Invalid branch suggested - use fallback
                console.log(`[Tier 4] AI suggested invalid branch '${suggestedBranch}', using fallback`);
                const fallbackBranch = ctaSettings.fallback_branch;

                if (fallbackBranch && branches[fallbackBranch]) {
                    const ctas = buildCtasFromBranch(fallbackBranch, config, completedForms);

                    // Check if fallback branch has an associated showcase item
                    const showcaseItem = getShowcaseForBranch(fallbackBranch, config);
                    let showcaseCard = null;

                    if (showcaseItem) {
                        // Build showcase card with resolved CTAs
                        const resolvedCtas = resolveShowcaseCTAs(showcaseItem, config);

                        showcaseCard = {
                            id: showcaseItem.id,
                            type: showcaseItem.type,
                            name: showcaseItem.name,
                            tagline: showcaseItem.tagline,
                            description: showcaseItem.description,
                            image_url: showcaseItem.image_url,
                            highlights: showcaseItem.highlights,
                            ctaButtons: resolvedCtas
                        };

                        console.log(`[Tier 4] Including showcase card from fallback: ${showcaseItem.id}`);
                    }

                    if (ctas.length > 0 || showcaseCard) {
                        const response = {
                            message: cleanedResponse,
                            ctaButtons: ctas,
                            metadata: {
                                enhanced: true,
                                branch: fallbackBranch,
                                routing_tier: 'tier4_ai_suggested',
                                routing_method: 'model_fallback',
                                original_suggestion: suggestedBranch
                            }
                        };

                        // Add showcase card if present
                        if (showcaseCard) {
                            response.showcaseCard = showcaseCard;
                            response.metadata.has_showcase = true;
                        }

                        console.log(`[Tier 4] Returning ${ctas.length} CTAs${showcaseCard ? ' + showcase card' : ''} from fallback branch '${fallbackBranch}'`);
                        return response;
                    }
                }
            }

            // If we parsed a branch hint but couldn't route, still return cleaned response
            bedrockResponse = cleanedResponse;
        }
        // ============================================================================
        // End of Tier 4 - Continue to existing logic for backward compatibility
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

        // ============================================================================
        // EVOLUTION v3.0: AI-Generated Dynamic Actions
        // ============================================================================
        // If the model generated actions via <!-- ACTIONS: [...] -->, validate and use them
        if (parsedActions && parsedActions.length > 0) {
            console.log(`[v3.0] Processing ${parsedActions.length} AI-generated actions`);
            const validatedActions = validateAiActions(parsedActions, config, completedForms);

            if (validatedActions.length > 0) {
                return {
                    message: bedrockResponse,
                    ctaButtons: validatedActions,
                    metadata: {
                        enhanced: true,
                        routing_tier: 'v3_dynamic',
                        routing_method: 'ai_generated',
                        actions_proposed: parsedActions.length,
                        actions_validated: validatedActions.length
                    }
                };
            }
            // If all actions were filtered/invalid, return with no CTAs (don't fall to legacy)
            console.log(`[v3.0] All AI-generated actions filtered — returning without CTAs`);
            return {
                message: bedrockResponse,
                ctaButtons: [],
                metadata: {
                    enhanced: false,
                    routing_tier: 'v3_dynamic',
                    routing_method: 'ai_generated_none_valid',
                    actions_proposed: parsedActions.length,
                    actions_validated: 0
                }
            };
        }

        // When DYNAMIC_ACTIONS is on but no actions were parsed, skip legacy entirely
        if (config?.feature_flags?.DYNAMIC_ACTIONS) {
            console.log(`[v3.0] DYNAMIC_ACTIONS enabled — skipping legacy keyword detection`);
            return {
                message: bedrockResponse,
                ctaButtons: [],
                metadata: {
                    enhanced: false,
                    routing_tier: 'v3_dynamic',
                    routing_method: 'no_actions_generated'
                }
            };
        }

        // ============================================================================
        // LEGACY: Keyword-based form triggers and branch detection
        // Active ONLY when DYNAMIC_ACTIONS feature flag is off
        // ============================================================================

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

/**
 * Get showcase item by ID directly (for action chip routing)
 *
 * Unlike getShowcaseForBranch, this looks up a showcase item directly by ID,
 * bypassing the branch lookup. Used when action chips target showcase items
 * with action: 'show_showcase'.
 *
 * @param {string} showcaseId - ID of the showcase item
 * @param {Object} config - Tenant configuration with content_showcase
 * @returns {Object|null} - Showcase item object with resolved CTAs, or null if not found
 */
function getShowcaseById(showcaseId, config) {
    const contentShowcase = config.content_showcase || [];

    if (!showcaseId) {
        console.log(`[Showcase] No showcase ID provided`);
        return null;
    }

    // Find matching showcase item
    const showcaseItem = contentShowcase.find(item => item.id === showcaseId);

    if (!showcaseItem) {
        console.log(`[Showcase] Showcase item '${showcaseId}' not found in content_showcase`);
        return null;
    }

    // Check if showcase item is enabled
    if (showcaseItem.hasOwnProperty('enabled') && !showcaseItem.enabled) {
        console.log(`[Showcase] Showcase item '${showcaseId}' is disabled`);
        return null;
    }

    console.log(`[Showcase] Found showcase item '${showcaseId}' for direct routing`);

    // Build showcase card with resolved CTAs
    const resolvedCtas = resolveShowcaseCTAs(showcaseItem, config);

    return {
        id: showcaseItem.id,
        type: showcaseItem.type,
        name: showcaseItem.name,
        tagline: showcaseItem.tagline,
        description: showcaseItem.description,
        image_url: showcaseItem.image_url,
        stats: showcaseItem.stats,
        testimonial: showcaseItem.testimonial,
        highlights: showcaseItem.highlights,
        ctaButtons: resolvedCtas
    };
}

// Export for use in main handler
module.exports = {
    enhanceResponse,
    loadTenantConfig,
    // v3.0 Evolution - AI-generated action/chip parsers
    parseAiActions,
    parseChips,
    validateAiActions,
    // Legacy - kept for backward compatibility
    detectConversationBranch,   // DEPRECATED - keyword-based branch detection
    getConversationBranch,      // Tier 1-3 explicit routing
    buildCtasFromBranch,        // Explicit CTA building from branch
    parseBranchHint,            // Tier 4 - AI-suggested branch parsing (legacy)
    getShowcaseForBranch,       // Phase 2.3 - Showcase items lookup
    resolveShowcaseCTAs,        // Phase 2.3 - Showcase CTA resolution
    getShowcaseById             // Action chip → showcase direct routing
};