/**
 * Picasso Config Manager Lambda Function
 * Handles S3-based CRUD operations for tenant configurations
 *
 * Runtime: Node.js 20.x
 * Handler: index.handler
 */

import {
  listTenantConfigs,
  loadConfig,
  getTenantMetadata,
  saveConfig,
  deleteConfig,
  listBackups,
  storeTenantMapping,
  saveDraft,
  loadDraft,
  deleteDraft,
} from './s3Operations.mjs';
import crypto from 'crypto';

import {
  mergeConfigSections,
  extractEditableSections,
  validateEditedSections,
  getSectionInfo,
  generateConfigDiff,
} from './mergeStrategy.mjs';

import { authenticateRequest } from './auth.mjs';

/**
 * Authentication enforcement flag
 * Set to false during initial deployment to allow permissive mode (log warnings only)
 * Set to true to enforce 401/403 responses on auth failures
 */
const ENFORCE_AUTH = false;

/**
 * Main Lambda handler
 * Routes requests to appropriate functions based on HTTP method and path
 * Supports both API Gateway and Lambda Function URL event formats
 */
export const handler = async (event) => {
  console.log('Event:', JSON.stringify(event, null, 2));

  // Handle both API Gateway and Function URL event formats
  const httpMethod = event.httpMethod || event.requestContext?.http?.method;
  const path = event.path || event.rawPath || event.requestContext?.http?.path;
  const body = event.body;
  const queryStringParameters = event.queryStringParameters;

  // Check if this is a Function URL request (which handles CORS automatically)
  const isFunctionUrl = event.requestContext?.http?.method !== undefined;

  // Only add CORS headers for API Gateway (not Function URLs)
  const headers = {
    'Content-Type': 'application/json',
    ...(!isFunctionUrl && {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    }),
  };

  try {
    // OPTIONS for CORS preflight
    if (httpMethod === 'OPTIONS') {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ message: 'OK' }),
      };
    }

    // Health check (no auth required)
    if (httpMethod === 'GET' && path === '/health') {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          status: 'healthy',
          service: 'picasso-config-manager',
          timestamp: new Date().toISOString(),
        }),
      };
    }

    // Authenticate all other requests
    const auth = await authenticateRequest(event);

    if (!auth.success) {
      console.warn(`Authentication failed: ${auth.error}`);

      if (ENFORCE_AUTH) {
        return {
          statusCode: 401,
          headers,
          body: JSON.stringify({
            error: 'Unauthorized',
            message: auth.error,
          }),
        };
      } else {
        console.warn('PERMISSIVE MODE: Allowing unauthenticated request');
      }
    } else {
      console.log(`Authenticated user: ${auth.email} (role: ${auth.role}, tenants: ${auth.tenants?.join(', ')})`);
    }

    // POST /config - Create new tenant configuration
    if (httpMethod === 'POST' && path === '/config') {
      // Authorization: Only super_admin can create tenants
      if (auth.success && auth.role !== 'super_admin') {
        console.warn(`User ${auth.email} attempted to create tenant without super_admin role`);

        if (ENFORCE_AUTH) {
          return {
            statusCode: 403,
            headers,
            body: JSON.stringify({
              error: 'Forbidden',
              message: 'Only super_admin role can create tenants',
            }),
          };
        } else {
          console.warn('PERMISSIVE MODE: Allowing tenant creation without super_admin role');
        }
      }

      const requestBody = JSON.parse(body);
      const { tenant_id, chat_title, subscription_tier, welcome_message, primary_color, knowledge_base_id } = requestBody;

      // Validate tenant_id
      if (!tenant_id) {
        return {
          statusCode: 400,
          headers,
          body: JSON.stringify({
            error: 'Bad Request',
            message: 'tenant_id is required',
          }),
        };
      }

      // Validate tenant_id format: alphanumeric + underscore + dash, max 50 chars
      const tenantIdRegex = /^[a-zA-Z0-9_-]{1,50}$/;
      if (!tenantIdRegex.test(tenant_id)) {
        return {
          statusCode: 400,
          headers,
          body: JSON.stringify({
            error: 'Bad Request',
            message: 'tenant_id must be alphanumeric with underscores or dashes, max 50 characters',
          }),
        };
      }

      // Check if tenant already exists
      try {
        await loadConfig(tenant_id);
        return {
          statusCode: 409,
          headers,
          body: JSON.stringify({
            error: 'Conflict',
            message: `Tenant ${tenant_id} already exists`,
          }),
        };
      } catch (error) {
        if (!error.message.includes('not found')) {
          throw error;
        }
      }

      // Generate tenant hash
      function generateTenantHash(tenantId) {
        const hash = crypto.createHash('sha256')
          .update(tenantId + 'picasso-2024-universal-widget')
          .digest('hex');
        const prefix = hash.substring(0, 2).toLowerCase();
        const hashPart = hash.substring(2, 14);
        return prefix + hashPart;
      }

      const tenantHash = generateTenantHash(tenant_id);

      // Build skeleton config
      const config = {
        tenant_id: tenant_id,
        tenant_hash: tenantHash,
        version: 1,
        generated_at: Date.now(),
        chat_title: chat_title || tenant_id,
        chat_subtitle: '',
        welcome_message: welcome_message || 'Hello! How can I help you today?',
        subscription_tier: subscription_tier || 'Free',
        tone_prompt: '',
        branding: {
          primary_color: primary_color || '#10B981',
          secondary_color: '#059669',
          accent_color: '#34D399',
          background_color: '#FFFFFF',
          text_color: '#1F2937',
          font_family: 'Inter, system-ui, sans-serif',
        },
        features: {
          uploads: false,
          photo_uploads: false,
          voice_input: false,
          streaming: true,
          conversational_forms: false,
          smart_cards: false,
          sms: false,
          webchat: true,
          qr: false,
          bedrock_kb: false,
          ats: false,
          interview_scheduling: false,
          dashboard_conversations: false,
          dashboard_forms: false,
          dashboard_attribution: false,
        },
        widget_behavior: {
          start_open: false,
          remember_state: true,
          auto_open_delay: 0,
        },
        quick_help: {
          enabled: false,
          title: 'Quick Help',
          toggle_text: 'Need help?',
          close_after_selection: true,
          prompts: [],
        },
        aws: {
          knowledge_base_id: knowledge_base_id || '',
          aws_region: 'us-east-1',
        },
        programs: [],
        conversational_forms: [],
        cta_definitions: [],
        conversation_branches: [],
        content_showcase: [],
        topic_definitions: [],
        feature_flags: {},
        cta_settings: { fallback_tags: [] },
        action_chips: [],
        bedrock_instructions: '',
        card_inventory: [],
        monitor: {
          enabled: true,
          siteUrl: '',
          keyPages: ['/'],
          dubTag: '',
          webhookUrl: 'https://integrate.myrecruiter.ai/webhook/kb-monitor',
        },
      };

      // Save config
      await saveConfig(tenant_id, config, false);

      // Store mapping
      await storeTenantMapping(tenant_id, tenantHash);

      const embedCode = `<script src="https://chat.myrecruiter.ai/widget.js" data-tenant-hash="${tenantHash}"></script>`;

      return {
        statusCode: 201,
        headers,
        body: JSON.stringify({
          success: true,
          tenant_id: tenant_id,
          tenant_hash: tenantHash,
          embed_code: embedCode,
          config: config,
        }),
      };
    }

    // GET /config/tenants - List all tenant configs
    if (httpMethod === 'GET' && path === '/config/tenants') {
      const tenants = await listTenantConfigs();

      // Filter tenants based on user role and permissions
      let filteredTenants = tenants;
      if (auth.success && auth.role !== 'super_admin') {
        const userTenants = auth.tenants || [];
        filteredTenants = tenants.filter(tenant => userTenants.includes(tenant.tenantId));
        console.log(`Filtered ${tenants.length} tenants to ${filteredTenants.length} for non-admin user`);
      }

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ tenants: filteredTenants }),
      };
    }

    // GET /config/{tenantId}/metadata - Get tenant metadata only
    if (httpMethod === 'GET' && path.match(/^\/config\/([^/]+)\/metadata$/)) {
      const tenantId = path.match(/^\/config\/([^/]+)\/metadata$/)[1];

      // Authorization check: verify user has access to this tenant
      if (auth.success && auth.role !== 'super_admin') {
        const userTenants = auth.tenants || [];
        if (!userTenants.includes(tenantId)) {
          console.warn(`User ${auth.email} attempted to access tenant ${tenantId} without permission`);

          if (ENFORCE_AUTH) {
            return {
              statusCode: 403,
              headers,
              body: JSON.stringify({
                error: 'Forbidden',
                message: 'You do not have access to this tenant',
              }),
            };
          } else {
            console.warn('PERMISSIVE MODE: Allowing unauthorized tenant access');
          }
        }
      }

      const metadata = await getTenantMetadata(tenantId);
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ metadata }),
      };
    }

    // GET /config/{tenantId}/backups - List backups for tenant
    if (httpMethod === 'GET' && path.match(/^\/config\/([^/]+)\/backups$/)) {
      const tenantId = path.match(/^\/config\/([^/]+)\/backups$/)[1];

      // Authorization check: verify user has access to this tenant
      if (auth.success && auth.role !== 'super_admin') {
        const userTenants = auth.tenants || [];
        if (!userTenants.includes(tenantId)) {
          console.warn(`User ${auth.email} attempted to access tenant ${tenantId} without permission`);

          if (ENFORCE_AUTH) {
            return {
              statusCode: 403,
              headers,
              body: JSON.stringify({
                error: 'Forbidden',
                message: 'You do not have access to this tenant',
              }),
            };
          } else {
            console.warn('PERMISSIVE MODE: Allowing unauthorized tenant access');
          }
        }
      }

      const backups = await listBackups(tenantId);
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ backups }),
      };
    }

    // GET /config/{tenantId} - Load full tenant config
    if (httpMethod === 'GET' && path.match(/^\/config\/([^/]+)$/)) {
      const tenantId = path.match(/^\/config\/([^/]+)$/)[1];
      const editableOnly = queryStringParameters?.editable_only === 'true';

      // Authorization check: verify user has access to this tenant
      if (auth.success && auth.role !== 'super_admin') {
        const userTenants = auth.tenants || [];
        if (!userTenants.includes(tenantId)) {
          console.warn(`User ${auth.email} attempted to access tenant ${tenantId} without permission`);

          if (ENFORCE_AUTH) {
            return {
              statusCode: 403,
              headers,
              body: JSON.stringify({
                error: 'Forbidden',
                message: 'You do not have access to this tenant',
              }),
            };
          } else {
            console.warn('PERMISSIVE MODE: Allowing unauthorized tenant access');
          }
        }
      }

      const config = await loadConfig(tenantId);

      if (editableOnly) {
        const editableConfig = extractEditableSections(config);
        return {
          statusCode: 200,
          headers,
          body: JSON.stringify({ config: editableConfig }),
        };
      }

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ config }),
      };
    }

    // PUT /config/{tenantId} - Save tenant config
    if (httpMethod === 'PUT' && path.match(/^\/config\/([^/]+)$/)) {
      const tenantId = path.match(/^\/config\/([^/]+)$/)[1];

      // Authorization check: verify user has access to this tenant
      if (auth.success && auth.role !== 'super_admin') {
        const userTenants = auth.tenants || [];
        if (!userTenants.includes(tenantId)) {
          console.warn(`User ${auth.email} attempted to modify tenant ${tenantId} without permission`);

          if (ENFORCE_AUTH) {
            return {
              statusCode: 403,
              headers,
              body: JSON.stringify({
                error: 'Forbidden',
                message: 'You do not have access to this tenant',
              }),
            };
          } else {
            console.warn('PERMISSIVE MODE: Allowing unauthorized tenant modification');
          }
        }
      }

      const requestBody = JSON.parse(body);

      const {
        config: editedConfig,
        merge = true,
        create_backup = true,
        validate_only = false,
      } = requestBody;

      // Only validate sections if merge=true (section-based editing)
      // When merge=false, full config replacement is allowed
      if (merge) {
        const validation = validateEditedSections(editedConfig);
        if (!validation.isValid) {
          return {
            statusCode: 400,
            headers,
            body: JSON.stringify({
              error: 'Invalid edited sections',
              message: `Validation failed: ${validation.errors.join('; ')}`,
              details: validation.errors,
            }),
          };
        }
      }

      // If validation only, return without saving
      if (validate_only) {
        return {
          statusCode: 200,
          headers,
          body: JSON.stringify({
            valid: true,
            message: 'Configuration is valid',
          }),
        };
      }

      let finalConfig = editedConfig;

      // If merge is enabled, load base config and merge
      if (merge) {
        try {
          const baseConfig = await loadConfig(tenantId);
          finalConfig = mergeConfigSections(baseConfig, editedConfig);

          // Generate diff for logging
          const diff = generateConfigDiff(baseConfig, finalConfig);
          console.log('Config diff:', JSON.stringify(diff, null, 2));
        } catch (error) {
          // If config doesn't exist, use edited config as-is
          if (error.message.includes('not found')) {
            console.log('Creating new config (no existing config found)');
            finalConfig = editedConfig;
          } else {
            throw error;
          }
        }
      }

      // Save the config
      const result = await saveConfig(tenantId, finalConfig, create_backup);

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          ...result,
        }),
      };
    }

    // DELETE /config/{tenantId} - Delete tenant config
    if (httpMethod === 'DELETE' && path.match(/^\/config\/([^/]+)$/)) {
      const tenantId = path.match(/^\/config\/([^/]+)$/)[1];

      // Authorization check: verify user has access to this tenant
      if (auth.success && auth.role !== 'super_admin') {
        const userTenants = auth.tenants || [];
        if (!userTenants.includes(tenantId)) {
          console.warn(`User ${auth.email} attempted to delete tenant ${tenantId} without permission`);

          if (ENFORCE_AUTH) {
            return {
              statusCode: 403,
              headers,
              body: JSON.stringify({
                error: 'Forbidden',
                message: 'You do not have access to this tenant',
              }),
            };
          } else {
            console.warn('PERMISSIVE MODE: Allowing unauthorized tenant deletion');
          }
        }
      }

      const result = await deleteConfig(tenantId);

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          ...result,
        }),
      };
    }

    // PUT /config/{tenantId}/draft - Save draft config
    if (httpMethod === 'PUT' && path.match(/^\/config\/([^/]+)\/draft$/)) {
      const tenantId = path.match(/^\/config\/([^/]+)\/draft$/)[1];

      // Authorization check: verify user has access to this tenant
      if (auth.success && auth.role !== 'super_admin') {
        const userTenants = auth.tenants || [];
        if (!userTenants.includes(tenantId)) {
          console.warn(`User ${auth.email} attempted to save draft for tenant ${tenantId} without permission`);

          if (ENFORCE_AUTH) {
            return {
              statusCode: 403,
              headers,
              body: JSON.stringify({
                error: 'Forbidden',
                message: 'You do not have access to this tenant',
              }),
            };
          } else {
            console.warn('PERMISSIVE MODE: Allowing unauthorized draft save');
          }
        }
      }

      const requestBody = JSON.parse(body);
      const { config } = requestBody;

      if (!config || typeof config !== 'object') {
        return {
          statusCode: 400,
          headers,
          body: JSON.stringify({
            error: 'Bad Request',
            message: 'config object is required in request body',
          }),
        };
      }

      console.log(`Saving draft for tenant ${tenantId}`);
      await saveDraft(tenantId, config);

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ success: true, message: 'Draft saved' }),
      };
    }

    // GET /config/{tenantId}/draft - Load draft config
    if (httpMethod === 'GET' && path.match(/^\/config\/([^/]+)\/draft$/)) {
      const tenantId = path.match(/^\/config\/([^/]+)\/draft$/)[1];

      // Authorization check: verify user has access to this tenant
      if (auth.success && auth.role !== 'super_admin') {
        const userTenants = auth.tenants || [];
        if (!userTenants.includes(tenantId)) {
          console.warn(`User ${auth.email} attempted to load draft for tenant ${tenantId} without permission`);

          if (ENFORCE_AUTH) {
            return {
              statusCode: 403,
              headers,
              body: JSON.stringify({
                error: 'Forbidden',
                message: 'You do not have access to this tenant',
              }),
            };
          } else {
            console.warn('PERMISSIVE MODE: Allowing unauthorized draft load');
          }
        }
      }

      console.log(`Loading draft for tenant ${tenantId}`);
      const { config, hasDraft } = await loadDraft(tenantId);

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ hasDraft, config }),
      };
    }

    // DELETE /config/{tenantId}/draft - Delete draft config
    if (httpMethod === 'DELETE' && path.match(/^\/config\/([^/]+)\/draft$/)) {
      const tenantId = path.match(/^\/config\/([^/]+)\/draft$/)[1];

      // Authorization check: verify user has access to this tenant
      if (auth.success && auth.role !== 'super_admin') {
        const userTenants = auth.tenants || [];
        if (!userTenants.includes(tenantId)) {
          console.warn(`User ${auth.email} attempted to delete draft for tenant ${tenantId} without permission`);

          if (ENFORCE_AUTH) {
            return {
              statusCode: 403,
              headers,
              body: JSON.stringify({
                error: 'Forbidden',
                message: 'You do not have access to this tenant',
              }),
            };
          } else {
            console.warn('PERMISSIVE MODE: Allowing unauthorized draft deletion');
          }
        }
      }

      console.log(`Deleting draft for tenant ${tenantId}`);
      await deleteDraft(tenantId);

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ success: true, message: 'Draft deleted' }),
      };
    }

    // GET /sections - Get section information
    if (httpMethod === 'GET' && path === '/sections') {
      const info = getSectionInfo();
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ sections: info }),
      };
    }

    // 404 - Route not found
    return {
      statusCode: 404,
      headers,
      body: JSON.stringify({
        error: 'Not Found',
        message: `Route not found: ${httpMethod} ${path}`,
      }),
    };

  } catch (error) {
    console.error('Error:', error);

    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        error: 'Internal Server Error',
        message: error.message,
        details: process.env.NODE_ENV === 'development' ? error.stack : undefined,
      }),
    };
  }
};
