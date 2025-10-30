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
} from './s3Operations.mjs';

import {
  mergeConfigSections,
  extractEditableSections,
  validateEditedSections,
  getSectionInfo,
  generateConfigDiff,
} from './mergeStrategy.mjs';

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

    // Health check
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

    // GET /config/tenants - List all tenant configs
    if (httpMethod === 'GET' && path === '/config/tenants') {
      const tenants = await listTenantConfigs();
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({ tenants }),
      };
    }

    // GET /config/{tenantId}/metadata - Get tenant metadata only
    if (httpMethod === 'GET' && path.match(/^\/config\/([^/]+)\/metadata$/)) {
      const tenantId = path.match(/^\/config\/([^/]+)\/metadata$/)[1];
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
