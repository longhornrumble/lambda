# Picasso Config Manager Lambda Function

AWS Lambda function for managing Picasso tenant configurations stored in S3.

## Overview

This Lambda function provides a REST API for CRUD operations on tenant configuration files stored in S3. It implements section-based editing to preserve read-only configuration sections and includes automatic backup functionality.

## Runtime

- **Node.js:** 20.x
- **Handler:** `index.handler`
- **Architecture:** arm64 or x86_64

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `S3_BUCKET` | No | `myrecruiter-picasso` | S3 bucket containing tenant configs |
| `AWS_REGION` | No | `us-east-1` | AWS region for S3 operations |
| `NODE_ENV` | No | `production` | Environment mode |

## API Endpoints

### Health Check
```
GET /health
```

Returns service health status.

**Response:**
```json
{
  "status": "healthy",
  "service": "picasso-config-manager",
  "timestamp": "2025-10-15T12:00:00.000Z"
}
```

### List Tenants
```
GET /config/tenants
```

Returns list of all tenant configurations.

**Response:**
```json
{
  "tenants": [
    {
      "tenantId": "MYR384719",
      "key": "MYR384719-config.json",
      "lastModified": "2025-10-15T10:00:00.000Z",
      "size": 12345
    }
  ]
}
```

### Get Tenant Metadata
```
GET /config/{tenantId}/metadata
```

Returns tenant metadata without loading full config.

**Response:**
```json
{
  "metadata": {
    "tenant_id": "MYR384719",
    "version": "1.3",
    "chat_title": "My Company HR",
    "company_name": "My Company Inc.",
    "last_updated": "2025-10-15T10:00:00.000Z",
    "program_count": 5,
    "form_count": 3,
    "cta_count": 8,
    "branch_count": 4
  }
}
```

### Load Tenant Config
```
GET /config/{tenantId}?editable_only=true
```

Loads full tenant configuration or editable sections only.

**Query Parameters:**
- `editable_only` (optional) - If `true`, returns only editable sections

**Response:**
```json
{
  "config": {
    "tenant_id": "MYR384719",
    "version": "1.3",
    "programs": { ... },
    "conversational_forms": { ... },
    ...
  }
}
```

### Save Tenant Config
```
PUT /config/{tenantId}
```

Saves tenant configuration with optional merge and backup.

**Request Body:**
```json
{
  "config": {
    "programs": { ... },
    "conversational_forms": { ... }
  },
  "merge": true,
  "create_backup": true,
  "validate_only": false
}
```

**Parameters:**
- `config` (required) - Configuration object with editable sections
- `merge` (optional, default: true) - Merge with existing config
- `create_backup` (optional, default: true) - Create backup before saving
- `validate_only` (optional, default: false) - Validate without saving

**Response:**
```json
{
  "success": true,
  "tenantId": "MYR384719",
  "key": "MYR384719-config.json",
  "timestamp": "2025-10-15T12:00:00.000Z"
}
```

### Delete Tenant Config
```
DELETE /config/{tenantId}
```

Deletes tenant configuration (creates backup first).

**Response:**
```json
{
  "success": true,
  "tenantId": "MYR384719",
  "deletedKey": "MYR384719-config.json",
  "backupKey": "backups/MYR384719-2025-10-15T12-00-00-000Z.json"
}
```

### List Backups
```
GET /config/{tenantId}/backups
```

Lists all backups for a tenant.

**Response:**
```json
{
  "backups": [
    {
      "key": "backups/MYR384719-2025-10-15T12-00-00-000Z.json",
      "lastModified": "2025-10-15T12:00:00.000Z",
      "size": 12345
    }
  ]
}
```

### Get Section Info
```
GET /sections
```

Returns information about editable and read-only sections.

**Response:**
```json
{
  "sections": {
    "editable": ["programs", "conversational_forms", "cta_definitions", "conversation_branches"],
    "readOnly": ["branding", "features", "quick_help", "action_chips", "widget_behavior", "aws", "card_inventory"],
    "metadata": ["tenant_id", "version", "chat_title", "company_name", "last_updated"]
  }
}
```

## Architecture

### Modules

#### `index.mjs`
Main Lambda handler. Routes requests to appropriate functions based on HTTP method and path.

#### `s3Operations.mjs`
Encapsulates all S3 interactions using AWS SDK v3.

**Key Functions:**
- `listTenantConfigs()` - List all configs
- `loadConfig(tenantId)` - Load config from S3
- `getTenantMetadata(tenantId)` - Get metadata
- `saveConfig(tenantId, config, createBackup)` - Save config
- `deleteConfig(tenantId)` - Delete config
- `createConfigBackup(tenantId, config)` - Create backup
- `listBackups(tenantId)` - List backups

#### `mergeStrategy.mjs`
Implements section-based editing logic.

**Key Functions:**
- `mergeConfigSections(baseConfig, editedSections)` - Merge logic
- `extractEditableSections(fullConfig)` - Extract editable sections
- `validateEditedSections(editedSections)` - Validate sections
- `generateConfigDiff(oldConfig, newConfig)` - Generate diff
- `isEditableSection(sectionName)` - Check if editable
- `isReadOnlySection(sectionName)` - Check if read-only

## Section-Based Editing

### Editable Sections
These sections can be modified through the config builder:
- `programs` - Program definitions
- `conversational_forms` - Form definitions
- `cta_definitions` - CTA button definitions
- `conversation_branches` - Branch routing rules

### Read-Only Sections
These sections are preserved during merge:
- `branding` - Visual styling
- `features` - Feature flags
- `quick_help` - Quick help prompts
- `action_chips` - Action chip definitions
- `widget_behavior` - Widget settings
- `aws` - AWS infrastructure config
- `card_inventory` - Extracted smart cards

### Merge Process

1. Client sends edited sections
2. Lambda loads full config from S3
3. Validates edited sections (rejects read-only modifications)
4. Merges edited sections into full config
5. Updates metadata (last_updated, tenant_id)
6. Creates backup if requested
7. Saves merged config to S3

## Deployment

### Prerequisites

- AWS CLI configured with appropriate credentials
- IAM role with S3 permissions
- API Gateway set up with Lambda proxy integration

### Package Function

```bash
npm ci --production
npm run package
```

This creates `deployment.zip` containing:
- `index.mjs`
- `s3Operations.mjs`
- `mergeStrategy.mjs`
- `package.json`
- `node_modules/` (production dependencies only)

### Deploy to AWS

```bash
npm run deploy
```

Or manually:

```bash
aws lambda update-function-code \
  --function-name Picasso_Config_Manager \
  --zip-file fileb://deployment.zip
```

### IAM Permissions

The Lambda execution role needs these S3 permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::myrecruiter-picasso",
        "arn:aws:s3:::myrecruiter-picasso/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    }
  ]
}
```

## Testing

### Local Testing

Use the local development server in `src/lib/api/localDevServer.ts` for local testing without AWS.

```bash
# From project root
npm run server:dev
```

### Lambda Testing

Test the deployed Lambda function:

```bash
# Health check
curl https://your-api-gateway-url/health

# List tenants
curl https://your-api-gateway-url/config/tenants

# Load config
curl https://your-api-gateway-url/config/MYR384719
```

## Error Handling

The Lambda function returns appropriate HTTP status codes:

- `200` - Success
- `400` - Bad Request (validation errors)
- `404` - Not Found (config doesn't exist)
- `500` - Internal Server Error

Error responses include:

```json
{
  "error": "Error Type",
  "message": "Detailed error message",
  "details": "Stack trace (development only)"
}
```

## Monitoring

### CloudWatch Logs

Logs are written to CloudWatch Logs group:
```
/aws/lambda/Picasso_Config_Manager
```

### Key Metrics to Monitor

- Invocation count
- Error count
- Duration
- Concurrent executions

### CloudWatch Alarms

Consider setting up alarms for:
- High error rate (> 5%)
- Long duration (> 10 seconds)
- Throttling

## Security

### CORS

CORS headers are included in all responses:
```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS
Access-Control-Allow-Headers: Content-Type, Authorization
```

For production, restrict `Access-Control-Allow-Origin` to specific domains.

### Input Validation

- Validates tenant IDs (alphanumeric + hyphens only)
- Validates JSON structure
- Rejects attempts to modify read-only sections
- Sanitizes file paths to prevent directory traversal

### Authentication

This Lambda currently has no authentication. For production:

1. Add JWT validation in Lambda
2. Use API Gateway authorizers
3. Implement role-based access control (RBAC)

## Backup Strategy

### Automatic Backups

Backups are automatically created:
- Before saving (if `create_backup: true`)
- Before deleting

### Backup Location

```
s3://myrecruiter-picasso/backups/{tenantId}-{timestamp}.json
```

Example:
```
s3://myrecruiter-picasso/backups/MYR384719-2025-10-15T12-30-45-123Z.json
```

### Backup Retention

Backups are not automatically deleted. Consider implementing:
- S3 lifecycle policies to delete old backups
- Versioning on the S3 bucket
- Cross-region replication for disaster recovery

## Troubleshooting

### "Module not found" errors

**Solution:** Ensure `npm ci --production` was run before packaging.

### "Access Denied" S3 errors

**Solution:** Check IAM role has correct S3 permissions.

### "Config not found" errors

**Solution:** Verify tenant ID is correct and config exists in S3.

### Timeout errors

**Solution:**
- Increase Lambda timeout (default: 3 seconds, max: 15 minutes)
- Check S3 bucket region matches Lambda region

## Dependencies

Production dependencies (included in deployment):

```json
{
  "@aws-sdk/client-s3": "^3.470.0"
}
```

The AWS SDK v3 uses modular packages to minimize bundle size.

## API Gateway Configuration

### Integration Type
Lambda Proxy Integration

### Routes
- Method: ANY
- Path: /{proxy+}

### CORS Configuration
Enable CORS with:
- Allow Origins: * (or specific domains)
- Allow Methods: GET, POST, PUT, DELETE, OPTIONS
- Allow Headers: Content-Type, Authorization

## Future Enhancements

1. **Authentication/Authorization**
   - JWT validation
   - Role-based access control
   - Audit logging

2. **Versioning**
   - Config version history
   - Rollback capability
   - Diff viewing

3. **Validation**
   - Schema validation using Zod
   - Business rule validation
   - Cross-reference validation

4. **Performance**
   - Caching layer (ElastiCache)
   - Connection pooling
   - Batch operations

5. **Monitoring**
   - X-Ray tracing
   - Custom CloudWatch metrics
   - Dashboard

## Support

For issues or questions:
1. Check CloudWatch logs for error details
2. Review this README and main documentation
3. Contact the development team

## License

Proprietary - MyRecruiter Inc.
