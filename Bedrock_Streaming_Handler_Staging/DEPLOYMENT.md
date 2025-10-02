# Bedrock Streaming Handler Deployment Guide

## Overview

This document provides comprehensive deployment instructions for the Bedrock_Streaming_Handler_Staging Lambda function. Follow these steps to ensure a successful deployment with all required configurations and permissions.

## Prerequisites

### Required Tools

- **Node.js**: v20.0.0 or higher
- **npm**: v9.0.0 or higher
- **AWS CLI**: v2.x configured with appropriate credentials
- **Git**: For version control

### Required Access

- AWS Account with Lambda deployment permissions
- IAM permissions to create/update roles and policies
- Access to S3, DynamoDB, Bedrock, SES, and SNS services

## Environment Variables

### Required Variables

These must be set in Lambda configuration:

| Variable | Description | Example | Default |
|----------|-------------|---------|---------|
| `CONFIG_BUCKET` | S3 bucket for tenant configs | `myrecruiter-picasso` | `myrecruiter-picasso` |
| `FORM_SUBMISSIONS_TABLE` | DynamoDB table for form data | `picasso-form-submissions` | `picasso-form-submissions` |
| `SMS_USAGE_TABLE` | DynamoDB table for SMS tracking | `picasso-sms-usage` | `picasso-sms-usage` |

### Optional Variables

These have sensible defaults but can be customized:

| Variable | Description | Default |
|----------|-------------|---------|
| `AWS_REGION` | AWS region for services | `us-east-1` |
| `BEDROCK_MODEL_ID` | Bedrock model to use | `us.anthropic.claude-3-5-haiku-20241022-v1:0` |
| `SMS_MONTHLY_LIMIT` | SMS limit per tenant/month | `100` |
| `SES_FROM_EMAIL` | Email address for notifications | `noreply@picasso.ai` |

### Environment-Specific Configuration

**Staging**:
```bash
CONFIG_BUCKET=myrecruiter-picasso-staging
FORM_SUBMISSIONS_TABLE=picasso-form-submissions-staging
SMS_USAGE_TABLE=picasso-sms-usage-staging
AWS_REGION=us-east-1
```

**Production**:
```bash
CONFIG_BUCKET=myrecruiter-picasso
FORM_SUBMISSIONS_TABLE=picasso-form-submissions
SMS_USAGE_TABLE=picasso-sms-usage
AWS_REGION=us-east-1
```

## Lambda Configuration

### Runtime Settings

**Runtime**: Node.js 20.x

**Handler**: `index.handler`

**Architecture**: x86_64 (or arm64 for cost savings)

### Memory and Timeout

**Recommended Settings**:
- **Memory**: 512 MB (minimum for streaming)
  - 256 MB may cause OOM with large responses
  - 1024 MB for high-traffic tenants
- **Timeout**: 300 seconds (5 minutes)
  - Required for long streaming responses
  - Allows full form fulfillment completion
- **Ephemeral Storage**: 512 MB (default, sufficient)

### Concurrency

**Reserved Concurrency**: 50-100 (recommended)
- Keeps instances warm
- Reduces cold start latency
- Prevents throttling on traffic spikes

**Provisioned Concurrency**: Optional
- Use for consistent <100ms cold starts
- Cost: ~$0.015 per GB-hour
- Consider for production high-SLA endpoints

### Streaming Configuration

**Critical**: Enable Lambda response streaming

**How to Enable**:

1. **Via AWS Console**:
   - Lambda → Configuration → General configuration
   - Enable "Response streaming"

2. **Via AWS CLI**:
   ```bash
   aws lambda update-function-configuration \
     --function-name Bedrock_Streaming_Handler_Staging \
     --invoke-mode RESPONSE_STREAM
   ```

3. **Via CloudFormation**:
   ```yaml
   BedrockStreamingHandler:
     Type: AWS::Lambda::Function
     Properties:
       InvokeMode: RESPONSE_STREAM
   ```

**Verification**:
```bash
aws lambda get-function-configuration \
  --function-name Bedrock_Streaming_Handler_Staging \
  --query 'InvokeMode'
# Should return: "RESPONSE_STREAM"
```

### Function URL

**Purpose**: Direct HTTPS endpoint for frontend

**How to Create**:

1. **Via AWS Console**:
   - Lambda → Configuration → Function URL
   - Create function URL
   - Auth type: NONE (or IAM for additional security)
   - Configure CORS:
     ```json
     {
       "AllowOrigins": ["*"],
       "AllowMethods": ["POST", "OPTIONS"],
       "AllowHeaders": ["Content-Type", "Authorization", "Accept"],
       "MaxAge": 86400
     }
     ```

2. **Via AWS CLI**:
   ```bash
   aws lambda create-function-url-config \
     --function-name Bedrock_Streaming_Handler_Staging \
     --auth-type NONE \
     --cors '{
       "AllowOrigins": ["*"],
       "AllowMethods": ["POST","OPTIONS"],
       "AllowHeaders": ["Content-Type","Authorization","Accept"],
       "MaxAge": 86400
     }'
   ```

**URL Format**: `https://<url-id>.lambda-url.us-east-1.on.aws/`

**Security Note**: For production, consider:
- Using API Gateway with WAF
- Implementing request signing
- Rate limiting per tenant

## IAM Permissions

### Lambda Execution Role

Create an IAM role with the following policies:

#### 1. Basic Lambda Execution

```json
{
  "Version": "2012-10-17",
  "Statement": [
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

#### 2. S3 Config Access

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject"
      ],
      "Resource": [
        "arn:aws:s3:::myrecruiter-picasso/mappings/*",
        "arn:aws:s3:::myrecruiter-picasso/tenants/*/config.json",
        "arn:aws:s3:::myrecruiter-picasso/tenants/*/*-config.json"
      ]
    }
  ]
}
```

#### 3. Bedrock Access

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel",
        "bedrock:InvokeModelWithResponseStream"
      ],
      "Resource": [
        "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-haiku-*",
        "arn:aws:bedrock:us-east-1::foundation-model/us.anthropic.claude-3-5-haiku-*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "bedrock:Retrieve"
      ],
      "Resource": "arn:aws:bedrock:us-east-1:*:knowledge-base/*"
    }
  ]
}
```

#### 4. DynamoDB Access

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:UpdateItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:us-east-1:*:table/picasso-form-submissions",
        "arn:aws:dynamodb:us-east-1:*:table/picasso-sms-usage"
      ]
    }
  ]
}
```

#### 5. SES Email Sending

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ses:SendEmail",
        "ses:SendRawEmail"
      ],
      "Resource": "*"
    }
  ]
}
```

**Note**: Restrict to specific email addresses if needed:
```json
"Resource": "arn:aws:ses:us-east-1:*:identity/noreply@picasso.ai"
```

#### 6. SNS SMS Sending

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sns:Publish"
      ],
      "Resource": "*"
    }
  ]
}
```

#### 7. Lambda Invocation (for fulfillment)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "arn:aws:lambda:us-east-1:*:function:*Handler*"
    }
  ]
}
```

#### 8. S3 Form Storage (optional)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::tenant-form-submissions/submissions/*/*"
    }
  ]
}
```

### Complete Policy Document

Combine all policies into a single managed policy or inline policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "CloudWatchLogs",
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:*:*:*"
    },
    {
      "Sid": "S3ConfigRead",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": [
        "arn:aws:s3:::myrecruiter-picasso/mappings/*",
        "arn:aws:s3:::myrecruiter-picasso/tenants/*"
      ]
    },
    {
      "Sid": "BedrockInference",
      "Effect": "Allow",
      "Action": [
        "bedrock:InvokeModel",
        "bedrock:InvokeModelWithResponseStream",
        "bedrock:Retrieve"
      ],
      "Resource": [
        "arn:aws:bedrock:us-east-1::foundation-model/*anthropic*",
        "arn:aws:bedrock:us-east-1:*:knowledge-base/*"
      ]
    },
    {
      "Sid": "DynamoDBFormStorage",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:UpdateItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:us-east-1:*:table/picasso-form-submissions",
        "arn:aws:dynamodb:us-east-1:*:table/picasso-sms-usage"
      ]
    },
    {
      "Sid": "SESEmailSending",
      "Effect": "Allow",
      "Action": ["ses:SendEmail", "ses:SendRawEmail"],
      "Resource": "*"
    },
    {
      "Sid": "SNSSMSSending",
      "Effect": "Allow",
      "Action": ["sns:Publish"],
      "Resource": "*"
    },
    {
      "Sid": "LambdaInvocation",
      "Effect": "Allow",
      "Action": ["lambda:InvokeFunction"],
      "Resource": "arn:aws:lambda:us-east-1:*:function:*Handler*"
    },
    {
      "Sid": "S3FormStorage",
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": "arn:aws:s3:::*/submissions/*/*"
    }
  ]
}
```

## Dependencies

### Package Management

**package.json**:
```json
{
  "name": "bedrock-streaming-handler",
  "version": "1.0.0",
  "description": "Bedrock streaming handler with true Lambda response streaming",
  "main": "index.js",
  "engines": {
    "node": ">=20.0.0"
  },
  "dependencies": {
    "@aws-sdk/client-bedrock-agent-runtime": "^3.600.0",
    "@aws-sdk/client-bedrock-runtime": "^3.600.0",
    "@aws-sdk/client-dynamodb": "^3.600.0",
    "@aws-sdk/client-lambda": "^3.600.0",
    "@aws-sdk/client-s3": "^3.600.0",
    "@aws-sdk/client-ses": "^3.600.0",
    "@aws-sdk/client-sns": "^3.600.0",
    "@aws-sdk/lib-dynamodb": "^3.600.0"
  },
  "scripts": {
    "build": "npm ci --production",
    "package": "zip -r deployment.zip index.js response_enhancer.js form_handler.js node_modules package.json",
    "test": "jest",
    "test:coverage": "jest --coverage"
  }
}
```

### AWS SDK v3 Packages

All dependencies use AWS SDK v3 for:
- Smaller bundle size (tree-shakeable)
- Better TypeScript support
- No security vulnerabilities (v2 deprecated)

**Key Packages**:
- `@aws-sdk/client-bedrock-runtime` - Bedrock streaming
- `@aws-sdk/client-bedrock-agent-runtime` - KB retrieval
- `@aws-sdk/client-s3` - Config loading
- `@aws-sdk/lib-dynamodb` - Form storage
- `@aws-sdk/client-ses` - Email sending
- `@aws-sdk/client-sns` - SMS sending
- `@aws-sdk/client-lambda` - Async invocation

## Build and Package

### Step 1: Install Dependencies

```bash
cd /Users/chrismiller/Desktop/Working_Folder/Lambdas/lambda/Bedrock_Streaming_Handler_Staging

# Install production dependencies only
npm ci --production
```

**Output**:
```
added 150 packages in 8s
```

### Step 2: Run Tests (Optional but Recommended)

```bash
# Install dev dependencies for testing
npm install

# Run test suite
npm test

# Check coverage
npm run test:coverage
```

**Expected Coverage**:
- Statements: >90%
- Branches: >85%
- Functions: >90%
- Lines: >90%

### Step 3: Package for Deployment

```bash
# Create deployment zip
npm run package
```

**This creates**: `deployment.zip` containing:
- `index.js`
- `response_enhancer.js`
- `form_handler.js`
- `node_modules/` (production only)
- `package.json`

**Package Size**: ~15-20 MB (compressed)

**Verification**:
```bash
# List contents
unzip -l deployment.zip | head -20

# Check size
ls -lh deployment.zip
```

## Deployment Process

### Method 1: AWS Console

1. **Navigate to Lambda**:
   - Go to AWS Lambda console
   - Find `Bedrock_Streaming_Handler_Staging`

2. **Upload Code**:
   - Code → Upload from → .zip file
   - Select `deployment.zip`
   - Click "Save"

3. **Configure**:
   - Set environment variables (see above)
   - Configure memory/timeout
   - Enable response streaming
   - Create/update function URL

4. **Test**:
   - Use test event or function URL

### Method 2: AWS CLI

```bash
# Update function code
aws lambda update-function-code \
  --function-name Bedrock_Streaming_Handler_Staging \
  --zip-file fileb://deployment.zip \
  --region us-east-1

# Update configuration
aws lambda update-function-configuration \
  --function-name Bedrock_Streaming_Handler_Staging \
  --timeout 300 \
  --memory-size 512 \
  --environment Variables="{
    CONFIG_BUCKET=myrecruiter-picasso,
    FORM_SUBMISSIONS_TABLE=picasso-form-submissions,
    SMS_USAGE_TABLE=picasso-sms-usage,
    AWS_REGION=us-east-1
  }" \
  --region us-east-1

# Enable streaming
aws lambda update-function-configuration \
  --function-name Bedrock_Streaming_Handler_Staging \
  --invoke-mode RESPONSE_STREAM \
  --region us-east-1
```

### Method 3: Automated Script

**deploy.sh**:
```bash
#!/bin/bash

# Configuration
FUNCTION_NAME="Bedrock_Streaming_Handler_Staging"
REGION="us-east-1"

# Build
echo "Building..."
npm ci --production

# Package
echo "Packaging..."
npm run package

# Deploy
echo "Deploying..."
aws lambda update-function-code \
  --function-name $FUNCTION_NAME \
  --zip-file fileb://deployment.zip \
  --region $REGION

# Wait for update to complete
echo "Waiting for deployment..."
aws lambda wait function-updated \
  --function-name $FUNCTION_NAME \
  --region $REGION

# Update configuration
echo "Updating configuration..."
aws lambda update-function-configuration \
  --function-name $FUNCTION_NAME \
  --timeout 300 \
  --memory-size 512 \
  --invoke-mode RESPONSE_STREAM \
  --region $REGION

echo "Deployment complete!"
```

**Usage**:
```bash
chmod +x deploy.sh
./deploy.sh
```

## Testing Before Deployment

### Local Testing (Not Supported)

Lambda response streaming requires the Lambda runtime environment. Local testing is limited to:

1. **Unit Tests**:
   ```bash
   npm test
   ```

2. **Module Tests**:
   ```bash
   node -e "
   const { validateFormField } = require('./form_handler');
   validateFormField('email', 'test@example.com', {})
     .then(result => console.log(result));
   "
   ```

### Staging Environment Testing

Always test in a staging Lambda first:

1. **Deploy to Staging**:
   ```bash
   aws lambda update-function-code \
     --function-name Bedrock_Streaming_Handler_Staging \
     --zip-file fileb://deployment.zip
   ```

2. **Run Integration Tests**:
   ```bash
   # Test normal conversation
   curl -X POST https://staging-url.lambda-url.us-east-1.on.aws/ \
     -H "Content-Type: application/json" \
     -d '{"tenant_hash":"test123","user_input":"Hello"}'

   # Test form validation
   curl -X POST https://staging-url.lambda-url.us-east-1.on.aws/ \
     -H "Content-Type: application/json" \
     -d '{
       "tenant_hash":"test123",
       "form_mode":true,
       "action":"validate_field",
       "field_id":"email",
       "field_value":"test@example.com"
     }'

   # Test form submission
   curl -X POST https://staging-url.lambda-url.us-east-1.on.aws/ \
     -H "Content-Type: application/json" \
     -d '{
       "tenant_hash":"test123",
       "form_mode":true,
       "action":"submit_form",
       "form_id":"volunteer_apply",
       "form_data":{"first_name":"Test","email":"test@example.com"}
     }'
   ```

3. **Check CloudWatch Logs**:
   ```bash
   aws logs tail /aws/lambda/Bedrock_Streaming_Handler_Staging \
     --follow \
     --format short
   ```

## Post-Deployment Validation

### Step 1: Verify Configuration

```bash
# Check function configuration
aws lambda get-function-configuration \
  --function-name Bedrock_Streaming_Handler_Staging \
  --query '{
    Runtime: Runtime,
    Timeout: Timeout,
    Memory: MemorySize,
    InvokeMode: InvokeMode,
    Environment: Environment
  }'
```

**Expected Output**:
```json
{
  "Runtime": "nodejs20.x",
  "Timeout": 300,
  "Memory": 512,
  "InvokeMode": "RESPONSE_STREAM",
  "Environment": {
    "Variables": {
      "CONFIG_BUCKET": "myrecruiter-picasso",
      "FORM_SUBMISSIONS_TABLE": "picasso-form-submissions",
      "SMS_USAGE_TABLE": "picasso-sms-usage"
    }
  }
}
```

### Step 2: Test Normal Conversation

```bash
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -d '{
    "tenant_hash": "abc123",
    "user_input": "What volunteer opportunities do you have?"
  }'
```

**Expected**: SSE stream with text chunks

### Step 3: Test Form Validation

```bash
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_hash": "abc123",
    "form_mode": true,
    "action": "validate_field",
    "field_id": "email",
    "field_value": "invalid-email"
  }'
```

**Expected**:
```
data: {"type":"validation_error","field":"email","errors":["Please enter a valid email address"],"status":"error"}

data: [DONE]
```

### Step 4: Test Form Submission

```bash
curl -X POST https://xyz.lambda-url.us-east-1.on.aws/ \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_hash": "abc123",
    "form_mode": true,
    "action": "submit_form",
    "form_id": "volunteer_apply",
    "form_data": {
      "first_name": "Test",
      "last_name": "User",
      "email": "test@example.com",
      "urgency": "normal"
    }
  }'
```

**Expected**:
```
data: {"type":"form_complete","status":"success","submissionId":"volunteer_apply_1696184900123","priority":"normal","fulfillment":[...]}

data: [DONE]
```

### Step 5: Check CloudWatch Logs

```bash
# View recent logs
aws logs tail /aws/lambda/Bedrock_Streaming_Handler_Staging \
  --since 5m \
  --format short

# Search for errors
aws logs filter-log-events \
  --log-group-name /aws/lambda/Bedrock_Streaming_Handler_Staging \
  --filter-pattern "ERROR" \
  --since 1h
```

**Look For**:
- ✅ "Lambda streaming support detected"
- ✅ "Config loaded from S3"
- ✅ "KB context retrieved"
- ✅ "Form saved to DynamoDB"
- ❌ Any error messages (investigate if found)

### Step 6: Verify Fulfillment Channels

**Check DynamoDB**:
```bash
aws dynamodb scan \
  --table-name picasso-form-submissions \
  --limit 5 \
  --query 'Items[*].[submission_id.S, status.S, priority.S]'
```

**Check SES Sending**:
- Look for confirmation emails in test inbox
- Check SES sending statistics in AWS Console

**Check SNS SMS**:
- Verify SMS received (if configured)
- Check SNS publish logs

### Step 7: Monitor Metrics

**CloudWatch Metrics to Monitor**:

1. **Invocations**: Should match expected traffic
2. **Errors**: Should be <1%
3. **Duration**: p95 should be <5s for normal mode
4. **Concurrent Executions**: Should not hit limits
5. **Throttles**: Should be 0

**View Metrics**:
```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --dimensions Name=FunctionName,Value=Bedrock_Streaming_Handler_Staging \
  --start-time $(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 300 \
  --statistics Sum
```

## Rollback Procedure

### If Deployment Fails

1. **Identify Last Working Version**:
   ```bash
   aws lambda list-versions-by-function \
     --function-name Bedrock_Streaming_Handler_Staging \
     --query 'Versions[*].[Version, LastModified]'
   ```

2. **Create Alias to Previous Version**:
   ```bash
   aws lambda update-alias \
     --function-name Bedrock_Streaming_Handler_Staging \
     --name live \
     --function-version 12  # Previous working version
   ```

3. **Update Function URL to Use Alias**:
   ```bash
   aws lambda update-function-url-config \
     --function-name Bedrock_Streaming_Handler_Staging:live \
     --auth-type NONE
   ```

### Emergency Rollback Script

**rollback.sh**:
```bash
#!/bin/bash

FUNCTION_NAME="Bedrock_Streaming_Handler_Staging"
PREVIOUS_VERSION=$1

if [ -z "$PREVIOUS_VERSION" ]; then
  echo "Usage: ./rollback.sh <version_number>"
  exit 1
fi

echo "Rolling back to version $PREVIOUS_VERSION..."

aws lambda update-alias \
  --function-name $FUNCTION_NAME \
  --name live \
  --function-version $PREVIOUS_VERSION

echo "Rollback complete. Verify at:"
aws lambda get-alias \
  --function-name $FUNCTION_NAME \
  --name live \
  --query 'FunctionVersion'
```

## Monitoring and Logging

### CloudWatch Log Groups

**Log Group**: `/aws/lambda/Bedrock_Streaming_Handler_Staging`

**Retention**: 30 days (adjust as needed)

**Key Log Patterns**:

1. **Successful Requests**:
   ```
   ✅ Config loaded from S3
   ✅ KB context retrieved
   ✅ Complete - 125 tokens in 2341ms
   ```

2. **Form Submissions**:
   ```
   📝 Form mode detected
   ✅ Form saved to DynamoDB with priority: normal
   ✅ Form email sent to team@example.com
   ```

3. **Errors**:
   ```
   ❌ Config load error: Access denied
   ❌ KB retrieval error: Timeout
   ❌ Form submission error: Missing parameters
   ```

### Structured Logging

**Q&A Complete Event**:
```json
{
  "type": "QA_COMPLETE",
  "timestamp": "2025-10-01T18:45:23.000Z",
  "session_id": "session_abc123",
  "tenant_hash": "abc123",
  "tenant_id": "xyz789",
  "conversation_id": "conv_abc123",
  "question": "What volunteer opportunities exist?",
  "answer": "We offer several volunteer programs...",
  "metrics": {
    "first_token_ms": 234,
    "total_tokens": 125,
    "total_time_ms": 2341,
    "answer_length": 456
  }
}
```

**Use for**:
- Analytics dashboards
- Performance monitoring
- Conversation quality tracking

### CloudWatch Alarms

**Recommended Alarms**:

1. **High Error Rate**:
   ```bash
   aws cloudwatch put-metric-alarm \
     --alarm-name Lambda-ErrorRate-High \
     --alarm-description "Lambda error rate >5%" \
     --metric-name Errors \
     --namespace AWS/Lambda \
     --statistic Average \
     --period 300 \
     --threshold 0.05 \
     --comparison-operator GreaterThanThreshold \
     --dimensions Name=FunctionName,Value=Bedrock_Streaming_Handler_Staging \
     --evaluation-periods 2
   ```

2. **High Latency**:
   ```bash
   aws cloudwatch put-metric-alarm \
     --alarm-name Lambda-Duration-High \
     --metric-name Duration \
     --namespace AWS/Lambda \
     --statistic Average \
     --period 300 \
     --threshold 5000 \
     --comparison-operator GreaterThanThreshold \
     --dimensions Name=FunctionName,Value=Bedrock_Streaming_Handler_Staging \
     --evaluation-periods 2
   ```

3. **Throttling**:
   ```bash
   aws cloudwatch put-metric-alarm \
     --alarm-name Lambda-Throttles \
     --metric-name Throttles \
     --namespace AWS/Lambda \
     --statistic Sum \
     --period 300 \
     --threshold 10 \
     --comparison-operator GreaterThanThreshold \
     --dimensions Name=FunctionName,Value=Bedrock_Streaming_Handler_Staging \
     --evaluation-periods 1
   ```

## Troubleshooting

### Issue: Deployment Package Too Large

**Symptoms**: "Request entity too large" error

**Solution**:
1. Use `--production` flag: `npm ci --production`
2. Remove dev dependencies from package
3. Consider Lambda layers for large dependencies

### Issue: Streaming Not Working

**Symptoms**: Buffered response instead of streaming

**Solution**:
1. Verify `InvokeMode: RESPONSE_STREAM` is set
2. Check handler exports `streamifyResponse(streamingHandler)`
3. Ensure function URL (not API Gateway) is used

### Issue: Bedrock Access Denied

**Symptoms**: "User is not authorized to perform: bedrock:InvokeModel"

**Solution**:
1. Add Bedrock permissions to Lambda execution role
2. Ensure model ARN matches region and ID
3. Request model access in AWS Bedrock console if needed

### Issue: Form Submissions Not Saving

**Symptoms**: Forms complete but not in DynamoDB

**Solution**:
1. Check `FORM_SUBMISSIONS_TABLE` environment variable
2. Verify DynamoDB table exists
3. Ensure Lambda role has `dynamodb:PutItem` permission
4. Check CloudWatch logs for DynamoDB errors

### Issue: SMS Not Sending

**Symptoms**: SMS status "skipped" in fulfillment

**Solution**:
1. Check monthly limit: `usage` vs `limit` in response
2. Verify phone number in E.164 format (+1XXXXXXXXXX)
3. Ensure Lambda role has `sns:Publish` permission
4. Check AWS account SMS spend limits

## Production Deployment Checklist

Before deploying to production:

- [ ] All tests passing (`npm test`)
- [ ] Code coverage >85%
- [ ] Deployed to staging and tested
- [ ] Environment variables configured
- [ ] IAM permissions verified
- [ ] Response streaming enabled
- [ ] Function URL created with CORS
- [ ] CloudWatch alarms configured
- [ ] Log retention set appropriately
- [ ] Rollback plan documented
- [ ] Team notified of deployment
- [ ] Monitoring dashboard ready
- [ ] On-call engineer assigned

## Maintenance

### Regular Tasks

**Weekly**:
- Review error logs for patterns
- Check CloudWatch metrics for anomalies
- Verify SMS usage vs limits

**Monthly**:
- Update dependencies (`npm update`)
- Review and optimize Lambda memory
- Audit IAM permissions (principle of least privilege)
- Reset SMS usage counters (automatic)

**Quarterly**:
- Performance optimization review
- Cost analysis and optimization
- Disaster recovery test
- Security audit

### Updating Dependencies

```bash
# Check for updates
npm outdated

# Update patch versions
npm update

# Update major versions (test thoroughly!)
npm install @aws-sdk/client-bedrock-runtime@latest

# Test
npm test

# Redeploy
npm run package
./deploy.sh
```

---

**Document Version**: 1.0
**Last Updated**: 2025-10-01
**Maintained By**: DevOps Engineering Team
