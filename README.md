# Lambda Functions Repository

This repository contains all Lambda functions for the MyRecruiter Picasso chat widget system.

## Functions

### 1. Master_Function_Staging
- **Purpose**: Main orchestrator for chat interactions
- **Runtime**: Python 3.x
- **Features**:
  - JWT authentication
  - Multi-tenant configuration
  - Conversation state management
  - Bedrock AI integration
  - Audit logging

### 2. Bedrock_Streaming_Handler_Staging
- **Purpose**: Handles SSE streaming for real-time chat responses
- **Runtime**: Node.js 20.x
- **Features**:
  - True Lambda response streaming
  - Knowledge Base integration
  - In-memory caching (5-min TTL)
  - Claude 3.5 Haiku model

### 3. Analytics_Function
- **Purpose**: Analytics and metrics collection
- **Runtime**: Python 3.x
- **Features**:
  - CloudWatch metrics reader
  - Tenant resolution
  - Usage analytics

### 4. Aggregator_Function
- **Purpose**: Data aggregation and processing
- **Runtime**: Python 3.x

## Deployment

Each function should be deployed independently to AWS Lambda.

### Environment Variables Required
- `S3_CONFIG_BUCKET`: Tenant configuration bucket
- `DYNAMODB_AUDIT_TABLE`: Audit log table
- `DYNAMODB_BLACKLIST_TABLE`: Token blacklist table
- `BEDROCK_MODEL_ID`: AI model identifier

## Development

### Python Functions
```bash
cd [function_name]
zip -r deployment.zip . -x "*.pyc" -x "__pycache__/*"
aws lambda update-function-code --function-name [function_name] --zip-file fileb://deployment.zip
```

### Node.js Functions
```bash
cd Bedrock_Streaming_Handler_Staging
npm ci --production
zip -r deployment.zip .
aws lambda update-function-code --function-name Bedrock_Streaming_Handler_Staging --zip-file fileb://deployment.zip
```
