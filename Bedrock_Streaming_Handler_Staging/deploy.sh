#!/bin/bash

# Deployment script for Bedrock_Streaming_Handler_Staging Lambda
# This Lambda includes the response_enhancer.js for CTA injection

echo "🚀 Deploying Bedrock_Streaming_Handler_Staging..."

# Package the Lambda
echo "📦 Creating deployment package..."
zip -r deployment.zip index.js response_enhancer.js node_modules package.json

# Deploy to AWS
echo "☁️ Uploading to AWS Lambda..."
aws lambda update-function-code \
    --function-name Bedrock_Streaming_Handler_Staging \
    --zip-file fileb://deployment.zip \
    --region us-east-1

if [ $? -eq 0 ]; then
    echo "✅ Deployment successful!"
    echo "🔍 Checking function status..."
    aws lambda get-function-configuration \
        --function-name Bedrock_Streaming_Handler_Staging \
        --region us-east-1 \
        --query '{LastModified:LastModified,CodeSize:CodeSize,Version:Version}' \
        --output json
else
    echo "❌ Deployment failed!"
    exit 1
fi

echo "🎯 CTA enhancement is now active in the Lambda function"