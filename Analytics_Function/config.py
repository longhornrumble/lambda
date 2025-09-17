import os

ENVIRONMENT = os.environ.get('ENVIRONMENT', 'development')

CONFIG_BUCKET = os.environ.get('CONFIG_BUCKET', 'myrecruiter-picasso')

# Determine log group names based on environment
if ENVIRONMENT == 'staging':
    LOG_GROUP_STREAMING = os.environ.get('LOG_GROUP_STREAMING', '/aws/lambda/Bedrock_Streaming_Handler_Staging')
    LOG_GROUP_MASTER = os.environ.get('LOG_GROUP_MASTER', '/aws/lambda/Master_Function_Staging')
else:
    LOG_GROUP_STREAMING = os.environ.get('LOG_GROUP_STREAMING', '/aws/lambda/Bedrock_Streaming_Handler')
    LOG_GROUP_MASTER = os.environ.get('LOG_GROUP_MASTER', '/aws/lambda/Master_Function')

CACHE_TTL_SECONDS = int(os.environ.get('CACHE_TTL_SECONDS', '300'))

AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

MAX_QUERY_RESULTS = int(os.environ.get('MAX_QUERY_RESULTS', '10000'))