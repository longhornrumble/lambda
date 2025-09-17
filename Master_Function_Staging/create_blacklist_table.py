"""
DynamoDB Table Creation Script for PICASSO Token Blacklist System
Creates healthcare-grade token blacklist table with proper TTL and indexing

Usage:
    python create_blacklist_table.py [environment]
    
Examples:
    python create_blacklist_table.py staging
    python create_blacklist_table.py production
"""

import boto3
import sys
import json
import time
from botocore.exceptions import ClientError

def create_token_blacklist_table(environment='staging', region='us-east-1'):
    """
    Create DynamoDB table for token blacklisting with optimized structure
    
    Table Design:
    - Partition Key: token_hash (SHA256 of JWT token)
    - TTL: expires_at (automatic cleanup of expired tokens)
    - GSI: tenant_id-blacklisted_at-index (for tenant-specific queries)
    - Point-in-time recovery enabled (healthcare compliance)
    - Encryption at rest enabled (security requirement)
    """
    
    table_name = f'picasso-token-blacklist-{environment}'
    
    # Initialize DynamoDB client
    dynamodb = boto3.client('dynamodb', region_name=region)
    
    print(f"🚀 Creating token blacklist table: {table_name}")
    print(f"📍 Region: {region}")
    print(f"🏥 Environment: {environment}")
    
    try:
        # Check if table already exists
        try:
            existing_table = dynamodb.describe_table(TableName=table_name)
            print(f"⚠️ Table {table_name} already exists!")
            print(f"📊 Current status: {existing_table['Table']['TableStatus']}")
            
            # Ask user if they want to continue
            response = input("Do you want to continue and update the existing table? (y/N): ")
            if response.lower() != 'y':
                print("❌ Operation cancelled by user")
                return False
                
        except ClientError as e:
            if e.response['Error']['Code'] != 'ResourceNotFoundException':
                raise
            print("✅ Table does not exist, proceeding with creation...")
        
        # Table creation parameters
        table_params = {
            'TableName': table_name,
            'KeySchema': [
                {
                    'AttributeName': 'token_hash',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            'AttributeDefinitions': [
                {
                    'AttributeName': 'token_hash',
                    'AttributeType': 'S'  # String
                },
                {
                    'AttributeName': 'tenant_id',
                    'AttributeType': 'S'  # String
                },
                {
                    'AttributeName': 'blacklisted_at',
                    'AttributeType': 'S'  # String (ISO 8601 timestamp)
                }
            ],
            'BillingMode': 'PAY_PER_REQUEST',  # On-demand pricing for variable healthcare workloads
            'GlobalSecondaryIndexes': [
                {
                    'IndexName': 'tenant-id-blacklisted-at-index',
                    'KeySchema': [
                        {
                            'AttributeName': 'tenant_id',
                            'KeyType': 'HASH'  # Partition key for GSI
                        },
                        {
                            'AttributeName': 'blacklisted_at',
                            'KeyType': 'RANGE'  # Sort key for GSI
                        }
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL'  # Include all attributes in index
                    }
                }
            ],
            'SSESpecification': {
                'Enabled': True,
                'SSEType': 'KMS',
                'KMSMasterKeyId': 'alias/aws/dynamodb'  # AWS managed key for encryption
            },
            'PointInTimeRecoverySpecification': {
                'PointInTimeRecoveryEnabled': True  # Healthcare compliance requirement
            },
            'Tags': [
                {
                    'Key': 'Environment',
                    'Value': environment
                },
                {
                    'Key': 'Service',
                    'Value': 'picasso-token-blacklist'
                },
                {
                    'Key': 'Purpose',
                    'Value': 'healthcare-jwt-revocation'
                },
                {
                    'Key': 'Compliance',
                    'Value': 'healthcare-security'
                },
                {
                    'Key': 'DataClassification',
                    'Value': 'security-tokens'
                }
            ]
        }
        
        # Create the table
        print("🔨 Creating DynamoDB table...")
        response = dynamodb.create_table(**table_params)
        
        print(f"✅ Table creation initiated successfully!")
        print(f"📋 Table ARN: {response['TableDescription']['TableArn']}")
        
        # Wait for table to become active
        print("⏳ Waiting for table to become active...")
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(
            TableName=table_name,
            WaiterConfig={
                'Delay': 5,  # Check every 5 seconds
                'MaxAttempts': 60  # Wait up to 5 minutes
            }
        )
        
        # Enable TTL for automatic token cleanup
        print("🕒 Configuring TTL for automatic token expiry...")
        try:
            dynamodb.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={
                    'AttributeName': 'expires_at',
                    'Enabled': True
                }
            )
            print("✅ TTL configured successfully on 'expires_at' attribute")
        except ClientError as e:
            print(f"⚠️ TTL configuration warning: {e}")
            print("💡 TTL can be configured manually in AWS Console if needed")
        
        # Get final table description
        table_description = dynamodb.describe_table(TableName=table_name)
        table_info = table_description['Table']
        
        print("\n🎉 Token Blacklist Table Created Successfully!")
        print("=" * 60)
        print(f"📊 Table Name: {table_info['TableName']}")
        print(f"📈 Table Status: {table_info['TableStatus']}")
        print(f"🔑 Partition Key: token_hash (String)")
        print(f"📅 TTL Attribute: expires_at")
        print(f"🔍 GSI: tenant-id-blacklisted-at-index")
        print(f"🛡️ Encryption: Enabled (KMS)")
        print(f"⏰ Point-in-Time Recovery: Enabled")
        print(f"💰 Billing Mode: Pay Per Request")
        print(f"🏷️ Environment: {environment}")
        
        # Print usage examples
        print("\n📖 Usage Examples:")
        print("=" * 40)
        print("1. Add token to blacklist:")
        print(f"   POST /?action=revoke_token&t=TENANT_HASH")
        print('   Body: {"token": "JWT_TOKEN", "reason": "user_logout"}')
        print()
        print("2. Check blacklist status:")
        print(f"   GET /?action=blacklist_status&t=TENANT_HASH")
        print()
        print("3. Tenant-wide revocation:")
        print(f"   POST /?action=revoke_token&t=TENANT_HASH")
        print('   Body: {"type": "tenant_wide", "reason": "security_incident"}')
        
        print("\n🔧 Environment Configuration:")
        print("=" * 40)
        print(f"Set environment variable: BLACKLIST_TABLE_NAME={table_name}")
        print(f"Ensure Lambda has permissions for: {table_info['TableArn']}")
        print(f"Consider setting up CloudWatch alarms for table metrics")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        
        print(f"❌ AWS Error: {error_code}")
        print(f"💬 Message: {error_message}")
        
        if error_code == 'ValidationException':
            print("💡 Suggestion: Check if table name follows AWS naming conventions")
        elif error_code == 'AccessDeniedException':
            print("💡 Suggestion: Ensure AWS credentials have DynamoDB permissions")
        elif error_code == 'LimitExceededException':
            print("💡 Suggestion: You may have reached the table limit for your account")
        
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

def delete_token_blacklist_table(environment, region='us-east-1'):
    """
    Delete token blacklist table (use with extreme caution)
    """
    table_name = f'picasso-token-blacklist-{environment}'
    dynamodb = boto3.client('dynamodb', region_name=region)
    
    print(f"⚠️ WARNING: About to delete table {table_name}")
    print("🚨 This action is IRREVERSIBLE and will permanently delete all blacklisted tokens!")
    
    # Triple confirmation for safety
    confirmations = [
        f"Type the table name to confirm: {table_name}",
        "Type 'DELETE' to confirm deletion: DELETE",
        f"Type the environment name to confirm: {environment}"
    ]
    
    for confirmation in confirmations:
        response = input(f"🔒 {confirmation}\n> ")
        expected = confirmation.split(": ")[1]
        if response != expected:
            print("❌ Confirmation failed. Deletion cancelled.")
            return False
    
    try:
        print(f"🗑️ Deleting table {table_name}...")
        dynamodb.delete_table(TableName=table_name)
        
        print("⏳ Waiting for table deletion to complete...")
        waiter = dynamodb.get_waiter('table_not_exists')
        waiter.wait(TableName=table_name)
        
        print(f"✅ Table {table_name} deleted successfully")
        return True
        
    except ClientError as e:
        print(f"❌ Failed to delete table: {e}")
        return False

def main():
    """Main function to handle command line arguments"""
    
    if len(sys.argv) < 2:
        print("📋 PICASSO Token Blacklist Table Management")
        print("=" * 50)
        print("Usage:")
        print("  python create_blacklist_table.py <environment> [action]")
        print()
        print("Examples:")
        print("  python create_blacklist_table.py staging")
        print("  python create_blacklist_table.py production")
        print("  python create_blacklist_table.py staging delete")
        print()
        print("Environments: staging, production, development, testing")
        return
    
    environment = sys.argv[1]
    action = sys.argv[2] if len(sys.argv) > 2 else 'create'
    
    # Validate environment
    valid_environments = ['staging', 'production', 'development', 'testing', 'dev', 'prod']
    if environment not in valid_environments:
        print(f"❌ Invalid environment: {environment}")
        print(f"✅ Valid environments: {', '.join(valid_environments)}")
        return
    
    # Get AWS region from environment or use default
    import os
    region = os.environ.get('AWS_REGION', 'us-east-1')
    
    print(f"🌍 AWS Region: {region}")
    print(f"🏥 Environment: {environment}")
    print(f"⚙️ Action: {action}")
    print()
    
    if action.lower() == 'delete':
        success = delete_token_blacklist_table(environment, region)
    else:
        success = create_token_blacklist_table(environment, region)
    
    if success:
        print("\n🎉 Operation completed successfully!")
    else:
        print("\n❌ Operation failed!")
        sys.exit(1)

if __name__ == '__main__':
    main()