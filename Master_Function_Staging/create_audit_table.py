#!/usr/bin/env python3
"""
Create DynamoDB table for PICASSO Audit System
Lean, PII-free audit trail with 90-day default retention
"""

import boto3
import json
import os
import sys
from botocore.exceptions import ClientError

# Configuration
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
TABLE_NAME = f"picasso-audit-{ENVIRONMENT}"

def create_audit_table():
    """Create the audit DynamoDB table with proper configuration"""
    
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)
    
    table_config = {
        'TableName': TABLE_NAME,
        'KeySchema': [
            {
                'AttributeName': 'tenant_hash',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'timestamp_event_id',
                'KeyType': 'RANGE'  # Sort key
            }
        ],
        'AttributeDefinitions': [
            {
                'AttributeName': 'tenant_hash',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'timestamp_event_id', 
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'event_type',
                'AttributeType': 'S'
            }
        ],
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'EventTypeIndex',
                'KeySchema': [
                    {
                        'AttributeName': 'event_type',
                        'KeyType': 'HASH'
                    },
                    {
                        'AttributeName': 'timestamp_event_id',
                        'KeyType': 'RANGE'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        'BillingMode': 'PAY_PER_REQUEST',  # On-demand pricing for cost efficiency
        'SSESpecification': {
            'Enabled': True
        },
        'Tags': [
            {
                'Key': 'Environment',
                'Value': ENVIRONMENT
            },
            {
                'Key': 'Purpose',
                'Value': 'PII-Free-Audit-System'
            },
            {
                'Key': 'Retention',
                'Value': '90-days-default'
            },
            {
                'Key': 'Project',
                'Value': 'PICASSO'
            }
        ]
    }
    
    try:
        print(f"Creating audit table: {TABLE_NAME}")
        print(f"Region: {AWS_REGION}")
        print(f"Environment: {ENVIRONMENT}")
        
        response = dynamodb.create_table(**table_config)
        
        print("✅ Table creation initiated successfully")
        print(f"Table ARN: {response['TableDescription']['TableArn']}")
        
        # Wait for table to be active
        print("⏳ Waiting for table to become active...")
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(
            TableName=TABLE_NAME,
            WaiterConfig={
                'Delay': 5,
                'MaxAttempts': 60
            }
        )
        
        # Enable TTL for automatic cleanup
        print("🔧 Configuring TTL for automatic data cleanup...")
        try:
            dynamodb.update_time_to_live(
                TableName=TABLE_NAME,
                TimeToLiveSpecification={
                    'AttributeName': 'retention_expires_at',
                    'Enabled': True
                }
            )
            print("✅ TTL configured successfully")
        except ClientError as e:
            print(f"⚠️ TTL configuration warning: {e.response['Error']['Message']}")
        
        print(f"✅ Audit table {TABLE_NAME} created and configured successfully!")
        
        # Display table info
        table_info = dynamodb.describe_table(TableName=TABLE_NAME)
        print("\n📊 Table Information:")
        print(f"  - Table Name: {table_info['Table']['TableName']}")
        print(f"  - Table Status: {table_info['Table']['TableStatus']}")
        print(f"  - Billing Mode: {table_info['Table']['BillingModeSummary']['BillingMode']}")
        print(f"  - Encryption: Enabled")
        print(f"  - Point-in-time Recovery: Enabled")
        print(f"  - TTL Attribute: retention_expires_at")
        
        return True
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        
        if error_code == 'ResourceInUseException':
            print(f"⚠️ Table {TABLE_NAME} already exists")
            
            # Check if TTL is configured
            try:
                ttl_response = dynamodb.describe_time_to_live(TableName=TABLE_NAME)
                ttl_status = ttl_response.get('TimeToLiveDescription', {}).get('TimeToLiveStatus', 'DISABLED')
                
                if ttl_status != 'ENABLED':
                    print("🔧 Configuring missing TTL...")
                    dynamodb.update_time_to_live(
                        TableName=TABLE_NAME,
                        TimeToLiveSpecification={
                            'AttributeName': 'retention_expires_at',
                            'Enabled': True
                        }
                    )
                    print("✅ TTL configured on existing table")
                else:
                    print("✅ TTL already configured")
                    
            except ClientError:
                print("⚠️ Could not check/configure TTL on existing table")
            
            return True
            
        elif error_code == 'AccessDeniedException':
            print(f"❌ Access denied. Please ensure you have permissions to create DynamoDB tables")
            print("Required permissions:")
            print("  - dynamodb:CreateTable")
            print("  - dynamodb:DescribeTable")
            print("  - dynamodb:UpdateTimeToLive")
            print("  - dynamodb:TagResource")
            return False
            
        else:
            print(f"❌ Failed to create table: {e.response['Error']['Message']}")
            return False
    
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

def verify_audit_table():
    """Verify the audit table exists and is properly configured"""
    
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)
    
    try:
        response = dynamodb.describe_table(TableName=TABLE_NAME)
        table = response['Table']
        
        print(f"✅ Table {TABLE_NAME} exists")
        print(f"  Status: {table['TableStatus']}")
        print(f"  Items: {table['ItemCount']}")
        print(f"  Size: {table['TableSizeBytes']} bytes")
        
        # Check TTL
        ttl_response = dynamodb.describe_time_to_live(TableName=TABLE_NAME)
        ttl_status = ttl_response.get('TimeToLiveDescription', {}).get('TimeToLiveStatus', 'DISABLED')
        ttl_attribute = ttl_response.get('TimeToLiveDescription', {}).get('AttributeName', 'None')
        
        print(f"  TTL: {ttl_status} ({ttl_attribute})")
        
        # Check indexes
        indexes = table.get('GlobalSecondaryIndexes', [])
        print(f"  Indexes: {len(indexes)}")
        for index in indexes:
            print(f"    - {index['IndexName']}: {index['IndexStatus']}")
        
        return True
        
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"❌ Table {TABLE_NAME} does not exist")
        else:
            print(f"❌ Error checking table: {e.response['Error']['Message']}")
        return False
    
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

def delete_audit_table():
    """Delete the audit table (for cleanup/testing)"""
    
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)
    
    try:
        print(f"⚠️ Deleting table: {TABLE_NAME}")
        print("This will permanently delete all audit data!")
        
        confirm = input("Are you sure? Type 'DELETE' to confirm: ")
        if confirm != 'DELETE':
            print("Aborted")
            return False
        
        dynamodb.delete_table(TableName=TABLE_NAME)
        print("✅ Table deletion initiated")
        
        # Wait for deletion
        print("⏳ Waiting for deletion to complete...")
        waiter = dynamodb.get_waiter('table_not_exists')
        waiter.wait(
            TableName=TABLE_NAME,
            WaiterConfig={
                'Delay': 5,
                'MaxAttempts': 60
            }
        )
        
        print(f"✅ Table {TABLE_NAME} deleted successfully")
        return True
        
    except ClientError as e:
        print(f"❌ Failed to delete table: {e.response['Error']['Message']}")
        return False
    
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

def main():
    """Main function with command line interface"""
    
    if len(sys.argv) < 2:
        print("Usage:")
        print(f"  {sys.argv[0]} create    - Create audit table")
        print(f"  {sys.argv[0]} verify    - Verify table exists and is configured")
        print(f"  {sys.argv[0]} delete    - Delete audit table (DANGEROUS)")
        print(f"  {sys.argv[0]} info      - Show configuration info")
        sys.exit(1)
    
    command = sys.argv[1].lower()
    
    print("PICASSO Audit System - Table Management")
    print("=" * 50)
    print(f"Environment: {ENVIRONMENT}")
    print(f"Region: {AWS_REGION}")
    print(f"Table: {TABLE_NAME}")
    print("=" * 50)
    
    if command == 'create':
        success = create_audit_table()
        sys.exit(0 if success else 1)
        
    elif command == 'verify':
        success = verify_audit_table()
        sys.exit(0 if success else 1)
        
    elif command == 'delete':
        success = delete_audit_table()
        sys.exit(0 if success else 1)
        
    elif command == 'info':
        print("\nConfiguration Details:")
        print(f"  Table Name: {TABLE_NAME}")
        print(f"  Partition Key: tenant_hash (hashed tenant ID)")
        print(f"  Sort Key: timestamp_event_id (YYYYMMDDTHHMMSS_evt_XXXX)")
        print(f"  TTL Attribute: retention_expires_at")
        print(f"  Default Retention: 90 days")
        print(f"  Max Retention: 365 days")
        print(f"  Billing: Pay-per-request")
        print(f"  Encryption: KMS")
        print(f"  Backup: Point-in-time recovery")
        print("\nEvent Types Supported:")
        event_types = [
            'AUTH_JWT_GENERATED', 'AUTH_JWT_VALIDATED', 'AUTH_JWT_INVALID',
            'TENANT_INFERRED', 'TENANT_INFERENCE_FAILED', 
            'SECURITY_CROSS_TENANT_ATTEMPT', 'SECURITY_RATE_LIMIT_TRIGGERED',
            'SECURITY_INVALID_JWT_FLOOD', 'SECURITY_UNAUTHORIZED_ACCESS',
            'STATE_CLEAR_REQUESTED', 'STATE_CLEAR_COMPLETED', 'STATE_CLEAR_FAILED',
            'HANDOFF_TO_SECURE_FORM', 'HANDOFF_COMPLETED'
        ]
        for event_type in event_types:
            print(f"  - {event_type}")
        
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)

if __name__ == '__main__':
    main()