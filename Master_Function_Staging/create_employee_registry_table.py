#!/usr/bin/env python3
"""
DynamoDB Table Creation Script for PICASSO Employee Registry
Queryable directory of tenant employees — Clerk remains auth SoR.

Usage:
    python create_employee_registry_table.py create
    python create_employee_registry_table.py verify
    python create_employee_registry_table.py delete

Environment variables:
    ENVIRONMENT  - staging (default) or production
    AWS_REGION   - us-east-1 (default)
"""

import boto3
import json
import os
import sys
from botocore.exceptions import ClientError

# Configuration
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')
TABLE_NAME = f"picasso-employee-registry-{ENVIRONMENT}"


def create_employee_registry_table():
    """Create the employee registry DynamoDB table with proper configuration"""

    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)

    table_config = {
        'TableName': TABLE_NAME,
        'KeySchema': [
            {
                'AttributeName': 'tenantId',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'clerkUserId',
                'KeyType': 'RANGE'
            }
        ],
        'AttributeDefinitions': [
            {
                'AttributeName': 'tenantId',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'clerkUserId',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'email',
                'AttributeType': 'S'
            }
        ],
        'GlobalSecondaryIndexes': [
            {
                'IndexName': 'EmailIndex',
                'KeySchema': [
                    {
                        'AttributeName': 'email',
                        'KeyType': 'HASH'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            },
            {
                'IndexName': 'ClerkUserIdIndex',
                'KeySchema': [
                    {
                        'AttributeName': 'clerkUserId',
                        'KeyType': 'HASH'
                    }
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                }
            }
        ],
        'BillingMode': 'PAY_PER_REQUEST',
        'SSESpecification': {
            'Enabled': True,
            'SSEType': 'KMS',
            'KMSMasterKeyId': 'alias/aws/dynamodb'
        },
        'Tags': [
            {
                'Key': 'Environment',
                'Value': ENVIRONMENT
            },
            {
                'Key': 'Purpose',
                'Value': 'employee-registry'
            },
            {
                'Key': 'Project',
                'Value': 'PICASSO'
            }
        ]
    }

    try:
        print(f"Creating employee registry table: {TABLE_NAME}")
        print(f"Region: {AWS_REGION}")
        print(f"Environment: {ENVIRONMENT}")

        response = dynamodb.create_table(**table_config)

        print("Table creation initiated successfully")
        print(f"Table ARN: {response['TableDescription']['TableArn']}")

        # Wait for table to be active
        print("Waiting for table to become active...")
        waiter = dynamodb.get_waiter('table_exists')
        waiter.wait(
            TableName=TABLE_NAME,
            WaiterConfig={
                'Delay': 5,
                'MaxAttempts': 60
            }
        )

        # Enable Point-in-Time Recovery
        print("Enabling Point-in-Time Recovery...")
        try:
            dynamodb.update_continuous_backups(
                TableName=TABLE_NAME,
                PointInTimeRecoverySpecification={
                    'PointInTimeRecoveryEnabled': True
                }
            )
            print("Point-in-Time Recovery enabled")
        except ClientError as e:
            print(f"PITR configuration warning: {e.response['Error']['Message']}")

        # No TTL — employee records are persistent

        print(f"Employee registry table {TABLE_NAME} created and configured successfully!")

        # Display table info
        table_info = dynamodb.describe_table(TableName=TABLE_NAME)
        table = table_info['Table']
        print(f"\nTable Information:")
        print(f"  Table Name: {table['TableName']}")
        print(f"  Table Status: {table['TableStatus']}")
        print(f"  Billing Mode: {table['BillingModeSummary']['BillingMode']}")
        print(f"  Encryption: KMS")
        print(f"  Point-in-Time Recovery: Enabled")
        print(f"  TTL: None (persistent records)")
        print(f"  GSIs: EmailIndex, ClerkUserIdIndex")

        return True

    except ClientError as e:
        error_code = e.response['Error']['Code']

        if error_code == 'ResourceInUseException':
            print(f"Table {TABLE_NAME} already exists")
            return True

        elif error_code == 'AccessDeniedException':
            print("Access denied. Required permissions:")
            print("  - dynamodb:CreateTable")
            print("  - dynamodb:DescribeTable")
            print("  - dynamodb:UpdateContinuousBackups")
            print("  - dynamodb:TagResource")
            return False

        else:
            print(f"Failed to create table: {e.response['Error']['Message']}")
            return False

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return False


def verify_employee_registry_table():
    """Verify the employee registry table exists and is properly configured"""

    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)

    try:
        response = dynamodb.describe_table(TableName=TABLE_NAME)
        table = response['Table']

        print(f"Table {TABLE_NAME} exists")
        print(f"  Status: {table['TableStatus']}")
        print(f"  Items: {table['ItemCount']}")
        print(f"  Size: {table['TableSizeBytes']} bytes")

        # Check PITR
        try:
            backup_response = dynamodb.describe_continuous_backups(TableName=TABLE_NAME)
            pitr_status = backup_response['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['PointInTimeRecoveryStatus']
            print(f"  PITR: {pitr_status}")
        except ClientError:
            print("  PITR: Unable to check")

        # Check indexes
        indexes = table.get('GlobalSecondaryIndexes', [])
        print(f"  Indexes: {len(indexes)}")
        for index in indexes:
            print(f"    - {index['IndexName']}: {index['IndexStatus']}")

        return True

    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"Table {TABLE_NAME} does not exist")
        else:
            print(f"Error checking table: {e.response['Error']['Message']}")
        return False

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return False


def delete_employee_registry_table():
    """Delete the employee registry table (for cleanup/testing)"""

    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)

    print(f"WARNING: About to delete table {TABLE_NAME}")
    print("This will permanently delete all employee registry data!")

    confirm = input("Are you sure? Type 'DELETE' to confirm: ")
    if confirm != 'DELETE':
        print("Aborted")
        return False

    try:
        dynamodb.delete_table(TableName=TABLE_NAME)
        print("Table deletion initiated")

        print("Waiting for deletion to complete...")
        waiter = dynamodb.get_waiter('table_not_exists')
        waiter.wait(
            TableName=TABLE_NAME,
            WaiterConfig={
                'Delay': 5,
                'MaxAttempts': 60
            }
        )

        print(f"Table {TABLE_NAME} deleted successfully")
        return True

    except ClientError as e:
        print(f"Failed to delete table: {e.response['Error']['Message']}")
        return False

    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        return False


def main():
    """Main function with command line interface"""

    if len(sys.argv) < 2:
        print("Usage:")
        print(f"  {sys.argv[0]} create    - Create employee registry table")
        print(f"  {sys.argv[0]} verify    - Verify table exists and is configured")
        print(f"  {sys.argv[0]} delete    - Delete table (DANGEROUS)")
        print(f"  {sys.argv[0]} info      - Show configuration info")
        sys.exit(1)

    command = sys.argv[1].lower()

    print("PICASSO Employee Registry - Table Management")
    print("=" * 50)
    print(f"Environment: {ENVIRONMENT}")
    print(f"Region: {AWS_REGION}")
    print(f"Table: {TABLE_NAME}")
    print("=" * 50)

    if command == 'create':
        success = create_employee_registry_table()
        sys.exit(0 if success else 1)

    elif command == 'verify':
        success = verify_employee_registry_table()
        sys.exit(0 if success else 1)

    elif command == 'delete':
        success = delete_employee_registry_table()
        sys.exit(0 if success else 1)

    elif command == 'info':
        print("\nTable Schema:")
        print(f"  Table Name: {TABLE_NAME}")
        print(f"  Partition Key: tenantId (String)")
        print(f"  Sort Key: clerkUserId (String)")
        print(f"  Billing: Pay-per-request")
        print(f"  Encryption: KMS (AWS managed)")
        print(f"  PITR: Enabled")
        print(f"  TTL: None (persistent records)")
        print("\nGlobal Secondary Indexes:")
        print("  - EmailIndex (PK: email) — invite dedup, notification resolution")
        print("  - ClerkUserIdIndex (PK: clerkUserId) — Clerk webhook lookups")
        print("\nNon-Key Attributes (stored but not indexed):")
        print("  - name, role (admin/member)")
        print("  - status (active/invited/deactivated)")
        print("  - createdAt, updatedAt")
        print("\nEnvironment Variable:")
        print(f"  EMPLOYEE_REGISTRY_TABLE={TABLE_NAME}")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == '__main__':
    main()
