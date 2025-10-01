#!/usr/bin/env python3
"""
Create DynamoDB tables for PICASSO conversation management
Run this script to create the required tables in your AWS environment
"""

import boto3
import sys
import os
from botocore.exceptions import ClientError

# Configuration
ENVIRONMENT = os.environ.get('ENVIRONMENT', 'staging')
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

# Table names
SUMMARIES_TABLE_NAME = f'{ENVIRONMENT}-conversation-summaries'
MESSAGES_TABLE_NAME = f'{ENVIRONMENT}-recent-messages'

def create_summaries_table(dynamodb_client):
    """Create the conversation summaries table"""
    try:
        response = dynamodb_client.create_table(
            TableName=SUMMARIES_TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'sessionId',
                    'KeyType': 'HASH'  # Partition key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'sessionId',
                    'AttributeType': 'S'
                }
            ],
            BillingMode='PAY_PER_REQUEST',  # On-demand pricing
            Tags=[
                {
                    'Key': 'Environment',
                    'Value': ENVIRONMENT
                },
                {
                    'Key': 'Service',
                    'Value': 'PICASSO'
                }
            ]
        )
        print(f"✅ Created table: {SUMMARIES_TABLE_NAME}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"⚠️  Table {SUMMARIES_TABLE_NAME} already exists")
            return True
        else:
            print(f"❌ Error creating {SUMMARIES_TABLE_NAME}: {e}")
            return False

def create_messages_table(dynamodb_client):
    """Create the recent messages table"""
    try:
        response = dynamodb_client.create_table(
            TableName=MESSAGES_TABLE_NAME,
            KeySchema=[
                {
                    'AttributeName': 'sessionId',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'messageTimestamp',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'sessionId',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'messageTimestamp',
                    'AttributeType': 'N'
                }
            ],
            BillingMode='PAY_PER_REQUEST',  # On-demand pricing
            Tags=[
                {
                    'Key': 'Environment',
                    'Value': ENVIRONMENT
                },
                {
                    'Key': 'Service',
                    'Value': 'PICASSO'
                }
            ]
        )
        print(f"✅ Created table: {MESSAGES_TABLE_NAME}")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceInUseException':
            print(f"⚠️  Table {MESSAGES_TABLE_NAME} already exists")
            return True
        else:
            print(f"❌ Error creating {MESSAGES_TABLE_NAME}: {e}")
            return False

def main():
    """Main function to create all required tables"""
    print(f"\n🚀 Creating DynamoDB tables for PICASSO conversation management")
    print(f"📍 Environment: {ENVIRONMENT}")
    print(f"📍 Region: {AWS_REGION}")
    print(f"📍 Tables to create:")
    print(f"   - {SUMMARIES_TABLE_NAME}")
    print(f"   - {MESSAGES_TABLE_NAME}")
    print()

    # Create DynamoDB client
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)

    # Create tables
    success = True
    success &= create_summaries_table(dynamodb)
    success &= create_messages_table(dynamodb)

    if success:
        print(f"\n✅ All tables created or already exist!")
        print(f"\n📝 Next steps:")
        print(f"   1. Ensure your Lambda function has IAM permissions to access these tables")
        print(f"   2. Set the following environment variables in your Lambda:")
        print(f"      - ENVIRONMENT={ENVIRONMENT}")
        print(f"      - SUMMARIES_TABLE_NAME={SUMMARIES_TABLE_NAME}")
        print(f"      - MESSAGES_TABLE_NAME={MESSAGES_TABLE_NAME}")
        return 0
    else:
        print(f"\n❌ Some tables failed to create. Please check the errors above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())