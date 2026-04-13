#!/bin/bash
# Create picasso-sms-consent DynamoDB table
# Consent records for contact-facing transactional SMS (TCPA compliance)
# No TTL — retained for 4-year TCPA statute of limitations
#
# PK: TENANT#{tenant_id}
# SK: CONSENT#{consent_type}#{phone_e164}
#
# GSI (phone-lookup): lookup all tenant consent records by phone number
#   Used when Telnyx STOP webhook arrives (only provides phone, no tenant context)

PROFILE="${AWS_PROFILE:-chris-admin}"
REGION="${AWS_REGION:-us-east-1}"
TABLE_NAME="picasso-sms-consent"

echo "Creating DynamoDB table: $TABLE_NAME"

aws dynamodb create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions \
    AttributeName=pk,AttributeType=S \
    AttributeName=sk,AttributeType=S \
    AttributeName=phone_e164,AttributeType=S \
  --key-schema \
    AttributeName=pk,KeyType=HASH \
    AttributeName=sk,KeyType=RANGE \
  --global-secondary-indexes \
    '[
      {
        "IndexName": "phone-lookup",
        "KeySchema": [
          {"AttributeName": "phone_e164", "KeyType": "HASH"},
          {"AttributeName": "pk", "KeyType": "RANGE"}
        ],
        "Projection": {"ProjectionType": "ALL"}
      }
    ]' \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION" \
  --profile "$PROFILE"

echo ""
echo "Table created. Verify with:"
echo "  aws dynamodb describe-table --table-name $TABLE_NAME --profile $PROFILE --region $REGION"
