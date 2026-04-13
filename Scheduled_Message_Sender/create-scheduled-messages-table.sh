#!/bin/bash
# Create picasso-scheduled-messages DynamoDB table
# Foundation for appointment reminders and future sequencing engine.
#
# PK: TENANT#{tenant_id}
# SK: SCHEDULED#{send_at_iso}#{message_id}
#
# GSI (by-appointment): lookup all scheduled messages for an appointment
#   Used when an appointment is rescheduled or cancelled — find and cancel all pending messages.

PROFILE="${AWS_PROFILE:-chris-admin}"
REGION="${AWS_REGION:-us-east-1}"
TABLE_NAME="picasso-scheduled-messages"

echo "Creating DynamoDB table: $TABLE_NAME"

aws dynamodb create-table \
  --table-name "$TABLE_NAME" \
  --attribute-definitions \
    AttributeName=pk,AttributeType=S \
    AttributeName=sk,AttributeType=S \
    AttributeName=appointment_id,AttributeType=S \
  --key-schema \
    AttributeName=pk,KeyType=HASH \
    AttributeName=sk,KeyType=RANGE \
  --global-secondary-indexes \
    '[
      {
        "IndexName": "by-appointment",
        "KeySchema": [
          {"AttributeName": "appointment_id", "KeyType": "HASH"},
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
