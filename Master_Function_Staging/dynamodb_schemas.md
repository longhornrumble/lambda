# DynamoDB Table Schemas for Form Processing

## 1. Form Submissions Table
**Table Name:** `picasso-form-submissions`

### Primary Key
- **Partition Key:** `tenant_id` (String)
- **Sort Key:** `submission_id` (String) - Format: `form_{type}_{timestamp}_{uuid}`

### Attributes
```json
{
  "tenant_id": "string",            // Tenant hash/ID
  "submission_id": "string",         // Unique submission ID
  "form_type": "string",            // Type of form (volunteer_signup, contact_us, etc.)
  "responses": "map",               // Form field responses
  "metadata": {
    "session_id": "string",
    "conversation_id": "string",
    "user_agent": "string",
    "ip_address": "string",
    "referrer": "string"
  },
  "status": "string",               // pending, processed, failed
  "notifications_sent": "list",     // Array of sent notifications
  "fulfillment": {
    "type": "string",               // lambda, email, s3
    "status": "string",
    "response": "map",
    "timestamp": "string"
  },
  "created_at": "string",           // ISO 8601 timestamp
  "updated_at": "string",
  "ttl": "number"                   // Optional TTL for data retention
}
```

### Global Secondary Indexes
1. **GSI1: FormTypeIndex**
   - Partition Key: `form_type`
   - Sort Key: `created_at`
   - Use case: Query all submissions of a specific type

2. **GSI2: StatusIndex**
   - Partition Key: `status`
   - Sort Key: `created_at`
   - Use case: Monitor pending/failed submissions

## 2. SMS Usage Tracking Table
**Table Name:** `picasso-sms-usage`

### Primary Key
- **Partition Key:** `tenant_month` (String) - Format: `{tenant_id}#{YYYY-MM}`
- **Sort Key:** `timestamp` (String) - ISO 8601 timestamp

### Attributes
```json
{
  "tenant_month": "string",         // tenant_id#YYYY-MM
  "timestamp": "string",            // When SMS was sent
  "phone_number": "string",         // Hashed phone number
  "message_length": "number",       // Character count
  "segments": "number",             // Number of SMS segments
  "submission_id": "string",        // Related form submission
  "status": "string",               // sent, failed
  "error": "string",                // Error message if failed
  "ttl": "number"                   // 90 days retention
}
```

### Global Secondary Index
**GSI1: MonthlyUsageIndex**
- Partition Key: `tenant_month`
- Projection: `COUNT`
- Use case: Count total SMS messages per month for usage limits

## 3. Form Templates Table (Future)
**Table Name:** `picasso-form-templates`

### Primary Key
- **Partition Key:** `tenant_id` (String)
- **Sort Key:** `template_id` (String)

### Attributes
```json
{
  "tenant_id": "string",
  "template_id": "string",          // email_welcome, sms_confirmation, etc.
  "type": "string",                 // email, sms
  "subject": "string",              // For emails
  "body": "string",                 // Template with {{placeholders}}
  "active": "boolean",
  "created_at": "string",
  "updated_at": "string"
}
```

## CloudFormation Template

```yaml
Resources:
  FormSubmissionsTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: picasso-form-submissions
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: tenant_id
          AttributeType: S
        - AttributeName: submission_id
          AttributeType: S
        - AttributeName: form_type
          AttributeType: S
        - AttributeName: status
          AttributeType: S
        - AttributeName: created_at
          AttributeType: S
      KeySchema:
        - AttributeName: tenant_id
          KeyType: HASH
        - AttributeName: submission_id
          KeyType: RANGE
      GlobalSecondaryIndexes:
        - IndexName: FormTypeIndex
          KeySchema:
            - AttributeName: form_type
              KeyType: HASH
            - AttributeName: created_at
              KeyType: RANGE
          Projection:
            ProjectionType: ALL
        - IndexName: StatusIndex
          KeySchema:
            - AttributeName: status
              KeyType: HASH
            - AttributeName: created_at
              KeyType: RANGE
          Projection:
            ProjectionType: INCLUDE
            NonKeyAttributes:
              - tenant_id
              - submission_id
              - form_type
      TimeToLiveSpecification:
        AttributeName: ttl
        Enabled: true
      StreamSpecification:
        StreamViewType: NEW_AND_OLD_IMAGES
      Tags:
        - Key: Application
          Value: Picasso
        - Key: Component
          Value: Forms

  SmsUsageTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: picasso-sms-usage
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: tenant_month
          AttributeType: S
        - AttributeName: timestamp
          AttributeType: S
      KeySchema:
        - AttributeName: tenant_month
          KeyType: HASH
        - AttributeName: timestamp
          KeyType: RANGE
      GlobalSecondaryIndexes:
        - IndexName: MonthlyUsageIndex
          KeySchema:
            - AttributeName: tenant_month
              KeyType: HASH
          Projection:
            ProjectionType: KEYS_ONLY
      TimeToLiveSpecification:
        AttributeName: ttl
        Enabled: true
      Tags:
        - Key: Application
          Value: Picasso
        - Key: Component
          Value: SMS

  FormTemplatesTable:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: picasso-form-templates
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: tenant_id
          AttributeType: S
        - AttributeName: template_id
          AttributeType: S
      KeySchema:
        - AttributeName: tenant_id
          KeyType: HASH
        - AttributeName: template_id
          KeyType: RANGE
      Tags:
        - Key: Application
          Value: Picasso
        - Key: Component
          Value: Templates
```

## IAM Policy for Lambda Function

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:Query",
        "dynamodb:BatchWriteItem"
      ],
      "Resource": [
        "arn:aws:dynamodb:*:*:table/picasso-form-submissions",
        "arn:aws:dynamodb:*:*:table/picasso-form-submissions/index/*",
        "arn:aws:dynamodb:*:*:table/picasso-sms-usage",
        "arn:aws:dynamodb:*:*:table/picasso-sms-usage/index/*",
        "arn:aws:dynamodb:*:*:table/picasso-form-templates"
      ]
    }
  ]
}
```

## Usage Examples

### Store Form Submission
```python
import boto3
from datetime import datetime
import uuid

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('picasso-form-submissions')

submission = {
    'tenant_id': 'tenant_abc123',
    'submission_id': f"form_volunteer_{int(datetime.utcnow().timestamp())}_{uuid.uuid4().hex[:8]}",
    'form_type': 'volunteer_signup',
    'responses': {
        'first_name': 'John',
        'last_name': 'Doe',
        'email': 'john@example.com',
        'phone': '555-1234',
        'availability': 'weekends'
    },
    'metadata': {
        'session_id': 'session_xyz789',
        'conversation_id': 'conv_123',
        'user_agent': 'Mozilla/5.0...'
    },
    'status': 'pending',
    'created_at': datetime.utcnow().isoformat(),
    'ttl': int(datetime.utcnow().timestamp()) + (90 * 24 * 3600)  # 90 days
}

table.put_item(Item=submission)
```

### Track SMS Usage
```python
table = dynamodb.Table('picasso-sms-usage')
now = datetime.utcnow()

usage_record = {
    'tenant_month': f"tenant_abc123#{now.strftime('%Y-%m')}",
    'timestamp': now.isoformat(),
    'phone_number': hashlib.sha256(phone.encode()).hexdigest(),
    'message_length': len(message),
    'segments': math.ceil(len(message) / 160),
    'submission_id': submission_id,
    'status': 'sent',
    'ttl': int(now.timestamp()) + (90 * 24 * 3600)
}

table.put_item(Item=usage_record)
```

### Query Monthly SMS Usage
```python
response = table.query(
    IndexName='MonthlyUsageIndex',
    KeyConditionExpression='tenant_month = :tm',
    ExpressionAttributeValues={
        ':tm': f"{tenant_id}#{datetime.utcnow().strftime('%Y-%m')}"
    },
    Select='COUNT'
)

monthly_count = response['Count']
```