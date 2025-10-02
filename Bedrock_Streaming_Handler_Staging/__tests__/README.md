# Bedrock Streaming Handler Test Suite

## Test Files

1. **`index.test.js`** - Integration tests for index.js (40 tests)
2. **`form_handler.test.js`** - Unit tests for form_handler.js
3. **`response_enhancer.test.js`** - Unit tests for response_enhancer.js
4. **`setup.js`** - Jest configuration and global mocks

## Running Tests

### Run All Tests
```bash
npm test
```

### Run Specific Test File
```bash
npm test -- __tests__/index.test.js
npm test -- __tests__/form_handler.test.js
npm test -- __tests__/response_enhancer.test.js
```

### Run Tests with Coverage
```bash
npm run test:coverage
```

### Run Tests for Specific File with Coverage
```bash
npm run test:coverage -- __tests__/index.test.js --collectCoverageFrom=index.js
```

### Run Tests in Watch Mode
```bash
npm run test:watch
```

### Run Specific Test by Name
```bash
npm test -- --testNamePattern="should load config from S3"
```

### Run Tests with Verbose Output
```bash
npm run test:verbose
```

## Test Coverage Goals

- **Statements:** 75%+ (✅ Achieved: 75.94%)
- **Branches:** 50%+ (✅ Achieved: 58.85%)
- **Functions:** 75%+ (✅ Achieved: 81.81%)
- **Lines:** 75%+ (✅ Achieved: 75.94%)

## Test Documentation

- **INDEX_TEST_SUMMARY.md** - Comprehensive summary of index.js integration tests
- **TEST_SUMMARY.md** - Overall test suite documentation

## Quick Test Examples

### Test Config Loading
```bash
npm test -- --testNamePattern="Config Loading"
```

### Test Form Mode
```bash
npm test -- --testNamePattern="Form Mode"
```

### Test Bedrock Streaming
```bash
npm test -- --testNamePattern="Bedrock Streaming"
```

### Test End-to-End Flows
```bash
npm test -- --testNamePattern="End-to-End"
```

## Debugging Tests

### Enable Console Output
Edit `__tests__/setup.js` and comment out the console mocking:

```javascript
// global.console = {
//   ...console,
//   log: jest.fn(),
//   ...
// };
```

### Run Single Test with Full Output
```bash
npm test -- --testNamePattern="specific test name" --verbose
```

## Test Structure

### index.test.js Structure
```
1. Config Loading & Caching (6 tests)
   - S3 config loading
   - Cache hit/miss
   - Fallback behavior
   - Cache expiration

2. Knowledge Base Integration (4 tests)
   - KB retrieval
   - KB caching
   - Error handling
   - Disabled KB

3. Form Mode Bypass (5 tests)
   - Field validation
   - Form submission
   - SSE streaming
   - Error handling

4. Bedrock Streaming (5 tests)
   - Model invocation
   - Stream parsing
   - Error handling
   - Default config

5. Response Enhancement (4 tests)
   - CTA injection
   - Session context
   - Error handling

6. Handler Entry Points (6 tests)
   - Request validation
   - OPTIONS handling
   - Event formats

7. End-to-End Integration (10 tests)
   - Complete flows
   - Conversation history
   - Form collection
   - Error recovery
```

## Mock Infrastructure

### AWS SDK Mocks
- **S3Client** - Config loading
- **BedrockRuntimeClient** - AI model invocation
- **BedrockAgentRuntimeClient** - Knowledge Base retrieval
- **SESClient** - Email sending (form_handler)
- **SNSClient** - SMS sending (form_handler)
- **DynamoDBDocumentClient** - Form storage (form_handler)

### Module Mocks
- **form_handler** - Form validation and submission
- **response_enhancer** - CTA injection and branch detection

### Global Mocks
- **awslambda.streamifyResponse** - Lambda streaming wrapper
- **console** - Output suppression during tests

## Test Helpers

### Mock Response Stream
```javascript
function createMockResponseStream() {
  return {
    write: jest.fn(),
    end: jest.fn(),
    getChunks: () => chunks
  };
}
```

### Mock S3 Response
```javascript
function createS3Response(data) {
  const stream = new Readable();
  stream.transformToString = async () => JSON.stringify(data);
  return { Body: stream };
}
```

### Mock Bedrock Stream
```javascript
function createBedrockStream(chunks) {
  return {
    body: {
      [Symbol.asyncIterator]: async function* () {
        for (const event of allEvents) yield event;
      }
    }
  };
}
```

## Continuous Integration

### GitHub Actions Example
```yaml
- name: Run Tests
  run: npm test

- name: Check Coverage
  run: npm run test:coverage
```

### Pre-commit Hook
```bash
#!/bin/bash
npm test || exit 1
```

## Troubleshooting

### Tests Timing Out
- Check async iterable implementation
- Verify mock responses are properly configured
- Ensure all promises are awaited

### Tests Failing Due to Cache
- Use unique tenant hashes per test
- Clear mocks in beforeEach
- Avoid shared test data

### Module Import Issues
- Ensure awslambda global is set before importing index.js
- Use beforeAll for one-time module imports
- Check mock order in test file

## Additional Resources

- [Jest Documentation](https://jestjs.io/docs/getting-started)
- [aws-sdk-client-mock](https://github.com/m-radzikowski/aws-sdk-client-mock)
- [AWS Lambda Streaming](https://docs.aws.amazon.com/lambda/latest/dg/configuration-response-streaming.html)
