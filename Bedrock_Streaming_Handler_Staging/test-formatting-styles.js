/**
 * Test script to compare formatting styles and detail levels
 * Invokes Lambda directly to test all 9 combinations quickly
 */

const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');

const lambda = new LambdaClient({
  region: 'us-east-1',
  profile: 'chris-admin' // Using chris-admin profile
});

// Test configurations - all 9 combinations
const testConfigs = [
  // Professional Concise
  { style: 'professional_concise', detail: 'concise', name: 'Professional + Concise' },
  { style: 'professional_concise', detail: 'balanced', name: 'Professional + Balanced' },
  { style: 'professional_concise', detail: 'comprehensive', name: 'Professional + Comprehensive' },

  // Warm Conversational
  { style: 'warm_conversational', detail: 'concise', name: 'Warm + Concise' },
  { style: 'warm_conversational', detail: 'balanced', name: 'Warm + Balanced' },
  { style: 'warm_conversational', detail: 'comprehensive', name: 'Warm + Comprehensive' },

  // Structured Detailed
  { style: 'structured_detailed', detail: 'concise', name: 'Structured + Concise' },
  { style: 'structured_detailed', detail: 'balanced', name: 'Structured + Balanced' },
  { style: 'structured_detailed', detail: 'comprehensive', name: 'Structured + Comprehensive' },
];

// Test question
const TEST_QUESTION = "Tell me about Dare to Dream";

async function testFormatting(config) {
  console.log(`\n${'='.repeat(80)}`);
  console.log(`Testing: ${config.name}`);
  console.log(`Style: ${config.style} | Detail Level: ${config.detail}`);
  console.log(`${'='.repeat(80)}\n`);

  // Build Lambda payload with custom bedrock_instructions
  const payload = {
    tenant_hash: 'auc5b0ecb0adcb', // Austin Angels
    user_input: TEST_QUESTION,
    session_id: `test_session_${Date.now()}`,
    streaming_message_id: `test_msg_${Date.now()}`,
    conversation_context: [],
    bedrock_instructions_override: {
      role_instructions: "You are a helpful virtual assistant for Austin Angels, a nonprofit supporting foster youth and families. Behave like you are an employee, responding with 'we' instead of 'they'. Your purpose is to answer questions about Austin Angels and its programs for its website visitors.",
      formatting_preferences: {
        emoji_usage: 'moderate',
        max_emojis_per_response: 3,
        response_style: config.style,
        detail_level: config.detail
      },
      custom_constraints: [],
      fallback_message: "I apologize, but I can't find information about that."
    }
  };

  try {
    const command = new InvokeCommand({
      FunctionName: 'Bedrock_Streaming_Handler_Staging',
      InvocationType: 'RequestResponse',
      Payload: JSON.stringify(payload)
    });

    const response = await lambda.send(command);
    const payloadString = Buffer.from(response.Payload).toString();

    // Parse SSE streaming response
    const lines = payloadString.split('\n');
    let fullResponse = '';

    for (const line of lines) {
      if (line.startsWith('data: ')) {
        const dataStr = line.substring(6).trim();
        if (dataStr === '[DONE]') continue;

        try {
          const data = JSON.parse(dataStr);
          if (data.type === 'text' && data.content) {
            fullResponse += data.content;
          }
        } catch (e) {
          // Ignore parse errors
        }
      }
    }

    if (fullResponse) {
      console.log(`RESPONSE:\n${fullResponse}\n`);
      console.log(`CHARACTER COUNT: ${fullResponse.length}`);
      console.log(`WORD COUNT: ${fullResponse.split(/\s+/).length}`);

      // Count emojis
      const emojiCount = (fullResponse.match(/[\u{1F300}-\u{1F9FF}]/gu) || []).length;
      console.log(`EMOJI COUNT: ${emojiCount}`);

    } else {
      console.log('Error: No response text extracted');
      console.log('Raw payload (first 500 chars):', payloadString.substring(0, 500));
    }

  } catch (error) {
    console.error(`Error testing ${config.name}:`, error.message);
  }
}

async function runAllTests() {
  console.log('\n📊 BEDROCK FORMATTING STYLE COMPARISON TEST');
  console.log('Question:', TEST_QUESTION);
  console.log('Tenant: Austin Angels (auc5b0ecb0adcb)');
  console.log('\nTesting all 9 combinations of response_style × detail_level...\n');

  for (const config of testConfigs) {
    await testFormatting(config);

    // Add delay between requests to avoid throttling
    await new Promise(resolve => setTimeout(resolve, 2000));
  }

  console.log('\n✅ All tests complete!');
  console.log('\nSummary of combinations tested:');
  testConfigs.forEach((c, i) => {
    console.log(`${i + 1}. ${c.name}`);
  });
}

// Run tests
runAllTests().catch(console.error);
