/**
 * Local test for response_enhancer.js
 * Tests context detection and CTA selection without deploying
 */

const { enhanceResponse, detectConversationBranch } = require('./response_enhancer');

// Mock config to avoid S3 calls
const mockConfig = {
  conversation_branches: {
    lovebox_discussion: {
      detection_keywords: ["Love Box", "LoveBox", "family support", "supporting families", "wraparound support"],
      available_ctas: {
        primary: "lovebox_apply",
        secondary: ["view_requirements", "schedule_discovery"]
      }
    },
    daretodream_discussion: {
      detection_keywords: ["Dare to Dream", "mentor", "mentoring", "youth", "aging out", "mentorship"],
      available_ctas: {
        primary: "daretodream_apply",
        secondary: ["learn_more", "view_requirements"]
      }
    },
    program_exploration: {
      detection_keywords: ["programs", "volunteer opportunities", "how can I help", "get involved"],
      available_ctas: {
        primary: "schedule_discovery",
        secondary: ["lovebox_info", "daretodream_info"]
      }
    }
  },
  cta_definitions: {
    lovebox_apply: {
      text: "Apply for Love Box",
      label: "Apply for Love Box",
      action: "start_form",
      formId: "lb_apply",
      type: "form_trigger",
      style: "primary"
    },
    daretodream_apply: {
      text: "Apply for Dare to Dream",
      label: "Become a Mentor",
      action: "start_form",
      formId: "dd_apply",
      type: "form_trigger",
      style: "primary"
    },
    schedule_discovery: {
      text: "Schedule Discovery Session",
      label: "Schedule Discovery Session",
      action: "external_link",
      url: "https://www.atlantaangels.org/volunteer.html",
      type: "external_link",
      style: "secondary"
    },
    view_requirements: {
      text: "View Requirements",
      label: "View Requirements",
      action: "show_info",
      infoType: "requirements",
      type: "info_request",
      style: "secondary"
    },
    lovebox_info: {
      text: "Learn About Love Box",
      label: "Learn About Love Box",
      action: "show_info",
      infoType: "lovebox",
      type: "info_request",
      style: "info"
    },
    daretodream_info: {
      text: "Learn About Dare to Dream",
      label: "Learn About Dare to Dream",
      action: "show_info",
      infoType: "daretodream",
      type: "info_request",
      style: "info"
    }
  },
  conversational_forms: {
    lovebox_application: {
      enabled: true,
      form_id: "lb_apply",
      title: "Love Box Application",
      cta_text: "Would you like to apply to be a Love Box Leader?",
      trigger_phrases: ["love box", "lovebox", "apply", "volunteer"],
      fields: []
    }
  }
};

// Test cases
const testCases = [
  {
    name: "Love Box discussion",
    bedrockResponse: `The Love Box Program is one of our core initiatives where volunteers provide wraparound support to foster families. Each Love Box team commits to supporting one family for at least a year, offering both practical assistance and emotional support.`,
    userQuery: "Tell me about Love Box",
    expectedBranch: "lovebox_discussion"
  },
  {
    name: "Dare to Dream discussion",
    bedrockResponse: `Dare to Dream is our mentorship program that connects caring adults with youth aging out of foster care. Mentors meet with youth regularly to help them develop life skills and prepare for adulthood.`,
    userQuery: "What is the mentoring program about?",
    expectedBranch: "daretodream_discussion"
  },
  {
    name: "Program overview",
    bedrockResponse: `We offer several volunteer opportunities including the Love Box program for family support and Dare to Dream for youth mentoring. Each program has different time commitments and requirements.`,
    userQuery: "What programs do you offer?",
    expectedBranch: "program_exploration"
  },
  {
    name: "No engagement",
    bedrockResponse: `Thank you for your interest in Atlanta Angels.`,
    userQuery: "ok",
    expectedBranch: null
  },
  {
    name: "Form trigger",
    bedrockResponse: `Love Box sounds like a great fit for you! You would be supporting a local family with children in foster care.`,
    userQuery: "I want to apply for love box",
    expectedBranch: "lovebox_discussion"
  }
];

console.log('🧪 Testing Response Enhancer\n');
console.log('=' .repeat(60));

testCases.forEach((test, index) => {
  console.log(`\nTest ${index + 1}: ${test.name}`);
  console.log('-'.repeat(40));
  console.log(`User: "${test.userQuery}"`);
  console.log(`Response snippet: "${test.bedrockResponse.substring(0, 100)}..."`);

  const result = detectConversationBranch(test.bedrockResponse, test.userQuery, mockConfig);

  if (result) {
    console.log(`✅ Detected branch: ${result.branch}`);
    console.log(`   CTAs returned: ${result.ctas.length}`);
    result.ctas.forEach(cta => {
      console.log(`   - ${cta.label} (${cta.action})`);
    });
  } else {
    console.log('❌ No branch detected');
  }

  if (test.expectedBranch) {
    if (result && result.branch === test.expectedBranch) {
      console.log('✓ PASS - Expected branch matched');
    } else {
      console.log(`✗ FAIL - Expected ${test.expectedBranch}, got ${result ? result.branch : 'null'}`);
    }
  } else {
    if (!result) {
      console.log('✓ PASS - Correctly detected no engagement');
    } else {
      console.log(`✗ FAIL - Expected no detection, got ${result.branch}`);
    }
  }
});

console.log('\n' + '='.repeat(60));
console.log('✅ Test complete');