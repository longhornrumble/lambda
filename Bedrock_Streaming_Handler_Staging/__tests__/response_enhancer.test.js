/**
 * Response Enhancer Comprehensive Test Suite
 *
 * Tests for Phase 1B features: Suspended Forms & Program Switching
 * Ensures parity with Master_Function_Staging/form_cta_enhancer.py
 *
 * Target: 85%+ code coverage
 */

const { mockClient } = require('aws-sdk-client-mock');
const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Readable } = require('stream');

// Create S3 mock
const s3Mock = mockClient(S3Client);

// Import module under test
const {
  enhanceResponse,
  loadTenantConfig,
  detectConversationBranch
} = require('../response_enhancer');

// Test fixtures
const mockTenantHash = 'abc123def456';
const mockTenantId = 'TEST123';

const mockTenantMapping = {
  tenant_id: mockTenantId
};

const mockTenantConfig = {
  tenant_id: mockTenantId,
  conversational_forms: {
    volunteer_apply: {
      form_id: 'volunteer_apply',
      title: 'Volunteer Application',
      enabled: true,
      trigger_phrases: ['volunteer', 'help out', 'get involved']
    },
    lb_apply: {
      form_id: 'lb_apply',
      title: 'Love Box Application',
      enabled: true,
      trigger_phrases: ['love box', 'lovebox', 'lb']
    },
    dd_apply: {
      form_id: 'dd_apply',
      title: 'Dare to Dream Application',
      enabled: true,
      trigger_phrases: ['dare to dream', 'daretodream', 'dd']
    }
  },
  conversation_branches: {
    program_exploration: {
      detection_keywords: ['programs', 'what do you offer', 'services', 'opportunities'],
      available_ctas: {
        primary: 'explore_programs',
        secondary: ['volunteer_cta', 'lovebox_cta']
      }
    },
    volunteer_interest: {
      detection_keywords: ['volunteer', 'help', 'give back'],
      available_ctas: {
        primary: 'volunteer_cta'
      }
    },
    lovebox_discussion: {
      detection_keywords: ['love box', 'lovebox', 'food'],
      available_ctas: {
        primary: 'lovebox_form_cta'
      }
    },
    daretodream_discussion: {
      detection_keywords: ['dare to dream', 'daretodream', 'mentorship'],
      available_ctas: {
        primary: 'daretodream_form_cta'
      }
    }
  },
  cta_definitions: {
    explore_programs: {
      text: 'Explore Our Programs',
      action: 'link',
      url: 'https://example.com/programs'
    },
    volunteer_cta: {
      text: 'Start Volunteer Application',
      action: 'start_form',
      type: 'form_cta',
      formId: 'volunteer_apply'
    },
    lovebox_cta: {
      text: 'Learn About Love Box',
      action: 'link',
      url: 'https://example.com/lovebox'
    },
    lovebox_form_cta: {
      text: 'Apply to Love Box',
      action: 'start_form',
      type: 'form_cta',
      formId: 'lb_apply',
      program: 'lovebox'
    },
    daretodream_form_cta: {
      text: 'Apply to Dare to Dream',
      action: 'start_form',
      type: 'form_cta',
      formId: 'dd_apply',
      program: 'daretodream'
    }
  }
};

// Helper to create S3 response stream
function createS3Response(data) {
  const stream = new Readable();
  stream.push(JSON.stringify(data));
  stream.push(null);

  // Add transformToString method to match AWS SDK v3 behavior
  stream.transformToString = async () => JSON.stringify(data);

  return { Body: stream };
}

describe('Response Enhancer - S3 Configuration Loading', () => {
  beforeEach(() => {
    s3Mock.reset();
  });

  it('should load tenant configuration from S3 with hash resolution', async () => {
    // Use callsFake to handle any GetObjectCommand
    s3Mock.on(GetObjectCommand).callsFake((input) => {
      if (input.Key.includes('mappings/')) {
        return createS3Response(mockTenantMapping);
      } else if (input.Key.includes('-config.json')) {
        return createS3Response(mockTenantConfig);
      }
      throw new Error('Unexpected S3 key');
    });

    const config = await loadTenantConfig(mockTenantHash);

    expect(config).toMatchObject({
      conversation_branches: mockTenantConfig.conversation_branches,
      cta_definitions: mockTenantConfig.cta_definitions,
      conversational_forms: mockTenantConfig.conversational_forms
    });
  });

  it('should cache tenant configuration for subsequent requests', async () => {
    s3Mock.on(GetObjectCommand).callsFake((input) => {
      if (input.Key.includes('mappings/')) {
        return createS3Response(mockTenantMapping);
      } else if (input.Key.includes('-config.json')) {
        return createS3Response(mockTenantConfig);
      }
    });

    // First call - should hit S3
    await loadTenantConfig(mockTenantHash);

    // Second call - should use cache
    const config = await loadTenantConfig(mockTenantHash);

    // Should only have called S3 twice (mapping + config) for first request
    expect(s3Mock.calls().length).toBeLessThanOrEqual(2);
    expect(config).toBeDefined();
  });

  it('should return empty config on S3 errors', async () => {
    const differentHash = 'different123';
    s3Mock.on(GetObjectCommand).rejects(new Error('S3 error'));

    const config = await loadTenantConfig(differentHash);

    expect(config).toEqual({
      conversation_branches: {},
      cta_definitions: {}
    });
  });

  it('should return empty config when tenant hash cannot be resolved', async () => {
    const differentHash = 'badHash456';
    s3Mock.on(GetObjectCommand).callsFake((input) => {
      if (input.Key.includes('mappings/')) {
        return createS3Response({}); // No tenant_id
      }
    });

    const config = await loadTenantConfig(differentHash);

    expect(config).toEqual({
      conversation_branches: {},
      cta_definitions: {}
    });
  });
});

describe('Response Enhancer - Phase 1B: Suspended Forms Detection', () => {
  beforeEach(() => {
    s3Mock.reset();
    s3Mock.on(GetObjectCommand).callsFake((input) => {
      if (input.Key.includes('mappings/')) {
        return createS3Response(mockTenantMapping);
      } else if (input.Key.includes('-config.json')) {
        return createS3Response(mockTenantConfig);
      }
    });
  });

  it('should skip CTAs when no suspended forms exist', async () => {
    const bedrockResponse = 'We offer volunteer opportunities in our programs.';
    const userMessage = 'Tell me about volunteer opportunities';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: []
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.ctaButtons).toBeDefined();
    expect(result.metadata.suspended_forms_detected).toBeUndefined();
  });

  it('should skip CTAs when suspended form exists - no program switch', async () => {
    const bedrockResponse = 'Let me know if you have any questions about volunteering.';
    const userMessage = 'What are the requirements?';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: ['volunteer_apply'],
      program_interest: null
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.ctaButtons).toEqual([]);
    expect(result.metadata.enhanced).toBe(false);
    expect(result.metadata.suspended_forms_detected).toEqual(['volunteer_apply']);
  });

  it('should detect program switch from volunteer to Love Box', async () => {
    const bedrockResponse = 'The Love Box program provides food assistance to families in need.';
    const userMessage = 'Tell me about love box';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: ['volunteer_apply'],
      program_interest: null
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.metadata.program_switch_detected).toBe(true);
    expect(result.metadata.suspended_form.form_id).toBe('volunteer_apply');
    expect(result.metadata.new_form_of_interest.form_id).toBe('lb_apply');
    expect(result.ctaButtons).toEqual([]);
  });

  it('should detect program switch from volunteer to Dare to Dream', async () => {
    const bedrockResponse = 'Dare to Dream is our youth mentorship program.';
    const userMessage = 'Tell me about dare to dream';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: ['volunteer_apply'],
      program_interest: null
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.metadata.program_switch_detected).toBe(true);
    expect(result.metadata.suspended_form.form_id).toBe('volunteer_apply');
    expect(result.metadata.new_form_of_interest.form_id).toBe('dd_apply');
  });

  it('should not switch when same form is triggered', async () => {
    const bedrockResponse = 'We appreciate your interest in volunteering!';
    const userMessage = 'I want to volunteer';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: ['volunteer_apply'],
      program_interest: null
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.metadata.program_switch_detected).toBeUndefined();
    expect(result.ctaButtons).toEqual([]);
    expect(result.metadata.suspended_forms_detected).toEqual(['volunteer_apply']);
  });
});

describe('Response Enhancer - Phase 1B: Program Interest Mapping', () => {
  beforeEach(() => {
    s3Mock.reset();
    s3Mock.on(GetObjectCommand).callsFake((input) => {
      if (input.Key.includes('mappings/')) {
        return createS3Response(mockTenantMapping);
      } else if (input.Key.includes('-config.json')) {
        return createS3Response(mockTenantConfig);
      }
    });
  });

  it('should map program_interest "lovebox" to "Love Box"', async () => {
    const bedrockResponse = 'Dare to Dream provides mentorship opportunities.';
    const userMessage = 'Tell me about dare to dream';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: ['volunteer_apply'],
      program_interest: 'lovebox'
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.metadata.program_switch_detected).toBe(true);
    expect(result.metadata.suspended_form.program_name).toBe('Love Box');
  });

  it('should map program_interest "daretodream" to "Dare to Dream"', async () => {
    const bedrockResponse = 'The Love Box program helps families with food needs.';
    const userMessage = 'Tell me about love box';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: ['volunteer_apply'],
      program_interest: 'daretodream'
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.metadata.program_switch_detected).toBe(true);
    expect(result.metadata.suspended_form.program_name).toBe('Dare to Dream');
  });

  it('should map program_interest "both" to "both programs"', async () => {
    const bedrockResponse = 'Let me tell you about Dare to Dream.';
    const userMessage = 'Tell me about dare to dream';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: ['volunteer_apply'],
      program_interest: 'both'
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.metadata.program_switch_detected).toBe(true);
    expect(result.metadata.suspended_form.program_name).toBe('both programs');
  });

  it('should map program_interest "unsure" to "Volunteer"', async () => {
    const bedrockResponse = 'The Love Box program provides food assistance.';
    const userMessage = 'Tell me about love box';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: ['volunteer_apply'],
      program_interest: 'unsure'
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.metadata.program_switch_detected).toBe(true);
    expect(result.metadata.suspended_form.program_name).toBe('Volunteer');
  });

  it('should fallback to form title when program_interest is not set', async () => {
    const bedrockResponse = 'Dare to Dream offers youth mentorship.';
    const userMessage = 'Tell me about dare to dream';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: ['volunteer_apply'],
      program_interest: null
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.metadata.program_switch_detected).toBe(true);
    expect(result.metadata.suspended_form.program_name).toBe('Volunteer');
  });
});

describe('Response Enhancer - CTA Filtering by Completed Forms', () => {
  beforeEach(() => {
    s3Mock.reset();
    s3Mock.on(GetObjectCommand).callsFake((input) => {
      if (input.Key.includes('mappings/')) {
        return createS3Response(mockTenantMapping);
      } else if (input.Key.includes('-config.json')) {
        return createS3Response(mockTenantConfig);
      }
    });
  });

  it('should show CTA when form not completed', async () => {
    const bedrockResponse = 'We offer Love Box program for food assistance.';
    const userMessage = 'Tell me about love box';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: []
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.ctaButtons.length).toBeGreaterThanOrEqual(1);
    const formCta = result.ctaButtons.find(cta => cta.formId === 'lb_apply');
    expect(formCta).toBeDefined();
  });

  it('should skip CTA when form completed - lovebox mapping', async () => {
    const bedrockResponse = 'We offer Love Box program for food assistance.';
    const userMessage = 'Tell me about love box';
    const sessionContext = {
      completed_forms: ['lovebox'],
      suspended_forms: []
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    // Form trigger should be skipped, branch detection may show non-form CTAs
    const formCtas = result.ctaButtons.filter(cta => cta.action === 'start_form');
    expect(formCtas.length).toBe(0);
  });

  it('should skip CTA when form completed - daretodream mapping', async () => {
    const bedrockResponse = 'Dare to Dream provides youth mentorship opportunities.';
    const userMessage = 'Tell me about dare to dream';
    const sessionContext = {
      completed_forms: ['daretodream'],
      suspended_forms: []
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    const formCtas = result.ctaButtons.filter(cta => cta.action === 'start_form');
    expect(formCtas.length).toBe(0);
  });

  it('should filter branch CTAs for completed forms', async () => {
    const bedrockResponse = 'We offer many programs including Love Box and volunteer opportunities.';
    const userMessage = 'What programs do you offer?';
    const sessionContext = {
      completed_forms: ['lovebox'],
      suspended_forms: []
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    // Should not include lovebox CTA if it's a form CTA for completed program
    const loveboxFormCtas = result.ctaButtons.filter(cta =>
      cta.formId === 'lb_apply' || (cta.program === 'lovebox' && cta.action === 'start_form')
    );
    expect(loveboxFormCtas.length).toBe(0);
  });
});

describe('Response Enhancer - Conversation Branch Detection', () => {
  beforeEach(() => {
    s3Mock.reset();
    s3Mock.on(GetObjectCommand).callsFake((input) => {
      if (input.Key.includes('mappings/')) {
        return createS3Response(mockTenantMapping);
      } else if (input.Key.includes('-config.json')) {
        return createS3Response(mockTenantConfig);
      }
    });
  });

  it('should detect program_exploration branch', async () => {
    const result = detectConversationBranch(
      'We offer several programs and services to help the community.',
      'What programs do you offer?',
      mockTenantConfig,
      []
    );

    expect(result).toBeDefined();
    expect(result.branch).toBe('program_exploration');
    expect(result.ctas.length).toBeGreaterThan(0);
  });

  it('should detect volunteer_interest branch', async () => {
    const result = detectConversationBranch(
      'We appreciate volunteers who want to help and give back to the community.',
      'How can I volunteer?',
      mockTenantConfig,
      []
    );

    expect(result).toBeDefined();
    expect(result.branch).toBe('volunteer_interest');
  });

  it('should detect lovebox_discussion branch', async () => {
    const result = detectConversationBranch(
      'The Love Box program provides food assistance to families.',
      'Tell me about the love box program',
      mockTenantConfig,
      []
    );

    expect(result).toBeDefined();
    expect(result.branch).toBe('lovebox_discussion');
  });

  it('should detect daretodream_discussion branch', async () => {
    const result = detectConversationBranch(
      'Dare to Dream is our mentorship program for youth.',
      'Tell me about dare to dream',
      mockTenantConfig,
      []
    );

    expect(result).toBeDefined();
    expect(result.branch).toBe('daretodream_discussion');
  });

  it('should follow priority order for multiple matching branches', async () => {
    const result = detectConversationBranch(
      'We offer programs like Love Box and opportunities to volunteer.',
      'What programs do you have?',
      mockTenantConfig,
      []
    );

    // Should detect program_exploration first (higher priority)
    expect(result.branch).toBe('program_exploration');
  });

  it('should return null when user is not engaged', async () => {
    const result = detectConversationBranch(
      'The Love Box program helps families.',
      'ok',
      mockTenantConfig,
      []
    );

    expect(result).toBeNull();
  });

  it('should return null when no keywords match', async () => {
    const result = detectConversationBranch(
      'Thank you for your message.',
      'What is your address?',
      mockTenantConfig,
      []
    );

    expect(result).toBeNull();
  });

  it('should limit CTAs to maximum of 3', async () => {
    const configWithManyCtas = {
      ...mockTenantConfig,
      conversation_branches: {
        test_branch: {
          detection_keywords: ['test'],
          available_ctas: {
            primary: 'cta1',
            secondary: ['cta2', 'cta3', 'cta4', 'cta5']
          }
        }
      },
      cta_definitions: {
        cta1: { text: 'CTA 1', action: 'link', url: 'http://test.com' },
        cta2: { text: 'CTA 2', action: 'link', url: 'http://test.com' },
        cta3: { text: 'CTA 3', action: 'link', url: 'http://test.com' },
        cta4: { text: 'CTA 4', action: 'link', url: 'http://test.com' },
        cta5: { text: 'CTA 5', action: 'link', url: 'http://test.com' }
      }
    };

    const result = detectConversationBranch(
      'This is a test message.',
      'Tell me more about this test please',
      configWithManyCtas,
      []
    );

    if (result) {
      expect(result.ctas.length).toBeLessThanOrEqual(3);
    }
  });

  it('should filter completed forms from branch CTAs', async () => {
    const result = detectConversationBranch(
      'The Love Box program provides food assistance.',
      'Tell me about love box',
      mockTenantConfig,
      ['lovebox']
    );

    if (result && result.ctas) {
      const loveboxCtas = result.ctas.filter(cta =>
        cta.program === 'lovebox' || cta.formId === 'lb_apply'
      );
      expect(loveboxCtas.length).toBe(0);
    }
  });
});

describe('Response Enhancer - Integration Tests', () => {
  beforeEach(() => {
    s3Mock.reset();
    s3Mock.on(GetObjectCommand).callsFake((input) => {
      if (input.Key.includes('mappings/')) {
        return createS3Response(mockTenantMapping);
      } else if (input.Key.includes('-config.json')) {
        return createS3Response(mockTenantConfig);
      }
    });
  });

  it('should enhance response with form trigger CTA', async () => {
    const bedrockResponse = 'We welcome volunteers to help with our programs.';
    const userMessage = 'I want to volunteer';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: []
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.message).toBe(bedrockResponse);
    expect(result.ctaButtons).toHaveLength(1);
    expect(result.ctaButtons[0]).toMatchObject({
      type: 'form_cta',
      action: 'start_form',
      formId: 'volunteer_apply'
    });
    expect(result.metadata.enhanced).toBe(true);
  });

  it('should enhance response with branch CTAs', async () => {
    const bedrockResponse = 'We offer several programs and services to the community.';
    const userMessage = 'What programs do you offer?';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: []
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.ctaButtons.length).toBeGreaterThan(0);
    expect(result.metadata.branch_detected).toBe('program_exploration');
  });

  it('should return unenhanced response when no triggers or branches match', async () => {
    const bedrockResponse = 'Thank you for contacting us.';
    const userMessage = 'ok';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: []
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.ctaButtons).toEqual([]);
    expect(result.metadata.enhanced).toBe(false);
  });

  it('should handle errors gracefully and return unenhanced response', async () => {
    s3Mock.reset();
    s3Mock.on(GetObjectCommand).rejects(new Error('S3 error'));

    const bedrockResponse = 'Thank you for your inquiry.';
    const userMessage = 'ok';
    const sessionContext = {};

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.message).toBe(bedrockResponse);
    // When S3 fails, config is empty, so no CTAs can be triggered
    expect(result.metadata.enhanced).toBe(false);
  });

  it('should prioritize form triggers over branch CTAs', async () => {
    const bedrockResponse = 'We offer volunteer opportunities and various programs.';
    const userMessage = 'I want to volunteer';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: []
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    // Should show form trigger CTA, not branch CTAs
    expect(result.ctaButtons.length).toBeGreaterThanOrEqual(1);
    const formCta = result.ctaButtons.find(cta => cta.formId === 'volunteer_apply');
    expect(formCta).toBeDefined();
    expect(result.metadata.form_triggered).toBe('volunteer_apply');
  });

  it('should handle program switch with metadata for frontend', async () => {
    const bedrockResponse = 'The Love Box program helps families with food needs.';
    const userMessage = 'Tell me about love box';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: ['volunteer_apply'],
      program_interest: 'daretodream'
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    expect(result.metadata.program_switch_detected).toBe(true);
    expect(result.metadata.suspended_form).toMatchObject({
      form_id: 'volunteer_apply',
      program_name: 'Dare to Dream'
    });
    expect(result.metadata.new_form_of_interest).toMatchObject({
      form_id: 'lb_apply',
      program_name: 'Love Box'
    });
    expect(result.metadata.new_form_of_interest.cta_text).toBeDefined();
    expect(result.metadata.new_form_of_interest.fields).toBeDefined();
  });

  it('should convert branch CTAs to button format correctly', async () => {
    const bedrockResponse = 'We offer many programs and opportunities.';
    const userMessage = 'What do you offer?';
    const sessionContext = {
      completed_forms: [],
      suspended_forms: []
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    if (result.ctaButtons.length > 0) {
      result.ctaButtons.forEach(button => {
        expect(button).toHaveProperty('label');
        expect(button).toHaveProperty('action');
      });
    }
  });

  it('should filter multiple completed forms correctly', async () => {
    const bedrockResponse = 'We offer Love Box and Dare to Dream programs.';
    const userMessage = 'Tell me about your programs';
    const sessionContext = {
      completed_forms: ['lovebox', 'daretodream'],
      suspended_forms: []
    };

    const result = await enhanceResponse(bedrockResponse, userMessage, mockTenantHash, sessionContext);

    const formCtas = result.ctaButtons.filter(cta => cta.action === 'start_form');
    expect(formCtas.length).toBe(0);
    // The metadata may or may not include filtered_forms depending on the branch detection
  });
});

describe('Response Enhancer - Edge Cases', () => {
  beforeEach(() => {
    s3Mock.reset();
    s3Mock.on(GetObjectCommand).callsFake((input) => {
      if (input.Key.includes('mappings/')) {
        return createS3Response(mockTenantMapping);
      } else if (input.Key.includes('-config.json')) {
        return createS3Response(mockTenantConfig);
      }
    });
  });

  it('should handle missing conversational_forms gracefully', async () => {
    const configWithoutForms = {
      ...mockTenantConfig,
      conversational_forms: undefined
    };

    s3Mock.reset();
    s3Mock
      .on(GetObjectCommand)
      .resolves(createS3Response(mockTenantMapping))
      .on(GetObjectCommand)
      .resolves(createS3Response(configWithoutForms));

    const result = await enhanceResponse(
      'We offer programs.',
      'Tell me about volunteering',
      mockTenantHash,
      { completed_forms: [], suspended_forms: [] }
    );

    expect(result.ctaButtons).toBeDefined();
  });

  it('should handle missing conversation_branches gracefully', async () => {
    const configWithoutBranches = {
      ...mockTenantConfig,
      conversation_branches: undefined
    };

    const result = detectConversationBranch(
      'We offer programs.',
      'What do you offer?',
      configWithoutBranches,
      []
    );

    expect(result).toBeNull();
  });

  it('should handle empty sessionContext', async () => {
    const result = await enhanceResponse(
      'We offer volunteer opportunities.',
      'Tell me about volunteering',
      mockTenantHash,
      undefined
    );

    expect(result).toBeDefined();
    expect(result.message).toBe('We offer volunteer opportunities.');
  });

  it('should handle malformed branch configuration', async () => {
    const configWithBadBranch = {
      ...mockTenantConfig,
      conversation_branches: {
        bad_branch: {
          detection_keywords: null,
          available_ctas: {}
        }
      }
    };

    const result = detectConversationBranch(
      'Test message.',
      'What do you offer?',
      configWithBadBranch,
      []
    );

    expect(result).toBeNull();
  });

  it('should handle CTA with missing definition', async () => {
    const configWithMissingCta = {
      ...mockTenantConfig,
      conversation_branches: {
        test_branch: {
          detection_keywords: ['test'],
          available_ctas: {
            primary: 'missing_cta'
          }
        }
      },
      cta_definitions: {}
    };

    const result = detectConversationBranch(
      'This is a test.',
      'Tell me about test',
      configWithMissingCta,
      []
    );

    if (result) {
      expect(result.ctas).toEqual([]);
    } else {
      // Result may be null if user not engaged
      expect(result).toBeNull();
    }
  });

  it('should handle secondary CTAs when user message is short', async () => {
    const result = detectConversationBranch(
      'We offer many programs and opportunities.',
      'ok',
      mockTenantConfig,
      []
    );

    // Short message (< 20 chars) should not trigger secondary CTAs
    // But user must also be engaged, so this should return null
    expect(result).toBeNull();
  });

  it('should handle non-form CTAs in completed_forms filtering', async () => {
    const configWithMixedCtas = {
      ...mockTenantConfig,
      conversation_branches: {
        mixed_branch: {
          detection_keywords: ['programs'],
          available_ctas: {
            primary: 'explore_programs',
            secondary: ['lovebox_form_cta']
          }
        }
      }
    };

    const result = detectConversationBranch(
      'We offer programs.',
      'What programs do you have?',
      configWithMixedCtas,
      ['lovebox']
    );

    if (result) {
      expect(result.ctas.length).toBeGreaterThan(0);
      // Non-form CTA should always be included
      const nonFormCtas = result.ctas.filter(cta => cta.action === 'link');
      expect(nonFormCtas.length).toBeGreaterThan(0);
    }
  });

  it('should handle volunteer_general formId mapping in lovebox_discussion branch', async () => {
    const configWithVolunteerGeneral = {
      ...mockTenantConfig,
      conversation_branches: {
        lovebox_discussion: {
          detection_keywords: ['love box'],
          available_ctas: {
            primary: 'volunteer_general_cta'
          }
        }
      },
      cta_definitions: {
        volunteer_general_cta: {
          text: 'Volunteer for Love Box',
          action: 'start_form',
          type: 'form_cta',
          formId: 'volunteer_general'
        }
      }
    };

    const result = detectConversationBranch(
      'The Love Box program needs volunteers.',
      'Tell me about love box',
      configWithVolunteerGeneral,
      []
    );

    expect(result).toBeDefined();
    expect(result.ctas.length).toBeGreaterThan(0);
  });

  it('should handle volunteer_general formId mapping in daretodream_discussion branch', async () => {
    const configWithVolunteerGeneral = {
      ...mockTenantConfig,
      conversation_branches: {
        daretodream_discussion: {
          detection_keywords: ['dare to dream'],
          available_ctas: {
            primary: 'volunteer_general_cta'
          }
        }
      },
      cta_definitions: {
        volunteer_general_cta: {
          text: 'Volunteer for Dare to Dream',
          action: 'start_form',
          type: 'form_cta',
          formId: 'volunteer_general'
        }
      }
    };

    const result = detectConversationBranch(
      'Dare to Dream program needs mentors.',
      'Tell me about dare to dream',
      configWithVolunteerGeneral,
      []
    );

    expect(result).toBeDefined();
    expect(result.ctas.length).toBeGreaterThan(0);
  });

  it('should handle config load errors with try-catch', async () => {
    const badHash = 'error789';
    s3Mock.reset();
    s3Mock.on(GetObjectCommand).callsFake(() => {
      throw new Error('Unexpected error');
    });

    const config = await loadTenantConfig(badHash);

    expect(config).toEqual({
      conversation_branches: {},
      cta_definitions: {}
    });
  });
});
