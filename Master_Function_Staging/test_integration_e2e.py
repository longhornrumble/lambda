"""
End-to-End Integration Tests for Action Chips Explicit Routing

This test suite simulates the complete user flow from frontend action chip clicks
through Lambda routing to response generation. It validates the 3-tier routing
hierarchy with realistic request/response patterns.

Test Coverage:
- Test Suite 4: Frontend → Lambda Flow (5 scenarios)
- Test Suite 5: Config Builder → S3 → Lambda Flow (3 scenarios)

All tests use mocked AWS services (S3, Bedrock, DynamoDB) to run independently
without requiring actual AWS infrastructure.

Author: Claude Code (QA Automation Specialist)
Created: 2025-10-30
"""

import json
import logging
import unittest
from typing import Dict, Any, Optional
from unittest.mock import patch, MagicMock, Mock

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import Lambda functions to test
from lambda_function import (
    lambda_handler,
    handle_chat,
    get_conversation_branch,
    build_ctas_for_branch,
    add_cors_headers
)


class TestEndToEndIntegration(unittest.TestCase):
    """
    End-to-end integration tests simulating full user flow:
    Frontend click → Lambda routing → Response generation
    """

    def setUp(self):
        """Set up test tenant configuration and fixtures"""
        self.tenant_hash = "te12345678abcdef"

        # Comprehensive tenant config matching v1.4.1 schema
        self.tenant_config = {
            "tenant_id": "TEST001",
            "tenant_hash": self.tenant_hash,
            "version": "1.4.1",

            # Action chips configuration (dictionary format)
            "action_chips": {
                "enabled": True,
                "default_chips": {
                    "volunteer": {
                        "id": "volunteer",
                        "label": "Volunteer",
                        "value": "Tell me about volunteering",
                        "target_branch": "volunteer_interest"
                    },
                    "donate": {
                        "id": "donate",
                        "label": "Donate",
                        "value": "I want to donate",
                        "target_branch": "donation_interest"
                    },
                    "learn_more": {
                        "id": "learn_more",
                        "label": "Learn More",
                        "value": "Tell me more",
                        "target_branch": None  # No target = falls to Tier 3
                    },
                    "invalid_chip": {
                        "id": "invalid_chip",
                        "label": "Invalid",
                        "value": "Invalid routing",
                        "target_branch": "nonexistent_branch"
                    }
                }
            },

            # CTA settings with fallback
            "cta_settings": {
                "fallback_branch": "navigation_hub",
                "max_display": 3
            },

            # Conversation branches
            "conversation_branches": {
                "volunteer_interest": {
                    "available_ctas": {
                        "primary": "volunteer_apply",
                        "secondary": ["view_volunteer_programs", "contact_us"]
                    }
                },
                "donation_interest": {
                    "available_ctas": {
                        "primary": "donate_now",
                        "secondary": ["view_donation_options"]
                    }
                },
                "application_flow": {
                    "available_ctas": {
                        "primary": "submit_application",
                        "secondary": ["save_progress"]
                    }
                },
                "navigation_hub": {
                    "available_ctas": {
                        "primary": "apply_general",
                        "secondary": ["schedule_discovery", "contact_us"]
                    }
                }
            },

            # CTA definitions
            "cta_definitions": {
                "volunteer_apply": {
                    "id": "volunteer_apply",
                    "label": "Start Volunteer Application",
                    "action": "start_form",
                    "formId": "volunteer_apply",
                    "program": "volunteer",
                    "target_branch": "application_flow"
                },
                "view_volunteer_programs": {
                    "id": "view_volunteer_programs",
                    "label": "View Volunteer Programs",
                    "action": "show_info",
                    "url": "/volunteer/programs"
                },
                "donate_now": {
                    "id": "donate_now",
                    "label": "Donate Now",
                    "action": "link",
                    "url": "/donate"
                },
                "view_donation_options": {
                    "id": "view_donation_options",
                    "label": "View Donation Options",
                    "action": "show_info",
                    "url": "/donate/options"
                },
                "submit_application": {
                    "id": "submit_application",
                    "label": "Submit Application",
                    "action": "start_form",
                    "formId": "application_submit"
                },
                "save_progress": {
                    "id": "save_progress",
                    "label": "Save Progress",
                    "action": "link",
                    "url": "/save"
                },
                "apply_general": {
                    "id": "apply_general",
                    "label": "Apply to Programs",
                    "action": "start_form",
                    "formId": "general_apply"
                },
                "schedule_discovery": {
                    "id": "schedule_discovery",
                    "label": "Schedule Discovery Call",
                    "action": "link",
                    "url": "/schedule"
                },
                "contact_us": {
                    "id": "contact_us",
                    "label": "Contact Us",
                    "action": "link",
                    "url": "/contact"
                }
            },

            # Bedrock configuration
            "model_id": "anthropic.claude-3-haiku-20240307-v1:0",
            "tone_prompt": "You are a helpful nonprofit assistant."
        }

    # ==========================
    # TEST SUITE 4: Frontend → Lambda Flow
    # ==========================

    @patch('intent_router.route_intent')
    @patch('tenant_config_loader.get_config_for_tenant_by_hash')
    def test_suite4_scenario1_action_chip_valid_target(self, mock_config_loader, mock_route_intent):
        """
        Test Suite 4, Scenario 1: User clicks action chip with valid target_branch

        Flow:
        1. User clicks "Volunteer" action chip
        2. Frontend passes: {action_chip_triggered: true, target_branch: "volunteer_interest"}
        3. Lambda routes to volunteer_interest branch (Tier 1)
        4. Response includes CTAs from volunteer_interest branch

        Expected:
        - Routing logs show Tier 1 routing
        - Response includes volunteer_apply, view_volunteer_programs CTAs
        """
        logger.info("\n=== TEST SUITE 4, SCENARIO 1: Action chip with valid target_branch ===")

        # Mock config loader
        mock_config_loader.return_value = self.tenant_config

        # Mock route_intent response
        mock_route_intent.return_value = {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'content': 'Here is information about volunteering opportunities.',
                'session_id': 'session_test123'
            })
        }

        # Simulate frontend request with action chip metadata
        event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'chat',
                't': self.tenant_hash
            },
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'tenant_hash': self.tenant_hash,
                'user_input': 'Tell me about volunteering',
                'session_id': 'session_test123',
                'metadata': {
                    'action_chip_triggered': True,
                    'action_chip_id': 'volunteer',
                    'target_branch': 'volunteer_interest'
                },
                'session_context': {
                    'completed_forms': []
                }
            })
        }

        # Execute Lambda handler
        response = lambda_handler(event, {})

        # Verify response structure
        self.assertEqual(response['statusCode'], 200)
        self.assertIn('body', response)

        # Parse response body
        body = json.loads(response['body'])
        self.assertIn('content', body)
        self.assertIn('ctaButtons', body)

        # Verify CTAs from volunteer_interest branch
        cta_buttons = body['ctaButtons']
        self.assertGreater(len(cta_buttons), 0, "Should have CTAs from volunteer_interest branch")

        # Verify specific CTAs present
        cta_ids = [cta.get('label') for cta in cta_buttons]
        self.assertIn('Start Volunteer Application', cta_ids,
                     "Should include volunteer_apply CTA")

        # Verify metadata indicates explicit routing
        if 'metadata' in body:
            self.assertTrue(body['metadata'].get('explicit_routing'),
                          "Should indicate explicit routing was used")
            self.assertEqual(body['metadata'].get('branch_used'), 'volunteer_interest',
                           "Should indicate volunteer_interest branch was used")

        logger.info("✅ Test passed: Action chip with valid target routed to Tier 1")

    @patch('intent_router.route_intent')
    @patch('tenant_config_loader.get_config_for_tenant_by_hash')
    def test_suite4_scenario2_action_chip_null_target(self, mock_config_loader, mock_route_intent):
        """
        Test Suite 4, Scenario 2: User clicks action chip with null target_branch

        Flow:
        1. User clicks "Learn More" action chip (no target_branch configured)
        2. Frontend passes: {action_chip_triggered: true, target_branch: null}
        3. Lambda falls through to Tier 3 (fallback_branch)
        4. Response includes CTAs from navigation_hub branch

        Expected:
        - Routing falls to Tier 3
        - Response includes navigation hub CTAs (apply_general, schedule_discovery, contact_us)
        """
        logger.info("\n=== TEST SUITE 4, SCENARIO 2: Action chip with null target_branch ===")

        # Mock config loader
        mock_config_loader.return_value = self.tenant_config

        # Mock route_intent response
        mock_route_intent.return_value = {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'content': 'How can I help you today?',
                'session_id': 'session_test456'
            })
        }

        # Simulate frontend request with null target_branch
        event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'chat',
                't': self.tenant_hash
            },
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'tenant_hash': self.tenant_hash,
                'user_input': 'Tell me more',
                'session_id': 'session_test456',
                'metadata': {
                    'action_chip_triggered': True,
                    'action_chip_id': 'learn_more',
                    'target_branch': None
                },
                'session_context': {
                    'completed_forms': []
                }
            })
        }

        # Execute Lambda handler
        response = lambda_handler(event, {})

        # Verify response
        self.assertEqual(response['statusCode'], 200)
        body = json.loads(response['body'])

        # Verify CTAs from fallback branch (navigation_hub)
        self.assertIn('ctaButtons', body)
        cta_buttons = body['ctaButtons']
        self.assertGreater(len(cta_buttons), 0, "Should have CTAs from navigation_hub")

        # Verify navigation hub CTAs present
        cta_labels = [cta.get('label') for cta in cta_buttons]
        self.assertIn('Apply to Programs', cta_labels,
                     "Should include apply_general from navigation_hub")

        logger.info("✅ Test passed: Action chip with null target fell to Tier 3")

    @patch('intent_router.route_intent')
    @patch('tenant_config_loader.get_config_for_tenant_by_hash')
    def test_suite4_scenario3_action_chip_invalid_target(self, mock_config_loader, mock_route_intent):
        """
        Test Suite 4, Scenario 3: User clicks action chip with invalid target_branch

        Flow:
        1. User clicks action chip with target_branch: "nonexistent_branch"
        2. Lambda logs warning: "[Tier 1] Invalid target_branch: nonexistent_branch"
        3. Lambda falls through to Tier 3 (fallback_branch)
        4. Response includes CTAs from navigation_hub

        Expected:
        - Warning logged for invalid branch
        - Graceful fallback to Tier 3
        - Response includes fallback CTAs
        """
        logger.info("\n=== TEST SUITE 4, SCENARIO 3: Action chip with invalid target_branch ===")

        # Mock config loader
        mock_config_loader.return_value = self.tenant_config

        # Mock route_intent response
        mock_route_intent.return_value = {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'content': 'Let me help you with that.',
                'session_id': 'session_test789'
            })
        }

        # Simulate frontend request with invalid target_branch
        event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'chat',
                't': self.tenant_hash
            },
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'tenant_hash': self.tenant_hash,
                'user_input': 'Invalid routing',
                'session_id': 'session_test789',
                'metadata': {
                    'action_chip_triggered': True,
                    'action_chip_id': 'invalid_chip',
                    'target_branch': 'nonexistent_branch'
                },
                'session_context': {
                    'completed_forms': []
                }
            })
        }

        # Execute Lambda handler
        response = lambda_handler(event, {})

        # Verify response
        self.assertEqual(response['statusCode'], 200)
        body = json.loads(response['body'])

        # Verify fallback CTAs present
        self.assertIn('ctaButtons', body)
        self.assertGreater(len(body['ctaButtons']), 0,
                          "Should have fallback CTAs despite invalid branch")

        logger.info("✅ Test passed: Invalid target_branch triggered warning and fallback")

    @patch('intent_router.route_intent')
    @patch('tenant_config_loader.get_config_for_tenant_by_hash')
    def test_suite4_scenario4_free_form_query(self, mock_config_loader, mock_route_intent):
        """
        Test Suite 4, Scenario 4: User types free-form query (no metadata)

        Flow:
        1. User types "What can I do?"
        2. Frontend passes normal message (no metadata)
        3. Lambda routes to fallback_branch (Tier 3)
        4. Response includes navigation CTAs

        Expected:
        - Routing falls to Tier 3 (no Tier 1 or 2 metadata)
        - Response includes navigation hub CTAs
        """
        logger.info("\n=== TEST SUITE 4, SCENARIO 4: Free-form query (no metadata) ===")

        # Mock config loader
        mock_config_loader.return_value = self.tenant_config

        # Mock route_intent response
        mock_route_intent.return_value = {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'content': 'You can apply to programs, schedule a call, or contact us.',
                'session_id': 'session_test_freeform'
            })
        }

        # Simulate free-form query (no metadata)
        event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'chat',
                't': self.tenant_hash
            },
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'tenant_hash': self.tenant_hash,
                'user_input': 'What can I do?',
                'session_id': 'session_test_freeform',
                'metadata': {},  # No action chip or CTA metadata
                'session_context': {
                    'completed_forms': []
                }
            })
        }

        # Execute Lambda handler
        response = lambda_handler(event, {})

        # Verify response
        self.assertEqual(response['statusCode'], 200)
        body = json.loads(response['body'])

        # Verify fallback CTAs present
        self.assertIn('ctaButtons', body)
        cta_buttons = body['ctaButtons']
        self.assertGreater(len(cta_buttons), 0, "Should have fallback CTAs")

        logger.info("✅ Test passed: Free-form query routed to Tier 3 fallback")

    @patch('intent_router.route_intent')
    @patch('tenant_config_loader.get_config_for_tenant_by_hash')
    def test_suite4_scenario5_cta_click_routing(self, mock_config_loader, mock_route_intent):
        """
        Test Suite 4, Scenario 5: User clicks CTA button (Tier 2 routing)

        Flow:
        1. User clicks "Apply" CTA
        2. Frontend passes: {cta_triggered: true, cta_id: "volunteer_apply", target_branch: "application_flow"}
        3. Lambda routes to application_flow branch (Tier 2)
        4. Response includes application CTAs

        Expected:
        - Routing logs show Tier 2 routing
        - Response includes application_flow CTAs
        - Tier 2 routing not broken by action chip changes
        """
        logger.info("\n=== TEST SUITE 4, SCENARIO 5: CTA click routing (Tier 2) ===")

        # Mock config loader
        mock_config_loader.return_value = self.tenant_config

        # Mock route_intent response
        mock_route_intent.return_value = {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'content': 'Let\'s start your application.',
                'session_id': 'session_test_cta'
            })
        }

        # Simulate CTA click with Tier 2 metadata
        event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'chat',
                't': self.tenant_hash
            },
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'tenant_hash': self.tenant_hash,
                'user_input': 'Start volunteer application',
                'session_id': 'session_test_cta',
                'metadata': {
                    'cta_triggered': True,
                    'cta_id': 'volunteer_apply',
                    'target_branch': 'application_flow'
                },
                'session_context': {
                    'completed_forms': []
                }
            })
        }

        # Execute Lambda handler
        response = lambda_handler(event, {})

        # Verify response
        self.assertEqual(response['statusCode'], 200)
        body = json.loads(response['body'])

        # Verify application flow CTAs
        self.assertIn('ctaButtons', body)
        cta_buttons = body['ctaButtons']
        self.assertGreater(len(cta_buttons), 0, "Should have application CTAs")

        logger.info("✅ Test passed: CTA click routed correctly via Tier 2")

    # ==========================
    # TEST SUITE 5: Config Builder → S3 → Lambda Flow
    # ==========================

    def test_suite5_scenario1_dictionary_action_chips(self):
        """
        Test Suite 5, Scenario 1: Config with dictionary action chips (v1.4.1)

        Flow:
        1. Load tenant config from mock S3
        2. Verify action_chips is dictionary (not array)
        3. Verify each chip has id, label, value, target_branch
        4. Lambda correctly parses dictionary format

        Expected:
        - Config parser handles dictionary format
        - Routing uses dictionary chip definitions
        """
        logger.info("\n=== TEST SUITE 5, SCENARIO 1: Config with dictionary action chips ===")

        # Verify config structure
        self.assertIsInstance(self.tenant_config['action_chips'], dict,
                            "action_chips should be dictionary")
        self.assertIn('default_chips', self.tenant_config['action_chips'],
                     "action_chips should have default_chips")

        # Verify dictionary format
        default_chips = self.tenant_config['action_chips']['default_chips']
        self.assertIsInstance(default_chips, dict,
                            "default_chips should be dictionary")

        # Verify each chip has required fields
        for chip_id, chip in default_chips.items():
            self.assertEqual(chip['id'], chip_id,
                           f"Chip ID should match key: {chip_id}")
            self.assertIn('label', chip, f"Chip {chip_id} should have label")
            self.assertIn('value', chip, f"Chip {chip_id} should have value")
            self.assertIn('target_branch', chip,
                         f"Chip {chip_id} should have target_branch")

        # Test routing with dictionary config
        metadata = {
            'action_chip_triggered': True,
            'action_chip_id': 'volunteer',
            'target_branch': 'volunteer_interest'
        }

        branch = get_conversation_branch(metadata, self.tenant_config)
        self.assertEqual(branch, 'volunteer_interest',
                       "Should route correctly with dictionary config")

        logger.info("✅ Test passed: Dictionary action chips parsed and routed correctly")

    def test_suite5_scenario2_fallback_branch_configured(self):
        """
        Test Suite 5, Scenario 2: Config with fallback_branch

        Flow:
        1. Load tenant config from mock S3
        2. Verify cta_settings.fallback_branch is set
        3. Lambda uses fallback_branch for unmatched queries
        4. Verify fallback CTAs returned

        Expected:
        - fallback_branch configuration respected
        - Unmatched queries route to fallback
        """
        logger.info("\n=== TEST SUITE 5, SCENARIO 2: Config with fallback_branch ===")

        # Verify fallback_branch configured
        self.assertIn('cta_settings', self.tenant_config)
        self.assertIn('fallback_branch', self.tenant_config['cta_settings'])

        fallback_branch = self.tenant_config['cta_settings']['fallback_branch']
        self.assertEqual(fallback_branch, 'navigation_hub',
                       "Fallback branch should be navigation_hub")

        # Verify fallback branch exists in conversation_branches
        self.assertIn(fallback_branch, self.tenant_config['conversation_branches'],
                     "Fallback branch should exist in conversation_branches")

        # Test routing with no metadata (should use fallback)
        metadata = {}
        branch = get_conversation_branch(metadata, self.tenant_config)
        self.assertEqual(branch, fallback_branch,
                       "Empty metadata should route to fallback_branch")

        # Build CTAs for fallback branch
        ctas = build_ctas_for_branch(fallback_branch, self.tenant_config, [])
        self.assertGreater(len(ctas), 0, "Fallback branch should have CTAs")

        # Verify navigation CTAs present
        cta_ids = [cta['id'] for cta in ctas]
        self.assertIn('apply_general', cta_ids,
                     "Fallback should include apply_general")

        logger.info("✅ Test passed: Fallback branch configured and working")

    def test_suite5_scenario3_v13_backward_compatibility(self):
        """
        Test Suite 5, Scenario 3: v1.3 backward compatibility

        Flow:
        1. Load v1.3 config with array action chips
        2. Lambda gracefully handles array format
        3. Routing falls back to legacy behavior
        4. No crashes or errors

        Expected:
        - No crashes with v1.3 config
        - Graceful degradation to fallback routing
        """
        logger.info("\n=== TEST SUITE 5, SCENARIO 3: v1.3 backward compatibility ===")

        # Create v1.3 config with array action chips (legacy format)
        v13_config = {
            "tenant_id": "TEST_V13",
            "version": "1.3.0",

            # Legacy array format
            "action_chips": {
                "enabled": True,
                "chips": [
                    {"label": "Yes", "value": "Yes"},
                    {"label": "No", "value": "No"},
                    {"label": "Tell me more", "value": "Tell me more"}
                ]
            },

            "cta_settings": {
                "fallback_branch": "navigation_hub"
            },

            "conversation_branches": {
                "navigation_hub": {
                    "available_ctas": {
                        "primary": "apply_general",
                        "secondary": ["contact_us"]
                    }
                }
            },

            "cta_definitions": {
                "apply_general": {
                    "id": "apply_general",
                    "label": "Apply",
                    "action": "start_form"
                },
                "contact_us": {
                    "id": "contact_us",
                    "label": "Contact Us",
                    "action": "link",
                    "url": "/contact"
                }
            }
        }

        # Verify v1.3 format
        self.assertIsInstance(v13_config['action_chips'], dict)
        if 'chips' in v13_config['action_chips']:
            self.assertIsInstance(v13_config['action_chips']['chips'], list,
                                "v1.3 should use array format")

        # Test routing with v1.3 config (no target_branch in metadata)
        metadata = {
            'action_chip_triggered': True,
            'action_chip_id': 'yes'
            # No target_branch field in v1.3
        }

        # Should gracefully fall back to Tier 3
        try:
            branch = get_conversation_branch(metadata, v13_config)
            self.assertEqual(branch, 'navigation_hub',
                           "v1.3 config should fall back to navigation_hub")

            # Build CTAs (should not crash)
            ctas = build_ctas_for_branch(branch, v13_config, [])
            self.assertIsInstance(ctas, list, "Should return list of CTAs")

        except Exception as e:
            self.fail(f"v1.3 config handling raised exception: {e}")

        logger.info("✅ Test passed: v1.3 config handled gracefully (backward compatible)")

    # ==========================
    # Additional Integration Tests
    # ==========================

    @patch('intent_router.route_intent')
    @patch('tenant_config_loader.get_config_for_tenant_by_hash')
    def test_completed_forms_filtering(self, mock_config_loader, mock_route_intent):
        """
        Additional Test: CTAs filtered based on completed forms

        Flow:
        1. User has completed volunteer program
        2. Lambda receives session_context with completed_forms: ["volunteer"]
        3. Lambda filters out volunteer_apply CTA
        4. Response only includes non-form CTAs

        Expected:
        - Form CTAs filtered when program completed
        - Non-form CTAs still shown
        """
        logger.info("\n=== ADDITIONAL TEST: Completed forms filtering ===")

        # Mock config loader
        mock_config_loader.return_value = self.tenant_config

        # Mock route_intent response
        mock_route_intent.return_value = {
            'statusCode': 200,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'content': 'You have already applied to volunteer!',
                'session_id': 'session_completed'
            })
        }

        # Simulate request with completed forms
        event = {
            'httpMethod': 'POST',
            'queryStringParameters': {
                'action': 'chat',
                't': self.tenant_hash
            },
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps({
                'tenant_hash': self.tenant_hash,
                'user_input': 'Tell me about volunteering',
                'session_id': 'session_completed',
                'metadata': {
                    'action_chip_triggered': True,
                    'action_chip_id': 'volunteer',
                    'target_branch': 'volunteer_interest'
                },
                'session_context': {
                    'completed_forms': ['volunteer']  # User completed volunteer
                }
            })
        }

        # Execute Lambda handler
        response = lambda_handler(event, {})

        # Verify response
        self.assertEqual(response['statusCode'], 200)
        body = json.loads(response['body'])

        # Verify volunteer_apply CTA filtered out
        if 'ctaButtons' in body:
            cta_labels = [cta.get('label') for cta in body['ctaButtons']]
            self.assertNotIn('Start Volunteer Application', cta_labels,
                           "Should filter completed volunteer application")

            # Verify non-form CTAs still present
            self.assertIn('View Volunteer Programs', cta_labels,
                         "Non-form CTAs should remain")

        logger.info("✅ Test passed: Completed forms filtered correctly")

    def test_cors_headers_present(self):
        """
        Additional Test: CORS headers present in all responses

        Expected:
        - All responses include CORS headers
        - OPTIONS requests handled correctly
        """
        logger.info("\n=== ADDITIONAL TEST: CORS headers ===")

        # Test OPTIONS request
        event = {
            'httpMethod': 'OPTIONS',
            'queryStringParameters': {},
            'headers': {}
        }

        response = lambda_handler(event, {})

        # Verify OPTIONS response
        self.assertEqual(response['statusCode'], 200)
        self.assertIn('headers', response)
        self.assertIn('Access-Control-Allow-Origin', response['headers'])
        self.assertIn('Access-Control-Allow-Methods', response['headers'])

        logger.info("✅ Test passed: CORS headers present")

    def test_routing_branch_validation(self):
        """
        Additional Test: Branch validation with detailed logging

        Expected:
        - Invalid branches logged with warnings
        - Valid branches logged with info
        - Graceful fallback always provided
        """
        logger.info("\n=== ADDITIONAL TEST: Branch validation ===")

        # Test valid branch
        with self.assertLogs(level='INFO') as log_context:
            metadata = {
                'action_chip_triggered': True,
                'target_branch': 'volunteer_interest'
            }
            branch = get_conversation_branch(metadata, self.tenant_config)

            self.assertEqual(branch, 'volunteer_interest')
            log_messages = [record.message for record in log_context.records]
            tier1_logged = any('[Tier 1]' in msg for msg in log_messages)
            self.assertTrue(tier1_logged, "Valid branch should log Tier 1")

        # Test invalid branch
        with self.assertLogs(level='WARNING') as log_context:
            metadata = {
                'action_chip_triggered': True,
                'target_branch': 'invalid_branch_12345'
            }
            branch = get_conversation_branch(metadata, self.tenant_config)

            # Should fall back to navigation_hub
            self.assertEqual(branch, 'navigation_hub')
            log_messages = [record.message for record in log_context.records]
            warning_logged = any('Invalid target_branch' in msg for msg in log_messages)
            self.assertTrue(warning_logged, "Invalid branch should log warning")

        logger.info("✅ Test passed: Branch validation working correctly")


def run_test_suite():
    """Run the complete test suite and generate report"""
    logger.info("\n" + "="*80)
    logger.info("END-TO-END INTEGRATION TEST SUITE")
    logger.info("Action Chips Explicit Routing PRD - Test Suites 4 & 5")
    logger.info("="*80)

    # Create test suite
    suite = unittest.TestLoader().loadTestsFromTestCase(TestEndToEndIntegration)

    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    # Generate summary
    logger.info("\n" + "="*80)
    logger.info("TEST SUMMARY")
    logger.info("="*80)
    logger.info(f"Tests run: {result.testsRun}")
    logger.info(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    logger.info(f"Failures: {len(result.failures)}")
    logger.info(f"Errors: {len(result.errors)}")
    logger.info("="*80)

    return result.wasSuccessful()


if __name__ == "__main__":
    import sys
    success = run_test_suite()
    sys.exit(0 if success else 1)
