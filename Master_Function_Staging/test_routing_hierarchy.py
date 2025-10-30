"""
Test Suite for 3-Tier Routing Hierarchy (Action Chips Explicit Routing PRD)

This test suite validates the implementation of FR-3 and FR-5 from the PRD:
- 3-tier routing hierarchy (action chip → CTA → fallback)
- Elimination of keyword-based detection
- Branch validation with graceful fallback

Test Scenarios (from PRD):
1. Action chip clicked with valid target_branch → routes to Tier 1
2. Action chip clicked with null target_branch → falls to Tier 3
3. Action chip clicked with invalid target_branch → falls to Tier 3 with warning
4. CTA clicked with valid target_branch → routes to Tier 2
5. Free-form query (no metadata) → routes to Tier 3
6. Free-form query + no fallback_branch → returns None (no CTAs)
"""

import json
import logging
from typing import Dict, Any, Optional

# Configure logging for tests
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import the routing function
from lambda_function import get_conversation_branch, build_ctas_for_branch


class TestRoutingHierarchy:
    """Test class for 3-tier routing hierarchy"""

    @staticmethod
    def create_test_config() -> Dict[str, Any]:
        """Create a sample tenant configuration for testing"""
        return {
            "conversation_branches": {
                "volunteer_interest": {
                    "available_ctas": {
                        "primary": "volunteer_apply",
                        "secondary": ["view_programs"]
                    }
                },
                "donation_interest": {
                    "available_ctas": {
                        "primary": "donate_now",
                        "secondary": ["view_donation_options"]
                    }
                },
                "navigation_hub": {
                    "available_ctas": {
                        "primary": "volunteer_apply",
                        "secondary": ["contact_us", "schedule_discovery"]
                    }
                }
            },
            "cta_settings": {
                "fallback_branch": "navigation_hub",
                "max_display": 3
            },
            "cta_definitions": {
                "volunteer_apply": {
                    "type": "form_cta",
                    "label": "Apply to Volunteer",
                    "action": "start_form",
                    "formId": "volunteer_apply",
                    "program": "volunteer"
                },
                "view_programs": {
                    "type": "cta_button",
                    "label": "View Programs",
                    "action": "link",
                    "url": "/programs"
                },
                "donate_now": {
                    "type": "cta_button",
                    "label": "Donate Now",
                    "action": "link",
                    "url": "/donate"
                },
                "view_donation_options": {
                    "type": "cta_button",
                    "label": "View Donation Options",
                    "action": "link",
                    "url": "/donate/options"
                },
                "contact_us": {
                    "type": "cta_button",
                    "label": "Contact Us",
                    "action": "link",
                    "url": "/contact"
                },
                "schedule_discovery": {
                    "type": "cta_button",
                    "label": "Schedule Discovery",
                    "action": "link",
                    "url": "/schedule"
                }
            }
        }

    def test_scenario_1_action_chip_valid_target(self):
        """
        Scenario 1: Action chip clicked with valid target_branch
        Expected: Routes to Tier 1, returns volunteer_interest
        """
        logger.info("\n=== Test Scenario 1: Action chip with valid target_branch ===")

        config = self.create_test_config()
        metadata = {
            "action_chip_triggered": True,
            "action_chip_id": "volunteer",
            "target_branch": "volunteer_interest"
        }

        result = get_conversation_branch(metadata, config)

        assert result == "volunteer_interest", f"Expected 'volunteer_interest', got '{result}'"
        logger.info("✅ Test passed: Routed to volunteer_interest via Tier 1")

    def test_scenario_2_action_chip_null_target(self):
        """
        Scenario 2: Action chip clicked with null target_branch
        Expected: Falls through to Tier 3, returns navigation_hub
        """
        logger.info("\n=== Test Scenario 2: Action chip with null target_branch ===")

        config = self.create_test_config()
        metadata = {
            "action_chip_triggered": True,
            "action_chip_id": "general_inquiry",
            "target_branch": None
        }

        result = get_conversation_branch(metadata, config)

        assert result == "navigation_hub", f"Expected 'navigation_hub', got '{result}'"
        logger.info("✅ Test passed: Fell through to Tier 3 fallback (navigation_hub)")

    def test_scenario_3_action_chip_invalid_target(self):
        """
        Scenario 3: Action chip clicked with invalid target_branch
        Expected: Falls through to Tier 3 with warning, returns navigation_hub
        """
        logger.info("\n=== Test Scenario 3: Action chip with invalid target_branch ===")

        config = self.create_test_config()
        metadata = {
            "action_chip_triggered": True,
            "action_chip_id": "volunteer",
            "target_branch": "nonexistent_branch"
        }

        result = get_conversation_branch(metadata, config)

        assert result == "navigation_hub", f"Expected 'navigation_hub', got '{result}'"
        logger.info("✅ Test passed: Invalid branch triggered fallback to Tier 3")

    def test_scenario_4_cta_valid_target(self):
        """
        Scenario 4: CTA clicked with valid target_branch
        Expected: Routes to Tier 2, returns donation_interest
        """
        logger.info("\n=== Test Scenario 4: CTA with valid target_branch ===")

        config = self.create_test_config()
        metadata = {
            "cta_triggered": True,
            "cta_id": "donate_now",
            "target_branch": "donation_interest"
        }

        result = get_conversation_branch(metadata, config)

        assert result == "donation_interest", f"Expected 'donation_interest', got '{result}'"
        logger.info("✅ Test passed: Routed to donation_interest via Tier 2")

    def test_scenario_5_free_form_query(self):
        """
        Scenario 5: Free-form query (no metadata)
        Expected: Routes to Tier 3, returns navigation_hub
        """
        logger.info("\n=== Test Scenario 5: Free-form query (no metadata) ===")

        config = self.create_test_config()
        metadata = {}  # No metadata = free-form query

        result = get_conversation_branch(metadata, config)

        assert result == "navigation_hub", f"Expected 'navigation_hub', got '{result}'"
        logger.info("✅ Test passed: Free-form query routed to Tier 3 fallback")

    def test_scenario_6_no_fallback_branch(self):
        """
        Scenario 6: Free-form query + no fallback_branch configured
        Expected: Returns None (no CTAs shown)
        """
        logger.info("\n=== Test Scenario 6: Free-form query with no fallback_branch ===")

        config = self.create_test_config()
        config["cta_settings"]["fallback_branch"] = None  # Remove fallback
        metadata = {}

        result = get_conversation_branch(metadata, config)

        assert result is None, f"Expected None, got '{result}'"
        logger.info("✅ Test passed: No fallback_branch returned None (backward compatible)")

    def test_cta_builder_with_completed_forms(self):
        """
        Additional Test: CTA builder filters completed forms
        """
        logger.info("\n=== Test: CTA builder with completed forms ===")

        config = self.create_test_config()
        completed_forms = ["volunteer"]  # User completed volunteer program

        ctas = build_ctas_for_branch("volunteer_interest", config, completed_forms)

        # Should filter out volunteer_apply CTA since program is completed
        cta_labels = [cta.get("label") for cta in ctas]
        assert "Apply to Volunteer" not in cta_labels, "Should filter completed program"
        assert "View Programs" in cta_labels, "Non-form CTAs should remain"

        logger.info(f"✅ Test passed: Filtered completed forms, {len(ctas)} CTAs remaining")

    def test_cta_builder_no_branch(self):
        """
        Additional Test: CTA builder with invalid branch name
        """
        logger.info("\n=== Test: CTA builder with invalid branch ===")

        config = self.create_test_config()
        ctas = build_ctas_for_branch("nonexistent_branch", config, [])

        assert ctas == [], f"Expected empty list, got {ctas}"
        logger.info("✅ Test passed: Invalid branch returns empty CTA list")

    def test_backward_compatibility_keyword_field(self):
        """
        Additional Test: Backward compatibility - detection_keywords ignored
        """
        logger.info("\n=== Test: Backward compatibility (keywords ignored) ===")

        config = self.create_test_config()
        # Add detection_keywords to branch (legacy config)
        config["conversation_branches"]["volunteer_interest"]["detection_keywords"] = [
            "volunteer", "help", "involved"
        ]

        metadata = {
            "action_chip_triggered": True,
            "target_branch": "volunteer_interest"
        }

        result = get_conversation_branch(metadata, config)

        # Should use explicit routing, not keyword detection
        assert result == "volunteer_interest", "Should use explicit routing"
        logger.info("✅ Test passed: detection_keywords ignored (backward compatible)")

    def run_all_tests(self):
        """Run all test scenarios"""
        logger.info("\n" + "="*70)
        logger.info("Running 3-Tier Routing Hierarchy Test Suite")
        logger.info("="*70)

        test_methods = [
            self.test_scenario_1_action_chip_valid_target,
            self.test_scenario_2_action_chip_null_target,
            self.test_scenario_3_action_chip_invalid_target,
            self.test_scenario_4_cta_valid_target,
            self.test_scenario_5_free_form_query,
            self.test_scenario_6_no_fallback_branch,
            self.test_cta_builder_with_completed_forms,
            self.test_cta_builder_no_branch,
            self.test_backward_compatibility_keyword_field
        ]

        passed = 0
        failed = 0

        for test in test_methods:
            try:
                test()
                passed += 1
            except AssertionError as e:
                logger.error(f"❌ Test failed: {test.__name__}")
                logger.error(f"   Error: {e}")
                failed += 1
            except Exception as e:
                logger.error(f"❌ Test error: {test.__name__}")
                logger.error(f"   Exception: {e}")
                failed += 1

        logger.info("\n" + "="*70)
        logger.info(f"Test Results: {passed} passed, {failed} failed")
        logger.info("="*70)

        return failed == 0


def main():
    """Main test runner"""
    test_suite = TestRoutingHierarchy()
    success = test_suite.run_all_tests()

    if success:
        logger.info("\n🎉 All tests passed! 3-tier routing is working correctly.")
        return 0
    else:
        logger.error("\n❌ Some tests failed. Please review the implementation.")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())
