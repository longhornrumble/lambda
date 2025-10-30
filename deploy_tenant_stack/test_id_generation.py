"""
Comprehensive Unit Tests for Action Chip ID Generation Algorithm

Tests the slugify() and generate_chip_id() functions in lambda_function.py
to ensure 100% coverage per PRD requirements.

Test Coverage:
- Basic slugification (5 tests)
- Special character handling (4 tests)
- Edge cases (4 tests)
- Collision detection (4 tests)
- Empty label handling (3 tests)

Total: 20+ test scenarios
"""

import unittest
from lambda_function import slugify, generate_chip_id


class TestSlugify(unittest.TestCase):
    """Test slugify function for URL-friendly conversion"""

    def test_basic_slugification_learn_more(self):
        """Test 1: 'Learn More' → 'learn_more'"""
        self.assertEqual(slugify("Learn More"), "learn_more")

    def test_basic_slugification_donate_now(self):
        """Test 2: 'Donate Now!' → 'donate_now'"""
        self.assertEqual(slugify("Donate Now!"), "donate_now")

    def test_basic_slugification_schedule_discovery_session(self):
        """Test 3: 'Schedule Discovery Session' → 'schedule_discovery_session'"""
        self.assertEqual(slugify("Schedule Discovery Session"), "schedule_discovery_session")

    def test_basic_slugification_faqs_info(self):
        """Test 4: 'FAQ's & Info' → 'faqs_info'"""
        self.assertEqual(slugify("FAQ's & Info"), "faqs_info")

    def test_basic_slugification_unicode(self):
        """Test 5: 'Español' → 'español' (unicode handling)"""
        # \w pattern in Python includes unicode letters, so they're preserved in lowercase
        result = slugify("Español")
        self.assertEqual(result, "español")

    def test_special_chars_all_special(self):
        """Test 6: '!@#$%^&*()' → '' (all special chars removed)"""
        self.assertEqual(slugify("!@#$%^&*()"), "")

    def test_special_chars_hyphens_to_underscores(self):
        """Test 7: 'a-b-c' → 'a_b_c' (hyphens to underscores)"""
        self.assertEqual(slugify("a-b-c"), "a_b_c")

    def test_special_chars_leading_trailing_spaces(self):
        """Test 8: '  spaces  ' → 'spaces' (leading/trailing spaces)"""
        self.assertEqual(slugify("  spaces  "), "spaces")

    def test_special_chars_multiple_underscores(self):
        """Test 9: 'multiple   spaces' → 'multiple_spaces' (dedup underscores)"""
        # Multiple spaces become single underscore
        self.assertEqual(slugify("multiple   spaces"), "multiple_spaces")

    def test_edge_case_empty_string(self):
        """Test 10: '' (empty string) → ''"""
        self.assertEqual(slugify(""), "")

    def test_edge_case_only_spaces(self):
        """Test 11: '   ' (only spaces) → ''"""
        self.assertEqual(slugify("   "), "")

    def test_edge_case_numbers_only(self):
        """Test 12: '123' (numbers only) → '123'"""
        self.assertEqual(slugify("123"), "123")

    def test_edge_case_very_long_label(self):
        """Test 13: Very long label (200 characters) is preserved"""
        long_label = "a" * 200
        result = slugify(long_label)
        self.assertEqual(len(result), 200)
        self.assertEqual(result, long_label)

    def test_mixed_case_conversion(self):
        """Test: Mixed case is converted to lowercase"""
        self.assertEqual(slugify("UPPERCASE"), "uppercase")
        self.assertEqual(slugify("MixedCase"), "mixedcase")

    def test_internal_special_chars(self):
        """Test: Internal special characters are removed"""
        self.assertEqual(slugify("hello@world"), "helloworld")
        self.assertEqual(slugify("test#value"), "testvalue")

    def test_combined_spaces_and_hyphens(self):
        """Test: Spaces and hyphens both become underscores"""
        self.assertEqual(slugify("hello world-test"), "hello_world_test")

    def test_leading_trailing_hyphens(self):
        """Test: Leading and trailing hyphens are stripped"""
        self.assertEqual(slugify("-test-"), "test")
        self.assertEqual(slugify("--multiple--"), "multiple")

    def test_alphanumeric_with_spaces(self):
        """Test: Alphanumeric with spaces"""
        self.assertEqual(slugify("Test 123 Value"), "test_123_value")


class TestGenerateChipId(unittest.TestCase):
    """Test generate_chip_id function with collision detection"""

    def test_no_collision_simple(self):
        """Test: ID generation without collisions"""
        existing_ids = set()
        chip_id = generate_chip_id("Volunteer", existing_ids)
        self.assertEqual(chip_id, "volunteer")

    def test_collision_single(self):
        """Test 14: ['Volunteer', 'Volunteer!'] → ['volunteer', 'volunteer_2']"""
        existing_ids = set()

        # First occurrence
        chip_id_1 = generate_chip_id("Volunteer", existing_ids)
        self.assertEqual(chip_id_1, "volunteer")
        existing_ids.add(chip_id_1)

        # Second occurrence (collision)
        chip_id_2 = generate_chip_id("Volunteer!", existing_ids)
        self.assertEqual(chip_id_2, "volunteer_2")

    def test_collision_triple(self):
        """Test 15: ['Learn More', 'Learn More!', 'Learn More??'] → ['learn_more', 'learn_more_2', 'learn_more_3']"""
        existing_ids = set()

        chip_id_1 = generate_chip_id("Learn More", existing_ids)
        self.assertEqual(chip_id_1, "learn_more")
        existing_ids.add(chip_id_1)

        chip_id_2 = generate_chip_id("Learn More!", existing_ids)
        self.assertEqual(chip_id_2, "learn_more_2")
        existing_ids.add(chip_id_2)

        chip_id_3 = generate_chip_id("Learn More??", existing_ids)
        self.assertEqual(chip_id_3, "learn_more_3")

    def test_collision_empty_labels(self):
        """Test 16: ['', '', ''] → ['action_chip', 'action_chip_2', 'action_chip_3']"""
        existing_ids = set()

        chip_id_1 = generate_chip_id("", existing_ids)
        self.assertEqual(chip_id_1, "action_chip")
        existing_ids.add(chip_id_1)

        chip_id_2 = generate_chip_id("", existing_ids)
        self.assertEqual(chip_id_2, "action_chip_2")
        existing_ids.add(chip_id_2)

        chip_id_3 = generate_chip_id("", existing_ids)
        self.assertEqual(chip_id_3, "action_chip_3")

    def test_collision_ten_times(self):
        """Test 17: ['x', 'x', ..., 'x'] (10 times) → ['x', 'x_2', ..., 'x_10']"""
        existing_ids = set()
        expected_ids = ["x"] + [f"x_{i}" for i in range(2, 11)]

        for i, expected_id in enumerate(expected_ids):
            chip_id = generate_chip_id("x", existing_ids)
            self.assertEqual(chip_id, expected_id, f"Iteration {i+1} failed")
            existing_ids.add(chip_id)

    def test_empty_label_default(self):
        """Test 18: '' with no existing IDs → 'action_chip'"""
        existing_ids = set()
        chip_id = generate_chip_id("", existing_ids)
        self.assertEqual(chip_id, "action_chip")

    def test_empty_label_collision(self):
        """Test 19: '' with existing 'action_chip' → 'action_chip_2'"""
        existing_ids = {"action_chip"}
        chip_id = generate_chip_id("", existing_ids)
        self.assertEqual(chip_id, "action_chip_2")

    def test_whitespace_label_fallback(self):
        """Test 20: '   ' (whitespace only) → 'action_chip'"""
        existing_ids = set()
        chip_id = generate_chip_id("   ", existing_ids)
        self.assertEqual(chip_id, "action_chip")

    def test_special_chars_only_fallback(self):
        """Test: Special chars only label → 'action_chip'"""
        existing_ids = set()
        chip_id = generate_chip_id("!@#$%", existing_ids)
        self.assertEqual(chip_id, "action_chip")

    def test_existing_ids_not_modified(self):
        """Test: Existing IDs set is not modified by function"""
        existing_ids = {"volunteer", "donate"}
        original_ids = existing_ids.copy()

        chip_id = generate_chip_id("Learn More", existing_ids)

        # Function should not modify the input set
        self.assertEqual(existing_ids, original_ids)
        self.assertEqual(chip_id, "learn_more")

    def test_collision_with_numbered_suffix(self):
        """Test: Collision detection works when existing ID has suffix"""
        existing_ids = {"volunteer", "volunteer_2", "volunteer_3"}
        chip_id = generate_chip_id("Volunteer!!!", existing_ids)
        self.assertEqual(chip_id, "volunteer_4")

    def test_complex_collision_scenario(self):
        """Test: Complex collision scenario with mixed labels"""
        existing_ids = set()

        # Add multiple different labels
        labels_and_expected = [
            ("Donate Now", "donate_now"),
            ("Donate Now!", "donate_now_2"),
            ("Learn More", "learn_more"),
            ("Volunteer", "volunteer"),
            ("Volunteer Today", "volunteer_today"),
            ("Volunteer", "volunteer_2"),
            ("", "action_chip"),
            ("   ", "action_chip_2"),
        ]

        for label, expected_id in labels_and_expected:
            chip_id = generate_chip_id(label, existing_ids)
            self.assertEqual(chip_id, expected_id)
            existing_ids.add(chip_id)

    def test_unicode_collision(self):
        """Test: Unicode characters in colliding labels"""
        existing_ids = set()

        chip_id_1 = generate_chip_id("Café", existing_ids)
        existing_ids.add(chip_id_1)

        chip_id_2 = generate_chip_id("Café!", existing_ids)
        # Both should slugify to same base, causing collision
        self.assertTrue(chip_id_2.startswith(chip_id_1))
        self.assertTrue("_2" in chip_id_2)


class TestIntegration(unittest.TestCase):
    """Integration tests for realistic action chip scenarios"""

    def test_realistic_tenant_chip_set(self):
        """Test: Realistic set of action chips for a tenant"""
        existing_ids = set()

        chips = [
            "Learn about our programs",
            "Schedule a meeting",
            "Donate now",
            "Volunteer opportunities",
            "FAQ",
            "Contact us",
        ]

        expected_results = [
            "learn_about_our_programs",
            "schedule_a_meeting",
            "donate_now",
            "volunteer_opportunities",
            "faq",
            "contact_us",
        ]

        for chip_label, expected_id in zip(chips, expected_results):
            chip_id = generate_chip_id(chip_label, existing_ids)
            self.assertEqual(chip_id, expected_id)
            existing_ids.add(chip_id)

    def test_batch_generation_maintains_uniqueness(self):
        """Test: Batch generation maintains uniqueness"""
        labels = [
            "Help", "Help!", "Help?", "Help...",
            "Info", "Information", "Info Center"
        ]

        existing_ids = set()
        generated_ids = []

        for label in labels:
            chip_id = generate_chip_id(label, existing_ids)
            generated_ids.append(chip_id)
            existing_ids.add(chip_id)

        # All IDs should be unique
        self.assertEqual(len(generated_ids), len(set(generated_ids)))

        # No duplicates in generated IDs
        self.assertEqual(len(generated_ids), len(labels))

    def test_idempotency(self):
        """Test: Same label with same existing IDs produces same result"""
        existing_ids = {"volunteer", "donate"}

        result1 = generate_chip_id("Learn More", existing_ids)
        result2 = generate_chip_id("Learn More", existing_ids)

        self.assertEqual(result1, result2)


class TestEdgeCasesAndBoundaries(unittest.TestCase):
    """Additional edge cases and boundary tests"""

    def test_slugify_only_special_chars(self):
        """Test: Only special characters"""
        self.assertEqual(slugify("!@#$%^&*()"), "")
        self.assertEqual(slugify("---"), "")
        self.assertEqual(slugify("___"), "")

    def test_slugify_mixed_alphanumeric_special(self):
        """Test: Mixed alphanumeric and special characters"""
        self.assertEqual(slugify("Test!123@Value"), "test123value")

    def test_generate_id_with_large_existing_set(self):
        """Test: Generate ID with large existing set"""
        existing_ids = {f"chip_{i}" for i in range(1000)}
        chip_id = generate_chip_id("New Chip", existing_ids)
        self.assertEqual(chip_id, "new_chip")
        self.assertNotIn(chip_id, existing_ids)

    def test_collision_counter_starts_at_2(self):
        """Test: Collision counter starts at 2 (not 1)"""
        existing_ids = {"test"}
        chip_id = generate_chip_id("test", existing_ids)
        self.assertEqual(chip_id, "test_2")

    def test_multiple_consecutive_spaces_and_hyphens(self):
        """Test: Multiple consecutive spaces and hyphens"""
        self.assertEqual(slugify("test   ---   value"), "test_value")

    def test_newlines_and_tabs(self):
        """Test: Newlines and tabs are treated as whitespace and become underscores"""
        # \s pattern matches newlines and tabs, which are then replaced with underscores
        self.assertEqual(slugify("test\nvalue"), "test_value")
        self.assertEqual(slugify("test\tvalue"), "test_value")


def run_test_suite():
    """Run the complete test suite and return results"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestSlugify))
    suite.addTests(loader.loadTestsFromTestCase(TestGenerateChipId))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestEdgeCasesAndBoundaries))

    # Run with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result


if __name__ == "__main__":
    print("=" * 80)
    print("ACTION CHIP ID GENERATION - COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    print()
    print("Testing slugify() and generate_chip_id() functions")
    print("Target: 100% code coverage")
    print()
    print("=" * 80)
    print()

    result = run_test_suite()

    print()
    print("=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Tests run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print()

    if result.wasSuccessful():
        print("✅ ALL TESTS PASSED - 100% SUCCESS RATE")
    else:
        print("❌ SOME TESTS FAILED - SEE DETAILS ABOVE")

    print("=" * 80)
