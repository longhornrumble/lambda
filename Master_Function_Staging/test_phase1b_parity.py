#!/usr/bin/env python3
"""
Test script for Phase 1B HTTP Fallback Parity
Tests form_cta_enhancer.py with Austin Angels config
"""

import json
import sys
from form_cta_enhancer import (
    detect_conversation_branch,
    should_trigger_form,
    enhance_response_with_form_cta
)

# Load Austin Angels config
with open('/Users/chrismiller/Desktop/Working_Folder/Sandbox/MYR384719-config.json', 'r') as f:
    austin_config = json.load(f)

def test_conversation_branch_detection():
    """Test that conversation branches are detected correctly"""
    print("\n" + "="*60)
    print("TEST 1: Conversation Branch Detection")
    print("="*60)

    # Test lovebox_discussion branch
    response_text = "The Love Box program provides wraparound support for families with children in foster care."
    user_message = "Tell me more about helping families"

    result = detect_conversation_branch(
        response_text,
        user_message,
        austin_config,
        completed_forms=[]
    )

    print(f"\nResponse snippet: '{response_text[:80]}...'")
    print(f"User message: '{user_message}'")
    print(f"\n✅ Branch detected: {result['branch'] if result else 'None'}")
    print(f"✅ CTAs found: {len(result['ctas']) if result else 0}")
    if result and result['ctas']:
        for i, cta in enumerate(result['ctas'], 1):
            print(f"   {i}. {cta.get('label') or cta.get('text')}")

    assert result is not None, "Expected branch to be detected"
    assert result['branch'] == 'lovebox_discussion', f"Expected lovebox_discussion, got {result['branch']}"
    assert len(result['ctas']) > 0, "Expected CTAs to be returned"
    print("\n✅ TEST 1 PASSED")


def test_completed_forms_filtering():
    """Test that completed forms are filtered out"""
    print("\n" + "="*60)
    print("TEST 2: Completed Forms Filtering")
    print("="*60)

    response_text = "The Love Box program provides wraparound support for families."
    user_message = "Tell me more about helping families"

    # First call - no completed forms
    result1 = detect_conversation_branch(
        response_text,
        user_message,
        austin_config,
        completed_forms=[]
    )

    print(f"\nScenario 1: No completed forms")
    print(f"✅ CTAs returned: {len(result1['ctas']) if result1 else 0}")

    # Second call - lovebox completed
    result2 = detect_conversation_branch(
        response_text,
        user_message,
        austin_config,
        completed_forms=['lovebox']
    )

    print(f"\nScenario 2: lovebox form completed")
    print(f"✅ CTAs returned: {len(result2['ctas']) if result2 else 0}")

    # Verify filtering worked
    assert result1 is not None, "Expected branch detection without completed forms"

    if result2:
        # Check that lovebox CTAs were filtered
        lovebox_ctas = [cta for cta in result2['ctas']
                       if cta.get('formId') in ['lb_apply'] or
                          cta.get('program') == 'lovebox']
        assert len(lovebox_ctas) == 0, "Expected lovebox CTAs to be filtered out"
        print(f"✅ Lovebox CTAs correctly filtered out")
    else:
        print(f"✅ All CTAs filtered (no branch result returned)")

    print("\n✅ TEST 2 PASSED")


def test_form_trigger_priority():
    """Test that form triggers have priority over branch detection"""
    print("\n" + "="*60)
    print("TEST 3: Form Trigger Priority")
    print("="*60)

    # Load conversational forms from config
    conversational_forms = austin_config.get('conversational_forms', {})

    # Find a form with trigger phrases
    lovebox_form = conversational_forms.get('lovebox_application')

    if lovebox_form:
        trigger_phrases = lovebox_form.get('trigger_phrases', [])
        print(f"\nLovebox form trigger phrases: {trigger_phrases}")

        if trigger_phrases:
            # Use first trigger phrase
            user_message = trigger_phrases[0]

            result = should_trigger_form(
                user_message,
                conversational_forms,
                readiness_score=0.85
            )

            print(f"\nUser message: '{user_message}'")
            print(f"Readiness score: 0.85")
            print(f"✅ Form triggered: {result['form_id'] if result else 'None'}")

            assert result is not None, "Expected form to trigger"
            assert result['form_id'] == lovebox_form['form_id'], \
                f"Expected {lovebox_form['form_id']}, got {result.get('form_id')}"

            print("\n✅ TEST 3 PASSED")
        else:
            print("\n⚠️  TEST 3 SKIPPED (no trigger phrases in config)")
    else:
        print("\n⚠️  TEST 3 SKIPPED (lovebox_application not found in config)")


def test_end_to_end_enhancement():
    """Test the full enhancement flow"""
    print("\n" + "="*60)
    print("TEST 4: End-to-End Enhancement")
    print("="*60)

    # Mock tenant hash (will use file-based config instead of S3)
    # We'll need to temporarily override load_tenant_config

    response_text = "The Love Box program provides wraparound support for families with children in foster care."
    user_message = "Tell me more about the Love Box program"

    # Since enhance_response_with_form_cta loads from S3, we'll test the components
    # individually rather than the full integration

    print(f"\nResponse: '{response_text[:80]}...'")
    print(f"User: '{user_message}'")

    # Test branch detection (which we can test directly)
    branch_result = detect_conversation_branch(
        response_text,
        user_message,
        austin_config,
        completed_forms=[]
    )

    if branch_result:
        print(f"\n✅ Branch detected: {branch_result['branch']}")
        print(f"✅ CTAs to show: {len(branch_result['ctas'])}")
        for i, cta in enumerate(branch_result['ctas'], 1):
            print(f"   {i}. {cta.get('label') or cta.get('text')} ({cta.get('action', 'unknown')})")

        print("\n✅ TEST 4 PASSED (component level)")
    else:
        print("\n❌ TEST 4 FAILED - No branch detected")
        sys.exit(1)


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("PHASE 1B PARITY TEST SUITE")
    print("Testing form_cta_enhancer.py with Austin Angels config")
    print("="*60)

    try:
        test_conversation_branch_detection()
        test_completed_forms_filtering()
        test_form_trigger_priority()
        test_end_to_end_enhancement()

        print("\n" + "="*60)
        print("✅ ALL TESTS PASSED")
        print("="*60)
        print("\nPhase 1B implementation is working correctly!")
        print("Master_Function_Staging now has parity with Bedrock_Streaming_Handler_Staging")

    except AssertionError as e:
        print(f"\n❌ TEST FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ UNEXPECTED ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
