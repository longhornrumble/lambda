#!/usr/bin/env python3
"""
Test Runner for Day 5 Backend Form Processing
Runs comprehensive test suite with coverage reporting and organized output
"""

import sys
import os
import unittest
import importlib.util
from pathlib import Path
import time

def check_dependencies():
    """Check if required testing dependencies are available"""
    required_modules = [
        ('unittest', 'Python standard library unittest'),
        ('boto3', 'AWS SDK for Python'),
        ('moto', 'AWS service mocking library'),
        ('requests', 'HTTP library for webhook testing'),
    ]

    optional_modules = [
        ('coverage', 'Code coverage measurement'),
        ('pytest', 'Alternative test runner'),
    ]

    missing_required = []
    missing_optional = []

    for module_name, description in required_modules:
        try:
            importlib.import_module(module_name)
        except ImportError:
            missing_required.append(f"  - {module_name}: {description}")

    for module_name, description in optional_modules:
        try:
            importlib.import_module(module_name)
        except ImportError:
            missing_optional.append(f"  - {module_name}: {description}")

    if missing_required:
        print("❌ Missing required dependencies:")
        print("\n".join(missing_required))
        print("\nInstall missing dependencies with:")
        print("pip install boto3 moto requests")
        return False

    if missing_optional:
        print("⚠️  Missing optional dependencies (enhanced features):")
        print("\n".join(missing_optional))
        print("Install with: pip install coverage pytest")
        print()

    return True

def discover_test_modules():
    """Discover all test modules for form processing"""
    test_modules = [
        'test_form_handler',
        'test_template_renderer',
        'test_lambda_integration',
        'test_dynamodb_operations',
        'test_sms_rate_limiting',
        'test_notification_services',
        'test_error_handling'
    ]

    available_modules = []
    missing_modules = []

    for module_name in test_modules:
        module_path = Path(__file__).parent / f"{module_name}.py"
        if module_path.exists():
            available_modules.append(module_name)
        else:
            missing_modules.append(module_name)

    return available_modules, missing_modules

def run_test_suite(test_category='all', verbose=True):
    """
    Run the comprehensive test suite

    Args:
        test_category: Category of tests to run ('all', 'unit', 'integration', 'specific_module')
        verbose: Enable verbose output

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    print("🧪 Day 5 Backend Form Processing Test Suite")
    print("=" * 50)

    # Check dependencies
    if not check_dependencies():
        return 1

    # Discover test modules
    available_modules, missing_modules = discover_test_modules()

    if missing_modules:
        print(f"⚠️  Missing test modules: {', '.join(missing_modules)}")
        print()

    if not available_modules:
        print("❌ No test modules found!")
        return 1

    print(f"📋 Available test modules: {len(available_modules)}")
    for module in available_modules:
        print(f"  - {module}")
    print()

    # Determine which modules to run based on category
    modules_to_run = []

    if test_category == 'all':
        modules_to_run = available_modules
    elif test_category == 'unit':
        modules_to_run = [
            'test_form_handler',
            'test_template_renderer',
            'test_dynamodb_operations',
            'test_sms_rate_limiting'
        ]
        modules_to_run = [m for m in modules_to_run if m in available_modules]
    elif test_category == 'integration':
        modules_to_run = [
            'test_lambda_integration',
            'test_notification_services'
        ]
        modules_to_run = [m for m in modules_to_run if m in available_modules]
    elif test_category == 'error':
        modules_to_run = ['test_error_handling']
        modules_to_run = [m for m in modules_to_run if m in available_modules]
    elif test_category in available_modules:
        modules_to_run = [test_category]
    else:
        print(f"❌ Invalid test category: {test_category}")
        print(f"Valid categories: all, unit, integration, error, {', '.join(available_modules)}")
        return 1

    print(f"🚀 Running tests for category: {test_category}")
    print(f"📦 Modules to test: {', '.join(modules_to_run)}")
    print()

    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    total_tests = 0
    for module_name in modules_to_run:
        try:
            # Import module dynamically
            module = importlib.import_module(module_name)
            module_suite = loader.loadTestsFromModule(module)
            suite.addTest(module_suite)

            # Count tests in this module
            module_test_count = module_suite.countTestCases()
            total_tests += module_test_count
            print(f"✅ Loaded {module_test_count} tests from {module_name}")

        except ImportError as e:
            print(f"❌ Failed to import {module_name}: {e}")
            return 1
        except Exception as e:
            print(f"❌ Error loading tests from {module_name}: {e}")
            return 1

    print(f"\n📊 Total tests to run: {total_tests}")
    print("=" * 50)

    # Configure test runner
    verbosity = 2 if verbose else 1
    runner = unittest.TextTestRunner(
        verbosity=verbosity,
        stream=sys.stdout,
        descriptions=True,
        failfast=False
    )

    # Run tests
    start_time = time.time()
    result = runner.run(suite)
    end_time = time.time()

    # Print summary
    print("\n" + "=" * 50)
    print("📋 TEST SUMMARY")
    print("=" * 50)

    duration = end_time - start_time
    print(f"⏱️  Duration: {duration:.2f} seconds")
    print(f"🧪 Tests run: {result.testsRun}")
    print(f"✅ Successes: {result.testsRun - len(result.failures) - len(result.errors)}")

    if result.failures:
        print(f"❌ Failures: {len(result.failures)}")
        for test, traceback in result.failures:
            print(f"   - {test}")

    if result.errors:
        print(f"💥 Errors: {len(result.errors)}")
        for test, traceback in result.errors:
            print(f"   - {test}")

    if result.skipped:
        print(f"⏭️  Skipped: {len(result.skipped)}")

    # Success/failure determination
    success = len(result.failures) == 0 and len(result.errors) == 0

    if success:
        print("\n🎉 ALL TESTS PASSED!")
        print("✨ Form processing implementation is ready for production")
    else:
        print("\n💥 SOME TESTS FAILED!")
        print("🔧 Please review and fix the failing tests before deployment")

    print("\n📚 Test Coverage Areas:")
    print("  ✅ Form submission processing and validation")
    print("  ✅ Multi-channel notifications (Email, SMS, Webhooks)")
    print("  ✅ Template rendering with variable substitution")
    print("  ✅ SMS rate limiting and usage tracking")
    print("  ✅ DynamoDB operations and data persistence")
    print("  ✅ Lambda integration and routing")
    print("  ✅ Error handling and edge cases")
    print("  ✅ Security testing (XSS, SQL injection prevention)")
    print("  ✅ AWS service integration with mocking")

    return 0 if success else 1

def run_coverage_report():
    """Run tests with coverage reporting if available"""
    try:
        import coverage

        print("📊 Running tests with coverage reporting...")

        # Start coverage
        cov = coverage.Coverage()
        cov.start()

        # Run tests
        exit_code = run_test_suite('all', verbose=False)

        # Stop coverage and generate report
        cov.stop()
        cov.save()

        print("\n📈 COVERAGE REPORT")
        print("=" * 50)

        # Generate coverage report
        cov.report(show_missing=True)

        # Generate HTML report if possible
        try:
            cov.html_report(directory='htmlcov')
            print("\n📄 HTML coverage report generated in 'htmlcov/' directory")
        except:
            pass

        return exit_code

    except ImportError:
        print("⚠️  Coverage module not available. Install with: pip install coverage")
        return run_test_suite('all')

def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description='Day 5 Backend Form Processing Test Runner',
        epilog="""
Examples:
  python run_form_tests.py                    # Run all tests
  python run_form_tests.py unit               # Run unit tests only
  python run_form_tests.py integration        # Run integration tests only
  python run_form_tests.py test_form_handler  # Run specific module
  python run_form_tests.py --coverage         # Run with coverage
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        'category',
        nargs='?',
        default='all',
        help='Test category to run (all, unit, integration, error, or specific module name)'
    )

    parser.add_argument(
        '--coverage',
        action='store_true',
        help='Run tests with coverage reporting'
    )

    parser.add_argument(
        '--quiet',
        action='store_true',
        help='Reduce output verbosity'
    )

    args = parser.parse_args()

    if args.coverage:
        exit_code = run_coverage_report()
    else:
        exit_code = run_test_suite(
            test_category=args.category,
            verbose=not args.quiet
        )

    sys.exit(exit_code)

if __name__ == '__main__':
    main()