#!/usr/bin/env python3
"""
PICASSO Security Test Runner
Simplified test execution with dependency checks

Usage:
    python run_security_tests.py [category]

Categories:
    all         - Run all security tests (default)
    auth        - JWT authentication tests
    pii         - PII detection tests  
    blacklist   - Token blacklisting tests
    memory      - Memory management tests
    performance - Performance validation tests
    compliance  - Healthcare compliance tests
    integration - End-to-end integration tests
"""

import sys
import os
import importlib.util

def check_dependencies():
    """Check if required dependencies are available"""
    required_modules = [
        ('jwt', 'PyJWT library for JWT handling'),
        ('boto3', 'AWS SDK for Python'),
        ('moto', 'AWS service mocking library')
    ]
    
    missing_modules = []
    for module_name, description in required_modules:
        try:
            importlib.import_module(module_name)
        except ImportError:
            missing_modules.append(f"  - {module_name}: {description}")
    
    if missing_modules:
        print("❌ Missing required dependencies:")
        print("\n".join(missing_modules))
        print("\nInstall missing dependencies with:")
        print("pip install PyJWT boto3 moto")
        return False
    
    return True

def main():
    """Main test runner"""
    print("🛡️  PICASSO Security Test Runner")
    print("=" * 40)
    
    # Check dependencies
    if not check_dependencies():
        sys.exit(1)
    
    # Get test category from command line
    test_category = sys.argv[1] if len(sys.argv) > 1 else "all"
    
    valid_categories = ["all", "auth", "pii", "blacklist", "memory", "performance", "compliance", "integration"]
    
    if test_category not in valid_categories:
        print(f"❌ Invalid test category: {test_category}")
        print(f"Valid categories: {', '.join(valid_categories)}")
        sys.exit(1)
    
    print(f"Running tests for category: {test_category}")
    print("=" * 40)
    
    # Import and run tests
    try:
        from test_secure_conversation_system import run_test_suite
        exit_code = run_test_suite(test_category)
        sys.exit(exit_code)
    except ImportError as e:
        print(f"❌ Error importing test suite: {e}")
        print("Make sure test_secure_conversation_system.py is in the current directory")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error running tests: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()