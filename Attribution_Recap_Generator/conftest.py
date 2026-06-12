"""
conftest.py for Attribution_Recap_Generator tests.
Adds the repo root to sys.path so cross-package imports work:
  from Attribution_Unsubscribe.lambda_function import _validate_token
"""
import sys
import os

# repo root = parent of this directory
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

# Also add Attribution_Unsubscribe as a package
_UNSUB_PKG = os.path.join(_REPO_ROOT, 'Attribution_Unsubscribe')
if _UNSUB_PKG not in sys.path:
    sys.path.insert(0, os.path.join(_REPO_ROOT))
