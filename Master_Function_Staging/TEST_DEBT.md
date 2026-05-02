# Master_Function_Staging — Python test debt

**Status:** Tracking artifact for a focused future session. Do NOT close this without addressing each cluster.

**Surfaced:** 2026-05-02 — installing `moto<5` in CI (per [PR #34](https://github.com/longhornrumble/lambda/pull/34)) exposed 101 pre-existing logic test failures that were previously hidden behind import-time collection errors. None were introduced by recent code changes; they are accumulated tech debt.

**Scope of this PR:** investigate and resolve all 101 failures. Do not extend scope. Do not refactor unrelated code.

---

## Failure clusters

Each cluster is a distinct root cause. Resolve in order; later clusters may resolve earlier failures as side effects, so re-run after each fix to recompute the remaining count.

### Cluster 1 — boto3 client created outside moto context (~48 failures)

**Files:** `test_form_handler.py`, `test_lambda_integration.py`, `test_notification_services.py`, `test_sms_rate_limiting.py`, `test_dynamodb_operations.py`

**Symptom:** `botocore.exceptions.ClientError: ... UnrecognizedClientException: The security token included in the request is invalid.`

**Root cause:** `@mock_dynamodb` (and equivalents) decorate `setUp` or individual test methods. boto3 clients/resources stored on `self` during `setUp` are valid only within that mock context — once `setUp` returns, the mock context exits, and subsequent test method calls hit real AWS.

**Fix pattern:** move the `@mock_*` decorator to the class level so the mock context wraps the entire test instance lifecycle.

```python
# Before:
class TestDynamoDBSchemas(unittest.TestCase):
    @mock_dynamodb
    def setUp(self):
        self.dynamodb = boto3.resource(...)

# After:
@mock_dynamodb
class TestDynamoDBSchemas(unittest.TestCase):
    def setUp(self):
        self.dynamodb = boto3.resource(...)
```

For tests that need multiple AWS services mocked, stack the decorators at class level: `@mock_dynamodb` then `@mock_ses` then `@mock_sns` etc.

**Effort:** ~1 hour. Six files; copy-paste fix; rerun pytest after each.

### Cluster 2 — test methods missing `@patch` mock parameters (25 failures)

**Files:** `test_form_handler.py` (most), `test_lambda_integration.py`

**Symptom:** `TypeError: takes 1 positional argument but 2 were given` when pytest invokes the test method.

**Root cause:** `@patch` decorators inject mock objects as positional arguments to the decorated method, but the method signature is missing the corresponding parameters. Example:

```python
# Wrong:
@patch('form_handler.FormHandler.send_email')
def test_send_webhook_notifications_error(self):  # ← missing mock_send_email param
    ...

# Right:
@patch('form_handler.FormHandler.send_email')
def test_send_webhook_notifications_error(self, mock_send_email):
    ...
```

**Effort:** ~30 min. Each affected method needs one parameter added.

### Cluster 3 — production drift, contact_extractor `name_full` field (6 failures)

**File:** `test_contact_extractor.py`

**Symptom:** `KeyError: 'name_full'`

**Root cause:** Production code (`contact_extractor.py`) was updated to remove or rename the `name_full` field; tests were not updated.

**Investigation needed:** read `contact_extractor.py` to confirm whether `name_full` was deliberately removed (update tests) or was a regression (fix production). If deliberate removal, also check downstream consumers (form_handler, notification logic) to ensure no caller expects `name_full`.

**Effort:** ~30 min.

### Cluster 4 — production drift, template renderer (4 failures)

**File:** `test_template_renderer.py`

**Symptoms:**
- `'Hello {first_name} {missing_var}!' != 'Hello John {missing_var}!'` — substitution behavior changed
- assorted assertion failures on email/SMS/webhook template output

**Investigation needed:** compare current `template_renderer.py` behavior against test expectations. Likely the template substitution logic was updated; tests need to follow.

**Effort:** ~30 min.

### Cluster 5 — assertion failures in form/error handling (~11 failures)

**Files:** `test_form_handler.py`, `test_error_handling.py`

**Symptoms:** Various `AssertionError` patterns:
- `'pending_fulfillment' != 'submitted'` — handler returns different status than test expects
- `False is not true` — boolean assertion failures
- `0 != 1` — count assertions

**Investigation needed:** each is a distinct production-vs-test mismatch. Audit one at a time to determine which side is correct.

**Effort:** ~1.5 hours. The most variable cluster.

### Cluster 6 — moto Lambda role-assumption restriction (2 failures)

**File:** `test_lambda_integration.py`

**Symptom:** `InvalidParameterValueException: The role defined for the function cannot be assumed by Lambda.`

**Root cause:** moto v4's Lambda mock requires an IAM role that satisfies its trust-policy validator. The test uses a placeholder role that doesn't match. Fix: create a fake role within the moto IAM mock first, then reference its ARN.

**Effort:** ~15 min.

### Cluster 7 — `'Mock' object is not subscriptable` (1 failure)

**File:** `test_form_handler.py::test_get_monthly_sms_usage_no_record`

**Root cause:** Mock object returned by `@patch` is being indexed as if it were a dict. Either the mock needs `.return_value` configured, or the test is using `.side_effect` incorrectly.

**Effort:** ~10 min.

### Cluster 8 — assorted single-test issues (~4 failures)

- `test_humanize_key_camel_case_handling` — assertion mismatch
- `test_format_template_missing_variable` — template substitution behavior
- `test_cors_headers_present` — CORS header expectation
- `test_null_byte_injection` — input sanitization expectation

**Effort:** ~30 min collectively.

---

## Total estimated effort

| Cluster | Effort |
|---|---|
| 1. boto3 outside moto context | 1 hour |
| 2. Missing `@patch` params | 30 min |
| 3. Contact extractor drift | 30 min |
| 4. Template renderer drift | 30 min |
| 5. Form/error assertion failures | 1.5 hours |
| 6. moto Lambda role | 15 min |
| 7. Mock subscriptable | 10 min |
| 8. Assorted | 30 min |
| **Total** | **~4.5 hours** |

This is a half-day session, not multi-day. Estimate is honest, not optimistic.

## Verification protocol

After each cluster's fix:

1. Run `pytest -q --tb=no` in `Master_Function_Staging/`
2. Confirm pass count increased and no new failures appeared
3. Re-categorize remaining failures (some may resolve as side effects of earlier fixes)
4. Commit per cluster with explicit "Cluster N — [name]: X passing tests recovered" message

## Definition of done

- [ ] All 273 tests in `Master_Function_Staging` pass on the CI runner
- [ ] No `pytest.skip` markers added (every fix is real)
- [ ] No tests deleted unless they validate behavior that has been deliberately removed (with explicit code-side justification)
- [ ] CI's `All Lambda Checks Passed` gate is green for the entire test suite, not just the install/collection phase

## What this PR does NOT cover

- Migration to moto v5 (out of scope; current `moto<5` pin from PR #34 is intentional)
- New tests for new behavior (this is a debt-resolution PR, not a coverage-expansion PR)
- Refactoring of the test suite structure (only fixes; no reorganization)
- Changes to production code (`form_handler.py`, `contact_extractor.py`, etc.) unless a test failure unambiguously points at a production bug

If a cluster reveals a real production issue, file a separate issue/PR for that fix; do not fold it into this debt cleanup.

## References

- [PR #34 — fix(ci): repair Master_Function_Staging Python test infrastructure](https://github.com/longhornrumble/lambda/pull/34)
- Original investigation transcript: 2026-05-02 session memory
