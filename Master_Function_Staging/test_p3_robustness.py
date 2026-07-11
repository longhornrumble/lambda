"""
P3 platform-robustness tests (MFS analysis 2026-07-11, superrepo
docs/audits/master_function_analysis_2026-07-11.md).

D3  — the circuit breaker must NOT count DynamoDB
      ConditionalCheckFailedException: it is by-design control flow
      (analytics_writer idempotency, conversation_handler CAS). Counting
      it opened the breaker on benign 409 bursts → blacklist fail-closed
      → platform-wide 503s for 60s.
D11 — hot-path boto3 clients carry explicit fail-fast timeouts instead
      of the 60s boto3 defaults.
D12 — table-name defaults are bare (account-as-environment), not the
      stale '-{env}'-suffixed legacy twins.
"""
import importlib
import os
import sys
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

from aws_client_manager import CircuitBreaker, boto_config_for


def _conditional_error():
    return ClientError(
        {'Error': {'Code': 'ConditionalCheckFailedException'}}, 'PutItem')


def _real_error():
    return ClientError(
        {'Error': {'Code': 'ProvisionedThroughputExceededException'}}, 'PutItem')


class TestD3BreakerIgnoresConditional409(unittest.TestCase):

    def test_conditional_failures_never_open_breaker(self):
        breaker = CircuitBreaker('dynamodb', failure_threshold=5, timeout=60)

        def raise_conditional():
            raise _conditional_error()

        # Far past the threshold — the breaker must stay CLOSED and keep
        # propagating the exception (callers handle 409s themselves).
        for _ in range(20):
            with self.assertRaises(ClientError):
                breaker.call(raise_conditional)
        self.assertEqual(breaker.state, 'CLOSED')
        self.assertEqual(breaker.failure_count, 0)

    def test_real_failures_still_open_breaker(self):
        breaker = CircuitBreaker('dynamodb', failure_threshold=5, timeout=60)

        def raise_real():
            raise _real_error()

        for _ in range(5):
            with self.assertRaises(ClientError):
                breaker.call(raise_real)
        self.assertEqual(breaker.state, 'OPEN')

    def test_conditional_failure_in_half_open_does_not_reopen(self):
        breaker = CircuitBreaker('dynamodb', failure_threshold=5, timeout=60)
        breaker.state = 'HALF_OPEN'

        def raise_conditional():
            raise _conditional_error()

        with self.assertRaises(ClientError):
            breaker.call(raise_conditional)
        # A benign 409 during recovery probing must not slam the door shut.
        self.assertEqual(breaker.state, 'HALF_OPEN')


class TestD11FailFastTimeouts(unittest.TestCase):

    def test_hot_path_services_have_config_entries(self):
        for service, max_read in (('ses', 5), ('sns', 5), ('lambda', 10),
                                  ('dynamodb', 5), ('s3', 3), ('bedrock', 30)):
            cfg = boto_config_for(service)
            self.assertLessEqual(cfg.connect_timeout, 5, service)
            self.assertLessEqual(cfg.read_timeout, max_read, service)

    def test_form_handler_clients_carry_timeouts(self):
        import form_handler as fh
        self.assertLessEqual(fh.ses.meta.config.read_timeout, 5)
        self.assertLessEqual(fh.sns.meta.config.read_timeout, 5)
        self.assertLessEqual(fh.lambda_client.meta.config.read_timeout, 10)
        self.assertLessEqual(
            fh.dynamodb.meta.client.meta.config.read_timeout, 5)

    def test_tenant_config_loader_s3_carries_timeouts(self):
        import tenant_config_loader as tcl
        self.assertLessEqual(tcl.s3.meta.config.read_timeout, 3)

    def test_bedrock_agent_client_is_memoized(self):
        import bedrock_handler_optimized as bho
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop('KB_RETRIEVER_ROLE_ARN', None)
            bho._default_kb_client_cache['client'] = None
            c1 = bho._get_bedrock_agent_client()
            c2 = bho._get_bedrock_agent_client()
        self.assertIs(c1, c2)
        self.assertLessEqual(c1.meta.config.read_timeout, 30)


class TestD12BareTableDefaults(unittest.TestCase):

    def _reimport_without_env(self, module_name, env_keys):
        saved_env = {k: os.environ.pop(k) for k in env_keys if k in os.environ}
        saved_mod = sys.modules.pop(module_name, None)
        try:
            return importlib.import_module(module_name)
        finally:
            os.environ.update(saved_env)
            if saved_mod is not None:
                sys.modules[module_name] = saved_mod

    def test_blacklist_default_is_bare(self):
        mod = self._reimport_without_env(
            'token_blacklist', ['BLACKLIST_TABLE_NAME', 'ENVIRONMENT'])
        self.assertEqual(mod.BLACKLIST_TABLE_NAME, 'picasso-token-blacklist')

    def test_audit_default_is_bare(self):
        mod = self._reimport_without_env(
            'audit_logger', ['AUDIT_TABLE_NAME', 'ENVIRONMENT'])
        self.assertEqual(mod.AUDIT_TABLE_NAME, 'picasso-audit')

    def test_tenant_registry_default_is_bare(self):
        mod = self._reimport_without_env(
            'tenant_registry', ['TENANT_REGISTRY_TABLE', 'ENVIRONMENT'])
        self.assertEqual(mod.TABLE_NAME, 'picasso-tenant-registry')


if __name__ == '__main__':
    unittest.main()
