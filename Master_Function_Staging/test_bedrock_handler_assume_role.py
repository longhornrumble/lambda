"""
Tests for the cross-account assume-role KB client factory in
bedrock_handler_optimized.py.

Mirrors the cases in:
  Bedrock_Streaming_Handler_Staging/__tests__/bedrock_core_assume_role.test.js

Design constraint: tests manipulate KB_RETRIEVER_ROLE_ARN via monkeypatch and
reset the module-level cache manually — no importlib.reload required.
"""

import io
import json
import sys
import time
from unittest.mock import ANY, MagicMock, call, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers to reset the module-level cache between test cases
# ---------------------------------------------------------------------------

def _reset_cache(module):
    """Reset the module-level client caches to their initial state."""
    module._kb_client_cache["client"] = None
    module._kb_client_cache["expires_at"] = 0
    module._default_kb_client_cache["client"] = None


# ---------------------------------------------------------------------------
# Shared fake STS credentials returned by assume_role
# ---------------------------------------------------------------------------

FAKE_CREDS = {
    "Credentials": {
        "AccessKeyId": "ASIA_FAKE",
        "SecretAccessKey": "secret",
        "SessionToken": "token",
        "Expiration": "2099-01-01T00:00:00Z",
    }
}

ROLE_ARN = "arn:aws:iam::614056832592:role/picasso-kb-retriever-from-staging"

# ===========================================================================
# bedrock_handler_optimized.py
# ===========================================================================

import bedrock_handler_optimized as bho


class TestBedrockHandlerOptimizedAssumeRole:
    """Tests for bedrock_handler_optimized._get_bedrock_agent_client()"""

    def setup_method(self):
        _reset_cache(bho)

    def test_env_unset_no_sts_call(self, monkeypatch):
        monkeypatch.delenv("KB_RETRIEVER_ROLE_ARN", raising=False)

        fake_client = MagicMock(name="bedrock-agent-default-opt")
        with patch("bedrock_handler_optimized.boto3") as mock_boto3:
            mock_boto3.client.return_value = fake_client
            result = bho._get_bedrock_agent_client()

        assert mock_boto3.client.call_count == 1
        # D11: the default-creds client now carries a fail-fast timeout Config.
        mock_boto3.client.assert_called_once_with("bedrock-agent-runtime", config=ANY)
        assert result is fake_client

    def test_env_set_assumes_role(self, monkeypatch):
        monkeypatch.setenv("KB_RETRIEVER_ROLE_ARN", ROLE_ARN)

        fake_bedrock_client = MagicMock(name="bedrock-agent-assumed-opt")
        mock_sts = MagicMock()
        mock_sts.assume_role.return_value = FAKE_CREDS

        with patch("bedrock_handler_optimized.boto3") as mock_boto3:
            mock_boto3.client.side_effect = [mock_sts, fake_bedrock_client]
            result = bho._get_bedrock_agent_client()

        mock_sts.assume_role.assert_called_once_with(
            RoleArn=ROLE_ARN,
            RoleSessionName="bedrock-kb-retriever",
            DurationSeconds=3600,
        )
        mock_boto3.client.assert_any_call(
            "bedrock-agent-runtime",
            aws_access_key_id="ASIA_FAKE",
            aws_secret_access_key="secret",
            aws_session_token="token",
            config=ANY,  # D11: fail-fast timeout Config
        )
        assert result is fake_bedrock_client

    def test_env_set_client_cached(self, monkeypatch):
        monkeypatch.setenv("KB_RETRIEVER_ROLE_ARN", ROLE_ARN)

        fake_bedrock_client = MagicMock(name="bedrock-agent-cached-opt")
        bho._kb_client_cache["client"] = fake_bedrock_client
        bho._kb_client_cache["expires_at"] = time.time() + 3000

        with patch("bedrock_handler_optimized.boto3") as mock_boto3:
            result = bho._get_bedrock_agent_client()

        mock_boto3.client.assert_not_called()
        assert result is fake_bedrock_client

    def test_env_set_sts_raises_logs_and_propagates(self, monkeypatch, capsys):
        monkeypatch.setenv("KB_RETRIEVER_ROLE_ARN", ROLE_ARN)

        mock_sts = MagicMock()
        mock_sts.assume_role.side_effect = Exception("AccessDenied")

        with patch("bedrock_handler_optimized.boto3") as mock_boto3:
            mock_boto3.client.return_value = mock_sts
            with pytest.raises(Exception, match="AccessDenied"):
                bho._get_bedrock_agent_client()

        captured = capsys.readouterr()
        log_line = json.loads(captured.out.strip())
        assert log_line["evt"] == "analytics_kb_creds_init_failed"
        assert "AccessDenied" in log_line["error"]
