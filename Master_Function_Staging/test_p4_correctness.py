"""
P4 correctness-medium tests (MFS analysis 2026-07-11, superrepo
docs/audits/master_function_analysis_2026-07-11.md).

D6  — the streaming-fallback loop variable no longer shadows the Lambda
      `event` param, so add_cors_headers reflects the real request origin.
D7  — client-supplied session_id is honored; fallback ids are unique.
D8  — form_cta_enhancer config cache expires (5-min TTL).
D9  — audit gate: _log_audit_event returns True on duplicate-condition
      failures (idempotent success) and False on real write errors; the
      conversation retrieval gate actually fails closed on False.
D14 — cache hits return deep copies (mutation can't poison the cache);
      legacy conversation delete paginates and keys on messageTimestamp.
"""
import json
import time
import unittest
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError


class TestD7SessionId(unittest.TestCase):

    def test_client_session_id_is_honored(self):
        from intent_router import extract_session_id
        event = {'body': json.dumps({'context': {'session_id': 'sess_client_1'}})}
        self.assertEqual(extract_session_id(event), 'sess_client_1')

    def test_fallback_ids_do_not_collide(self):
        # Pre-fix, the function-local `import time` broke every earlier
        # branch and the final fallback was second-granularity —
        # concurrent users collided. All generated ids must be unique.
        import intent_router
        with patch.object(intent_router, 'extract_session_data',
                          side_effect=RuntimeError('no session utils')):
            a = intent_router.extract_session_id({})
            b = intent_router.extract_session_id({})
        self.assertTrue(a.startswith('session_'))
        self.assertNotEqual(a, b)


class TestD8ConfigCacheTtl(unittest.TestCase):

    def test_stale_entry_is_refetched(self):
        import form_cta_enhancer as fce
        stale = {'conversational_forms': {'old': {}}, 'form_settings': {},
                 'conversation_branches': {}, 'cta_definitions': {},
                 '_cached_at': time.time() - 600}
        fce.config_cache['hash1234567890'] = stale
        self.addCleanup(fce.config_cache.pop, 'hash1234567890', None)
        with patch.object(fce, 'resolve_tenant_hash', return_value='T1'), \
             patch.object(fce, 's3_client') as mock_s3:
            mock_s3.get_object.return_value = {
                'Body': MagicMock(read=lambda: json.dumps(
                    {'conversational_forms': {'new': {}}}).encode())
            }
            result = fce.load_tenant_config('hash1234567890')
        self.assertIn('new', result['conversational_forms'])
        self.assertNotIn('old', result['conversational_forms'])

    def test_fresh_entry_served_from_cache(self):
        import form_cta_enhancer as fce
        fresh = {'conversational_forms': {'cached': {}}, 'form_settings': {},
                 'conversation_branches': {}, 'cta_definitions': {},
                 '_cached_at': time.time()}
        fce.config_cache['hash1234567890'] = fresh
        self.addCleanup(fce.config_cache.pop, 'hash1234567890', None)
        with patch.object(fce, 's3_client') as mock_s3:
            result = fce.load_tenant_config('hash1234567890')
            mock_s3.get_object.assert_not_called()
        self.assertIn('cached', result['conversational_forms'])


class TestD9AuditTruthfulness(unittest.TestCase):

    def _log_event(self, error_code=None):
        import audit_logger as al
        logger_inst = al.AuditLogger()
        mock_ddb = MagicMock()
        if error_code:
            mock_ddb.put_item.side_effect = ClientError(
                {'Error': {'Code': error_code}}, 'PutItem')
        logger_inst.dynamodb = mock_ddb
        return logger_inst._log_audit_event(
            tenant_id='T1', event_type='CONVERSATION_RETRIEVED',
            session_id='sess1')

    def test_duplicate_condition_counts_as_success(self):
        self.assertTrue(self._log_event('ConditionalCheckFailedException'))

    def test_real_write_error_returns_false(self):
        self.assertFalse(self._log_event('ProvisionedThroughputExceededException'))

    def test_clean_write_returns_true(self):
        self.assertTrue(self._log_event(None))

    def test_retrieval_gate_fails_closed_on_false(self):
        import conversation_handler as ch
        token_data = {'sessionId': 'sess_123456789012',
                      'tenantId': 'T1_23456789', 'turn': 1}
        with patch.object(ch, '_validate_state_token', return_value=token_data), \
             patch.object(ch, '_check_rate_limit'), \
             patch.object(ch, '_get_conversation_from_db', return_value={}), \
             patch.object(ch, '_generate_rotated_token', return_value='tok2'), \
             patch.object(ch, 'AUDIT_LOGGER_AVAILABLE', True), \
             patch.object(ch.audit_logger, '_log_audit_event', return_value=False):
            with self.assertRaises(ch.ConversationError) as ctx:
                ch.handle_get_conversation({'headers': {}})
        self.assertEqual(ctx.exception.status_code, 503)

    def test_retrieval_succeeds_when_audit_ok(self):
        import conversation_handler as ch
        token_data = {'sessionId': 'sess_123456789012',
                      'tenantId': 'T1_23456789', 'turn': 1}
        with patch.object(ch, '_validate_state_token', return_value=token_data), \
             patch.object(ch, '_check_rate_limit'), \
             patch.object(ch, '_get_conversation_from_db', return_value={}), \
             patch.object(ch, '_generate_rotated_token', return_value='tok2'), \
             patch.object(ch, 'AUDIT_LOGGER_AVAILABLE', True), \
             patch.object(ch.audit_logger, '_log_audit_event', return_value=True):
            response = ch.handle_get_conversation({'headers': {}})
        self.assertEqual(response['statusCode'], 200)


class TestD14CacheDeepCopy(unittest.TestCase):

    def test_cache_hit_mutation_does_not_poison_cache(self):
        import tenant_config_loader as tcl
        tenant_hash = 'aabbccdd1122'
        seeded = {'conversational_forms': {'f1': {'title': 'orig'}},
                  'tenant_hash': tenant_hash}
        tcl.cached_config[tenant_hash] = seeded
        tcl.cache_timestamps[tenant_hash] = time.time()
        self.addCleanup(tcl.cached_config.pop, tenant_hash, None)
        self.addCleanup(tcl.cache_timestamps.pop, tenant_hash, None)

        with patch.object(tcl, 'is_valid_tenant_hash', return_value=True):
            first = tcl.get_config_for_tenant_by_hash(tenant_hash)
            first['conversational_forms']['f1']['title'] = 'MUTATED'
            second = tcl.get_config_for_tenant_by_hash(tenant_hash)
        self.assertEqual(second['conversational_forms']['f1']['title'], 'orig')


class TestD14DeletePagination(unittest.TestCase):

    def test_legacy_delete_paginates_and_uses_message_timestamp(self):
        import conversation_handler as ch
        pages = [
            {'Items': [{'sessionId': {'S': 's1'},
                        'messageTimestamp': {'N': '1'}}],
             'LastEvaluatedKey': {'sessionId': {'S': 's1'},
                                  'messageTimestamp': {'N': '1'}}},
            {'Items': [{'sessionId': {'S': 's1'},
                        'messageTimestamp': {'N': '2'}}]},
        ]
        mock_ddb = MagicMock()
        mock_ddb.query.side_effect = pages
        with patch.object(ch, 'AWS_CLIENT_MANAGER_AVAILABLE', False), \
             patch.object(ch, 'dynamodb', mock_ddb, create=True):
            ch._delete_conversation_from_db('s1', 'T1')

        # Both pages queried; second call carries ExclusiveStartKey
        self.assertEqual(mock_ddb.query.call_count, 2)
        self.assertIn('ExclusiveStartKey', mock_ddb.query.call_args_list[1].kwargs)
        # Legacy branch projects the REAL sort key (was 'timestamp' → KeyError)
        for call in mock_ddb.query.call_args_list:
            self.assertEqual(call.kwargs['ExpressionAttributeNames'],
                             {'#ts': 'messageTimestamp'})
        # Both messages deleted, keyed on messageTimestamp
        delete_keys = [c.kwargs['Key']['messageTimestamp']['N']
                       for c in mock_ddb.delete_item.call_args_list
                       if 'messageTimestamp' in c.kwargs.get('Key', {})]
        self.assertEqual(sorted(delete_keys), ['1', '2'])


class TestD6NoEventShadow(unittest.TestCase):

    def test_streaming_fallback_reflects_request_origin(self):
        import lambda_function as lf
        event = {
            'headers': {'origin': 'https://staging.chat.myrecruiter.ai'},
            'body': json.dumps({'user_input': 'hello'}),
        }
        fake_stream = {'body': [
            {'chunk': {'bytes': json.dumps({
                'type': 'content_block_delta',
                'delta': {'type': 'text_delta', 'text': 'hi'}}).encode()}},
            {'chunk': {'bytes': json.dumps({'type': 'message_stop'}).encode()}},
        ]}
        mock_bedrock = MagicMock()
        mock_bedrock.invoke_model_with_response_stream.return_value = fake_stream
        with patch('boto3.client', return_value=mock_bedrock), \
             patch.dict('os.environ', {'BEDROCK_MODEL_ID': 'model-x'}), \
             patch('tenant_config_loader.get_config_for_tenant_by_hash',
                   return_value=None):
            response = lf.handle_streaming_chat_fallback(event, 'aabbccdd1122')
        self.assertEqual(response['statusCode'], 200)
        # Pre-fix, `event` was shadowed by the last Bedrock chunk and the
        # ACAO fell back to the prod default instead of the staging origin.
        self.assertEqual(
            response['headers']['Access-Control-Allow-Origin'],
            'https://staging.chat.myrecruiter.ai')


if __name__ == '__main__':
    unittest.main()
