#!/usr/bin/env python3
"""action=log_error — log-only widget-host diagnostics (mis-embed detection).

The widget host reports when the embed snippet runs inside a sandboxed
builder iframe (e.g. a Wix "Embed HTML" element / filesusr.com), where
position:fixed pins the widget to the element's box instead of the viewport
(Atlanta Angels, 2026-07-18). The handler emits one structured CloudWatch
line and stores nothing.

Open-endpoint discipline (pii-data-lifecycle-advisor review 2026-07-18):
hostname-shape allowlist server-side, tenant-hash format check, drop-don't-log
on validation failure, always 200, never echo input, and NEVER log the raw
event envelope (sourceIp / user-agent live there).
"""

import json
import unittest


def _event(body=None, source_ip='198.51.100.7'):
    return {
        'httpMethod': 'POST',
        'headers': {'user-agent': 'sneaky-agent/1.0'},
        'requestContext': {'http': {'method': 'POST', 'sourceIp': source_ip}},
        'body': json.dumps(body) if isinstance(body, dict) else body,
    }


VALID_REPORT = {
    'type': 'embed_sandboxed_frame',
    'frame_host': 'www-atlantaangels-org.filesusr.com',
    'page_host': 'www.atlantaangels.org',
}


class TestHandleLogError(unittest.TestCase):

    def test_valid_report_logs_structured_line(self):
        from lambda_function import handle_log_error
        with self.assertLogs(level='WARNING') as logs:
            response = handle_log_error(_event(VALID_REPORT), 'at807c3896fbd2')
        self.assertEqual(response['statusCode'], 200)
        self.assertEqual(json.loads(response['body']), {'ok': True})
        line = '\n'.join(logs.output)
        self.assertIn('WIDGET_EMBED_MISCONFIG', line)
        self.assertIn('tenant=at807c3896fbd2', line)
        self.assertIn('frame_host=www-atlantaangels-org.filesusr.com', line)
        self.assertIn('page_host=www.atlantaangels.org', line)

    def test_never_logs_request_envelope(self):
        from lambda_function import handle_log_error
        with self.assertLogs(level='INFO') as logs:
            handle_log_error(_event(VALID_REPORT), 'at807c3896fbd2')
        joined = '\n'.join(logs.output)
        self.assertNotIn('198.51.100.7', joined)
        self.assertNotIn('sneaky-agent', joined)

    def test_hostname_injection_stripped(self):
        from lambda_function import handle_log_error
        report = dict(VALID_REPORT, frame_host='evil\nWIDGET_EMBED_MISCONFIG tenant=spoofed')
        with self.assertLogs(level='WARNING') as logs:
            handle_log_error(_event(report), 'at807c3896fbd2')
        line = '\n'.join(logs.output)
        self.assertIn('frame_host= ', line + ' ')  # field emptied, not echoed
        self.assertNotIn('spoofed', line)

    def test_full_url_in_hostname_field_dropped(self):
        from lambda_function import handle_log_error
        report = dict(VALID_REPORT, page_host='https://example.org/path?email=a@b.c')
        with self.assertLogs(level='WARNING') as logs:
            handle_log_error(_event(report), 'at807c3896fbd2')
        joined = '\n'.join(logs.output)
        self.assertNotIn('example.org/path', joined)
        self.assertNotIn('a@b.c', joined)

    def test_invalid_tenant_hash_dropped(self):
        from lambda_function import handle_log_error
        with self.assertLogs(level='INFO') as logs:
            response = handle_log_error(_event(VALID_REPORT), 'INVALID tenant!')
        self.assertEqual(response['statusCode'], 200)
        self.assertNotIn('WIDGET_EMBED_MISCONFIG', '\n'.join(logs.output))

    def test_unknown_type_dropped(self):
        from lambda_function import handle_log_error
        with self.assertLogs(level='INFO') as logs:
            response = handle_log_error(_event({'type': 'something_else'}), 'at807c3896fbd2')
        self.assertEqual(response['statusCode'], 200)
        self.assertNotIn('WIDGET_EMBED_MISCONFIG', '\n'.join(logs.output))

    def test_garbage_body_returns_200(self):
        from lambda_function import handle_log_error
        for body in ('not json', '[]', '', None):
            with self.assertLogs(level='INFO'):
                response = handle_log_error(_event(body), 'at807c3896fbd2')
            self.assertEqual(response['statusCode'], 200)

    def test_dispatch_routes_log_error(self):
        from lambda_function import lambda_handler
        event = _event(VALID_REPORT)
        event['queryStringParameters'] = {'action': 'log_error', 't': 'at807c3896fbd2'}
        with self.assertLogs(level='WARNING') as logs:
            response = lambda_handler(event, None)
        self.assertEqual(response['statusCode'], 200)
        self.assertIn('WIDGET_EMBED_MISCONFIG', '\n'.join(logs.output))


if __name__ == '__main__':
    unittest.main()
