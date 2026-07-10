"""
Contract tests for the X-Tenant-Override authorization boundary.

The dashboard's super-admin tenant switcher sends X-Tenant-Override on every
API call; the header is fully client-controlled. Tenant isolation therefore
rests on lambda_handler deriving the caller's role from the SIGNATURE-VERIFIED
JWT and refusing the override for anyone below super_admin. These tests pin
that boundary end-to-end through lambda_handler (real HMAC tokens, patched
signing secret) so a refactor of the auth block cannot silently weaken it:

  - non-super-admin + override  -> 403, no handler reached (tamper signal)
  - super_admin + override      -> honored, handler gets the OVERRIDE tenant
  - no override                 -> handler gets the token's own tenant
  - forged signature            -> 401 (the role claim cannot be spoofed)
  - super_admin + bad override  -> 400 (sanitizer still applies)
"""

import base64
import hashlib
import hmac
import json
import time
from unittest.mock import patch

import lambda_function
from lambda_function import lambda_handler

TEST_SECRET = 'test-signing-secret'
OWN_TENANT = 'TEN000000001'
OTHER_TENANT = 'TEN000000002'


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b'=').decode()


def make_token(role: str, tenant_id: str = OWN_TENANT, secret: str = TEST_SECRET) -> str:
    """Mint an HMAC-SHA256 JWT the way SSO_Token_Generator / the Clerk bridge does."""
    header = _b64url(json.dumps({'alg': 'HS256', 'typ': 'JWT'}).encode())
    payload = _b64url(json.dumps({
        'tenant_id': tenant_id,
        'email': f'{role}@test.example',
        'role': role,
        'exp': int(time.time()) + 3600,
    }).encode())
    sig = _b64url(hmac.new(secret.encode(), f'{header}.{payload}'.encode(), hashlib.sha256).digest())
    return f'{header}.{payload}.{sig}'


def make_event(token: str, override: str = None, override_header: str = 'X-Tenant-Override'):
    headers = {'Authorization': f'Bearer {token}'}
    if override is not None:
        headers[override_header] = override
    return {
        'requestContext': {'http': {'method': 'GET'}},
        'rawPath': '/features',
        'headers': headers,
    }


def invoke(event):
    """Run lambda_handler with the signing secret pinned and /features stubbed
    to capture which tenant the request was resolved to."""
    seen = {}

    def fake_features(tenant_id):
        seen['tenant_id'] = tenant_id
        return lambda_function.cors_response(200, {'features': {}})

    with patch.object(lambda_function, 'get_jwt_secret', return_value=TEST_SECRET), \
         patch.object(lambda_function, 'handle_features', side_effect=fake_features):
        resp = lambda_handler(event, None)
    return resp, seen


def test_member_with_override_gets_403_and_no_data():
    resp, seen = invoke(make_event(make_token('member'), override=OTHER_TENANT))
    assert resp['statusCode'] == 403
    assert 'Not authorized' in json.loads(resp['body'])['error']
    assert seen == {}  # short-circuited before any handler ran


def test_admin_with_override_gets_403():
    # admin is still below super_admin for cross-tenant access
    resp, seen = invoke(make_event(make_token('admin'), override=OTHER_TENANT))
    assert resp['statusCode'] == 403
    assert seen == {}


def test_lowercase_override_header_is_also_rejected():
    resp, seen = invoke(make_event(make_token('member'), override=OTHER_TENANT,
                                   override_header='x-tenant-override'))
    assert resp['statusCode'] == 403
    assert seen == {}


def test_super_admin_override_is_honored():
    resp, seen = invoke(make_event(make_token('super_admin'), override=OTHER_TENANT))
    assert resp['statusCode'] == 200
    assert seen['tenant_id'] == OTHER_TENANT


def test_spaced_role_normalizes_to_super_admin():
    # Legacy tokens carry 'Super Admin'; authenticate_request normalizes it.
    resp, seen = invoke(make_event(make_token('Super Admin'), override=OTHER_TENANT))
    assert resp['statusCode'] == 200
    assert seen['tenant_id'] == OTHER_TENANT


def test_no_override_scopes_to_own_tenant():
    resp, seen = invoke(make_event(make_token('member')))
    assert resp['statusCode'] == 200
    assert seen['tenant_id'] == OWN_TENANT


def test_forged_signature_is_401_even_with_super_admin_role():
    # Signed with the WRONG secret: the role claim inside is worthless.
    forged = make_token('super_admin', secret='attacker-secret')
    resp, seen = invoke(make_event(forged, override=OTHER_TENANT))
    assert resp['statusCode'] == 401
    assert seen == {}


def test_super_admin_with_malformed_override_gets_400():
    resp, seen = invoke(make_event(make_token('super_admin'), override="TEN'; DROP--"))
    assert resp['statusCode'] == 400
    assert seen == {}
