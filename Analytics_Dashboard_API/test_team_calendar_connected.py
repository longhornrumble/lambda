"""
Unit tests for G8 (WS-E-PORTAL E13 D3 warnings): the `calendar_connected` boolean on
GET /team/members, derived from the per-coordinator OAuth secret (the §B7 signal).

connected = the secret EXISTS AND its `status` is not 'revoked'. A missing secret → not
connected; a shapeless/absent-status existing secret → connected (mirrors §B7 keeping it);
any read error → not connected (conservative). Covers the helper + the projection wiring.
"""

import json
from unittest.mock import patch, MagicMock

from botocore.exceptions import ClientError

import lambda_function as lf
from lambda_function import _coordinator_calendar_connected


def _secret(value):
    return {'SecretString': value}


def _not_found():
    return ClientError({'Error': {'Code': 'ResourceNotFoundException'}}, 'GetSecretValue')


# --------------------------------------------------------------------------- #
# helper
# --------------------------------------------------------------------------- #

@patch('lambda_function._secrets_client')
def test_connected_when_secret_present_and_not_revoked(mock_sm):
    for status in ('connected', 'active', 'anything-not-revoked'):
        mock_sm.get_secret_value.return_value = _secret(json.dumps({'status': status}))
        assert _coordinator_calendar_connected('TEN1', 'maya@x.com') is True, status


@patch('lambda_function._secrets_client')
def test_connected_when_status_absent_or_null(mock_sm):
    # §B7 keeps a candidate whose secret has no/null status → treat as connected.
    mock_sm.get_secret_value.return_value = _secret(json.dumps({'refresh_token': 'x'}))
    assert _coordinator_calendar_connected('TEN1', 'maya@x.com') is True
    mock_sm.get_secret_value.return_value = _secret(json.dumps({'status': None}))
    assert _coordinator_calendar_connected('TEN1', 'maya@x.com') is True


@patch('lambda_function._secrets_client')
def test_not_connected_when_revoked(mock_sm):
    mock_sm.get_secret_value.return_value = _secret(json.dumps({'status': 'revoked'}))
    assert _coordinator_calendar_connected('TEN1', 'maya@x.com') is False


@patch('lambda_function._secrets_client')
def test_not_connected_when_secret_missing(mock_sm):
    mock_sm.get_secret_value.side_effect = _not_found()
    assert _coordinator_calendar_connected('TEN1', 'nobody@x.com') is False


@patch('lambda_function._secrets_client')
def test_connected_when_secret_shapeless(mock_sm):
    # secret exists but is not JSON → it exists, so connected (mirrors §B7 keeping it).
    mock_sm.get_secret_value.return_value = _secret('not-json')
    assert _coordinator_calendar_connected('TEN1', 'maya@x.com') is True


@patch('lambda_function._secrets_client')
def test_not_connected_on_empty_secret_string(mock_sm):
    mock_sm.get_secret_value.return_value = _secret('')
    assert _coordinator_calendar_connected('TEN1', 'maya@x.com') is False


@patch('lambda_function._secrets_client')
def test_not_connected_on_transient_error_conservative(mock_sm):
    # a non-NotFound error → not connected (surface the 'connect calendar' nudge, don't hide it).
    mock_sm.get_secret_value.side_effect = ClientError({'Error': {'Code': 'Throttling'}}, 'GetSecretValue')
    assert _coordinator_calendar_connected('TEN1', 'maya@x.com') is False


def test_empty_coordinator_id_is_not_connected_no_read():
    # no email → not connected, and never reads a secret.
    with patch('lambda_function._secrets_client') as mock_sm:
        assert _coordinator_calendar_connected('TEN1', '') is False
        mock_sm.get_secret_value.assert_not_called()


@patch('lambda_function._secrets_client')
def test_secret_path_uses_lowercased_email_under_the_prefix(mock_sm):
    mock_sm.get_secret_value.return_value = _secret(json.dumps({'status': 'connected'}))
    _coordinator_calendar_connected('TEN1', 'maya@x.com')
    sid = mock_sm.get_secret_value.call_args.kwargs['SecretId']
    assert sid == f'{lf._OAUTH_SECRET_PATH_PREFIX}/TEN1/maya@x.com'


# --------------------------------------------------------------------------- #
# projection wiring: GET /team/members surfaces calendar_connected per member
# --------------------------------------------------------------------------- #

_ROSTER = [
    {'employeeId': 'e1', 'email': 'Maya@X.com', 'name': 'Maya', 'role': 'member', 'status': 'active', 'type': 'registry'},
    {'employeeId': 'e2', 'email': 'alex@x.com', 'name': 'Alex', 'role': 'member', 'status': 'active', 'type': 'registry'},
]


@patch('lambda_function._coordinator_calendar_connected')
@patch('lambda_function.tenant_registry_ops')
@patch('lambda_function._resolve_team_org_id')
def test_admin_sees_calendar_connected_for_all(mock_org, mock_reg, mock_conn):
    mock_org.return_value = (None, None)  # no Clerk org → skip membership/clerk enrichment
    mock_reg.list_employees.return_value = list(_ROSTER)
    mock_conn.side_effect = lambda t, coord: coord == 'maya@x.com'  # only Maya connected
    r = lf.handle_team_members_list({'role': 'admin', 'email': 'admin@x.com'}, 'TEN1')
    assert r['statusCode'] == 200
    members = {m['name']: m for m in json.loads(r['body'])['members']}
    assert members['Maya']['calendar_connected'] is True
    assert members['Alex']['calendar_connected'] is False
    mock_conn.assert_any_call('TEN1', 'maya@x.com')  # looked up by the LOWER-cased email


@patch('lambda_function._coordinator_calendar_connected')
@patch('lambda_function.tenant_registry_ops')
@patch('lambda_function._resolve_team_org_id')
def test_member_sees_calendar_connected_only_for_own_and_reads_one_secret(mock_org, mock_reg, mock_conn):
    # B2 gate: a non-admin sees calendar_connected ONLY for their own row (others → None), which
    # also bounds the per-request secret reads to exactly 1 (their own) — not the whole roster.
    mock_org.return_value = (None, None)
    mock_reg.list_employees.return_value = list(_ROSTER)
    mock_conn.return_value = True
    r = lf.handle_team_members_list({'role': 'member', 'email': 'Maya@X.com'}, 'TEN1')
    members = {m['name']: m for m in json.loads(r['body'])['members']}
    assert members['Maya']['calendar_connected'] is True   # own → real value
    assert members['Alex']['calendar_connected'] is None    # others → gated to None
    assert mock_conn.call_count == 1                         # only the caller's own secret was read
    mock_conn.assert_called_once_with('TEN1', 'maya@x.com')
