#!/usr/bin/env python3
"""
One-time backfill: populate employee registry from Clerk org memberships.

Usage:
    AWS_PROFILE=chris-admin CLERK_SECRET_KEY=sk_live_xxx python3 backfill_employee_registry.py

Optionally override the DynamoDB table environment:
    ENVIRONMENT=production AWS_PROFILE=chris-admin CLERK_SECRET_KEY=sk_live_xxx python3 backfill_employee_registry.py

Safe to re-run (idempotent — put_employee uses PutItem which overwrites).
Each run writes the current Clerk state for all org members across all tenants
that have a clerkOrgId.
"""

import os
import sys
import json
import urllib.request
import urllib.error
import tenant_registry_ops

CLERK_SECRET_KEY = os.environ.get('CLERK_SECRET_KEY', '')
CLERK_API_BASE = 'https://api.clerk.com'


def clerk_request(method, path):
    """Make an authenticated request to the Clerk API.

    Returns the parsed JSON body on success, or None on HTTP error.
    Network errors are re-raised so the caller can decide how to handle them.
    """
    url = f'{CLERK_API_BASE}{path}'
    req = urllib.request.Request(url, method=method)
    req.add_header('Authorization', f'Bearer {CLERK_SECRET_KEY}')
    req.add_header('Content-Type', 'application/json')
    req.add_header('User-Agent', 'picasso-backfill/1.0')
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='replace')
        print(f'  ERROR {e.code} {path}: {body[:200]}')
        return None


def get_memberships(org_id):
    """Fetch all memberships for a Clerk org.

    Handles the list envelope Clerk returns: {"data": [...], "total_count": N}.
    Returns a list of membership objects, or an empty list on error.
    """
    path = f'/v1/organizations/{org_id}/memberships?limit=100'
    result = clerk_request('GET', path)
    if result is None:
        return []
    # Clerk returns {"data": [...], "total_count": N} for list endpoints
    if isinstance(result, list):
        return result
    return result.get('data', [])


def get_user(user_id):
    """Fetch a single Clerk user by ID. Returns the user dict or None."""
    return clerk_request('GET', f'/v1/users/{user_id}')


def primary_email(user):
    """Extract the primary email address from a Clerk user object."""
    primary_id = user.get('primary_email_address_id', '')
    for addr in user.get('email_addresses', []):
        if addr.get('id') == primary_id:
            return addr.get('email_address', '')
    # Fall back to first address if primary ID not matched
    addresses = user.get('email_addresses', [])
    if addresses:
        return addresses[0].get('email_address', '')
    return ''


def map_role(clerk_role):
    """Map Clerk org role string to portal role.

    org:admin  → admin
    anything else → member
    """
    return 'admin' if clerk_role == 'org:admin' else 'member'


def main():
    if not CLERK_SECRET_KEY:
        print('ERROR: CLERK_SECRET_KEY environment variable is required')
        sys.exit(1)

    env = os.environ.get('ENVIRONMENT', 'staging')
    print(f'Environment : {env}')
    print(f'Tenant table: {tenant_registry_ops.TENANT_TABLE}')
    print(f'Employee table: {tenant_registry_ops.EMPLOYEE_TABLE}')
    print()

    # --- Step 1: load tenants ---
    print('Fetching tenants from registry...')
    try:
        tenants = tenant_registry_ops.list_all_tenants()
    except Exception as exc:
        print(f'ERROR: Could not load tenants: {exc}')
        sys.exit(1)

    clerk_tenants = [t for t in tenants if t.get('clerkOrgId')]
    skipped = len(tenants) - len(clerk_tenants)
    print(
        f'Found {len(tenants)} total tenants, '
        f'{len(clerk_tenants)} with Clerk orgs, '
        f'{skipped} skipped (no clerkOrgId)'
    )

    total_employees = 0
    errors = 0

    # --- Step 2: iterate tenants ---
    for tenant in clerk_tenants:
        tenant_id = tenant['tenantId']
        org_id = tenant['clerkOrgId']
        company = tenant.get('companyName', tenant_id)
        print(f'\n--- {company} (tenant={tenant_id}, org={org_id}) ---')

        members = get_memberships(org_id)
        if not members:
            print('  No memberships returned (or error fetching) — skipping tenant')
            errors += 1
            continue

        print(f'  Found {len(members)} memberships')

        for m in members:
            # Clerk membership object has public_user_data.user_id
            public_data = m.get('public_user_data', {})
            user_id = public_data.get('user_id', '')
            if not user_id:
                print('  WARN: membership missing user_id — skipping')
                continue

            # Fetch full user record for email + display name
            user = get_user(user_id)
            if not user:
                print(f'  ERROR: could not fetch user {user_id} — skipping')
                errors += 1
                continue

            first = user.get('first_name', '') or ''
            last = user.get('last_name', '') or ''
            name = f'{first} {last}'.strip()
            email = primary_email(user)
            clerk_role = m.get('role', 'org:member')
            role = map_role(clerk_role)

            print(f'  Writing: {email or user_id} | {name or "(no name)"} | {role}')
            try:
                tenant_registry_ops.put_employee(tenant_id, user_id, {
                    'email': email,
                    'name': name,
                    'role': role,
                    'status': 'active',
                })
                total_employees += 1
            except Exception as exc:
                print(f'  ERROR writing employee {email or user_id}: {exc}')
                errors += 1

    # --- Summary ---
    print()
    print('=' * 50)
    print(f'Done: {total_employees} employees written, {errors} errors')
    print('=' * 50)

    if errors:
        sys.exit(1)


if __name__ == '__main__':
    main()
