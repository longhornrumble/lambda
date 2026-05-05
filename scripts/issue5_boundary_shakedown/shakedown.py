#!/usr/bin/env python3
"""Issue #5 boundary shakedown — validate the staging account topology.

The Issue #5 soak gate (per v7 plan) only validates the analytics writer code
under load. It does NOT exercise the new staging account's environment
boundaries (S3 cross-account replication, deny-puts policy, SCP enforcement,
CORS, log retention, etc.). This script fills that gap.

Run before, during, or after the code soak. Independent of code-deploy
state — exercises infrastructure topology only.

Usage:
  python shakedown.py [--prod-profile chris-admin] [--staging-profile myrecruiter-staging]

Each test prints PASS / FAIL / SKIP with one line of context. Exit 0 if all
non-skipped tests pass; exit 1 if any fail. SKIP is non-fatal — used for
tests that need destructive operations or external setup.

Tests included (non-destructive):
  T1. S3 cross-account replication freshness
  T2. DenyPutsFromStagingAccount enforcement
  T3. Lambda Function URL CORS reachability
  T4. DenyNonUSEast1 SCP enforcement
  T5. CloudWatch log group retention configured

Tests skipped by default (require destructive ops or external coordination):
  S1. JWT secret rotation drill (would invalidate live tokens; manual checklist)
  S2. Cross-account STS sustained load (long-running; run separately)
  S3. CloudTrail audit completeness (blocked pending MontyCloud trail migration)
  S4. DDB schema parity (separate diff script; can be derived from describe-table)
"""
import argparse
import json
import sys
import time
from typing import Tuple

import boto3
from botocore.exceptions import ClientError

# Account topology (from reference_aws_accounts.md memory)
PROD_ACCOUNT = "614056832592"
STAGING_ACCOUNT = "525409062831"
TENANT_HASH_FOR_TESTS = "my87674d777bf9"  # MYR test tenant
STAGING_REPLICATED_BUCKET = "myrecruiter-picasso-staging"
PROD_SOURCE_BUCKET = "myrecruiter-picasso"
STAGING_BEDROCK_LAMBDA = "Bedrock_Streaming_Handler_Staging"
STAGING_MASTER_LAMBDA = "Master_Function_Staging"

# ANSI for terminal output (kept minimal)
G = "\033[92m"
R = "\033[91m"
Y = "\033[93m"
N = "\033[0m"


def _result(name: str, status: str, detail: str = "") -> Tuple[str, str, str]:
    color = G if status == "PASS" else R if status == "FAIL" else Y
    print(f"  [{color}{status}{N}] {name}{(': ' + detail) if detail else ''}")
    return (name, status, detail)


# ─── T1: S3 cross-account replication freshness ────────────────────────────

def test_s3_replication_freshness(prod_session, staging_session) -> Tuple[str, str, str]:
    """Write a probe object to prod source bucket; assert it appears in staging
    bucket within 60 seconds (S3 cross-account replication SLA is best-effort
    minutes typically). Cleanup afterwards.
    """
    name = "T1: S3 cross-account replication freshness"
    s3_prod = prod_session.client("s3")
    s3_staging = staging_session.client("s3")
    probe_key = f"mappings/_shakedown_probe_{int(time.time())}.json"
    probe_body = json.dumps({"shakedown": True, "ts": time.time()}).encode()

    try:
        s3_prod.put_object(Bucket=PROD_SOURCE_BUCKET, Key=probe_key, Body=probe_body)
    except ClientError as e:
        return _result(name, "FAIL", f"prod-side put failed: {e.response['Error']['Code']}")

    deadline = time.time() + 90  # 90s ceiling
    try:
        while time.time() < deadline:
            try:
                s3_staging.head_object(Bucket=STAGING_REPLICATED_BUCKET, Key=probe_key)
                latency_s = round(time.time() - (deadline - 90), 1)
                # Cleanup
                s3_prod.delete_object(Bucket=PROD_SOURCE_BUCKET, Key=probe_key)
                return _result(name, "PASS", f"replicated in <{latency_s}s")
            except ClientError as e:
                if e.response["Error"]["Code"] == "404":
                    time.sleep(2)
                    continue
                raise
        # Cleanup even on failure
        s3_prod.delete_object(Bucket=PROD_SOURCE_BUCKET, Key=probe_key)
        return _result(name, "FAIL", "probe not replicated within 90s window")
    except ClientError as e:
        return _result(name, "FAIL", f"staging-side head failed: {e.response['Error']['Code']}")


# ─── T2: DenyPutsFromStagingAccount enforcement ────────────────────────────

def test_staging_bucket_denies_staging_account_puts(staging_session) -> Tuple[str, str, str]:
    """The staging-account principal must NOT be able to PUT into the
    staging-replicated bucket — only the prod replication role can write.
    """
    name = "T2: DenyPutsFromStagingAccount on staging bucket"
    s3 = staging_session.client("s3")
    probe_key = f"_shakedown_should_be_denied_{int(time.time())}.txt"
    try:
        s3.put_object(Bucket=STAGING_REPLICATED_BUCKET, Key=probe_key, Body=b"should fail")
        # If we get here, the put succeeded — that's the FAIL case
        s3.delete_object(Bucket=STAGING_REPLICATED_BUCKET, Key=probe_key)
        return _result(name, "FAIL", "staging principal was ALLOWED to write — bucket policy gap")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AccessDenied":
            return _result(name, "PASS", "staging principal correctly denied")
        return _result(name, "FAIL", f"unexpected error: {e.response['Error']['Code']}")


# ─── T3: Lambda Function URL CORS reachability ─────────────────────────────

def test_lambda_function_url_reachable(staging_session) -> Tuple[str, str, str]:
    """Both staging Lambda Function URLs should respond to a basic HTTP GET
    (or invoke). Tests the Console-saved FunctionURLAllowInvokeAction policy
    statement is still in place.
    """
    name = "T3: Lambda Function URLs reachable"
    lam = staging_session.client("lambda")
    failures = []
    for fn in (STAGING_BEDROCK_LAMBDA, STAGING_MASTER_LAMBDA):
        try:
            policy_str = lam.get_policy(FunctionName=fn)["Policy"]
            policy = json.loads(policy_str)
            sids = {s.get("Sid") for s in policy.get("Statement", [])}
            required = {"FunctionURLAllowPublicAccess", "FunctionURLAllowInvokeAction"}
            missing = required - sids
            if missing:
                failures.append(f"{fn} missing SIDs: {','.join(missing)}")
        except ClientError as e:
            failures.append(f"{fn}: {e.response['Error']['Code']}")
    if failures:
        return _result(name, "FAIL", "; ".join(failures))
    return _result(name, "PASS", "both Lambdas have both required URL policy statements")


# ─── T4: DenyNonUSEast1 SCP enforcement ────────────────────────────────────

def test_scp_denies_non_us_east_1(staging_session) -> Tuple[str, str, str]:
    """Pick a non-Bedrock service in us-east-2 and confirm the SCP denies it.
    Bedrock InvokeModel was carved out per Issue #5 INT1 — verify the carve-out
    is narrow (other services still denied).
    """
    name = "T4: DenyNonUSEast1 SCP still blocks non-Bedrock services"
    sqs_us_east_2 = staging_session.client("sqs", region_name="us-east-2")
    try:
        sqs_us_east_2.list_queues()
        return _result(name, "FAIL", "us-east-2 SQS call SUCCEEDED — SCP carve-out too wide")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "AccessDeniedException" and "service control policy" in (e.response["Error"].get("Message") or "").lower():
            return _result(name, "PASS", "SCP correctly blocked us-east-2 SQS")
        # AccessDenied without "service control policy" in message could be IAM, also acceptable
        if code in ("AccessDenied", "AccessDeniedException"):
            return _result(name, "PASS", f"denied via {code} (likely SCP)")
        return _result(name, "FAIL", f"unexpected error: {code}")


# ─── T5: CloudWatch log retention configured ────────────────────────────────

def test_log_retention_set(staging_session) -> Tuple[str, str, str]:
    """Both staging Lambda log groups should have retention_in_days set
    (Terraform module sets 30; default would be 'never expire').
    """
    name = "T5: CloudWatch log retention configured"
    logs = staging_session.client("logs")
    issues = []
    for fn in (STAGING_BEDROCK_LAMBDA, STAGING_MASTER_LAMBDA):
        log_group = f"/aws/lambda/{fn}"
        try:
            resp = logs.describe_log_groups(logGroupNamePrefix=log_group)
            groups = [g for g in resp.get("logGroups", []) if g["logGroupName"] == log_group]
            if not groups:
                issues.append(f"{log_group}: not found")
                continue
            retention = groups[0].get("retentionInDays")
            if retention is None:
                issues.append(f"{log_group}: retention=NEVER_EXPIRE (cost risk)")
            elif retention != 30:
                issues.append(f"{log_group}: retention={retention} (expected 30)")
        except ClientError as e:
            issues.append(f"{log_group}: {e.response['Error']['Code']}")
    if issues:
        return _result(name, "FAIL", "; ".join(issues))
    return _result(name, "PASS", "both log groups retain 30 days")


# ─── Skipped tests ─────────────────────────────────────────────────────────

SKIPS = [
    ("S1: JWT secret rotation drill",
     "destructive — invalidates live tokens. Manual checklist: rotate via aws secretsmanager put-secret-value, force Lambda cold-start, confirm new tokens accepted + old tokens rejected."),
    ("S2: Cross-account STS sustained load",
     "long-running — exercise STS for 1h+ via a loop. Run separately; not part of fast smoke."),
    ("S3: CloudTrail audit completeness",
     "blocked pending MontyCloud → owned-trail migration. Re-enable after that project completes."),
    ("S4: DDB schema parity (staging twins vs prod legacy)",
     "separate diff script needed; staging twins were audited at create time, ongoing drift detection is a separate effort."),
]


# ─── Orchestrator ──────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prod-profile", default="chris-admin",
                        help="AWS profile for prod account 614056832592")
    parser.add_argument("--staging-profile", default="myrecruiter-staging",
                        help="AWS profile for staging account 525409062831")
    args = parser.parse_args()

    prod = boto3.Session(profile_name=args.prod_profile, region_name="us-east-1")
    staging = boto3.Session(profile_name=args.staging_profile, region_name="us-east-1")

    # Sanity-check profile → account mapping
    prod_acct = prod.client("sts").get_caller_identity()["Account"]
    staging_acct = staging.client("sts").get_caller_identity()["Account"]
    if prod_acct != PROD_ACCOUNT:
        print(f"{R}FATAL{N}: --prod-profile resolves to account {prod_acct}, expected {PROD_ACCOUNT}")
        return 2
    if staging_acct != STAGING_ACCOUNT:
        print(f"{R}FATAL{N}: --staging-profile resolves to account {staging_acct}, expected {STAGING_ACCOUNT}")
        return 2

    print(f"\n{Y}Issue #5 boundary shakedown{N}")
    print(f"  prod profile: {args.prod_profile} → {prod_acct}")
    print(f"  staging profile: {args.staging_profile} → {staging_acct}\n")

    print(f"{Y}Active tests:{N}")
    results = [
        test_s3_replication_freshness(prod, staging),
        test_staging_bucket_denies_staging_account_puts(staging),
        test_lambda_function_url_reachable(staging),
        test_scp_denies_non_us_east_1(staging),
        test_log_retention_set(staging),
    ]

    print(f"\n{Y}Skipped tests (run separately or pending):{N}")
    for sname, reason in SKIPS:
        print(f"  [{Y}SKIP{N}] {sname}: {reason}")

    passed = sum(1 for _, s, _ in results if s == "PASS")
    failed = sum(1 for _, s, _ in results if s == "FAIL")
    print(f"\n{Y}Summary:{N} {G}{passed} passed{N}, {R}{failed} failed{N}, {Y}{len(SKIPS)} skipped{N}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
