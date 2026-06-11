# Calendar_OAuth_Connect — deploy notes (integrator applies)

WS-E-OAUTH (E11) backend. This Lambda is staging-only; **no prod**. Per CLAUDE.md the IAM,
Function URL, CloudFront routing, and the init-token mint are **integrator glue** — this file is
the spec for them. The Lambda code itself is file-disjoint and ships in this PR.

Staging account: **525409062831**, region **us-east-1**.

---

## 1. Environment variables

| Var | Value (staging) | Notes |
|---|---|---|
| `ENVIRONMENT` | `staging` | drives the ONBOARDER default |
| `OAUTH_PLATFORM_SECRET_NAME` | `picasso/scheduling/oauth/_platform/google-app` | the one platform OAuth app (D2) |
| `OAUTH_STATE_SIGNING_SECRET_NAME` | `picasso/scheduling/oauth/_state-signing-key` | HMAC key for init + state tokens (shared with the integrator's mint) |
| `OAUTH_SECRET_PATH_PREFIX` | `picasso/scheduling/oauth` | matches oauth-client.js |
| `OAUTH_REDIRECT_URI` | `https://staging.schedule.myrecruiter.ai/oauth/callback` | fallback if not in the platform secret; MUST equal the Google console redirect URI |
| `DASHBOARD_RETURN_URL` | the dashboard scheduling page (https) | where the user lands after connect |
| `ONBOARDER_FUNCTION_NAME` | `Calendar_Watch_Onboarder-staging` | the B5 watch onboarder to fire |
| `CONFIG_BUCKET` | the tenant-config bucket (same as the redemption handler / onboarder) | featureGate Flag-A read |
| `STATE_TTL_SECONDS` | `600` | OAuth state lifetime |
| `JTI_BLACKLIST_TABLE` | `picasso-token-jti-blacklist` | **BARE name** — NOT `-staging`-suffixed. The live table is bare-named; the IaC grant (picasso#527) targets the bare name. When unset/empty, single-use enforcement is OFF (fail-open — safe for deploy before the table is wired, but wire it before Beta). Requires `dynamodb:PutItem` on the table (see §2 IAM below). |
| `AWS_REQUEST_TIMEOUT_MS` / `AWS_CONNECTION_TIMEOUT_MS` / `AWS_MAX_ATTEMPTS` | `5000` / `3000` / `2` | bounded SDK client |

---

## 2. Per-Lambda IAM (dedicated role `Calendar_OAuth_Connect-exec-staging` — NEVER shared, CLAUDE.md)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ReadPlatformAndStateSecrets",
      "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": [
        "arn:aws:secretsmanager:us-east-1:525409062831:secret:picasso/scheduling/oauth/_platform/google-app-*",
        "arn:aws:secretsmanager:us-east-1:525409062831:secret:picasso/scheduling/oauth/_state-signing-key-*"
      ]
    },
    {
      "Sid": "ManagePerCoordinatorOAuthSecrets",
      "Effect": "Allow",
      "Action": [
        "secretsmanager:GetSecretValue",
        "secretsmanager:DescribeSecret",
        "secretsmanager:PutSecretValue",
        "secretsmanager:CreateSecret"
      ],
      "Resource": "arn:aws:secretsmanager:us-east-1:525409062831:secret:picasso/scheduling/oauth/*",
      "Condition": {
        "StringNotLike": {
          "secretsmanager:SecretId": "arn:aws:secretsmanager:us-east-1:525409062831:secret:picasso/scheduling/oauth/_*"
        }
      }
    },
    {
      "Sid": "FireB5WatchOnboarder",
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:us-east-1:525409062831:function:Calendar_Watch_Onboarder-staging"
    },
    {
      "Sid": "ReadTenantConfigForFlagAGate",
      "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::<TENANT_CONFIG_BUCKET>/tenants/*"
    },
    {
      "Sid": "BurnInitTokenJti",
      "Effect": "Allow",
      "Action": "dynamodb:PutItem",
      "Resource": "arn:aws:dynamodb:us-east-1:525409062831:table/picasso-token-jti-blacklist"
    }
  ]
}

```

Plus the standard `logs:CreateLogGroup`/`CreateLogStream`/`PutLogEvents` (or `AWSLambdaBasicExecutionRole`).

**IAM-charset note (CLAUDE.md):** the `Sid`/descriptions above are ASCII — run
`grep -rnP '[^\x09\x0A\x0D\x20-\x7E\xA1-\xFF]' infra/` before apply.

**Least-privilege (defense-in-depth, hardened per audit 2026-06-05):** statement 2 now carries a
`StringNotLike` condition excluding `picasso/scheduling/oauth/_*`, so the per-coordinator writer
cannot write the `_platform`/`_state-signing-key` secrets at the IAM layer — not only the code-level
reserved-`_`-prefix guard in `secrets.buildSecretPath`. Two independent gates.

**KMS:** the scheduling OAuth secrets use the default `aws/secretsmanager` key (per the B
provisioning runbook). If any are re-keyed to a CMK, add `kms:Decrypt` on that key ARN.

---

## 3. WS-D3 CloudFront routing note (integrator wires in `infra/`)

The D3 distribution (`staging.schedule.myrecruiter.ai`) currently fronts the WS-D4 redemption
Function URL. This Lambda adds **three GET paths** that must route to the **Calendar_OAuth_Connect
Function URL** origin:

- `GET /connect`
- `GET /oauth/callback`   ← MUST exactly equal the Google console "Authorized redirect URI" and `OAUTH_REDIRECT_URI`
- `GET /connection/status`

Mirror the WS-D3 module pattern (`infra/modules/scheduling-redemption-domain-staging/`): add a
CloudFront cache-behavior (or a second origin + path-pattern behaviors) for these paths →
this Lambda's Function URL, HTTPS-only, `cache-control: no-store` already set by the handler.
The redemption paths (`/cancel`, `/reschedule`, `/resume`, `/attended/*`) stay on D4.

---

## 4. Operator-provision items (out-of-band, before first connect)

1. **Platform OAuth app secret** `picasso/scheduling/oauth/_platform/google-app`:
   ```json
   { "client_id": "<google web client id>", "client_secret": "<...>",
     "redirect_uri": "https://staging.schedule.myrecruiter.ai/oauth/callback" }
   ```
   Reuse the existing `picasso-scheduling-staging-web` OAuth client (sub-phase B runbook Step 1);
   add the callback above to its **Authorized redirect URIs** in Google Cloud Console.
2. **State-signing key** `picasso/scheduling/oauth/_state-signing-key`: a random 32-byte secret,
   e.g. `aws secretsmanager create-secret --name picasso/scheduling/oauth/_state-signing-key
   --secret-string "$(openssl rand -hex 32)"`. Shared with the integrator's init-token mint.
3. **Testing mode (D2):** the OAuth consent screen stays in Testing for the operator-tenant test
   (7-day refresh-token expiry + 100-user cap accepted). Google app verification is deferred to
   Beta (operator task O4).

---

## 5. init-token contract — the integrator mints (proposed FROZEN_CONTRACTS §E0)

The public `/connect` + `/connection/status` endpoints read the coordinator identity ONLY from a
signed `init` token (never the query) — so an anonymous caller cannot point a consent flow at
someone else's calendar slot. The Clerk-authed dashboard backend (Analytics_Dashboard_API, which
already verifies Clerk JWTs) mints it AFTER authenticating the staff member:

```js
// in the Clerk-authed backend, using the SHARED _state-signing-key:
const initToken = await state.sign({
  typ: 'init',
  claims: {
    tenant_id,           // from the verified Clerk org
    coordinator_id,      // from the verified Clerk identity — NEVER client-supplied
    coordinator_email,   // from the verified Clerk identity
  },
  ttlSeconds: 300,
});
// then navigate the browser to: `${OAUTH_FUNCTION_URL}/connect?init=${initToken}`
//   (and for status polling: `${OAUTH_FUNCTION_URL}/connection/status?init=${initToken}`)
```

`state.sign`/`verify` (this PR's `state.js`) is the shared signer/verifier. The two systems share
only the `_state-signing-key` secret. `coordinator_id` here is the v1 calendar id = the lower-cased
registry email (matches `candidate-resolver` `resourceId === coordinatorEmail`).

**⚠️ REQUIRED-before-Beta (audit 2026-06-05, Security B2):** the init-token is currently NOT
single-use — it is replay-able within its TTL. Forgery is impossible (identity is signed, never
client-supplied) and the no-referrer header keeps it out of Referer, but an attacker who
*intercepts* a valid init token within the TTL could replay it to drive a consent that writes
into the victim's slot. Mitigated for the staging-operator-only pilot by: short TTL + no-referrer.
**Before Beta, make it single-use** — recommended: a conditional `PutItem` of the token `nonce`
to the EXISTING `picasso-token-jti-blacklist` table (the §B4 one-time-use pattern; `attribute_not_exists`
→ first use wins, replay → reject). Adds a DDB `PutItem` grant on that table to the role. Keep the
TTL short (≤300s) regardless. Lock the single-use requirement into FROZEN_CONTRACTS §E0.

**Operational note (`invalid_client`) — FIXED 2026-06-06 (integrator directive #3):** `revocation.js`
classifies `invalid_client` as **PLATFORM** (not a per-coordinator `PERMANENT_MARKER`), deliberately
diverging from the shipped BCH `classifyAuthError`. So a broken platform-app credential makes
`/connection/status` log `status_platform_credential_error` + report `stale_connected` (NO secret
stamp) instead of mass-revoking every polling coordinator. Wire `status_platform_credential_error`
to an operator alarm before Beta.

**Operational note (`jti_burn_unavailable` — alarm on this log):** when DDB is unavailable the
jti burn fails open — the connect proceeds but single-use enforcement is offline for that request.
A handful of isolated occurrences is noise (transient throttle / cold-start). Sustained occurrences
(>N within a short window) mean replay protection is offline for ALL coordinators and require
immediate investigation. Alarm on `jti_burn_unavailable` in CloudWatch Logs Insights before Beta.

**⚠️ Beta-gated PII (track; NOT a staging blocker):** the signed `state`/`init` token's base64 payload
carries `coordinator_email`/`coordinator_id`, and `state` rides in the `/oauth/callback?state=` query
string → **CloudFront access logs would capture the email**. Before Beta either disable query-string
logging on the D3 distribution for these paths, or carry an opaque reference instead of the email in
the token payload. (In v1 `coordinator_id === coordinator_email`, so dropping `coordinator_email`
alone does not remove the PII — the id is the email.)

---

## 6. Deploy

```bash
cd Calendar_OAuth_Connect
npm ci
npm test
npm run package           # → deployment.zip (dist/index.js)
aws lambda update-function-code --function-name Calendar_OAuth_Connect-staging \
  --zip-file fileb://deployment.zip --profile myrecruiter-staging
```

(Or via the integrator's Terraform Lambda module, mirroring
`infra/modules/lambda-scheduling-redemption-handler-staging/`.)

---

## 7. pii-inventory snippet (Living-Inventory Rule — integrator + PII session apply)

Refresh tokens are a new PII/credential surface. Per CLAUDE.md this MUST update
`docs/roadmap/PII-Project/pii-inventory.md` (picasso repo) in coordination with the PII session.
**Do not let me (worker) edit that shared file unilaterally** — snippet for them to apply:

**§A. Lambda processing surfaces** — add row:

```
| `Calendar_OAuth_Connect` | Node.js 20 | The per-coordinator Google OAuth consent flow (WS-E-OAUTH/E11). Handles transiently in memory: the authorization `code`, the minted `refresh_token`, and `coordinator_email` (from the signed init/state token) — **none logged** (logs carry tenant_id + a SHA-256 coordinator_id hash prefix only, §5.7). WRITES the refresh_token + coordinator_email to the per-coordinator OAuth secret (the at-rest sink below). Init/state are HMAC-signed; identity is never client-supplied (slot-poisoning impossible). | Secrets Manager `picasso/scheduling/oauth/{tenant}/{coordinator}` (CreateSecret/PutSecretValue — the at-rest refresh-token sink); invokes `Calendar_Watch_Onboarder` (B5). Reads the platform app secret + state-signing key + tenant config (Flag-A gate). | New WRITER of the per-coordinator OAuth secret (previously hand-provisioned per the sub-phase B runbook; read by Listener/Onboarder/Renewer/Offboarder/Remediator/Booking_Commit_Handler + availability.js). Coordinator (operator-tier) PII — same volunteer-coordinator G-H counsel caveat as the calendar-watch rows. Dedicated role `Calendar_OAuth_Connect-exec-staging` (§2 above); per-coordinator secret writes guarded to non-`_`-prefixed paths. |
```

**Scheduling-Lambdas note** (extend the existing line 69 follow-up): the per-coordinator OAuth
secret `picasso/scheduling/oauth/{tenant}/{coordinator}` — already READ by 6 scheduling
Lambdas + `availability.js` — now gets its **first programmatic writer** (this Lambda). Its at-rest
shape (back-compat, ratified 2026-06-05): `{ provider, client_id, client_secret, refresh_token,
coordinator_email, scopes, token_endpoint, calendar_id, connected_at, status }`. Consider a
dedicated Secrets-Manager subsection row for it when the PII-secret inventory is next refreshed
(the highest-sensitivity field is the `refresh_token` — a Google calendar-write credential).
