'use strict';

/**
 * M7a — Conversational form engine (docs/messenger/CONTRACTS.md C3/C4/C7/C9).
 *
 * Server-side form sessions on the C4 `form_session` row: field prompts, QR
 * enums (C3 `ffld`/`fctl` payloads), the FB-only `user_email` prefill QR (C5),
 * validation/re-prompt, a this-session-only summary + confirm, and submission
 * via the MFS live-lane-shaped invoke (fact 13 / contract S1).
 *
 * G-P3 binding conditions implemented here (see plan §6 M7a + the G-P3 gate
 * verdict this subphase was scoped against):
 *   T1 — expires_at is epoch SECONDS everywhere (matches C4).
 *   T2 — a row whose expires_at has passed is treated as ABSENT at read time
 *        (DDB TTL sweep lags); loadFormSession filters it and best-effort
 *        deletes it (mirrors escalation.js's deleteExpiredPauseRow pattern).
 *   T3 — a FAILED submission keeps the row only until its EXISTING
 *        expires_at — confirmForm never re-saves/extends TTL on failure.
 *   S1 — submission is an IAM `Invoke` (RequestResponse) of the MFS_FUNCTION
 *        env, using the EXACT payload shape the widget's HTTP fallback lane
 *        sends today (HTTPChatProvider.jsx `submitFormToLambda` ->
 *        `?action=chat` body `{form_mode:true, action:'submit_form',
 *        form_id, form_data, session_id, conversation_id}` -> MFS
 *        lambda_function.handle_chat's form_mode branch -> FormHandler).
 *        tenant_id/tenant_hash come from the caller's own config resolution
 *        (index.js's tenantHash), never from message content. The invoke
 *        also carries the `x-picasso-cf-origin` header (getCfOriginSecret)
 *        so it passes MFS's own REQUIRE_CF_ORIGIN_HEADER validator — MFS's
 *        fail-closed gate stays meaningful (only secret-holders or
 *        CloudFront pass); a fetch failure/empty secret omits the header and
 *        lets MFS's 403 flow into the existing submission-failure path (T3).
 *   D1/X3 — answer VALUES never appear in any log line here (field names,
 *        counts, and status enums only — every log call below is audited
 *        against this).
 *   D2 — buildSummary echoes ONLY this session's own answers map.
 *   E1/E2 — the FB-only `user_email` QR is rendered for email fields; a tap
 *        arrives to the webhook/processor as ordinary free text (Meta omits
 *        `quick_reply.payload` on a user_email QR, so classify.js's
 *        `msg.quick_reply?.payload` check falls through to the plain 'text'
 *        eventKind) — it therefore runs through the IDENTICAL validateAnswer
 *        path as a typed email, and an UNTAPPED QR is never read/logged/
 *        stored (there is nothing to read until the user acts).
 *
 * PII note (C4 "answers" attribute is a PII surface): this module never logs
 * `session.answers`, `rawText`, or any resolved field value — only field
 * keys, form ids, attempt counts, and status enums.
 */

const { GetCommand, PutCommand, DeleteCommand } = require('@aws-sdk/lib-dynamodb');
const { LambdaClient, InvokeCommand } = require('@aws-sdk/client-lambda');
const { SecretsManagerClient, GetSecretValueCommand } = require('@aws-sdk/client-secrets-manager');
const crypto = require('crypto');
const { QUICK_REPLY_MAX } = require('./capabilities');
const { truncateTitle } = require('./renderMessengerActions');

const STATE_TYPE_FORM_SESSION = 'form_session';
/** G-P3: idle TTL = 1 hour from last update, refreshed on each answer. */
const FORM_SESSION_TTL_SECONDS = 60 * 60;
/** After this many consecutive invalid attempts on one field, nudge toward cancel. */
const MAX_INVALID_ATTEMPTS = 3;
/** Sentinel `current_field` value meaning "all fields answered — awaiting confirm/cancel". */
const SUMMARY_STAGE = '__summary__';

const DEFAULT_FORM_SUMMARY_INTRO = "Here's what you told me:";
const DEFAULT_FORM_SUBMITTED = 'Thanks — your submission has been received!';
const DEFAULT_FORM_SUBMISSION_ERROR =
  "Sorry — something went wrong on our end and your submission didn't go through. " +
  'You can try again, or type "cancel" to stop.';
const DEFAULT_FORM_CANCELLED =
  "No problem — I've cancelled that. Let me know if you'd like to start again.";
const DEFAULT_FIELD_REQUIRED = 'This field is required. Could you provide a value?';

// ─── C2 string precedence (small local copy — avoids a circular require with
// index.js, which also defines this; keep in sync if C2 precedence changes) ──
function getMessengerString(config, channelType, key, fallback) {
  const behavior = config?.messenger_behavior || {};
  const channelOverride = behavior.channel_overrides?.[channelType]?.strings?.[key];
  if (channelOverride !== undefined) return channelOverride;
  const topLevel = behavior.strings?.[key];
  if (topLevel !== undefined) return topLevel;
  return fallback;
}

function nowSec() {
  return Math.floor(Date.now() / 1000);
}

function refreshedExpiry(nowMs) {
  return Math.floor(nowMs / 1000) + FORM_SESSION_TTL_SECONDS;
}

// ─── C4 row CRUD ──────────────────────────────────────────────────────────────

/**
 * Load the active form_session row for a conversation. T2: a row whose
 * expires_at has already passed is treated as ABSENT (DynamoDB's own TTL
 * sweep can lag well behind the logical expiry) — this function filters it
 * out and best-effort deletes it, mirroring escalation.js's
 * `deleteExpiredPauseRow` opportunistic-cleanup pattern. The delete is
 * conditioned on the SAME expires_at check so a session refreshed between
 * this read and the delete (vanishingly unlikely — single-writer per C7 lock
 * — but cheap to guard) is never removed.
 *
 * @returns {Promise<object|null>}
 */
async function loadFormSession({ client, tableName, sessionId, log }) {
  const result = await client.send(
    new GetCommand({
      TableName: tableName,
      Key: { sessionId, stateType: STATE_TYPE_FORM_SESSION },
    })
  );
  if (!result.Item) return null;

  const cutoff = nowSec();
  if (typeof result.Item.expires_at === 'number' && result.Item.expires_at <= cutoff) {
    try {
      await client.send(
        new DeleteCommand({
          TableName: tableName,
          Key: { sessionId, stateType: STATE_TYPE_FORM_SESSION },
          ConditionExpression: 'expires_at <= :cutoff',
          ExpressionAttributeValues: { ':cutoff': cutoff },
        })
      );
    } catch (cleanupErr) {
      if (cleanupErr.name !== 'ConditionalCheckFailedException') {
        log && log('WARN', 'Stale form-session cleanup failed (non-fatal — TTL sweep will catch it)', {
          sessionId,
          error: cleanupErr.message,
        });
      }
    }
    return null; // T2: expired ⇒ absent, regardless of cleanup outcome
  }
  return result.Item;
}

async function saveFormSession({ client, tableName, session }) {
  await client.send(new PutCommand({ TableName: tableName, Item: session }));
}

async function deleteFormSession({ client, tableName, sessionId }) {
  await client.send(
    new DeleteCommand({ TableName: tableName, Key: { sessionId, stateType: STATE_TYPE_FORM_SESSION } })
  );
}

/** Bump updated_at/expires_at only — used for "activity but not an answer" turns. */
function touchSession(session) {
  const now = Date.now();
  return { ...session, updated_at: now, expires_at: refreshedExpiry(now) };
}

// ─── Field flattening (composite name/address subfields → sequential steps) ──

/**
 * Turn a ConversationalForm's `fields[]` (config-builder `config.ts` shape)
 * into a flat, ordered list of collection steps. A composite field
 * (`subfields` present — `name`/`address` types) expands into one step per
 * subfield; the answer is written back under `answers[field.id][subfield.id]`
 * (the nested-object shape MFS's `build_labeled_form_data` already handles for
 * composite fields — form_handler.py:182-197).
 *
 * @param {object} form — a `config.conversational_forms[formId]` entry
 * @returns {Array<object>} steps, each `{ key, parentId, subId, type, label, prompt, required, options, validation }`
 */
function flattenSteps(form) {
  const steps = [];
  for (const field of form?.fields || []) {
    if (Array.isArray(field.subfields) && field.subfields.length > 0) {
      for (const sub of field.subfields) {
        steps.push({
          key: `${field.id}.${sub.id}`,
          parentId: field.id,
          subId: sub.id,
          type: sub.type === 'select' ? 'select' : sub.type || 'text',
          label: sub.label,
          prompt: sub.placeholder || sub.label,
          required: sub.required !== false,
          options: sub.options,
          validation: sub.validation,
        });
      }
    } else {
      steps.push({
        key: field.id,
        parentId: null,
        subId: null,
        type: field.type,
        label: field.label,
        prompt: field.prompt || field.label,
        required: field.required !== false,
        options: field.options,
        validation: field.validation,
      });
    }
  }
  return steps;
}

function findOptionMatch(options, trimmedLower) {
  return (options || []).find(
    (o) => String(o.value).toLowerCase() === trimmedLower || String(o.label).toLowerCase() === trimmedLower
  );
}

function findOptionLabel(step, value) {
  const match = (step.options || []).find((o) => String(o.value) === String(value));
  return match ? match.label || match.value : null;
}

// ─── Validation (mirrors the widget's client-side rules — FormModeContext.jsx
// — so server-side re-validation of a Messenger answer matches what a widget
// user would have seen) ────────────────────────────────────────────────────

const EMAIL_REGEX = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
const PHONE_CHARS_REGEX = /^[\d\s\-()+]+$/;

/**
 * @param {object} step — a flattened step (see flattenSteps)
 * @param {string} rawText — trimmed on entry; may be a typed answer or a
 *   resolved `ffld` option value (validated the same way either way)
 * @returns {{valid: boolean, value?: string, error?: string}}
 */
function validateAnswer(step, rawText) {
  const trimmed = typeof rawText === 'string' ? rawText.trim() : '';

  if (!trimmed) {
    if (step.required === false) return { valid: true, value: '' };
    return { valid: false, error: DEFAULT_FIELD_REQUIRED };
  }

  // Tenant-configured custom pattern (FormFieldValidation) wins over the
  // built-in type check for every type except `select` (option-match always
  // governs an enum, a custom pattern there would be ambiguous/unreachable).
  if (step.type !== 'select' && step.validation?.pattern) {
    let re = null;
    try {
      re = new RegExp(step.validation.pattern);
    } catch (_regexErr) {
      re = null; // malformed tenant-authored pattern — fail open to no custom check
    }
    if (re && !re.test(trimmed)) {
      return { valid: false, error: step.validation.message || "That doesn't look right — could you try again?" };
    }
    if (re) return { valid: true, value: trimmed };
  }

  switch (step.type) {
    case 'email':
      if (!EMAIL_REGEX.test(trimmed)) {
        return { valid: false, error: "That doesn't look like a valid email — could you try again? (e.g., name@example.com)" };
      }
      return { valid: true, value: trimmed.toLowerCase() };

    case 'phone': {
      const digitsOnly = trimmed.replace(/\D/g, '');
      if (!PHONE_CHARS_REGEX.test(trimmed) || digitsOnly.length < 10) {
        return { valid: false, error: "That doesn't look like a valid phone number — please include at least 10 digits." };
      }
      return { valid: true, value: trimmed };
    }

    case 'select': {
      const match = findOptionMatch(step.options, trimmed.toLowerCase());
      if (!match) {
        const choices = (step.options || []).map((o) => o.label || o.value).join(', ');
        return { valid: false, error: choices ? `Please choose one of: ${choices}` : 'Please choose a valid option.' };
      }
      return { valid: true, value: match.value };
    }

    case 'number':
      if (Number.isNaN(Number(trimmed))) {
        return { valid: false, error: 'Please enter a number.' };
      }
      return { valid: true, value: trimmed };

    case 'date':
      if (Number.isNaN(Date.parse(trimmed))) {
        return { valid: false, error: 'Please enter a valid date.' };
      }
      return { valid: true, value: trimmed };

    default: // text, textarea, name-subfield defaults, etc.
      return { valid: true, value: trimmed };
  }
}

// ─── Message building (prompts, summary) ─────────────────────────────────────

/**
 * Build the send payload for one field's prompt. Enum (`select`) options
 * become `PIC1:ffld:{formId}:{stepKey}:{optionValue}` quick replies (C3);
 * an `email`-type step on the FB Messenger channel (never Instagram — C5)
 * gets the platform `user_email` prefill quick reply prepended.
 *
 * @param {object} step
 * @param {string} formId
 * @param {object} config
 * @param {string} channelType
 * @param {string} [prefixText] — prepended (e.g. a validation error) before the prompt
 * @returns {{text: string, quickReplies: Array<object>}}
 */
function fieldPromptMessage(step, formId, config, channelType, prefixText) {
  const quickReplies = [];

  if (step.type === 'select' && Array.isArray(step.options)) {
    for (const opt of step.options) {
      quickReplies.push({
        content_type: 'text',
        title: truncateTitle(opt.label || opt.value),
        payload: `PIC1:ffld:${formId}:${step.key}:${opt.value}`,
      });
    }
  }

  // C5: user_email quick reply is FB Messenger only — Instagram DM has no
  // equivalent capability. E2: this is rendered blind (we never read the
  // value until the user actually taps or types it).
  if (step.type === 'email' && channelType === 'messenger') {
    quickReplies.unshift({ content_type: 'user_email' });
  }

  if (quickReplies.length > QUICK_REPLY_MAX) quickReplies.length = QUICK_REPLY_MAX;

  const basePrompt = step.prompt || step.label || 'Please provide a value:';
  const text = prefixText ? `${prefixText}\n\n${basePrompt}` : basePrompt;
  return { text, quickReplies };
}

function confirmCancelQuickReplies(formId) {
  return [
    { content_type: 'text', title: 'Confirm', payload: `PIC1:fctl:${formId}:confirm` },
    { content_type: 'text', title: 'Cancel', payload: `PIC1:fctl:${formId}:cancel` },
  ];
}

/**
 * D2: echoes ONLY this session's own answers (never anything from a prior
 * session, another tenant, or the wider conversation history).
 *
 * @param {object} session
 * @param {object} form
 * @param {object} config
 * @param {string} channelType
 * @returns {{text: string, quickReplies: Array<object>}}
 */
function buildSummary(session, form, config, channelType) {
  const steps = flattenSteps(form);
  const intro = getMessengerString(config, channelType, 'form_summary_intro', DEFAULT_FORM_SUMMARY_INTRO);

  const lines = steps
    .map((step) => {
      const raw = step.parentId ? (session.answers?.[step.parentId] || {})[step.subId] : session.answers?.[step.key];
      if (raw === undefined || raw === null || raw === '') return null;
      const display = step.type === 'select' ? findOptionLabel(step, raw) || raw : raw;
      return `${step.label}: ${display}`;
    })
    .filter(Boolean);

  const text = [intro, ...lines, '', 'Reply "confirm" to submit or "cancel" to stop.'].join('\n');
  return { text, quickReplies: confirmCancelQuickReplies(session.form_id) };
}

// ─── Keyword detection (C9 free-text fallback — every tap has a typed equivalent) ──

/** Exact word, case-insensitive — never a substring match (an option value of
 * "cancellation" or similar must not trip this). */
function isCancelKeyword(text) {
  return typeof text === 'string' && /^cancel$/i.test(text.trim());
}

function isConfirmKeyword(text) {
  return typeof text === 'string' && /^(confirm|yes)$/i.test(text.trim());
}

// ─── Payload parsing (C3 ffld/fctl routes) ────────────────────────────────────

/**
 * Parse a `PIC1:ffld:{formId}:{fieldKey}:{optionValue}` payload.
 * @returns {{formId: string, fieldKey: string, value: string}|null}
 */
function parseFfldPayload(payload) {
  if (typeof payload !== 'string' || !payload.startsWith('PIC1:ffld:')) return null;
  const parts = payload.slice('PIC1:ffld:'.length).split(':');
  if (parts.length < 3) return null;
  const [formId, fieldKey, ...valueParts] = parts;
  const value = valueParts.join(':');
  if (!formId || !fieldKey || !value) return null;
  return { formId, fieldKey, value };
}

/**
 * Parse a `PIC1:fctl:{formId}:{op}` payload (`op` ∈ confirm|cancel).
 * @returns {{formId: string, op: 'confirm'|'cancel'}|null}
 */
function parseFctlPayload(payload) {
  if (typeof payload !== 'string' || !payload.startsWith('PIC1:fctl:')) return null;
  const parts = payload.slice('PIC1:fctl:'.length).split(':');
  if (parts.length !== 2) return null;
  const [formId, op] = parts;
  if (!formId || (op !== 'confirm' && op !== 'cancel')) return null;
  return { formId, op };
}

// ─── Form lifecycle: begin / answer ───────────────────────────────────────────

/**
 * Start a new form session (C3 `start_form` CTA resolution, or a re-trigger).
 * Overwrites any existing session row for this conversation — C4 documents
 * exactly ONE active form per conversation.
 *
 * @param {{sessionId: string, formId: string, form: object, config: object, channelType: string}} params
 * @returns {{session: object, message: {text: string, quickReplies: Array<object>}}}
 */
function beginForm({ sessionId, formId, form, config, channelType }) {
  const steps = flattenSteps(form);
  const firstStep = steps[0] || null;
  const now = Date.now();

  const session = {
    sessionId,
    stateType: STATE_TYPE_FORM_SESSION,
    form_id: formId,
    current_field: firstStep ? firstStep.key : SUMMARY_STAGE,
    answers: {},
    attempts: 0,
    started_at: now,
    updated_at: now,
    schema_version: 1,
    expires_at: refreshedExpiry(now),
  };

  const message = firstStep
    ? fieldPromptMessage(firstStep, formId, config, channelType)
    : buildSummary(session, form, config, channelType);

  return { session, message };
}

/**
 * Validate + record one answer for the session's CURRENT field, advancing to
 * the next step (or to the summary). Invalid input re-prompts the SAME field
 * with the validation error; after MAX_INVALID_ATTEMPTS consecutive misses,
 * a gentler nudge-toward-cancel replaces the raw error (attempts then reset,
 * so the cycle can repeat rather than permanently changing behavior).
 *
 * D1/X3: only fieldKey/status/attempts are ever logged by CALLERS of this
 * function — this function itself does not log at all (pure).
 *
 * @param {{session: object, form: object, config: object, channelType: string, rawText: string}} params
 * @returns {{session: object, message: {text: string, quickReplies: Array<object>}, status: 'invalid'|'next_field'|'summary'}}
 */
function handleAnswer({ session, form, config, channelType, rawText }) {
  const steps = flattenSteps(form);
  const idx = steps.findIndex((s) => s.key === session.current_field);
  const step = steps[idx];
  const now = Date.now();

  if (!step) {
    // Defensive: current_field points nowhere resolvable (e.g. SUMMARY_STAGE
    // reached this function by mistake, or a config edit removed the field
    // mid-session) — re-show the summary rather than throwing.
    const touched = touchSession(session);
    return { session: touched, message: buildSummary(touched, form, config, channelType), status: 'summary' };
  }

  const { valid, value, error } = validateAnswer(step, rawText);

  if (!valid) {
    const attempts = (session.attempts || 0) + 1;
    if (attempts >= MAX_INVALID_ATTEMPTS) {
      const gentle = `${error} Having trouble? Type "cancel" to stop, or try again.`;
      return {
        session: { ...session, attempts: 0, updated_at: now, expires_at: refreshedExpiry(now) },
        message: fieldPromptMessage(step, form.form_id || session.form_id, config, channelType, gentle),
        status: 'invalid',
      };
    }
    return {
      session: { ...session, attempts, updated_at: now, expires_at: refreshedExpiry(now) },
      message: fieldPromptMessage(step, form.form_id || session.form_id, config, channelType, error),
      status: 'invalid',
    };
  }

  const answers = { ...session.answers };
  if (step.parentId) {
    answers[step.parentId] = { ...(answers[step.parentId] || {}), [step.subId]: value };
  } else {
    answers[step.key] = value;
  }

  const nextStep = steps[idx + 1] || null;
  const nextSession = {
    ...session,
    answers,
    attempts: 0,
    current_field: nextStep ? nextStep.key : SUMMARY_STAGE,
    updated_at: now,
    expires_at: refreshedExpiry(now),
  };

  if (nextStep) {
    return {
      session: nextSession,
      message: fieldPromptMessage(nextStep, form.form_id || session.form_id, config, channelType),
      status: 'next_field',
    };
  }
  return { session: nextSession, message: buildSummary(nextSession, form, config, channelType), status: 'summary' };
}

// ─── Submission (S1/S2/T3) ────────────────────────────────────────────────────

const CF_ORIGIN_HEADER_NAME = 'x-picasso-cf-origin';
/** Success cache TTL — mirrors Meta_Webhook_Handler's getAppSecret pattern. */
const CF_ORIGIN_SECRET_TTL_MS = 5 * 60 * 1000;

const secretsManagerClient = new SecretsManagerClient({});
let _cfOriginSecretCache = null;
let _cfOriginSecretFetchedAt = 0;

/** Test-only: clear the module-scope secret cache between test cases. */
function resetCfOriginSecretCacheForTests() {
  _cfOriginSecretCache = null;
  _cfOriginSecretFetchedAt = 0;
}

/**
 * Fetch the CF-origin secret MFS's `REQUIRE_CF_ORIGIN_HEADER` validator
 * checks (Master_Function_Staging/lambda_function.py:137-169), so the S1
 * direct IAM invoke can carry `x-picasso-cf-origin` and pass MFS's
 * fail-closed gate — the gate itself stays meaningful (only secret-holders
 * or CloudFront pass) rather than being carved out for this Lambda.
 *
 * `MFS_CF_ORIGIN_SECRET_NAME` unset ⇒ feature disabled: no Secrets Manager
 * call at all, no header (matches MFS's own REQUIRE_CF_ORIGIN_HEADER=false
 * no-op default).
 *
 * Cached 5 minutes on SUCCESS (mirrors Meta_Webhook_Handler's `getAppSecret`).
 * NEVER throws: a fetch failure or an empty/unusable secret logs WARN (the
 * secret NAME only — never its value) and resolves to `null` — the caller
 * proceeds WITHOUT the header, and MFS's own fail-closed validator is the
 * backstop (403s the invoke; the existing submission-failure path — T3, no
 * row touch on failure — handles it exactly like any other MFS rejection).
 *
 * @returns {Promise<string|null>}
 */
async function getCfOriginSecret({ log } = {}) {
  const secretName = process.env.MFS_CF_ORIGIN_SECRET_NAME || '';
  if (!secretName) return null;

  const now = Date.now();
  if (_cfOriginSecretCache && now - _cfOriginSecretFetchedAt < CF_ORIGIN_SECRET_TTL_MS) {
    return _cfOriginSecretCache;
  }

  try {
    const result = await secretsManagerClient.send(new GetSecretValueCommand({ SecretId: secretName }));
    const raw = result.SecretString || '';
    // Console-created secrets store JSON {"secret":"..."} or {"value":"..."};
    // plaintext secrets are the raw string (mirrors BSH's
    // cf-origin-validator.js getCfOriginSecret envelope handling exactly).
    let candidate = raw;
    try {
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === 'object') {
        candidate = parsed.secret || parsed.value || raw;
      }
    } catch (_parseErr) {
      // not JSON — use the raw string
    }

    if (!candidate || !String(candidate).trim()) {
      log && log('WARN', 'CF origin secret is empty or unusable — proceeding without the header', { secretName });
      return null;
    }

    _cfOriginSecretCache = String(candidate);
    _cfOriginSecretFetchedAt = now;
    return _cfOriginSecretCache;
  } catch (fetchErr) {
    log && log('WARN', 'Failed to fetch CF origin secret — proceeding without the header', {
      secretName,
      error: fetchErr.message,
    });
    return null;
  }
}

/**
 * Build the exact widget-live-lane invoke event (S2's pinned contract
 * fixture). This is a plain Lambda `event` object shaped so MFS's
 * `lambda_handler` routes it through `action=chat` -> `handle_chat`'s
 * `form_mode` branch -> `handle_form_submission` -> `FormHandler`, IDENTICAL
 * to the payload `HTTPChatProvider.jsx`'s `submitFormToLambda` POSTs today
 * (Picasso/src/context/HTTPChatProvider.jsx:858-866) — the only proven,
 * already-live MFS form-submission shape (BSH's own form_handler.js does NOT
 * invoke MFS for form processing; it has its own independent, fully-ported
 * pipeline — there is no BSH->MFS proven shape to mirror instead).
 *
 * `client_submission_id` is deterministic (sha256 of sessionId+formId) so a
 * retried confirm (T3: a failed submission keeps the row for retry) reuses
 * the SAME idempotency token MFS's FS5 dedup already understands
 * (form_handler.py IDEM_TOKEN_SHAPE `^[A-Za-z0-9_-]{16,128}$`) — a second
 * confirm after a transient failure cannot double-fulfil.
 *
 * `cfOriginSecret` (optional): when present, adds the `x-picasso-cf-origin`
 * header MFS's REQUIRE_CF_ORIGIN_HEADER validator checks; absent ⇒ headers
 * unchanged from the pre-CF-origin shape (S2 fixture stays byte-identical
 * for callers that don't pass one, e.g. existing contract tests).
 *
 * @param {{tenantHash: string, formId: string, answers: object, sessionId: string, cfOriginSecret?: string|null}} params
 * @returns {object} a Lambda `event` shape suitable for direct `Invoke`
 */
function buildSubmissionEvent({ tenantHash, formId, answers, sessionId, cfOriginSecret }) {
  const clientSubmissionId = crypto.createHash('sha256').update(`${sessionId}:${formId}`).digest('hex');
  const body = {
    tenant_hash: tenantHash,
    form_mode: true,
    action: 'submit_form',
    form_id: formId,
    form_data: answers,
    session_id: sessionId,
    conversation_id: sessionId,
    client_submission_id: clientSubmissionId,
  };
  const headers = { 'Content-Type': 'application/json' };
  if (cfOriginSecret) {
    headers[CF_ORIGIN_HEADER_NAME] = cfOriginSecret;
  }
  return {
    httpMethod: 'POST',
    headers,
    queryStringParameters: { action: 'chat', t: tenantHash },
    body: JSON.stringify(body),
  };
}

/**
 * S1: direct IAM `Invoke` (RequestResponse, never a public lane) of the MFS
 * function with the S2-pinned payload. Never throws — every failure mode
 * (invoke error, function error, non-200, malformed payload) resolves to
 * `{success: false}` so the caller can log the `[FORM_SUBMISSION_LOST]`
 * marker (D3/D4 twin — mirrors MFS's own marker for its store-failure path)
 * and keep the session row for retry (T3).
 *
 * @returns {Promise<{success: boolean, submissionId?: string}>}
 */
async function invokeMfsSubmission({ lambdaClient, functionName, tenantHash, formId, answers, sessionId, log }) {
  const cfOriginSecret = await getCfOriginSecret({ log });
  const event = buildSubmissionEvent({ tenantHash, formId, answers, sessionId, cfOriginSecret });
  try {
    const result = await lambdaClient.send(
      new InvokeCommand({
        FunctionName: functionName,
        InvocationType: 'RequestResponse',
        Payload: Buffer.from(JSON.stringify(event)),
      })
    );

    if (result.FunctionError) {
      log && log('ERROR', '[FORM_SUBMISSION_LOST] MFS invoke returned a function error', {
        formId,
        functionError: result.FunctionError,
      });
      return { success: false };
    }

    const payloadStr = result.Payload ? Buffer.from(result.Payload).toString('utf-8') : '';
    let parsed;
    try {
      parsed = JSON.parse(payloadStr);
    } catch (parseErr) {
      log && log('ERROR', '[FORM_SUBMISSION_LOST] MFS invoke payload unparsable', { formId, error: parseErr.message });
      return { success: false };
    }

    if (parsed.statusCode !== 200) {
      log && log('ERROR', '[FORM_SUBMISSION_LOST] MFS submission returned non-200', {
        formId,
        statusCode: parsed.statusCode,
      });
      return { success: false };
    }

    let respBody = {};
    try {
      respBody = JSON.parse(parsed.body || '{}');
    } catch (_bodyParseErr) {
      respBody = {};
    }
    if (!respBody.success) {
      log && log('ERROR', '[FORM_SUBMISSION_LOST] MFS reported success=false', { formId });
      return { success: false };
    }
    return { success: true, submissionId: respBody.submission_id };
  } catch (invokeErr) {
    log && log('ERROR', '[FORM_SUBMISSION_LOST] MFS invoke threw', { formId, error: invokeErr.message });
    return { success: false };
  }
}

/**
 * Attempt to submit the completed form. Success ⇒ delete the session row and
 * return the config-driven success string. Failure ⇒ T3: the row is left
 * COMPLETELY UNTOUCHED (no save, no TTL extension) so it expires at its
 * existing expires_at; the user can retry confirm until then.
 *
 * @returns {Promise<{message: {text: string, quickReplies: Array<object>}, submitted: boolean}>}
 */
async function confirmForm({ session, config, channelType, tenantHash, lambdaClient, functionName, client, tableName, log }) {
  if (!functionName) {
    log && log('ERROR', '[FORM_SUBMISSION_LOST] MFS_FUNCTION not configured — cannot submit (session kept for retry)', {
      formId: session.form_id,
    });
    return {
      message: {
        text: getMessengerString(config, channelType, 'form_submission_error', DEFAULT_FORM_SUBMISSION_ERROR),
        quickReplies: confirmCancelQuickReplies(session.form_id),
      },
      submitted: false,
    };
  }

  const result = await invokeMfsSubmission({
    lambdaClient,
    functionName,
    tenantHash,
    formId: session.form_id,
    answers: session.answers,
    sessionId: session.sessionId,
    log,
  });

  if (result.success) {
    try {
      await deleteFormSession({ client, tableName, sessionId: session.sessionId });
    } catch (delErr) {
      log && log('WARN', 'Form session delete after successful submission failed (non-fatal)', {
        sessionId: session.sessionId,
        error: delErr.message,
      });
    }
    return {
      message: {
        text: getMessengerString(config, channelType, 'form_submitted', DEFAULT_FORM_SUBMITTED),
        quickReplies: [],
      },
      submitted: true,
    };
  }

  // T3: do NOT save/touch the row here — it keeps its existing expires_at.
  return {
    message: {
      text: getMessengerString(config, channelType, 'form_submission_error', DEFAULT_FORM_SUBMISSION_ERROR),
      quickReplies: confirmCancelQuickReplies(session.form_id),
    },
    submitted: false,
  };
}

module.exports = {
  // row CRUD
  loadFormSession,
  saveFormSession,
  deleteFormSession,
  touchSession,
  // lifecycle primitives (reused by M8a per the plan)
  beginForm,
  handleAnswer,
  buildSummary,
  confirmForm,
  invokeMfsSubmission,
  buildSubmissionEvent,
  getCfOriginSecret,
  resetCfOriginSecretCacheForTests,
  // parsing / keyword helpers
  parseFfldPayload,
  parseFctlPayload,
  isCancelKeyword,
  isConfirmKeyword,
  // internals exposed for unit tests
  flattenSteps,
  validateAnswer,
  fieldPromptMessage,
  confirmCancelQuickReplies,
  // constants
  STATE_TYPE_FORM_SESSION,
  FORM_SESSION_TTL_SECONDS,
  MAX_INVALID_ATTEMPTS,
  SUMMARY_STAGE,
  DEFAULT_FORM_SUMMARY_INTRO,
  DEFAULT_FORM_SUBMITTED,
  DEFAULT_FORM_SUBMISSION_ERROR,
  DEFAULT_FORM_CANCELLED,
  CF_ORIGIN_HEADER_NAME,
};
