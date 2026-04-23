/**
 * Core proposal applier.
 *
 * Contract:
 *   - Input: {tenantId, proposalId, approvedItemIds[]}
 *   - Load the proposal from S3, filter to approved items.
 *   - Load the tenant's config (with ETag) and KB (single-file convention) ONCE at the top.
 *   - For each operation, mutate the in-memory config or KB.
 *   - At the end of each item's ops, persist KB (if dirty) and config (if dirty, with ETag).
 *   - Dub calls happen inline — no batching, one upsert per dub.upsert operation.
 *   - Per-item `applicationResult` captures success/failure. Proposal-level status:
 *     `applied` if every op succeeded, `partial_apply_error` if any failed.
 *
 * Failure isolation: an op failure inside item-A does NOT abort item-B. We record the error,
 * move on, and surface everything in the audit trail. No rollback — per the plan, the Applier
 * doesn't compensate, it reports.
 *
 * One subtlety: KB and config are shared across items. If item-A's kb.append succeeds and
 * item-B's kb.replace fails, item-A's change is already persisted (config + KB are flushed
 * after each item, not at the very end). This matches the "no rollback" contract and lets
 * per-item retry from the UI work correctly.
 */

import {
  loadProposal,
  writeAppliedAudit,
  loadConfigWithETag,
  saveConfig,
  ConfigETagMismatchError,
  discoverKbKey,
  readKb,
  writeKb,
} from './s3Ops.mjs';

import {
  appendAfterMarker,
  replaceBySourceMarker,
  removeBySourceMarker,
} from './kbOps.mjs';

import {
  applyAdd,
  applyDelete,
  applyAppendToArray,
} from './configOps.mjs';

import {
  buildPayload,
  dubUpsert,
  categorizeUrl,
  slugify,
} from './lib/dub.mjs';

import { triggerBedrockSync } from './bedrockSync.mjs';

function deepClone(obj) {
  return JSON.parse(JSON.stringify(obj));
}

/**
 * Apply all ops for a single proposal item with per-item atomicity.
 *
 * Snapshots state before the first op. If any op throws, reverts to the snapshot and marks
 * the item failed. Subsequent ops in the same item are NOT attempted after a failure — the
 * item is atomic. Earlier items that already succeeded keep their mutations (no cross-item
 * rollback).
 *
 * Intentionally a pure transform on caller-owned state: no S3, no HTTP, no env access.
 * `applyOp` is injected so tests can use synthetic op handlers to force specific failures.
 */
export async function applyItemAtomically(item, state, ctx, applyOp) {
  const snapshot = {
    kb: state.kb,
    kbDirty: state.kbDirty,
    config: deepClone(state.config),
    configDirty: state.configDirty,
  };

  const opResults = [];
  let itemFailed = false;

  for (let i = 0; i < (item.operations || []).length; i++) {
    const op = item.operations[i];
    try {
      const result = await applyOp(op, state, ctx);
      opResults.push({
        index: i,
        verb: op.verb,
        status: 'applied',
        ...(result && { result }),
      });
    } catch (error) {
      opResults.push({
        index: i,
        verb: op.verb,
        status: 'error',
        error: error.message,
      });
      itemFailed = true;
      break;
    }
  }

  if (itemFailed) {
    state.kb = snapshot.kb;
    state.kbDirty = snapshot.kbDirty;
    state.config = snapshot.config;
    state.configDirty = snapshot.configDirty;
  }

  return { itemFailed, opResults };
}

/**
 * Persist the in-memory KB + config state back to S3 and trigger Bedrock sync.
 *
 * Returns `{ configSaveError, bedrockSync, persistFailed }`:
 *   - `configSaveError` — non-null string if config save failed (ETag mismatch or other)
 *   - `bedrockSync` — the triggerBedrockSync result object, or null if KB not dirty
 *   - `persistFailed` — true if any persist step failed (config save error triggers this)
 *
 * Dependencies are injected via `deps` so tests can substitute fakes for `writeKb`,
 * `saveConfig`, `triggerBedrockSync`, and `ConfigETagMismatchError`. Production code wires
 * the real dependencies from ./s3Ops.mjs and ./bedrockSync.mjs.
 *
 * Ordering rationale (comments in-line):
 */
export async function persistState({ tenantId, kbKey, state, deps }) {
  let configSaveError = null;
  let persistFailed = false;

  // KB first — the file mutation is what the user cares about. If it fails, a later config
  // write would point at stale markdown, so we fail fast and bubble.
  if (state.kbDirty) {
    await deps.writeKb(kbKey, state.kb);
  }

  // Config save with ETag. Mismatch means someone wrote between our GET and PUT; per the
  // plan's no-rollback contract we report the conflict, not try to merge.
  if (state.configDirty) {
    try {
      await deps.saveConfig(tenantId, state.config, state.configETag);
    } catch (error) {
      if (error instanceof deps.ConfigETagMismatchError) {
        configSaveError = `config_changed_externally (current ETag ${error.currentETag})`;
      } else {
        configSaveError = error.message;
      }
      persistFailed = true;
    }
  }

  // Bedrock sync fires after any KB write, regardless of config outcome — the KB is stale
  // in Bedrock whether or not config also updated.
  let bedrockSync = null;
  if (state.kbDirty) {
    bedrockSync = await deps.triggerBedrockSync(state.config);
  }

  return { configSaveError, bedrockSync, persistFailed };
}

/**
 * Dispatch a single operation against in-memory KB + config state.
 * Mutates `state.kb` (string) and `state.config` (object) in place. Returns the Dub result
 * or undefined — callers don't need anything else.
 */
export async function applyOperation(op, state, ctx) {
  switch (op.verb) {
    case 'kb.append': {
      if (!op.afterMarker || !op.markdown) {
        throw new Error('kb.append requires afterMarker and markdown');
      }
      const sourceLine = op.sourceMarker ? op.sourceMarker + '\n' : '';
      state.kb = appendAfterMarker(state.kb, op.afterMarker, sourceLine + op.markdown);
      state.kbDirty = true;
      return;
    }

    case 'kb.replace': {
      if (!op.sourceMarker || !op.markdown) {
        throw new Error('kb.replace requires sourceMarker and markdown');
      }
      state.kb = replaceBySourceMarker(state.kb, op.sourceMarker, op.markdown);
      state.kbDirty = true;
      return;
    }

    case 'kb.remove': {
      if (!op.sourceMarker) {
        throw new Error('kb.remove requires sourceMarker');
      }
      state.kb = removeBySourceMarker(state.kb, op.sourceMarker);
      state.kbDirty = true;
      return;
    }

    case 'config.add': {
      if (!op.path || op.value === undefined) {
        throw new Error('config.add requires path and value');
      }
      applyAdd(state.config, op.path, op.value, op);
      state.configDirty = true;
      return;
    }

    case 'config.delete': {
      if (!op.path || !op.matchBy) {
        throw new Error('config.delete requires path and matchBy');
      }
      applyDelete(state.config, op.path, op.matchBy);
      state.configDirty = true;
      return;
    }

    case 'config.append_to_array': {
      if (!op.path || op.value === undefined) {
        throw new Error('config.append_to_array requires path and value');
      }
      applyAppendToArray(state.config, op.path, op.value);
      state.configDirty = true;
      return;
    }

    case 'dub.upsert': {
      if (!op.url) throw new Error('dub.upsert requires url');
      if (!ctx.dubApiKey) throw new Error('DUB_API_KEY env var not set');

      const monitor = state.config.monitor || {};
      const tag = monitor.dubTag;
      if (!tag) throw new Error(`Tenant ${ctx.tenantId} has no monitor.dubTag — cannot create Dub link`);

      const domain = monitor.dubDomain || 'myrctr.link';
      const link = {
        url: op.url,
        label: op.label || op.slug || op.url,
        category: op.category || categorizeUrl(op.url) || 'registration',
        sections: op.section ? [op.section] : (op.sections || []),
      };
      const payload = buildPayload(
        link,
        ctx.tenantId,
        tag,
        domain,
        monitor.dubFolderId,
        monitor.orgName,
        monitor.dubOgImageUrl,
      );
      if (op.externalId) payload.externalId = op.externalId;
      if (op.slug) payload.key = op.slug;

      return await dubUpsert(ctx.dubApiKey, payload);
    }

    default:
      throw new Error(`Unknown verb: ${op.verb}`);
  }
}

/**
 * Apply a proposal's approved items and return the updated proposal envelope.
 *
 * Throws on unrecoverable errors (proposal missing, tenant config missing, KB missing).
 * Per-item/per-op errors are captured in `applicationResult` and do NOT throw.
 */
export async function applyProposal({ tenantId, proposalId, approvedItemIds, dubApiKey }) {
  const proposal = await loadProposal(tenantId, proposalId);

  if (proposal.tenantId !== tenantId) {
    throw new Error(`Proposal ${proposalId} is for tenant ${proposal.tenantId}, not ${tenantId}`);
  }
  if (!Array.isArray(proposal.items)) {
    throw new Error(`Proposal has no items array`);
  }

  // Filter to approved items. If approvedItemIds is missing or empty, apply none (explicit opt-in).
  const approvedSet = new Set(approvedItemIds || []);
  const itemsToApply = proposal.items.filter(item => approvedSet.has(item.id));

  if (itemsToApply.length === 0) {
    return {
      ...proposal,
      status: 'applied',
      applicationResult: {
        appliedAt: new Date().toISOString(),
        appliedCount: 0,
        failedCount: 0,
        itemResults: [],
        note: 'No approved items — nothing to apply.',
      },
    };
  }

  // Load state once at the top. KB discovery + config fetch both must succeed or we abort —
  // these aren't per-item failures, they're environment problems.
  const kbKey = await discoverKbKey(tenantId);
  const { config: loadedConfig, etag: loadedETag } = await loadConfigWithETag(tenantId);

  const state = {
    kb: await readKb(kbKey),
    kbDirty: false,
    config: deepClone(loadedConfig),
    configDirty: false,
    configETag: loadedETag,
  };

  const ctx = { tenantId, proposalId, dubApiKey };

  const itemResults = [];
  let anyFailed = false;

  for (const item of itemsToApply) {
    const { itemFailed, opResults } = await applyItemAtomically(item, state, ctx, applyOperation);
    if (itemFailed) anyFailed = true;
    itemResults.push({
      itemId: item.id,
      status: itemFailed ? 'error' : 'applied',
      operations: opResults,
    });
  }

  const { configSaveError, bedrockSync, persistFailed } = await persistState({
    tenantId,
    kbKey,
    state,
    deps: { writeKb, saveConfig, triggerBedrockSync, ConfigETagMismatchError },
  });
  if (persistFailed) anyFailed = true;

  const appliedProposal = {
    ...proposal,
    status: anyFailed ? 'partial_apply_error' : 'applied',
    applicationResult: {
      appliedAt: new Date().toISOString(),
      appliedCount: itemResults.filter(r => r.status === 'applied').length,
      failedCount: itemResults.filter(r => r.status === 'error').length,
      itemResults,
      ...(configSaveError && { configSaveError }),
      kbKey,
      kbWritten: state.kbDirty && !configSaveError ? true : state.kbDirty,
      configWritten: state.configDirty && !configSaveError,
      ...(bedrockSync && { bedrockSync }),
    },
  };

  // Write the audit trail alongside the original proposal.
  const auditKey = await writeAppliedAudit(tenantId, proposalId, {
    original: proposal,
    applied: appliedProposal,
    kbKey,
  });
  appliedProposal.applicationResult.auditKey = auditKey;

  return appliedProposal;
}
