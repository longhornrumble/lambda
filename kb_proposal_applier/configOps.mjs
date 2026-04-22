/**
 * In-memory config mutations for `config.*` verbs.
 *
 * Paths are dotted strings resolving to fields on the tenant config object:
 *   "content_showcase"              → config.content_showcase (ARRAY)
 *   "action_chips.default_chips"    → config.action_chips.default_chips (DICT keyed by slug)
 *   "retired_showcase_ids"          → config.retired_showcase_ids (ARRAY)
 *
 * Production tenant configs use dicts for some collections (per CLAUDE.md "v1.4.1 dictionary
 * format") and arrays for others. The Applier detects the existing shape at runtime:
 *   - If the target is an ARRAY, ops behave as push/filter.
 *   - If the target is a DICT, ops behave as keyed set/delete. Operations on dicts MUST
 *     supply either `op.key` explicitly OR a `value.id` / `value.showcase_id` the Applier
 *     can use as the key. That's the shape the scanner emits today.
 *
 * Every mutation is a pure function on an already-cloned config — the caller owns
 * cloning before the loop and serializing after. We never mutate in place.
 */

function getByPath(obj, path) {
  const parts = path.split('.');
  let cur = obj;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}

function setByPath(obj, path, value) {
  const parts = path.split('.');
  let cur = obj;
  for (let i = 0; i < parts.length - 1; i++) {
    const p = parts[i];
    if (cur[p] == null || typeof cur[p] !== 'object') cur[p] = {};
    cur = cur[p];
  }
  cur[parts[parts.length - 1]] = value;
}

function matchesCriteria(item, matchBy) {
  if (!matchBy || typeof matchBy !== 'object') return false;
  return Object.entries(matchBy).every(([k, v]) => item?.[k] === v);
}

function deriveDictKey(op, value) {
  if (op && op.key) return op.key;
  if (value && typeof value === 'object') {
    // Showcase items use `id`; action_chip entries paired to a showcase use `showcase_id`.
    if (value.id) return value.id;
    if (value.showcase_id) return value.showcase_id;
  }
  return null;
}

/**
 * `config.add` — insert `value` at `path`. Shape-detecting:
 *   - If `path` resolves to an array (or doesn't exist → initialize as array), push.
 *   - If `path` resolves to a dict, use `op.key` or `value.id` / `value.showcase_id` as the key.
 *
 * Deduplication is the scanner's responsibility — the Applier trusts its input.
 */
export function applyAdd(config, path, value, op = {}) {
  const current = getByPath(config, path);
  if (current == null) {
    // Default-initialize as array when the target doesn't exist — safest assumption. Ops that
    // want dict initialization should pass an explicit shape hint in a future schema revision.
    setByPath(config, path, [value]);
    return;
  }
  if (Array.isArray(current)) {
    current.push(value);
    return;
  }
  if (typeof current === 'object') {
    const key = deriveDictKey(op, value);
    if (!key) {
      throw new Error(
        `config.add on dict target ${path} requires op.key or value.id/value.showcase_id`,
      );
    }
    current[key] = value;
    return;
  }
  throw new Error(`config.add target must be array or dict: ${path} (got ${typeof current})`);
}

/**
 * `config.delete` — remove items matching `matchBy` from `path`. Shape-detecting:
 *   - If `path` resolves to an array, filter out matches.
 *   - If `path` resolves to a dict, delete the matching keyed entry. `matchBy` can match on
 *     the dict entry's value (e.g., `{showcase_id: "foo"}`) OR on the dict key directly
 *     (if `matchBy` has exactly one `{key: "..."}` field).
 *
 * Throws if no match — tells the caller the proposal was generated against stale state.
 */
export function applyDelete(config, path, matchBy) {
  const current = getByPath(config, path);

  if (Array.isArray(current)) {
    const remaining = current.filter(item => !matchesCriteria(item, matchBy));
    if (remaining.length === current.length) {
      throw new Error(`config.delete found no items matching ${JSON.stringify(matchBy)} at ${path}`);
    }
    setByPath(config, path, remaining);
    return;
  }

  if (current && typeof current === 'object') {
    // Dict delete: find keys whose value matches the criteria. Supports an explicit
    // `matchBy: {key: "slug"}` shorthand for dict-key-based deletion.
    const explicitKey = matchBy?.key && Object.keys(matchBy).length === 1 ? matchBy.key : null;
    const toDelete = explicitKey
      ? (explicitKey in current ? [explicitKey] : [])
      : Object.entries(current).filter(([, v]) => matchesCriteria(v, matchBy)).map(([k]) => k);

    if (toDelete.length === 0) {
      throw new Error(`config.delete found no entries matching ${JSON.stringify(matchBy)} at ${path}`);
    }
    for (const k of toDelete) delete current[k];
    return;
  }

  throw new Error(`config.delete target must be array or dict: ${path}`);
}

/**
 * `config.append_to_array` — append `value` to the array at `path`. If the array doesn't
 * exist, initialize it. Identical semantics to `config.add` for array targets; the verb split
 * exists so the scanner can signal intent (new entity vs. memory/audit-list append). A future
 * validator can diverge the two if needed — today they share an implementation.
 */
export function applyAppendToArray(config, path, value) {
  applyAdd(config, path, value);
}
