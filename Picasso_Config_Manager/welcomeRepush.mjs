/**
 * welcomeRepush — server-side backstop for pushing Messenger welcome surfaces.
 *
 * After Config Manager writes a tenant config that includes
 * messenger_behavior.welcome (ice breakers / persistent menu), it fire-and-forget
 * invokes Meta_OAuth_Handler's repush-welcome route so the live FB/IG profile
 * syncs even if the Config Builder browser tab closed before its own post-deploy
 * call. Both triggers are idempotent (the Messenger Profile API is a PUT).
 *
 * Best-effort: never throws into the write path, and no-ops when
 * META_OAUTH_FUNCTION is unset (feature inert until the IaC grant lands).
 */

import { LambdaClient, InvokeCommand } from '@aws-sdk/client-lambda';

const REGION = process.env.AWS_REGION || 'us-east-1';

let _lambda = null;
function defaultClient() {
  if (!_lambda) _lambda = new LambdaClient({ region: REGION });
  return _lambda;
}

/** True when the config carries Messenger welcome surfaces worth pushing. */
export function shouldRepushWelcome(config) {
  const welcome = config?.messenger_behavior?.welcome;
  return Boolean(welcome?.ice_breakers?.length || welcome?.persistent_menu?.length);
}

/**
 * Async-invoke (InvocationType 'Event') Meta_OAuth_Handler's repush-welcome
 * route for a tenant. Never throws — returns a small status object.
 * `deps.client` is injectable for tests.
 */
export async function maybeRepushWelcomeSurfaces(tenantId, config, deps = {}) {
  const fn = process.env.META_OAUTH_FUNCTION;
  if (!fn) return { skipped: 'META_OAUTH_FUNCTION unset' };
  if (!shouldRepushWelcome(config)) return { skipped: 'no welcome surfaces' };

  const client = deps.client || defaultClient();
  const event = {
    httpMethod: 'POST',
    path: `/meta/channels/${encodeURIComponent(tenantId)}/repush-welcome`,
    body: null,
  };
  try {
    await client.send(
      new InvokeCommand({
        FunctionName: fn,
        InvocationType: 'Event', // fire-and-forget; we don't wait on the push result
        Payload: Buffer.from(JSON.stringify(event)),
      })
    );
    return { invoked: true };
  } catch (err) {
    // Best-effort: a failed backstop must never fail the config write.
    console.warn(`[welcome-repush] invoke failed for tenant_id=${tenantId}: ${err.message}`);
    return { error: err.message };
  }
}
