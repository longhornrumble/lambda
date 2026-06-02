import * as esbuild from 'esbuild';

// Lambda Node.js 20.x ships @aws-sdk/* — don't bundle them.
// @googleapis/calendar + google-auth-library are NOT in the runtime; they must bundle.
// The bundle pulls in ../shared/scheduling/{availability,routing} (reassign re-runs
// routing), so their @aws-sdk/* requires are externalised here too.
// NOTE: @smithy/node-http-handler is deliberately NOT externalized. It is not reliably
// top-level resolvable in the Node 20 runtime, so a flat bundle that externalizes it can
// throw MODULE_NOT_FOUND on cold start. It is pure JS (~20KB), so bundling it is safe.
// (E2E 2026-06-02: externalizing it crashed this fn at import — the sibling consumers
// Calendar_Event_Consumer / Calendar_Lifecycle_Consumer already bundle it for this reason.)
const LAMBDA_EXTERNALS = [
  '@aws-sdk/client-dynamodb',
  '@aws-sdk/client-secrets-manager',
];

await esbuild.build({
  entryPoints: ['index.js'],
  bundle: true,
  platform: 'node',
  target: 'node20',
  outfile: 'dist/index.js',
  format: 'cjs',
  minify: true,
  sourcemap: false,
  external: LAMBDA_EXTERNALS,
  treeShaking: true,
  keepNames: true,
});

console.log('Bundle complete: dist/index.js');
