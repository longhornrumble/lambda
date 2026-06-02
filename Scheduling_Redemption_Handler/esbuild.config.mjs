import * as esbuild from 'esbuild';

// Lambda Node.js 20.x ships @aws-sdk/* — don't bundle them. The bundle pulls in
// ../shared/scheduling/tokens.js, whose @aws-sdk/client-dynamodb + client-secrets-manager
// requires are externalised here too.
//
// NOTE: @smithy/node-http-handler is deliberately NOT externalized (lambda#202). It is not
// reliably top-level resolvable in the Node 20 runtime, so a flat bundle that externalizes
// it throws MODULE_NOT_FOUND on cold start (E2E 2026-06-02 crashed the remediator this way).
// It is pure JS (~20KB) — bundling it is safe. The sibling consumers
// (Calendar_Event_Consumer / Calendar_Lifecycle_Consumer / Stranded_Booking_Remediator)
// all bundle it for the same reason.
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
