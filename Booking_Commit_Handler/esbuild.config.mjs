import * as esbuild from 'esbuild';

// Lambda Node.js 20.x ships @aws-sdk/* — don't bundle them.
// @googleapis/calendar + google-auth-library are NOT in the runtime; they must bundle.
// NOTE: @smithy/node-http-handler is deliberately NOT externalized. It is not reliably
// top-level resolvable in the Node 20 runtime, so a flat bundle that externalizes it
// throws MODULE_NOT_FOUND on cold start (caught live 2026-06-03). It is pure JS (~20KB),
// so bundling it is safe. Matches the Calendar_Event_Consumer precedent (lambda#195).
const LAMBDA_EXTERNALS = [
  '@aws-sdk/client-dynamodb',
  '@aws-sdk/client-s3',
  '@aws-sdk/client-secrets-manager',
  '@aws-sdk/client-ses',
  '@aws-sdk/client-sns',
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
