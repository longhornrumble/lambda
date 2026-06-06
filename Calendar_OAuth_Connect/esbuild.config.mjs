import * as esbuild from 'esbuild';

// Lambda Node.js 20.x ships @aws-sdk/* — don't bundle them. The bundle also pulls in
// ../shared/scheduling/featureGate.js, whose @aws-sdk/client-s3 require is externalised too.
//
// google-auth-library is NOT in the runtime — it MUST bundle (same as Calendar_Watch_Onboarder).
//
// @smithy/node-http-handler is deliberately NOT externalized (lambda#202): it is not reliably
// top-level resolvable in the Node 20 runtime, so a flat bundle that externalizes it throws
// MODULE_NOT_FOUND on cold start. It is pure JS (~20KB) — bundling it is safe.
const LAMBDA_EXTERNALS = [
  '@aws-sdk/client-lambda',
  '@aws-sdk/client-s3',
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
