import * as esbuild from 'esbuild';

// Lambda Node.js 20.x ships @aws-sdk/* — don't bundle them.
// NOTE: @smithy/node-http-handler is deliberately NOT externalized. It is not reliably
// top-level resolvable in the Node 20 runtime, so a flat bundle that externalizes it can
// throw MODULE_NOT_FOUND on cold start. It is pure JS (~20KB), so bundling it is safe.
const LAMBDA_EXTERNALS = [
  '@aws-sdk/client-dynamodb',
  '@aws-sdk/client-sns',
  // gap C reoffer wire: notify.js (Y) → client-lambda; tokens.js (§13.4) → client-secrets-manager.
  '@aws-sdk/client-lambda',
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
