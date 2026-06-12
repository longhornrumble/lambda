import * as esbuild from 'esbuild';

// Lambda Node.js 20.x ships @aws-sdk/* — don't bundle them.
// @smithy/node-http-handler is deliberately NOT externalized: it is not reliably
// top-level resolvable in the Node 20 runtime, so a flat bundle that externalizes it
// throws MODULE_NOT_FOUND on cold start (caught live 2026-06-03, lambda#195/#202). It is
// pure JS (~20KB), so bundling it is safe. Matches the BCH / Redemption-Handler precedent.
const LAMBDA_EXTERNALS = [
  '@aws-sdk/client-cloudwatch',
  '@aws-sdk/client-dynamodb',
  '@aws-sdk/client-lambda',
  '@aws-sdk/client-sns',
  // Pulled transitively by ../shared/scheduling/disposition.js -> tokens.js (the
  // disposition cycle); runtime ships it, same as the others (Calendar_OAuth_Connect
  // precedent for shared-pulled clients).
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
