import * as esbuild from 'esbuild';

// Lambda Node.js 20.x ships @aws-sdk/* — don't bundle them. The bundle pulls in
// ../shared/scheduling/sessionBinding.js (its @aws-sdk/client-dynamodb require is
// externalised here too).
//
// NOTE: @smithy/node-http-handler is deliberately NOT externalized (lambda#202) — it is
// not reliably top-level resolvable in the Node 20 runtime, so a flat bundle that
// externalizes it throws MODULE_NOT_FOUND on cold start. It is pure JS (~20KB); bundling
// it is safe (the sibling Function-URL Lambdas all do the same).
const LAMBDA_EXTERNALS = [
  '@aws-sdk/client-dynamodb',
  '@aws-sdk/client-lambda',
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
