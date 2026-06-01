import * as esbuild from 'esbuild';

// Lambda Node.js 20.x ships @aws-sdk/* — don't bundle them.
// @googleapis/calendar + google-auth-library are NOT in the runtime; they must bundle.
// The bundle pulls in ../shared/scheduling/{availability,routing} (reassign re-runs
// routing), so their @aws-sdk/* requires are externalised here too.
const LAMBDA_EXTERNALS = [
  '@aws-sdk/client-dynamodb',
  '@aws-sdk/client-secrets-manager',
  // Ships in the Node 20 runtime as an @aws-sdk/* transitive dep — use the runtime
  // copy so the bounded request handler matches the SDK clients' expected version.
  '@smithy/node-http-handler',
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
