import * as esbuild from 'esbuild';

// Lambda Node.js 20.x ships @aws-sdk/* — don't bundle them. The bundle pulls in the
// shared/scheduling logic modules + featureGate (S3) + tokens (DDB + Secrets Manager);
// their @aws-sdk requires are externalised here too.
//
// NOTE: @smithy/node-http-handler is deliberately NOT externalized (lambda#202) — it is not
// reliably top-level resolvable in the Node 20 runtime; it is pure JS (~20KB) so bundling it
// is safe. Mirrors Scheduling_Redemption_Handler / the calendar consumers.
const LAMBDA_EXTERNALS = [
  '@aws-sdk/client-dynamodb',
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
