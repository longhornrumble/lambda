import * as esbuild from 'esbuild';

// Lambda Node.js 20.x has these built-in - don't bundle them
const LAMBDA_EXTERNALS = [
  '@aws-sdk/client-s3',
  '@aws-sdk/client-sqs',
  '@aws-sdk/client-dynamodb',
  '@aws-sdk/client-sns',
  '@aws-sdk/client-ses',
  '@aws-sdk/client-lambda',
  '@aws-sdk/lib-dynamodb',
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
  // Mark Lambda built-ins as external
  external: LAMBDA_EXTERNALS,
  // Tree-shake unused code
  treeShaking: true,
  // Keep function names for debugging
  keepNames: true,
});

console.log('✅ Bundle complete: dist/index.js');
