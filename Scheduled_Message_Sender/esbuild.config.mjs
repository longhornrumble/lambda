import * as esbuild from 'esbuild';

// Track 1 S3: this Lambda now imports the FROZEN shared §E3 gate (../shared/scheduling/
// channels.js + consent.js). The raw-zip deploy can't see ../shared, so bundle index.mjs +
// its shared imports into a single self-contained dist/index.mjs.
//
// Lambda Node.js 20.x ships @aws-sdk/* — externalize (don't bundle the SDK). The shared
// modules are pure JS (no @aws-sdk at module load), so they bundle cleanly.
const LAMBDA_EXTERNALS = [
  '@aws-sdk/client-dynamodb',
  '@aws-sdk/client-lambda',
  '@aws-sdk/client-scheduler',
  '@aws-sdk/lib-dynamodb',
];

await esbuild.build({
  entryPoints: ['index.mjs'],
  bundle: true,
  platform: 'node',
  target: 'node20',
  outfile: 'dist/index.mjs',
  format: 'esm', // preserves the named `export async function handler` for the runtime
  external: LAMBDA_EXTERNALS,
  treeShaking: true,
  keepNames: true,
});

console.log('Bundle complete: dist/index.mjs');
