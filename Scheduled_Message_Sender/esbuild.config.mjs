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
  // ESM output (dist/index.mjs) — the honest fit: the source is .mjs and package.json is
  // "type":"module", so a .js/cjs bundle would be mis-interpreted as ESM under that package.json
  // (load returns an empty module). The .mjs extension forces ESM regardless of any package.json,
  // so the bundle loads correctly + the deploy needs no package.json in the zip. Lambda handler
  // stays `index.handler` (the runtime resolves index.mjs as ESM and reads the named export).
  format: 'esm',
  external: LAMBDA_EXTERNALS,
  treeShaking: true,
  keepNames: true,
});

console.log('Bundle complete: dist/index.mjs');
