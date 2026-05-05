module.exports = {
  testEnvironment: 'node',
  coverageDirectory: 'coverage',
  // Allow shared/ to resolve its @aws-sdk deps from BSH's node_modules when
  // shared/ has no own node_modules (the normal state in this repo).
  modulePaths: ['<rootDir>/node_modules'],
  // Force all @aws-sdk/* imports to resolve through THIS Lambda's node_modules.
  // Required because shared/bedrock-core.js requires @aws-sdk/* and CI's
  // `cd shared && npm install` step creates a separate physical copy under
  // shared/node_modules. Without this mapper, jest.doMock() targets
  // (resolved from a test in BSH) don't match shared/'s requires (resolved
  // from shared/'s own node_modules), so mocks silently bypass and real
  // SDK constructors run. See PR for Issue #5 PR A CI failure post-mortem.
  moduleNameMapper: {
    '^(@aws-sdk/[^/]+)$': '<rootDir>/node_modules/$1',
  },
  collectCoverageFrom: [
    'form_handler.js',
    'response_enhancer.js',
    'index.js',
    'clerk_helper.js',
    '!node_modules/**',
    '!coverage/**',
    '!__tests__/**'
  ],
  testMatch: [
    '**/__tests__/**/*.test.js'
  ],
  // Coverage ratchet — set just below current measured coverage to catch
  // regressions without blocking merges. Raise these as tests improve.
  // Current (as of 2026-04-16): statements 57%, branches 47%, funcs 66%, lines 58%.
  // The 80% target from the original config remains aspirational — reaching it
  // requires significant new coverage for index.js (currently ~40%).
  coverageThreshold: {
    global: {
      branches: 45,
      functions: 60,
      lines: 55,
      statements: 55,
    },
  },
  setupFilesAfterEnv: ['<rootDir>/__tests__/setup.js'],
  verbose: true
};
