module.exports = {
  testEnvironment: 'node',
  coverageDirectory: 'coverage',
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
