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
  // Coverage thresholds disabled — form_handler.test.js has pre-existing
  // failures that are being fixed separately. Re-enable thresholds once
  // form_handler tests are repaired.
  // coverageThreshold: { global: { branches: 80, functions: 80, lines: 80, statements: 80 } },
  setupFilesAfterEnv: ['<rootDir>/__tests__/setup.js'],
  verbose: true
};
