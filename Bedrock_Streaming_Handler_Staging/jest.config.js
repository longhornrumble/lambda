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
  coverageThreshold: {
    global: {
      // Lowered after extracting loadConfig/retrieveKB to shared/bedrock-core.js
      // and adding Bubble webhook integration to form_handler.js.
      // TODO: Raise thresholds as coverage improves.
      branches: 50,
      functions: 70,
      lines: 60,
      statements: 60
    }
  },
  setupFilesAfterEnv: ['<rootDir>/__tests__/setup.js'],
  verbose: true
};
