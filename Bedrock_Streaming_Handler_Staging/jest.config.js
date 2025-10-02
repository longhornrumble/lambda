module.exports = {
  testEnvironment: 'node',
  coverageDirectory: 'coverage',
  collectCoverageFrom: [
    'form_handler.js',
    'response_enhancer.js',
    'index.js',
    '!node_modules/**',
    '!coverage/**',
    '!__tests__/**'
  ],
  testMatch: [
    '**/__tests__/**/*.test.js'
  ],
  coverageThreshold: {
    global: {
      branches: 90,
      functions: 90,
      lines: 90,
      statements: 90
    }
  },
  setupFilesAfterEnv: ['<rootDir>/__tests__/setup.js'],
  verbose: true
};
