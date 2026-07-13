module.exports = {
  testEnvironment: 'node',
  coverageDirectory: 'coverage',
  collectCoverageFrom: [
    'prompt_v4.js',
    'prompt_v5.js',
    'streamTail.js',
    '!node_modules/**',
    '!coverage/**',
    '!__tests__/**',
  ],
  testMatch: ['**/__tests__/**/*.test.js'],
  // Coverage ratchets transplanted verbatim from BSH's jest.config.js when the
  // trio moved here (M2) — same floors, same convention (set just below the
  // measured floor to catch regressions without blocking merges).
  coverageThreshold: {
    './prompt_v4.js': {
      branches: 88,
      functions: 95,
      lines: 95,
      statements: 95,
    },
    './streamTail.js': {
      branches: 95,
      functions: 95,
      lines: 95,
      statements: 95,
    },
    './prompt_v5.js': {
      branches: 95,
      functions: 95,
      lines: 95,
      statements: 95,
    },
  },
  verbose: true,
};
