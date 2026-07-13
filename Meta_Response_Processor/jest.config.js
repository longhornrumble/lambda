module.exports = {
  testEnvironment: 'node',
  // Force @aws-sdk/* imports to resolve through THIS Lambda's node_modules.
  // Required because shared/prompt/prompt_v4.js lazy-requires
  // @aws-sdk/client-bedrock-runtime and shared/ has no node_modules of its
  // own — without this mapper the resolution fails in jest (and, worse, a
  // separate physical copy would bypass aws-sdk-client-mock). Same fix and
  // rationale as Bedrock_Streaming_Handler_Staging/jest.config.js.
  moduleNameMapper: {
    '^(@aws-sdk/[^/]+)$': '<rootDir>/node_modules/$1',
  },
  testMatch: ['**/*.test.js'],
};
