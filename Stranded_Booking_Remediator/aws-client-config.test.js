'use strict';

const { sdkConfig, _MAX_ATTEMPTS } = require('./aws-client-config');

describe('sdkConfig', () => {
  test('bounds every client: maxAttempts + a request handler with timeouts', () => {
    const cfg = sdkConfig();
    expect(cfg.maxAttempts).toBe(_MAX_ATTEMPTS);
    expect(cfg.requestHandler).toBeDefined();
    // NodeHttpHandler carries the configured connection/request timeouts.
    expect(cfg.requestHandler).toHaveProperty('config');
  });

  test('merges extra config without dropping the bounds', () => {
    const cfg = sdkConfig({ region: 'us-east-1' });
    expect(cfg.region).toBe('us-east-1');
    expect(cfg.maxAttempts).toBe(_MAX_ATTEMPTS);
    expect(cfg.requestHandler).toBeDefined();
  });
});
