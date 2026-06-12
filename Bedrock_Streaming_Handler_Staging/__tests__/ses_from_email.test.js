/**
 * getSESFromEmail — SES sender env resolution tests.
 *
 * Verifies SES_FROM_EMAIL is used when set, and that the legacy hardcoded
 * fallback is used with a loud SENDER_ENV_MISSING warning (once per Lambda
 * instance) when unset — never crashing (prod envs aren't wired yet).
 *
 * jest.isolateModules gives each test a fresh module registry so the
 * once-per-instance warning flag resets between tests.
 */

describe('getSESFromEmail', () => {
  const ORIGINAL_SES_FROM_EMAIL = process.env.SES_FROM_EMAIL;
  let warnSpy;

  beforeEach(() => {
    warnSpy = jest.spyOn(console, 'warn').mockImplementation(() => {});
  });

  afterEach(() => {
    warnSpy.mockRestore();
    if (ORIGINAL_SES_FROM_EMAIL === undefined) {
      delete process.env.SES_FROM_EMAIL;
    } else {
      process.env.SES_FROM_EMAIL = ORIGINAL_SES_FROM_EMAIL;
    }
  });

  function freshFormHandler() {
    let mod;
    jest.isolateModules(() => {
      mod = require('../form_handler');
    });
    return mod;
  }

  it('uses SES_FROM_EMAIL when set, with no SENDER_ENV_MISSING warning', () => {
    process.env.SES_FROM_EMAIL = 'notify@staging.myrecruiter.ai';
    const { getSESFromEmail } = freshFormHandler();

    expect(getSESFromEmail()).toBe('notify@staging.myrecruiter.ai');
    expect(warnSpy).not.toHaveBeenCalledWith(
      expect.stringContaining('SENDER_ENV_MISSING')
    );
  });

  it('falls back to the legacy hardcoded sender with a loud warning when unset', () => {
    delete process.env.SES_FROM_EMAIL;
    const { getSESFromEmail } = freshFormHandler();

    expect(getSESFromEmail()).toBe('notify@myrecruiter.ai');
    expect(warnSpy).toHaveBeenCalledWith(
      expect.stringContaining('SENDER_ENV_MISSING')
    );
  });

  it('warns only once per Lambda instance', () => {
    delete process.env.SES_FROM_EMAIL;
    const { getSESFromEmail } = freshFormHandler();

    getSESFromEmail();
    const warnsAfterFirstCall = warnSpy.mock.calls.length;
    getSESFromEmail();

    expect(getSESFromEmail()).toBe('notify@myrecruiter.ai');
    expect(warnSpy.mock.calls.length).toBe(warnsAfterFirstCall);
  });
});
