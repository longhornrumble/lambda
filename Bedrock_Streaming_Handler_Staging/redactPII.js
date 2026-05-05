// Redact email + phone from a string before logging/persisting it.
// Mirrors regexes in Master_Function_Staging/lambda_function.py:88-104
// (canonical Python source). Both writers must produce identical
// outputs for the analytics_writer_contract.json fixture.

const EMAIL_RE = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/g;
const PHONE_RE = /(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/g;

function redactPII(input) {
  if (typeof input !== 'string' || input.length === 0) return '';
  return input.replace(EMAIL_RE, '[EMAIL]').replace(PHONE_RE, '[PHONE]');
}

module.exports = { redactPII };
