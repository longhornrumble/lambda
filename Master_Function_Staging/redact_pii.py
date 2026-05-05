"""Redact email + phone from a string before logging/persisting it.

Mirrors regexes in lambda_function.py:88-104 (canonical) and
Bedrock_Streaming_Handler_Staging/redactPII.js. Both writers must produce
identical outputs for the analytics_writer_contract.json fixture.
"""
import re

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
PHONE_RE = re.compile(r'(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}')


def redact_pii(text):
    """Return ``text`` with email and US-NANP phone numbers replaced by tokens.

    Returns empty string for non-strings or empty input. Truncation to a max
    length is the caller's responsibility (see analytics_writer.write_session_summary
    for the 50-char first_question truncation).
    """
    if not isinstance(text, str) or len(text) == 0:
        return ''
    text = EMAIL_RE.sub('[EMAIL]', text)
    text = PHONE_RE.sub('[PHONE]', text)
    return text
