'use strict';

/**
 * calendar-events.js — Google Calendar events.insert / events.delete for C8.
 *
 * Distinct from the Listener's read-only calendar-api.js (getEvent/listChangedEvents):
 * C8 is the only WRITE path to a coordinator's calendar.
 *
 * Two invariants enforced here so they cannot be forgotten at a call site:
 *   1. EVERY inserted event carries `extendedProperties.private.booking_id` = the
 *      Booking PK (FROZEN_CONTRACTS §A ownership tag + listener_dispatch_interface
 *      "Delta Discovery" row 10). Without it the B2 listener can't attribute the
 *      change and the typed-event system silently degrades to skipped_non_platform.
 *   2. Event CONTENT obeys the §5.7 write-side PII boundary: title = type + first
 *      name only; description = first+last + auth-gated deep link; attendee = email;
 *      conferencing in the native conferenceData field. Control chars (incl. CR/LF)
 *      are stripped defensively here too (C10 owns the exhaustive sanitization pass).
 */

const calendarApi = require('@googleapis/calendar');

const calendar = calendarApi.calendar('v3');

// Control chars (NUL..0x1F and 0x7F, which includes CR/LF) collapse to a single
// space; ordinary punctuation (hyphens, etc.) is preserved. Defense-in-depth
// alongside C10 — never let a name inject calendar-field structure.
const CONTROL_CHARS = [];
for (let i = 0; i <= 0x1f; i += 1) CONTROL_CHARS.push(String.fromCharCode(i));
CONTROL_CHARS.push(String.fromCharCode(0x7f));
const CONTROL_CHARS_RE = new RegExp(`[${CONTROL_CHARS.join('')}]+`, 'g');

function clean(value) {
  if (value == null) return '';
  return String(value).replace(CONTROL_CHARS_RE, ' ').replace(/\s{2,}/g, ' ').trim();
}

/**
 * Build the events.insert requestBody. Content per §5.7.
 *   { bookingId, appointmentTypeName, attendeeFirstName, attendeeLastName,
 *     attendeeEmail, start, end, timezone, deepLink,
 *     conference: { provider, joinUrl?, conferenceId?, calendarCreateRequest? } }
 */
function buildEventBody({
  bookingId,
  appointmentTypeName,
  attendeeFirstName,
  attendeeLastName,
  attendeeEmail,
  start,
  end,
  timezone,
  deepLink,
  conference,
}) {
  if (!bookingId) throw new Error('bookingId is required (ownership tag)');
  const firstName = clean(attendeeFirstName);
  const typeName = clean(appointmentTypeName) || 'Appointment';

  // Title: type + first name ONLY (visible in previews / lock-screen). No last name.
  const summary = `${typeName}${firstName ? ` — ${firstName}` : ''}`;

  // Description: first+last + auth-gated deep link. No phone, no form contents.
  const fullName = clean(`${attendeeFirstName || ''} ${attendeeLastName || ''}`);
  const descLines = [];
  if (fullName) descLines.push(`Attendee: ${fullName}`);
  if (deepLink) descLines.push(`Manage this booking: ${clean(deepLink)}`);

  const body = {
    summary,
    description: descLines.join('\n'),
    start: { dateTime: start, timeZone: timezone || 'UTC' },
    end: { dateTime: end, timeZone: timezone || 'UTC' },
    attendees: attendeeEmail ? [{ email: clean(attendeeEmail) }] : [],
    // Ownership tag — load-bearing for the B2 listener (FROZEN §A).
    extendedProperties: {
      private: { booking_id: String(bookingId) },
    },
  };

  const conf = conference || {};
  if (conf.calendarCreateRequest) {
    // Google Meet: minted by this same insert call (§6.2). conferenceDataVersion=1.
    body.conferenceData = { createRequest: conf.calendarCreateRequest };
  } else if (conf.joinUrl) {
    // Zoom / Null: attach the externally-minted join URL. location is the robust,
    // always-rendered home; conferenceData is the calendar-native field (§5.7).
    body.location = clean(conf.joinUrl);
    body.conferenceData = {
      conferenceId: conf.conferenceId ? String(conf.conferenceId) : undefined,
      conferenceSolution: {
        key: { type: 'addOn' },
        name: conf.provider === 'zoom' ? 'Zoom' : 'Conference',
      },
      entryPoints: [{ entryPointType: 'video', uri: clean(conf.joinUrl) }],
    };
  }
  return body;
}

// conferenceDataVersion must be 1 whenever we touch conferenceData (Meet createRequest
// OR an attached entryPoint), or Google ignores/rejects the field.
async function insertEvent(authClient, calendarId, requestBody) {
  if (!authClient || !calendarId || !requestBody) {
    throw new Error('authClient, calendarId, and requestBody are required');
  }
  const response = await calendar.events.insert({
    auth: authClient,
    calendarId,
    conferenceDataVersion: requestBody.conferenceData ? 1 : 0,
    requestBody,
  });
  return response.data;
}

// Compensating delete (§4.5). 404/410 means already gone, treat as success (idempotent).
async function deleteEvent(authClient, calendarId, eventId) {
  if (!authClient || !calendarId || !eventId) {
    throw new Error('authClient, calendarId, and eventId are required');
  }
  try {
    await calendar.events.delete({ auth: authClient, calendarId, eventId });
  } catch (err) {
    const code = err.code ?? err.response?.status;
    if (code === 404 || code === 410) return; // already gone
    throw err;
  }
}

// Pull the Google Meet join URL out of an inserted event's conferenceData.
function extractMeetJoinUrl(event) {
  const entryPoints = event?.conferenceData?.entryPoints || [];
  const video = entryPoints.find((e) => e.entryPointType === 'video');
  return video ? video.uri : null;
}

/**
 * Classify an events.insert error for the §5.5-row-4 OAuth-401 decision.
 *   → { isAuth: boolean, permanent: boolean }
 * permanent = the grant is revoked / invalid (re-auth needed; degrade the coordinator);
 * transient = an expired access token a refresh+retry can fix.
 */
function classifyAuthError(err) {
  const code = err?.code ?? err?.response?.status;
  // Google's two error shapes differ: the OAuth token endpoint returns
  // `response.data.error` as a STRING ('invalid_grant', ...), but the Calendar API
  // returns it as an OBJECT ({ code, message, errors[], status }). Coerce to a
  // searchable string so the marker scan below can't crash with
  // "<x>.includes is not a function" (which would mask the real calendar-write error
  // and force COMMIT_FAILED — caught by the 2026-06-04 live booking UAT).
  const rawOauthError = err?.response?.data?.error;
  const oauthError =
    typeof rawOauthError === 'string'
      ? rawOauthError
      : (rawOauthError && (rawOauthError.message || rawOauthError.status)) || '';
  const message = String(err?.message || '');
  const PERMANENT_MARKERS = [
    'invalid_grant',
    'unauthorized_client',
    'invalid_client',
    'Token has been expired or revoked',
  ];
  const looksPermanent = PERMANENT_MARKERS.some(
    (m) => oauthError.includes(m) || message.includes(m)
  );
  if (looksPermanent) return { isAuth: true, permanent: true };
  if (code === 401) return { isAuth: true, permanent: false };
  return { isAuth: false, permanent: false };
}

module.exports = {
  buildEventBody,
  insertEvent,
  deleteEvent,
  extractMeetJoinUrl,
  classifyAuthError,
  clean,
};
