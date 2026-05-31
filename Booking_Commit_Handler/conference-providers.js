'use strict';

/**
 * conference-providers.js — the §5.2-item-4 / FROZEN_CONTRACTS §B6 ConferenceProvider
 * interface and its three v1 implementations.
 *
 * The commit path (index.js) NEVER branches `if (zoom) … else if (meet) …` inline:
 * it resolves ONE provider up front and calls `createConference(ctx)` against the
 * interface. This is the guard against the v2 Microsoft Teams addition forcing a
 * rewrite — and the `NullConferenceProvider` makes the seam testable (inject it →
 * the whole commit completes touching neither Google nor Zoom).
 *
 * Interface:
 *   createConference(ctx) → {
 *     provider:       'google_meet' | 'zoom' | 'null',
 *     conferenceId:   string | null,     // null for Meet until events.insert returns it
 *     joinUrl:        string | null,      // null for Meet until events.insert returns it
 *     deferToCalendarInsert: boolean,     // true ⇒ Meet: attach createRequest to events.insert
 *     calendarCreateRequest?: { requestId, conferenceSolutionKey },  // Meet only
 *   }
 *   ctx = { tenantId, coordinatorId, bookingId, topic, start, end, timezone,
 *           attendeeEmail, existingConferenceId? }
 *
 * Why Meet "defers": Google Meet links are minted by the SAME `events.insert` call
 * (conferenceData.createRequest), not a separate API — §6.2. So GoogleMeetProvider
 * does NOT call out here; it returns the createRequest plan (with a DETERMINISTIC
 * requestId derived from bookingId = Google-native idempotency) and index.js reads
 * the join URL back off the inserted event. Zoom + Null produce a join URL up front
 * (steps run before events.insert) which index.js attaches to the event.
 */

const crypto = require('crypto');
const zoomClient = require('./zoom-client');

// Stable Meet idempotency token (Google dedupes events.insert on createRequest.requestId).
// Deterministic from bookingId so a retried insert reuses the same Meet conference.
function meetRequestId(bookingId) {
  return `meet-${crypto.createHash('sha256').update(String(bookingId)).digest('hex').slice(0, 32)}`;
}

class ConferenceProvider {
  // eslint-disable-next-line class-methods-use-this, no-unused-vars
  async createConference(ctx) {
    throw new Error('createConference must be implemented by a ConferenceProvider subclass');
  }
}

class GoogleMeetProvider extends ConferenceProvider {
  // eslint-disable-next-line class-methods-use-this
  async createConference(ctx) {
    if (!ctx || !ctx.bookingId) throw new Error('bookingId is required for Google Meet conference');
    return {
      provider: 'google_meet',
      conferenceId: null, // resolved from the inserted event's conferenceData
      joinUrl: null,
      deferToCalendarInsert: true,
      calendarCreateRequest: {
        requestId: meetRequestId(ctx.bookingId),
        conferenceSolutionKey: { type: 'hangoutsMeet' },
      },
    };
  }
}

class ZoomProvider extends ConferenceProvider {
  // The zoom client is injectable so unit tests verify read-before-write without
  // a network call; production uses the real ./zoom-client.
  constructor(client = zoomClient) {
    super();
    this.client = client;
  }

  async createConference(ctx) {
    if (!ctx || !ctx.tenantId || !ctx.coordinatorId) {
      throw new Error('tenantId and coordinatorId are required for Zoom conference');
    }
    // read-before-write: reuse a prior partial-attempt meeting id → no duplicate.
    const { meetingId, joinUrl } = await this.client.createMeeting({
      tenantId: ctx.tenantId,
      coordinatorId: ctx.coordinatorId,
      topic: ctx.topic,
      start: ctx.start,
      end: ctx.end,
      timezone: ctx.timezone,
      existingMeetingId: ctx.existingConferenceId || undefined,
    });
    return {
      provider: 'zoom',
      conferenceId: meetingId,
      joinUrl,
      deferToCalendarInsert: false,
    };
  }
}

class NullConferenceProvider extends ConferenceProvider {
  // eslint-disable-next-line class-methods-use-this
  async createConference(ctx) {
    const bookingId = (ctx && ctx.bookingId) || 'unknown';
    // Synthetic, deterministic ids — NO external call. This is the interface-seam
    // verification: index.js completes the entire commit against this provider.
    const conferenceId = `null-conf-${bookingId}`;
    return {
      provider: 'null',
      conferenceId,
      joinUrl: `https://conference.invalid/${encodeURIComponent(conferenceId)}`,
      deferToCalendarInsert: false,
    };
  }
}

// Factory: resolve the provider from the booking's conference type. DI override
// (`overrides`) lets index.js / tests inject a NullConferenceProvider or a Zoom
// client double without touching the commit logic.
function resolveProvider(conferenceType, overrides = {}) {
  if (overrides[conferenceType]) return overrides[conferenceType];
  switch (conferenceType) {
    case 'google_meet':
      return new GoogleMeetProvider();
    case 'zoom':
      return new ZoomProvider(overrides.zoomClient || zoomClient);
    case 'null':
      return new NullConferenceProvider();
    default:
      throw new Error(`unknown conference_type: ${conferenceType}`);
  }
}

module.exports = {
  ConferenceProvider,
  GoogleMeetProvider,
  ZoomProvider,
  NullConferenceProvider,
  resolveProvider,
  meetRequestId,
};
