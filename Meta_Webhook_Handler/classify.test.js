'use strict';

/**
 * Unit tests for the payload-v2 classifier (contract C1).
 *
 * The M1a DONE line: every fixture produces a typed v2 classification or a
 * logged intentional skip — zero silent drops. The final meta-test enforces
 * that over the ENTIRE fixture library, so adding a fixture without handling
 * its shape fails this suite.
 */

const { classifyMessagingEvent } = require('./classify');
const F = require('./__fixtures__/messagingEvents');

describe('classify — text / quick reply / postback (v1-compatible kinds)', () => {
  test.each([
    ['fbText', F.fbText, 'Hello from Messenger'],
    ['igText', F.igText, 'Hello from Instagram'],
  ])('%s → text with v1 fields intact', (_name, fixture, text) => {
    const [c] = classifyMessagingEvent(fixture);
    expect(c.eventKind).toBe('text');
    expect(c.messageText).toBe(text);
    expect(c.messageMid).toBe(F.MID);
    expect(c.isPostback).toBe(false);
    expect(c.quickReplyPayload).toBeNull();
  });

  test.each([
    ['fbQuickReply', F.fbQuickReply, 'PIC1:cta:volunteer'],
    ['igQuickReply', F.igQuickReply, 'PIC1:cta:apply'],
  ])('%s → quick_reply carries payload AND keeps message.text as messageText', (_n, fixture, payload) => {
    const [c] = classifyMessagingEvent(fixture);
    expect(c.eventKind).toBe('quick_reply');
    expect(c.quickReplyPayload).toBe(payload);
    expect(c.messageText).toBe(fixture.message.text); // v1 behavior preserved
    expect(c.isPostback).toBe(false);
  });

  test.each([
    ['fbPostback', F.fbPostback, 'GET_STARTED'],
    ['igPostback', F.igPostback, 'PIC1:cta:programs'],
  ])('%s → postback with payload as messageText (v1 behavior)', (_n, fixture, payload) => {
    const [c] = classifyMessagingEvent(fixture);
    expect(c.eventKind).toBe('postback');
    expect(c.messageText).toBe(payload);
    expect(c.isPostback).toBe(true);
    expect(c.messageMid).toMatch(/^postback_/);
  });
});

describe('classify — attachments and stickers', () => {
  test('attachment-only message → attachment, messageText null, types captured', () => {
    const [c] = classifyMessagingEvent(F.fbAttachmentImage);
    expect(c.eventKind).toBe('attachment');
    expect(c.messageText).toBeNull();
    expect(c.attachmentTypes).toEqual(['image']);
  });

  test('IG audio attachment → attachment with audio type', () => {
    const [c] = classifyMessagingEvent(F.igAttachmentAudio);
    expect(c.eventKind).toBe('attachment');
    expect(c.attachmentTypes).toEqual(['audio']);
  });

  test('text WITH attachment → text (attachmentTypes still populated)', () => {
    const [c] = classifyMessagingEvent(F.fbTextWithAttachment);
    expect(c.eventKind).toBe('text');
    expect(c.messageText).toBe('look at this');
    expect(c.attachmentTypes).toEqual(['image']);
  });

  test('sticker pre-Aug-30-2026 shape (sticker_id) → sticker', () => {
    const [c] = classifyMessagingEvent(F.fbStickerPreMigration);
    expect(c.eventKind).toBe('sticker');
  });

  test('sticker post-Aug-30-2026 shape (attachment type) → sticker', () => {
    const [c] = classifyMessagingEvent(F.fbStickerPostMigration);
    expect(c.eventKind).toBe('sticker');
    expect(c.attachmentTypes).toEqual(['sticker']);
  });
});

describe('classify — edits and deletes', () => {
  test.each([
    ['fbEdit', F.fbEdit, 'edited text'],
    ['igEdit', F.igEdit, 'edited IG text'],
  ])('%s → edit with targetMid + editedText', (_n, fixture, text) => {
    const [c] = classifyMessagingEvent(fixture);
    expect(c.eventKind).toBe('edit');
    expect(c.targetMid).toBe(F.MID);
    expect(c.editedText).toBe(text);
  });

  test('FB message_deletions with 2 mids → 2 delete results', () => {
    const results = classifyMessagingEvent(F.fbDeleteTwoMids);
    expect(results).toHaveLength(2);
    expect(results.map((r) => r.eventKind)).toEqual(['delete', 'delete']);
    expect(results.map((r) => r.targetMid)).toEqual(['m_deleted_1', 'm_deleted_2']);
  });

  test('IG message.is_deleted → delete with targetMid', () => {
    const [c] = classifyMessagingEvent(F.igDelete);
    expect(c.eventKind).toBe('delete');
    expect(c.targetMid).toBe(F.MID);
  });
});

describe('classify — echoes (sender/recipient inversion, C1)', () => {
  test.each([
    ['fbEcho', F.fbEcho, F.PSID, '1122334455'],
    ['igEcho', F.igEcho, F.IGSID, '9988776655'],
  ])('%s → echo with psid from recipient.id, appId string, NULL messageText', (_n, fixture, customerId, appId) => {
    const [c] = classifyMessagingEvent(fixture);
    expect(c.eventKind).toBe('echo');
    expect(c.psid).toBe(customerId); // the CUSTOMER, not the business account
    expect(c.appId).toBe(appId);
    // Loop guard (C1 v1.1): echo text must never reach the processor as messageText
    expect(c.messageText).toBeNull();
  });
});

describe('classify — reply-to context', () => {
  test('IG story reply → text with replyTo.storyUrl', () => {
    const [c] = classifyMessagingEvent(F.igStoryReply);
    expect(c.eventKind).toBe('text');
    expect(c.replyTo).toEqual({ storyUrl: 'https://instagram.com/stories/x/123' });
  });

  test('FB reply_to.mid → text with replyTo.mid', () => {
    const [c] = classifyMessagingEvent(F.fbReplyTo);
    expect(c.replyTo).toEqual({ mid: 'm_original_001' });
  });
});

describe('classify — metadata-only events are intentional skips (C1)', () => {
  test.each([
    ['reaction', F.fbReaction],
    ['delivery receipt', F.fbDeliveryReceipt],
    ['read receipt', F.fbReadReceipt],
    ['standalone referral', F.fbStandaloneReferral],
    ['response_feedback', F.fbResponseFeedback],
  ])('%s → skip (no invoke, no fallback reply)', (_n, fixture) => {
    const [c] = classifyMessagingEvent(fixture);
    expect(c.skip).toBeTruthy();
  });
});

describe('classify — future/unknown shapes', () => {
  test('message with mid but unreadable content → unsupported (never silent)', () => {
    const [c] = classifyMessagingEvent(F.fbUnsupportedFutureContent);
    expect(c.eventKind).toBe('unsupported');
    expect(c.messageMid).toBe(F.MID);
  });

  test('empty message object → receipt-like skip', () => {
    const [c] = classifyMessagingEvent(F.fbReceiptLikeMessage);
    expect(c.skip).toMatch(/receipt-like/);
  });

  test('unknown event layout → skip with shape keys, no values', () => {
    const [c] = classifyMessagingEvent(F.unknownEventShape);
    expect(c.skip).toContain('some_new_meta_event');
    expect(c.skip).not.toContain('anything'); // key names only, never values
  });

  test('null/garbage input → skip, never throws', () => {
    expect(classifyMessagingEvent(null)[0].skip).toBeTruthy();
    expect(classifyMessagingEvent('string')[0].skip).toBeTruthy();
  });
});

describe('classify — standby flag', () => {
  test('standby text event classifies normally with isStandby=true', () => {
    const [c] = classifyMessagingEvent(F.fbText, true);
    expect(c.eventKind).toBe('text');
    expect(c.isStandby).toBe(true);
  });
});

describe('DONE-line meta-test: zero silent drops over the whole fixture library', () => {
  test('every fixture yields ≥1 result, each a typed classification or an explicit skip', () => {
    const fixtures = Object.entries(require('./__fixtures__/messagingEvents')).filter(
      ([, v]) => v && typeof v === 'object' && !Array.isArray(v)
    );
    expect(fixtures.length).toBeGreaterThanOrEqual(20);
    for (const [name, fixture] of fixtures) {
      const results = classifyMessagingEvent(fixture);
      expect(results.length).toBeGreaterThanOrEqual(1);
      for (const r of results) {
        const typed = typeof r.eventKind === 'string' && r.eventKind.length > 0;
        const skipped = typeof r.skip === 'string' && r.skip.length > 0;
        if (!typed && !skipped) {
          throw new Error(`SILENT DROP: fixture ${name} produced neither a typed classification nor a skip`);
        }
      }
    }
  });
});
