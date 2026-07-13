'use strict';

/**
 * Webhook messaging-event fixture library (contract C1 — M1a deliverable).
 *
 * FB and IG variants of every event kind the classifier handles. Downstream
 * subphases (M1b processor hygiene, M3b wiring, M6b coexistence) test against
 * these shapes — extend here, don't fork per-suite fixtures.
 *
 * Shapes follow Meta's webhook reference as of July 2026 (research pack
 * Facebook/messenger-research-2026-07/, picasso repo). The sticker fixtures
 * carry BOTH the pre- and post-Aug-30-2026 shapes (webhook migration).
 */

const PSID = '987654321012345';
const PAGE_ID = '112233445566778';
const IGSID = '17841400000000001';
const IG_ACCOUNT_ID = '17841405822304914';
const MID = 'm_fixture_mid_001';
const TS = 1752300000000; // fixed epoch ms — fixtures are deterministic

const sender = { id: PSID };
const igSender = { id: IGSID };
const recipient = { id: PAGE_ID };
const igRecipient = { id: IG_ACCOUNT_ID };

module.exports = {
  PSID,
  PAGE_ID,
  IGSID,
  IG_ACCOUNT_ID,
  MID,
  TS,

  // ── text ──
  fbText: {
    sender, recipient, timestamp: TS,
    message: { mid: MID, text: 'Hello from Messenger' },
  },
  igText: {
    sender: igSender, recipient: igRecipient, timestamp: TS,
    message: { mid: MID, text: 'Hello from Instagram' },
  },

  // ── quick reply tap (arrives as a message, NOT a postback) ──
  fbQuickReply: {
    sender, recipient, timestamp: TS,
    message: {
      mid: MID,
      text: 'Volunteer opportunities',
      quick_reply: { payload: 'PIC1:cta:volunteer' },
    },
  },
  igQuickReply: {
    sender: igSender, recipient: igRecipient, timestamp: TS,
    message: {
      mid: MID,
      text: 'How do I apply?',
      quick_reply: { payload: 'PIC1:cta:apply' },
    },
  },

  // ── postback (persistent menu / button template / ice breaker) ──
  fbPostback: {
    sender, recipient, timestamp: TS,
    postback: { payload: 'GET_STARTED', title: 'Get Started' },
  },
  igPostback: {
    sender: igSender, recipient: igRecipient, timestamp: TS,
    postback: { payload: 'PIC1:cta:programs', title: 'Our programs' },
  },

  // ── attachments ──
  fbAttachmentImage: {
    sender, recipient, timestamp: TS,
    message: {
      mid: MID,
      attachments: [{ type: 'image', payload: { url: 'https://cdn.example/img.jpg' } }],
    },
  },
  igAttachmentAudio: {
    sender: igSender, recipient: igRecipient, timestamp: TS,
    message: {
      mid: MID,
      attachments: [{ type: 'audio', payload: { url: 'https://cdn.example/voice.mp4' } }],
    },
  },
  fbTextWithAttachment: {
    sender, recipient, timestamp: TS,
    message: {
      mid: MID,
      text: 'look at this',
      attachments: [{ type: 'image', payload: { url: 'https://cdn.example/img.jpg' } }],
    },
  },

  // ── stickers: pre- and post-Aug-30-2026 webhook shapes ──
  fbStickerPreMigration: {
    sender, recipient, timestamp: TS,
    message: {
      mid: MID,
      sticker_id: 369239263222822,
      attachments: [{ type: 'image', payload: { url: 'https://cdn.example/sticker.png', sticker_id: 369239263222822 } }],
    },
  },
  fbStickerPostMigration: {
    sender, recipient, timestamp: TS,
    message: {
      mid: MID,
      attachments: [{ type: 'sticker', payload: { url: 'https://cdn.example/sticker.png' } }],
    },
  },

  // ── edit (FB message_edits field / IG message_edit) ──
  fbEdit: {
    sender, recipient, timestamp: TS,
    message_edit: { mid: MID, text: 'edited text', num_edit: 1 },
  },
  igEdit: {
    sender: igSender, recipient: igRecipient, timestamp: TS,
    message_edit: { mid: MID, text: 'edited IG text' },
  },

  // ── delete: FB message_deletions (mids list) / IG message.is_deleted ──
  fbDeleteTwoMids: {
    sender, recipient, timestamp: TS,
    delete: { mids: ['m_deleted_1', 'm_deleted_2'] },
  },
  igDelete: {
    sender: igSender, recipient: igRecipient, timestamp: TS,
    message: { mid: MID, is_deleted: true },
  },

  // ── echo (sender/recipient INVERTED — business is sender) ──
  fbEcho: {
    sender: { id: PAGE_ID },
    recipient: { id: PSID },
    timestamp: TS,
    message: { mid: MID, text: 'Our bot reply echoed back', is_echo: true, app_id: 1122334455 },
  },
  igEcho: {
    sender: { id: IG_ACCOUNT_ID },
    recipient: { id: IGSID },
    timestamp: TS,
    message: { mid: MID, text: 'IG bot reply echoed', is_echo: true, app_id: 9988776655 },
  },

  // ── IG story reply (message with reply_to.story) ──
  igStoryReply: {
    sender: igSender, recipient: igRecipient, timestamp: TS,
    message: {
      mid: MID,
      text: 'love this story!',
      reply_to: { story: { url: 'https://instagram.com/stories/x/123', id: 'story_123' } },
    },
  },
  fbReplyTo: {
    sender, recipient, timestamp: TS,
    message: { mid: MID, text: 'replying to that', reply_to: { mid: 'm_original_001' } },
  },

  // ── metadata-only events (logged intentional skips — C1) ──
  fbReaction: {
    sender, recipient, timestamp: TS,
    reaction: { mid: MID, action: 'react', emoji: '❤️', reaction: 'love' },
  },
  fbDeliveryReceipt: {
    sender, recipient, timestamp: TS,
    delivery: { mids: [MID], watermark: TS },
  },
  fbReadReceipt: {
    sender, recipient, timestamp: TS,
    read: { watermark: TS },
  },
  fbStandaloneReferral: {
    sender, recipient, timestamp: TS,
    referral: { ref: 'campaign_x', source: 'SHORTLINK', type: 'OPEN_THREAD' },
  },
  fbResponseFeedback: {
    sender, recipient, timestamp: TS,
    response_feedback: { mid: MID, feedback: 'thumbs_up' },
  },

  // ── future/unknown shapes ──
  fbUnsupportedFutureContent: {
    sender, recipient, timestamp: TS,
    message: { mid: MID, some_future_content_type: { data: 'x' } },
  },
  fbReceiptLikeMessage: {
    sender, recipient, timestamp: TS,
    message: {},
  },
  unknownEventShape: {
    sender, recipient, timestamp: TS,
    some_new_meta_event: { anything: true },
  },
};
