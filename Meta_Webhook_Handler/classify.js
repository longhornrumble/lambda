'use strict';

/**
 * Payload-v2 event classifier (contract C1 — docs/messenger/CONTRACTS.md).
 *
 * Pure function: takes one entry.messaging[] (or entry.standby[]) event and
 * returns a list of classification results. Each result is either
 *   { skip: '<reason>' }                          — logged intentional skip
 * or a classification the caller turns into a v2 invoke payload:
 *   { eventKind, psid, messageText, messageMid, isPostback,
 *     quickReplyPayload, appId, attachmentTypes, targetMid, editedText,
 *     replyTo, isStandby }
 *
 * Rules frozen in C1. Never throws on malformed shapes — unknown layouts
 * become a skip with shape-only diagnostics (the caller logs keys, never values).
 */

/**
 * @param {object} messagingEvent - one element of entry.messaging[] / entry.standby[]
 * @param {boolean} isStandby     - true when the event came from entry.standby[]
 * @returns {Array<object>} classification results (usually length 1; FB
 *   message_deletions with N mids yields N 'delete' results)
 */
function classifyMessagingEvent(messagingEvent, isStandby = false) {
  if (!messagingEvent || typeof messagingEvent !== 'object') {
    return [{ skip: 'malformed messaging event' }];
  }

  const senderId = messagingEvent.sender?.id || null;
  const recipientId = messagingEvent.recipient?.id || null;

  // Metadata-only events carry no user input to answer (C1): logged skip,
  // no invoke, no fallback reply.
  if (messagingEvent.reaction) return [{ skip: 'reaction' }];
  if (messagingEvent.delivery) return [{ skip: 'delivery receipt' }];
  if (messagingEvent.read) return [{ skip: 'read receipt' }];
  if (messagingEvent.referral) return [{ skip: 'standalone referral' }];
  if (messagingEvent.response_feedback) return [{ skip: 'response_feedback' }];

  const base = {
    psid: senderId,
    messageText: null,
    messageMid: null,
    isPostback: false,
    quickReplyPayload: null,
    appId: null,
    attachmentTypes: [],
    targetMid: null,
    editedText: null,
    replyTo: null,
    isStandby,
  };

  // ── message_edit (FB message_edits field / IG message_edit) ──
  if (messagingEvent.message_edit) {
    const edit = messagingEvent.message_edit;
    if (!senderId) return [{ skip: 'edit without sender.id' }];
    return [{
      ...base,
      eventKind: 'edit',
      targetMid: edit.mid || null,
      editedText: edit.text || null,
      messageMid: edit.mid ? `edit_${edit.mid}` : null,
    }];
  }

  // ── FB message_deletions field: delete.mids[] — one result per mid ──
  if (messagingEvent.delete && Array.isArray(messagingEvent.delete.mids)) {
    if (!senderId) return [{ skip: 'delete without sender.id' }];
    const mids = messagingEvent.delete.mids.filter(Boolean);
    if (mids.length === 0) return [{ skip: 'delete with no mids' }];
    return mids.map((mid) => ({
      ...base,
      eventKind: 'delete',
      targetMid: mid,
      messageMid: `delete_${mid}`,
    }));
  }

  // ── postback (persistent menu / button template) ──
  if (messagingEvent.postback) {
    if (!senderId) return [{ skip: 'postback without sender.id' }];
    return [{
      ...base,
      eventKind: 'postback',
      messageText: messagingEvent.postback.payload || null,
      messageMid: `postback_${senderId}_${Date.now()}`,
      isPostback: true,
    }];
  }

  // ── message ──
  if (messagingEvent.message) {
    const msg = messagingEvent.message;

    // Echo: Meta INVERTS sender/recipient — the business is sender.id, the
    // customer is recipient.id (C1). messageText is deliberately null on echo
    // payloads: the legacy processor would otherwise treat our own echoed
    // reply as a user turn and answer it — an infinite loop. M6b's pause
    // logic needs only psid/appId/timestamp. (C1 v1.1 clarification.)
    if (msg.is_echo) {
      if (!recipientId) return [{ skip: 'echo without recipient.id' }];
      return [{
        ...base,
        eventKind: 'echo',
        psid: recipientId,
        appId: msg.app_id != null ? String(msg.app_id) : null,
        messageMid: msg.mid || null,
      }];
    }

    if (!senderId) return [{ skip: 'message without sender.id' }];

    // IG message deletion arrives as message.is_deleted (research 03)
    if (msg.is_deleted) {
      return [{
        ...base,
        eventKind: 'delete',
        targetMid: msg.mid || null,
        messageMid: msg.mid ? `delete_${msg.mid}` : null,
      }];
    }

    const attachmentTypes = Array.isArray(msg.attachments)
      ? msg.attachments.map((a) => a?.type).filter(Boolean)
      : [];
    // Pre-Aug-30-2026 FB sticker shape already carries a 'sticker' attachment
    // type (or sticker_id); post-migration it is the attachment type itself.
    const isSticker = attachmentTypes.includes('sticker') || msg.sticker_id != null;

    // reply_to context (IG story replies / reply-to — C1)
    const replyTo = msg.reply_to
      ? {
          ...(msg.reply_to.mid ? { mid: msg.reply_to.mid } : {}),
          ...(msg.reply_to.story?.url ? { storyUrl: msg.reply_to.story.url } : {}),
        }
      : null;

    // Quick-reply tap: arrives as a message with quick_reply.payload (NOT a
    // postback). messageText keeps message.text for v1 compatibility.
    if (msg.quick_reply?.payload) {
      return [{
        ...base,
        eventKind: 'quick_reply',
        messageText: msg.text || null,
        messageMid: msg.mid || null,
        quickReplyPayload: msg.quick_reply.payload,
        attachmentTypes,
        replyTo,
      }];
    }

    if (msg.text) {
      return [{
        ...base,
        eventKind: 'text',
        messageText: msg.text,
        messageMid: msg.mid || null,
        attachmentTypes,
        replyTo,
      }];
    }

    if (attachmentTypes.length > 0) {
      return [{
        ...base,
        eventKind: isSticker ? 'sticker' : 'attachment',
        messageMid: msg.mid || null,
        attachmentTypes,
        replyTo,
      }];
    }

    // Message with a mid but no recognisable content: future content types.
    // Carries user input we can't read — 'unsupported', never a silent drop.
    if (msg.mid) {
      return [{
        ...base,
        eventKind: 'unsupported',
        messageMid: msg.mid,
        replyTo,
      }];
    }

    return [{ skip: 'message with no text, attachments, or mid (receipt-like)' }];
  }

  // Unknown event layout — no user input we can identify. Logged skip with
  // shape diagnostics (caller logs key names only, never values).
  return [{ skip: `unknown event shape (keys=${Object.keys(messagingEvent).join(',')})` }];
}

module.exports = { classifyMessagingEvent };
