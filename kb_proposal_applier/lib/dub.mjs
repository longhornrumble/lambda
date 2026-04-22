/**
 * VENDORED from picasso-webscraping/rag-scraper/lib/dub.mjs
 *
 * This file is a byte-identical copy of the canonical module in the
 * picasso-webscraping repo. Both consumers (the onboarding CLI and this
 * Lambda) must produce identical Dub payloads — externalId, title,
 * description, and UTM block. If you change one copy, change the other
 * or externalId collisions and drifted metadata will follow.
 *
 * Keep in sync: picasso-webscraping/rag-scraper/lib/dub.mjs
 */

export const DUB_API_URL = 'https://api.dub.co';
export const RATE_LIMIT_DELAY = 250;

export const CONVERSION_RULES = [
  { category: 'donation',     test: url => /neoncrm\.com\/forms\/(donate|angel-alliance|waitlist)/i.test(url) },
  { category: 'payment',      test: url => /neoncrm\.com\/forms\/(.*fee|61$)/i.test(url) },
  { category: 'registration', test: url => /neoncrm\.com\/(.*eventRegistration|forms\/.*(?:volunteer-sign-up|campaign))/i.test(url) },
  { category: 'application',  test: url => /socialsolutionsportal\.com\/apricot-intake\//i.test(url) },
  { category: 'application',  test: url => /socialsolutionsportal\.com\/login/i.test(url) },
  { category: 'registration', test: url => /calendly\.com/i.test(url) },
  { category: 'registration', test: url => /calendar\.app\.google/i.test(url) },
  { category: 'partnership',  test: url => /\/partnertoday\b/i.test(url) },
];

export const CATEGORY_LABELS = {
  donation:     { verb: 'Donate',   noun: 'donation form' },
  application:  { verb: 'Apply',    noun: 'application form' },
  registration: { verb: 'Register', noun: 'registration' },
  payment:      { verb: 'Pay',      noun: 'payment form' },
  partnership:  { verb: 'Partner',  noun: 'partnership inquiry' },
};

export function isConversionLink(url) {
  return CONVERSION_RULES.some(rule => rule.test(url));
}

export function categorizeUrl(url) {
  for (const rule of CONVERSION_RULES) {
    if (rule.test(url)) return rule.category;
  }
  return null;
}

export function slugify(text) {
  return String(text)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/(^-|-$)/g, '')
    .substring(0, 40);
}

export function hashUrl(url) {
  let h = 0;
  for (let i = 0; i < url.length; i++) {
    h = ((h << 5) - h + url.charCodeAt(i)) | 0;
  }
  return Math.abs(h).toString(36).substring(0, 6);
}

export function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

export function buildPayload(link, tenantId, tag, domain, folderId, orgName, imageUrl) {
  const catLabel = CATEGORY_LABELS[link.category] || { verb: 'Visit', noun: 'link' };
  const org = orgName || tenantId;
  const sections = link.sections || [];

  const title = orgName
    ? `${link.label} — ${org}`
    : link.label;

  const description = orgName
    ? `${catLabel.verb} with ${org}. ${link.label}${sections[0] ? ` — ${sections[0]}` : ''}.`
    : `${org} ${catLabel.noun}${sections[0] ? ` (${sections[0]})` : ''}`;

  const payload = {
    url: link.url,
    domain,
    externalId: `${tag}:${link.category}:${slugify(link.label)}:${hashUrl(link.url)}`,
    tagNames: [tag],
    proxy: true,
    title,
    description,
    utm_source: 'webchat',
    utm_medium: 'chat',
    utm_campaign: link.category,
    utm_content: sections[0] ? slugify(sections[0]) : 'general',
    trackConversion: false,
    comments: `${link.label} — ${org} ${catLabel.noun}. Tracked via MyRecruiter webchat.`,
  };
  if (folderId) payload.folderId = folderId;
  if (imageUrl) payload.image = imageUrl;
  return payload;
}

export async function dubUpsert(apiKey, payload) {
  const res = await fetch(`${DUB_API_URL}/links/upsert`, {
    method: 'PUT',
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  if (!res.ok) {
    const body = await res.text();
    throw new Error(`Dub API ${res.status}: ${body}`);
  }

  return res.json();
}
