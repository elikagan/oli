// OLI — Object Lesson Intelligence
// Artist Prospecting CRM — Cloudflare Worker API
// Scrapes past auction results, identifies living artists 65+, manages outreach pipeline

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (request.method === 'OPTIONS')
      return new Response(null, { status: 204, headers: cors(request) });

    try {
      const p = url.pathname;
      const m = request.method;

      // Artists
      if (p === '/artists' && m === 'GET') return getArtists(request, url, env);
      if (p === '/artists' && m === 'PATCH') return patchArtist(request, env);
      if (p.match(/^\/artists\/\d+$/) && m === 'GET') return getArtist(request, url, env);
      if (p.match(/^\/artists\/\d+$/) && m === 'DELETE') return deleteArtist(request, url, env);
      if (p.match(/^\/artists\/\d+\/lots$/) && m === 'GET') return getArtistLots(request, url, env);

      // Pipeline
      if (p === '/pipeline/stats' && m === 'GET') return getPipelineStats(request, env);

      // Scraping & Import
      if (p === '/scrape' && m === 'POST') return scrapeHouse(request, env);
      if (p === '/scrape/all' && m === 'POST') return scrapeAllHouses(request, env);
      if (p === '/extract-artists' && m === 'POST') return extractArtists(request, env);
      if (p === '/link-lots' && m === 'POST') return linkLotsToArtists(request, env);

      // Research
      if (p === '/research' && m === 'POST') return researchArtist(request, env);
      if (p === '/research/batch' && m === 'POST') return researchBatch(request, env);

      // Email
      if (p === '/email/generate' && m === 'POST') return generateEmail(request, env);
      if (p === '/email/send' && m === 'POST') return sendEmail(request, env);
      if (p === '/outreach' && m === 'GET') return getOutreach(request, url, env);

      // Lots
      if (p === '/lots' && m === 'GET') return getLots(request, url, env);

      // Debug
      if (p === '/debug/scrape-test' && m === 'GET') return debugScrapeTest(request, env);

      return new Response('Not found', { status: 404, headers: cors(request) });
    } catch (e) {
      console.error('Worker error:', e);
      return json({ error: e.message }, 500, request);
    }
  },

  // Cron: scrape completed auctions from houses in rotation
  async scheduled(event, env, ctx) {
    ctx.waitUntil((async () => {
      try {
        // 1. Scrape one house per run (rotates by hour)
        const houses = Object.entries(TARGET_HOUSES);
        const hour = new Date().getUTCHours();
        const idx = Math.floor(hour / 4) % houses.length;
        const [sellerId, name] = houses[idx];
        const lots = await scrapeCompletedAuctions(env, parseInt(sellerId), name);
        console.log(`[OLI] Cron scraped ${name}: ${lots} lots`);

        // 2. Extract artists from unprocessed lots (2 batches of 20)
        for (let i = 0; i < 2; i++) {
          const lotsRes = await supa(env, 'oli_lots?artist_name=is.null&select=id,title,auction_house&limit=20');
          const untagged = await lotsRes.json();
          if (!Array.isArray(untagged) || !untagged.length) break;
          const titles = untagged.map((l, j) => `${j}: ${l.title}`).join('\n');
          const prompt = `You are analyzing auction lot titles to extract artist/designer/maker names.
For each numbered title, extract the artist or designer name if one is clearly attributable.
Return ONLY a JSON array: [{"idx": 0, "name": "Full Name"}, ...]
Skip generic descriptions. Skip "attributed to", "manner of", "after", "school of".
Clean names: proper case, no dates, no suffixes.\n\nTitles:\n${titles}`;
          const raw = await gemini(env, prompt, { maxTokens: 8192 });
          if (!raw) continue;
          const cleaned = raw.replace(/```json\s*/g, '').replace(/```\s*/g, '');
          const jsonStr = cleaned.match(/\[[\s\S]*\]/)?.[0];
          if (!jsonStr) continue;
          try {
            const extracted = JSON.parse(jsonStr);
            for (const { idx: ei, name: ename } of extracted) {
              if (ei >= untagged.length || !ename) continue;
              await supa(env, `oli_lots?id=eq.${untagged[ei].id}`, { method: 'PATCH', body: JSON.stringify({ artist_name: ename }) });
            }
            const names = [...new Set(extracted.map(e => e.name).filter(Boolean))];
            for (const n of names) {
              const aLots = untagged.filter((l, j) => extracted.find(e => e.idx === j && e.name === n));
              await supa(env, 'oli_artists', {
                method: 'POST', body: JSON.stringify({ name: n, auction_houses: [...new Set(aLots.map(l => l.auction_house))] }),
                headers: { 'Prefer': 'return=minimal,resolution=ignore-duplicates' }
              });
            }
          } catch (e) { console.error('[OLI] Extract parse error:', e.message); }
        }

        // 3. Research 3 unresearched artists
        const fakeReq = new Request('http://localhost/research/batch', {
          method: 'POST', body: JSON.stringify({ limit: 3 }),
          headers: { 'Content-Type': 'application/json' }
        });
        await researchBatch(fakeReq, env);

        // 4. Link lots for 5 artists missing stats
        const fakeLink = new Request('http://localhost/link-lots', {
          method: 'POST', headers: { 'Content-Type': 'application/json' }
        });
        await linkLotsToArtists(fakeLink, env);

        console.log('[OLI] Cron pipeline complete');
      } catch (e) {
        console.error('[OLI] Cron error:', e);
      }
    })());
  }
};

// ── Config ────────────────────────────────────────────────

const TARGET_HOUSES = {
  3822: 'Billings',
  8902: 'Circa',
  237:  'LAMA',
  369:  'Wright',
  176:  'Rago',
  390:  'Uniques and Antiques'
};

const LA_SEARCH_URL = 'https://search-party-prod.liveauctioneers.com/search/v4/web';
const LA_IMAGE_BASE = 'https://p1.liveauctioneers.com';

// Seller page slugs for catalog discovery
const HOUSE_SLUGS = {
  3822: 'billings-auction-gallery',
  8902: 'circa-auction-gallery',
  237:  'los-angeles-modern-auctions',
  369:  'wright',
  176:  'rago-arts-and-auction-center',
  390:  'uniques-and-antiques-inc'
};

// ── Helpers ───────────────────────────────────────────────

function cors(request) {
  const origin = request?.headers?.get('Origin') || '*';
  return {
    'Access-Control-Allow-Origin': origin,
    'Access-Control-Allow-Methods': 'GET, POST, DELETE, PATCH, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type'
  };
}

function json(data, status = 200, request) {
  return new Response(JSON.stringify(data), {
    status,
    headers: { ...cors(request), 'Content-Type': 'application/json' }
  });
}

function supa(env, path, opts = {}) {
  return fetch(`${env.SUPABASE_URL}/rest/v1/${path}`, {
    ...opts,
    headers: {
      'apikey': env.SUPABASE_SERVICE_KEY,
      'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      'Content-Type': 'application/json',
      'Prefer': opts.prefer || 'return=representation',
      ...(opts.headers || {})
    }
  });
}

async function gemini(env, prompt, opts = {}) {
  const model = opts.model || 'gemini-2.5-flash';
  const url = `https://generativelanguage.googleapis.com/v1beta/models/${model}:generateContent?key=${env.GEMINI_KEY}`;
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      contents: [{ parts: [{ text: prompt }] }],
      generationConfig: { temperature: opts.temperature || 0.3, maxOutputTokens: opts.maxTokens || 4096 }
    })
  });
  const data = await res.json();
  const text = data?.candidates?.[0]?.content?.parts?.[0]?.text;
  if (!text && opts._debug) return { error: true, status: res.status, body: JSON.stringify(data).substring(0, 500) };
  return text || '';
}

// ── Scraping: LiveAuctioneers ─────────────────────────────
// Strategy:
// 1. Get catalog IDs from seller page HTML
// 2. Get item IDs from catalog page HTML
// 3. Batch-fetch full item details via /content/items API

const LA_HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
  'Origin': 'https://www.liveauctioneers.com',
  'Referer': 'https://www.liveauctioneers.com/'
};

const LA_CONTENT_URL = 'https://www.liveauctioneers.com/content/items';

// Get catalog IDs from a seller's page
async function getSellerCatalogs(sellerId, slug) {
  const url = `https://www.liveauctioneers.com/auctioneer/${sellerId}/${slug}/`;
  const res = await fetch(url, { headers: LA_HEADERS });
  if (!res.ok) return [];
  const html = await res.text();
  const matches = html.match(/"catalogId":(\d+)/g) || [];
  const ids = [...new Set(matches.map(m => parseInt(m.split(':')[1])).filter(id => id > 0))];
  return ids.sort((a, b) => b - a); // newest first
}

// Get item IDs from a catalog page. Returns { itemIds, isDone }
async function getCatalogItemIds(catalogId) {
  const url = `https://www.liveauctioneers.com/catalog/${catalogId}/`;
  const res = await fetch(url, { headers: LA_HEADERS });
  if (!res.ok) return { itemIds: [], isDone: false };
  const html = await res.text();

  // Check if catalog is completed
  const statusMatch = html.match(/"catalogStatus":"([^"]+)"/);
  const isDone = statusMatch?.[1] === 'done';
  if (!isDone) return { itemIds: [], isDone: false };

  // Extract item IDs from HTML (works from CF even through Incapsula)
  const idMatches = html.match(/"itemId":(\d+)/g) || [];
  const ids = [...new Set(idMatches.map(m => parseInt(m.split(':')[1])).filter(id => id > 1000))];
  if (!ids.length) return { itemIds: [], isDone: true };

  const minId = Math.min(...ids);

  // Extract total lot count
  const lotsMatch = html.match(/"lotsListed":(\d+)/);
  const totalLots = lotsMatch ? parseInt(lotsMatch[1]) : (Math.max(...ids) - minId + 50);

  // Generate sequential IDs from min
  const result = [];
  for (let i = 0; i < Math.min(totalLots + 10, 500); i++) {
    result.push(minId + i);
  }
  return { itemIds: result, isDone: true };
}

// Fetch item details in batches via /content/items API
async function fetchItemDetails(itemIds) {
  const items = [];
  // Batch in groups of 50 (API limit)
  for (let i = 0; i < itemIds.length; i += 50) {
    const batch = itemIds.slice(i, i + 50);
    const lotIds = batch.join(',');
    const url = `${LA_CONTENT_URL}?c=20170802&identifier=catalog-cover-items-for-quickload&liveStateFetch=false&lotIds=${lotIds}`;
    const res = await fetch(url, { headers: LA_HEADERS });
    if (!res.ok) continue;
    const data = await res.json();
    const batchItems = data?.payload?.items || [];
    items.push(...batchItems);
  }
  return items;
}

// Scrape lots from a single catalog
async function scrapeCatalog(env, catalogId, houseName) {
  const { itemIds, isDone } = await getCatalogItemIds(catalogId);
  if (!isDone || !itemIds.length) return -1; // -1 = not done, 0 = done but empty

  const items = await fetchItemDetails(itemIds);
  if (!items.length) return 0;

  let totalNew = 0;
  const lots = items
    .filter(r => r.isSold && (r.salePrice > 0 || r.leadingBid > 0))
    .map(r => {
      const photos = r.photos || [1];
      const imgBase = `${LA_IMAGE_BASE}/${r.sellerId}/${r.catalogId}/${r.itemId}`;
      return {
        title: r.title || '',
        lot_number: r.lotNumber?.toString() || null,
        auction_house: houseName,
        sale_name: r.catalogTitle || null,
        sale_date: r.saleStartTs
          ? new Date(r.saleStartTs * 1000).toISOString().split('T')[0]
          : null,
        hammer_price: r.salePrice || r.leadingBid || null,
        currency: r.currency || 'USD',
        image_urls: photos.slice(0, 6).map(p => `${imgBase}_${p}_x.jpg`),
        la_lot_id: r.itemId?.toString() || null,
        la_catalog_id: catalogId.toString(),
        source_url: `https://www.liveauctioneers.com/item/${r.itemId}`
      };
    });

  if (lots.length > 0) {
    for (let i = 0; i < lots.length; i += 50) {
      const chunk = lots.slice(i, i + 50);
      const upsertRes = await supa(env, 'oli_lots', {
        method: 'POST',
        body: JSON.stringify(chunk),
        headers: { 'Prefer': 'return=minimal,resolution=merge-duplicates' }
      });
      if (upsertRes.ok) totalNew += chunk.length;
    }
  }

  return totalNew;
}

// Scrape a house: get catalogs, then scrape each
async function scrapeCompletedAuctions(env, sellerId, houseName, maxCatalogs = 3) {
  const slug = HOUSE_SLUGS[sellerId] || '';
  if (!slug) return 0;

  const catalogs = await getSellerCatalogs(sellerId, slug);
  if (!catalogs.length) return 0;

  let total = 0;
  let scraped = 0;
  for (const catId of catalogs) {
    if (scraped >= maxCatalogs) break;
    const count = await scrapeCatalog(env, catId, houseName);
    if (count === -1) continue; // skip non-done catalogs
    total += count;
    scraped++;
  }
  return total;
}

// ── POST /scrape — Scrape a specific house ───────────────

async function scrapeHouse(request, env) {
  const { seller_id, house_name, pages } = await request.json();
  const sid = seller_id || Object.keys(TARGET_HOUSES)[0];
  const name = house_name || TARGET_HOUSES[sid] || 'Unknown';

  const slug = HOUSE_SLUGS[parseInt(sid)] || '';
  const catalogs = await getSellerCatalogs(parseInt(sid), slug);

  const details = { catalogs: catalogs.length };
  let total = 0;
  let scraped = 0;

  for (const catId of catalogs) {
    if (scraped >= (pages || 3)) break;
    const { itemIds, isDone } = await getCatalogItemIds(catId);
    if (!isDone) { details[`cat_${catId}`] = 'not_done'; continue; }
    if (!itemIds.length) { details[`cat_${catId}`] = 'no_items'; continue; }

    details[`cat_${catId}`] = `${itemIds.length} ids`;
    const items = await fetchItemDetails(itemIds);
    details[`cat_${catId}_fetched`] = items.length;

    const sold = items.filter(r => r.isSold && (r.salePrice > 0 || r.leadingBid > 0));
    details[`cat_${catId}_sold`] = sold.length;

    if (sold.length > 0) {
      const lots = sold.map(r => {
        const photos = r.photos || [1];
        const imgBase = `${LA_IMAGE_BASE}/${r.sellerId}/${r.catalogId}/${r.itemId}`;
        return {
          title: r.title || '',
          lot_number: r.lotNumber?.toString() || null,
          auction_house: name,
          sale_name: r.catalogTitle || null,
          sale_date: r.saleStartTs ? new Date(r.saleStartTs * 1000).toISOString().split('T')[0] : null,
          hammer_price: r.salePrice || r.leadingBid || null,
          currency: r.currency || 'USD',
          image_urls: photos.slice(0, 6).map(p => `${imgBase}_${p}_x.jpg`),
          la_lot_id: r.itemId?.toString() || null,
          la_catalog_id: catId.toString(),
          source_url: `https://www.liveauctioneers.com/item/${r.itemId}`
        };
      });

      for (let i = 0; i < lots.length; i += 50) {
        const chunk = lots.slice(i, i + 50);
        const upsertRes = await supa(env, 'oli_lots', {
          method: 'POST',
          body: JSON.stringify(chunk),
          headers: { 'Prefer': 'return=minimal,resolution=merge-duplicates' }
        });
        if (upsertRes.ok) {
          total += chunk.length;
        } else {
          const err = await upsertRes.text();
          details[`cat_${catId}_db_error`] = err.substring(0, 200);
        }
      }
    }
    scraped++;
  }

  return json({ ok: true, house: name, lots_imported: total, details }, 200, request);
}

// ── POST /scrape/all — Scrape all houses ─────────────────

async function scrapeAllHouses(request, env) {
  const results = {};
  // Process sequentially to stay within subrequest limits
  for (const [sid, name] of Object.entries(TARGET_HOUSES)) {
    try {
      const count = await scrapeCompletedAuctions(env, parseInt(sid), name, 3);
      results[name] = count;
    } catch (e) {
      results[name] = `error: ${e.message}`;
    }
  }
  return json({ ok: true, results }, 200, request);
}

// ── POST /extract-artists — Use Gemini to extract artist names from lots ──

async function extractArtists(request, env) {
  try {
  // Get lots without artist_name
  const lotsRes = await supa(env, 'oli_lots?artist_name=is.null&select=id,title,auction_house&limit=20');
  const lots = await lotsRes.json();
  if (!Array.isArray(lots)) return json({ ok: false, error: 'lots_query_failed', detail: JSON.stringify(lots).substring(0, 300) }, 200, request);
  if (!lots.length) return json({ ok: true, extracted: 0, message: 'no lots without artist_name' }, 200, request);

  // Batch titles for Gemini
  const titles = lots.map((l, i) => `${i}: ${l.title}`).join('\n');

  const prompt = `You are analyzing auction lot titles to extract artist/designer/maker names.
For each numbered title, extract the artist or designer name if one is clearly attributable.
Return ONLY a JSON array of objects: [{"idx": 0, "name": "Full Name"}, ...]
Skip titles that are generic descriptions with no clear maker (e.g. "Antique oak table", "Lot of pottery").
Skip "attributed to", "manner of", "after", "school of" — we need real names.
Clean up names: proper capitalization, no dates, no "(American, 1920-2005)" suffixes.

Titles:
${titles}`;

  const raw = await gemini(env, prompt, { maxTokens: 8192, _debug: true });
  if (raw?.error) return json({ ok: false, error: 'gemini_failed', detail: raw }, 200, request);
  if (!raw) return json({ ok: false, error: 'gemini_empty', lot_count: lots.length }, 200, request);

  let extracted = [];
  const cleaned = raw.replace(/```json\s*/g, '').replace(/```\s*/g, '');
  const jsonStr = cleaned.match(/\[[\s\S]*\]/)?.[0];
  if (!jsonStr) {
    return json({ ok: true, extracted: 0, error: 'no_json_found', raw_preview: raw.substring(0, 500) }, 200, request);
  }
  try {
    extracted = JSON.parse(jsonStr);
  } catch (e) { return json({ ok: true, extracted: 0, error: 'parse_failed', message: e.message, raw_preview: raw.substring(0, 500) }, 200, request); }

  // Update lots with artist names
  let updated = 0;
  for (const { idx, name } of extracted) {
    if (idx >= lots.length || !name) continue;
    const lot = lots[idx];
    await supa(env, `oli_lots?id=eq.${lot.id}`, {
      method: 'PATCH',
      body: JSON.stringify({ artist_name: name })
    });
    updated++;
  }

  // Upsert new artists into oli_artists
  const uniqueNames = [...new Set(extracted.map(e => e.name).filter(Boolean))];
  for (const name of uniqueNames) {
    // Get house info for this artist
    const artistLots = lots.filter((l, i) => extracted.find(e => e.idx === i && e.name === name));
    const houses = [...new Set(artistLots.map(l => l.auction_house))];

    await supa(env, 'oli_artists', {
      method: 'POST',
      body: JSON.stringify({ name, auction_houses: houses }),
      prefer: 'return=minimal,resolution=ignore-duplicates',
      headers: { 'Prefer': 'return=minimal,resolution=ignore-duplicates' }
    });
  }

  return json({ ok: true, extracted: updated, new_artists: uniqueNames.length }, 200, request);
  } catch (err) {
    return json({ ok: false, error: 'extract_exception', message: err.message, stack: err.stack?.substring(0, 300) }, 500, request);
  }
}

// ── POST /link-lots — Link lots to artists by name + update artist stats ──

async function linkLotsToArtists(request, env) {
  // Get artists that need stats (lot_count is null or 0)
  const aRes = await supa(env, 'oli_artists?or=(lot_count.is.null,lot_count.eq.0)&select=id,name&limit=5');
  const artists = await aRes.json();
  if (!Array.isArray(artists)) return json({ ok: false, error: 'artists_query_failed' }, 200, request);
  if (!artists.length) return json({ ok: true, message: 'all artists linked', remaining: 0 }, 200, request);

  let linked = 0, statsUpdated = 0;
  for (const a of artists) {
    const lotsRes = await supa(env, `oli_lots?artist_name=eq.${encodeURIComponent(a.name)}&select=id,hammer_price,auction_house,image_urls`);
    const lots = await lotsRes.json();
    if (!Array.isArray(lots) || !lots.length) {
      // No lots found — set lot_count to -1 so we don't retry
      await supa(env, `oli_artists?id=eq.${a.id}`, { method: 'PATCH', body: JSON.stringify({ lot_count: -1 }) });
      continue;
    }

    // Link lots
    for (const lot of lots) {
      await supa(env, `oli_lots?id=eq.${lot.id}&artist_id=is.null`, {
        method: 'PATCH',
        body: JSON.stringify({ artist_id: a.id })
      });
      linked++;
    }

    // Calculate stats
    const prices = lots.filter(l => l.hammer_price).map(l => parseFloat(l.hammer_price));
    const houses = [...new Set(lots.map(l => l.auction_house).filter(Boolean))];
    const images = lots.flatMap(l => (l.image_urls || []).slice(0, 1)).slice(0, 12);
    const sorted = [...prices].sort((a, b) => a - b);

    const stats = { lot_count: lots.length, auction_houses: houses, image_urls: images };
    if (prices.length) {
      stats.max_sale = Math.max(...prices);
      stats.median_sale = sorted[Math.floor(sorted.length / 2)];
      stats.avg_sale = Math.round(prices.reduce((s, p) => s + p, 0) / prices.length);
    }

    await supa(env, `oli_artists?id=eq.${a.id}`, { method: 'PATCH', body: JSON.stringify(stats) });
    statsUpdated++;
  }

  return json({ ok: true, processed: artists.length, linked, statsUpdated }, 200, request);
}

// ── POST /research — Research a single artist ────────────

async function researchArtist(request, env) {
  const { artist_id } = await request.json();
  if (!artist_id) return json({ error: 'artist_id required' }, 400, request);

  const aRes = await supa(env, `oli_artists?id=eq.${artist_id}&select=*`);
  const artists = await aRes.json();
  if (!artists?.length) return json({ error: 'not found' }, 404, request);
  const artist = artists[0];

  // Get their lots for context
  const lotsRes = await supa(env, `oli_lots?artist_name=eq.${encodeURIComponent(artist.name)}&select=title,hammer_price,auction_house,sale_date,image_urls&order=sale_date.desc&limit=50`);
  const lots = await lotsRes.json();

  // Calculate auction stats
  const prices = lots.filter(l => l.hammer_price).map(l => parseFloat(l.hammer_price));
  const stats = {
    lot_count: lots.length,
    max_sale: prices.length ? Math.max(...prices) : null,
    median_sale: prices.length ? median(prices) : null,
    avg_sale: prices.length ? Math.round(prices.reduce((a, b) => a + b, 0) / prices.length * 100) / 100 : null,
    auction_houses: [...new Set(lots.map(l => l.auction_house))],
    categories: [],
    image_urls: lots.flatMap(l => l.image_urls || []).slice(0, 30)
  };

  // Step 1: Wikidata lookup for birth/death
  let wikidata = {};
  try {
    wikidata = await wikidataLookup(artist.name);
  } catch (e) {
    console.error('Wikidata error:', e);
  }

  // Step 2: Gemini web research
  const lotContext = lots.slice(0, 10).map(l =>
    `"${l.title}" — $${l.hammer_price || '?'} at ${l.auction_house}`
  ).join('\n');

  const researchPrompt = `Research the artist/designer "${artist.name}" for a gallery owner looking to potentially work with them.

Known auction lots:
${lotContext}

${wikidata.birthYear ? `Born: ${wikidata.birthYear}` : ''}
${wikidata.deathYear ? `Died: ${wikidata.deathYear}` : ''}

Please determine:
1. Are they alive or dead? (best guess if uncertain)
2. Estimated birth year and current age
3. Where do they live/work? (city, state/country)
4. Their website URL (if any)
5. Their Instagram handle (if any)
6. Their email (if publicly available)
7. Gallery representation (list galleries)
8. Are they "too established"? (repped by top-tier galleries like Gagosian/Pace/Hauser & Wirth, pieces regularly sell for $100k+, median over $25k)
9. What categories describe their work? (art, furniture, lighting, sculpture, ceramics, etc.)
10. A 2-3 sentence summary of who they are and their work, written for a dealer.

Return as JSON:
{
  "alive": true/false/null,
  "birth_year": number or null,
  "estimated_age": number or null,
  "death_year": number or null,
  "location": "City, State" or null,
  "website": "url" or null,
  "instagram": "@handle" or null,
  "email": "email" or null,
  "gallery_rep": ["Gallery Name", ...] or [],
  "is_too_established": true/false,
  "disqualify_reason": "reason" or null,
  "categories": ["art", "furniture", ...],
  "ai_summary": "2-3 sentence summary"
}`;

  const raw = await gemini(env, researchPrompt, { temperature: 0.2, maxTokens: 2048 });
  let research = {};
  try {
    const jsonStr = raw.match(/\{[\s\S]*\}/)?.[0];
    research = JSON.parse(jsonStr);
  } catch {
    research = { ai_summary: 'Research failed to parse' };
  }

  // Merge wikidata with Gemini results (wikidata takes precedence for dates)
  const update = {
    alive: wikidata.deathYear ? false : (research.alive ?? null),
    birth_year: wikidata.birthYear || research.birth_year || null,
    death_year: wikidata.deathYear || research.death_year || null,
    estimated_age: null,
    location: research.location || null,
    website: research.website || null,
    instagram: research.instagram || null,
    email: research.email || null,
    gallery_rep: research.gallery_rep || [],
    is_too_established: research.is_too_established || false,
    disqualify_reason: research.disqualify_reason || null,
    categories: research.categories || [],
    ai_summary: research.ai_summary || null,
    research_status: 'complete',
    wikidata_id: wikidata.id || null,
    last_researched_at: new Date().toISOString(),
    ...stats
  };

  // Calculate age
  if (update.birth_year && update.alive !== false) {
    update.estimated_age = new Date().getFullYear() - update.birth_year;
  } else if (research.estimated_age) {
    update.estimated_age = research.estimated_age;
  }

  await supa(env, `oli_artists?id=eq.${artist_id}`, {
    method: 'PATCH',
    body: JSON.stringify(update)
  });

  return json({ ok: true, artist: artist.name, ...update }, 200, request);
}

// ── POST /research/batch — Research multiple artists ─────

async function researchBatch(request, env) {
  const { limit: batchLimit } = await request.json().catch(() => ({}));
  const lim = batchLimit || 5;

  // Get unresearched artists, prioritize those with more lots
  const res = await supa(env, `oli_artists?research_status=eq.pending&alive=is.null&select=id,name&order=lot_count.desc.nullsfirst&limit=${lim}`);
  const artists = await res.json();
  if (!artists?.length) return json({ ok: true, researched: 0, message: 'none pending' }, 200, request);

  const results = [];
  for (const a of artists) {
    try {
      // Simulate a request to researchArtist
      const fakeReq = new Request('http://localhost/research', {
        method: 'POST',
        body: JSON.stringify({ artist_id: a.id }),
        headers: { 'Content-Type': 'application/json' }
      });
      const r = await researchArtist(fakeReq, env);
      const data = await r.json();
      results.push({ id: a.id, name: a.name, status: data.ok ? 'done' : 'error' });
    } catch (e) {
      results.push({ id: a.id, name: a.name, status: 'error', error: e.message });
      // Mark as failed so we don't retry endlessly
      await supa(env, `oli_artists?id=eq.${a.id}`, {
        method: 'PATCH',
        body: JSON.stringify({ research_status: 'failed' })
      });
    }
  }

  return json({ ok: true, researched: results.length, results }, 200, request);
}

// ── Wikidata SPARQL Lookup ───────────────────────────────

async function wikidataLookup(name) {
  const sparql = `
    SELECT ?person ?birthDate ?deathDate WHERE {
      ?person rdfs:label "${name}"@en .
      ?person wdt:P31 wd:Q5 .
      OPTIONAL { ?person wdt:P569 ?birthDate }
      OPTIONAL { ?person wdt:P570 ?deathDate }
    } LIMIT 1`;

  const url = `https://query.wikidata.org/sparql?query=${encodeURIComponent(sparql)}&format=json`;
  const res = await fetch(url, {
    headers: { 'User-Agent': 'OLI-ArtistResearch/1.0 (objectlesson.la)' }
  });

  if (!res.ok) return {};
  const data = await res.json();
  const binding = data?.results?.bindings?.[0];
  if (!binding) return {};

  return {
    id: binding.person?.value?.split('/').pop() || null,
    birthYear: binding.birthDate?.value ? new Date(binding.birthDate.value).getFullYear() : null,
    deathYear: binding.deathDate?.value ? new Date(binding.deathDate.value).getFullYear() : null
  };
}

// ── Email Generation ─────────────────────────────────────

async function generateEmail(request, env) {
  const { artist_id } = await request.json();
  if (!artist_id) return json({ error: 'artist_id required' }, 400, request);

  const aRes = await supa(env, `oli_artists?id=eq.${artist_id}&select=*`);
  const artists = await aRes.json();
  if (!artists?.length) return json({ error: 'not found' }, 404, request);
  const artist = artists[0];

  // Get template
  const tRes = await supa(env, 'oli_email_templates?is_default=eq.true&limit=1');
  const templates = await tRes.json();
  const template = templates?.[0];

  // Get some lots for context
  const lotsRes = await supa(env, `oli_lots?artist_name=eq.${encodeURIComponent(artist.name)}&select=title,hammer_price,auction_house&limit=10`);
  const lots = await lotsRes.json();

  const lotContext = lots.slice(0, 5).map(l =>
    `"${l.title}" at ${l.auction_house}${l.hammer_price ? ` ($${l.hammer_price})` : ''}`
  ).join(', ');

  const prompt = `Write a warm, brief, personal email from Eli Kagan (who runs Object Lesson, a vintage/art shop in Pasadena, CA with his partner Megan Gage) to ${artist.name}.

Context about the artist:
- ${artist.ai_summary || 'An artist/designer whose work has appeared at auction.'}
- Their work has appeared at: ${artist.auction_houses?.join(', ') || 'various auction houses'}
- Recent lots: ${lotContext}
- They are based in: ${artist.location || 'unknown location'}
- Categories: ${artist.categories?.join(', ') || 'art/design'}

The email should:
- Be genuinely warm and personal, not corporate
- Reference specific pieces or aspects of their work that Eli might admire
- Mention Object Lesson naturally (objectlesson.la)
- Suggest working together — consigning pieces, a small collaboration, or just connecting
- Be SHORT — 3-4 short paragraphs max, casual tone
- Sign off as "Eli Kagan" with "Object Lesson" and "objectlesson.la" below
- Do NOT include a subject line in the body

Also generate a subject line.

Return JSON: {"subject": "...", "body": "..."}`;

  const raw = await gemini(env, prompt, { temperature: 0.7, maxTokens: 2048 });
  let email = {};
  try {
    const jsonStr = raw.match(/\{[\s\S]*\}/)?.[0];
    email = JSON.parse(jsonStr);
  } catch {
    return json({ error: 'email generation failed' }, 500, request);
  }

  return json({
    ok: true,
    subject: email.subject,
    body: email.body,
    to: artist.email || null,
    from: 'eli@objectlesson.la',
    artist_name: artist.name
  }, 200, request);
}

// ── Email Sending via Resend ─────────────────────────────

async function sendEmail(request, env) {
  const { artist_id, to, subject, body, from } = await request.json();
  if (!to || !subject || !body) return json({ error: 'to, subject, body required' }, 400, request);

  const fromAddr = from || 'Eli Kagan <eli@objectlesson.la>';

  const res = await fetch('https://api.resend.com/emails', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${env.RESEND_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      from: fromAddr,
      to: [to],
      subject,
      text: body
    })
  });

  const result = await res.json();

  if (!res.ok) {
    return json({ error: 'send failed', details: result }, res.status, request);
  }

  // Log outreach
  if (artist_id) {
    await supa(env, 'oli_outreach', {
      method: 'POST',
      body: JSON.stringify({
        artist_id,
        type: 'email',
        direction: 'outbound',
        subject,
        body,
        to_address: to,
        from_address: fromAddr,
        resend_id: result.id,
        status: 'sent',
        sent_at: new Date().toISOString()
      })
    });

    // Update artist status to contacted
    await supa(env, `oli_artists?id=eq.${artist_id}&status=eq.lead`, {
      method: 'PATCH',
      body: JSON.stringify({ status: 'contacted', updated_at: new Date().toISOString() })
    });
  }

  return json({ ok: true, resend_id: result.id }, 200, request);
}

// ── GET /artists ─────────────────────────────────────────

async function getArtists(request, url, env) {
  const status = url.searchParams.get('status');
  const search = url.searchParams.get('q');
  const limit = url.searchParams.get('limit') || '50';
  const offset = url.searchParams.get('offset') || '0';
  const order = url.searchParams.get('order') || 'lot_count.desc.nullslast';

  let filter = 'select=*';
  if (status) filter += `&status=eq.${status}`;
  if (search) filter += `&name=ilike.*${encodeURIComponent(search)}*`;
  filter += `&order=${order}&limit=${limit}&offset=${offset}`;

  const res = await supa(env, `oli_artists?${filter}`, {
    prefer: 'return=representation,count=exact',
    headers: { 'Prefer': 'return=representation,count=exact' }
  });

  const artists = await res.json();
  const total = res.headers.get('content-range')?.split('/')?.[1] || null;

  return json({ artists: artists || [], total: total ? parseInt(total) : null }, 200, request);
}

// ── GET /artists/:id ─────────────────────────────────────

async function getArtist(request, url, env) {
  const id = url.pathname.split('/')[2];
  const res = await supa(env, `oli_artists?id=eq.${id}&select=*`);
  const artists = await res.json();
  if (!artists?.length) return json({ error: 'not found' }, 404, request);

  // Get outreach history
  const outRes = await supa(env, `oli_outreach?artist_id=eq.${id}&select=*&order=created_at.desc`);
  const outreach = await outRes.json();

  // Get lot count and recent lots
  const lotsRes = await supa(env, `oli_lots?artist_name=eq.${encodeURIComponent(artists[0].name)}&select=*&order=sale_date.desc.nullslast&limit=50`);
  const lots = await lotsRes.json();

  return json({
    artist: artists[0],
    outreach: outreach || [],
    lots: lots || []
  }, 200, request);
}

// ── PATCH /artists ───────────────────────────────────────

async function patchArtist(request, env) {
  const body = await request.json();
  const { id, ...updates } = body;
  if (!id) return json({ error: 'id required' }, 400, request);

  updates.updated_at = new Date().toISOString();

  const res = await supa(env, `oli_artists?id=eq.${id}`, {
    method: 'PATCH',
    body: JSON.stringify(updates)
  });

  const result = await res.json();
  return json({ ok: true, artist: result?.[0] || null }, 200, request);
}

// ── DELETE /artists/:id ──────────────────────────────────

async function deleteArtist(request, url, env) {
  const id = url.pathname.split('/')[2];
  await supa(env, `oli_artists?id=eq.${id}`, { method: 'DELETE' });
  return json({ ok: true }, 200, request);
}

// ── GET /artists/:id/lots ────────────────────────────────

async function getArtistLots(request, url, env) {
  const id = url.pathname.split('/')[2];
  const aRes = await supa(env, `oli_artists?id=eq.${id}&select=name`);
  const artists = await aRes.json();
  if (!artists?.length) return json({ error: 'not found' }, 404, request);

  const lotsRes = await supa(env, `oli_lots?artist_name=eq.${encodeURIComponent(artists[0].name)}&select=*&order=sale_date.desc.nullslast`);
  const lots = await lotsRes.json();
  return json({ lots: lots || [] }, 200, request);
}

// ── GET /lots ────────────────────────────────────────────

async function getLots(request, url, env) {
  const house = url.searchParams.get('house');
  const unassigned = url.searchParams.get('unassigned');
  const limit = url.searchParams.get('limit') || '100';

  let filter = 'select=id,title,auction_house,hammer_price,sale_date,image_urls,artist_name';
  if (house) filter += `&auction_house=eq.${encodeURIComponent(house)}`;
  if (unassigned === 'true') filter += '&artist_name=is.null';
  filter += `&order=sale_date.desc.nullslast&limit=${limit}`;

  const res = await supa(env, `oli_lots?${filter}`);
  const lots = await res.json();
  return json({ lots: lots || [] }, 200, request);
}

// ── GET /outreach ────────────────────────────────────────

async function getOutreach(request, url, env) {
  const artistId = url.searchParams.get('artist_id');
  let filter = 'select=*,oli_artists(name)&order=created_at.desc&limit=50';
  if (artistId) filter = `artist_id=eq.${artistId}&${filter}`;

  const res = await supa(env, `oli_outreach?${filter}`);
  const outreach = await res.json();
  return json({ outreach: outreach || [] }, 200, request);
}

// ── GET /pipeline/stats ──────────────────────────────────

async function getPipelineStats(request, env) {
  try {
    const res = await supa(env, 'oli_artists?select=status');
    const artistsRaw = await res.json();
    const artists = Array.isArray(artistsRaw) ? artistsRaw : [];

    const counts = {};
    for (const a of artists) {
      counts[a.status] = (counts[a.status] || 0) + 1;
    }

    return json({
      pipeline: counts,
      total_artists: artists.length,
      total_lots: 0,
      outreach: {},
      total_outreach: 0
    }, 200, request);
  } catch (e) {
    return json({ error: e.message, pipeline: {}, total_artists: 0 }, 200, request);
  }
}

// ── Utility ──────────────────────────────────────────────

// ── Debug ────────────────────────────────────────────────

async function debugScrapeTest(request, env) {
  const catalogs = await getSellerCatalogs(237, 'los-angeles-modern-auctions');

  // Test the /content/items API directly with known LAMA item IDs
  const testIds = '211402805,211402806,211402807';
  const contentRes = await fetch(`${LA_CONTENT_URL}?c=20170802&identifier=catalog-cover-items-for-quickload&liveStateFetch=false&lotIds=${testIds}`, {
    headers: LA_HEADERS
  });
  const contentText = await contentRes.text();
  let contentData = null;
  try { contentData = JSON.parse(contentText); } catch {}

  // Also check what metadata we can get from a completed catalog page
  let catDebug = {};
  for (const c of catalogs.slice(0, 5)) {
    const res = await fetch(`https://www.liveauctioneers.com/catalog/${c}/`, { headers: LA_HEADERS });
    const html = await res.text();
    const statusMatch = html.match(/"catalogStatus":"([^"]+)"/);
    if (statusMatch?.[1] === 'done') {
      const lotsMatch = html.match(/"lotsListed":(\d+)/);
      const itemIdMatches = html.match(/"itemId":(\d+)/g) || [];
      const itemIds = [...new Set(itemIdMatches.map(m => parseInt(m.split(':')[1])).filter(id => id > 1000))];
      catDebug = {
        catalogId: c,
        lotsListed: lotsMatch ? parseInt(lotsMatch[1]) : null,
        itemIdsFromHtml: itemIds.slice(0, 5),
        totalItemIds: itemIds.length
      };
      break;
    }
  }

  return json({
    catalogs_found: catalogs.length,
    content_api_status: contentRes.status,
    content_api_items: contentData?.payload?.items?.length || 0,
    content_api_sample: contentData?.payload?.items?.slice(0, 2)?.map(r => ({
      title: r.title, price: r.salePrice, sold: r.isSold
    })) || [],
    catalog_debug: catDebug
  }, 200, request);
}

function median(arr) {
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}
