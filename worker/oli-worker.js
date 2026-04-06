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

      // Research
      if (p === '/research' && m === 'POST') return researchArtist(request, env);
      if (p === '/research/batch' && m === 'POST') return researchBatch(request, env);

      // Email
      if (p === '/email/generate' && m === 'POST') return generateEmail(request, env);
      if (p === '/email/send' && m === 'POST') return sendEmail(request, env);
      if (p === '/outreach' && m === 'GET') return getOutreach(request, url, env);

      // Lots
      if (p === '/lots' && m === 'GET') return getLots(request, url, env);

      return new Response('Not found', { status: 404, headers: cors(request) });
    } catch (e) {
      console.error('Worker error:', e);
      return json({ error: e.message }, 500, request);
    }
  },

  // Cron: scrape completed auctions from houses in rotation
  async scheduled(event, env, ctx) {
    const houses = Object.entries(TARGET_HOUSES);
    const hour = new Date().getUTCHours();
    const idx = Math.floor(hour / 4) % houses.length;
    const [sellerId, name] = houses[idx];
    ctx.waitUntil(
      scrapeCompletedAuctions(env, parseInt(sellerId), name)
        .then(n => console.log(`[OLI] Cron scraped ${name}: ${n} lots`))
        .catch(e => console.error(`[OLI] Cron ${name}:`, e))
    );
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

// Algolia price results (29M+ records)
const ALGOLIA_APP = 'NR5KEURV76';
const ALGOLIA_KEY = '8a1358d26d1fbcdf18d06f7c5f7b5c47';
const ALGOLIA_URL = `https://${ALGOLIA_APP}-dsn.algolia.net/1/indexes/price_results/query`;

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
  const model = opts.model || 'gemini-2.0-flash';
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
  return data?.candidates?.[0]?.content?.parts?.[0]?.text || '';
}

// ── Scraping: LiveAuctioneers Completed Results ──────────

async function scrapeCompletedAuctions(env, sellerId, houseName, maxPages = 3) {
  let totalNew = 0;

  for (let page = 1; page <= maxPages; page++) {
    const body = {
      keyword: '',
      page,
      pageSize: 60,
      sort: '-timems',
      status: ['completed'],
      seller: [sellerId],
      priceResult: true
    };

    const res = await fetch(LA_SEARCH_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    });

    if (!res.ok) {
      console.error(`LA search failed: ${res.status}`);
      break;
    }

    const data = await res.json();
    const records = data?.records || [];
    if (records.length === 0) break;

    // Build lot records
    const lots = records
      .filter(r => r.priceResult && r.currentBid > 0)
      .map(r => ({
        title: r.title || '',
        lot_number: r.lotNumber?.toString() || null,
        auction_house: houseName,
        sale_name: r.auctionTitle || null,
        sale_date: r.endDate ? new Date(r.endDate).toISOString().split('T')[0] : null,
        hammer_price: r.currentBid || null,
        currency: r.currencyCode || 'USD',
        image_urls: r.images?.length
          ? r.images.slice(0, 6).map(img => `${LA_IMAGE_BASE}/${img}`)
          : [],
        la_lot_id: r.lotId?.toString() || null,
        la_catalog_id: r.catalogId?.toString() || null,
        source_url: r.lotId ? `https://www.liveauctioneers.com/item/${r.lotId}` : null
      }));

    if (lots.length === 0) break;

    // Upsert (skip duplicates by la_lot_id)
    const upsertRes = await supa(env, 'oli_lots', {
      method: 'POST',
      body: JSON.stringify(lots),
      prefer: 'return=representation,resolution=merge-duplicates',
      headers: { 'Prefer': 'return=representation,resolution=merge-duplicates' }
    });

    const inserted = await upsertRes.json();
    totalNew += Array.isArray(inserted) ? inserted.length : 0;

    if (records.length < 60) break; // Last page
  }

  return totalNew;
}

// ── POST /scrape — Scrape a specific house ───────────────

async function scrapeHouse(request, env) {
  const { seller_id, house_name, pages } = await request.json();
  const sid = seller_id || Object.keys(TARGET_HOUSES)[0];
  const name = house_name || TARGET_HOUSES[sid] || 'Unknown';
  const count = await scrapeCompletedAuctions(env, parseInt(sid), name, pages || 5);
  return json({ ok: true, house: name, lots_imported: count }, 200, request);
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
  // Get lots without artist_name
  const lotsRes = await supa(env, 'oli_lots?artist_name=is.null&select=id,title,auction_house&limit=100');
  const lots = await lotsRes.json();
  if (!lots?.length) return json({ ok: true, extracted: 0 }, 200, request);

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

  const raw = await gemini(env, prompt, { maxTokens: 8192 });
  let extracted = [];
  try {
    const jsonStr = raw.match(/\[[\s\S]*\]/)?.[0];
    extracted = JSON.parse(jsonStr);
  } catch { return json({ ok: true, extracted: 0, error: 'parse_failed' }, 200, request); }

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
  // Get counts by status
  const res = await supa(env, 'oli_artists?select=status', { prefer: 'return=representation' });
  const artists = await res.json();

  const counts = {};
  for (const a of (artists || [])) {
    counts[a.status] = (counts[a.status] || 0) + 1;
  }

  // Get total lots
  const lotsRes = await supa(env, 'oli_lots?select=id', {
    prefer: 'return=representation,count=exact',
    headers: { 'Prefer': 'return=representation,count=exact,head=true' }
  });
  const lotTotal = lotsRes.headers.get('content-range')?.split('/')?.[1] || '0';

  // Get outreach stats
  const outRes = await supa(env, 'oli_outreach?select=status');
  const outreach = await outRes.json();
  const outCounts = {};
  for (const o of (outreach || [])) {
    outCounts[o.status] = (outCounts[o.status] || 0) + 1;
  }

  return json({
    pipeline: counts,
    total_artists: artists?.length || 0,
    total_lots: parseInt(lotTotal),
    outreach: outCounts,
    total_outreach: outreach?.length || 0
  }, 200, request);
}

// ── Utility ──────────────────────────────────────────────

function median(arr) {
  const sorted = [...arr].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}
