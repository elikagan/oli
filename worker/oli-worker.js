// OLI — Object Lesson Intelligence
// Cloudflare Worker: scraping, AI processing, taste ranking, API

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: corsHeaders(request) });
    }

    try {
      // Route requests
      if (url.pathname === '/feed' && request.method === 'GET')
        return handleFeed(request, url, env);

      if (url.pathname === '/swipe' && request.method === 'POST')
        return handleSwipe(request, env);

      if (url.pathname === '/favorites' && request.method === 'GET')
        return handleGetFavorites(request, env);

      if (url.pathname === '/favorites' && request.method === 'POST')
        return handleAddFavorite(request, env);

      if (url.pathname.startsWith('/favorites/') && request.method === 'DELETE')
        return handleDeleteFavorite(request, url, env);

      if (url.pathname === '/scrape' && request.method === 'POST')
        return handleScrape(request, env);

      if (url.pathname === '/process' && request.method === 'POST')
        return handleProcess(request, env);

      if (url.pathname === '/debug-process' && request.method === 'POST')
        return handleDebugProcess(request, env);

      if (url.pathname === '/stats' && request.method === 'GET')
        return handleStats(request, env);

      if (url.pathname === '/fix-houses' && request.method === 'POST')
        return handleFixHouses(request, env);

      return new Response('Not found', { status: 404, headers: corsHeaders(request) });
    } catch (e) {
      console.error('Worker error:', e);
      return json({ error: e.message }, 500, request);
    }
  },

  // Cron trigger — scrape one house per run (rotates through them)
  async scheduled(event, env, ctx) {
    const sellerIds = Object.keys(LA_SELLERS);
    // Use hour of day to rotate which house gets scraped
    const hour = new Date().getUTCHours();
    const idx = Math.floor(hour / 4) % sellerIds.length; // 6 cron runs/day, rotate through houses
    const sellerId = parseInt(sellerIds[idx]);
    const houseName = LA_SELLERS[sellerId];
    ctx.waitUntil(
      scrapeSellerListings(env, sellerId, houseName)
        .then(count => console.log(`[OLI] Cron scraped ${houseName}: ${count} new`))
        .catch(e => console.error(`[OLI] Cron failed for ${houseName}:`, e))
    );
  }
};

// ── CORS ──────────────────────────────────────────────────

function corsHeaders(request) {
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
    headers: { ...corsHeaders(request), 'Content-Type': 'application/json' }
  });
}

// ── Supabase Helpers ──────────────────────────────────────

function supa(env, path, opts = {}) {
  const url = `${env.SUPABASE_URL}/rest/v1/${path}`;
  return fetch(url, {
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

function supaRpc(env, fnName, params = {}) {
  const url = `${env.SUPABASE_URL}/rest/v1/rpc/${fnName}`;
  return fetch(url, {
    method: 'POST',
    headers: {
      'apikey': env.SUPABASE_SERVICE_KEY,
      'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(params)
  });
}

// ── GET /feed ─────────────────────────────────────────────
// Returns ranked listings for the swipe UI

async function handleFeed(request, url, env) {
  const limit = parseInt(url.searchParams.get('limit') || '20');
  const excludeRaw = url.searchParams.get('exclude') || '';
  const clientExcludeIds = excludeRaw ? excludeRaw.split(',').filter(Boolean) : [];

  // Fetch all previously-swiped listing IDs from server (dedupes across devices)
  const swipedRes = await supa(env, 'swipes?select=listing_id');
  const swipedRows = await swipedRes.json();
  const serverSwipedIds = (Array.isArray(swipedRows) ? swipedRows : []).map(r => r.listing_id);

  // Merge client + server exclude lists
  const excludeIds = [...new Set([...clientExcludeIds, ...serverSwipedIds])];

  // Get taste profile
  const profileRes = await supa(env, 'taste_profile?id=eq.1&select=*');
  const profiles = await profileRes.json();
  const profile = profiles[0];

  let listings;

  const selectFields = 'id,platform,platform_id,title,description,price,location,url,hero_image,image_urls,auction_house,auction_date,lot_number,ai_description';
  const excludeFilter = excludeIds.length > 0
    ? `&id=not.in.(${excludeIds.join(',')})`
    : '';

  if (profile && profile.positive_centroid && profile.positive_count >= 10) {
    // ── Ranked mode: use taste model ──
    const rankedCount = Math.ceil(limit * 0.8);
    const randomCount = limit - rankedCount;

    const matchRes = await supaRpc(env, 'match_listings', {
      query_embedding: profile.positive_centroid,
      match_count: rankedCount,
      exclude_ids: excludeIds
    });
    const ranked = await matchRes.json();

    // Random exploration (exclude ranked IDs too)
    const rankedIds = (Array.isArray(ranked) ? ranked : []).map(r => r.id);
    const allExcludeFilter = [...excludeIds, ...rankedIds].length > 0
      ? `&id=not.in.(${[...excludeIds, ...rankedIds].join(',')})`
      : '';
    const randomRes = await supa(env,
      `listings?status=eq.active&hero_image=not.is.null&embedding=not.is.null${allExcludeFilter}&select=${selectFields}&order=scraped_at.desc&limit=${randomCount}`
    );
    const random = await randomRes.json();

    listings = [...(Array.isArray(ranked) ? ranked : []), ...(Array.isArray(random) ? random : [])];
  } else {
    // ── Cold start: fetch a big pool and shuffle to mix auction houses ──
    const poolSize = Math.min(limit * 5, 200);
    const res = await supa(env,
      `listings?status=eq.active&hero_image=not.is.null${excludeFilter}&select=${selectFields}&order=scraped_at.desc&limit=${poolSize}`
    );
    const pool = await res.json();
    listings = shuffle(pool || []).slice(0, limit);
  }

  // Also shuffle ranked results to avoid runs of similar items
  listings = shuffle(listings || []);

  return json({ listings }, 200, request);
}

function shuffle(arr) {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

// ── POST /swipe ───────────────────────────────────────────

async function handleSwipe(request, env) {
  const { listing_id, action } = await request.json();

  if (!listing_id || !['left', 'right', 'favorite', 'super_like', 'super_hate'].includes(action)) {
    return json({ error: 'Invalid swipe' }, 400, request);
  }

  // Record swipe
  await supa(env, 'swipes', {
    method: 'POST',
    body: JSON.stringify({ listing_id, action })
  });

  // Update taste profile if listing has embedding
  const listingRes = await supa(env, `listings?id=eq.${listing_id}&select=embedding`);
  const listings = await listingRes.json();
  if (listings[0]?.embedding) {
    await updateTasteProfile(env, listings[0].embedding, action);
  }

  return json({ ok: true }, 200, request);
}

// ── Taste Profile Update ──────────────────────────────────

async function updateTasteProfile(env, embedding, action) {
  const side = (action === 'left' || action === 'super_hate') ? 'negative' : 'positive';
  const centroidKey = side + '_centroid';
  const countKey = side + '_count';

  // Super like/hate = 25x weight (nuclear options for strong taste signals)
  const weight = (action === 'super_hate' || action === 'super_like') ? 25 : 1;

  // Get current profile
  const res = await supa(env, 'taste_profile?id=eq.1&select=*');
  const profiles = await res.json();
  let profile = profiles[0];

  if (!profile) {
    const initial = {
      id: 1,
      positive_centroid: null,
      negative_centroid: null,
      positive_count: 0,
      negative_count: 0
    };
    await supa(env, 'taste_profile', {
      method: 'POST',
      body: JSON.stringify(initial)
    });
    profile = initial;
  }

  const currentCentroid = profile[centroidKey];
  const currentCount = profile[countKey] || 0;
  const newCount = currentCount + weight;

  let newCentroid;
  if (!currentCentroid) {
    newCentroid = embedding;
  } else {
    // Running weighted average: (old * count + new * weight) / (count + weight)
    newCentroid = currentCentroid.map((v, i) =>
      (v * currentCount + embedding[i] * weight) / newCount
    );
  }

  // Update profile
  await supa(env, 'taste_profile?id=eq.1', {
    method: 'PATCH',
    body: JSON.stringify({
      [centroidKey]: newCentroid,
      [countKey]: newCount,
      updated_at: new Date().toISOString()
    })
  });
}

// ── GET /favorites ────────────────────────────────────────

async function handleGetFavorites(request, env) {
  const res = await supa(env,
    'favorites?select=id,notes,status,created_at,listing:listings(id,title,description,price,location,url,hero_image,platform,auction_house,auction_date)&order=created_at.desc'
  );
  const favorites = await res.json();
  return json({ favorites: favorites || [] }, 200, request);
}

// ── POST /favorites ───────────────────────────────────────

async function handleAddFavorite(request, env) {
  const { listing_id } = await request.json();
  if (!listing_id) return json({ error: 'Missing listing_id' }, 400, request);

  const res = await supa(env, 'favorites', {
    method: 'POST',
    body: JSON.stringify({ listing_id }),
    headers: { 'Prefer': 'return=representation,resolution=merge-duplicates' }
  });
  const data = await res.json();
  return json({ favorite: data[0] }, 201, request);
}

// ── DELETE /favorites/:id ─────────────────────────────────

async function handleDeleteFavorite(request, url, env) {
  const id = url.pathname.split('/').pop();
  await supa(env, `favorites?id=eq.${id}`, { method: 'DELETE' });
  return json({ ok: true }, 200, request);
}

// ── GET /stats ────────────────────────────────────────────

async function handleStats(request, env) {
  const profileRes = await supa(env, 'taste_profile?id=eq.1&select=positive_count,negative_count');
  const profiles = await profileRes.json();
  const profile = profiles[0] || { positive_count: 0, negative_count: 0 };

  const favCountRes = await supa(env, 'favorites?select=id', { headers: { 'Prefer': 'count=exact' } });
  const favCount = parseInt(favCountRes.headers.get('content-range')?.split('/')[1] || '0');

  const listingCountRes = await supa(env, 'listings?status=eq.active&select=id', { headers: { 'Prefer': 'count=exact' } });
  const listingCount = parseInt(listingCountRes.headers.get('content-range')?.split('/')[1] || '0');

  return json({
    positive_count: profile.positive_count,
    negative_count: profile.negative_count,
    favorites_count: favCount,
    active_listings: listingCount
  }, 200, request);
}

// ── POST /fix-houses ─────────────────────────────────────

async function handleFixHouses(request, env) {
  // Fix listings with auction_house = 'Unknown' → 'Rago' (all from seller 176 batch)
  const res = await supa(env, "listings?auction_house=eq.Unknown", {
    method: 'PATCH',
    body: JSON.stringify({ auction_house: 'Rago' }),
    headers: { 'Prefer': 'return=representation' }
  });
  const fixed = await res.json();
  return json({ ok: true, fixed: Array.isArray(fixed) ? fixed.length : 0 }, 200, request);
}

// ── POST /scrape ──────────────────────────────────────────

async function handleScrape(request, env) {
  try {
    const { seller_id } = await request.json().catch(() => ({}));

    if (seller_id) {
      // Scrape a single house
      const houseName = LA_SELLERS[seller_id] || 'Unknown';
      const count = await scrapeSellerListings(env, parseInt(seller_id), houseName);
      return json({ ok: true, new_listings: count, house: houseName }, 200, request);
    }

    // No seller_id: return list of houses to scrape (caller loops)
    const houses = Object.entries(LA_SELLERS).map(([id, name]) => ({ id, name }));
    return json({ houses, message: 'POST with {seller_id} to scrape one house' }, 200, request);
  } catch (e) {
    return json({ error: e.message, stack: e.stack }, 500, request);
  }
}

async function handleProcess(request, env) {
  try {
    const result = await processUnembeddedListings(env);
    return json({ ok: true, ...result }, 200, request);
  } catch (e) {
    return json({ error: e.message, stack: e.stack, type: 'handleProcess' }, 500, request);
  }
}

// Debug endpoint
async function handleDebugProcess(request, env) {
  const errors = [];
  try {
    const res = await supa(env,
      'listings?ai_description=is.null&status=eq.active&select=id,title,hero_image&limit=1'
    );
    const listings = await res.json();
    if (!listings || listings.length === 0) return json({ msg: 'No unprocessed listings' }, 200, request);

    const listing = listings[0];
    errors.push({ step: 'got_listing', title: listing.title, image: listing.hero_image });

    // Try fetching image
    const imgRes = await fetch(listing.hero_image);
    errors.push({ step: 'image_fetch', status: imgRes.status, type: imgRes.headers.get('content-type') });

    const imgBuf = await imgRes.arrayBuffer();
    errors.push({ step: 'image_buffer', size: imgBuf.byteLength });

    const bytes = new Uint8Array(imgBuf);
    let binary = '';
    for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
    const imageBase64 = btoa(binary);
    errors.push({ step: 'base64', length: imageBase64.length });

    // Try Gemini
    const geminiRes = await fetch(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${env.GEMINI_KEY}`,
      {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          contents: [{ parts: [
            { inline_data: { mime_type: 'image/jpeg', data: imageBase64 } },
            { text: 'Describe this auction item in one paragraph.' }
          ]}]
        })
      }
    );
    const geminiData = await geminiRes.json();
    const desc = geminiData?.candidates?.[0]?.content?.parts?.[0]?.text;
    errors.push({ step: 'gemini', status: geminiRes.status, desc_length: desc?.length, desc_preview: desc?.slice(0, 100), error: geminiData?.error });

    // Try embedding
    if (desc) {
      const embRes = await fetch(
        `https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key=${env.GEMINI_KEY}`,
        {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            model: 'models/gemini-embedding-001',
            content: { parts: [{ text: desc }] },
            outputDimensionality: 768
          })
        }
      );
      const embData = await embRes.json();
      const emb = embData?.embedding?.values;
      errors.push({ step: 'embedding', status: embRes.status, dim: emb?.length, error: embData?.error });
    }

    return json({ debug: errors }, 200, request);
  } catch (e) {
    errors.push({ step: 'error', msg: e.message, stack: e.stack });
    return json({ debug: errors }, 500, request);
  }
}

// ── Pipeline: Scrape → AI Describe → Embed ────────────────

async function runPipeline(env) {
  console.log('[OLI] Pipeline starting...');

  // Step 1: Scrape listings from all platforms
  const newListings = await scrapeAll(env);
  console.log(`[OLI] Scraped ${newListings} new listings`);

  // Step 2: Process un-described listings with Gemini
  await processUnembeddedListings(env);

  // Step 3: Expire old listings
  await expireOldListings(env);

  console.log('[OLI] Pipeline complete');
}

// ── Scraping ──────────────────────────────────────────────

async function scrapeAll(env) {
  let total = 0;

  // LiveAuctioneers
  try {
    const la = await scrapeLiveAuctioneers(env);
    total += la;
  } catch (e) {
    console.error('[OLI] LiveAuctioneers scrape failed:', e);
  }

  // Craigslist (Phase 2)
  // try {
  //   const cl = await scrapeCraigslist(env);
  //   total += cl;
  // } catch (e) {
  //   console.error('[OLI] Craigslist scrape failed:', e);
  // }

  return total;
}

// LiveAuctioneers seller IDs for tracked auction houses
const LA_SELLERS = {
  // Primary sources
  5004:  'Hughes Estate Sales',
  1285:  'Abell Auction',
  6110:  'Redlands Antique Auction',
  10356: "Salon d'Marquis",
  // Additional houses
  237:   'Los Angeles Modern Auctions',
  3822:  'BILLINGS',
  7732:  'Catalog Projects',
  8902:  'Circa Auction',
  390:   'Uniques and Antiques, Inc.',
  3967:  'Cain Modern Auctions',
  176:   'Rago/Wright',
  369:   'Wright'
};

const LA_SEARCH_URL = 'https://search-party-prod.liveauctioneers.com/search/v4/web';
const LA_IMAGE_BASE = 'https://p1.liveauctioneers.com';

async function scrapeLiveAuctioneers(env) {
  let totalNew = 0;

  for (const [sellerId, houseName] of Object.entries(LA_SELLERS)) {
    try {
      const count = await scrapeSellerListings(env, parseInt(sellerId), houseName);
      totalNew += count;
      console.log(`[OLI] ${houseName}: ${count} new listings`);
    } catch (e) {
      console.error(`[OLI] Failed to scrape ${houseName}:`, e);
    }
    // Small delay between houses
    await sleep(1000);
  }

  return totalNew;
}

async function scrapeSellerListings(env, sellerId, houseName) {
  let totalNew = 0;
  let page = 1;
  const maxPages = 5; // Keep runs short to avoid Worker timeout

  while (page <= maxPages) {
    const params = {
      analyticsTags: ['web'],
      categories: [],
      distance: {},
      options: {
        status: ['upcoming', 'live', 'online'],
        auctionHouse: [{ exclude: [], include: [sellerId] }]
      },
      page,
      pageSize: 24,
      publishDate: {},
      ranges: {},
      saleDate: {},
      searchTerm: '',
      citySlug: '',
      region: '',
      sort: '-relevance',
      seoSearch: false
    };

    const url = `${LA_SEARCH_URL}?parameters=${encodeURIComponent(JSON.stringify(params))}&skipAggs=true`;

    const res = await fetch(url, {
      headers: {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
        'Accept': 'application/json'
      }
    });

    if (!res.ok) {
      console.error(`[OLI] LA search failed: ${res.status}`);
      break;
    }

    const data = await res.json();
    const items = data?.payload?.items || [];

    if (items.length === 0) break;

    // Batch transform all items on this page
    const batch = items.map(item => transformLAItem(item, sellerId, houseName));

    // Bulk upsert entire page in one Supabase call
    const upsertRes = await supa(env, 'listings', {
      method: 'POST',
      body: JSON.stringify(batch),
      headers: {
        'Prefer': 'return=representation,resolution=ignore-duplicates'
      }
    });
    const inserted = await upsertRes.json();
    totalNew += Array.isArray(inserted) ? inserted.length : 0;

    // Check if more pages
    const totalPages = data?.payload?.totalPages || 0;
    if (page >= totalPages) break;
    page++;

    await sleep(300);
  }

  return totalNew;
}

function buildLAImageUrl(sellerId, catalogId, itemId, photoIndex, imageVersion) {
  return `${LA_IMAGE_BASE}/${sellerId}/${catalogId}/${itemId}_${photoIndex}_x.jpg?height=600&quality=95&version=${imageVersion || ''}`;
}

function transformLAItem(item, sellerId, houseName) {
  const photos = item.photos || [1];
  const imageUrls = photos.map(p =>
    buildLAImageUrl(sellerId, item.catalogId, item.itemId, p, item.imageVersion)
  );

  return {
    platform: 'liveauctioneers',
    platform_id: String(item.itemId),
    title: item.title || 'Untitled',
    description: item.shortDescription || '',
    price: item.lowBidEstimate || item.startPrice || null,
    currency: item.currency || 'USD',
    location: [item.sellerCity, item.sellerStateCode].filter(Boolean).join(', '),
    url: `https://www.liveauctioneers.com/item/${item.itemId}`,
    image_urls: imageUrls,
    hero_image: imageUrls[0] || null,
    auction_house: houseName,
    auction_date: item.saleStartTs ? new Date(item.saleStartTs * 1000).toISOString() : null,
    lot_number: item.lotNumber || null,
    status: 'active'
  };
}

async function scrapeCraigslist(env) {
  // TODO: Phase 2
  // 1. Fetch RSS: {city}.craigslist.org/search/ata?format=rss
  // 2. Parse XML for titles, links, descriptions
  // 3. Fetch individual pages for images
  // 4. Upsert into Supabase
  console.log('[OLI] Craigslist scraper: not yet implemented');
  return 0;
}

// ── AI Processing ─────────────────────────────────────────

async function processUnembeddedListings(env) {
  // Process only 5 per call to stay within 50 subrequest limit
  // (each listing: 1 image fetch + 1 Gemini + 1 embedding + 1 Supabase write = 4 subrequests)
  const res = await supa(env,
    'listings?ai_description=is.null&status=eq.active&select=id,title,description,hero_image&limit=5'
  );
  const unprocessed = await res.json();

  if (!unprocessed || unprocessed.length === 0) {
    console.log('[OLI] No listings to process');
    return { processed: 0, remaining: 0 };
  }

  let processed = 0;

  for (const listing of unprocessed) {
    try {
      // Step 1: Generate AI description from image
      const aiDesc = await generateDescription(env, listing);
      if (!aiDesc) continue;

      // Step 2: Generate embedding from combined text (auction data + AI description)
      // Auction houses provide rich catalog data (artist, medium, period, dimensions)
      // — embed that alongside the AI visual description for best taste signal
      const combinedText = [listing.title, listing.description, aiDesc].filter(Boolean).join('. ');
      const embedding = await generateEmbedding(env, combinedText);
      if (!embedding) continue;

      // Step 3: Update listing
      await supa(env, `listings?id=eq.${listing.id}`, {
        method: 'PATCH',
        body: JSON.stringify({
          ai_description: aiDesc,
          embedding: embedding
        })
      });

      processed++;
      console.log(`[OLI] Processed: ${listing.title}`);
    } catch (e) {
      console.error(`[OLI] Failed to process listing ${listing.id}:`, e);
    }
  }

  return { processed, remaining: 'unknown' };
}

async function generateDescription(env, listing) {
  if (!listing.hero_image) return null;

  // Fetch image and convert to base64
  let imageBase64;
  try {
    const imgRes = await fetch(listing.hero_image);
    const imgBuf = await imgRes.arrayBuffer();
    // Use chunked approach to avoid max call stack with spread operator
    const bytes = new Uint8Array(imgBuf);
    let binary = '';
    for (let i = 0; i < bytes.length; i++) {
      binary += String.fromCharCode(bytes[i]);
    }
    imageBase64 = btoa(binary);
  } catch (e) {
    console.error(`[OLI] Image fetch failed for ${listing.id}:`, e);
    // Fall back to text-only description
    return generateTextOnlyDescription(listing);
  }

  const mimeType = listing.hero_image.includes('.png') ? 'image/png' : 'image/jpeg';

  const prompt = `Analyze this auction/sale listing image of a vintage, antique, or decorative object. Describe it in a single paragraph optimized for embedding similarity search. Include:
- Object type (vase, chair, sculpture, lamp, painting, textile, etc.)
- Style/period (mid-century modern, art deco, brutalist, primitive, folk art, Memphis, postmodern, Arts & Crafts, etc.)
- Material (ceramic, stoneware, wood, brass, bronze, stone, glass, etc.)
- Color palette and surface qualities (earth tones, patina, matte glaze, polished, weathered, etc.)
- Aesthetic qualities (organic form, geometric, sculptural, minimal, ornate, textured, etc.)
- Size impression (small decorative, table-scale, furniture-scale)
- Any maker/origin indicators if visible

Title from listing: "${listing.title || 'Unknown'}"
${listing.description ? `Description: "${listing.description}"` : ''}

Write a rich, descriptive paragraph. Do not say "this is" or "the image shows". Just describe the object directly.`;

  const body = {
    contents: [{
      parts: [
        { inline_data: { mime_type: mimeType, data: imageBase64 } },
        { text: prompt }
      ]
    }]
  };

  const res = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${env.GEMINI_KEY}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body)
    }
  );

  const data = await res.json();
  return data?.candidates?.[0]?.content?.parts?.[0]?.text || null;
}

function generateTextOnlyDescription(listing) {
  // Fallback: when no image, use the auction house's own catalog data
  // For LiveAuctioneers this is rich (artist, medium, date, dimensions)
  return [listing.title, listing.description].filter(Boolean).join('. ');
}

async function generateEmbedding(env, text) {
  const res = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key=${env.GEMINI_KEY}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'models/gemini-embedding-001',
        content: { parts: [{ text }] },
        outputDimensionality: 768
      })
    }
  );

  const data = await res.json();
  return data?.embedding?.values || null;
}

// ── Expiration ────────────────────────────────────────────

async function expireOldListings(env) {
  const now = new Date().toISOString();
  await supa(env, `listings?status=eq.active&auction_date=lt.${now}`, {
    method: 'PATCH',
    body: JSON.stringify({ status: 'expired' })
  });
}

// ── Utils ─────────────────────────────────────────────────

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
