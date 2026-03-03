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

      if (url.pathname === '/stats' && request.method === 'GET')
        return handleStats(request, env);

      return new Response('Not found', { status: 404, headers: corsHeaders(request) });
    } catch (e) {
      console.error('Worker error:', e);
      return json({ error: e.message }, 500, request);
    }
  },

  // Cron trigger — scrape + process
  async scheduled(event, env, ctx) {
    ctx.waitUntil(runPipeline(env));
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
  const excludeIds = excludeRaw ? excludeRaw.split(',').filter(Boolean) : [];

  // Get taste profile
  const profileRes = await supa(env, 'taste_profile?id=eq.1&select=*');
  const profiles = await profileRes.json();
  const profile = profiles[0];

  let listings;

  if (profile && profile.positive_centroid && profile.positive_count >= 10) {
    // ── Ranked mode: use taste model ──
    // Get 80% ranked + 20% random
    const rankedCount = Math.ceil(limit * 0.8);
    const randomCount = limit - rankedCount;

    // Ranked via pgvector similarity
    const matchRes = await supaRpc(env, 'match_listings', {
      query_embedding: profile.positive_centroid,
      match_count: rankedCount,
      exclude_ids: excludeIds
    });
    const ranked = await matchRes.json();

    // Random exploration
    const rankedIds = ranked.map(r => r.id);
    const allExclude = [...excludeIds, ...rankedIds];
    const randomRes = await supa(env,
      `listings?status=eq.active&embedding=not.is.null&id=not.in.(${allExclude.join(',')})&select=id,platform,platform_id,title,description,price,location,url,hero_image,image_urls,auction_house,auction_date,lot_number,ai_description&order=scraped_at.desc&limit=${randomCount}`
    );
    const random = await randomRes.json();

    // Merge and shuffle (keeping ranked first but mixing in random)
    listings = [...ranked, ...random];
  } else {
    // ── Cold start: newest first with some randomization ──
    const res = await supa(env,
      `listings?status=eq.active&id=not.in.(${excludeIds.join(',') || "''"})&select=id,platform,platform_id,title,description,price,location,url,hero_image,image_urls,auction_house,auction_date,lot_number,ai_description&order=scraped_at.desc&limit=${limit}`
    );
    listings = await res.json();
  }

  return json({ listings: listings || [] }, 200, request);
}

// ── POST /swipe ───────────────────────────────────────────

async function handleSwipe(request, env) {
  const { listing_id, action } = await request.json();

  if (!listing_id || !['left', 'right', 'favorite'].includes(action)) {
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
  const side = (action === 'left') ? 'negative' : 'positive';
  const centroidKey = side + '_centroid';
  const countKey = side + '_count';

  // Get current profile
  const res = await supa(env, 'taste_profile?id=eq.1&select=*');
  const profiles = await res.json();
  let profile = profiles[0];

  if (!profile) {
    // Create initial profile
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
  const newCount = currentCount + 1;

  let newCentroid;
  if (!currentCentroid) {
    newCentroid = embedding;
  } else {
    // Running average: (old * count + new) / (count + 1)
    newCentroid = currentCentroid.map((v, i) =>
      (v * currentCount + embedding[i]) / newCount
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

// ── POST /scrape ──────────────────────────────────────────

async function handleScrape(request, env) {
  await runPipeline(env);
  return json({ ok: true, message: 'Scrape + process pipeline complete' }, 200, request);
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
  5004:  'Hughes Estate Sales',
  1285:  'Abell Auction',
  6110:  'Redlands Antique Auction',
  10356: "Salon d'Marquis"
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
  const maxPages = 10; // Safety limit

  while (page <= maxPages) {
    const params = {
      analyticsTags: ['web'],
      categories: [],
      distance: {},
      options: {
        status: ['upcoming', 'live', 'online'],
        auctionHouse: { exclude: [], include: [sellerId] }
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

    // Transform and upsert each item
    for (const item of items) {
      const inserted = await upsertLAListing(env, item, sellerId, houseName);
      if (inserted) totalNew++;
    }

    // Check if more pages
    const totalPages = data?.payload?.totalPages || 0;
    if (page >= totalPages) break;
    page++;

    await sleep(500);
  }

  return totalNew;
}

function buildLAImageUrl(sellerId, catalogId, itemId, photoIndex, imageVersion) {
  return `${LA_IMAGE_BASE}/${sellerId}/${catalogId}/${itemId}_${photoIndex}_x.jpg?height=600&quality=95&version=${imageVersion || ''}`;
}

async function upsertLAListing(env, item, sellerId, houseName) {
  const platformId = String(item.itemId);

  // Build image URLs from photos array
  const photos = item.photos || [1];
  const imageUrls = photos.map(p =>
    buildLAImageUrl(sellerId, item.catalogId, item.itemId, p, item.imageVersion)
  );

  const location = [item.sellerCity, item.sellerStateCode].filter(Boolean).join(', ');
  const lotUrl = `https://www.liveauctioneers.com/item/${item.itemId}`;

  // Use low estimate as price, fall back to start price
  const price = item.lowBidEstimate || item.startPrice || null;

  // Convert sale start timestamp to ISO date
  const auctionDate = item.saleStartTs
    ? new Date(item.saleStartTs * 1000).toISOString()
    : null;

  const listing = {
    platform: 'liveauctioneers',
    platform_id: platformId,
    title: item.title || 'Untitled',
    description: item.shortDescription || '',
    price,
    currency: item.currency || 'USD',
    location,
    url: lotUrl,
    image_urls: imageUrls,
    hero_image: imageUrls[0] || null,
    auction_house: houseName,
    auction_date: auctionDate,
    lot_number: item.lotNumber || null,
    status: 'active'
  };

  // Upsert (insert or skip if exists)
  const res = await supa(env, 'listings', {
    method: 'POST',
    body: JSON.stringify(listing),
    headers: {
      'Prefer': 'return=representation,resolution=ignore-duplicates'
    }
  });

  const result = await res.json();
  // If result array has an item, it was inserted (new)
  return Array.isArray(result) && result.length > 0;
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
  // Get listings that have no ai_description yet
  const res = await supa(env,
    'listings?ai_description=is.null&status=eq.active&select=id,title,description,hero_image&limit=20'
  );
  const unprocessed = await res.json();

  if (!unprocessed || unprocessed.length === 0) {
    console.log('[OLI] No listings to process');
    return;
  }

  console.log(`[OLI] Processing ${unprocessed.length} listings...`);

  for (const listing of unprocessed) {
    try {
      // Step 1: Generate AI description from image
      const aiDesc = await generateDescription(env, listing);
      if (!aiDesc) continue;

      // Step 2: Generate embedding from description
      const embedding = await generateEmbedding(env, aiDesc);
      if (!embedding) continue;

      // Step 3: Update listing
      await supa(env, `listings?id=eq.${listing.id}`, {
        method: 'PATCH',
        body: JSON.stringify({
          ai_description: aiDesc,
          embedding: embedding
        })
      });

      console.log(`[OLI] Processed: ${listing.title}`);

      // Rate limit: ~7 listings/min (2 Gemini calls per listing, 15 RPM limit)
      await sleep(9000);
    } catch (e) {
      console.error(`[OLI] Failed to process listing ${listing.id}:`, e);
    }
  }
}

async function generateDescription(env, listing) {
  if (!listing.hero_image) return null;

  // Fetch image and convert to base64
  let imageBase64;
  try {
    const imgRes = await fetch(listing.hero_image);
    const imgBuf = await imgRes.arrayBuffer();
    imageBase64 = btoa(String.fromCharCode(...new Uint8Array(imgBuf)));
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
    `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=${env.GEMINI_KEY}`,
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
  // Fallback: create description from title and listing description only
  return [listing.title, listing.description].filter(Boolean).join('. ');
}

async function generateEmbedding(env, text) {
  const res = await fetch(
    `https://generativelanguage.googleapis.com/v1beta/models/text-embedding-004:embedContent?key=${env.GEMINI_KEY}`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        model: 'models/text-embedding-004',
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
