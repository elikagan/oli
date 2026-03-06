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

      if (url.pathname === '/stats/accuracy-history' && request.method === 'GET')
        return handleAccuracyHistory(request, env);

      if (url.pathname === '/debug/swipe-dupes' && request.method === 'GET')
        return handleSwipeDupes(request, url, env);

      if (url.pathname === '/admin/cleanup-nk' && request.method === 'POST')
        return handleCleanupNK(request, env);

      if (url.pathname === '/admin/process' && request.method === 'POST')
        return handleProcessBatch(request, env);

      if (url.pathname === '/fix-houses' && request.method === 'POST')
        return handleFixHouses(request, env);

      if (url.pathname === '/rebuild-taste' && request.method === 'POST')
        return handleRebuildTaste(request, env);

      if (url.pathname === '/migrate' && request.method === 'POST')
        return handleMigrate(request, env);

      if (url.pathname === '/scrape/craigslist' && request.method === 'POST')
        return handleScrapeCraigslist(request, env);

      if (url.pathname === '/artists' && request.method === 'GET')
        return handleGetArtists(request, env);

      if (url.pathname === '/artists' && request.method === 'POST')
        return handleImportArtists(request, env);

      if (url.pathname === '/artists' && request.method === 'DELETE')
        return handleDeleteArtist(request, env, url);

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
    // Also scrape one Craigslist category per run (rotates through 5 searches)
    const clIdx = Math.floor(hour / 4) % CL_SEARCHES.length;
    const clSearch = CL_SEARCHES[clIdx];
    ctx.waitUntil(
      Promise.all([
        scrapeSellerListings(env, sellerId, houseName)
          .then(count => console.log(`[OLI] Cron scraped ${houseName}: ${count} new`))
          .catch(e => console.error(`[OLI] Cron failed for ${houseName}:`, e)),
        scrapeCraigslistSearch(env, clSearch)
          .then(count => console.log(`[OLI] Cron CL ${clSearch.name}: ${count} new`))
          .catch(e => console.error(`[OLI] Cron CL ${clSearch.name} failed:`, e))
      ])
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

  // Fetch ALL swiped IDs with pagination (Supabase caps at 1000 per request)
  let swipedIds = [];
  let page = 0;
  const PAGE_SIZE = 1000;
  while (true) {
    const from = page * PAGE_SIZE;
    const to = from + PAGE_SIZE - 1;
    const res = await fetch(`${env.SUPABASE_URL}/rest/v1/swipes?select=listing_id`, {
      headers: {
        'apikey': env.SUPABASE_SERVICE_KEY,
        'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
        'Content-Type': 'application/json',
        'Range': `${from}-${to}`
      }
    });
    const rows = await res.json();
    if (!Array.isArray(rows) || rows.length === 0) break;
    swipedIds = swipedIds.concat(rows.map(r => r.listing_id));
    if (rows.length < PAGE_SIZE) break;
    page++;
  }
  const swipedSet = new Set(swipedIds);
  const _debugInfo = { swipe_rows: swipedIds.length, swiped_unique: swipedSet.size };

  // Get taste profile
  const profileRes = await supa(env, 'taste_profile?id=eq.1&select=*');
  const profiles = await profileRes.json();
  const profile = profiles[0];

  let listings;

  const selectFields = 'id,platform,platform_id,title,description,price,location,url,hero_image,image_urls,auction_house,auction_date,lot_number,ai_description,maker,auction_data';

  // Parse centroids (pgvector returns strings)
  const parseCentroid = (c) => {
    if (!c) return null;
    if (Array.isArray(c)) return c;
    if (typeof c === 'string') { try { return JSON.parse(c); } catch { return null; } }
    return null;
  };
  const posCentroid = parseCentroid(profile?.positive_centroid);
  const negCentroid = parseCentroid(profile?.negative_centroid);

  if (posCentroid && (profile?.positive_count || 0) >= 10) {
    // ── Ranked mode: taste model with negative centroid ──
    const rankedCount = Math.ceil(limit * 0.8);
    const randomCount = limit - rankedCount;

    // Fetch more from RPC to account for JS-side filtering of swiped items
    const rpcParams = {
      query_embedding: posCentroid,
      match_count: swipedSet.size + limit + 50
    };
    if (negCentroid) rpcParams.neg_embedding = negCentroid;

    const matchRes = await supaRpc(env, 'match_listings', rpcParams);
    const ranked = await matchRes.json();
    _debugInfo.rpc_status = matchRes.status;
    _debugInfo.ranked_count = Array.isArray(ranked) ? ranked.length : 'not_array';
    if (!Array.isArray(ranked)) {
      _debugInfo.rpc_error = JSON.stringify(ranked).slice(0, 200);
      return json({ listings: [], _debug: _debugInfo }, 200, request);
    }

    // Filter out already-swiped items in JS
    const rankedFiltered = ranked.filter(l => !swipedSet.has(l.id)).slice(0, rankedCount);
    _debugInfo.ranked_after_filter = rankedFiltered.length;

    // Random exploration (include listings without embeddings, paginate to find unswiped)
    const rankedIds = new Set(rankedFiltered.map(r => r.id));
    let randomAll = [];
    for (let rp = 0; rp < 5; rp++) {
      const from = rp * 1000;
      const to = from + 999;
      const randomRes = await fetch(`${env.SUPABASE_URL}/rest/v1/listings?status=eq.active&hero_image=not.is.null&select=${selectFields}&order=scraped_at.desc`, {
        headers: {
          'apikey': env.SUPABASE_SERVICE_KEY,
          'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
          'Content-Type': 'application/json',
          'Range': `${from}-${to}`
        }
      });
      const batch = await randomRes.json();
      if (!Array.isArray(batch) || batch.length === 0) break;
      const unswiped = batch.filter(l => !swipedSet.has(l.id) && !rankedIds.has(l.id));
      randomAll = randomAll.concat(unswiped);
      if (randomAll.length >= limit) break; // Got enough
      if (batch.length < 1000) break; // No more pages
    }
    const random = shuffle(randomAll).slice(0, Math.max(randomCount, limit - rankedFiltered.length));
    _debugInfo.random_found = randomAll.length;
    _debugInfo.random_used = random.length;

    listings = [...rankedFiltered, ...random];
  } else {
    // ── Cold start: fetch pool, exclude swiped, shuffle ──
    const poolSize = Math.min(limit * 5, 500);
    const res = await supa(env,
      `listings?status=eq.active&hero_image=not.is.null&select=${selectFields}&order=scraped_at.desc&limit=${poolSize}`
    );
    const pool = await res.json();
    const filtered = (Array.isArray(pool) ? pool : []).filter(l => !swipedSet.has(l.id));
    listings = shuffle(filtered).slice(0, limit);
  }

  // k-NN prediction: score each listing by its nearest swiped neighbors
  if (listings && listings.length > 0) {
    try {
      const targetIds = listings.map(l => l.id);
      const knnRes = await supaRpc(env, 'predict_knn', { target_ids: targetIds, k: 10 });
      const knnData = await knnRes.json();
      if (Array.isArray(knnData)) {
        const knnMap = new Map(knnData.map(r => [r.listing_id, r.knn_score]));
        listings.forEach(l => {
          const knn = knnMap.get(l.id);
          if (knn != null) l.knn_score = Math.round(knn * 100);
        });
      }
    } catch (e) {
      console.error('k-NN prediction failed:', e);
    }
  }

  listings = shuffle(listings || []);

  return json({ listings }, 200, request);
}

async function handleSwipeDupes(request, url, env) {
  // Find listings swiped more than once
  const res = await fetch(`${env.SUPABASE_URL}/rest/v1/swipes?select=listing_id,action,created_at&order=created_at.desc`, {
    headers: {
      'apikey': env.SUPABASE_SERVICE_KEY,
      'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      'Range': '0-99999',
      'Content-Type': 'application/json'
    }
  });
  const rows = await res.json();
  if (!Array.isArray(rows)) return json({ error: 'failed to fetch swipes' }, 500, request);

  // Count swipes per listing
  const counts = {};
  rows.forEach(r => {
    if (!counts[r.listing_id]) counts[r.listing_id] = [];
    counts[r.listing_id].push({ action: r.action, at: r.created_at });
  });

  const dupes = Object.entries(counts)
    .filter(([, swipes]) => swipes.length > 1)
    .sort((a, b) => b[1].length - a[1].length)
    .slice(0, 20);

  // Get titles for dupe listings
  const dupeIds = dupes.map(([id]) => id);
  let titles = {};
  if (dupeIds.length > 0) {
    const listRes = await supa(env, `listings?id=in.(${dupeIds.join(',')})&select=id,title`);
    const listRows = await listRes.json();
    if (Array.isArray(listRows)) listRows.forEach(l => titles[l.id] = l.title);
  }

  // Also show most recent 5 swipes
  const recent = rows.slice(0, 5).map(r => ({
    listing_id: r.listing_id,
    action: r.action,
    at: r.created_at,
    title: titles[r.listing_id] || '?'
  }));

  return json({
    total_swipes: rows.length,
    unique_listings_swiped: Object.keys(counts).length,
    duplicates: dupes.map(([id, swipes]) => ({
      listing_id: id,
      title: titles[id] || '?',
      swipe_count: swipes.length,
      swipes
    })),
    recent_5: recent
  }, 200, request);
}

async function handleProcessBatch(request, env) {
  try {
    const result = await processUnembeddedListings(env, 5);
    return json(result, 200, request);
  } catch (e) {
    return json({ error: e.message, stack: e.stack?.slice(0, 500) }, 500, request);
  }
}

async function handleCleanupNK(request, env) {
  // Mark cheap NK listings as inactive (under $50 = pencils, magnets, openers, etc.)
  const cheapRes = await fetch(`${env.SUPABASE_URL}/rest/v1/listings?auction_house=eq.Nickey%20Kehoe&status=eq.active&price=lt.50&select=id,title,price`, {
    headers: { 'apikey': env.SUPABASE_SERVICE_KEY, 'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`, 'Range': '0-999' }
  });
  const cheap = await cheapRes.json();

  // Also get null-price NK listings (events, consultancies)
  const nullRes = await fetch(`${env.SUPABASE_URL}/rest/v1/listings?auction_house=eq.Nickey%20Kehoe&status=eq.active&price=is.null&select=id,title`, {
    headers: { 'apikey': env.SUPABASE_SERVICE_KEY, 'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`, 'Range': '0-999' }
  });
  const nullPrice = await nullRes.json();

  const toDeactivate = [...(Array.isArray(cheap) ? cheap : []), ...(Array.isArray(nullPrice) ? nullPrice : [])];
  const ids = toDeactivate.map(l => l.id);

  if (ids.length > 0) {
    // Batch update in groups of 100
    for (let i = 0; i < ids.length; i += 100) {
      const batch = ids.slice(i, i + 100);
      await fetch(`${env.SUPABASE_URL}/rest/v1/listings?id=in.(${batch.join(',')})`, {
        method: 'PATCH',
        headers: {
          'apikey': env.SUPABASE_SERVICE_KEY,
          'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
          'Content-Type': 'application/json',
          'Prefer': 'return=minimal'
        },
        body: JSON.stringify({ status: 'inactive' })
      });
    }
  }

  return json({
    deactivated: ids.length,
    samples: toDeactivate.slice(0, 10).map(l => ({ title: l.title, price: l.price || null }))
  }, 200, request);
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
  const { listing_id, action, predicted_score } = await request.json();

  if (!listing_id || !['left', 'right', 'favorite', 'super_like', 'super_hate'].includes(action)) {
    return json({ error: 'Invalid swipe' }, 400, request);
  }

  // Record swipe with prediction score for accuracy tracking
  const swipeData = { listing_id, action };
  if (predicted_score != null) swipeData.predicted_score = predicted_score;
  await supa(env, 'swipes', {
    method: 'POST',
    body: JSON.stringify(swipeData)
  });

  // Update taste profile if listing has embedding
  const listingRes = await supa(env, `listings?id=eq.${listing_id}&select=embedding,maker,hero_image,title`);
  const listings = await listingRes.json();
  const listing = listings[0];
  if (listing?.embedding) {
    await updateTasteProfile(env, listing.embedding, action);
  }

  // Auto-add artist on positive swipe if listing has a maker
  const positive = ['right', 'favorite', 'super_like'];
  if (positive.includes(action) && listing?.maker) {
    try {
      await supa(env, 'artists?on_conflict=name', {
        method: 'POST',
        body: JSON.stringify([{
          name: listing.maker,
          source: 'auto_swipe',
          priority: 'med'
        }]),
        headers: { 'Prefer': 'return=minimal,resolution=ignore-duplicates' }
      });
    } catch (e) {
      console.error('Auto-add artist failed:', e);
    }
  }

  return json({ ok: true }, 200, request);
}

// ── Taste Profile Update ──────────────────────────────────

async function updateTasteProfile(env, embedding, action) {
  const side = (action === 'left' || action === 'super_hate') ? 'negative' : 'positive';
  const centroidKey = side + '_centroid';
  const countKey = side + '_count';

  // Weighted signals: super=3x, favorite=2x, regular=1x
  const weight = (action === 'super_hate' || action === 'super_like') ? 3
    : (action === 'favorite') ? 2 : 1;

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

  // Prediction accuracy: analyze scored swipes
  let accuracy = null;
  try {
    const scoredRes = await supa(env, 'swipes?predicted_score=not.is.null&select=listing_id,action,predicted_score&order=created_at.desc&limit=500');
    const scoredRaw = await scoredRes.json();
    // Deduplicate: keep only the latest swipe per listing (already ordered desc)
    const seen = new Set();
    const scored = [];
    if (Array.isArray(scoredRaw)) {
      for (const s of scoredRaw) {
        if (!seen.has(s.listing_id)) {
          seen.add(s.listing_id);
          scored.push(s);
        }
      }
    }
    if (scored.length >= 10) {
      const positive = ['right', 'favorite', 'super_like'];
      let correct = 0;
      scored.forEach(s => {
        const liked = positive.includes(s.action);
        const predicted = s.predicted_score > 50;
        if (liked === predicted) correct++;
      });
      accuracy = {
        total_scored: scored.length,
        correct,
        pct: Math.round((correct / scored.length) * 100),
        avg_liked_score: Math.round(scored.filter(s => positive.includes(s.action)).reduce((sum, s) => sum + s.predicted_score, 0) / Math.max(1, scored.filter(s => positive.includes(s.action)).length)),
        avg_skipped_score: Math.round(scored.filter(s => !positive.includes(s.action)).reduce((sum, s) => sum + s.predicted_score, 0) / Math.max(1, scored.filter(s => !positive.includes(s.action)).length))
      };
    }
  } catch (e) {
    console.error('Accuracy calc failed:', e);
  }

  return json({
    positive_count: profile.positive_count,
    negative_count: profile.negative_count,
    favorites_count: favCount,
    active_listings: listingCount,
    accuracy
  }, 200, request);
}

// ── GET /stats/accuracy-history ──────────────────────────

async function handleAccuracyHistory(request, env) {
  try {
    const scoredRes = await supa(env, 'swipes?predicted_score=not.is.null&select=listing_id,action,predicted_score,created_at&order=created_at.asc&limit=1000');
    const scoredRaw = await scoredRes.json();
    // Deduplicate: keep only the first swipe per listing (asc order)
    const seenIds = new Set();
    const scored = [];
    if (Array.isArray(scoredRaw)) {
      for (const s of scoredRaw) {
        if (!seenIds.has(s.listing_id)) {
          seenIds.add(s.listing_id);
          scored.push(s);
        }
      }
    }
    if (scored.length < 10) {
      return json({ points: [] }, 200, request);
    }

    const positive = ['right', 'favorite', 'super_like'];
    const windowSize = 20;
    const points = [];

    for (let i = windowSize - 1; i < scored.length; i++) {
      const window = scored.slice(i - windowSize + 1, i + 1);
      let correct = 0;
      window.forEach(s => {
        const liked = positive.includes(s.action);
        const predicted = s.predicted_score > 50;
        if (liked === predicted) correct++;
      });
      points.push({
        date: scored[i].created_at,
        accuracy: Math.round((correct / windowSize) * 100),
        index: i + 1
      });
    }

    // Downsample to ~30 points max
    if (points.length > 30) {
      const step = Math.ceil(points.length / 30);
      const downsampled = [];
      for (let i = 0; i < points.length; i += step) {
        downsampled.push(points[i]);
      }
      if (downsampled[downsampled.length - 1] !== points[points.length - 1]) {
        downsampled.push(points[points.length - 1]);
      }
      return json({ points: downsampled }, 200, request);
    }

    return json({ points }, 200, request);
  } catch (e) {
    console.error('Accuracy history failed:', e);
    return json({ points: [] }, 200, request);
  }
}

// ── POST /rebuild-taste ──────────────────────────────────

async function handleRebuildTaste(request, env) {
  try {
  // Incremental rebuild: process a batch of swipes each call
  // Accepts ?reset=1 to start fresh, otherwise continues from current profile
  const url = new URL(request.url);
  const reset = url.searchParams.get('reset') === '1';
  const batchOffset = parseInt(url.searchParams.get('offset') || '0');
  const batchSize = 10; // Stay well under 50 subrequest limit

  // Get current profile (or reset)
  if (reset) {
    await supa(env, 'taste_profile?id=eq.1', {
      method: 'PATCH',
      body: JSON.stringify({
        positive_centroid: null, negative_centroid: null,
        positive_count: 0, negative_count: 0,
        updated_at: new Date().toISOString()
      })
    });
  }

  // Fetch a batch of swipes
  const swipesRes = await supa(env,
    `swipes?select=listing_id,action&order=created_at.asc&limit=${batchSize}&offset=${batchOffset}`
  );
  const swipes = await swipesRes.json();
  if (!Array.isArray(swipes) || swipes.length === 0) {
    return json({ ok: true, done: true, offset: batchOffset, message: 'All swipes processed' }, 200, request);
  }

  // Get current profile
  const profRes = await supa(env, 'taste_profile?id=eq.1&select=*');
  const profiles = await profRes.json();
  let profile = profiles[0] || { positive_centroid: null, negative_centroid: null, positive_count: 0, negative_count: 0 };

  // pgvector may return centroids as strings — parse them
  const parseCentroid = (c) => {
    if (!c) return null;
    if (Array.isArray(c)) return c;
    if (typeof c === 'string') {
      try { return JSON.parse(c); } catch { return null; }
    }
    return null;
  };
  let posCentroid = parseCentroid(profile.positive_centroid);
  let negCentroid = parseCentroid(profile.negative_centroid);
  let posCount = profile.positive_count || 0;
  let negCount = profile.negative_count || 0;
  let processed = 0, skipped = 0;

  // Process each swipe individually (fetch embedding one at a time to stay under limits)
  for (const swipe of swipes) {
    const listRes = await supa(env, `listings?id=eq.${swipe.listing_id}&select=embedding`);
    const listings = await listRes.json();
    const rawEmb = listings?.[0]?.embedding;
    const embedding = parseCentroid(rawEmb);

    if (!embedding) { skipped++; continue; }

    const side = (swipe.action === 'left' || swipe.action === 'super_hate') ? 'negative' : 'positive';
    const weight = (swipe.action === 'super_hate' || swipe.action === 'super_like') ? 3
      : (swipe.action === 'favorite') ? 2 : 1;

    if (side === 'positive') {
      const newCount = posCount + weight;
      posCentroid = !posCentroid ? embedding
        : posCentroid.map((v, i) => (v * posCount + embedding[i] * weight) / newCount);
      posCount = newCount;
    } else {
      const newCount = negCount + weight;
      negCentroid = !negCentroid ? embedding
        : negCentroid.map((v, i) => (v * negCount + embedding[i] * weight) / newCount);
      negCount = newCount;
    }
    processed++;
  }

  // Save progress
  await supa(env, 'taste_profile?id=eq.1', {
    method: 'PATCH',
    body: JSON.stringify({
      positive_centroid: posCentroid, negative_centroid: negCentroid,
      positive_count: posCount, negative_count: negCount,
      updated_at: new Date().toISOString()
    })
  });

  const nextOffset = batchOffset + batchSize;
  return json({
    ok: true, done: swipes.length < batchSize,
    processed, skipped, batch: swipes.length,
    next_offset: nextOffset,
    positive_count: posCount, negative_count: negCount
  }, 200, request);

  } catch (e) {
    return json({ error: e.message, stack: e.stack }, 500, request);
  }
}

// ── POST /migrate ────────────────────────────────────────

async function supaSQL(env, query) {
  // Use Supabase's raw SQL endpoint (requires service_role key)
  const res = await fetch(`${env.SUPABASE_URL}/rest/v1/rpc/exec_sql`, {
    method: 'POST',
    headers: {
      'apikey': env.SUPABASE_SERVICE_KEY,
      'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query })
  });
  return { status: res.status, ok: res.ok, body: await res.text() };
}

async function handleMigrate(request, env) {
  // Return the SQL that needs to be run manually in Supabase SQL Editor
  const sql = `
-- Phase 2A: Prediction accuracy tracking
ALTER TABLE swipes ADD COLUMN IF NOT EXISTS predicted_score FLOAT;

-- Phase 3A: Richer auction data
ALTER TABLE listings ADD COLUMN IF NOT EXISTS auction_data JSONB;

-- 1. Add maker column to listings
ALTER TABLE listings ADD COLUMN IF NOT EXISTS maker TEXT;

-- 2. Create artists table
CREATE TABLE IF NOT EXISTS artists (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name TEXT NOT NULL UNIQUE,
  birth_year INT,
  age INT,
  location TEXT,
  medium TEXT,
  rep_status TEXT,
  rep_label TEXT,
  priority TEXT,
  tags TEXT,
  links TEXT[],
  notes TEXT,
  source TEXT DEFAULT 'manual',
  listing_count INT DEFAULT 0,
  avg_price NUMERIC,
  created_at TIMESTAMPTZ DEFAULT now(),
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- 3. RLS for artists table
ALTER TABLE artists ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS "anon_read_artists" ON artists;
CREATE POLICY "anon_read_artists" ON artists FOR SELECT TO anon USING (true);
DROP POLICY IF EXISTS "service_write_artists" ON artists;
CREATE POLICY "service_write_artists" ON artists FOR ALL TO service_role USING (true);

-- 4. Update match_listings: neg weight 0.3 → 0.5
CREATE OR REPLACE FUNCTION match_listings(
  query_embedding VECTOR(768),
  neg_embedding VECTOR(768) DEFAULT NULL,
  match_count INT DEFAULT 20,
  exclude_ids UUID[] DEFAULT '{}'
)
RETURNS TABLE (
  id UUID, platform TEXT, platform_id TEXT, title TEXT, description TEXT,
  price NUMERIC, location TEXT, url TEXT, hero_image TEXT, image_urls TEXT[],
  auction_house TEXT, auction_date TIMESTAMPTZ, lot_number TEXT,
  ai_description TEXT, maker TEXT, auction_data JSONB, similarity FLOAT
)
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  SELECT
    l.id, l.platform, l.platform_id, l.title, l.description,
    l.price, l.location, l.url, l.hero_image, l.image_urls,
    l.auction_house, l.auction_date, l.lot_number,
    l.ai_description, l.maker, l.auction_data,
    CASE
      WHEN neg_embedding IS NOT NULL THEN
        (1 - (l.embedding <=> query_embedding))::FLOAT - 0.5 * (1 - (l.embedding <=> neg_embedding))::FLOAT
      ELSE
        (1 - (l.embedding <=> query_embedding))::FLOAT
    END AS similarity
  FROM listings l
  WHERE l.status = 'active'
    AND l.embedding IS NOT NULL
    AND l.hero_image IS NOT NULL
    AND NOT (l.id = ANY(exclude_ids))
  ORDER BY similarity DESC
  LIMIT match_count;
END;
$$;

-- 5. k-NN prediction: for each listing, find K nearest swiped items and return % liked
CREATE OR REPLACE FUNCTION predict_knn(
  target_ids UUID[],
  k INT DEFAULT 10
)
RETURNS TABLE (listing_id UUID, knn_score FLOAT)
LANGUAGE plpgsql AS $$
BEGIN
  RETURN QUERY
  SELECT
    t.id AS listing_id,
    COALESCE(nn.score, 0.5)::FLOAT AS knn_score
  FROM unnest(target_ids) AS tid(id)
  JOIN listings t ON t.id = tid.id AND t.embedding IS NOT NULL
  LEFT JOIN LATERAL (
    SELECT AVG(
      CASE WHEN s.action IN ('right', 'favorite', 'super_like') THEN 1.0 ELSE 0.0 END
    ) AS score
    FROM (
      SELECT sw.action
      FROM swipes sw
      JOIN listings sl ON sl.id = sw.listing_id AND sl.embedding IS NOT NULL
      ORDER BY sl.embedding <=> t.embedding
      LIMIT k
    ) s
  ) nn ON true;
END;
$$;

-- 6. Index for faster k-NN lookups
CREATE INDEX IF NOT EXISTS idx_listings_embedding_hnsw
  ON listings USING hnsw (embedding vector_cosine_ops);
  `.trim();

  return json({
    message: 'Run this SQL in Supabase SQL Editor (https://supabase.com/dashboard → SQL Editor)',
    sql
  }, 200, request);
}

// ── GET /artists ─────────────────────────────────────────

async function handleGetArtists(request, env) {
  const res = await supa(env, 'artists?select=*&order=priority.asc,name.asc');
  const artists = await res.json();
  if (!Array.isArray(artists) || artists.length === 0) {
    return json({ artists: [] }, 200, request);
  }

  // Fetch thumbnails: for each artist, find a liked listing by that maker
  const names = artists.map(a => a.name);
  const thumbRes = await fetch(
    `${env.SUPABASE_URL}/rest/v1/listings?maker=in.(${names.map(n => encodeURIComponent('"' + n.replace(/"/g, '\\"') + '"')).join(',')})&hero_image=not.is.null&select=maker,hero_image&limit=500`,
    { headers: { 'apikey': env.SUPABASE_SERVICE_KEY, 'Authorization': `Bearer ${env.SUPABASE_SERVICE_KEY}` } }
  );
  const thumbRows = await thumbRes.json();
  const thumbMap = {};
  if (Array.isArray(thumbRows)) {
    thumbRows.forEach(r => { if (!thumbMap[r.maker]) thumbMap[r.maker] = r.hero_image; });
  }

  artists.forEach(a => { a.thumbnail = thumbMap[a.name] || null; });
  return json({ artists }, 200, request);
}

// ── POST /artists ────────────────────────────────────────

async function handleImportArtists(request, env) {
  try {
    const artists = await request.json();
    if (!Array.isArray(artists)) {
      return json({ error: 'Expected JSON array of artists' }, 400, request);
    }

    // Upsert by name (on_conflict=name for merge-duplicates)
    const res = await supa(env, 'artists?on_conflict=name', {
      method: 'POST',
      body: JSON.stringify(artists),
      headers: {
        'Prefer': 'return=representation,resolution=merge-duplicates'
      }
    });
    const result = await res.json();
    if (!Array.isArray(result)) {
      return json({ ok: false, error: result, imported: 0 }, 200, request);
    }
    return json({
      ok: true,
      imported: result.length
    }, 200, request);
  } catch (e) {
    return json({ error: e.message, stack: e.stack }, 500, request);
  }
}

// ── DELETE /artists?name=... ─────────────────────────────

async function handleDeleteArtist(request, env, url) {
  const name = url.searchParams.get('name');
  if (!name) return json({ error: 'name param required' }, 400, request);
  const res = await supa(env, `artists?name=eq.${encodeURIComponent(name)}`, {
    method: 'DELETE',
    headers: { 'Prefer': 'return=representation' }
  });
  const result = await res.json();
  return json({ ok: true, deleted: Array.isArray(result) ? result.length : 0 }, 200, request);
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
    const body = await request.json().catch(() => ({}));

    if (body.shopify) {
      // Scrape a Shopify store by key
      const count = await scrapeShopifyStore(env, body.shopify);
      const store = SHOPIFY_STORES[body.shopify];
      return json({ ok: true, new_listings: count, store: store?.name || body.shopify }, 200, request);
    }

    if (body.seller_id) {
      // Scrape a single LiveAuctioneers house
      const houseName = LA_SELLERS[body.seller_id] || 'Unknown';
      const count = await scrapeSellerListings(env, parseInt(body.seller_id), houseName);
      return json({ ok: true, new_listings: count, house: houseName }, 200, request);
    }

    // No params: return all available sources
    const laHouses = Object.entries(LA_SELLERS).map(([id, name]) => ({ id, name, type: 'liveauctioneers' }));
    const shopifyStores = Object.entries(SHOPIFY_STORES).map(([key, s]) => ({ key, name: s.name, type: 'shopify' }));
    return json({ sources: [...laHouses, ...shopifyStores], message: 'POST with {seller_id} or {shopify: "key"}' }, 200, request);
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

  // Shopify stores
  try {
    const sh = await scrapeAllShopify(env);
    total += sh;
  } catch (e) {
    console.error('[OLI] Shopify scrape failed:', e);
  }

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
  369:   'Wright',
  5584:  'Chairish Auctions'
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

    // Bulk upsert — merge to update auction_data on existing listings
    const upsertRes = await supa(env, 'listings?on_conflict=platform,platform_id', {
      method: 'POST',
      body: JSON.stringify(batch),
      headers: {
        'Prefer': 'return=representation,resolution=merge-duplicates'
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
    status: 'active',
    auction_data: {
      low_estimate: item.lowBidEstimate || null,
      high_estimate: item.highBidEstimate || null,
      start_price: item.startPrice || null,
      leading_bid: item.leadingBid || 0,
      bid_count: item.bidCount || 0,
      sale_start: item.saleStartTs ? new Date(item.saleStartTs * 1000).toISOString() : null,
      lot_end_estimate: item.lotEndTimeEstimatedTs ? new Date(item.lotEndTimeEstimatedTs * 1000).toISOString() : null,
      catalog_status: item.catalogStatus || null,
      is_live: !!item.isLiveAuction,
      is_timed: !!item.isTimedAuction,
      is_sold: !!item.isSold,
      is_passed: !!item.isPassed,
      sale_price: item.salePrice || null,
      catalog_id: item.catalogId || null
    }
  };
}

// ── Shopify Store Scraping ──────────────────────────────────

const SHOPIFY_STORES = {
  'blackmancruz': { url: 'https://www.blackmancruz.com', name: 'Blackman Cruz' },
  'thewindowla':  { url: 'https://thewindowla.com',      name: 'The Window LA' },
  'nickeykehoe':  { url: 'https://nickeykehoe.com',       name: 'Nickey Kehoe', minPrice: 50,
    excludeTypes: ['WOOD/FABRIC/WALLPAPER SAMPLE', 'CLEANING + UTILITY', 'BATH ACCESSORIES',
      'BATH TOWELS', 'SHEETS & PILLOWCASES', 'BEDDING', 'BRUSHES', 'APOTHECARY', 'CANDLES',
      'PAPER + OFFICE', 'FLATWARE', 'KITCHEN TOOLS', 'DINNERWARE', 'GARDEN ACCESSORIES'],
    excludeTags: ['Events', 'Gift', 'Samples', 'Swatch', 'Fabric'] }
};

async function scrapeShopifyStore(env, storeKey) {
  const store = SHOPIFY_STORES[storeKey];
  if (!store) return 0;

  let totalNew = 0;
  let page = 1;
  const maxPages = 6;

  while (page <= maxPages) {
    const res = await fetch(`${store.url}/products.json?limit=250&page=${page}`, {
      headers: { 'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json' }
    });

    if (!res.ok) {
      console.error(`[OLI] Shopify ${store.name} fetch failed: ${res.status}`);
      break;
    }

    const data = await res.json();
    const products = data?.products || [];
    if (products.length === 0) break;

    // Transform to our listing format, applying store-specific filters
    const batch = products
      .filter(p => {
        // Exclude by product type
        if (store.excludeTypes && store.excludeTypes.includes(p.product_type)) return false;
        // Exclude by tags
        if (store.excludeTags) {
          const tags = typeof p.tags === 'string' ? p.tags : (p.tags || []).join(',');
          if (store.excludeTags.some(t => tags.includes(t))) return false;
        }
        // Exclude by minimum price
        if (store.minPrice) {
          const price = parseFloat(p.variants?.[0]?.price) || 0;
          if (price < store.minPrice) return false;
        }
        return true;
      })
      .map(p => transformShopifyProduct(p, store)).filter(l => l.hero_image);

    // Upsert
    if (batch.length > 0) {
      const upsertRes = await supa(env, 'listings', {
        method: 'POST',
        body: JSON.stringify(batch),
        headers: { 'Prefer': 'return=representation,resolution=ignore-duplicates' }
      });
      const inserted = await upsertRes.json();
      totalNew += Array.isArray(inserted) ? inserted.length : 0;
    }

    page++;
    await sleep(500);
  }

  console.log(`[OLI] Shopify ${store.name}: ${totalNew} new listings`);
  return totalNew;
}

function stripHtml(html) {
  if (!html) return '';
  return html.replace(/<[^>]*>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
}

function transformShopifyProduct(product, store) {
  const images = (product.images || []).map(img => img.src);
  const variant = product.variants?.[0] || {};
  const price = parseFloat(variant.price) || null;
  const available = variant.available !== false;

  // Extract maker from vendor (Shopify stores often use this for artist/designer name)
  const vendor = product.vendor || '';
  const maker = (vendor && vendor !== store.name && vendor !== 'The Window') ? vendor : null;

  return {
    platform: 'shopify',
    platform_id: String(product.id),
    title: product.title || 'Untitled',
    description: stripHtml(product.body_html),
    price,
    currency: 'USD',
    location: 'Los Angeles, CA',
    url: `${store.url}/products/${product.handle}`,
    image_urls: images,
    hero_image: images[0] || null,
    auction_house: store.name,
    auction_date: product.published_at || null,
    lot_number: null,
    maker,
    status: available ? 'active' : 'sold'
  };
}

async function scrapeAllShopify(env) {
  let total = 0;
  for (const key of Object.keys(SHOPIFY_STORES)) {
    try {
      total += await scrapeShopifyStore(env, key);
    } catch (e) {
      console.error(`[OLI] Shopify ${key} failed:`, e);
    }
  }
  return total;
}

// ── Craigslist Scraping ──────────────────────────────────

const CL_SEARCHES = [
  { url: 'https://losangeles.craigslist.org/search/ata', name: 'LA Antiques' },
  { url: 'https://losangeles.craigslist.org/search/ara', name: 'LA Art' },
  { url: 'https://losangeles.craigslist.org/search/fua?query=vintage', name: 'LA Vintage Furniture' },
  { url: 'https://losangeles.craigslist.org/search/fua?query=mid+century', name: 'LA MCM Furniture' },
  { url: 'https://losangeles.craigslist.org/search/fua?query=lamp', name: 'LA Lamps' },
  { url: 'https://losangeles.craigslist.org/search/fua?query=sculpture', name: 'LA Sculpture' }
];

const CL_UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36';

async function handleScrapeCraigslist(request, env) {
  try {
    // Only scrape ONE search per call to stay under subrequest limit
    // Rotate through searches using a simple index
    const url = new URL(request.url);
    const idxParam = url.searchParams.get('idx');
    const idx = idxParam != null ? parseInt(idxParam) : null;

    if (idx != null && idx >= 0 && idx < CL_SEARCHES.length) {
      // Scrape a specific search
      const search = CL_SEARCHES[idx];
      const count = await scrapeCraigslistSearch(env, search);
      return json({ ok: true, new_listings: count, search: search.name, next_idx: (idx + 1) % CL_SEARCHES.length }, 200, request);
    }

    // Default: scrape the first one, return next index
    const search = CL_SEARCHES[0];
    const count = await scrapeCraigslistSearch(env, search);
    return json({ ok: true, new_listings: count, search: search.name, total_searches: CL_SEARCHES.length, next_idx: 1 }, 200, request);
  } catch (e) {
    return json({ error: e.message }, 500, request);
  }
}

async function scrapeCraigslistSearch(env, search) {
  // Step 1: Fetch search results page
  const res = await fetch(search.url, {
    headers: { 'User-Agent': CL_UA, 'Accept': 'text/html' }
  });
  if (!res.ok) {
    console.error(`[OLI] CL search failed: ${res.status}`);
    return 0;
  }
  const html = await res.text();

  // Step 2: Parse static search results
  const resultPattern = /<li[^>]*class="[^"]*cl-static-search-result[^"]*"[^>]*>(.*?)<\/li>/gs;
  const listings = [];
  let match;
  while ((match = resultPattern.exec(html)) !== null) {
    const r = match[1];
    const urlMatch = r.match(/href="([^"]+)"/);
    const titleMatch = r.match(/<div class="title">(.*?)<\/div>/);
    const priceMatch = r.match(/<div class="price">([^<]*)<\/div>/);
    const locMatch = r.match(/<div class="location">\s*(.*?)\s*<\/div>/s);
    if (!urlMatch) continue;

    const url = urlMatch[1];
    const clId = url.match(/\/(\d+)\.html/)?.[1];
    if (!clId) continue;

    const priceStr = (priceMatch?.[1] || '').replace(/[^0-9.]/g, '');
    listings.push({
      platform_id: `cl_${clId}`,
      title: (titleMatch?.[1] || 'Untitled').trim(),
      price: priceStr ? parseFloat(priceStr) : null,
      location: (locMatch?.[1] || 'Los Angeles').trim(),
      url
    });
  }

  if (listings.length === 0) {
    console.log(`[OLI] CL ${search.name}: no results found`);
    return 0;
  }

  // Step 3: Check which listings are already in DB (single query for all CL listings)
  const existingIds = new Set();
  const checkRes = await supa(env, `listings?platform=eq.craigslist&select=platform_id`, { headers: { 'Range': '0-9999' } });
  const existing = await checkRes.json();
  if (Array.isArray(existing)) existing.forEach(e => existingIds.add(e.platform_id));

  const newListings = listings.filter(l => !existingIds.has(l.platform_id));
  if (newListings.length === 0) {
    console.log(`[OLI] CL ${search.name}: ${listings.length} found, 0 new`);
    return 0;
  }

  // Step 4: Fetch individual pages for images (limit to 8 per run to stay under 50 subrequest limit)
  const toFetch = newListings.slice(0, 8);
  const batch = [];

  for (const listing of toFetch) {
    try {
      const pageRes = await fetch(listing.url, {
        headers: { 'User-Agent': CL_UA, 'Accept': 'text/html' }
      });
      if (!pageRes.ok) continue;
      const pageHtml = await pageRes.text();

      // Extract first image
      const imgMatch = pageHtml.match(/"(https:\/\/images\.craigslist\.org\/[^"]+_600x450\.jpg)"/);
      const heroImage = imgMatch?.[1] || null;

      // Extract description
      const bodyMatch = pageHtml.match(/id="postingbody"[^>]*>(.*?)<\/section>/s);
      let description = '';
      if (bodyMatch) {
        description = bodyMatch[1].replace(/<[^>]*>/g, ' ').replace(/QR Code Link to This Post/i, '').replace(/\s+/g, ' ').trim();
      }

      batch.push({
        platform: 'craigslist',
        platform_id: listing.platform_id,
        title: listing.title,
        description: description || listing.title,
        price: listing.price,
        currency: 'USD',
        location: listing.location || 'Los Angeles, CA',
        url: listing.url,
        image_urls: heroImage ? [heroImage] : [],
        hero_image: heroImage,
        auction_house: 'Craigslist LA',
        auction_date: new Date().toISOString(),
        lot_number: null,
        maker: null,
        status: 'active'
      });

      await sleep(200); // Be nice to CL servers
    } catch (e) {
      console.error(`[OLI] CL fetch failed for ${listing.url}:`, e);
    }
  }

  // Step 5: Upsert to DB
  if (batch.length > 0) {
    const upsertRes = await supa(env, 'listings', {
      method: 'POST',
      body: JSON.stringify(batch),
      headers: { 'Prefer': 'return=representation,resolution=ignore-duplicates' }
    });
    const inserted = await upsertRes.json();
    const count = Array.isArray(inserted) ? inserted.length : 0;
    console.log(`[OLI] CL ${search.name}: ${listings.length} found, ${newListings.length} new, ${count} inserted`);
    return count;
  }

  return 0;
}

// ── AI Processing ─────────────────────────────────────────

async function processUnembeddedListings(env, batchSize = 10) {
  // Each listing: 1 image fetch + 1 Gemini + 1 embedding + 1 Supabase write = 4 subrequests
  // Worker limit: 50 subrequests. With batchSize=10, that's 40 + a few overhead = safe.
  const res = await supa(env,
    `listings?ai_description=is.null&status=eq.active&hero_image=not.is.null&select=id,title,description,hero_image&limit=${batchSize}`
  );
  const unprocessed = await res.json();

  if (!unprocessed || unprocessed.length === 0) {
    console.log('[OLI] No listings to process');
    return { processed: 0, remaining: 0 };
  }

  let processed = 0;

  for (const listing of unprocessed) {
    try {
      // Step 1: Generate AI description + maker from image
      const aiResult = await generateDescription(env, listing);
      if (!aiResult) continue;
      const aiDesc = typeof aiResult === 'string' ? aiResult : aiResult.description;
      const maker = (typeof aiResult === 'object' ? aiResult.maker : null);

      // Step 2: Generate embedding from combined text (auction data + AI description)
      const combinedText = [listing.title, listing.description, aiDesc].filter(Boolean).join('. ');
      const embedding = await generateEmbedding(env, combinedText);
      if (!embedding) continue;

      // Step 3: Update listing with description, embedding, and maker if found
      const updateData = { ai_description: aiDesc, embedding };
      if (maker) updateData.maker = maker;
      await supa(env, `listings?id=eq.${listing.id}`, {
        method: 'PATCH',
        body: JSON.stringify(updateData)
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
    if (!imgRes.ok) throw new Error(`HTTP ${imgRes.status}`);
    const imgBuf = await imgRes.arrayBuffer();
    // Skip images over 2MB to avoid CPU time limit
    if (imgBuf.byteLength > 2 * 1024 * 1024) {
      console.log(`[OLI] Skipping large image (${(imgBuf.byteLength / 1024 / 1024).toFixed(1)}MB) for ${listing.id}`);
      return { description: generateTextOnlyDescription(listing), maker: null };
    }
    // Use chunked approach to avoid max call stack with spread operator
    const bytes = new Uint8Array(imgBuf);
    let binary = '';
    const CHUNK = 8192;
    for (let i = 0; i < bytes.length; i += CHUNK) {
      binary += String.fromCharCode.apply(null, bytes.subarray(i, i + CHUNK));
    }
    imageBase64 = btoa(binary);
  } catch (e) {
    console.error(`[OLI] Image fetch failed for ${listing.id}:`, e);
    return { description: generateTextOnlyDescription(listing), maker: null };
  }

  const mimeType = listing.hero_image.includes('.png') ? 'image/png' : 'image/jpeg';

  const prompt = `Analyze this auction/sale listing image of a vintage, antique, or decorative object. Return a JSON object with two fields:

1. "description": A single paragraph optimized for embedding similarity search. Include:
   - Object type (vase, chair, sculpture, lamp, painting, textile, etc.)
   - Style/period (mid-century modern, art deco, brutalist, primitive, folk art, Memphis, postmodern, Arts & Crafts, etc.)
   - Material (ceramic, stoneware, wood, brass, bronze, stone, glass, etc.)
   - Color palette and surface qualities
   - Aesthetic qualities (organic form, geometric, sculptural, minimal, ornate, textured, etc.)
   - Size impression (small decorative, table-scale, furniture-scale)
   Write a rich, descriptive paragraph. Do not say "this is" or "the image shows".

2. "maker": The artist, designer, or maker name if identifiable from the title, description, or image. Return null if unknown or if it's a mass-produced item. Only include individual artist/designer names, not company or auction house names.

Title from listing: "${listing.title || 'Unknown'}"
${listing.description ? `Description: "${listing.description}"` : ''}

Return ONLY valid JSON like: {"description": "...", "maker": "..." or null}`;

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

  if (!res.ok) {
    console.error(`[OLI] Gemini API error: ${res.status}`);
    return null;
  }

  let data;
  try {
    data = await res.json();
  } catch (e) {
    console.error('[OLI] Gemini response parse error:', e);
    return null;
  }
  const text = data?.candidates?.[0]?.content?.parts?.[0]?.text || null;
  if (!text) return null;

  // Try to parse as JSON (new format with maker extraction)
  try {
    const cleaned = text.replace(/```json\s*/g, '').replace(/```\s*/g, '').trim();
    const parsed = JSON.parse(cleaned);
    return { description: parsed.description, maker: parsed.maker || null };
  } catch {
    // Fallback: old format (plain text description)
    return { description: text, maker: null };
  }
}

function generateTextOnlyDescription(listing) {
  // Fallback: when no image, use the auction house's own catalog data
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

  if (!res.ok) {
    console.error(`[OLI] Embedding API error: ${res.status}`);
    return null;
  }
  let data;
  try {
    data = await res.json();
  } catch (e) {
    console.error('[OLI] Embedding response parse error:', e);
    return null;
  }
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
