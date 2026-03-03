(function () {
  'use strict';

  // ── Config ──────────────────────────────────────────────
  const DB_NAME = 'oli_config';
  const DB_STORE = 'kv';
  const BATCH_SIZE = 20;
  const PREFETCH_THRESHOLD = 5;
  const SWIPE_THRESHOLD = 80;
  const SWIPE_UP_THRESHOLD = -100;
  const TILT_FACTOR = 0.15;

  // ── State ───────────────────────────────────────────────
  const config = {
    workerUrl: 'https://oli-api.objectlesson.workers.dev',
    supaUrl: 'https://zscgcppjkfhaqchjxqkm.supabase.co',
    supaKey: 'sb_publishable_B9lLQBVV-D-Z5d1dEk7uAg_kaYB_13B'
  };
  let feed = [];
  let feedIndex = 0;
  let favorites = [];
  let swipedIds = new Set();
  let currentTab = 'scout';
  let isDragging = false;
  let startX = 0, startY = 0, deltaX = 0, deltaY = 0;
  let isLoading = false;

  // ── DOM refs ────────────────────────────────────────────
  const scoutView = document.getElementById('scout-view');
  const investigateView = document.getElementById('investigate-view');
  const settingsView = document.getElementById('settings-view');
  const cardStack = document.getElementById('card-stack');
  const emptyState = document.getElementById('empty-state');
  const favList = document.getElementById('fav-list');
  const favEmpty = document.getElementById('fav-empty');
  const tabbar = document.getElementById('tabbar');
  const settingsBtn = document.getElementById('settings-btn');
  const cfgCancel = document.getElementById('cfg-cancel');
  const settingsStats = document.getElementById('settings-stats');
  const loadingEl = document.getElementById('loading');
  const favBtn = document.getElementById('fav-btn');
  const superBtn = document.getElementById('super-btn');
  const hateBtn = document.getElementById('hate-btn');
  const actionBtns = document.getElementById('action-btns');

  // ── IndexedDB ───────────────────────────────────────────
  function openDB() {
    return new Promise((resolve, reject) => {
      const req = indexedDB.open(DB_NAME, 1);
      req.onupgradeneeded = () => req.result.createObjectStore(DB_STORE);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async function dbGet(key) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(DB_STORE, 'readonly');
      const req = tx.objectStore(DB_STORE).get(key);
      req.onsuccess = () => resolve(req.result);
      req.onerror = () => reject(req.error);
    });
  }

  async function dbSet(key, val) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(DB_STORE, 'readwrite');
      tx.objectStore(DB_STORE).put(val, key);
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
  }

  // ── Swiped ID persistence ─────────────────────────────
  async function loadSwipedIds() {
    try {
      const saved = await dbGet('swipedIds');
      if (saved && Array.isArray(saved)) {
        swipedIds = new Set(saved);
      }
    } catch (e) {
      console.warn('Failed to load swiped IDs:', e);
    }
  }

  async function saveSwipedIds() {
    try {
      await dbSet('swipedIds', [...swipedIds]);
    } catch (e) {
      console.warn('Failed to save swiped IDs:', e);
    }
  }

  // ── API Calls ───────────────────────────────────────────
  async function apiFetch(path, opts = {}) {
    if (!config.workerUrl) throw new Error('Worker URL not configured');
    const url = config.workerUrl.replace(/\/$/, '') + path;
    const res = await fetch(url, {
      ...opts,
      headers: {
        'Content-Type': 'application/json',
        ...(opts.headers || {})
      }
    });
    if (!res.ok) throw new Error(`API ${res.status}: ${await res.text()}`);
    return res.json();
  }

  async function fetchFeed() {
    try {
      const excluded = [...swipedIds];
      const data = await apiFetch('/feed?limit=' + BATCH_SIZE + '&exclude=' + excluded.join(','));
      return data.listings || [];
    } catch (e) {
      console.error('Feed fetch failed:', e);
      return [];
    }
  }

  async function recordSwipe(listingId, action) {
    swipedIds.add(listingId);
    saveSwipedIds(); // persist to IndexedDB (fire and forget)
    try {
      await apiFetch('/swipe', {
        method: 'POST',
        body: JSON.stringify({ listing_id: listingId, action })
      });
    } catch (e) {
      console.error('Swipe record failed:', e);
    }
  }

  async function addFavorite(listingId) {
    try {
      await apiFetch('/favorites', {
        method: 'POST',
        body: JSON.stringify({ listing_id: listingId })
      });
    } catch (e) {
      console.error('Favorite add failed:', e);
    }
  }

  async function fetchFavorites() {
    try {
      const data = await apiFetch('/favorites');
      return data.favorites || [];
    } catch (e) {
      console.error('Favorites fetch failed:', e);
      return [];
    }
  }

  async function removeFavorite(favId) {
    try {
      await apiFetch('/favorites/' + favId, { method: 'DELETE' });
    } catch (e) {
      console.error('Favorite remove failed:', e);
    }
  }

  async function fetchStats() {
    try {
      return await apiFetch('/stats');
    } catch (e) {
      console.error('Stats fetch failed:', e);
      return null;
    }
  }

  // ── Card Rendering ──────────────────────────────────────
  function esc(s) {
    const d = document.createElement('div');
    d.textContent = s || '';
    return d.innerHTML;
  }

  // Preload an image and return a promise
  function preloadImage(url) {
    return new Promise((resolve) => {
      if (!url) { resolve(false); return; }
      const img = new Image();
      img.onload = () => resolve(true);
      img.onerror = () => resolve(false);
      img.src = url;
    });
  }

  function createCard(listing, zIndex) {
    const card = document.createElement('div');
    card.className = 'card';
    card.dataset.id = listing.id;
    card.style.zIndex = zIndex;

    const meta = [
      listing.auction_house || listing.platform,
      listing.location
    ].filter(Boolean).join(' \u00B7 ');

    const heroUrl = listing.hero_image || '';

    card.innerHTML = `
      <div class="card-image" style="background-color: #f0f0f0">
        <span class="card-badge">${esc(listing.platform)}</span>
        <div class="card-overlay like">LIKE</div>
        <div class="card-overlay skip">SKIP</div>
        <div class="card-overlay fav">\u2605</div>
      </div>
      <div class="card-info">
        <div class="card-title">${esc(listing.title)}</div>
        <div class="card-meta">
          <span>${esc(meta)}</span>
        </div>
      </div>
    `;

    // Load image in background, set once ready
    if (heroUrl) {
      preloadImage(heroUrl).then(ok => {
        if (ok) {
          card.querySelector('.card-image').style.backgroundImage = `url('${heroUrl}')`;
        }
      });
    }

    return card;
  }

  // Preload images ahead in the feed so they're ready when user swipes
  function preloadUpcoming() {
    const start = feedIndex + 3; // beyond visible cards
    const end = Math.min(start + 5, feed.length);
    for (let i = start; i < end; i++) {
      if (feed[i] && feed[i].hero_image) {
        preloadImage(feed[i].hero_image);
      }
    }
  }

  // Build the initial 3-card stack without destroying existing cards
  function renderCards() {
    cardStack.innerHTML = '';
    // Skip listings with no hero_image
    while (feedIndex < feed.length && !feed[feedIndex].hero_image) feedIndex++;

    const remaining = feed.slice(feedIndex, feedIndex + 3).filter(l => l.hero_image);

    if (remaining.length === 0) {
      emptyState.classList.remove('hidden');
      actionBtns.classList.add('hidden');
      return;
    }

    emptyState.classList.add('hidden');
    actionBtns.classList.remove('hidden');

    remaining.forEach((listing, i) => {
      const zIndex = 5 - i; // first card on top
      const card = createCard(listing, zIndex);
      // Scale down cards behind
      if (i === 1) card.style.transform = 'scale(0.96) translateY(8px)';
      if (i === 2) card.style.transform = 'scale(0.92) translateY(16px)';
      cardStack.appendChild(card);
    });

    attachSwipeHandlers(cardStack.firstElementChild);
    preloadUpcoming();
  }

  // After a swipe: just remove top card, promote others, add new card at back
  function advanceCard() {
    feedIndex++;

    // Promote remaining cards
    const cards = cardStack.querySelectorAll('.card:not(.animating)');
    cards.forEach((card, i) => {
      card.style.zIndex = 5 - i;
      card.style.transition = 'transform 0.25s ease-out';
      if (i === 0) {
        card.style.transform = '';
        attachSwipeHandlers(card);
      } else if (i === 1) {
        card.style.transform = 'scale(0.96) translateY(8px)';
      }
    });

    // Add next card at back of stack if available
    const nextIdx = feedIndex + cards.length;
    if (nextIdx < feed.length) {
      const card = createCard(feed[nextIdx], 5 - cards.length);
      card.style.transform = 'scale(0.92) translateY(16px)';
      cardStack.appendChild(card);
    }

    // Check if we're out of cards
    if (cards.length === 0) {
      emptyState.classList.remove('hidden');
      actionBtns.classList.add('hidden');
    }

    // Preload upcoming images
    preloadUpcoming();

    // Prefetch if running low
    if (feed.length - feedIndex <= PREFETCH_THRESHOLD) {
      loadMoreFeed();
    }
  }

  // ── Swipe Gesture Handling ──────────────────────────────
  function attachSwipeHandlers(card) {
    if (!card) return;
    card.addEventListener('pointerdown', onPointerDown);
    card.addEventListener('pointermove', onPointerMove);
    card.addEventListener('pointerup', onPointerUp);
    card.addEventListener('pointercancel', onPointerUp);
  }

  function onPointerDown(e) {
    if (isDragging) return;
    isDragging = true;
    startX = e.clientX;
    startY = e.clientY;
    deltaX = 0;
    deltaY = 0;
    e.target.closest('.card')?.setPointerCapture(e.pointerId);
  }

  function onPointerMove(e) {
    if (!isDragging) return;
    deltaX = e.clientX - startX;
    deltaY = e.clientY - startY;

    const card = e.target.closest('.card');
    if (!card) return;

    const rotation = deltaX * TILT_FACTOR;
    card.style.transition = 'none';
    card.style.transform = `translate(${deltaX}px, ${deltaY}px) rotate(${rotation}deg)`;

    const likeOverlay = card.querySelector('.card-overlay.like');
    const skipOverlay = card.querySelector('.card-overlay.skip');
    const favOverlay = card.querySelector('.card-overlay.fav');

    const xProgress = Math.min(Math.abs(deltaX) / SWIPE_THRESHOLD, 1);
    const yProgress = Math.min(Math.abs(deltaY) / Math.abs(SWIPE_UP_THRESHOLD), 1);

    if (deltaX > 20) {
      likeOverlay.style.opacity = xProgress;
      skipOverlay.style.opacity = 0;
      favOverlay.style.opacity = 0;
    } else if (deltaX < -20) {
      skipOverlay.style.opacity = xProgress;
      likeOverlay.style.opacity = 0;
      favOverlay.style.opacity = 0;
    } else if (deltaY < -20) {
      skipOverlay.style.opacity = yProgress;
      likeOverlay.style.opacity = 0;
      favOverlay.style.opacity = 0;
    } else {
      likeOverlay.style.opacity = 0;
      skipOverlay.style.opacity = 0;
      favOverlay.style.opacity = 0;
    }
  }

  function onPointerUp(e) {
    if (!isDragging) return;
    isDragging = false;

    const card = e.target.closest('.card');
    if (!card) return;

    const listingId = card.dataset.id;

    if (deltaX > SWIPE_THRESHOLD) {
      animateOut(card, 'right');
      recordSwipe(listingId, 'right');
    } else if (deltaX < -SWIPE_THRESHOLD) {
      animateOut(card, 'left');
      recordSwipe(listingId, 'left');
    } else if (deltaY < SWIPE_UP_THRESHOLD) {
      animateOut(card, 'up');
      recordSwipe(listingId, 'left');
    } else {
      // Snap back
      card.style.transition = 'transform 0.3s ease-out';
      card.style.transform = '';
      const likeOverlay = card.querySelector('.card-overlay.like');
      const skipOverlay = card.querySelector('.card-overlay.skip');
      const favOverlay = card.querySelector('.card-overlay.fav');
      if (likeOverlay) likeOverlay.style.opacity = 0;
      if (skipOverlay) skipOverlay.style.opacity = 0;
      if (favOverlay) favOverlay.style.opacity = 0;
    }
  }

  function animateOut(card, direction) {
    // Remove pointer handlers so they can't fire again
    card.removeEventListener('pointerdown', onPointerDown);
    card.removeEventListener('pointermove', onPointerMove);
    card.removeEventListener('pointerup', onPointerUp);
    card.removeEventListener('pointercancel', onPointerUp);

    card.classList.add('animating');
    card.style.transition = 'transform 0.35s ease-out, opacity 0.35s ease-out';

    const offX = direction === 'right' ? window.innerWidth * 1.5 :
                 direction === 'left' ? -window.innerWidth * 1.5 : 0;
    const offY = direction === 'up' ? -window.innerHeight : 0;
    const rotation = direction === 'right' ? 30 : direction === 'left' ? -30 : 0;

    card.style.transform = `translate(${offX}px, ${offY}px) rotate(${rotation}deg)`;
    card.style.opacity = '0';

    setTimeout(() => {
      card.remove(); // just remove the swiped card from DOM
      advanceCard(); // promote remaining cards, no rebuild
    }, 350);
  }

  // ── Action button handlers ─────────────────────────────
  function handleFavButton() {
    const topCard = cardStack.querySelector('.card:not(.animating)');
    if (!topCard) return;

    const listingId = topCard.dataset.id;
    animateOut(topCard, 'up');
    recordSwipe(listingId, 'favorite');
    addFavorite(listingId);
  }

  function handleSuperLike() {
    const topCard = cardStack.querySelector('.card:not(.animating)');
    if (!topCard) return;

    const listingId = topCard.dataset.id;
    topCard.style.boxShadow = '0 0 30px rgba(239,68,68,0.5)';
    animateOut(topCard, 'up');
    recordSwipe(listingId, 'super_like');
    addFavorite(listingId);
  }

  function handleSuperHate() {
    const topCard = cardStack.querySelector('.card:not(.animating)');
    if (!topCard) return;

    const listingId = topCard.dataset.id;
    topCard.style.boxShadow = '0 0 30px rgba(0,0,0,0.4)';
    animateOut(topCard, 'left');
    recordSwipe(listingId, 'super_hate');
  }

  async function loadMoreFeed() {
    const more = await fetchFeed();
    if (more.length > 0) {
      feed = feed.concat(more);
    }
  }

  // ── Pull to Refresh ───────────────────────────────────
  async function refreshFeed() {
    if (isLoading) return;
    isLoading = true;
    loadingEl.classList.remove('hidden');
    cardStack.innerHTML = '';
    emptyState.classList.add('hidden');

    feedIndex = 0;
    feed = await fetchFeed();
    renderCards();

    loadingEl.classList.add('hidden');
    isLoading = false;
  }

  // ── Investigate List ────────────────────────────────────
  function renderFavorites() {
    if (favorites.length === 0) {
      favList.innerHTML = '';
      favEmpty.classList.remove('hidden');
      return;
    }

    favEmpty.classList.add('hidden');
    favList.innerHTML = favorites.map(fav => {
      const listing = fav.listing || {};
      const price = listing.price ? '$' + Number(listing.price).toLocaleString() : '';
      const meta = [listing.auction_house || listing.platform, listing.location].filter(Boolean).join(' \u00B7 ');

      return `
        <div class="fav-item" data-fav-id="${esc(fav.id)}">
          <div class="fav-thumb" style="background-image: url('${esc(listing.hero_image || '')}')"></div>
          <div class="fav-info">
            <div class="fav-title">${esc(listing.title)}</div>
            <div class="fav-detail">${esc(meta)}</div>
            ${price ? `<div class="fav-price">${price}</div>` : ''}
            ${fav.status ? `<span class="fav-status ${fav.status}">${fav.status}</span>` : ''}
          </div>
          <div class="fav-actions">
            <a href="${esc(listing.url || '#')}" target="_blank" rel="noopener" class="fav-action-btn open-btn">Open</a>
            <button class="fav-action-btn remove-btn" data-fav-id="${esc(fav.id)}">&times;</button>
          </div>
        </div>
      `;
    }).join('');

    favList.querySelectorAll('.remove-btn').forEach(btn => {
      btn.addEventListener('click', async () => {
        const favId = btn.dataset.favId;
        await removeFavorite(favId);
        favorites = favorites.filter(f => f.id !== favId);
        renderFavorites();
      });
    });
  }

  // ── Navigation ──────────────────────────────────────────
  function switchTab(tab) {
    currentTab = tab;

    scoutView.classList.add('hidden');
    investigateView.classList.add('hidden');
    settingsView.classList.add('hidden');

    if (tab === 'scout') {
      scoutView.classList.remove('hidden');
      if (feed.length === 0 && config.workerUrl) {
        refreshFeed();
      }
    } else if (tab === 'investigate') {
      investigateView.classList.remove('hidden');
      loadFavorites();
    } else if (tab === 'settings') {
      settingsView.classList.remove('hidden');
      loadStats();
    }

    tabbar.querySelectorAll('.tab').forEach(t => {
      t.classList.toggle('active', t.dataset.tab === tab);
    });
  }

  async function loadFavorites() {
    favorites = await fetchFavorites();
    renderFavorites();
  }

  async function loadStats() {
    const stats = await fetchStats();
    if (!stats) {
      settingsStats.innerHTML = '<p>Could not load stats.</p>';
      return;
    }
    settingsStats.innerHTML = `
      <div class="stat-row"><span class="stat-label">Right swipes</span><span class="stat-val">${stats.positive_count || 0}</span></div>
      <div class="stat-row"><span class="stat-label">Left swipes</span><span class="stat-val">${stats.negative_count || 0}</span></div>
      <div class="stat-row"><span class="stat-label">Favorites</span><span class="stat-val">${stats.favorites_count || 0}</span></div>
      <div class="stat-row"><span class="stat-label">Active listings</span><span class="stat-val">${stats.active_listings || 0}</span></div>
    `;
  }

  // ── Init ────────────────────────────────────────────────
  async function init() {
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('./sw.js').catch(() => {});
    }

    // Load persisted swiped IDs so we don't show dupes across sessions/devices
    await loadSwipedIds();

    // Tab bar navigation
    tabbar.addEventListener('click', e => {
      const tab = e.target.closest('.tab');
      if (tab) switchTab(tab.dataset.tab);
    });

    // Settings
    settingsBtn.addEventListener('click', () => switchTab('settings'));
    cfgCancel.addEventListener('click', () => switchTab('scout'));

    // Action buttons
    hateBtn.addEventListener('click', handleSuperHate);
    favBtn.addEventListener('click', handleFavButton);
    superBtn.addEventListener('click', handleSuperLike);

    // Load initial feed with loading spinner
    isLoading = true;
    loadingEl.classList.remove('hidden');

    feed = await fetchFeed();
    renderCards();

    loadingEl.classList.add('hidden');
    isLoading = false;
  }

  init();
})();
