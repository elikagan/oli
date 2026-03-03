(function () {
  'use strict';

  // ── Config ──────────────────────────────────────────────
  const DB_NAME = 'oli_config';
  const DB_STORE = 'kv';
  const BATCH_SIZE = 20;
  const PREFETCH_THRESHOLD = 5;
  const SWIPE_THRESHOLD = 80; // px to trigger swipe
  const SWIPE_UP_THRESHOLD = -100; // px to trigger favorite (negative = up)
  const TILT_FACTOR = 0.15; // degrees per px of drag

  // ── State ───────────────────────────────────────────────
  let config = { workerUrl: '', supaUrl: '', supaKey: '' };
  let feed = [];           // current batch of listings
  let feedIndex = 0;       // current card index in feed
  let favorites = [];      // investigate list
  let swipedIds = new Set(); // already-swiped listing IDs (session)
  let currentTab = 'scout';
  let isDragging = false;
  let startX = 0, startY = 0, deltaX = 0, deltaY = 0;

  // ── DOM refs ────────────────────────────────────────────
  const scoutView = document.getElementById('scout-view');
  const investigateView = document.getElementById('investigate-view');
  const settingsView = document.getElementById('settings-view');
  const cardStack = document.getElementById('card-stack');
  const emptyState = document.getElementById('empty-state');
  const swipeHint = document.getElementById('swipe-hint');
  const favList = document.getElementById('fav-list');
  const favEmpty = document.getElementById('fav-empty');
  const tabbar = document.getElementById('tabbar');
  const settingsBtn = document.getElementById('settings-btn');
  const cfgSave = document.getElementById('cfg-save');
  const cfgCancel = document.getElementById('cfg-cancel');
  const cfgStatus = document.getElementById('cfg-status');
  const settingsStats = document.getElementById('settings-stats');

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

  // ── Config Load/Save ────────────────────────────────────
  async function loadConfig() {
    try {
      const saved = await dbGet('config');
      if (saved) config = { ...config, ...saved };
    } catch (e) {
      console.warn('Config load failed:', e);
    }
  }

  async function saveConfig() {
    const workerUrl = document.getElementById('cfg-worker-url').value.trim();
    const supaUrl = document.getElementById('cfg-supa-url').value.trim();
    const supaKey = document.getElementById('cfg-supa-key').value.trim();
    config = { workerUrl, supaUrl, supaKey };
    await dbSet('config', config);
    cfgStatus.textContent = 'Saved.';
    setTimeout(() => { cfgStatus.textContent = ''; }, 2000);
  }

  function populateSettings() {
    document.getElementById('cfg-worker-url').value = config.workerUrl || '';
    document.getElementById('cfg-supa-url').value = config.supaUrl || '';
    document.getElementById('cfg-supa-key').value = config.supaKey || '';
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

  function createCard(listing, index) {
    const card = document.createElement('div');
    card.className = 'card';
    card.dataset.id = listing.id;
    card.dataset.index = index;

    const meta = [
      listing.auction_house || listing.platform,
      listing.location
    ].filter(Boolean).join(' \u00B7 ');

    card.innerHTML = `
      <div class="card-image" style="background-image: url('${esc(listing.hero_image || '')}')">
        <span class="card-badge">${esc(listing.platform)}</span>
        <div class="card-overlay like">LIKE</div>
        <div class="card-overlay skip">SKIP</div>
        <div class="card-overlay fav">&starf;</div>
      </div>
      <div class="card-info">
        <div class="card-title">${esc(listing.title)}</div>
        <div class="card-meta">
          <span>${esc(meta)}</span>
        </div>
      </div>
    `;

    return card;
  }

  function renderCards() {
    cardStack.innerHTML = '';
    const remaining = feed.slice(feedIndex, feedIndex + 3);

    if (remaining.length === 0) {
      emptyState.classList.remove('hidden');
      swipeHint.classList.add('hidden');
      return;
    }

    emptyState.classList.add('hidden');
    swipeHint.classList.remove('hidden');

    remaining.forEach((listing, i) => {
      const card = createCard(listing, feedIndex + i);
      cardStack.appendChild(card);
    });

    attachSwipeHandlers(cardStack.firstElementChild);
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

    // Show overlay indicators
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
      favOverlay.style.opacity = yProgress;
      likeOverlay.style.opacity = 0;
      skipOverlay.style.opacity = 0;
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

    // Determine action
    if (deltaX > SWIPE_THRESHOLD) {
      animateOut(card, 'right');
      recordSwipe(listingId, 'right');
    } else if (deltaX < -SWIPE_THRESHOLD) {
      animateOut(card, 'left');
      recordSwipe(listingId, 'left');
    } else if (deltaY < SWIPE_UP_THRESHOLD) {
      animateOut(card, 'up');
      recordSwipe(listingId, 'favorite');
      addFavorite(listingId);
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
    card.classList.add('animating');

    const offX = direction === 'right' ? window.innerWidth * 1.5 :
                 direction === 'left' ? -window.innerWidth * 1.5 : 0;
    const offY = direction === 'up' ? -window.innerHeight : 0;
    const rotation = direction === 'right' ? 30 : direction === 'left' ? -30 : 0;

    card.style.transform = `translate(${offX}px, ${offY}px) rotate(${rotation}deg)`;
    card.style.opacity = '0';

    setTimeout(() => {
      feedIndex++;
      renderCards();

      // Prefetch if running low
      if (feed.length - feedIndex <= PREFETCH_THRESHOLD) {
        loadMoreFeed();
      }
    }, 350);
  }

  async function loadMoreFeed() {
    const more = await fetchFeed();
    if (more.length > 0) {
      feed = feed.concat(more);
    }
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

    // Remove handlers
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
    } else if (tab === 'investigate') {
      investigateView.classList.remove('hidden');
      loadFavorites();
    } else if (tab === 'settings') {
      settingsView.classList.remove('hidden');
      populateSettings();
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
    // Register service worker
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('/sw.js').catch(() => {});
    }

    // Load config
    await loadConfig();

    // Tab bar navigation
    tabbar.addEventListener('click', e => {
      const tab = e.target.closest('.tab');
      if (tab) switchTab(tab.dataset.tab);
    });

    // Settings
    settingsBtn.addEventListener('click', () => switchTab('settings'));
    cfgSave.addEventListener('click', saveConfig);
    cfgCancel.addEventListener('click', () => switchTab('scout'));

    // Check if configured
    if (!config.workerUrl) {
      switchTab('settings');
      cfgStatus.textContent = 'Configure your Worker URL to get started.';
      return;
    }

    // Load initial feed
    feed = await fetchFeed();
    renderCards();
  }

  init();
})();
