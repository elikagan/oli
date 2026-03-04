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
  let artists = [];
  let swipedIds = new Set();
  let currentTab = 'scout';
  let isDragging = false;
  let startX = 0, startY = 0, deltaX = 0, deltaY = 0;
  let isLoading = false;
  let menuOpen = false;

  // ── DOM refs ────────────────────────────────────────────
  const scoutView = document.getElementById('scout-view');
  const investigateView = document.getElementById('investigate-view');
  const artistsView = document.getElementById('artists-view');
  const settingsView = document.getElementById('settings-view');
  const cardStack = document.getElementById('card-stack');
  const emptyState = document.getElementById('empty-state');
  const favList = document.getElementById('fav-list');
  const favEmpty = document.getElementById('fav-empty');
  const artistList = document.getElementById('artist-list');
  const artistEmpty = document.getElementById('artist-empty');
  const savedCount = document.getElementById('saved-count');
  const artistsCount = document.getElementById('artists-count');
  const tabbar = document.getElementById('tabbar');
  const menuBtn = document.getElementById('menu-btn');
  const menuDropdown = document.getElementById('menu-dropdown');
  const cfgCancel = document.getElementById('cfg-cancel');
  const settingsStats = document.getElementById('settings-stats');
  const accuracyChartWrap = document.getElementById('accuracy-chart-wrap');
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
      const data = await apiFetch('/feed?limit=' + BATCH_SIZE);
      return data.listings || [];
    } catch (e) {
      console.error('Feed fetch failed:', e);
      return [];
    }
  }

  async function recordSwipe(listingId, action) {
    swipedIds.add(listingId);
    saveSwipedIds();
    const listing = feed.find(l => l.id === listingId);
    const predicted_score = listing?.similarity != null ? Math.round(listing.similarity * 100) : null;
    try {
      await apiFetch('/swipe', {
        method: 'POST',
        body: JSON.stringify({ listing_id: listingId, action, predicted_score })
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

  async function fetchArtists() {
    try {
      const data = await apiFetch('/artists');
      return data.artists || [];
    } catch (e) {
      console.error('Artists fetch failed:', e);
      return [];
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

  async function fetchAccuracyHistory() {
    try {
      return await apiFetch('/stats/accuracy-history');
    } catch (e) {
      console.error('Accuracy history fetch failed:', e);
      return { points: [] };
    }
  }

  // ── Card Rendering ──────────────────────────────────────
  function esc(s) {
    const d = document.createElement('div');
    d.textContent = s || '';
    return d.innerHTML;
  }

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
    const price = listing.price ? '$' + Number(listing.price).toLocaleString() : '';
    const score = listing.similarity != null ? Math.round(listing.similarity * 100) : null;
    const maker = listing.maker || '';
    const ad = listing.auction_data || {};

    let urgencyHtml = '';
    if (ad.lot_end_estimate || ad.sale_start) {
      const endDate = new Date(ad.lot_end_estimate || ad.sale_start);
      const now = new Date();
      const hoursLeft = Math.max(0, (endDate - now) / 3600000);
      const bids = ad.bid_count || 0;
      const parts = [];
      if (hoursLeft < 24) parts.push(`${Math.round(hoursLeft)}h left`);
      else if (hoursLeft < 168) parts.push(`${Math.round(hoursLeft / 24)}d left`);
      if (bids > 0) parts.push(`${bids} bid${bids > 1 ? 's' : ''}`);
      if (parts.length) urgencyHtml = `<span class="card-urgency${hoursLeft < 24 ? ' hot' : ''}">${esc(parts.join(' · '))}</span>`;
    }

    let estimateHtml = '';
    if (ad.low_estimate && ad.high_estimate) {
      estimateHtml = `<span class="card-estimate">Est $${Number(ad.low_estimate).toLocaleString()}\u2013$${Number(ad.high_estimate).toLocaleString()}</span>`;
    }

    card.innerHTML = `
      <div class="card-image" style="background-color: #f0f0f0">
        <span class="card-badge">${esc(listing.platform)}</span>
        ${score !== null ? `<span class="card-score">${score}%</span>` : ''}
        ${urgencyHtml}
        <div class="card-overlay like">LIKE</div>
        <div class="card-overlay skip">SKIP</div>
        <div class="card-overlay fav">\u2605</div>
      </div>
      <div class="card-info">
        <div class="card-title">${esc(listing.title)}</div>
        ${maker ? `<div class="card-maker">${esc(maker)}</div>` : ''}
        <div class="card-meta">
          <span>${esc(meta)}</span>
          ${price ? `<span class="card-price">${price}</span>` : ''}
        </div>
        ${estimateHtml}
      </div>
    `;

    if (heroUrl) {
      preloadImage(heroUrl).then(ok => {
        if (ok) {
          card.querySelector('.card-image').style.backgroundImage = `url('${heroUrl}')`;
        }
      });
    }

    return card;
  }

  function preloadUpcoming() {
    const start = feedIndex + 3;
    const end = Math.min(start + 5, feed.length);
    for (let i = start; i < end; i++) {
      if (feed[i] && feed[i].hero_image) {
        preloadImage(feed[i].hero_image);
      }
    }
  }

  function renderCards() {
    cardStack.innerHTML = '';
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
      const zIndex = 5 - i;
      const card = createCard(listing, zIndex);
      if (i === 1) card.style.transform = 'scale(0.96) translateY(8px)';
      if (i === 2) card.style.transform = 'scale(0.92) translateY(16px)';
      cardStack.appendChild(card);
    });

    attachSwipeHandlers(cardStack.firstElementChild);
    preloadUpcoming();
  }

  function advanceCard() {
    feedIndex++;

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

    const nextIdx = feedIndex + cards.length;
    if (nextIdx < feed.length) {
      const card = createCard(feed[nextIdx], 5 - cards.length);
      card.style.transform = 'scale(0.92) translateY(16px)';
      cardStack.appendChild(card);
    }

    preloadUpcoming();

    // Auto-fetch when running low or out of cards
    if (feed.length - feedIndex <= PREFETCH_THRESHOLD) {
      loadMoreFeed().then(() => {
        // If we ran out and got more, re-render
        if (cards.length === 0 && feed.length > feedIndex) {
          renderCards();
        }
      });
    }

    if (cards.length === 0 && feed.length <= feedIndex) {
      emptyState.classList.remove('hidden');
      actionBtns.classList.add('hidden');
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
      card.remove();
      advanceCard();
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
  const pullIndicator = document.getElementById('pull-indicator');
  const scoutContent = document.getElementById('scout-content');
  let pullStartY = 0;
  let isPulling = false;
  let isRefreshing = false;
  const PULL_THRESHOLD = 70;
  const PULL_MAX = 120;

  function initPullToRefresh() {
    document.addEventListener('touchstart', (e) => {
      if (currentTab !== 'scout' || isRefreshing) return;
      if (e.target.closest('.card') || e.target.closest('.action-btn') || e.target.closest('#tabbar') || e.target.closest('#topbar')) return;
      pullStartY = e.touches[0].clientY;
      isPulling = true;
    }, { passive: true });

    document.addEventListener('touchmove', (e) => {
      if (!isPulling || currentTab !== 'scout' || isRefreshing) return;
      const dy = e.touches[0].clientY - pullStartY;
      if (dy > 0) {
        // Rubber-band: diminishing returns past threshold
        const pull = Math.min(dy * 0.5, PULL_MAX);
        scoutContent.style.transition = 'none';
        scoutContent.style.transform = `translateY(${pull}px)`;
        pullIndicator.style.transition = 'none';
        pullIndicator.style.top = `${pull - 40}px`;
        pullIndicator.classList.add('visible');

        // Rotate spinner based on pull progress (visual feedback before spinning)
        const spinnerEl = pullIndicator.querySelector('.spinner');
        if (dy < PULL_THRESHOLD) {
          const angle = (dy / PULL_THRESHOLD) * 360;
          spinnerEl.style.animation = 'none';
          spinnerEl.style.transform = `rotate(${angle}deg)`;
          spinnerEl.style.borderTopColor = 'var(--light)';
        } else {
          // Past threshold — start spinning and darken
          spinnerEl.style.animation = '';
          spinnerEl.style.transform = '';
          spinnerEl.style.borderTopColor = 'var(--black)';
        }
      }
    }, { passive: true });

    document.addEventListener('touchend', async () => {
      if (!isPulling || isRefreshing) return;
      isPulling = false;

      const currentY = parseFloat(scoutContent.style.transform.replace(/[^0-9.-]/g, '')) || 0;

      if (currentY >= PULL_THRESHOLD * 0.5) {
        // Triggered — hold at a small offset while loading
        isRefreshing = true;
        scoutContent.style.transition = 'transform 0.3s cubic-bezier(0.2, 0, 0, 1)';
        scoutContent.style.transform = 'translateY(50px)';
        pullIndicator.style.transition = 'top 0.3s cubic-bezier(0.2, 0, 0, 1)';
        pullIndicator.style.top = '10px';
        const spinnerEl = pullIndicator.querySelector('.spinner');
        spinnerEl.style.animation = '';
        spinnerEl.style.transform = '';
        spinnerEl.style.borderTopColor = 'var(--black)';

        // Do the actual refresh
        feedIndex = 0;
        feed = await fetchFeed();
        renderCards();

        // Snap back
        scoutContent.style.transition = 'transform 0.3s cubic-bezier(0.2, 0, 0, 1)';
        scoutContent.style.transform = '';
        pullIndicator.style.transition = 'top 0.3s, opacity 0.2s';
        pullIndicator.style.top = '-50px';
        pullIndicator.classList.remove('visible');
        isRefreshing = false;
      } else {
        // Not enough pull — snap back
        scoutContent.style.transition = 'transform 0.3s cubic-bezier(0.2, 0, 0, 1)';
        scoutContent.style.transform = '';
        pullIndicator.style.transition = 'top 0.3s, opacity 0.2s';
        pullIndicator.style.top = '-50px';
        pullIndicator.classList.remove('visible');
      }
    });
  }

  // ── Hamburger Menu ────────────────────────────────────
  function toggleMenu() {
    menuOpen = !menuOpen;
    menuDropdown.classList.toggle('hidden', !menuOpen);
  }

  function closeMenu() {
    menuOpen = false;
    menuDropdown.classList.add('hidden');
  }

  function handleMenuAction(action) {
    closeMenu();
    if (action === 'stats') {
      switchTab('settings');
    } else if (action === 'refresh') {
      if (currentTab !== 'scout') switchTab('scout');
      doRefresh();
    }
  }

  async function doRefresh() {
    loadingEl.classList.remove('hidden');
    cardStack.innerHTML = '';
    emptyState.classList.add('hidden');
    actionBtns.classList.add('hidden');
    feedIndex = 0;
    feed = await fetchFeed();
    loadingEl.classList.add('hidden');
    renderCards();
  }

  // ── Investigate List ────────────────────────────────────
  function renderFavorites() {
    savedCount.textContent = favorites.length ? `(${favorites.length})` : '';

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

  // ── Artist Targets List ────────────────────────────────

  function getLinkLabel(url) {
    try {
      const host = new URL(url).hostname.replace('www.', '');
      if (host.includes('artnet')) return 'Artnet';
      if (host.includes('wikipedia')) return 'Wikipedia';
      if (host.includes('instagram')) return 'Instagram';
      if (host.includes('artsy')) return 'Artsy';
      if (host.includes('gagosian') || host.includes('gallery') || host.includes('art')) return 'Gallery';
      return host.split('.')[0].charAt(0).toUpperCase() + host.split('.')[0].slice(1);
    } catch {
      return 'Link';
    }
  }

  function renderArtists() {
    artistsCount.textContent = artists.length ? `(${artists.length})` : '';

    if (artists.length === 0) {
      artistList.innerHTML = '';
      artistEmpty.classList.remove('hidden');
      return;
    }

    artistEmpty.classList.add('hidden');
    artistList.innerHTML = artists.map((a, idx) => {
      const age = a.age ? `${a.age}` : '';
      const loc = a.location || '';
      const meta = [age ? `Age ${age}` : '', loc].filter(Boolean).join(' \u00B7 ');
      const repClass = a.rep_status || 'rep-none';
      const repLabel = a.rep_label || 'Unknown';

      // Links
      const links = (a.links || []).map(url =>
        `<a href="${esc(url)}" target="_blank" rel="noopener" class="artist-link">${esc(getLinkLabel(url))}</a>`
      ).join('');

      // Notes
      const notesHtml = a.notes ? `<div class="artist-notes">${esc(a.notes)}</div>` : '';

      // Detail rows
      const detailRows = [];
      if (a.medium) detailRows.push(`<div class="artist-detail-row"><span class="artist-detail-label">Medium</span><span class="artist-detail-val">${esc(a.medium)}</span></div>`);
      if (a.location) detailRows.push(`<div class="artist-detail-row"><span class="artist-detail-label">Location</span><span class="artist-detail-val">${esc(a.location)}</span></div>`);
      if (a.birth_year) detailRows.push(`<div class="artist-detail-row"><span class="artist-detail-label">Born</span><span class="artist-detail-val">${a.birth_year}${age ? ` (age ${age})` : ''}</span></div>`);

      return `
        <div class="artist-item" data-artist-idx="${idx}">
          <div class="artist-priority ${esc(a.priority || 'med')}"></div>
          <div class="artist-info">
            <div class="artist-name">${esc(a.name)}</div>
            <div class="artist-medium">${esc(a.medium || '')}</div>
            <span class="artist-rep ${esc(repClass)}">${esc(repLabel)}</span>
          </div>
          <span class="artist-chevron">\u203A</span>
        </div>
        <div class="artist-detail hidden" data-detail-idx="${idx}">
          ${detailRows.join('')}
          ${links ? `<div class="artist-links">${links}</div>` : ''}
          ${notesHtml}
        </div>
      `;
    }).join('');

    // Attach expand/collapse handlers
    artistList.querySelectorAll('.artist-item').forEach(item => {
      item.addEventListener('click', () => {
        const idx = item.dataset.artistIdx;
        const detail = artistList.querySelector(`.artist-detail[data-detail-idx="${idx}"]`);
        const isExpanded = item.classList.contains('expanded');

        // Collapse all others
        artistList.querySelectorAll('.artist-item.expanded').forEach(other => {
          other.classList.remove('expanded');
          const otherDetail = artistList.querySelector(`.artist-detail[data-detail-idx="${other.dataset.artistIdx}"]`);
          if (otherDetail) otherDetail.classList.add('hidden');
        });

        if (!isExpanded) {
          item.classList.add('expanded');
          detail.classList.remove('hidden');
        }
      });
    });
  }

  // ── Accuracy Chart ─────────────────────────────────────
  function renderAccuracyChart(points) {
    if (!points || points.length === 0) {
      accuracyChartWrap.innerHTML = `
        <div class="accuracy-chart-wrap">
          <div class="accuracy-chart-title">Swipe Prediction</div>
          <div class="accuracy-chart-empty">Need more swipes to show chart</div>
        </div>
      `;
      return;
    }

    const W = 300, H = 140;
    const padL = 28, padR = 8, padT = 8, padB = 28;
    const chartW = W - padL - padR;
    const chartH = H - padT - padB;

    // Always show 40-100% range for context (50% = coin flip)
    const minY = 40;
    const maxY = 100;
    const rangeY = maxY - minY;

    const coords = points.map((p, i) => {
      const x = padL + (i / Math.max(1, points.length - 1)) * chartW;
      const y = padT + chartH - ((Math.max(minY, Math.min(maxY, p.accuracy)) - minY) / rangeY) * chartH;
      return `${x.toFixed(1)},${y.toFixed(1)}`;
    });

    // Grid lines
    let gridLines = '';
    for (const pct of [50, 75, 100]) {
      const y = padT + chartH - ((pct - minY) / rangeY) * chartH;
      gridLines += `<line class="grid" x1="${padL}" y1="${y.toFixed(1)}" x2="${W - padR}" y2="${y.toFixed(1)}"/>`;
      gridLines += `<text class="axis-label" x="${padL - 4}" y="${(y + 3).toFixed(1)}" text-anchor="end">${pct}%</text>`;
    }

    // 50% baseline (coin flip / random)
    const baseline50 = padT + chartH - ((50 - minY) / rangeY) * chartH;

    // X-axis: swipe count labels
    let xLabels = '';
    const labelCount = Math.min(4, points.length);
    for (let i = 0; i < labelCount; i++) {
      const idx = labelCount === 1 ? 0 : Math.round(i * (points.length - 1) / (labelCount - 1));
      const x = padL + (idx / Math.max(1, points.length - 1)) * chartW;
      const y = padT + chartH + 14;
      xLabels += `<text class="axis-label" x="${x.toFixed(1)}" y="${y}" text-anchor="middle">${points[idx].index}</text>`;
    }
    // "swipes" label at bottom right
    xLabels += `<text class="axis-label" x="${W - padR}" y="${padT + chartH + 24}" text-anchor="end" style="font-size:8px">swipes</text>`;

    // 50% annotation
    const coinFlipLabel = `<text class="axis-label" x="${W - padR}" y="${(baseline50 - 3).toFixed(1)}" text-anchor="end" style="font-size:7px;fill:var(--gray)">coin flip</text>`;

    const latest = points[points.length - 1].accuracy;

    accuracyChartWrap.innerHTML = `
      <div class="accuracy-chart-wrap">
        <div class="accuracy-chart-title">
          Swipe Prediction
          <span style="float:right;color:var(--black);font-weight:700;">${latest}%</span>
        </div>
        <div style="font-size:11px;color:var(--gray);margin-bottom:8px;">How often OLI correctly guesses your swipe</div>
        <svg class="accuracy-chart" viewBox="0 0 ${W} ${H}" preserveAspectRatio="xMidYMid meet">
          ${gridLines}
          <line class="baseline" x1="${padL}" y1="${baseline50.toFixed(1)}" x2="${W - padR}" y2="${baseline50.toFixed(1)}"/>
          ${coinFlipLabel}
          <polyline class="line" points="${coords.join(' ')}"/>
          ${xLabels}
        </svg>
      </div>
    `;
  }

  // ── Navigation ──────────────────────────────────────────
  function switchTab(tab) {
    currentTab = tab;
    closeMenu();

    scoutView.classList.add('hidden');
    investigateView.classList.add('hidden');
    artistsView.classList.add('hidden');
    settingsView.classList.add('hidden');

    if (tab === 'scout') {
      scoutView.classList.remove('hidden');
      if (feed.length === 0 && config.workerUrl) {
        loadMoreFeed().then(() => { if (feed.length) renderCards(); });
      }
    } else if (tab === 'investigate') {
      investigateView.classList.remove('hidden');
      loadFavorites();
    } else if (tab === 'artists') {
      artistsView.classList.remove('hidden');
      loadArtists();
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

  async function loadArtists() {
    artists = await fetchArtists();
    renderArtists();
  }

  async function loadStats() {
    const [stats, history] = await Promise.all([fetchStats(), fetchAccuracyHistory()]);
    if (!stats) {
      settingsStats.innerHTML = '<p>Could not load stats.</p>';
      return;
    }
    let accuracyHtml = '';
    if (stats.accuracy) {
      const a = stats.accuracy;
      accuracyHtml = `
        <div style="margin-top:16px;padding-top:12px;border-top:1px solid var(--border);">
          <div class="stat-row"><span class="stat-label" style="font-weight:700;">Correct predictions</span><span class="stat-val">${a.pct}% of ${a.total_scored} swipes</span></div>
          <div class="stat-row"><span class="stat-label">Avg confidence on likes</span><span class="stat-val">${a.avg_liked_score}%</span></div>
          <div class="stat-row"><span class="stat-label">Avg confidence on skips</span><span class="stat-val">${a.avg_skipped_score}%</span></div>
        </div>
      `;
    } else {
      accuracyHtml = `
        <div style="margin-top:16px;padding-top:12px;border-top:1px solid var(--border);">
          <div class="stat-row"><span class="stat-label">Swipe prediction</span><span class="stat-val" style="color:var(--gray);">Need 10+ swipes</span></div>
        </div>
      `;
    }

    settingsStats.innerHTML = `
      <div class="stat-row"><span class="stat-label">Right swipes</span><span class="stat-val">${stats.positive_count || 0}</span></div>
      <div class="stat-row"><span class="stat-label">Left swipes</span><span class="stat-val">${stats.negative_count || 0}</span></div>
      <div class="stat-row"><span class="stat-label">Favorites</span><span class="stat-val">${stats.favorites_count || 0}</span></div>
      <div class="stat-row"><span class="stat-label">Active listings</span><span class="stat-val">${stats.active_listings || 0}</span></div>
      ${accuracyHtml}
    `;

    renderAccuracyChart(history.points);
  }

  // ── Init ────────────────────────────────────────────────
  async function init() {
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('./sw.js').catch(() => {});
    }

    await loadSwipedIds();

    // Tab bar navigation
    tabbar.addEventListener('click', e => {
      const tab = e.target.closest('.tab');
      if (tab) switchTab(tab.dataset.tab);
    });

    // Hamburger menu
    menuBtn.addEventListener('click', (e) => {
      e.stopPropagation();
      toggleMenu();
    });

    menuDropdown.addEventListener('click', (e) => {
      const item = e.target.closest('.menu-item');
      if (item) handleMenuAction(item.dataset.action);
    });

    // Close menu on outside click
    document.addEventListener('click', (e) => {
      if (menuOpen && !e.target.closest('.menu-wrap')) {
        closeMenu();
      }
    });

    // Settings back button
    cfgCancel.addEventListener('click', () => switchTab('scout'));

    // Action buttons
    hateBtn.addEventListener('click', handleSuperHate);
    favBtn.addEventListener('click', handleFavButton);
    superBtn.addEventListener('click', handleSuperLike);

    // Pull to refresh
    initPullToRefresh();

    // Load initial feed
    loadingEl.classList.remove('hidden');
    feed = await fetchFeed();
    loadingEl.classList.add('hidden');
    renderCards();
  }

  init();
})();
