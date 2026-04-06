(function () {
  'use strict';

  const API = 'https://oli-api.objectlesson.workers.dev';
  let artists = [];
  let currentView = 'prospects';
  let currentArtist = null;
  let searchTimeout = null;
  let filters = { local: false, '65+': false, contact: false };

  // LA metro area cities for "Local" detection
  const LA_METRO = [
    'los angeles', 'pasadena', 'altadena', 'south pasadena', 'long beach',
    'santa monica', 'venice', 'glendale', 'burbank', 'culver city',
    'west hollywood', 'beverly hills', 'malibu', 'topanga', 'eagle rock',
    'highland park', 'silver lake', 'echo park', 'el segundo', 'inglewood',
    'torrance', 'redondo', 'hermosa', 'manhattan beach', 'san pedro',
    'woodland hills', 'encino', 'sherman oaks', 'studio city', 'north hollywood',
    'van nuys', 'calabasas', 'arcadia', 'monrovia', 'san marino',
    'la crescenta', 'la canada', 'claremont', 'pomona', 'whittier',
    'downey', 'compton', 'chatsworth'
  ];

  const SOCAL = [
    ...LA_METRO, 'san diego', 'orange county', 'irvine', 'newport beach',
    'laguna', 'costa mesa', 'anaheim', 'riverside', 'palm springs',
    'santa barbara', 'ojai', 'ventura', 'oxnard', 'thousand oaks',
    'san bernardino', 'joshua tree', 'twentynine palms'
  ];

  // ── DOM refs ────────────────────────────────────────────
  const $ = id => document.getElementById(id);
  const artistList = $('artist-list');
  const emptyState = $('empty-state');
  const loadingEl = $('loading');
  const detailOverlay = $('artist-detail');
  const emailModal = $('email-modal');
  const adminSheet = $('admin-sheet');
  const searchInput = $('search-input');
  const filterBar = $('filter-bar');
  const mainEl = $('main');

  // ── Init ────────────────────────────────────────────────
  async function init() {
    bindEvents();
    await loadArtists();
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('./sw.js');
    }
  }

  function bindEvents() {
    // View tabs
    document.querySelectorAll('.vtab').forEach(tab => {
      tab.addEventListener('click', () => {
        document.querySelectorAll('.vtab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        currentView = tab.dataset.view;
        updateFilterVisibility();
        renderArtists();
      });
    });

    // Filter chips
    document.querySelectorAll('.chip').forEach(chip => {
      chip.addEventListener('click', () => {
        chip.classList.toggle('active');
        filters[chip.dataset.filter] = chip.classList.contains('active');
        renderArtists();
      });
    });

    // Bottom tabs
    document.querySelectorAll('.tab').forEach(tab => {
      tab.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        if (tab.dataset.tab === 'admin') showAdmin();
        else hideAdmin();
      });
    });

    // Search
    searchInput.addEventListener('input', () => {
      clearTimeout(searchTimeout);
      searchTimeout = setTimeout(() => loadArtists(searchInput.value), 300);
    });

    // Detail
    $('detail-back').addEventListener('click', closeDetail);
    $('detail-status').addEventListener('change', async (e) => {
      if (!currentArtist) return;
      await api('/artists', 'PATCH', { id: currentArtist.id, status: e.target.value });
      currentArtist.status = e.target.value;
      const idx = artists.findIndex(a => a.id === currentArtist.id);
      if (idx >= 0) artists[idx].status = e.target.value;
      renderArtists();
    });

    // Email
    $('email-close').addEventListener('click', () => emailModal.classList.add('hidden'));
    $('email-generate').addEventListener('click', generateEmail);
    $('email-send').addEventListener('click', sendEmailAction);

    // Admin
    $('btn-scrape-all').addEventListener('click', scrapeAll);
    $('btn-extract').addEventListener('click', extractArtists);
    $('btn-research').addEventListener('click', researchBatch);
    adminSheet.addEventListener('click', (e) => {
      if (e.target === adminSheet) hideAdmin();
    });
  }

  function updateFilterVisibility() {
    if (currentView === 'prospects') {
      filterBar.classList.remove('hidden');
      mainEl.classList.remove('no-filters');
    } else {
      filterBar.classList.add('hidden');
      mainEl.classList.add('no-filters');
    }
  }

  // ── Location helpers ────────────────────────────────────

  function isLAMetro(loc) {
    if (!loc) return false;
    const l = loc.toLowerCase();
    return LA_METRO.some(c => l.includes(c));
  }

  function isSoCal(loc) {
    if (!loc) return false;
    const l = loc.toLowerCase();
    return SOCAL.some(c => l.includes(c));
  }

  function isCalifornia(loc) {
    if (!loc) return false;
    const l = loc.toLowerCase();
    return l.includes('california') || l.includes(', ca') || l.includes(' ca,') || isSoCal(l);
  }

  // ── Prospect Score ──────────────────────────────────────
  // Higher = better prospect for Object Lesson

  function prospectScore(a) {
    // Dead or too established = not a prospect
    if (a.alive === false) return -1;
    if (a.is_too_established) return -1;

    let score = 0;

    // Alive confirmed (vs unknown)
    if (a.alive === true) score += 10;

    // Age 65+ (our target demographic)
    if (a.estimated_age && a.estimated_age >= 65) score += 25;
    else if (a.estimated_age && a.estimated_age >= 50) score += 10;

    // Location: LA metro is gold, SoCal is good, California is okay
    if (isLAMetro(a.location)) score += 35;
    else if (isSoCal(a.location)) score += 25;
    else if (isCalifornia(a.location)) score += 15;

    // Has contact info (actionable)
    if (a.email) score += 15;
    if (a.phone) score += 10;
    if (a.website) score += 5;

    // Price range: sweet spot is $500-$25k (our market)
    const med = a.median_sale || a.avg_sale || 0;
    if (med >= 500 && med <= 25000) score += 15;
    else if (med > 0 && med < 500) score += 5;

    // Has lots = established auction presence
    const lots = a.lot_count || 0;
    score += Math.min(lots * 3, 15);

    return score;
  }

  // ── API Helper ──────────────────────────────────────────
  async function api(path, method = 'GET', body = null) {
    const opts = { method, headers: { 'Content-Type': 'application/json' } };
    if (body) opts.body = JSON.stringify(body);
    const res = await fetch(API + path, opts);
    return res.json();
  }

  // ── Load Artists ────────────────────────────────────────
  async function loadArtists(search = '') {
    loadingEl.classList.remove('hidden');
    emptyState.classList.add('hidden');

    let url = '/artists?limit=300';
    if (search) url += `&q=${encodeURIComponent(search)}`;

    try {
      const data = await api(url);
      artists = data.artists || [];
      // Compute scores
      artists.forEach(a => { a._score = prospectScore(a); });
    } catch (e) {
      console.error('Load failed:', e);
      artists = [];
    }

    loadingEl.classList.add('hidden');
    renderArtists();
    updateTabCounts();
  }

  // ── Filter + Sort ──────────────────────────────────────

  function getFilteredArtists() {
    let list = artists;

    if (currentView === 'prospects') {
      // Only alive, not too established
      list = list.filter(a => a.alive !== false && !a.is_too_established);

      // Apply active filter chips
      if (filters.local) {
        list = list.filter(a => isCalifornia(a.location));
      }
      if (filters['65+']) {
        list = list.filter(a => a.estimated_age && a.estimated_age >= 65);
      }
      if (filters.contact) {
        list = list.filter(a => a.email || a.phone || a.website);
      }

      // Sort by prospect score descending
      list.sort((a, b) => b._score - a._score);

    } else if (currentView === 'all') {
      list.sort((a, b) => b._score - a._score);

    } else {
      // Pipeline stage filter
      list = list.filter(a => a.status === currentView);
      list.sort((a, b) => b._score - a._score);
    }

    return list;
  }

  // ── Render Artist List ──────────────────────────────────

  function renderArtists() {
    const filtered = getFilteredArtists();

    if (filtered.length === 0) {
      artistList.innerHTML = '';
      emptyState.classList.remove('hidden');
      return;
    }

    emptyState.classList.add('hidden');

    artistList.innerHTML = filtered.map(a => {
      // Image strip — show up to 4 lot images
      const images = (a.image_urls || []).slice(0, 4);
      let imageHtml = '';
      if (images.length) {
        imageHtml = `<div class="card-images">${images.map(url =>
          `<img src="${esc(url)}" alt="" loading="lazy" onerror="this.parentElement.removeChild(this)">`
        ).join('')}</div>`;
      } else {
        imageHtml = `<div class="card-images"><div class="img-placeholder">${esc(a.name?.[0] || '?')}</div></div>`;
      }

      // Score
      const score = a._score || 0;
      const scoreClass = score >= 60 ? '' : score >= 30 ? 'score-mid' : 'score-low';

      // Location badge
      let locBadge = '';
      if (isLAMetro(a.location)) locBadge = '<span class="card-loc-badge badge-la">LA</span>';
      else if (isSoCal(a.location)) locBadge = '<span class="card-loc-badge badge-socal">SoCal</span>';
      else if (isCalifornia(a.location)) locBadge = '<span class="card-loc-badge badge-ca">CA</span>';

      // Detail line: location, age, price
      const details = [];
      if (a.location) {
        let loc = a.location;
        if (loc.includes(',')) {
          const parts = loc.split(',').map(s => s.trim());
          loc = parts.length > 2 ? parts.slice(0, 2).join(', ') : loc;
        }
        details.push(locBadge + esc(loc));
      }
      if (a.alive === false) {
        details.push('<span style="color:var(--red)">Deceased</span>');
      } else if (a.estimated_age) {
        details.push(a.estimated_age + ' years old');
      }
      if (a.median_sale) {
        details.push('$' + fmtPrice(a.median_sale) + ' median sale');
      }
      if (a.lot_count) {
        details.push('<span style="font-family:var(--mono)">' + a.lot_count + '</span> auction lots');
      }

      // Contact links
      const contacts = [];
      if (a.email) contacts.push(`<span class="card-contact-link">Email</span>`);
      if (a.phone) contacts.push(`<span class="card-contact-link">Phone</span>`);
      if (a.website) contacts.push(`<span class="card-contact-link">Website</span>`);
      if (a.instagram) contacts.push(`<span class="card-contact-link">Instagram</span>`);

      // Banners
      let banner = '';
      if (a.is_too_established) banner = '<div class="card-established-banner">Too established for outreach</div>';

      return `
        <div class="artist-card" data-id="${a.id}">
          ${imageHtml}
          <div class="card-body">
            <div class="card-row-top">
              <div class="card-name">${esc(a.name)}</div>
              ${score > 0 ? `<div class="card-score ${scoreClass}">${score}</div>` : ''}
            </div>
            <div class="card-details">
              ${details.map(d => `<div class="card-detail-line">${d}</div>`).join('')}
            </div>
            ${contacts.length ? `<div class="card-contact">${contacts.join('')}</div>` : ''}
            ${banner}
          </div>
        </div>`;
    }).join('');

    artistList.querySelectorAll('.artist-card').forEach(card => {
      card.addEventListener('click', () => openDetail(parseInt(card.dataset.id)));
    });
  }

  // ── Tab counts ─────────────────────────────────────────

  function updateTabCounts() {
    const alive = artists.filter(a => a.alive !== false && !a.is_too_established);
    const counts = {
      prospects: alive.length,
      contacted: artists.filter(a => a.status === 'contacted').length,
      responded: artists.filter(a => a.status === 'responded').length,
      deal: artists.filter(a => a.status === 'deal').length,
      consigning: artists.filter(a => a.status === 'consigning').length,
      all: artists.length,
    };

    document.querySelectorAll('.vtab').forEach(tab => {
      const v = tab.dataset.view;
      const existing = tab.querySelector('.vtab-count');
      if (existing) existing.remove();
      const n = counts[v] || 0;
      if (n > 0) {
        const span = document.createElement('span');
        span.className = 'vtab-count';
        span.textContent = n;
        tab.appendChild(span);
      }
    });
  }

  // ── Artist Detail ───────────────────────────────────────

  async function openDetail(id) {
    detailOverlay.classList.remove('hidden');
    $('detail-name').textContent = 'Loading...';
    $('detail-meta').textContent = '';
    $('detail-summary').innerHTML = '';
    $('detail-contact').innerHTML = '';
    $('detail-images').innerHTML = '<div class="loading-state"><div class="spinner"></div></div>';
    $('detail-lots').innerHTML = '';
    $('detail-outreach').innerHTML = '';
    $('detail-action-btns').innerHTML = '';

    try {
      const data = await api(`/artists/${id}`);
      currentArtist = data.artist;
      renderDetail(data.artist, data.lots, data.outreach);
    } catch (e) {
      console.error('Detail load failed:', e);
      $('detail-name').textContent = 'Error loading artist';
    }
  }

  function renderDetail(artist, lots, outreach) {
    $('detail-name').textContent = artist.name;
    const metaParts = [];
    if (artist.estimated_age && artist.alive !== false) metaParts.push(`Age ~${artist.estimated_age}`);
    if (artist.alive === false) metaParts.push('Deceased' + (artist.death_year ? ` (${artist.death_year})` : ''));
    if (artist.location) metaParts.push(artist.location);
    if (artist._score > 0 || artist.alive !== false) {
      const s = prospectScore(artist);
      if (s > 0) metaParts.push(`Score: ${s}`);
    }
    $('detail-meta').textContent = metaParts.join(' · ');

    const statusSel = $('detail-status');
    statusSel.innerHTML = ['lead','contacted','responded','deal','consigning','passed','disqualified']
      .map(s => `<option value="${s}" ${s === artist.status ? 'selected' : ''}>${s}</option>`)
      .join('');

    if (artist.ai_summary) {
      $('detail-summary').innerHTML = `
        <div class="section-label">About</div>
        <div class="detail-summary-text">${esc(artist.ai_summary)}</div>`;
    }

    const contacts = [];
    if (artist.email) contacts.push({ label: 'Email', value: `<a href="mailto:${esc(artist.email)}">${esc(artist.email)}</a>` });
    if (artist.website) contacts.push({ label: 'Website', value: `<a href="${esc(artist.website)}" target="_blank">${esc(artist.website.replace(/^https?:\/\//, ''))}</a>` });
    if (artist.instagram) contacts.push({ label: 'Instagram', value: `<a href="https://instagram.com/${esc(artist.instagram.replace('@',''))}" target="_blank">${esc(artist.instagram)}</a>` });
    if (artist.phone) contacts.push({ label: 'Phone', value: esc(artist.phone) });
    if (artist.gallery_rep?.length) contacts.push({ label: 'Gallery Rep', value: esc(artist.gallery_rep.join(', ')) });

    if (contacts.length) {
      $('detail-contact').innerHTML = `
        <div class="section-label">Contact</div>
        <div class="contact-grid">
          ${contacts.map(c => `
            <div class="contact-item">
              <div class="contact-label">${c.label}</div>
              <div class="contact-value">${c.value}</div>
            </div>`).join('')}
        </div>`;
    }

    const btns = [];
    if (artist.email) {
      btns.push(`<button class="btn-primary btn-sm" id="btn-compose">Email ${artist.name.split(' ')[0]}</button>`);
    }
    if (artist.research_status !== 'complete') {
      btns.push(`<button class="btn-secondary btn-sm" id="btn-research-one">Research</button>`);
    }
    $('detail-action-btns').innerHTML = btns.join('');

    if ($('btn-compose')) $('btn-compose').addEventListener('click', () => openEmailCompose(artist));
    if ($('btn-research-one')) $('btn-research-one').addEventListener('click', () => researchOne(artist.id));

    const allImages = lots.flatMap(l => l.image_urls || []);
    if (allImages.length) {
      $('detail-images').innerHTML = allImages.slice(0, 30).map(url =>
        `<img src="${esc(url)}" alt="" loading="lazy" onerror="this.style.display='none'">`
      ).join('');
      $('detail-images').querySelectorAll('img').forEach(img => {
        img.addEventListener('click', () => {
          const lb = document.createElement('div');
          lb.className = 'lightbox';
          lb.innerHTML = `<img src="${img.src}">`;
          lb.addEventListener('click', () => lb.remove());
          document.body.appendChild(lb);
        });
      });
    } else {
      $('detail-images').innerHTML = '<p style="color:var(--muted);font-size:13px">No images yet.</p>';
    }

    if (lots.length) {
      $('detail-lots').innerHTML = `
        <div class="section-label">Lot History (${lots.length})</div>
        ${lots.slice(0, 30).map(l => `
          <div class="lot-row">
            ${l.image_urls?.[0] ? `<img class="lot-thumb" src="${esc(l.image_urls[0])}" alt="" loading="lazy">` : ''}
            <div class="lot-info">
              <div class="lot-title">${esc(l.title)}</div>
              <div class="lot-meta">${esc(l.auction_house)}${l.sale_date ? ' · ' + l.sale_date : ''}</div>
            </div>
            ${l.hammer_price ? `<div class="lot-price">$${fmtPrice(l.hammer_price)}</div>` : ''}
          </div>`).join('')}`;
    }

    if (outreach?.length) {
      $('detail-outreach').innerHTML = `
        <div class="section-label">Outreach History</div>
        ${outreach.map(o => `
          <div class="outreach-row">
            <div class="outreach-header">
              <span class="outreach-type">${esc(o.type)} · ${esc(o.direction)}</span>
              <span class="outreach-status outreach-status-${o.status}">${esc(o.status)}</span>
            </div>
            ${o.subject ? `<div class="outreach-subject">${esc(o.subject)}</div>` : ''}
            <div class="outreach-date">${o.sent_at ? new Date(o.sent_at).toLocaleDateString() : ''}</div>
          </div>`).join('')}`;
    }
  }

  function closeDetail() {
    detailOverlay.classList.add('hidden');
    currentArtist = null;
  }

  // ── Email Compose ───────────────────────────────────────

  function openEmailCompose(artist) {
    $('email-to').value = artist.email || '';
    $('email-subject').value = '';
    $('email-body').value = '';
    emailModal.classList.remove('hidden');
    emailModal._artistId = artist.id;
  }

  async function generateEmail() {
    const btn = $('email-generate');
    btn.disabled = true;
    btn.textContent = 'Generating...';
    try {
      const data = await api('/email/generate', 'POST', { artist_id: emailModal._artistId });
      if (data.ok) {
        $('email-subject').value = data.subject || '';
        $('email-body').value = data.body || '';
        if (data.to && !$('email-to').value) $('email-to').value = data.to;
      }
    } catch (e) { console.error('Generate failed:', e); }
    btn.disabled = false;
    btn.textContent = 'Generate with AI';
  }

  async function sendEmailAction() {
    const to = $('email-to').value.trim();
    const subject = $('email-subject').value.trim();
    const body = $('email-body').value.trim();
    if (!to || !subject || !body) return alert('Fill in all fields');

    const btn = $('email-send');
    btn.disabled = true;
    btn.textContent = 'Sending...';

    try {
      const data = await api('/email/send', 'POST', {
        artist_id: emailModal._artistId, to, subject, body
      });
      if (data.ok) {
        emailModal.classList.add('hidden');
        if (currentArtist) openDetail(currentArtist.id);
      } else {
        alert('Send failed: ' + (data.error || 'unknown'));
      }
    } catch (e) { alert('Send failed: ' + e.message); }

    btn.disabled = false;
    btn.textContent = 'Send';
  }

  // ── Research ────────────────────────────────────────────

  async function researchOne(artistId) {
    const btn = $('btn-research-one');
    if (btn) { btn.disabled = true; btn.textContent = 'Researching...'; }
    try {
      await api('/research', 'POST', { artist_id: artistId });
      openDetail(artistId);
    } catch (e) { console.error('Research failed:', e); }
  }

  // ── Admin Panel ─────────────────────────────────────────

  async function showAdmin() {
    adminSheet.classList.remove('hidden');
    try {
      const data = await api('/pipeline/stats');
      const grid = $('stats-grid');
      const p = data.pipeline || {};
      grid.innerHTML = [
        { n: data.total_artists, l: 'Artists' },
        { n: data.total_lots, l: 'Lots' },
        { n: p.lead || 0, l: 'Leads' },
        { n: p.contacted || 0, l: 'Contacted' },
        { n: p.responded || 0, l: 'Responded' },
        { n: data.total_outreach || 0, l: 'Emails' }
      ].map(s => `
        <div class="stat-card">
          <div class="stat-num">${s.n}</div>
          <div class="stat-lbl">${s.l}</div>
        </div>`).join('');
    } catch (e) { console.error('Stats failed:', e); }
  }

  function hideAdmin() {
    adminSheet.classList.add('hidden');
    document.querySelectorAll('.tab').forEach(t => {
      t.classList.toggle('active', t.dataset.tab === 'pipeline');
    });
  }

  function adminLog(msg) {
    const log = $('import-log');
    log.textContent += msg + '\n';
    log.scrollTop = log.scrollHeight;
  }

  async function scrapeAll() {
    $('import-log').textContent = '';
    adminLog('Scraping all houses...');
    $('btn-scrape-all').disabled = true;
    try {
      const data = await api('/scrape/all', 'POST');
      if (data.results) {
        Object.entries(data.results).forEach(([house, count]) => {
          adminLog(`  ${house}: ${count} lots`);
        });
      }
      adminLog('Done!');
    } catch (e) { adminLog('Error: ' + e.message); }
    $('btn-scrape-all').disabled = false;
  }

  async function extractArtists() {
    adminLog('Extracting artists...');
    $('btn-extract').disabled = true;
    try {
      const data = await api('/extract-artists', 'POST');
      adminLog(`Extracted: ${data.extracted} lots, ${data.new_artists} new artists`);
      await loadArtists();
    } catch (e) { adminLog('Error: ' + e.message); }
    $('btn-extract').disabled = false;
  }

  async function researchBatch() {
    adminLog('Researching batch...');
    $('btn-research').disabled = true;
    try {
      const data = await api('/research/batch', 'POST', { limit: 5 });
      if (data.results) {
        data.results.forEach(r => adminLog(`  ${r.name}: ${r.status}`));
      }
      adminLog('Done!');
      await loadArtists();
    } catch (e) { adminLog('Error: ' + e.message); }
    $('btn-research').disabled = false;
  }

  // ── Utility ─────────────────────────────────────────────

  function esc(s) {
    if (!s) return '';
    const d = document.createElement('div');
    d.textContent = String(s);
    return d.innerHTML;
  }

  function fmtPrice(n) {
    const num = parseFloat(n);
    if (isNaN(num)) return '0';
    if (num >= 1000) return Math.round(num).toLocaleString();
    return num.toFixed(0);
  }

  // ── Start ───────────────────────────────────────────────
  init();
})();
