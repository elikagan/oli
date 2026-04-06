(function () {
  'use strict';

  const API = 'https://oli-api.objectlesson.workers.dev';
  let artists = [];
  let currentFilter = 'all';
  let currentArtist = null;
  let searchTimeout = null;

  // ── DOM refs ────────────────────────────────────────────
  const $ = id => document.getElementById(id);
  const artistList = $('artist-list');
  const emptyState = $('empty-state');
  const loadingEl = $('loading');
  const detailOverlay = $('artist-detail');
  const emailModal = $('email-modal');
  const adminSheet = $('admin-sheet');
  const searchInput = $('search-input');

  // ── Init ────────────────────────────────────────────────
  async function init() {
    bindEvents();
    await loadArtists();
    if ('serviceWorker' in navigator) {
      navigator.serviceWorker.register('./sw.js');
    }
  }

  function bindEvents() {
    // Pipeline tabs
    document.querySelectorAll('.ptab').forEach(tab => {
      tab.addEventListener('click', () => {
        document.querySelectorAll('.ptab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        currentFilter = tab.dataset.status;
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

    // Detail back
    $('detail-back').addEventListener('click', closeDetail);

    // Detail status change
    $('detail-status').addEventListener('change', async (e) => {
      if (!currentArtist) return;
      await api('/artists', 'PATCH', { id: currentArtist.id, status: e.target.value });
      currentArtist.status = e.target.value;
      // Update in list too
      const idx = artists.findIndex(a => a.id === currentArtist.id);
      if (idx >= 0) artists[idx].status = e.target.value;
      renderArtists();
    });

    // Email modal
    $('email-close').addEventListener('click', () => emailModal.classList.add('hidden'));
    $('email-generate').addEventListener('click', generateEmail);
    $('email-send').addEventListener('click', sendEmailAction);

    // Admin actions
    $('btn-scrape-all').addEventListener('click', scrapeAll);
    $('btn-extract').addEventListener('click', extractArtists);
    $('btn-research').addEventListener('click', researchBatch);

    // Close admin sheet on background click
    adminSheet.addEventListener('click', (e) => {
      if (e.target === adminSheet) hideAdmin();
    });
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

    let url = '/artists?limit=200';
    if (search) url += `&q=${encodeURIComponent(search)}`;

    try {
      const data = await api(url);
      artists = data.artists || [];
    } catch (e) {
      console.error('Load failed:', e);
      artists = [];
    }

    loadingEl.classList.add('hidden');
    renderArtists();
    updatePipelineCounts();
  }

  // ── Render Artist List ──────────────────────────────────
  function renderArtists() {
    const filtered = currentFilter === 'all'
      ? artists
      : artists.filter(a => a.status === currentFilter);

    if (filtered.length === 0) {
      artistList.innerHTML = '';
      emptyState.classList.remove('hidden');
      return;
    }

    emptyState.classList.add('hidden');

    artistList.innerHTML = filtered.map(a => {
      const thumb = a.image_urls?.[0] || '';
      const thumbHtml = thumb
        ? `<img class="artist-thumb" src="${esc(thumb)}" alt="" loading="lazy" onerror="this.style.display='none'">`
        : `<div class="artist-thumb" style="display:flex;align-items:center;justify-content:center;font-size:20px;font-weight:700;color:#bbb">${esc(a.name?.[0] || '?')}</div>`;

      const houses = (a.auction_houses || []).slice(0, 2).map(h =>
        `<span class="tag tag-house">${esc(h)}</span>`
      ).join('');

      let ageBadge = '';
      if (a.alive === false) {
        ageBadge = '<span class="tag tag-dead">Deceased</span>';
      } else if (a.estimated_age) {
        ageBadge = `<span class="tag tag-age">${a.estimated_age}y</span>`;
      }

      let statusBadge = '';
      if (a.status !== 'lead') {
        statusBadge = `<span class="tag tag-status">${esc(a.status)}</span>`;
      }

      let establishedBadge = '';
      if (a.is_too_established) {
        establishedBadge = '<span class="tag tag-established">Established</span>';
      }

      const medianStr = a.median_sale ? `~$${fmtPrice(a.median_sale)}` : '';

      return `
        <div class="artist-card" data-id="${a.id}">
          ${thumbHtml}
          <div class="artist-info">
            <div class="artist-name">${esc(a.name)}</div>
            <div class="artist-sub">${esc(a.location || '')}${a.location && a.categories?.length ? ' · ' : ''}${esc((a.categories || []).slice(0, 2).join(', '))}</div>
            <div class="artist-tags">
              ${statusBadge}${ageBadge}${establishedBadge}${houses}
            </div>
          </div>
          <div class="artist-stats">
            <div class="stat-lot-count">${a.lot_count || 0}</div>
            <div class="stat-label">lots</div>
            ${medianStr ? `<div class="stat-price">${medianStr}</div>` : ''}
          </div>
        </div>`;
    }).join('');

    // Bind click
    artistList.querySelectorAll('.artist-card').forEach(card => {
      card.addEventListener('click', () => openDetail(parseInt(card.dataset.id)));
    });
  }

  // ── Pipeline counts in tabs ─────────────────────────────
  function updatePipelineCounts() {
    const counts = {};
    artists.forEach(a => { counts[a.status] = (counts[a.status] || 0) + 1; });

    document.querySelectorAll('.ptab').forEach(tab => {
      const s = tab.dataset.status;
      const existing = tab.querySelector('.ptab-count');
      if (existing) existing.remove();

      const n = s === 'all' ? artists.length : (counts[s] || 0);
      if (n > 0) {
        const span = document.createElement('span');
        span.className = 'ptab-count';
        span.textContent = n;
        tab.appendChild(span);
      }
    });
  }

  // ── Artist Detail ───────────────────────────────────────
  async function openDetail(id) {
    detailOverlay.classList.remove('hidden');

    // Show loading
    $('detail-name').textContent = 'Loading…';
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
    // Header
    $('detail-name').textContent = artist.name;
    const metaParts = [];
    if (artist.estimated_age && artist.alive !== false) metaParts.push(`Age ~${artist.estimated_age}`);
    if (artist.alive === false) metaParts.push('Deceased' + (artist.death_year ? ` (${artist.death_year})` : ''));
    if (artist.location) metaParts.push(artist.location);
    $('detail-meta').textContent = metaParts.join(' · ');

    // Status select
    const statusSel = $('detail-status');
    statusSel.innerHTML = ['lead','contacted','responded','deal','consigning','passed','disqualified']
      .map(s => `<option value="${s}" ${s === artist.status ? 'selected' : ''}>${s}</option>`)
      .join('');

    // Summary
    if (artist.ai_summary) {
      $('detail-summary').innerHTML = `
        <div class="section-label">About</div>
        <div class="detail-summary-text">${esc(artist.ai_summary)}</div>`;
    }

    // Contact info
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

    // Action buttons
    const btns = [];
    if (artist.email) {
      btns.push(`<button class="btn-primary btn-sm" id="btn-compose">Email ${artist.name.split(' ')[0]}</button>`);
    }
    if (artist.research_status !== 'complete') {
      btns.push(`<button class="btn-secondary btn-sm" id="btn-research-one">Research</button>`);
    }
    $('detail-action-btns').innerHTML = btns.join('');

    if ($('btn-compose')) {
      $('btn-compose').addEventListener('click', () => openEmailCompose(artist));
    }
    if ($('btn-research-one')) {
      $('btn-research-one').addEventListener('click', () => researchOne(artist.id));
    }

    // Image grid — collect all lot images
    const allImages = lots.flatMap(l => l.image_urls || []);
    if (allImages.length) {
      $('detail-images').innerHTML = allImages.slice(0, 30).map(url =>
        `<img src="${esc(url)}" alt="" loading="lazy" onerror="this.style.display='none'">`
      ).join('');

      // Lightbox
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
      $('detail-images').innerHTML = '<p style="color:var(--muted);font-size:13px">No images yet. Run scraping to import auction lots.</p>';
    }

    // Lot list
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

    // Outreach history
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
    btn.textContent = 'Generating…';

    try {
      const data = await api('/email/generate', 'POST', { artist_id: emailModal._artistId });
      if (data.ok) {
        $('email-subject').value = data.subject || '';
        $('email-body').value = data.body || '';
        if (data.to && !$('email-to').value) $('email-to').value = data.to;
      }
    } catch (e) {
      console.error('Generate failed:', e);
    }

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
    btn.textContent = 'Sending…';

    try {
      const data = await api('/email/send', 'POST', {
        artist_id: emailModal._artistId,
        to, subject, body
      });

      if (data.ok) {
        emailModal.classList.add('hidden');
        // Refresh detail to show outreach
        if (currentArtist) openDetail(currentArtist.id);
      } else {
        alert('Send failed: ' + (data.error || 'unknown'));
      }
    } catch (e) {
      alert('Send failed: ' + e.message);
    }

    btn.disabled = false;
    btn.textContent = 'Send';
  }

  // ── Research ────────────────────────────────────────────
  async function researchOne(artistId) {
    const btn = $('btn-research-one');
    if (btn) { btn.disabled = true; btn.textContent = 'Researching…'; }

    try {
      await api('/research', 'POST', { artist_id: artistId });
      // Reload detail
      openDetail(artistId);
    } catch (e) {
      console.error('Research failed:', e);
    }
  }

  // ── Admin Panel ─────────────────────────────────────────
  async function showAdmin() {
    adminSheet.classList.remove('hidden');
    // Load stats
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
    } catch (e) {
      console.error('Stats failed:', e);
    }
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
    adminLog('Scraping all houses…');
    $('btn-scrape-all').disabled = true;

    try {
      const data = await api('/scrape/all', 'POST');
      if (data.results) {
        Object.entries(data.results).forEach(([house, count]) => {
          adminLog(`  ${house}: ${count} lots`);
        });
      }
      adminLog('Done!');
    } catch (e) {
      adminLog('Error: ' + e.message);
    }

    $('btn-scrape-all').disabled = false;
  }

  async function extractArtists() {
    adminLog('Extracting artists from lot titles…');
    $('btn-extract').disabled = true;

    try {
      const data = await api('/extract-artists', 'POST');
      adminLog(`Extracted: ${data.extracted} lots, ${data.new_artists} new artists`);
      await loadArtists();
    } catch (e) {
      adminLog('Error: ' + e.message);
    }

    $('btn-extract').disabled = false;
  }

  async function researchBatch() {
    adminLog('Researching batch of artists…');
    $('btn-research').disabled = true;

    try {
      const data = await api('/research/batch', 'POST', { limit: 5 });
      if (data.results) {
        data.results.forEach(r => adminLog(`  ${r.name}: ${r.status}`));
      }
      adminLog('Done!');
      await loadArtists();
    } catch (e) {
      adminLog('Error: ' + e.message);
    }

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
