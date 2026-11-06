/* ============================================================
   URLKING Cloud Console — app.js
   (API contracts unchanged; UI layer modernized)
   ============================================================ */
const API_BASE = window.location.origin;
let currentPage = 1;
let totalPages = 1;
let secretKey = localStorage.getItem('telestore_v2_key') || '';
let clickCount = 0;
let clickTimer;
let scanInterval;

// ---------- helpers ----------
function esc(s) {
  return String(s == null ? '' : s)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function toast(msg, type = 'ok', ms = 3500) {
  const box = document.getElementById('toastBox');
  if (!box) return;
  const el = document.createElement('div');
  el.className = `toast ${type}`;
  el.innerHTML = `<span>${type === 'err' ? '⚠️' : '✅'}</span><span>${esc(msg)}</span>`;
  box.appendChild(el);
  setTimeout(() => {
    el.classList.add('out');
    setTimeout(() => el.remove(), 260);
  }, ms);
}

function getIcon(name) {
  const ext = String(name || '').split('.').pop().toLowerCase();
  const map = { mp4:'🎬', mkv:'🎬', avi:'🎬', zip:'📦', rar:'📦', '7z':'📦', pdf:'📕', jpg:'🖼️', jpeg:'🖼️', png:'🖼️', gif:'🖼️', mp3:'🎵', flac:'🎵', apk:'📱', apks:'📱', exe:'⚙️', iso:'💿' };
  return map[ext] || '📄';
}

// ---------- auth (3× logo click) ----------
document.getElementById('secretTrigger').addEventListener('click', () => {
  clickCount++;
  clearTimeout(clickTimer);
  if (clickCount >= 3) {
    document.getElementById('authModal').classList.add('open');
    document.getElementById('accessKey').focus();
    clickCount = 0;
  } else {
    clickTimer = setTimeout(() => { clickCount = 0; }, 1500);
  }
});

function closeModal() { document.getElementById('authModal').classList.remove('open'); }
function handleEnter(e) { if (e.key === 'Enter') verifyAccess(); }

async function verifyAccess() {
  const key = document.getElementById('accessKey').value;
  if (!key) return;

  const btn = document.getElementById('loginBtn');
  btn.textContent = 'Verifying...'; btn.disabled = true;

  const ok = await loadFiles(1, key);

  btn.textContent = 'Access Files'; btn.disabled = false;

  if (ok) {
    secretKey = key;
    localStorage.setItem('telestore_v2_key', key);
    closeModal();
    document.getElementById('authError').style.display = 'none';
    loadGlobalSettings();
  } else {
    document.getElementById('authError').style.display = 'block';
  }
}

function logout() { localStorage.removeItem('telestore_v2_key'); location.reload(); }

// ---------- files ----------
function showSkeletons(n = 5) {
  document.getElementById('filesList').innerHTML =
    Array.from({ length: n }, () => '<div class="skeleton"></div>').join('');
}

async function loadFiles(page, keyToTry = secretKey) {
  if (!keyToTry) return false;

  try {
    const mode = document.getElementById('downloadMode')?.value || 'random';
    const url = `${API_BASE}/files?page=${page}&limit=10&key=${encodeURIComponent(keyToTry)}&mode=${mode}`;
    const res = await fetch(url);
    if (!res.ok) return false;

    const data = await res.json();

    document.getElementById('promoSection').style.display = 'none';
    document.getElementById('filesSection').style.display = 'block';
    document.getElementById('logoutBtn').style.display = 'block';

    currentPage = data.page;
    totalPages = data.total_pages;

    document.getElementById('fileCount').textContent = `${data.total} files managed`;
    document.getElementById('pageInfo').textContent = `Page ${currentPage} / ${totalPages}`;
    document.getElementById('btnPrev').disabled = currentPage <= 1;
    document.getElementById('btnNext').disabled = currentPage >= totalPages;
    document.getElementById('paginationControls').style.display = 'flex';

    const list = document.getElementById('filesList');
    if (!data.files.length) {
      list.innerHTML = `
        <div class="empty-state">
          <div class="big">🗂️</div>
          <p>No files yet. Upload via the API to see them here.</p>
        </div>`;
      return true;
    }

    list.innerHTML = data.files.map(f => `
      <div class="list-row">
        <div class="row-icon">${getIcon(f.filename)}</div>
        <div class="row-body">
          <div class="row-title" title="${esc(f.filename)}">${esc(f.filename)}</div>
          <div class="row-meta">${esc(f.size)} • ID: ${esc(f.short_id)}</div>
        </div>
        <div class="row-actions">
          <a href="${esc(f.download_link)}" class="btn-cta" style="padding: 9px 18px; font-size: 13px;" target="_blank" rel="noopener">Download</a>
        </div>
      </div>
    `).join('');

    return true;
  } catch (e) {
    console.error('loadFiles error:', e);
    return false;
  }
}

function changePage(dir) {
  showSkeletons(3);
  loadFiles(currentPage + dir);
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

// ---------- virus scanner ----------
function toggleScanner() {
  const el = document.getElementById('scannerSection');
  el.style.display = el.style.display === 'block' ? 'none' : 'block';
  if (el.style.display === 'block') {
    checkScanProgress();
  }
}

async function startScan() {
  if (!secretKey) return;
  try {
    const btn = document.getElementById('btnStartScan');
    btn.disabled = true;
    btn.textContent = 'Starting...';

    await fetch(`${API_BASE}/api/scan/start?key=${encodeURIComponent(secretKey)}`);

    checkScanProgress();
    if (!scanInterval) {
      scanInterval = setInterval(checkScanProgress, 5000);
    }
  } catch (e) {
    console.error('Scan Start Error:', e);
    toast('Scan could not be started.', 'err');
  }
}

async function checkScanProgress() {
  if (!secretKey) return;
  try {
    const res = await fetch(`${API_BASE}/api/scan/progress?key=${encodeURIComponent(secretKey)}`);
    const data = await res.json();

    renderScannerUI(data);

    if (data.status !== 'running' && data.status !== 'initializing') {
      clearInterval(scanInterval);
      scanInterval = null;
    } else if (!scanInterval) {
      scanInterval = setInterval(checkScanProgress, 5000);
    }
  } catch (e) {
    console.error('Scan Progress Error:', e);
  }
}

function renderScannerUI(data) {
  const statusEl = document.getElementById('scanStatus');
  const barEl = document.getElementById('scanBar');
  const infectedListEl = document.getElementById('infectedList');
  const btnStart = document.getElementById('btnStartScan');

  if (!data || data.status === 'idle') {
    statusEl.textContent = 'System ready. Click start to scan all files.';
    barEl.style.width = '0%';
    btnStart.disabled = false;
    btnStart.textContent = 'Start Full Scan';
    infectedListEl.innerHTML = '';
    return;
  }

  if (data.status === 'running' || data.status === 'initializing') {
    btnStart.disabled = true;
    btnStart.textContent = 'Scanning...';
    const pct = data.total > 0 ? Math.round((data.scanned / data.total) * 100) : 0;
    statusEl.textContent = `Scanning database... ${data.scanned} / ${data.total} (${pct}%)`;
    barEl.style.width = `${pct}%`;
  } else if (data.status === 'completed') {
    btnStart.disabled = false;
    btnStart.textContent = 'Scan Completed — Run Again';
    statusEl.textContent = `Scan finished. Safe files: ${data.safe || 0} | Infected: ${data.infected ? data.infected.length : 0}`;
    barEl.style.width = '100%';
  } else {
    statusEl.textContent = `Status: ${data.status}`;
    btnStart.disabled = false;
  }

  if (data.infected && data.infected.length > 0) {
    infectedListEl.innerHTML = data.infected.map(f => `
      <div class="list-row danger" id="inf-${esc(f.file_code)}">
        <div class="row-icon">🚨</div>
        <div class="row-body">
          <div class="row-title">${esc(f.filename)}</div>
          <div class="row-meta">ID: ${esc(f.file_code)} | Flagged by ${esc(f.engines)} engines</div>
        </div>
        <div class="row-actions">
          <button class="btn-danger" onclick="deleteInfectedFile('${esc(f.file_code)}')">Delete File</button>
        </div>
      </div>
    `).join('');
  } else if (data.status === 'completed') {
    infectedListEl.innerHTML = `<div class="safe-banner">✅ All files are safe — no virus detected.</div>`;
  } else {
    infectedListEl.innerHTML = '';
  }
}

async function deleteInfectedFile(fileCode) {
  if (!confirm('Permanently delete this infected file?')) return;

  try {
    const btn = document.querySelector(`#inf-${CSS.escape(fileCode)} button`);
    if (btn) { btn.textContent = 'Deleting...'; btn.disabled = true; }

    await fetch(`${API_BASE}/api/scan/delete?key=${encodeURIComponent(secretKey)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ file_code: fileCode })
    });

    const row = document.getElementById(`inf-${fileCode}`);
    if (row) row.style.display = 'none';

    toast('Infected file deleted.');
    loadFiles(currentPage);
  } catch (e) {
    toast('Failed to delete file.', 'err');
    console.error(e);
  }
}

// ---------- workers manager ----------
function toggleWorkersManager() {
  const el = document.getElementById('workersSection');
  el.style.display = el.style.display === 'block' ? 'none' : 'block';
  if (el.style.display === 'block') {
    loadWorkers();
  }
}

async function loadWorkers() {
  if (!secretKey) return;
  try {
    const res = await fetch(`${API_BASE}/api/workers?key=${encodeURIComponent(secretKey)}`);
    const workers = await res.json();

    const listEl = document.getElementById('workersList');
    if (!Array.isArray(workers) || workers.length === 0) {
      listEl.innerHTML = `<div class="muted-banner">No workers added yet.</div>`;
      return;
    }

    listEl.innerHTML = workers.map(w => {
      const isHealthy = w.status === 'healthy';
      const chip = isHealthy
        ? '<span class="chip ok">● HEALTHY</span>'
        : '<span class="chip bad">● FLAGGED</span>';

      const actionButton = isHealthy
        ? `<button class="btn-ghost" onclick="deleteWorker('${esc(w.url)}')">Remove</button>`
        : `<button class="btn-cta" style="padding: 8px 14px; font-size: 13px;" onclick="replaceWorker('${esc(w.url)}', this)">Auto-Replace</button>
           <button class="btn-danger" onclick="deleteWorker('${esc(w.url)}')">Remove</button>`;

      return `
        <div class="list-row">
          <div class="row-icon">${isHealthy ? '🟢' : '🔴'}</div>
          <div class="row-body">
            <div class="row-title" title="${esc(w.url)}">${esc(w.url)}</div>
            <div class="row-meta" style="margin-top:6px;">${chip}</div>
          </div>
          <div class="row-actions">${actionButton}</div>
        </div>
      `;
    }).join('');
  } catch (e) {
    console.error('Load Workers Error:', e);
    toast('Workers list load failed.', 'err');
  }
}

async function replaceWorker(url, btnElement) {
  if (!confirm(`Replace this flagged worker with a fresh one?\n${url}`)) return;
  if (!secretKey) return;

  try {
    btnElement.textContent = 'Replacing...';
    btnElement.disabled = true;

    const res = await fetch(`${API_BASE}/api/workers/replace?key=${encodeURIComponent(secretKey)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: url })
    });

    const data = await res.json();
    if (res.ok) {
      toast(`Worker replaced. New URL: ${data.new_url}`, 'ok', 6000);
    } else {
      toast(`Replacement failed: ${data.detail || 'Error'}`, 'err');
    }
    loadWorkers();
  } catch (e) {
    toast('Replacement failed.', 'err');
    console.error(e);
  }
}

async function addWorker() {
  const urlInput = document.getElementById('newWorkerUrl');
  const url = urlInput.value.trim();
  if (!url || !secretKey) return;

  try {
    const res = await fetch(`${API_BASE}/api/workers/add?key=${encodeURIComponent(secretKey)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: url })
    });
    if (!res.ok) throw new Error();
    urlInput.value = '';
    toast('Worker added.');
    loadWorkers();
  } catch (e) {
    toast('Add worker failed.', 'err');
    console.error('Add Worker Error:', e);
  }
}

async function deleteWorker(url) {
  if (!confirm(`Remove this worker?\n${url}`)) return;
  if (!secretKey) return;

  try {
    const res = await fetch(`${API_BASE}/api/workers/delete?key=${encodeURIComponent(secretKey)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ url: url })
    });
    if (!res.ok) throw new Error();
    toast('Worker removed.');
    loadWorkers();
  } catch (e) {
    toast('Delete worker failed.', 'err');
    console.error('Delete Worker Error:', e);
  }
}

// ---------- settings ----------
function openSettingsModal() {
  document.getElementById('settingsModal').classList.add('open');
  loadGlobalSettings();
}

function closeSettingsModal() {
  document.getElementById('settingsModal').classList.remove('open');
}

async function loadGlobalSettings() {
  if (!secretKey) return;
  try {
    const res = await fetch(`${API_BASE}/api/settings/worker-mode?key=${encodeURIComponent(secretKey)}`);
    if (res.ok) {
      const data = await res.json();
      const select = document.getElementById('globalWorkerMode');
      if (select) select.value = data.mode;
    }
  } catch (e) {
    console.error('Failed to load settings:', e);
  }
}

async function saveGlobalSettings() {
  const select = document.getElementById('globalWorkerMode');
  const mode = select?.value || 'random';
  if (!secretKey) return;
  try {
    const res = await fetch(`${API_BASE}/api/settings/worker-mode?key=${encodeURIComponent(secretKey)}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mode: mode })
    });
    if (res.ok) {
      toast('Global routing mode updated for all users.');
      closeSettingsModal();
    } else {
      toast('Failed to update routing mode.', 'err');
    }
  } catch (e) {
    toast('Failed to update routing mode.', 'err');
    console.error('Failed to update global routing mode:', e);
  }
}

// Modal backdrop click → close
['authModal', 'settingsModal'].forEach(id => {
  const el = document.getElementById(id);
  if (el) el.addEventListener('click', e => {
    if (e.target === el) {
      if (id === 'authModal') closeModal(); else closeSettingsModal();
    }
  });
});

// init
if (secretKey) {
  showSkeletons(5);
  loadFiles(1);
}
