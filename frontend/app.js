const API_BASE = window.location.origin; 
let currentPage = 1;
let totalPages = 1;
let secretKey = localStorage.getItem('telestore_v2_key') || '';
let clickCount = 0;
let clickTimer;
let scanInterval; // For scanning progress

// Auth Logic
document.getElementById('secretTrigger').addEventListener('click', () => {
  clickCount++;
  clearTimeout(clickTimer);

  if (clickCount >= 3) {
    document.getElementById('authModal').style.display = 'flex';
    document.getElementById('accessKey').focus();
    clickCount = 0;
  } else {
    clickTimer = setTimeout(() => { clickCount = 0; }, 1500); 
  }
});

function closeModal() { document.getElementById('authModal').style.display = 'none'; }
function handleEnter(e) { if (e.key === 'Enter') verifyAccess(); }

async function verifyAccess() {
  const key = document.getElementById('accessKey').value;
  if (!key) return;

  const btn = document.getElementById('loginBtn');
  btn.textContent = "Verifying..."; btn.disabled = true;

  const ok = await loadFiles(1, key);

  btn.textContent = "Access Files"; btn.disabled = false;

  if (ok) {
    secretKey = key;
    localStorage.setItem('telestore_v2_key', key);
    closeModal();
    document.getElementById('authError').style.display = 'none';
  } else {
    document.getElementById('authError').style.display = 'block';
  }
}

function logout() { localStorage.removeItem('telestore_v2_key'); location.reload(); }

function getIcon(name) {
  const ext = name.split('.').pop().toLowerCase();
  const map = { mp4:'🎬', mkv:'🎬', zip:'📦', rar:'📦', pdf:'📕', jpg:'🖼️', png:'🖼️', mp3:'🎵', apk:'📱', apks:'📱', exe:'⚙️' };
  return map[ext] || '📄';
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

    const list = document.getElementById('filesList');
    list.innerHTML = data.files.map(f => `
      <div class="file-card">
        <div class="file-icon">${getIcon(f.filename)}</div>
        <div class="file-info">
          <div class="file-name" title="${f.filename}">${f.filename}</div>
          <div class="file-meta">${f.size} • ID: ${f.short_id}</div>
        </div>
        <a href="${f.download_link}" class="btn-download" target="_blank">Download</a>
      </div>
    `).join('');

    return true;
  } catch (e) {
    return false;
  }
}

function changePage(dir) {
  loadFiles(currentPage + dir);
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

// =========================================================
// 🔥 VIRUS SCANNER JS LOGIC
// =========================================================

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
    document.getElementById('btnStartScan').disabled = true;
    document.getElementById('btnStartScan').textContent = "Starting...";
    
    await fetch(`${API_BASE}/api/scan/start?key=${encodeURIComponent(secretKey)}`);
    
    checkScanProgress(); 
    if (!scanInterval) {
      scanInterval = setInterval(checkScanProgress, 5000); 
    }
  } catch(e) { console.error("Scan Start Error:", e); }
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
  } catch(e) { console.error("Scan Progress Error:", e); }
}

function renderScannerUI(data) {
  const statusEl = document.getElementById('scanStatus');
  const barEl = document.getElementById('scanBar');
  const infectedListEl = document.getElementById('infectedList');
  const btnStart = document.getElementById('btnStartScan');

  if (!data || data.status === 'idle') {
    statusEl.textContent = 'System Ready. Click Start to scan files.';
    barEl.style.width = '0%';
    btnStart.disabled = false;
    btnStart.textContent = "Start Full Scan";
    return;
  }

  if (data.status === 'running' || data.status === 'initializing') {
    btnStart.disabled = true;
    btnStart.textContent = "Scanning...";
    const pct = data.total > 0 ? Math.round((data.scanned / data.total) * 100) : 0;
    statusEl.textContent = `Scanning Database... ${data.scanned} / ${data.total} (${pct}%)`;
    barEl.style.width = `${pct}%`;
  } else if (data.status === 'completed') {
    btnStart.disabled = false;
    btnStart.textContent = "Scan Completed - Run Again";
    statusEl.textContent = `Scan Finished. Safe Files: ${data.safe || 0} | Infected: ${data.infected ? data.infected.length : 0}`;
    barEl.style.width = `100%`;
  } else {
    statusEl.textContent = `Status: ${data.status}`;
    btnStart.disabled = false;
  }

  // Render Infected Files List
  if (data.infected && data.infected.length > 0) {
    infectedListEl.innerHTML = data.infected.map(f => `
      <div class="infected-item" id="inf-${f.file_code}">
        <div>
          <strong style="color: #991b1b; font-size: 14px;">🚨 ${f.filename}</strong><br>
          <span style="font-size: 12px; color: #b91c1c; font-family: monospace;">ID: ${f.file_code} | Flagged by ${f.engines} engines</span>
        </div>
        <button class="btn-danger" onclick="deleteInfectedFile('${f.file_code}')">Delete File</button>
      </div>
    `).join('');
  } else if (data.status === 'completed') {
    infectedListEl.innerHTML = `<div style="padding:14px; background:#ecfdf5; color:#065f46; font-weight:700; border-radius:10px; border:1px solid #a7f3d0; text-align:center;">✅ All files are safe! No virus detected.</div>`;
  } else {
    infectedListEl.innerHTML = '';
  }
}

async function deleteInfectedFile(fileCode) {
  if(!confirm('Are you sure you want to permanently delete this infected file?')) return;
  
  try {
    const btn = document.querySelector(`#inf-${fileCode} button`);
    if(btn) { btn.textContent = "Deleting..."; btn.disabled = true; }

    await fetch(`${API_BASE}/api/scan/delete?key=${encodeURIComponent(secretKey)}`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({file_code: fileCode})
    });
    
    const row = document.getElementById(`inf-${fileCode}`);
    if(row) row.style.display = 'none';

    loadFiles(currentPage); 
  } catch(e) { 
    alert("Failed to delete file."); 
    console.error(e); 
  }
}

// =========================================================
// ☁️ WORKERS MANAGER JS LOGIC
// =========================================================

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
    if (workers.length === 0) {
      listEl.innerHTML = '<div style="padding:14px; background:#f1f5f9; color:var(--text-muted); border-radius:10px; text-align:center; width: 100%;">No workers added yet.</div>';
      return;
    }
    
    listEl.innerHTML = workers.map(w => {
      const isHealthy = w.status === 'healthy';
      const statusBg = isHealthy ? '#ecfdf5' : '#fef2f2';
      const statusBorder = isHealthy ? '#a7f3d0' : '#fecaca';
      const statusColor = isHealthy ? '#065f46' : '#991b1b';
      
      const actionButton = isHealthy 
        ? `<button class="btn-danger" style="background:#f1f5f9; color:var(--text); border:1px solid var(--border);" onclick="deleteWorker('${w.url}')">Remove</button>`
        : `<div style="display: flex; gap: 8px;">
             <button class="btn-cta" style="padding: 8px 14px; font-size: 13px; background:#4f46e5; border-radius: 8px;" onclick="replaceWorker('${w.url}', this)">Auto-Replace</button>
             <button class="btn-danger" onclick="deleteWorker('${w.url}')">Remove</button>
           </div>`;
      
      return `
        <div class="infected-item" style="background: ${statusBg}; border-color: ${statusBorder};">
          <div>
            <strong style="color: var(--text); font-size: 14px;">${w.url}</strong><br>
            <span style="font-size: 12px; color: ${statusColor}; font-weight: bold;">Status: ${w.status.toUpperCase()}</span>
          </div>
          ${actionButton}
        </div>
      `;
    }).join('');
  } catch(e) { console.error("Load Workers Error:", e); }
}

async function replaceWorker(url, btnElement) {
  if (!confirm(`Are you sure you want to replace this flagged worker with a fresh one?\n${url}`)) return;
  if (!secretKey) return;
  
  try {
    btnElement.textContent = "Replacing...";
    btnElement.disabled = true;
    
    const res = await fetch(`${API_BASE}/api/workers/replace?key=${encodeURIComponent(secretKey)}`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({url: url})
    });
    
    const data = await res.json();
    if (res.ok) {
      alert(`Worker replaced successfully!\nNew URL: ${data.new_url}`);
    } else {
      alert(`Replacement failed: ${data.detail || 'Error'}`);
    }
    loadWorkers();
  } catch(e) { 
    alert("Replacement failed."); 
    console.error(e); 
  }
}

async function addWorker() {
  const urlInput = document.getElementById('newWorkerUrl');
  const url = urlInput.value.trim();
  if (!url || !secretKey) return;
  
  try {
    await fetch(`${API_BASE}/api/workers/add?key=${encodeURIComponent(secretKey)}`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({url: url})
    });
    urlInput.value = '';
    loadWorkers();
  } catch(e) { console.error("Add Worker Error:", e); }
}

async function deleteWorker(url) {
  if (!confirm(`Are you sure you want to remove this worker?\n${url}`)) return;
  if (!secretKey) return;
  
  try {
    await fetch(`${API_BASE}/api/workers/delete?key=${encodeURIComponent(secretKey)}`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({url: url})
    });
    loadWorkers();
  } catch(e) { console.error("Delete Worker Error:", e); }
}

if (secretKey) {
  loadFiles(1);
}
