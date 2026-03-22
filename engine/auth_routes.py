"""Username/password authentication routes with onboarding flow.

Routes:
1. GET  /login                  → login/signup page HTML
2. POST /auth/login             → authenticate, return session cookie
3. POST /auth/signup            → create new account
4. GET  /onboarding             → portfolio onboarding page HTML
5. POST /auth/complete-onboarding → save portfolio, mark onboarded
6. POST /auth/logout            → destroy session
7. GET  /auth/me                → current user info
"""

import logging
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse

from .user_auth import (
    authenticate, create_user, create_session, destroy_session,
    get_session_user, get_session_data, set_user_portfolio, set_user_onboarded,
    update_session, SESSION_COOKIE_NAME, SESSION_MAX_AGE,
)
from .rate_limit import limiter

logger = logging.getLogger(__name__)

router = APIRouter()

AUTH_LIMIT = "5/minute"
SIGNUP_LIMIT = "3/minute"


# ---------------------------------------------------------------------------
# Shared CSS — extracted so all three pages stay consistent
# ---------------------------------------------------------------------------
_SHARED_CSS = """
*{margin:0;padding:0;box-sizing:border-box}

/* ── Animated gradient background ── */
body{
  font-family:-apple-system,BlinkMacSystemFont,'SF Pro Display','Segoe UI',sans-serif;
  color:#f0f0f5;
  min-height:100vh;
  display:flex;
  align-items:center;
  justify-content:center;
  overflow-x:hidden;
  background:#080b18;
}
.bg{
  position:fixed;inset:0;z-index:0;
  background:linear-gradient(135deg,#080b18 0%,#0a1628 25%,#0d1f3c 50%,#0a1a35 75%,#080b18 100%);
  background-size:400% 400%;
  animation:gradientShift 20s ease infinite;
}
@keyframes gradientShift{
  0%{background-position:0% 50%}
  25%{background-position:100% 0%}
  50%{background-position:100% 100%}
  75%{background-position:0% 100%}
  100%{background-position:0% 50%}
}

/* ── Floating orbs ── */
.orb{
  position:fixed;border-radius:50%;filter:blur(80px);opacity:0.35;z-index:0;
  pointer-events:none;
}
.orb-1{
  width:500px;height:500px;
  background:radial-gradient(circle,#1a3a8f 0%,transparent 70%);
  top:-10%;left:-5%;
  animation:float1 25s ease-in-out infinite;
}
.orb-2{
  width:400px;height:400px;
  background:radial-gradient(circle,#1e4fd8 0%,transparent 70%);
  bottom:-15%;right:-10%;
  animation:float2 30s ease-in-out infinite;
}
.orb-3{
  width:300px;height:300px;
  background:radial-gradient(circle,#2563eb 0%,transparent 70%);
  top:40%;left:60%;
  animation:float3 22s ease-in-out infinite;
}
@keyframes float1{0%,100%{transform:translate(0,0)}50%{transform:translate(60px,80px)}}
@keyframes float2{0%,100%{transform:translate(0,0)}50%{transform:translate(-50px,-60px)}}
@keyframes float3{0%,100%{transform:translate(0,0)}33%{transform:translate(-40px,50px)}66%{transform:translate(30px,-40px)}}

/* ── Glassmorphism card ── */
.card{
  position:relative;z-index:1;
  width:100%;max-width:420px;
  padding:44px 40px;
  background:rgba(255,255,255,0.04);
  border:1px solid rgba(255,255,255,0.08);
  border-radius:24px;
  backdrop-filter:blur(40px) saturate(1.4);
  -webkit-backdrop-filter:blur(40px) saturate(1.4);
  box-shadow:
    0 8px 32px rgba(0,0,0,0.4),
    inset 0 1px 0 rgba(255,255,255,0.06);
}

/* ── Logo ── */
.logo{text-align:center;margin-bottom:36px}
.logo h1{
  font-size:32px;font-weight:800;letter-spacing:-0.5px;
  background:linear-gradient(135deg,#6ea1ff 0%,#a78bfa 50%,#6ea1ff 100%);
  background-size:200% 200%;
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;
  background-clip:text;
  animation:logoShimmer 6s ease infinite;
}
@keyframes logoShimmer{0%,100%{background-position:0% 50%}50%{background-position:100% 50%}}
.logo p{
  color:rgba(255,255,255,0.35);font-size:13px;font-weight:400;
  letter-spacing:2px;text-transform:uppercase;margin-top:6px;
}

/* ── Form fields ── */
.field{margin-bottom:18px}
.field label{
  display:block;font-size:12px;font-weight:500;
  color:rgba(255,255,255,0.4);margin-bottom:7px;
  letter-spacing:0.5px;text-transform:uppercase;
}
.field input{
  width:100%;padding:14px 16px;
  background:rgba(255,255,255,0.05);
  border:1px solid rgba(255,255,255,0.08);
  border-radius:12px;
  color:#f0f0f5;font-size:15px;
  outline:none;
  transition:all 0.25s ease;
}
.field input::placeholder{color:rgba(255,255,255,0.2)}
.field input:focus{
  border-color:rgba(110,161,255,0.5);
  background:rgba(255,255,255,0.07);
  box-shadow:0 0 0 3px rgba(110,161,255,0.1);
}
.row{display:flex;gap:12px}
.row .field{flex:1}

/* ── Button ── */
.btn{
  width:100%;padding:15px;
  background:linear-gradient(135deg,#2563eb 0%,#1e4fd8 100%);
  color:#fff;border:none;border-radius:12px;
  font-size:15px;font-weight:600;
  cursor:pointer;margin-top:10px;
  transition:all 0.25s ease;
  position:relative;overflow:hidden;
}
.btn::before{
  content:'';position:absolute;inset:0;
  background:linear-gradient(135deg,rgba(255,255,255,0.1) 0%,transparent 50%);
  border-radius:12px;
}
.btn:hover{
  transform:translateY(-1px);
  box-shadow:0 8px 24px rgba(37,99,235,0.35);
}
.btn:active{transform:translateY(0)}
.btn:disabled{
  background:rgba(255,255,255,0.06);
  color:rgba(255,255,255,0.3);
  cursor:not-allowed;transform:none;box-shadow:none;
}
.btn-ghost{
  background:transparent;border:1px solid rgba(255,255,255,0.1);
  color:rgba(255,255,255,0.4);font-size:14px;margin-top:12px;
  padding:12px;
}
.btn-ghost::before{display:none}
.btn-ghost:hover{
  background:rgba(255,255,255,0.04);color:rgba(255,255,255,0.6);
  transform:none;box-shadow:none;
}

/* ── Messages ── */
.msg{
  padding:12px 16px;border-radius:12px;font-size:13px;
  margin-bottom:16px;display:none;
  backdrop-filter:blur(10px);line-height:1.5;
}
.msg-error{
  background:rgba(248,113,113,0.08);
  border:1px solid rgba(248,113,113,0.2);
  color:#fca5a5;
}
.msg-success{
  background:rgba(52,211,153,0.08);
  border:1px solid rgba(52,211,153,0.2);
  color:#6ee7b7;
}

/* ── Responsive ── */
@media(max-width:480px){
  .card{margin:16px;padding:32px 24px;border-radius:20px}
  .orb{opacity:0.2}
  .row{flex-direction:column;gap:0}
}
"""


# ---------------------------------------------------------------------------
# Login / Sign Up page
# ---------------------------------------------------------------------------
LOGIN_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>W25 — Sign In</title>
<style>
""" + _SHARED_CSS + """

/* ── Tabs ── */
.tabs{display:flex;margin-bottom:28px;border-bottom:1px solid rgba(255,255,255,0.06)}
.tab{
  flex:1;padding:12px 0;text-align:center;
  font-size:14px;font-weight:500;color:rgba(255,255,255,0.35);
  cursor:pointer;transition:all 0.25s;
  border-bottom:2px solid transparent;
  margin-bottom:-1px;
}
.tab:hover{color:rgba(255,255,255,0.6)}
.tab.active{color:#6ea1ff;border-bottom-color:#2563eb}
.pane{display:none}
.pane.active{display:block}
</style>
</head>
<body>
<div class="bg"></div>
<div class="orb orb-1"></div>
<div class="orb orb-2"></div>
<div class="orb orb-3"></div>

<div class="card">
  <div class="logo">
    <h1>W25</h1>
    <p>Intelligence</p>
  </div>

  <div id="msg" class="msg" role="alert" aria-live="polite"></div>

  <div class="tabs">
    <div class="tab active" role="tab" data-pane="login-pane">Login</div>
    <div class="tab" role="tab" data-pane="signup-pane">Sign Up</div>
  </div>

  <!-- Login pane -->
  <div id="login-pane" class="pane active" role="tabpanel">
    <form id="login-form" autocomplete="on">
      <div class="field">
        <label for="login-id">Username or Email</label>
        <input type="text" id="login-id" placeholder="you@example.com" autocomplete="username" required>
      </div>
      <div class="field">
        <label for="login-pw">Password</label>
        <input type="password" id="login-pw" placeholder="Your password" autocomplete="current-password" required>
      </div>
      <button type="submit" class="btn" id="login-btn">Login</button>
    </form>
  </div>

  <!-- Sign Up pane -->
  <div id="signup-pane" class="pane" role="tabpanel">
    <form id="signup-form" autocomplete="on">
      <div class="row">
        <div class="field">
          <label for="su-first">First Name</label>
          <input type="text" id="su-first" placeholder="Jane" autocomplete="given-name" required>
        </div>
        <div class="field">
          <label for="su-last">Last Name</label>
          <input type="text" id="su-last" placeholder="Doe" autocomplete="family-name" required>
        </div>
      </div>
      <div class="field">
        <label for="su-email">Email</label>
        <input type="email" id="su-email" placeholder="you@example.com" autocomplete="email" required>
      </div>
      <div class="field">
        <label for="su-user">Username</label>
        <input type="text" id="su-user" placeholder="janedoe" autocomplete="username" required>
      </div>
      <div class="field">
        <label for="su-pw">Password</label>
        <input type="password" id="su-pw" placeholder="Min 8 chars, 1 upper, 1 number" autocomplete="new-password" required minlength="8">
      </div>
      <div class="field">
        <label for="su-pw2">Confirm Password</label>
        <input type="password" id="su-pw2" placeholder="Repeat password" autocomplete="new-password" required>
      </div>
      <button type="submit" class="btn" id="signup-btn">Create Account</button>
    </form>
  </div>
</div>

<script>
/* ── Helpers ── */
function showMsg(text, type) {
  const el = document.getElementById('msg');
  el.textContent = text;
  el.className = 'msg ' + (type === 'error' ? 'msg-error' : 'msg-success');
  el.style.display = 'block';
}
function hideMsg() { document.getElementById('msg').style.display = 'none'; }

/* ── Tab switching ── */
document.querySelectorAll('.tab').forEach(t => {
  t.addEventListener('click', () => {
    document.querySelectorAll('.tab').forEach(x => x.classList.remove('active'));
    document.querySelectorAll('.pane').forEach(x => x.classList.remove('active'));
    t.classList.add('active');
    document.getElementById(t.dataset.pane).classList.add('active');
    hideMsg();
  });
});

function activateTab(paneId) {
  document.querySelectorAll('.tab').forEach(x => {
    x.classList.toggle('active', x.dataset.pane === paneId);
  });
  document.querySelectorAll('.pane').forEach(x => {
    x.classList.toggle('active', x.id === paneId);
  });
}

/* ── Login ── */
document.getElementById('login-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  hideMsg();
  const btn = document.getElementById('login-btn');
  const login = document.getElementById('login-id').value.trim();
  const password = document.getElementById('login-pw').value;

  if (!login || !password) { showMsg('Please fill in all fields.', 'error'); return; }

  btn.disabled = true;
  btn.textContent = 'Signing in\u2026';
  try {
    const res = await fetch('/auth/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ login, password }),
    });
    const data = await res.json();
    if (res.ok && data.success) {
      window.location.href = data.redirect || '/';
    } else {
      showMsg(data.detail || 'Invalid credentials.', 'error');
    }
  } catch (err) {
    showMsg('Network error. Please try again.', 'error');
  } finally {
    btn.disabled = false;
    btn.textContent = 'Login';
  }
});

/* ── Sign Up ── */
document.getElementById('signup-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  hideMsg();
  const btn = document.getElementById('signup-btn');
  const first_name = document.getElementById('su-first').value.trim();
  const last_name  = document.getElementById('su-last').value.trim();
  const email      = document.getElementById('su-email').value.trim();
  const username   = document.getElementById('su-user').value.trim();
  const password   = document.getElementById('su-pw').value;
  const password2  = document.getElementById('su-pw2').value;

  if (!first_name || !last_name || !email || !username || !password) {
    showMsg('Please fill in all fields.', 'error'); return;
  }
  if (password !== password2) {
    showMsg('Passwords do not match.', 'error'); return;
  }
  if (password.length < 8) { showMsg('Password must be at least 8 characters.', 'error'); return; }
  if (!/[A-Z]/.test(password)) { showMsg('Password must contain at least one uppercase letter.', 'error'); return; }
  if (!/[0-9]/.test(password)) { showMsg('Password must contain at least one number.', 'error'); return; }

  btn.disabled = true;
  btn.textContent = 'Creating account\u2026';
  try {
    const res = await fetch('/auth/signup', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ username, email, password, first_name, last_name }),
    });
    const data = await res.json();
    if (res.ok && data.success) {
      document.getElementById('signup-form').reset();
      showMsg('Account created! You can now log in.', 'success');
      activateTab('login-pane');
      document.getElementById('login-id').value = username;
      document.getElementById('login-id').focus();
    } else {
      showMsg(data.detail || 'Signup failed.', 'error');
    }
  } catch (err) {
    showMsg('Network error. Please try again.', 'error');
  } finally {
    btn.disabled = false;
    btn.textContent = 'Create Account';
  }
});
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Onboarding page
# ---------------------------------------------------------------------------
ONBOARDING_PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>W25 — Onboarding</title>
<style>
""" + _SHARED_CSS + """

.card{max-width:560px}

/* ── Onboarding specifics ── */
.title{font-size:22px;font-weight:700;margin-bottom:6px;text-align:center}
.subtitle{
  color:rgba(255,255,255,0.4);font-size:14px;
  text-align:center;margin-bottom:28px;line-height:1.5;
}

/* search */
.search-wrap{position:relative;margin-bottom:20px}
.search-wrap input{
  width:100%;padding:14px 16px 14px 44px;
  background:rgba(255,255,255,0.05);
  border:1px solid rgba(255,255,255,0.08);
  border-radius:12px;
  color:#f0f0f5;font-size:15px;outline:none;
  transition:all 0.25s ease;
}
.search-wrap input::placeholder{color:rgba(255,255,255,0.2)}
.search-wrap input:focus{
  border-color:rgba(110,161,255,0.5);
  background:rgba(255,255,255,0.07);
  box-shadow:0 0 0 3px rgba(110,161,255,0.1);
}
.search-icon{
  position:absolute;left:16px;top:50%;transform:translateY(-50%);
  color:rgba(255,255,255,0.25);font-size:16px;pointer-events:none;
}

/* results */
.results{
  max-height:220px;overflow-y:auto;margin-bottom:20px;
  scrollbar-width:thin;scrollbar-color:rgba(255,255,255,0.1) transparent;
}
.results::-webkit-scrollbar{width:5px}
.results::-webkit-scrollbar-track{background:transparent}
.results::-webkit-scrollbar-thumb{background:rgba(255,255,255,0.1);border-radius:4px}
.result-item{
  display:flex;align-items:center;justify-content:space-between;
  padding:12px 16px;margin-bottom:6px;
  background:rgba(255,255,255,0.03);
  border:1px solid rgba(255,255,255,0.06);
  border-radius:12px;cursor:pointer;
  transition:all 0.2s ease;
}
.result-item:hover{
  background:rgba(110,161,255,0.08);
  border-color:rgba(110,161,255,0.2);
}
.result-ticker{font-weight:700;font-size:14px;color:#6ea1ff}
.result-name{font-size:13px;color:rgba(255,255,255,0.5);margin-left:10px}
.result-plus{color:rgba(255,255,255,0.2);font-size:18px;font-weight:300}
.no-results{
  text-align:center;padding:16px;color:rgba(255,255,255,0.25);
  font-size:13px;
}

/* selections */
.selections-label{
  font-size:12px;font-weight:500;color:rgba(255,255,255,0.4);
  letter-spacing:0.5px;text-transform:uppercase;margin-bottom:10px;
}
.selections{margin-bottom:20px;min-height:40px}
.sel-item{
  display:flex;align-items:center;gap:10px;
  padding:10px 14px;margin-bottom:6px;
  background:rgba(110,161,255,0.06);
  border:1px solid rgba(110,161,255,0.12);
  border-radius:12px;
  animation:fadeIn 0.2s ease;
}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
.sel-ticker{font-weight:700;font-size:14px;color:#6ea1ff;min-width:55px}
.sel-name{flex:1;font-size:13px;color:rgba(255,255,255,0.45);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sel-shares{
  width:72px;padding:8px 10px;text-align:center;
  background:rgba(255,255,255,0.05);
  border:1px solid rgba(255,255,255,0.08);
  border-radius:8px;color:#f0f0f5;font-size:14px;
  outline:none;transition:border-color 0.2s;
}
.sel-shares:focus{border-color:rgba(110,161,255,0.4)}
.sel-shares::placeholder{color:rgba(255,255,255,0.15)}
.sel-remove{
  background:none;border:none;color:rgba(255,255,255,0.2);
  font-size:18px;cursor:pointer;padding:0 4px;
  transition:color 0.2s;line-height:1;
}
.sel-remove:hover{color:#fca5a5}
.empty-sel{color:rgba(255,255,255,0.15);font-size:13px;text-align:center;padding:12px 0}
</style>
</head>
<body>
<div class="bg"></div>
<div class="orb orb-1"></div>
<div class="orb orb-2"></div>
<div class="orb orb-3"></div>

<div class="card">
  <div class="logo">
    <h1>W25</h1>
    <p>Intelligence</p>
  </div>

  <div id="msg" class="msg" role="alert" aria-live="polite"></div>

  <div class="title">Set up your portfolio</div>
  <div class="subtitle">Search and add stocks from Bunq's stock list</div>

  <div class="search-wrap">
    <label for="search-input" class="search-icon">&#x1F50D;</label>
    <input type="text" id="search-input" placeholder="Search stocks (e.g. AAPL, Tesla)">
  </div>

  <div id="results" class="results"></div>

  <div class="selections-label">My Selections</div>
  <div id="selections" class="selections">
    <div class="empty-sel" id="empty-msg">No stocks selected yet</div>
  </div>

  <button class="btn" id="continue-btn">Continue</button>
  <button class="btn btn-ghost" id="skip-btn">Skip for now</button>
</div>

<script>
let selected = [];  /* [{ticker, name, shares}] */
let debounce = null;

function showMsg(text, type) {
  const el = document.getElementById('msg');
  el.textContent = text;
  el.className = 'msg ' + (type === 'error' ? 'msg-error' : 'msg-success');
  el.style.display = 'block';
}
function hideMsg() { document.getElementById('msg').style.display = 'none'; }

/* ── Search ── */
const searchInput = document.getElementById('search-input');
searchInput.addEventListener('input', () => {
  clearTimeout(debounce);
  debounce = setTimeout(doSearch, 300);
});

async function doSearch() {
  const q = searchInput.value.trim();
  const container = document.getElementById('results');
  if (q.length < 1) { container.innerHTML = ''; return; }
  try {
    const res = await fetch('/api/portfolio/search?q=' + encodeURIComponent(q));
    const data = await res.json();
    const items = data.results || data;
    if (!items.length) {
      container.innerHTML = '<div class="no-results">No stocks found</div>';
      return;
    }
    container.innerHTML = items.map(s =>
      '<div class="result-item" data-ticker="' + s.ticker + '" data-name="' + (s.name || s.ticker) + '">' +
        '<div><span class="result-ticker">' + s.ticker + '</span>' +
        '<span class="result-name">' + (s.name || '') + '</span></div>' +
        '<span class="result-plus">+</span>' +
      '</div>'
    ).join('');
    container.querySelectorAll('.result-item').forEach(el => {
      el.addEventListener('click', () => addStock(el.dataset.ticker, el.dataset.name));
    });
  } catch (err) {
    container.innerHTML = '<div class="no-results">Search error</div>';
  }
}

/* ── Add / remove stocks ── */
function addStock(ticker, name) {
  if (selected.find(s => s.ticker === ticker)) return;
  selected.push({ ticker, name, shares: 1 });
  renderSelections();
}

function removeStock(ticker) {
  selected = selected.filter(s => s.ticker !== ticker);
  renderSelections();
}

function renderSelections() {
  const container = document.getElementById('selections');
  const empty = document.getElementById('empty-msg');
  if (!selected.length) {
    container.innerHTML = '<div class="empty-sel" id="empty-msg">No stocks selected yet</div>';
    return;
  }
  container.innerHTML = selected.map((s, i) =>
    '<div class="sel-item">' +
      '<span class="sel-ticker">' + s.ticker + '</span>' +
      '<span class="sel-name">' + s.name + '</span>' +
      '<input type="number" class="sel-shares" min="1" value="' + s.shares + '" ' +
        'placeholder="Qty" data-idx="' + i + '">' +
      '<button class="sel-remove" data-ticker="' + s.ticker + '">&times;</button>' +
    '</div>'
  ).join('');
  container.querySelectorAll('.sel-shares').forEach(el => {
    el.addEventListener('change', (e) => {
      const idx = parseInt(e.target.dataset.idx);
      selected[idx].shares = Math.max(1, parseInt(e.target.value) || 1);
    });
  });
  container.querySelectorAll('.sel-remove').forEach(el => {
    el.addEventListener('click', () => removeStock(el.dataset.ticker));
  });
}

/* ── Submit ── */
async function submitOnboarding(holdings) {
  hideMsg();
  const btn = document.getElementById('continue-btn');
  btn.disabled = true;
  btn.textContent = 'Saving\u2026';
  try {
    const res = await fetch('/auth/complete-onboarding', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ holdings }),
    });
    const data = await res.json();
    if (res.ok && data.success) {
      window.location.href = data.redirect || '/';
    } else {
      showMsg(data.detail || 'Failed to save.', 'error');
    }
  } catch (err) {
    showMsg('Network error. Please try again.', 'error');
  } finally {
    btn.disabled = false;
    btn.textContent = 'Continue';
  }
}

document.getElementById('continue-btn').addEventListener('click', () => {
  const holdings = selected.map(s => ({ ticker: s.ticker, shares: s.shares }));
  submitOnboarding(holdings);
});

document.getElementById('skip-btn').addEventListener('click', () => {
  submitOnboarding([]);
});
</script>
</body>
</html>"""


# ---------------------------------------------------------------------------
# Auth API endpoints
# ---------------------------------------------------------------------------

@router.post("/auth/login")
@limiter.limit(AUTH_LIMIT)
async def login(request: Request):
    """Authenticate with username/email + password and create a session."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid request body")

    login_id = (body.get("login") or "").strip()
    password = body.get("password") or ""

    if not login_id or not password:
        raise HTTPException(400, detail="Username/email and password are required")

    user = authenticate(login_id, password)
    if not user:
        raise HTTPException(401, detail="Invalid credentials or account locked")

    session_token = create_session(user)

    redirect = "/"
    if not user.onboarded:
        redirect = "/onboarding"

    response = JSONResponse({"success": True, "redirect": redirect})
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=session_token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        secure=True,
        samesite="lax",
        path="/",
    )
    logger.info("User %s logged in", user.username)
    return response


@router.post("/auth/signup")
@limiter.limit(SIGNUP_LIMIT)
async def signup(request: Request):
    """Create a new user account."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid request body")

    username = (body.get("username") or "").strip()
    email = (body.get("email") or "").strip()
    password = body.get("password") or ""
    first_name = (body.get("first_name") or "").strip()
    last_name = (body.get("last_name") or "").strip()

    if not all([username, email, password, first_name, last_name]):
        raise HTTPException(400, detail="All fields are required")

    try:
        create_user(username, email, password, first_name, last_name)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    except Exception:
        logger.exception("Signup error")
        raise HTTPException(500, detail="Account creation failed. Please try again.")

    return {"success": True}


@router.post("/auth/complete-onboarding")
async def complete_onboarding(request: Request):
    """Save portfolio and mark user as onboarded."""
    token = request.cookies.get(SESSION_COOKIE_NAME)
    session_data = get_session_data(token)
    if not session_data:
        raise HTTPException(401, detail="Not authenticated")

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, detail="Invalid request body")

    holdings = body.get("holdings", [])
    if not isinstance(holdings, list):
        raise HTTPException(400, detail="holdings must be a list")

    # Validate each holding
    clean_holdings = []
    for h in holdings:
        ticker = (h.get("ticker") or "").strip().upper()
        shares = h.get("shares", 0)
        if not ticker:
            continue
        try:
            shares = int(shares)
        except (TypeError, ValueError):
            shares = 1
        if shares < 1:
            shares = 1
        clean_holdings.append({"ticker": ticker, "shares": shares})

    user_id = session_data["user_id"]
    set_user_portfolio(user_id, clean_holdings)
    set_user_onboarded(user_id)

    # Update in-memory session so middleware stops redirecting to /onboarding
    update_session(token, onboarded=True)

    logger.info("User %s completed onboarding with %d holdings",
                session_data.get("username"), len(clean_holdings))

    return {"success": True, "redirect": "/"}


@router.post("/auth/logout")
async def logout(request: Request):
    """Destroy session and clear cookie."""
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if token:
        destroy_session(token)

    response = JSONResponse({"success": True})
    response.delete_cookie(SESSION_COOKIE_NAME, path="/")
    return response


@router.get("/auth/me")
async def auth_me(request: Request):
    """Return authenticated user info or 401."""
    token = request.cookies.get(SESSION_COOKIE_NAME)
    session_data = get_session_data(token)
    if not session_data:
        raise HTTPException(401, detail="Not authenticated")

    return {
        "authenticated": True,
        "user_id": session_data["user_id"],
        "username": session_data["username"],
        "email": session_data["email"],
        "display_name": session_data.get("display_name", session_data["username"]),
        "onboarded": session_data.get("onboarded", False),
        "is_admin": session_data.get("is_admin", False),
    }


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """Serve the login/signup page."""
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if get_session_user(token):
        return RedirectResponse(url="/", status_code=302)
    return HTMLResponse(content=LOGIN_PAGE)


@router.get("/onboarding", response_class=HTMLResponse)
async def onboarding_page(request: Request):
    """Serve the onboarding page. Requires authentication."""
    token = request.cookies.get(SESSION_COOKIE_NAME)
    if not get_session_user(token):
        return RedirectResponse(url="/login", status_code=302)
    return HTMLResponse(content=ONBOARDING_PAGE)
