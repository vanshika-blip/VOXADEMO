const express   = require('express');
const fetch     = require('node-fetch');
const compression = require('compression');
const NodeCache = require('node-cache');
const cors      = require('cors');
const path      = require('path');
const { startPoller, jobs, getStatus } = require('./poller');

const app   = express();
const cache = new NodeCache({ stdTTL: 30 });
const GAS_URL      = process.env.GAS_URL;
const POLLER_TOKEN = process.env.POLLER_TOKEN || 'voxa-poller-secret';

app.use(cors());
app.use(compression());
app.use(express.json({ limit: '10mb' }));
app.use(express.text({ type: 'text/plain', limit: '10mb' }));
app.use(express.static(path.join(__dirname, 'public')));

// ── GAS proxy (unchanged from original) ──────────────────────────────
const CACHEABLE = ['getDashboard','getCampaigns','listAgents','listTeams','listUsers','getUsage'];
const BUST = { uploadContacts:1, triggercampaign:1, upsertUser:1, deleteUser:1, upsertTeam:1, upsertAgent:1, assignLead:1, updateLead:1 };

app.post('/api', async (req, res) => {
  let parsed = req.body;
  if (typeof parsed === 'string') {
    try { parsed = JSON.parse(parsed); } catch (e) { return res.json({ ok: false, error: 'BAD_REQUEST' }); }
  }
  const { action, session, ...rest } = parsed || {};
  if (!action) return res.json({ ok: false, error: 'NO_ACTION' });
  if (!GAS_URL) return res.status(503).json({ ok: false, error: 'SERVER_MISCONFIGURED' });

  const cacheKey = action + '|' + session + '|' + JSON.stringify(rest);
  if (CACHEABLE.includes(action)) { const hit = cache.get(cacheKey); if (hit) return res.json({ ...hit, _cached: true }); }
  if (BUST[action]) cache.flushAll();

  try {
    const r = await fetch(GAS_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'text/plain;charset=utf-8' },
      body: JSON.stringify({ action, session, ...rest }),
      timeout: 60000,
    });
    const text = await r.text();
    let data;
    try { data = JSON.parse(text); } catch (e) {
      return res.status(502).json({ ok: false, error: 'GAS_INVALID_RESPONSE', message: text.slice(0, 200) });
    }
    if (CACHEABLE.includes(action) && data.ok) cache.set(cacheKey, data);
    res.json(data);
  } catch (e) { res.status(502).json({ ok: false, error: 'UPSTREAM_ERROR', message: e.message }); }
});

// ── Poller admin routes ───────────────────────────────────────────────
// All protected by: Authorization: Bearer <POLLER_TOKEN>
// Set POLLER_TOKEN env var on Render to something secret.

function authPoller(req, res, next) {
  const auth = req.headers.authorization || '';
  if (auth !== 'Bearer ' + POLLER_TOKEN) return res.status(401).json({ ok: false, error: 'UNAUTHORIZED' });
  next();
}

// GET /poller/status
// Returns last run times, whether poll is running, recent errors
app.get('/poller/status', authPoller, (req, res) => {
  res.json({ ok: true, status: getStatus() });
});

// POST /poller/run/:job
// Force-run any job immediately.
// job names: poll | backfill | repair | sessions | callbacks | retries | archLeads | archMT | archManual
// Optional body: { "agentCode": "rm_zfunds" }  — scopes poll/backfill to one agent
//
// Example:
//   curl -X POST https://your-render-url.com/poller/run/poll \
//     -H "Authorization: Bearer voxa-poller-secret" \
//     -H "Content-Type: application/json" \
//     -d '{"agentCode":"rm_zfunds"}'
app.post('/poller/run/:job', authPoller, async (req, res) => {
  const { job } = req.params;
  const fn = jobs[job];
  if (!fn) return res.json({ ok: false, error: 'UNKNOWN_JOB', available: Object.keys(jobs) });
  try {
    console.log(`[poller] Force-run: ${job}`, req.body || '');
    const result = await fn(req.body || {});
    res.json({ ok: true, job, result });
  } catch (e) {
    res.status(500).json({ ok: false, job, error: e.message });
  }
});

// POST /poller/force-refresh
// Shortcut for force-refreshing calls.
// Body: { "agentCode": "rm_zfunds" }  OR  {}  for all agents
//
// Example — refresh all agents:
//   curl -X POST https://your-render-url.com/poller/force-refresh \
//     -H "Authorization: Bearer voxa-poller-secret" \
//     -H "Content-Type: application/json" \
//     -d '{}'
//
// Example — refresh one agent:
//   curl -X POST https://your-render-url.com/poller/force-refresh \
//     -H "Authorization: Bearer voxa-poller-secret" \
//     -H "Content-Type: application/json" \
//     -d '{"agentCode":"rm_zfunds"}'
app.post('/poller/force-refresh', authPoller, async (req, res) => {
  try {
    const { agentCode } = req.body || {};
    console.log(`[poller] Force refresh: ${agentCode || 'ALL'}`);
    const result = await jobs.poll({ agentCode });
    res.json({ ok: true, agentCode: agentCode || 'ALL', result });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// SPA fallback — frontend completely unchanged
app.get('*', (_, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Voxa running on port ${PORT}`);
  startPoller(); // starts all 9 background cron jobs
});

// Keep-alive ping for free tier
if (process.env.RENDER_EXTERNAL_URL) {
  setInterval(() => fetch(process.env.RENDER_EXTERNAL_URL + '/api', {
    method: 'POST', body: JSON.stringify({ action: 'ping' }),
    headers: { 'Content-Type': 'text/plain' },
  }).catch(() => {}), 14 * 60 * 1000);
}
