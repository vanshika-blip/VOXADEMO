'use strict';

// ═══════════════════════════════════════════════════════════════════════
// poller.js — Voxa Portal Background Jobs
// Replaces ALL GAS triggers. One file, all logic.
//
// Jobs:
//   poll          → every 1 min   (pollActiveBatches)
//   backfill      → every 5 min   (backfillMissingOutputs)
//   repair        → every 10 min  (repairUnassignedLeads)
//   sessions      → every 1 hr    (cleanupExpiredSessions)
//   callbacks     → daily 11am IST (processCallbackQueue)
//   retries       → daily 11am IST (processRetryQueue)
//   archive:leads → daily 2am IST  (archiveCompletedLeads)
//   archive:mt    → daily 3am IST  (archiveCompletedMT)
//   archive:manual→ daily 4am IST  (archiveManualTracker)
// ═══════════════════════════════════════════════════════════════════════

const { google } = require('googleapis');
const fetch      = require('node-fetch');
const cron       = require('node-cron');

// ── Config ──────────────────────────────────────────────────────────────
const SPREADSHEET_ID       = process.env.SPREADSHEET_ID || '1C6-YtK0x2Q5MLAFihh9_vzZ-oC_e0qZRwrySZraz_Ow';
const HUNAR_API_BASE       = 'https://api.voice.hunar.ai';
const HUNAR_API_KEY        = process.env.HUNAR_API_KEY  || 'hunar_va_live_sk_qH3Xewk3DcBI68rKsMtLmBxw60earGaMWQdcZIEW_mcmIdwn_x8FRQ';
const DEFAULT_TIMEZONE     = 'Asia/Kolkata';
const EST_SECONDS_PER_CALL = 60;
const MAX_POLL_FAILURES    = 5;

const FINAL_STATUSES = new Set(['COMPLETED', 'NOT_CONNECTED', 'CANCELLED', 'FAILED']);

// ── Sheet names ─────────────────────────────────────────────────────────
const S = {
  USERS:        'Users',
  TEAMS:        'Teams',
  AGENTS:       'Agents',
  TRIGGER_LOG:  'Trigger Log',
  SESSIONS:     'Sessions',
  AUDIT:        'Audit Log',
  CB_QUEUE:     '_Callback_Queue',
  RETRY_QUEUE:  '_Retry_Queue',
  IL:           '_Interview_Lineup',
  MANUAL:       '_Manual_Tracker',
};

const AGT = {
  CI:  '_Call_Input',
  CT:  '_Campaign_Tracker',
  MT:  '_Master_Tracker',
  QL:  '_Qualified_Leads',
  NC:  '_Not_Connected',
  CB:  '_Callbacks',
};

const ARCH = {
  LEADS:  '_Completed_Leads',
  MT:     '_Completed_MT',
  MANUAL: '_Completed_Manual',
};

const IL_HEADERS = [
  'Source','Call ID','Callee Name','Mobile Number',
  'Agent Code','Agent Name','Campaign Name','Request ID',
  'Started At','Month','Selection Process','Turnup Status',
  'Assigned Recruiter Email','Assigned Recruiter Name','Client',
  'Email','DOB','Qualification','Work Experience',
  'Current CTC','Expected CTC','Notice Period',
  'Role','Location','CIBIL Score','SPOC Name','Current Employer','CV Link',
  'Date Added',
];

const CB_QUEUE_HEADERS = [
  'Queue ID','Agent Code','Agent ID','Call ID','Request ID','Callee Name','Mobile Number',
  'Callback Field','Callback Value','Assigned To Email','Recruiter Name','Team',
  'Scheduled Date','Status','New Request ID','Created At','Triggered At',
];

const RETRY_QUEUE_HEADERS = [
  'Queue ID','Agent Code','Agent ID','Original Request ID','Team',
  'Retry After Date','Status','Lead Count','New Request ID','Created At','Triggered At',
];

const CB_EXTRA = ['Callback Field','Callback Value','Scheduled Date','Queue ID','Trigger Status','Retry Request ID','Date Added'];
const NC_HEADERS = ['Call ID','Callee Name','Mobile Number','Status','Request ID','Retry Count','Retries Left','Next Retry Scheduled At','Triggered By','Last Updated','Retry Scheduled Date','Trigger Status','Retry Request ID'];

// ── Job status tracker (for /poller/status endpoint) ──────────────────
const STATUS = {
  lastPoll:      null,
  lastBackfill:  null,
  lastRepair:    null,
  lastSessions:  null,
  lastCallbacks: null,
  lastRetries:   null,
  lastArchive:   null,
  pollRunning:   false,
  errors:        [],
};

function logStatus(job, result) {
  STATUS['last' + job.charAt(0).toUpperCase() + job.slice(1)] = { time: new Date().toISOString(), result };
  if (STATUS.errors.length > 20) STATUS.errors = STATUS.errors.slice(-20);
}

// ── Google Sheets client ────────────────────────────────────────────────
let _sheets = null;

async function getSheets() {
  if (_sheets) return _sheets;
  const raw = process.env.GOOGLE_SERVICE_ACCOUNT_B64;
  if (!raw) throw new Error('GOOGLE_SERVICE_ACCOUNT_B64 not set');
  const creds = JSON.parse(Buffer.from(raw, 'base64').toString('utf8'));
  const auth  = new google.auth.GoogleAuth({ credentials: creds, scopes: ['https://www.googleapis.com/auth/spreadsheets'] });
  _sheets = google.sheets({ version: 'v4', auth });
  return _sheets;
}

// ── Sheets helpers ──────────────────────────────────────────────────────
async function readSheet(sheetName, ssId) {
  ssId = ssId || SPREADSHEET_ID;
  try {
    const res = await _sheets.spreadsheets.values.get({ spreadsheetId: ssId, range: `'${sheetName}'` });
    return res.data.values || [];
  } catch (e) {
    if (e.code === 400 || e.code === 404) return [];
    throw e;
  }
}

async function appendRows(sheetName, rows, ssId) {
  ssId = ssId || SPREADSHEET_ID;
  if (!rows.length) return;
  await _sheets.spreadsheets.values.append({
    spreadsheetId: ssId,
    range: `'${sheetName}'!A1`,
    valueInputOption: 'USER_ENTERED',
    insertDataOption: 'INSERT_ROWS',
    requestBody: { values: rows },
  });
}

async function batchWrite(updates, ssId) {
  ssId = ssId || SPREADSHEET_ID;
  if (!updates.length) return;
  for (let i = 0; i < updates.length; i += 50) {
    const chunk = updates.slice(i, i + 50);
    await _sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: ssId,
      requestBody: { valueInputOption: 'USER_ENTERED', data: chunk.map(u => ({ range: u.range, values: [u.values] })) },
    });
    if (i + 50 < updates.length) await sleep(1100);
  }
}

async function deleteRows(sheetName, rowNumbers, ssId) {
  ssId = ssId || SPREADSHEET_ID;
  if (!rowNumbers.length) return;
  const meta    = await _sheets.spreadsheets.get({ spreadsheetId: ssId });
  const sheet   = meta.data.sheets.find(s => s.properties.title === sheetName);
  if (!sheet) return;
  const sheetId = sheet.properties.sheetId;
  const sorted  = [...new Set(rowNumbers)].sort((a,b) => b-a);
  const reqs    = sorted.map(n => ({ deleteDimension: { range: { sheetId, dimension: 'ROWS', startIndex: n-1, endIndex: n } } }));
  for (let i = 0; i < reqs.length; i += 100) {
    await _sheets.spreadsheets.batchUpdate({ spreadsheetId: ssId, requestBody: { requests: reqs.slice(i, i+100) } });
    if (i + 100 < reqs.length) await sleep(500);
  }
}

async function ensureExternalSheet(ssId, sheetName, headers) {
  let existing = [];
  try {
    const res = await _sheets.spreadsheets.values.get({ spreadsheetId: ssId, range: `'${sheetName}'!1:1` });
    existing = (res.data.values || [])[0] || [];
  } catch (e) {
    if (e.code !== 400 && e.code !== 404) throw e;
  }
  if (!existing.length) {
    try { await _sheets.spreadsheets.batchUpdate({ spreadsheetId: ssId, requestBody: { requests: [{ addSheet: { properties: { title: sheetName } } }] } }); } catch (_) {}
    await _sheets.spreadsheets.values.update({ spreadsheetId: ssId, range: `'${sheetName}'!A1`, valueInputOption: 'USER_ENTERED', requestBody: { values: [headers] } });
    return;
  }
  const missing = headers.filter(h => !existing.includes(h));
  if (missing.length) {
    await _sheets.spreadsheets.values.update({ spreadsheetId: ssId, range: `'${sheetName}'!${col(existing.length+1)}1`, valueInputOption: 'USER_ENTERED', requestBody: { values: [missing] } });
  }
}

// ── Row helpers ─────────────────────────────────────────────────────────
function set(row, headers, name, val) { const i = headers.indexOf(name); if (i >= 0) row[i] = val ?? ''; }
function get(row, headers, name)      { const i = headers.indexOf(name); return i >= 0 ? (row[i] || '') : ''; }
function pad(row, len)                { const r = [...(row||[])]; while (r.length < len) r.push(''); return r; }
function col(n)                       { let s=''; while(n>0){const r=(n-1)%26; s=String.fromCharCode(65+r)+s; n=Math.floor((n-1)/26);} return s; }
function sleep(ms)                    { return new Promise(r => setTimeout(r, ms)); }
function todayIST()                   { return new Date().toLocaleDateString('en-CA', { timeZone: DEFAULT_TIMEZONE }); }
function futureDateIST(days)          { return new Date(Date.now() + days*86400000).toLocaleDateString('en-CA', { timeZone: DEFAULT_TIMEZONE }); }

// ── Hunar API ───────────────────────────────────────────────────────────
const HUNAR_HEADERS = { 'X-API-Key': HUNAR_API_KEY, 'Content-Type': 'application/json' };

async function hunarGetCall(callId) {
  try {
    const res  = await fetch(`${HUNAR_API_BASE}/external/v1/calls/${encodeURIComponent(callId)}/`, { headers: HUNAR_HEADERS, timeout: 15000 });
    const text = await res.text();
    if (res.ok) return { ok: true, data: JSON.parse(text) };
    return { ok: false, error: `HTTP ${res.status}: ${text.slice(0,200)}` };
  } catch (e) { return { ok: false, error: e.message }; }
}

async function hunarBulkCall(payload) {
  try {
    const res  = await fetch(`${HUNAR_API_BASE}/external/v1/calls/bulk/`, { method:'POST', headers: HUNAR_HEADERS, body: JSON.stringify({ timezone: DEFAULT_TIMEZONE, ...payload }), timeout: 30000 });
    const text = await res.text();
    if (res.ok) return { ok: true, data: JSON.parse(text) };
    return { ok: false, error: `HTTP ${res.status}: ${text.slice(0,500)}` };
  } catch (e) { return { ok: false, error: e.message }; }
}

// ── Parse helpers ───────────────────────────────────────────────────────
function parseAgents(rows, activeOnly = false) {
  if (!rows || rows.length < 2) return [];
  const h   = rows[0];
  const g   = (r, name) => { const i = h.indexOf(name); return i >= 0 ? String(r[i]||'').trim() : ''; };
  return rows.slice(1).map(r => {
    let cv=[], rs={}, qv=[], qr=[];
    try { cv = JSON.parse(g(r,'Custom Variables')||'[]'); }  catch(_){}
    try { rs = JSON.parse(g(r,'Result Schema')||'{}'); }     catch(_){}
    try { const raw=g(r,'Qualification Values'); qv = raw.startsWith('[')?JSON.parse(raw):(raw?[raw]:[]); } catch(_){}
    try { const raw=g(r,'Qualification Rules');  if(raw.startsWith('[')) qr=JSON.parse(raw); } catch(_){}
    const active = g(r,'Active').toUpperCase();
    return {
      agentCode: g(r,'Agent Code'), agentId: g(r,'Agent ID'),
      displayName: g(r,'Display Name'), description: g(r,'Description'),
      language: g(r,'Language')||'ENGLISH', voicePersona: g(r,'Voice Persona'),
      active: active==='TRUE'||active==='1',
      customVariables: Array.isArray(cv)?cv:[], resultSchema: rs||{},
      qualificationField: g(r,'Qualification Field'),
      qualificationValues: Array.isArray(qv)?qv.filter(Boolean):[],
      qualificationRules: Array.isArray(qr)?qr:[],
      estSecondsPerCall: Number(g(r,'Est Seconds Per Call')||EST_SECONDS_PER_CALL),
      createdBy: g(r,'Created By').toLowerCase(), clientName: g(r,'Client Name'),
    };
  }).filter(a => a.agentCode && (!activeOnly || a.active));
}

function parseUsers(rows) {
  if (!rows || rows.length < 2) return [];
  const h = rows[0];
  const g = (r,name) => { const i=h.indexOf(name); return i>=0?String(r[i]||'').trim():''; };
  return rows.slice(1).map(r => ({
    email: g(r,'Email').toLowerCase(), name: g(r,'Name'),
    role: g(r,'Role').toLowerCase(), team: g(r,'Team'),
    dailyMinuteLimit: Number(g(r,'Daily Minute Limit')||0),
    active: g(r,'Active').toUpperCase()==='TRUE',
  })).filter(u => u.email);
}

function parseTeams(rows) {
  if (!rows || rows.length < 2) return [];
  const h = rows[0];
  const g = (r,name) => { const i=h.indexOf(name); return i>=0?String(r[i]||'').trim():''; };
  return rows.slice(1).map(r => ({ id: g(r,'Team ID'), name: g(r,'Team Name'), spreadsheetId: g(r,'Spreadsheet ID') })).filter(t => t.name);
}

function resultFieldNames(schema) {
  if (!schema || typeof schema !== 'object') return [];
  if (schema.properties) return Object.keys(schema.properties);
  return Object.keys(schema).filter(k => k!=='type'&&k!=='required');
}

// ── Qualification logic ─────────────────────────────────────────────────
function isQualified(agent, result) {
  const rules = agent.qualificationRules;
  if (Array.isArray(rules) && rules.length > 0) {
    return rules.every(rule => {
      if (!rule.field) return true;
      const val = result[rule.field];
      if (!val) return false;
      const kws = Array.isArray(rule.keywords) ? rule.keywords.filter(Boolean) : [];
      if (!kws.length) return !!val;
      const lower = String(val).toLowerCase();
      return kws.some(kw => lower.includes(String(kw).toLowerCase()));
    });
  }
  if (!agent.qualificationField) return false;
  const val = result[agent.qualificationField];
  if (!val) return false;
  const vals = agent.qualificationValues||[];
  if (!vals.length) return !!val;
  const lower = String(val).toLowerCase().trim();
  return vals.some(v => lower.includes(String(v).toLowerCase().trim()));
}

function isInterviewLinedUp(val) {
  if (!val) return false;
  const s = String(val).toLowerCase();
  return s.includes('interview lined up')||s.includes('interested: interview')||s.includes('interested - interview')||s.includes('interested – interview');
}

function detectCallbackField(agent, result) {
  const CB = ['call back','callback','call-back','ring back','follow up','followup','follow-up','reschedule'];
  const match = v => v && CB.some(kw => String(v).toLowerCase().includes(kw));
  const rules = agent.qualificationRules;
  if (Array.isArray(rules)) { for (const r of rules) { if (r.field && match(result[r.field])) return r.field; } }
  if (agent.qualificationField && match(result[agent.qualificationField])) return agent.qualificationField;
  for (const k of Object.keys(result||{})) { if (match(result[k])) return k; }
  return null;
}

// ── User/team helpers ───────────────────────────────────────────────────
function userMap(users) { const m={}; users.forEach(u => { m[u.email]=u; }); return m; }

function triggerMap(tlRows) {
  const m={};
  if (!tlRows||tlRows.length<2) return m;
  const h=tlRows[0]; const rc=h.indexOf('Request ID'); const ec=h.indexOf('User Email'); const ac=h.indexOf('Agent Code');
  tlRows.slice(1).forEach(r => { const id=String(r[rc]||'').trim(); const em=String(r[ec]||'').toLowerCase().trim(); if(id&&em) m[id]={email:em,agentCode:String(r[ac]||'')}; });
  return m;
}

function resolveAssignment(reqId, mobile, mtRow, mtH, uMap) {
  const trigIdx = mtH.indexOf('Triggered By');
  const by = trigIdx>=0 ? String(mtRow[trigIdx]||'').toLowerCase().trim() : '';
  if (by && uMap[by]) return { assignEmail: by, recruiterName: uMap[by].name||by };
  return { assignEmail:'', recruiterName:'' };
}

function systemTrigger(team, users) {
  if (team) { const m=users.filter(u=>u.team===team&&u.active); const l=m.find(u=>u.role==='team_lead'||u.role==='individual_contributor'); if(l) return l.email; const r=m.find(u=>u.role==='recruiter'); if(r) return r.email; }
  const sa=users.find(u=>u.role==='super_admin'); return sa?sa.email:'system';
}

function agentTeamMap(agents, users) {
  const e2t={}; users.forEach(u=>{if(u.team)e2t[u.email.toLowerCase()]=u.team;});
  const m={}; agents.forEach(a=>{const t=e2t[(a.createdBy||'').toLowerCase()]; if(t) m[a.agentCode]=t;}); return m;
}

// ── Seed Master Tracker ─────────────────────────────────────────────────
async function seedMT(agent, createdCalls, requestId, triggeredBy) {
  if (!createdCalls||!createdCalls.length) return;
  const mtName = agent.agentCode + AGT.MT;
  const mtAll  = await readSheet(mtName);
  if (!mtAll.length) return;
  const h = mtAll[0]; const cidCol = h.indexOf('Call ID');
  const existing = new Set(); if(cidCol>=0&&mtAll.length>1) mtAll.slice(1).forEach(r=>{if(r[cidCol])existing.add(String(r[cidCol]).trim());});
  const rows = [];
  createdCalls.forEach(c=>{
    const cid=String(c.id||'').trim(); if(!cid||existing.has(cid)) return; existing.add(cid);
    const row=new Array(h.length).fill('');
    const s=(name,val)=>{const i=h.indexOf(name);if(i>=0)row[i]=val;};
    s('Call ID',cid); s('Request ID',c.request_id||requestId); s('Callee Name',c.callee_name||''); s('Mobile Number',c.mobile_number||''); s('Status',c.status||'INITIATED'); s('Triggered By',triggeredBy); s('Created At',new Date().toISOString());
    rows.push(row);
  });
  if (rows.length) await appendRows(mtName, rows);
}

// ── Refresh Campaign Tracker ────────────────────────────────────────────
async function refreshCT(agent) {
  try {
    const [ctAll,mtAll,qlAll] = await Promise.all([readSheet(agent.agentCode+AGT.CT), readSheet(agent.agentCode+AGT.MT), readSheet(agent.agentCode+AGT.QL)]);
    if (ctAll.length<2||mtAll.length<2) return;
    const mtH=mtAll[0]; const ridC=mtH.indexOf('Request ID'); const stC=mtH.indexOf('Status'); const durC=mtH.indexOf('Duration (Minutes)');
    const stats={};
    mtAll.slice(1).forEach(r=>{
      const rid=String(r[ridC]||''); if(!rid) return;
      if(!stats[rid]) stats[rid]={total:0,completed:0,connected:0,notConnected:0,failed:0,minutes:0,qualified:0};
      stats[rid].total++;
      const s=String(r[stC]||'').toUpperCase();
      if(s==='COMPLETED'){stats[rid].completed++;stats[rid].connected++;}
      if(s==='NOT_CONNECTED') stats[rid].notConnected++;
      if(s==='FAILED'||s==='CANCELLED') stats[rid].failed++;
      stats[rid].minutes+=Number(r[durC]||0);
    });
    if(qlAll.length>1){const qlH=qlAll[0];const qlRid=qlH.indexOf('Request ID');if(qlRid>=0)qlAll.slice(1).forEach(r=>{const rid=String(r[qlRid]||'');if(rid&&stats[rid])stats[rid].qualified++;});}
    const updates=[];
    ctAll.slice(1).forEach((r,idx)=>{
      const rid=String(r[0]||''); const s=stats[rid]; if(!s) return;
      const done=s.completed+s.notConnected+s.failed;
      const status=done>=s.total?'COMPLETED':'IN_PROGRESS';
      updates.push({range:`'${agent.agentCode+AGT.CT}'!F${idx+2}:N${idx+2}`, values:[status,s.completed,s.connected,s.notConnected,s.failed,s.qualified,Math.round(s.minutes*100)/100,r[12]||0,new Date().toISOString()]});
    });
    if(updates.length) await batchWrite(updates);
  } catch(e){ console.error(`[${agent.agentCode}] refreshCT:`,e.message); }
}

// ── Interview Lineup ────────────────────────────────────────────────────
async function addToLineup(source, payload) {
  try {
    const ilAll = await readSheet(S.IL);
    if (!ilAll.length) return;
    const headers = ilAll[0];
    const out = {}; IL_HEADERS.forEach(h=>{out[h]='';});
    let lookupCallId = '';

    if (source === 'AI') {
      const { agent, mtRow, mtH, qlRow, qlH, callId, requestId, campaignName } = payload;
      if (!callId) return;
      lookupCallId = callId;
      const fromMT  = n => { const i=mtH.indexOf(n);           return i>=0?mtRow[i]:''; };
      const fromQL  = n => { const i=qlH.indexOf(n);           return i>=0?qlRow[i]:''; };
      const fromOut = n => { const i=mtH.indexOf('out.'+n);    return i>=0?mtRow[i]:''; };
      const fromIn  = n => { const i=mtH.indexOf('in.'+n);     return i>=0?mtRow[i]:''; };
      const pick = (...candidates) => { for(const c of candidates){const v=fromOut(c);if(v&&String(v).trim())return String(v);} for(const c of candidates){const v=fromIn(c);if(v&&String(v).trim())return String(v);} return ''; };
      const startedAt = fromMT('Started At');
      let month=''; try{if(startedAt){const d=new Date(startedAt);if(!isNaN(d.getTime()))month=d.toLocaleDateString('en-CA',{timeZone:DEFAULT_TIMEZONE}).slice(0,7);}}catch(_){}
      const ae=String(fromQL('Assigned To Email')||'').trim().toLowerCase();
      const rn=String(fromQL('Recruiter')||'').trim();
      out['Source']='AI'; out['Call ID']=callId; out['Callee Name']=String(fromMT('Callee Name')||''); out['Mobile Number']=String(fromMT('Mobile Number')||'');
      out['Agent Code']=agent.agentCode; out['Agent Name']=agent.displayName||agent.agentCode;
      out['Campaign Name']=campaignName||''; out['Request ID']=requestId||''; out['Started At']=startedAt||''; out['Month']=month;
      out['Selection Process']=String(fromQL('Selection Process')||''); out['Turnup Status']=String(fromQL('Turnup Status')||'');
      out['Assigned Recruiter Email']=ae; out['Assigned Recruiter Name']=rn; out['Client']=String(agent.clientName||'');
      out['Email']           = pick('email','Email','email_id','emailAddress');
      out['DOB']             = pick('dob','DOB','date_of_birth');
      out['Qualification']   = pick('qualification','Qualification','education','highest_qualification');
      out['Work Experience'] = pick('work_experience','experience','total_experience','years_of_experience');
      out['Current CTC']     = pick('current_ctc','ctc','currentCtc','current_salary');
      out['Expected CTC']    = pick('expected_ctc','expectedCtc','expected_salary');
      out['Notice Period']   = pick('notice_period','noticePeriod','notice');
      out['Role']            = pick('role','Role','job_role','position');
      out['Location']        = pick('location','Location','current_location','city');
      out['CIBIL Score']     = pick('cibil','cibil_score','credit_score');
      out['SPOC Name']       = pick('spoc','spoc_name','poc','point_of_contact');
      out['Current Employer']= pick('current_employer','employer','company','current_company');
      out['CV Link']         = pick('cv_link','cv','resume','resume_link');
      out['Date Added']=new Date().toISOString();
    } else if (source === 'Manual') {
      const { entry, uniqueId } = payload;
      if (!uniqueId) return;
      lookupCallId = 'MT_' + uniqueId;
      let month=''; try{if(entry['Date']){const d=new Date(entry['Date']);if(!isNaN(d.getTime()))month=d.toLocaleDateString('en-CA',{timeZone:DEFAULT_TIMEZONE}).slice(0,7);}}catch(_){}
      out['Source']='Manual'; out['Call ID']=lookupCallId;
      out['Callee Name']=String(entry['Candidate Name']||''); out['Mobile Number']=String(entry['Contact Number']||'');
      out['Assigned Recruiter Email']=String(entry['Added By Email']||''); out['Assigned Recruiter Name']=String(entry['Added By Name']||'');
      out['Client']=String(entry['Client']||''); out['Role']=String(entry['Role']||''); out['Location']=String(entry['Location']||'');
      out['Started At']=entry['Date']||''; out['Month']=month; out['Date Added']=new Date().toISOString();
    }

    // Idempotency check
    const cidColIdx = headers.indexOf('Call ID');
    if (cidColIdx>=0 && ilAll.length>1) {
      const existIdx = ilAll.slice(1).findIndex(r => String(r[cidColIdx]||'').trim()===lookupCallId);
      if (existIdx>=0) return; // already in lineup
    }

    const rowVals = headers.map(h => out[h]!==undefined?out[h]:'');
    await appendRows(S.IL, [rowVals]);
  } catch(e){ console.error('addToLineup error:',e.message); }
}

// ── Enqueue callback ────────────────────────────────────────────────────
async function enqueueCallback(agent, callId, result, qlRow, qlH, mtRow, mtH) {
  try {
    const cbField = detectCallbackField(agent, result); if (!cbField) return;
    const cbValue     = String(result[cbField]||'');
    const assignEmail = String(qlRow[qlH.indexOf('Assigned To Email')]||'').toLowerCase().trim();
    const recrName    = String(qlRow[qlH.indexOf('Recruiter')]||'');
    const calleeName  = String(mtRow[mtH.indexOf('Callee Name')]||'');
    const mobile      = String(mtRow[mtH.indexOf('Mobile Number')]||'');
    const reqId       = String(mtRow[mtH.indexOf('Request ID')]||'');
    const queueId     = 'CB_'+callId+'_'+Date.now();
    await appendRows(S.CB_QUEUE, [[queueId,agent.agentCode,agent.agentId,callId,reqId,calleeName,mobile,cbField,cbValue,assignEmail,recrName,'',futureDateIST(1),'PENDING','',new Date().toISOString(),'']]);
    // Also to agent's Callbacks sheet
    const cbAll = await readSheet(agent.agentCode+AGT.CB);
    if (cbAll.length>0) {
      const cbH=cbAll[0]; const cbRow=new Array(cbH.length).fill('');
      qlH.forEach((h,i)=>{const ci=cbH.indexOf(h);if(ci>=0)cbRow[ci]=qlRow[i];});
      CB_EXTRA.forEach(name=>{const ci=cbH.indexOf(name);if(ci>=0){const vals={['Callback Field']:cbField,['Callback Value']:cbValue,['Scheduled Date']:futureDateIST(1),['Queue ID']:queueId,['Trigger Status']:'PENDING',['Date Added']:new Date().toISOString()};cbRow[ci]=vals[name]||'';}});
      await appendRows(agent.agentCode+AGT.CB, [cbRow]);
    }
  } catch(e){ console.error(`[${agent.agentCode}] enqueueCallback:`,e.message); }
}

// ── Enqueue retry ───────────────────────────────────────────────────────
async function enqueueRetry(agent, reqId, triggeredBy, mtH, mtRows) {
  try {
    if (!reqId||reqId.startsWith('NC_AUTO_')||reqId.startsWith('CB_AUTO_')) return;
    const rqAll = await readSheet(S.RETRY_QUEUE);
    if (rqAll.length>1){ const h=rqAll[0]; const oc=h.indexOf('Original Request ID'); if(oc>=0&&rqAll.slice(1).some(r=>String(r[oc]||')===reqId'))) return; }
    const stC=mtH.indexOf('Status'); const ridC=mtH.indexOf('Request ID');
    let lc=0; mtRows.forEach(r=>{if(String(r[ridC]||'')!==reqId)return;const s=String(r[stC]||'').toUpperCase();if(s==='NOT_CONNECTED'||s==='FAILED')lc++;});
    if(!lc) return;
    await appendRows(S.RETRY_QUEUE, [['RT_'+reqId+'_'+Date.now(),agent.agentCode,agent.agentId,reqId,'',futureDateIST(7),'PENDING',lc,'',new Date().toISOString(),'']]);
  } catch(e){ console.error(`[${agent.agentCode}] enqueueRetry:`,e.message); }
}

// ── Flush NC leads ──────────────────────────────────────────────────────
async function flushNC(agent, reqId, mtH, mtRows) {
  try {
    const ncName=agent.agentCode+AGT.NC; const ncAll=await readSheet(ncName);
    const ncExist=new Set(); if(ncAll.length>1) ncAll.slice(1).forEach(r=>{if(r[0])ncExist.add(String(r[0]));});
    const cidC=mtH.indexOf('Call ID'); const stC=mtH.indexOf('Status'); const calleeC=mtH.indexOf('Callee Name'); const mobC=mtH.indexOf('Mobile Number'); const trigC=mtH.indexOf('Triggered By'); const ridC=mtH.indexOf('Request ID');
    const toAdd=[];
    mtRows.forEach(r=>{
      if(String(r[ridC]||'')!==reqId)return;
      const s=String(r[stC]||'').toUpperCase(); if(s!=='NOT_CONNECTED'&&s!=='FAILED')return;
      const cid=String(r[cidC]||'').trim(); if(!cid||ncExist.has(cid))return;
      toAdd.push([cid,calleeC>=0?String(r[calleeC]||''):'',mobC>=0?String(r[mobC]||''):'',s,reqId,0,0,'',trigC>=0?String(r[trigC]||''):'',new Date().toISOString(),futureDateIST(7),'PENDING','']);
      ncExist.add(cid);
    });
    if(toAdd.length) await appendRows(ncName,toAdd);
  } catch(e){ console.error(`[${agent.agentCode}] flushNC:`,e.message); }
}

// ═══════════════════════════════════════════════════════════════════════
// CORE POLL
// ═══════════════════════════════════════════════════════════════════════
const _failCounts = {};

async function pollAgent(agent, allUsers) {
  const mtName=agent.agentCode+AGT.MT; const qlName=agent.agentCode+AGT.QL;
  const [mtAll,qlAll,ctAll] = await Promise.all([readSheet(mtName),readSheet(qlName),readSheet(agent.agentCode+AGT.CT)]);
  if(mtAll.length<2) return {fetched:0,updated:0,errors:0,qlAdded:0};

  const mtH=mtAll[0]; const mtData=mtAll.slice(1);
  const cidC=mtH.indexOf('Call ID'); const stC=mtH.indexOf('Status'); const ridC=mtH.indexOf('Request ID');
  if(cidC<0||stC<0) return {fetched:0,updated:0,errors:0,qlAdded:0};

  const rf=resultFieldNames(agent.resultSchema); const cv=agent.customVariables||[];
  const qlH=qlAll.length>0?qlAll[0]:[]; const qlCidC=qlH.indexOf('Call ID');
  const qlExist=new Set(); if(qlCidC>=0&&qlAll.length>1) qlAll.slice(1).forEach(r=>{if(r[qlCidC])qlExist.add(String(r[qlCidC]).trim());});

  const ctStatus={}; if(ctAll.length>1) ctAll.slice(1).forEach(r=>{const id=String(r[0]||'');const s=String(r[5]||'IN_PROGRESS').toUpperCase();if(id)ctStatus[id]=s;});
  const uMap=userMap(allUsers);

  const mtUpdates=[]; const qlNewRows=[]; let fetched=0,updated=0,errors=0,qlAdded=0;

  for(let i=0;i<mtData.length;i++){
    const row=mtData[i]; const callId=String(row[cidC]||'').trim(); const status=String(row[stC]||'').toUpperCase(); const reqId=ridC>=0?String(row[ridC]||''):'';
    if(!callId) continue;
    const campaignDone=reqId&&ctStatus[reqId]==='COMPLETED';
    if(status==='COMPLETED'){const hasR=rf.some(f=>{const c=mtH.indexOf('out.'+f);return c>=0&&String(row[c]||'').trim()!=='';});if(hasR) continue;}
    else if(FINAL_STATUSES.has(status)){if(campaignDone) continue;}

    fetched++;
    const r=await hunarGetCall(callId);

    if(!r.ok){
      errors++;
      const fkey=agent.agentCode+':'+callId; _failCounts[fkey]=(_failCounts[fkey]||0)+1;
      console.log(`  [${agent.agentCode}] FAIL ${callId} (${_failCounts[fkey]}x): ${r.error}`);
      if(_failCounts[fkey]>=MAX_POLL_FAILURES){
        const nr=pad([...row],mtH.length); set(nr,mtH,'Status','FAILED'); set(nr,mtH,'Engagement Status','POLL_GAVE_UP: '+String(r.error).slice(0,200)); set(nr,mtH,'Updated At',new Date().toISOString());
        mtUpdates.push({range:`'${mtName}'!A${i+2}`,values:nr}); delete _failCounts[fkey]; updated++;
      }
      await sleep(200); continue;
    }
    delete _failCounts[agent.agentCode+':'+callId];

    const d=r.data; const newStatus=String(d.status||status).toUpperCase(); const result=d.result||{};
    const nr=pad([...row],mtH.length);
    set(nr,mtH,'Status',newStatus); set(nr,mtH,'Duration (Minutes)',d.duration_minutes??0); set(nr,mtH,'Duration (Seconds)',d.duration_seconds??0);
    set(nr,mtH,'Started At',d.started_at||''); set(nr,mtH,'Ended At',d.ended_at||''); set(nr,mtH,'Answered By',d.answered_by||'');
    set(nr,mtH,'Engagement Status',d.engagement_status||''); set(nr,mtH,'Call Ended By',d.call_ended_by||''); set(nr,mtH,'Recording URL',d.recording_url||''); set(nr,mtH,'Updated At',new Date().toISOString());
    cv.forEach(c=>{const src=d.custom_data&&d.custom_data[c];if(src!==undefined)set(nr,mtH,'in.'+c,src);});
    rf.forEach(f=>set(nr,mtH,'out.'+f,result[f]!==undefined?result[f]:''));
    mtUpdates.push({range:`'${mtName}'!A${i+2}`,values:nr}); updated++;

    if(newStatus==='COMPLETED'&&qlH.length&&!qlExist.has(callId)&&isQualified(agent,result)){
      const qlRow=new Array(qlH.length).fill('');
      qlH.forEach((h,k)=>{const mi=mtH.indexOf(h);if(mi>=0)qlRow[k]=nr[mi];});
      const mobC=mtH.indexOf('Mobile Number'); const mobile=mobC>=0?String(row[mobC]||''):'';
      const {assignEmail,recruiterName}=resolveAssignment(reqId,mobile,row,mtH,uMap);
      const ac=qlH.indexOf('Assigned To Email'); const rc=qlH.indexOf('Recruiter'); const dc=qlH.indexOf('Date Added');
      if(ac>=0&&assignEmail)qlRow[ac]=assignEmail; if(rc>=0&&recruiterName)qlRow[rc]=recruiterName; if(dc>=0)qlRow[dc]=new Date().toISOString();
      qlNewRows.push(qlRow); qlExist.add(callId); qlAdded++;
      await enqueueCallback(agent,callId,result,qlRow,qlH,nr,mtH);
      try{if(Object.values(result).some(v=>isInterviewLinedUp(v)))await addToLineup('AI',{agent,mtRow:nr,mtH,qlRow,qlH,callId,requestId:reqId});}catch(_){}
    }
    await sleep(200);
  }

  if(mtUpdates.length) await batchWrite(mtUpdates);
  if(qlNewRows.length) await appendRows(qlName,qlNewRows);

  if(updated>0){
    await refreshCT(agent);
    const ctFresh=await readSheet(agent.agentCode+AGT.CT); const mtFresh=await readSheet(mtName);
    const mtFH=mtFresh.length>0?mtFresh[0]:mtH; const mtFD=mtFresh.slice(1);
    if(ctFresh.length>1){
      for(let ci=1;ci<ctFresh.length;ci++){
        const rid=String(ctFresh[ci][0]||''); const prev=ctStatus[rid]||'IN_PROGRESS'; const now=String(ctFresh[ci][5]||'').toUpperCase();
        if(now==='COMPLETED'&&prev!=='COMPLETED'){await flushNC(agent,rid,mtFH,mtFD);const trig=String(ctFresh[ci][2]||'');await enqueueRetry(agent,rid,trig,mtFH,mtFD);}
      }
    }
  }
  return {fetched,updated,errors,qlAdded};
}

// ═══════════════════════════════════════════════════════════════════════
// BACKFILL MISSING OUTPUTS
// ═══════════════════════════════════════════════════════════════════════
async function backfillAgent(agent, allUsers) {
  const mtName=agent.agentCode+AGT.MT; const qlName=agent.agentCode+AGT.QL;
  const [mtAll,qlAll]=await Promise.all([readSheet(mtName),readSheet(qlName)]);
  if(mtAll.length<2) return {missing:0,filled:0};
  const mtH=mtAll[0]; const mtData=mtAll.slice(1);
  const cidC=mtH.indexOf('Call ID'); const stC=mtH.indexOf('Status');
  const rf=resultFieldNames(agent.resultSchema); const cv=agent.customVariables||[];
  if(cidC<0||!rf.length) return {missing:0,filled:0};
  const qlH=qlAll.length>0?qlAll[0]:[]; const qlCidC=qlH.indexOf('Call ID');
  const qlByCallId={}; const qlExist=new Set();
  if(qlCidC>=0&&qlAll.length>1) qlAll.slice(1).forEach((r,idx)=>{const cid=String(r[qlCidC]||'').trim();if(cid){qlByCallId[cid]=idx+2;qlExist.add(cid);}});
  const uMap=userMap(allUsers); const mtUpdates=[]; const qlNewRows=[]; const qlUpdates=[]; let missing=0,filled=0;
  for(let i=0;i<mtData.length&&filled<300;i++){
    const row=mtData[i]; const callId=String(row[cidC]||'').trim(); const status=String(row[stC]||'').toUpperCase();
    if(!callId||status!=='COMPLETED') continue;
    const hasR=rf.some(f=>{const c=mtH.indexOf('out.'+f);return c>=0&&String(row[c]||'').trim()!=='';});
    if(hasR) continue;
    missing++;
    const r=await hunarGetCall(callId); if(!r.ok) continue;
    const d=r.data; const result=d.result||{}; const nr=pad([...row],mtH.length);
    set(nr,mtH,'Duration (Minutes)',d.duration_minutes??0); set(nr,mtH,'Duration (Seconds)',d.duration_seconds??0);
    set(nr,mtH,'Started At',d.started_at||''); set(nr,mtH,'Ended At',d.ended_at||''); set(nr,mtH,'Answered By',d.answered_by||'');
    set(nr,mtH,'Engagement Status',d.engagement_status||''); set(nr,mtH,'Call Ended By',d.call_ended_by||''); set(nr,mtH,'Recording URL',d.recording_url||''); set(nr,mtH,'Updated At',new Date().toISOString());
    cv.forEach(c=>{const src=d.custom_data&&d.custom_data[c];if(src!==undefined)set(nr,mtH,'in.'+c,src);});
    rf.forEach(f=>set(nr,mtH,'out.'+f,result[f]!==undefined?result[f]:''));
    mtUpdates.push({range:`'${mtName}'!A${i+2}`,values:nr}); filled++;
    const ridC=mtH.indexOf('Request ID'); const reqId=ridC>=0?String(row[ridC]||''):'';
    const mobC=mtH.indexOf('Mobile Number'); const mobile=mobC>=0?String(row[mobC]||''):'';
    if(qlH.length&&!qlExist.has(callId)&&isQualified(agent,result)){
      const qlRow=new Array(qlH.length).fill('');
      qlH.forEach((h,k)=>{const mi=mtH.indexOf(h);if(mi>=0)qlRow[k]=nr[mi];});
      const {assignEmail,recruiterName}=resolveAssignment(reqId,mobile,row,mtH,uMap);
      const ac=qlH.indexOf('Assigned To Email');const rc=qlH.indexOf('Recruiter');const dc=qlH.indexOf('Date Added');
      if(ac>=0&&assignEmail)qlRow[ac]=assignEmail;if(rc>=0&&recruiterName)qlRow[rc]=recruiterName;if(dc>=0)qlRow[dc]=new Date().toISOString();
      qlNewRows.push(qlRow); qlExist.add(callId);
    } else if(qlByCallId[callId]&&qlH.length){
      const qlRowIdx=qlByCallId[callId]; const existQL=qlAll[qlRowIdx-1]||[]; const newQL=pad([...existQL],qlH.length); let changed=false;
      rf.forEach(f=>{const col='out.'+f; const qlci=qlH.indexOf(col); if(qlci>=0&&!String(newQL[qlci]||'').trim()&&result[f]!==undefined){newQL[qlci]=result[f];changed=true;}});
      if(changed) qlUpdates.push({range:`'${qlName}'!A${qlRowIdx}`,values:newQL});
    }
    await sleep(200);
  }
  if(mtUpdates.length) await batchWrite(mtUpdates);
  if(qlNewRows.length) await appendRows(qlName,qlNewRows);
  if(qlUpdates.length) await batchWrite(qlUpdates);
  return {missing,filled};
}

// ═══════════════════════════════════════════════════════════════════════
// REPAIR UNASSIGNED LEADS
// ═══════════════════════════════════════════════════════════════════════
async function repairAgentLeads(agent, uMap, tMap) {
  const qlAll=await readSheet(agent.agentCode+AGT.QL); if(qlAll.length<2) return 0;
  const h=qlAll[0]; const assignC=h.indexOf('Assigned To Email'); const recrC=h.indexOf('Recruiter'); const ridC=h.indexOf('Request ID');
  if(assignC<0||ridC<0) return 0;
  const updates=[];
  qlAll.slice(1).forEach((r,i)=>{
    const assigned=String(r[assignC]||'').trim(); if(assigned) return;
    const reqId=String(r[ridC]||'').trim(); if(!reqId) return;
    const trigger=tMap[reqId]; if(!trigger) return;
    const u=uMap[trigger.email]; if(!u) return;
    const rowNum=i+2;
    updates.push({range:`'${agent.agentCode+AGT.QL}'!${col(assignC+1)}${rowNum}`,values:[trigger.email]});
    if(recrC>=0) updates.push({range:`'${agent.agentCode+AGT.QL}'!${col(recrC+1)}${rowNum}`,values:[u.name||trigger.email]});
  });
  if(updates.length){
    await _sheets.spreadsheets.values.batchUpdate({spreadsheetId:SPREADSHEET_ID,requestBody:{valueInputOption:'USER_ENTERED',data:updates.map(u=>({range:u.range,values:[[u.values[0]]]}))}});
  }
  return updates.length;
}

// ═══════════════════════════════════════════════════════════════════════
// CALLBACK QUEUE
// ═══════════════════════════════════════════════════════════════════════
async function fireCallbackGroup(agent, team, items, qH, allUsers) {
  const contacts=[]; const assignMap={};
  items.forEach(r=>{
    const mobile=String(r[qH.indexOf('Mobile Number')]||''); const callee=String(r[qH.indexOf('Callee Name')]||''); const ae=String(r[qH.indexOf('Assigned To Email')]||'').toLowerCase().trim();
    if(!mobile) return; contacts.push({callee_name:callee,mobile_number:mobile,custom_data:{}}); if(ae) assignMap[mobile]=ae;
  });
  if(!contacts.length) return null;
  const seen=new Set(); const unique=contacts.filter(c=>{if(seen.has(c.mobile_number))return false;seen.add(c.mobile_number);return true;});
  const today=todayIST(); const reqId=`CB_AUTO_${today.replace(/-/g,'')}_${agent.agentCode.slice(0,8)}_${Date.now()}`;
  const apiRes=await hunarBulkCall({agent_id:agent.agentId,request_id:reqId,data:unique,remove_invalid_rows:true,remove_duplicate_phone_numbers:true});
  if(!apiRes.ok){console.error(`[callbacks] Hunar failed:`,apiRes.error);return null;}
  const created=Array.isArray(apiRes.data)?apiRes.data:[];
  const trigBy=systemTrigger(team,allUsers);
  await seedMT(agent,created,reqId,trigBy);
  const estMin=(unique.length*agent.estSecondsPerCall)/60;
  await appendRows(agent.agentCode+AGT.CT,[[reqId,`AUTO_CB_${today}_${(team||'noteam').replace(/\s/g,'_').slice(0,12)}`,trigBy,new Date().toISOString(),unique.length,'IN_PROGRESS',0,0,0,0,0,0,Math.round(estMin*10)/10,new Date().toISOString()]]);
  await appendRows(S.TRIGGER_LOG,[[new Date().toISOString(),trigBy,'AUTO_CALLBACK',team,agent.agentCode,reqId,unique.length,estMin]]);
  console.log(`[callbacks] Fired ${agent.agentCode} team=${team} contacts=${unique.length}`);
  return reqId;
}

// ═══════════════════════════════════════════════════════════════════════
// RETRY QUEUE
// ═══════════════════════════════════════════════════════════════════════
async function fireRetryGroup(agent, team, origReqId, allUsers) {
  const [mtAll,qlAll]=await Promise.all([readSheet(agent.agentCode+AGT.MT),readSheet(agent.agentCode+AGT.QL)]);
  if(mtAll.length<2) return null;
  const mtH=mtAll[0]; const ridC=mtH.indexOf('Request ID'); const stC=mtH.indexOf('Status'); const calleeC=mtH.indexOf('Callee Name'); const mobC=mtH.indexOf('Mobile Number'); const cidC=mtH.indexOf('Call ID'); const trigC=mtH.indexOf('Triggered By');
  const qlCallMap={};
  if(qlAll.length>1){const qlH=qlAll[0];const qCid=qlH.indexOf('Call ID');const qAs=qlH.indexOf('Assigned To Email');if(qCid>=0&&qAs>=0)qlAll.slice(1).forEach(r=>{const c=String(r[qCid]||'').trim();const e=String(r[qAs]||'').toLowerCase().trim();if(c&&e)qlCallMap[c]=e;});}
  const contacts=[]; const assignMap={};
  mtAll.slice(1).forEach(r=>{
    if(String(r[ridC]||'')!==origReqId) return;
    const s=String(r[stC]||'').toUpperCase(); if(s!=='NOT_CONNECTED'&&s!=='FAILED') return;
    const mobile=String(r[mobC]||'').trim(); if(!mobile) return;
    const cid=cidC>=0?String(r[cidC]||'').trim():''; let ae=cid?(qlCallMap[cid]||''):'';
    if(!ae&&trigC>=0) ae=String(r[trigC]||'').toLowerCase().trim();
    contacts.push({callee_name:String(r[calleeC]||''),mobile_number:mobile,custom_data:{}}); if(ae) assignMap[mobile]=ae;
  });
  if(!contacts.length) return null;
  const seen=new Set(); const unique=contacts.filter(c=>{if(seen.has(c.mobile_number))return false;seen.add(c.mobile_number);return true;});
  const today=todayIST(); const reqId=`NC_AUTO_${today.replace(/-/g,'')}_${agent.agentCode.slice(0,8)}_${Date.now()}`;
  const apiRes=await hunarBulkCall({agent_id:agent.agentId,request_id:reqId,data:unique,remove_invalid_rows:true,remove_duplicate_phone_numbers:true});
  if(!apiRes.ok){console.error('[retries] Hunar failed:',apiRes.error);return null;}
  const created=Array.isArray(apiRes.data)?apiRes.data:[];
  const trigBy=systemTrigger(team,allUsers);
  await seedMT(agent,created,reqId,trigBy);
  const estMin=(unique.length*agent.estSecondsPerCall)/60;
  await appendRows(agent.agentCode+AGT.CT,[[reqId,`AUTO_RETRY_${today}_${(team||'noteam').replace(/\s/g,'_').slice(0,12)}`,trigBy,new Date().toISOString(),unique.length,'IN_PROGRESS',0,0,0,0,0,0,Math.round(estMin*10)/10,new Date().toISOString()]]);
  await appendRows(S.TRIGGER_LOG,[[new Date().toISOString(),trigBy,'AUTO_RETRY_NC',team,agent.agentCode,reqId,unique.length,estMin]]);
  // Mark NC rows TRIGGERED
  const ncAll=await readSheet(agent.agentCode+AGT.NC);
  if(ncAll.length>1){const ncH=ncAll[0];const nrc=ncH.indexOf('Request ID');const nts=ncH.indexOf('Trigger Status');const nrr=ncH.indexOf('Retry Request ID');const nup=ncH.indexOf('Last Updated');const ups=[];ncAll.slice(1).forEach((r,i)=>{if(String(r[nrc]||'')!==origReqId||String(r[nts]||'')==='TRIGGERED')return;const rn=i+2;if(nts>=0)ups.push({range:`'${agent.agentCode+AGT.NC}'!${col(nts+1)}${rn}`,values:['TRIGGERED']});if(nrr>=0)ups.push({range:`'${agent.agentCode+AGT.NC}'!${col(nrr+1)}${rn}`,values:[reqId]});if(nup>=0)ups.push({range:`'${agent.agentCode+AGT.NC}'!${col(nup+1)}${rn}`,values:[new Date().toISOString()]});});if(ups.length)await _sheets.spreadsheets.values.batchUpdate({spreadsheetId:SPREADSHEET_ID,requestBody:{valueInputOption:'USER_ENTERED',data:ups.map(u=>({range:u.range,values:[[u.values[0]]]}))}});}
  console.log(`[retries] Fired ${agent.agentCode} orig=${origReqId} new=${reqId} contacts=${unique.length}`);
  return reqId;
}

// ═══════════════════════════════════════════════════════════════════════
// ARCHIVE
// ═══════════════════════════════════════════════════════════════════════
function isLeadDone(cs,fb){ const c=String(cs||'').trim();const f=String(fb||'').trim();if(c==='Hiring On hold'||c==='Hiring On Hold')return true;if(c==='DNP-3'||c==='DNP-4'||c==='DNP-5')return true;if((c==='Connected'||c==='Irrelevant')&&f!=='')return true;return false;}

async function appendArchiveRow(exSsId, sheetName, srcH, srcRow, srcSheetName) {
  const archAll=await readSheet(sheetName,exSsId);
  const archH=archAll.length>0?archAll[0]:srcH.concat(['Archived At','Archived From Sheet']);
  const out=new Array(archH.length).fill('');
  archH.forEach((h,k)=>{const si=srcH.indexOf(h);if(si>=0)out[k]=srcRow[si];});
  const ac=archH.indexOf('Archived At'); const afc=archH.indexOf('Archived From Sheet');
  if(ac>=0)out[ac]=new Date().toISOString(); if(afc>=0)out[afc]=srcSheetName;
  await appendRows(sheetName,[out],exSsId);
}

// ═══════════════════════════════════════════════════════════════════════
// EXPORTED JOB FUNCTIONS (called by cron + force-refresh endpoints)
// ═══════════════════════════════════════════════════════════════════════

async function runPollActiveBatches({ agentCode } = {}) {
  const [agentRows,userRows]=await Promise.all([readSheet(S.AGENTS),readSheet(S.USERS)]);
  let agents=parseAgents(agentRows,true); const allUsers=parseUsers(userRows);
  if(agentCode) agents=agents.filter(a=>a.agentCode===agentCode);
  if(!agents.length) return {ok:true,message:'No active agents found'};
  let totF=0,totU=0,totE=0,totQ=0;
  for(const agent of agents){try{const r=await pollAgent(agent,allUsers);totF+=r.fetched;totU+=r.updated;totE+=r.errors;totQ+=r.qlAdded;if(r.fetched>0)console.log(`[poll] ${agent.agentCode}: fetched=${r.fetched} updated=${r.updated} errors=${r.errors} ql=${r.qlAdded}`);}catch(e){console.error(`[poll] Error on ${agent.agentCode}:`,e.message);}}
  return {ok:true,fetched:totF,updated:totU,errors:totE,qlAdded:totQ};
}

async function runBackfill({ agentCode } = {}) {
  const [agentRows,userRows]=await Promise.all([readSheet(S.AGENTS),readSheet(S.USERS)]);
  let agents=parseAgents(agentRows,true); const allUsers=parseUsers(userRows);
  if(agentCode) agents=agents.filter(a=>a.agentCode===agentCode);
  let total=0;
  for(const a of agents){try{const r=await backfillAgent(a,allUsers);total+=r.filled;if(r.missing>0)console.log(`[backfill] ${a.agentCode}: missing=${r.missing} filled=${r.filled}`);}catch(e){console.error(`[backfill] Error on ${a.agentCode}:`,e.message);}}
  return {ok:true,totalFilled:total};
}

async function runRepairLeads() {
  const [agentRows,userRows,tlRows]=await Promise.all([readSheet(S.AGENTS),readSheet(S.USERS),readSheet(S.TRIGGER_LOG)]);
  const agents=parseAgents(agentRows,true); const allUsers=parseUsers(userRows);
  const uMap=userMap(allUsers); const tMap=triggerMap(tlRows);
  let total=0;
  for(const a of agents){try{const f=await repairAgentLeads(a,uMap,tMap);total+=f;if(f>0)console.log(`[repair] ${a.agentCode}: fixed ${f}`);}catch(e){console.error(`[repair] Error on ${a.agentCode}:`,e.message);}}
  return {ok:true,totalFixed:total};
}

async function runCleanupSessions() {
  const all=await readSheet(S.SESSIONS); if(all.length<2) return {ok:true,removed:0};
  const now=Date.now(); const keep=[all[0]]; let removed=0;
  all.slice(1).forEach(r=>{if(new Date(r[3]).getTime()>now)keep.push(r);else removed++;});
  if(removed>0){
    await _sheets.spreadsheets.values.clear({spreadsheetId:SPREADSHEET_ID,range:`'${S.SESSIONS}'!A2:Z`});
    if(keep.length>1)await _sheets.spreadsheets.values.update({spreadsheetId:SPREADSHEET_ID,range:`'${S.SESSIONS}'!A2`,valueInputOption:'USER_ENTERED',requestBody:{values:keep.slice(1)}});
    console.log(`[sessions] Removed ${removed} expired.`);
  }
  return {ok:true,removed};
}

async function runCallbackQueue() {
  const cqAll=await readSheet(S.CB_QUEUE); if(cqAll.length<2) return {ok:true,groups:0};
  const qH=cqAll[0]; const today=todayIST();
  const [agentRows,userRows]=await Promise.all([readSheet(S.AGENTS),readSheet(S.USERS)]);
  const aMap={}; parseAgents(agentRows,true).forEach(a=>{aMap[a.agentCode]=a;}); const allUsers=parseUsers(userRows);
  const groups={};
  cqAll.slice(1).forEach((r,i)=>{
    const status=String(r[qH.indexOf('Status')]||''); const sched=String(r[qH.indexOf('Scheduled Date')]||'');
    if(status!=='PENDING'||sched>today) return;
    const ac=String(r[qH.indexOf('Agent Code')]||''); const team=String(r[qH.indexOf('Team')]||'');
    const key=ac+'|||'+team; if(!groups[key]) groups[key]={agentCode:ac,team,items:[],rowNums:[]};
    groups[key].items.push(r); groups[key].rowNums.push(i+2);
  });
  const updates=[];
  for(const key of Object.keys(groups)){
    const g=groups[key]; const agent=aMap[g.agentCode];
    const newReqId=agent?await fireCallbackGroup(agent,g.team,g.items,qH,allUsers):null;
    const s=newReqId?'TRIGGERED':'SKIPPED';
    g.rowNums.forEach(rn=>{
      updates.push({range:`'${S.CB_QUEUE}'!${col(qH.indexOf('Status')+1)}${rn}`,values:[[s]]});
      if(newReqId)updates.push({range:`'${S.CB_QUEUE}'!${col(qH.indexOf('New Request ID')+1)}${rn}`,values:[[newReqId]]});
      if(s==='TRIGGERED')updates.push({range:`'${S.CB_QUEUE}'!${col(qH.indexOf('Triggered At')+1)}${rn}`,values:[[new Date().toISOString()]]});
    });
  }
  if(updates.length)await _sheets.spreadsheets.values.batchUpdate({spreadsheetId:SPREADSHEET_ID,requestBody:{valueInputOption:'USER_ENTERED',data:updates}});
  return {ok:true,groups:Object.keys(groups).length};
}

async function runRetryQueue() {
  const rqAll=await readSheet(S.RETRY_QUEUE); if(rqAll.length<2) return {ok:true,processed:0};
  const rqH=rqAll[0]; const today=todayIST();
  const [agentRows,userRows]=await Promise.all([readSheet(S.AGENTS),readSheet(S.USERS)]);
  const aMap={}; parseAgents(agentRows,true).forEach(a=>{aMap[a.agentCode]=a;}); const allUsers=parseUsers(userRows);
  const stC=rqH.indexOf('Status'); const raC=rqH.indexOf('Retry After Date'); const acC=rqH.indexOf('Agent Code'); const orC=rqH.indexOf('Original Request ID'); const tmC=rqH.indexOf('Team'); const nrC=rqH.indexOf('New Request ID'); const taC=rqH.indexOf('Triggered At');
  const updates=[]; let processed=0;
  for(let i=0;i<rqAll.slice(1).length;i++){
    const r=rqAll.slice(1)[i]; if(String(r[stC]||'')!=='PENDING'||String(r[raC]||'')>today) continue;
    const ac=String(r[acC]||''); const origReqId=String(r[orC]||''); const team=String(r[tmC]||''); const agent=aMap[ac]; const rn=i+2;
    if(!agent){updates.push({range:`'${S.RETRY_QUEUE}'!${col(stC+1)}${rn}`,values:[['SKIPPED']]});continue;}
    const newReqId=await fireRetryGroup(agent,team,origReqId,allUsers); processed++;
    updates.push({range:`'${S.RETRY_QUEUE}'!${col(stC+1)}${rn}`,values:[[newReqId?'TRIGGERED':'SKIPPED']]});
    if(newReqId&&nrC>=0)updates.push({range:`'${S.RETRY_QUEUE}'!${col(nrC+1)}${rn}`,values:[[newReqId]]});
    if(taC>=0)updates.push({range:`'${S.RETRY_QUEUE}'!${col(taC+1)}${rn}`,values:[[new Date().toISOString()]]});
  }
  if(updates.length)await _sheets.spreadsheets.values.batchUpdate({spreadsheetId:SPREADSHEET_ID,requestBody:{valueInputOption:'USER_ENTERED',data:updates}});
  return {ok:true,processed};
}

async function runArchiveLeads() {
  const [agentRows,userRows,teamRows]=await Promise.all([readSheet(S.AGENTS),readSheet(S.USERS),readSheet(S.TEAMS)]);
  const agents=parseAgents(agentRows); const allUsers=parseUsers(userRows); const teams=parseTeams(teamRows); const atm=agentTeamMap(agents,allUsers);
  let total=0;
  for(const agent of agents){
    const teamName=atm[agent.agentCode]; if(!teamName) continue;
    const team=teams.find(t=>t.name===teamName); if(!team||!team.spreadsheetId) continue;
    const exSsId=team.spreadsheetId;
    const qlAll=await readSheet(agent.agentCode+AGT.QL); if(qlAll.length<2) continue;
    const h=qlAll[0]; const csC=h.indexOf('Call Status'); const fbC=h.indexOf('Feedback'); if(csC<0||fbC<0) continue;
    await ensureExternalSheet(exSsId,ARCH.LEADS,h.concat(['Archived At','Archived From Sheet']));
    const toArchive=[]; const toDelete=[];
    qlAll.slice(1).forEach((r,i)=>{if(isLeadDone(r[csC],r[fbC])){toArchive.push(r);toDelete.push(i+2);}});
    for(const r of toArchive)await appendArchiveRow(exSsId,ARCH.LEADS,h,r,agent.agentCode+AGT.QL);
    if(toDelete.length)await deleteRows(agent.agentCode+AGT.QL,toDelete);
    total+=toArchive.length; if(toArchive.length)console.log(`[archive:leads] ${agent.agentCode}→${teamName}: ${toArchive.length}`);
  }
  return {ok:true,totalArchived:total};
}

async function runArchiveMT() {
  const [agentRows,userRows,teamRows]=await Promise.all([readSheet(S.AGENTS),readSheet(S.USERS),readSheet(S.TEAMS)]);
  const agents=parseAgents(agentRows); const allUsers=parseUsers(userRows); const teams=parseTeams(teamRows); const atm=agentTeamMap(agents,allUsers);
  let total=0;
  for(const agent of agents){
    const teamName=atm[agent.agentCode]; if(!teamName) continue;
    const team=teams.find(t=>t.name===teamName); if(!team||!team.spreadsheetId) continue;
    const exSsId=team.spreadsheetId;
    const archivedIds=new Set();
    try{const ar=await readSheet(ARCH.LEADS,exSsId);if(ar.length>1){const c=ar[0].indexOf('Call ID');if(c>=0)ar.slice(1).forEach(r=>{if(r[c])archivedIds.add(String(r[c]).trim());});}}catch(_){}
    if(!archivedIds.size) continue;
    const alreadyMT=new Set();
    try{const ar=await readSheet(ARCH.MT,exSsId);if(ar.length>1){const c=ar[0].indexOf('Call ID');if(c>=0)ar.slice(1).forEach(r=>{if(r[c])alreadyMT.add(String(r[c]).trim());});}}catch(_){}
    const mtAll=await readSheet(agent.agentCode+AGT.MT); if(mtAll.length<2) continue;
    const mtH=mtAll[0]; const mcC=mtH.indexOf('Call ID'); if(mcC<0) continue;
    await ensureExternalSheet(exSsId,ARCH.MT,mtH.concat(['Archived At','Archived From Sheet']));
    const toArchive=[]; const toDelete=[];
    mtAll.slice(1).forEach((r,i)=>{const cid=String(r[mcC]||'').trim();if(!cid) return;if(archivedIds.has(cid)&&!alreadyMT.has(cid)){toArchive.push(r);toDelete.push(i+2);}});
    for(const r of toArchive)await appendArchiveRow(exSsId,ARCH.MT,mtH,r,agent.agentCode+AGT.MT);
    if(toDelete.length)await deleteRows(agent.agentCode+AGT.MT,toDelete);
    total+=toArchive.length; if(toArchive.length)console.log(`[archive:mt] ${agent.agentCode}→${teamName}: ${toArchive.length}`);
  }
  return {ok:true,totalArchived:total};
}

async function runArchiveManual() {
  const [teamRows,mtAll]=await Promise.all([readSheet(S.TEAMS),readSheet(S.MANUAL)]);
  const teams=parseTeams(teamRows); if(mtAll.length<2) return {ok:true,total:0};
  const h=mtAll[0]; const csC=h.indexOf('Call Status'); const tmC=h.indexOf('Team'); if(csC<0) return {ok:true,total:0};
  const DONE=new Set(['Connected','DNP-3','DNP-4','DNP-5','Hiring On Hold','Hiring On hold','Irrelevant','Not Interested']);
  const byTeam={}; const toDelete=[];
  mtAll.slice(1).forEach((r,i)=>{const cs=String(r[csC]||'').trim();const team=tmC>=0?String(r[tmC]||'').trim():'';if(!DONE.has(cs)||!team)return;if(!byTeam[team])byTeam[team]=[];byTeam[team].push({row:r,rowNum:i+2});});
  let total=0;
  for(const [teamName,items] of Object.entries(byTeam)){
    const team=teams.find(t=>t.name===teamName);if(!team||!team.spreadsheetId)continue;
    await ensureExternalSheet(team.spreadsheetId,ARCH.MANUAL,h.concat(['Archived At','Archived From Sheet']));
    for(const item of items){await appendArchiveRow(team.spreadsheetId,ARCH.MANUAL,h,item.row,S.MANUAL);toDelete.push(item.rowNum);}
    total+=items.length;
  }
  if(toDelete.length)await deleteRows(S.MANUAL,toDelete);
  return {ok:true,totalArchived:total};
}

// ═══════════════════════════════════════════════════════════════════════
// CRON SCHEDULER + START
// ═══════════════════════════════════════════════════════════════════════
const IST = { timezone: DEFAULT_TIMEZONE };
let _pollRunning = false;

async function safe(name, fn, args={}) {
  try { const r=await fn(args); logStatus(name,r); return r; }
  catch(e){ console.error(`[${name}] Error:`,e.message); STATUS.errors.push({job:name,error:e.message,time:new Date().toISOString()}); return {ok:false,error:e.message}; }
}

async function scheduledPoll() {
  if(_pollRunning){console.log('[poll] Skipping — still running.');return;}
  _pollRunning=true; STATUS.pollRunning=true;
  try{ await safe('poll',runPollActiveBatches); }
  finally{_pollRunning=false;STATUS.pollRunning=false;}
}

function startPoller() {
  if (!process.env.GOOGLE_SERVICE_ACCOUNT_B64) {
    console.warn('[poller] GOOGLE_SERVICE_ACCOUNT_B64 not set — disabled.');
    return;
  }

  getSheets().then(() => {
    console.log('[poller] Google Sheets connected ✓');

    // Boot run after 15s
    setTimeout(() => safe('poll', runPollActiveBatches), 15000);

    cron.schedule('* * * * *',     scheduledPoll);                                     // every 1 min
    cron.schedule('*/5 * * * *',   () => safe('backfill',  runBackfill),        IST);  // every 5 min
    cron.schedule('*/10 * * * *',  () => safe('repair',    runRepairLeads),      IST);  // every 10 min
    cron.schedule('0 * * * *',     () => safe('sessions',  runCleanupSessions),  IST);  // every hour
    cron.schedule('0 11 * * *',    () => safe('callbacks', runCallbackQueue),    IST);  // 11am IST
    cron.schedule('0 11 * * *',    () => safe('retries',   runRetryQueue),       IST);  // 11am IST
    cron.schedule('0 2 * * *',     () => safe('archLeads', runArchiveLeads),     IST);  // 2am IST
    cron.schedule('0 3 * * *',     () => safe('archMT',    runArchiveMT),        IST);  // 3am IST
    cron.schedule('0 4 * * *',     () => safe('archManual',runArchiveManual),    IST);  // 4am IST

    console.log('[poller] All 9 jobs scheduled ✓');
  }).catch(e => console.error('[poller] Failed to connect:', e.message));
}

// ═══════════════════════════════════════════════════════════════════════
// EXPORTS — used by server.js for force-refresh endpoints
// ═══════════════════════════════════════════════════════════════════════
module.exports = {
  startPoller,
  // Force-run any job on demand (called from /poller/* API routes)
  jobs: {
    poll:        runPollActiveBatches,  // { agentCode? }
    backfill:    runBackfill,           // { agentCode? }
    repair:      runRepairLeads,
    sessions:    runCleanupSessions,
    callbacks:   runCallbackQueue,
    retries:     runRetryQueue,
    archLeads:   runArchiveLeads,
    archMT:      runArchiveMT,
    archManual:  runArchiveManual,
  },
  getStatus: () => STATUS,
};
