# Voxa Portal — Node Poller Setup

## Folder Structure

```
voxa-portal/
├── public/
│   └── index.html          ← YOUR EXISTING FRONTEND (unchanged)
├── poller/
│   ├── index.js            ← starts all cron jobs
│   ├── constants.js        ← all constants (mirrors GAS)
│   ├── sheets.js           ← Google Sheets API helpers
│   ├── hunar.js            ← Hunar API helpers
│   ├── agents.js           ← agent/user parsing + qualification logic
│   ├── poll.js             ← pollActiveBatches (every 1 min)
│   ├── backfill.js         ← backfillMissingOutputs (every 5 min)
│   ├── callbacks.js        ← processCallbackQueue (daily 11am IST)
│   ├── retries.js          ← processRetryQueue (daily 11am IST)
│   ├── lineup.js           ← addToInterviewLineup
│   ├── repair.js           ← repairUnassignedLeads (every 10 min)
│   ├── archive.js          ← archive jobs (daily 2/3/4am IST)
│   └── sessions.js         ← cleanupExpiredSessions (hourly)
├── server.js               ← Express server + starts poller
└── package.json
```

## What moved from GAS to Node

| GAS Trigger                  | Node Module       | Schedule         |
|------------------------------|-------------------|------------------|
| _scheduledPollActiveBatches  | poll.js           | Every 1 min      |
| backfillMissingOutputs       | backfill.js       | Every 5 min      |
| repairUnassignedLeads        | repair.js         | Every 10 min     |
| cleanupExpiredSessions       | sessions.js       | Every hour       |
| processCallbackQueue         | callbacks.js      | Daily 11am IST   |
| processRetryQueue            | retries.js        | Daily 11am IST   |
| archiveCompletedLeads        | archive.js        | Daily 2am IST    |
| archiveCompletedMT           | archive.js        | Daily 3am IST    |
| archiveManualTracker         | archive.js        | Daily 4am IST    |

## GAS triggers to DELETE (after deploying this)

Go to GAS → Triggers and delete:
- _scheduledPollActiveBatches
- _scheduledBackfill
- backfillMissingOutputs
- repairUnassignedLeads
- cleanupExpiredSessions
- processCallbackQueue
- processRetryQueue
- archiveCompletedLeads
- archiveCompletedMT
- archiveManualTracker
- syncPendingAgentHeaders

Keep in GAS (user-facing actions, fast + cheap):
- doPost (all user actions: login, getLeads, updateLead, etc.)

## Environment Variables (set on Render)

| Variable                    | Value                                              |
|-----------------------------|----------------------------------------------------|
| GAS_URL                     | Your GAS web app URL (existing)                    |
| GOOGLE_SERVICE_ACCOUNT_B64  | Base64 of your service account JSON (see below)   |
| SPREADSHEET_ID              | 1C6-YtK0x2Q5MLAFihh9_vzZ-oC_e0qZRwrySZraz_Ow    |
| HUNAR_API_KEY               | Your Hunar API key (optional, hardcoded as fallback)|

### How to get GOOGLE_SERVICE_ACCOUNT_B64

**Mac/Linux:**
```bash
base64 -i service-account.json | tr -d '\n'
```

**Windows PowerShell:**
```powershell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("service-account.json"))
```

Copy the output and paste it as the env var value on Render.

## Archive feature note

The archive jobs write completed leads to each TEAM's external spreadsheet.
For this to work, share each team's external spreadsheet with the service account:
`voxabfsi@evocative-tower-496309-h1.iam.gserviceaccount.com`
as **Editor**.

## Frontend

Your `public/index.html` is completely unchanged.
The frontend still talks to GAS via `/api` for all user actions.
Only the background polling moves to Node.
