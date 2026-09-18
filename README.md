# state-vamc-sync

Cloud Run service: nightly state + vamc_presumed backfill for subscribed Early
Alert contacts (ITDO-420/421/424). Cloud Run port of the four manual scripts
(`itdo424_backfill_pull.sql`, `resolve_state_vamc_backfill.ps1`,
`itdo424_backfill_bq.sql`, `state_vamc_writeback_pull.sql` +
`textit_writeback_state_vamc.ps1`) into one in-process pipeline — no CSV
round-trips.

## What it does (`POST /sync`)

1. **PULL** — subscribed contacts with a valid-zip proxy missing `state` and/or
   `vamc_presumed`. VAMC gate: `veteran=yes OR va_baa_affiliated=yes OR orgcode
   LIKE 'va-%'`. (pull SQL lifted verbatim from itdo424_backfill_pull.sql.)
2. **RESOLVE** — one zip-lookup call per DISTINCT zip; build zip→(state, vamc)
   map. Skip empties/failures — no blank writes (stricter than the live flow).
3. **STAGE** — load per-contact resolved set into
   `OPS.state_vamc_stage` (all STRING, WRITE_TRUNCATE).
4. **LOG + MERGE** — log changed cells to `OPS.state_vamc_log`
   (own table, NOT contacts_sync_diff — this is a backfill, not the sync), then
   MERGE non-empty values into `users`. (itdo424_backfill_bq.sql, verbatim.)
5. **WRITEBACK** — push resolved values to TextIt for contacts STILL present in
   `OPS.contacts_sync_textit_staging` (bq_only contacts 400 on TextIt write). Throttled
   ~4 req/sec. Pass `{"do_textit": false}` to skip writeback (BQ-only run).
   Pass `{"writeback_limit": 1}` to cap the TextIt writeback to N contacts
   (Rule-23 single-record-before-bulk) — use 1 for the first real TextIt run,
   omit for unbounded.

`/health` (GET) → `{"status":"ok"}`. Both endpoints require GCP auth; `/sync`
additionally checks a body password.

## Locked logic

- `vamc_presumed` is Sta# — zip-lookup returns Sta# post-ITDO-372. The service
  passes through whatever zip-lookup returns.
- Valid zip = zip-lookup returns non-empty state. Writes BOTH BQ and TextIt.
- Per-field gating: set a value only if the contact needs that field AND the
  lookup returned a non-empty value for it.

## Orchestrator ordering

Runs AFTER contacts-sync (so the TextIt-wins sync can't clobber these writes)
and BEFORE vamc-sync (which derives `vamc_display_name` from `vamc_presumed`).
contacts-sync freshly rewrites `OPS.contacts_sync_textit_staging` each run, so the WRITEBACK
EXISTS-filter is current. `run_backfill()` is the callable core for the unified
nightly orchestrator (backup → contacts-sync → state/vamc → vamc-sync).

## Config — Cloud Run console (NOT repo)

- `TEXTIT_TOKEN` — TextIt API token (writeback).
- `SYNC_PASSWORD` — POST-body auth.
- `ZIP_LOOKUP_URL` — default the live zip-lookup service URL.
- `ZIP_LOOKUP_TOKEN` — only if zip-lookup enforces token auth.
- `GCP_PROJECT` — defaults early-alert-responses.
- `WRITEBACK_THROTTLE_SEC` (default 0.25), `ZIP_THROTTLE_SEC` (default 0.05).

## Service account / IAM

Runtime SA (compute SA `853176470965-compute@developer.gserviceaccount.com`)
needs **BigQuery Data Editor + BigQuery Job User** on early-alert-responses.
It calls zip-lookup (Cloud Run) — if zip-lookup requires auth, the SA needs
`roles/run.invoker` on it.

## Tables

- `RESPONSES.users` — backfill target (state, vamc_presumed).
- `OPS.state_vamc_stage` — resolved set (WRITE_TRUNCATE each run).
- `OPS.state_vamc_log` — per-cell change log (audit/rollback).
- `OPS.contacts_sync_textit_staging` — read for the writeback EXISTS-filter
  (written by contacts-sync).
