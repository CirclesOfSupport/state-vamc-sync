import os
import re
import time
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, request, jsonify
from google.cloud import bigquery

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# state-vamc-sync  (ITDO-420/421/424)
#
# Cloud Run port of the four manual backfill scripts. Runs the whole pipeline
# in-process — no CSV round-trips:
#   1. PULL    : subscribed contacts missing state and/or vamc_presumed,
#                with a valid-zip proxy. (itdo424_backfill_pull.sql, verbatim)
#   2. RESOLVE : one zip-lookup call per DISTINCT zip; build zip->(state,vamc)
#                map; skip empties/failures (no blank writes).
#                (resolve_state_vamc_backfill.ps1)
#   3. STAGE   : load the per-contact resolved set into
#                OPS.state_vamc_stage (all STRING, WRITE_TRUNCATE).
#   4. LOG+MERGE: log changed cells to state_vamc_log (NOT
#                contacts_sync_diff — this is a backfill, not the sync), then MERGE
#                non-empty values into users. (itdo424_backfill_bq.sql, verbatim)
#   5. WRITEBACK: push resolved values to TextIt for contacts STILL PRESENT in
#                contacts_sync_textit_staging (bq_only contacts 400 on TextIt write).
#                Throttled ~4 req/sec. (state_vamc_writeback_pull.sql +
#                textit_writeback_state_vamc.ps1)
#
# Locked logic (per handoff): valid zip = zip-lookup returns non-empty state.
# VAMC gate = veteran=yes OR va_baa_affiliated=yes OR orgcode LIKE 'va-%'.
# vamc_presumed is Sta# (zip-lookup returns Sta# post-ITDO-372). Writes BOTH BQ
# and TextIt. Own log table.
#
# Ordering in the nightly orchestrator: runs AFTER contacts-sync (so the sync
# can't clobber these writes) and BEFORE vamc-sync (which derives display names
# from vamc_presumed). contacts-sync freshly rewrites its staging table each
# run, so the WRITEBACK EXISTS-filter is current.
# ---------------------------------------------------------------------------

BQ_PROJECT = os.environ.get("GCP_PROJECT", "early-alert-responses")
BQ_DATASET = "RESPONSES"          # core warehouse: the users table only
OPS_DATASET = "OPS"               # operational objects for the nightly pipeline
STAGE_TABLE = f"{BQ_PROJECT}.{OPS_DATASET}.state_vamc_stage"
LOG_TABLE = f"{BQ_PROJECT}.{OPS_DATASET}.state_vamc_log"
USERS_TABLE = f"{BQ_PROJECT}.{BQ_DATASET}.users"
TEXTIT_FULL_TABLE = f"{BQ_PROJECT}.{OPS_DATASET}.contacts_sync_textit_staging"

ZIP_LOOKUP_URL = os.environ.get("ZIP_LOOKUP_URL", "https://zip-lookup-853176470965.us-east1.run.app/")
ZIP_LOOKUP_TOKEN = os.environ.get("ZIP_LOOKUP_TOKEN", "")  # only if zip-lookup enforces it

TEXTIT_TOKEN = os.environ.get("TEXTIT_TOKEN", "")
TEXTIT_CONTACTS_URL = "https://textit.com/api/v2/contacts.json"

SYNC_PASSWORD = os.environ.get("SYNC_PASSWORD", "")

WRITEBACK_THROTTLE_SEC = float(os.environ.get("WRITEBACK_THROTTLE_SEC", "1.44"))  # >=1.44s => <=2500/hr (ITDO-454; was 0.25 = ~4x over budget)
ZIP_THROTTLE_SEC = float(os.environ.get("ZIP_THROTTLE_SEC", "0.05"))

# itdo424_backfill_pull.sql — lifted verbatim. The population to resolve.
PULL_SQL = r"""
WITH users_dedup AS (
  SELECT * EXCEPT(rn) FROM (
    SELECT u.*, ROW_NUMBER() OVER (PARTITION BY uuid ORDER BY uuid) AS rn
    FROM `early-alert-responses.RESPONSES.users` u WHERE uuid IS NOT NULL
  ) WHERE rn = 1
),
prepped AS (
  SELECT
    uuid,
    zipcode AS zip_raw,
    REGEXP_EXTRACT(
      REGEXP_REPLACE(SPLIT(SPLIT(LTRIM(NULLIF(TRIM(zipcode),''), "'"), '-')[OFFSET(0)], '.')[OFFSET(0)], r'[^0-9]', ''),
      r'^(\d{3,})'
    ) AS zip_digits,
    NULLIF(TRIM(state), '') AS state_val,
    NULLIF(TRIM(vamc_presumed), '') AS vamc_val,
    LOWER(NULLIF(TRIM(subscribed), '')) AS sub,
    (LOWER(NULLIF(TRIM(veteran),'')) = 'yes'
      OR LOWER(NULLIF(TRIM(va_baa_affiliated),'')) = 'yes'
      OR LOWER(NULLIF(TRIM(orgCode),'')) LIKE 'va-%') AS vamc_eligible
  FROM users_dedup
)
SELECT
  uuid,
  zip_raw,
  zip_digits,
  (state_val IS NULL) AS needs_state,
  (vamc_eligible AND vamc_val IS NULL) AS needs_vamc,
  vamc_eligible
FROM prepped
WHERE sub = 'yes'
  AND zip_digits IS NOT NULL
  AND ( state_val IS NULL OR (vamc_eligible AND vamc_val IS NULL) )
ORDER BY uuid
"""


def get_bq_client():
    return bigquery.Client(project=BQ_PROJECT)


# ---------------------------------------------------------------------------
# 1. PULL
# ---------------------------------------------------------------------------

def pull_population(client):
    rows = list(client.query(PULL_SQL).result())
    logger.info(f"PULL: {len(rows)} affected contacts")
    return rows


# ---------------------------------------------------------------------------
# 2. RESOLVE — one zip-lookup call per distinct zip
# ---------------------------------------------------------------------------

def resolve_zips(rows):
    distinct = sorted({r["zip_digits"] for r in rows if r["zip_digits"]})
    logger.info(f"RESOLVE: {len(distinct)} distinct zips")
    headers = {"Content-Type": "application/json"}
    if ZIP_LOOKUP_TOKEN:
        headers["token"] = ZIP_LOOKUP_TOKEN

    zip_map = {}
    for i, zip_ in enumerate(distinct, 1):
        try:
            resp = requests.post(ZIP_LOOKUP_URL, json={"zipcode": zip_},
                                 headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            zip_map[zip_] = {
                "state": (data.get("state") or "").strip(),
                "vamc": (data.get("vamc_presumed") or "").strip(),
            }
        except Exception as e:
            zip_map[zip_] = {"state": "", "vamc": ""}
            logger.warning(f"  zip {zip_} lookup failed: {e}")
        if i % 50 == 0:
            logger.info(f"  resolved {i}/{len(distinct)}")
        if ZIP_THROTTLE_SEC:
            time.sleep(ZIP_THROTTLE_SEC)
    return zip_map


def build_resolved(rows, zip_map):
    """Per-contact resolved values. set_state only if needs_state and lookup
    returned non-empty state; set_vamc only if needs_vamc and non-empty vamc.
    (Mirrors resolve_state_vamc_backfill.ps1 exactly.)"""
    out = []
    for r in rows:
        m = zip_map.get(r["zip_digits"])
        set_state = ""
        set_vamc = ""
        if m:
            if r["needs_state"] and m["state"] != "":
                set_state = m["state"]
            if r["needs_vamc"] and m["vamc"] != "":
                set_vamc = m["vamc"]
        out.append({
            "uuid": r["uuid"],
            "zip_digits": r["zip_digits"] or "",
            "set_state": set_state,
            "set_vamc": set_vamc,
        })
    return out


# ---------------------------------------------------------------------------
# 3. STAGE — load resolved set into state_vamc_stage (WRITE_TRUNCATE)
# ---------------------------------------------------------------------------

def stage_resolved(client, resolved):
    schema = [
        bigquery.SchemaField("uuid", "STRING"),
        bigquery.SchemaField("zip_digits", "STRING"),
        bigquery.SchemaField("set_state", "STRING"),
        bigquery.SchemaField("set_vamc", "STRING"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_json(resolved, STAGE_TABLE, job_config=job_config)
    job.result()
    logger.info(f"STAGE: {len(resolved)} rows -> {STAGE_TABLE} (WRITE_TRUNCATE)")


# ---------------------------------------------------------------------------
# 4. LOG + MERGE — itdo424_backfill_bq.sql, verbatim (STEP1 log, STEP2 merge)
# ---------------------------------------------------------------------------

def apply_backfill(client):
    run_id = "svbackfill_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    client.query(f"""
        CREATE TABLE IF NOT EXISTS `{LOG_TABLE}` (
          run_id STRING, uuid STRING, field STRING, old_value STRING, new_value STRING, logged_at TIMESTAMP
        )
    """).result()

    # STEP 1 — log changed cells (non-empty staged value that differs from current)
    log_job = client.query(f"""
        INSERT INTO `{LOG_TABLE}`
          (run_id, uuid, field, old_value, new_value, logged_at)
        WITH stg AS (SELECT * FROM `{STAGE_TABLE}`)
        SELECT @run_id, uuid, field, old_value, new_value, CURRENT_TIMESTAMP() FROM (
          SELECT s.uuid, 'state' AS field,
            NULLIF(TRIM(u.state),'') AS old_value, NULLIF(TRIM(s.set_state),'') AS new_value
          FROM `{USERS_TABLE}` u JOIN stg s ON s.uuid=u.uuid
          WHERE u.uuid IS NOT NULL AND NULLIF(TRIM(s.set_state),'') IS NOT NULL
            AND NULLIF(TRIM(u.state),'') IS DISTINCT FROM NULLIF(TRIM(s.set_state),'')
          UNION ALL
          SELECT s.uuid, 'vamc_presumed' AS field,
            NULLIF(TRIM(u.vamc_presumed),'') AS old_value, NULLIF(TRIM(s.set_vamc),'') AS new_value
          FROM `{USERS_TABLE}` u JOIN stg s ON s.uuid=u.uuid
          WHERE u.uuid IS NOT NULL AND NULLIF(TRIM(s.set_vamc),'') IS NOT NULL
            AND NULLIF(TRIM(u.vamc_presumed),'') IS DISTINCT FROM NULLIF(TRIM(s.set_vamc),'')
        )
    """, job_config=bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("run_id", "STRING", run_id)
    ]))
    log_job.result()
    logged = log_job.num_dml_affected_rows or 0

    # STEP 2 — apply. Only writes non-empty staged values; leaves field untouched otherwise.
    merge_job = client.query(f"""
        MERGE `{USERS_TABLE}` U
        USING `{STAGE_TABLE}` s
        ON U.uuid = s.uuid
        WHEN MATCHED THEN UPDATE SET
          state = CASE WHEN NULLIF(TRIM(s.set_state),'') IS NOT NULL THEN TRIM(s.set_state) ELSE U.state END,
          vamc_presumed = CASE WHEN NULLIF(TRIM(s.set_vamc),'') IS NOT NULL THEN TRIM(s.set_vamc) ELSE U.vamc_presumed END
    """)
    merge_job.result()
    merged = merge_job.num_dml_affected_rows or 0

    logger.info(f"BACKFILL: run_id={run_id} logged={logged} merged_rows={merged}")
    return {"run_id": run_id, "cells_logged": logged, "rows_merged": merged}


# ---------------------------------------------------------------------------
# 5. WRITEBACK — TextIt, filtered to contacts still present in the staging table
# ---------------------------------------------------------------------------

def get_writeback_population(client, limit=None):
    """state_vamc_writeback_pull.sql — resolved set filtered to contacts still in
    TextIt (EXISTS in contacts_sync_textit_staging), with something to write.
    `limit` caps the population (Rule-23 single-record-before-bulk): pass 1 for
    the first real TextIt run, then None (unbounded) after verifying that contact."""
    limit_clause = f"LIMIT {int(limit)}" if limit is not None else ""
    rows = list(client.query(f"""
        SELECT s.uuid, s.set_state, s.set_vamc
        FROM `{STAGE_TABLE}` s
        WHERE EXISTS (
          SELECT 1 FROM `{TEXTIT_FULL_TABLE}` t WHERE t.uuid = s.uuid
        )
        AND ( NULLIF(TRIM(s.set_state),'') IS NOT NULL OR NULLIF(TRIM(s.set_vamc),'') IS NOT NULL )
        ORDER BY s.uuid
        {limit_clause}
    """).result())
    return rows


def _textit_post_writeback(url, payload, headers):
    """POST a single contact field write, with the TextIt 2,500-req/hr rate
    limit handled per ITDO-454.

    A 429 is NOT a per-contact failure — it means "wait N seconds and try the
    same write again." Parse the 'available in N seconds' body, sleep N+3, and
    retry the SAME request until it succeeds. This HOLDS the writeback (and the
    nightly chain) until the throttle clears, by design: downstream vamc-sync
    derives display names from vamc_presumed, so a throttled write must land
    tonight, not be deferred — dropping it would silently under-process.

    Any OTHER error (400/404/500, network) IS a real per-contact failure and is
    raised to the caller, which records it in the errors list and moves on — the
    prior behavior, preserved. Only 429 loops."""
    attempt = 0
    while True:
        attempt += 1
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
        if resp.status_code == 429:
            wait = 60
            m = re.search(r"available in (\d+)", resp.text)
            if m:
                wait = int(m.group(1)) + 3
            logger.warning(f"  textit 429 on writeback; sleeping {wait}s (attempt {attempt})")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp


def writeback_textit(rows):
    headers = {"Authorization": f"Token {TEXTIT_TOKEN}", "Content-Type": "application/json"}
    ok = 0
    bad = 0
    errors = []
    for i, r in enumerate(rows, 1):
        fields = {}
        if (r["set_state"] or "").strip():
            fields["state"] = r["set_state"].strip()
        if (r["set_vamc"] or "").strip():
            fields["vamc_presumed"] = r["set_vamc"].strip()
        if not fields:
            continue
        try:
            _textit_post_writeback(
                f"{TEXTIT_CONTACTS_URL}?uuid={r['uuid']}",
                {"fields": fields}, headers,
            )
            ok += 1
        except Exception as e:
            bad += 1
            errors.append({"uuid": r["uuid"], "error": str(e)})
            logger.warning(f"  writeback ERR {r['uuid']}: {e}")
        if i % 100 == 0:
            logger.info(f"  writeback {i}/{len(rows)}")
        if WRITEBACK_THROTTLE_SEC:
            time.sleep(WRITEBACK_THROTTLE_SEC)
    logger.info(f"WRITEBACK: {ok} ok, {bad} failed")
    return {"sent_ok": ok, "sent_failed": bad, "errors": errors[:50]}


# ---------------------------------------------------------------------------
# Core callable — lifts into the nightly orchestrator
# ---------------------------------------------------------------------------

def run_backfill(do_textit=True, writeback_limit=None):
    client = get_bq_client()

    rows = pull_population(client)
    if not rows:
        return {"status": "success", "affected": 0, "note": "no contacts need state/vamc"}

    zip_map = resolve_zips(rows)
    resolved = build_resolved(rows, zip_map)
    stage_resolved(client, resolved)

    state_writes = sum(1 for r in resolved if r["set_state"])
    vamc_writes = sum(1 for r in resolved if r["set_vamc"])

    backfill = apply_backfill(client)

    writeback = None
    if do_textit:
        if not TEXTIT_TOKEN:
            writeback = {"skipped": "TEXTIT_TOKEN not set"}
        else:
            wb_rows = get_writeback_population(client, limit=writeback_limit)
            writeback = writeback_textit(wb_rows)
            if writeback_limit is not None:
                writeback["limited_to"] = int(writeback_limit)

    return {
        "status": "success",
        "affected": len(rows),
        "distinct_zips": len(zip_map),
        "resolved_state_values": state_writes,
        "resolved_vamc_values": vamc_writes,
        "bq": backfill,
        "textit_writeback": writeback,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"}), 200


@app.route("/sync", methods=["POST"])
def sync():
    body = request.get_json(force=True, silent=True) or {}
    if SYNC_PASSWORD and body.get("password") != SYNC_PASSWORD:
        return jsonify({"status": "error", "message": "Unauthorized"}), 403
    # do_textit defaults true; pass {"do_textit": false} to run BQ-only (dry-er run).
    # writeback_limit caps the TextIt writeback (Rule-23 single-record-before-bulk):
    # pass {"writeback_limit": 1} for the first real TextIt run, omit for unbounded.
    do_textit = body.get("do_textit", True)
    writeback_limit = body.get("writeback_limit", None)
    try:
        result = run_backfill(do_textit=do_textit, writeback_limit=writeback_limit)
        return jsonify(result), 200
    except Exception as e:
        logger.exception("state-vamc backfill failed")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
