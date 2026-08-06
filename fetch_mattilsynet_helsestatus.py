"""
fetch_mattilsynet_helsestatus.py
---------------------------------
Fetches currently-active disease status per locality from Mattilsynet
public API. No authentication required — Client-Id header only.
This is a status table, not a locality registry — only localities with
an open disease case are included (locality/company/production-form
detail already lives in the localities/licenses tables; full disease
history lives in mattilsynet_disease). One row per active case, so a
locality with two simultaneous diseases gets two rows.
Deletes all rows and reinserts fresh daily (current-state snapshot).
"""

import requests
import os
import json
import datetime
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID        = "salmofin"
DATASET_ID        = "salmofin"
HELSESTATUS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.mattilsynet_helsestatus"
BASE_URL          = "https://akvakultur-offentlig-api.fisk.mattilsynet.io/api/helsestatus/v2/lokaliteter"
HEADERS           = {"Client-Id": "salmofin", "Accept": "application/json"}

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def fetch_all(limit=100):
    print("Fetching helsestatus from Mattilsynet...")
    all_rows = []
    offset = 0
    while True:
        resp = requests.get(BASE_URL, headers=HEADERS, params={"limit": limit, "offset": offset})
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_rows.extend(batch)
        print(f"  Fetched {len(all_rows):,} localities...")
        if len(batch) < limit:
            break
        offset += limit
    print(f"  Total localities: {len(all_rows):,}")
    return all_rows

def flatten_to_rows(records):
    fetched_at = datetime.datetime.now(datetime.timezone.utc)
    rows = []
    for r in records:
        for s in (r.get("sykdomstilfeller") or []):
            rows.append({
                "lokalitetsnummer": r.get("lokalitetsnummer"),
                "lokalitetsnavn":   r.get("lokalitetsnavn"),
                "sykdomstype":      s.get("sykdomstype"),
                "diagnosedato":     s.get("diagnoseDato"),
                "fetched_at":       fetched_at,
            })
    return rows

def reload_bigquery(client, df):
    if len(df) == 0:
        raise Exception("Fetch returned 0 rows — aborting, not deleting existing data!")
    print(f"Truncating {HELSESTATUS_TABLE}...")
    client.query(f"DELETE FROM `{HELSESTATUS_TABLE}` WHERE true").result()
    print(f"Inserting {len(df):,} rows...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, HELSESTATUS_TABLE, job_config=job_config).result()
    print("  Done.")

if __name__ == "__main__":
    print("Script starting...")
    client  = get_bq_client()
    print("BQ client ok")
    records = fetch_all()
    rows    = flatten_to_rows(records)
    df      = pd.DataFrame(rows)

    df["lokalitetsnummer"] = pd.to_numeric(df["lokalitetsnummer"], errors="coerce").astype("Int64")
    df["diagnosedato"]     = pd.to_datetime(df["diagnosedato"], errors="coerce", utc=True).astype("datetime64[us, UTC]")
    df["fetched_at"]       = pd.to_datetime(df["fetched_at"], utc=True).astype("datetime64[us, UTC]")

    print(df.shape)
    reload_bigquery(client, df)
    print("Done!")
