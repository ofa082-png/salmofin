"""
fetch_mattilsynet_disease.py
----------------------------
Fetches disease cases per facility from Mattilsynet public API.
No authentication required — Client-Id header only.
Deletes all rows and reinserts fresh (full dataset, not year-based).
"""

import requests
import os
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID     = "salmofin"
DATASET_ID     = "salmofin"
DISEASE_TABLE  = f"{PROJECT_ID}.{DATASET_ID}.mattilsynet_disease"
BASE_URL = "https://akvakultur-offentlig-api.fisk.mattilsynet.io/api/sykdomstilfeller/v1/rapporteringer"
CLIENT_ID      = "salmofin"
HEADERS        = {"Client-Id": CLIENT_ID, "Accept": "application/json"}

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def fetch_all_facilities():
    print("Fetching facilities from Mattilsynet...")
    all_rows = []
    offset = 0
    limit = 100
    while True:
        resp = requests.get(BASE_URL, headers=HEADERS, params={"limit": limit, "offset": offset})
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_rows.extend(batch)
        print(f"  Fetched {len(all_rows):,} facilities...")
        if len(batch) < limit:
            break
        offset += limit
    print(f"  Total facilities: {len(all_rows):,}")
    return all_rows

def flatten_to_rows(facilities):
    rows = []
    for f in facilities:
        base = {
            "anleggId":        f.get("anleggId"),
            "anleggNavn":      f.get("anleggNavn"),
            "produksjonsform": ", ".join(f.get("produksjonsform") or []),
        }
        owners   = f.get("eiere") or []
        org_nr   = owners[0].get("id")   if owners else None
        org_navn = owners[0].get("navn") if owners else None
        diseases = f.get("sykdomstilfeller") or []
        if not diseases:
            rows.append({**base, "organisasjonsnummer": org_nr, "organisasjonsnavn": org_navn,
                         "sykdomstype": None, "diagnoseDato": None})
        else:
            for d in diseases:
                rows.append({**base,
                    "organisasjonsnummer": org_nr,
                    "organisasjonsnavn":   org_navn,
                    "sykdomstype":         d.get("sykdomstype"),
                    "diagnoseDato":        d.get("diagnoseDato"),
                })
    return rows

def reload_bigquery(client, df):
    print("Deleting existing rows...")
    client.query(f"DELETE FROM `{DISEASE_TABLE}` WHERE true").result()
    print(f"Inserting {len(df):,} rows...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, DISEASE_TABLE, job_config=job_config).result()
    print("  Done.")

if __name__ == "__main__":
    print("Script starting...")
    client     = get_bq_client()
    print("BQ client ok")
    facilities = fetch_all_facilities()
    print(f"Got {len(facilities)} facilities")
    rows       = flatten_to_rows(facilities)
    print(f"Flattened to {len(rows)} rows")
    df         = pd.DataFrame(rows)
    df["anleggId"]    = pd.to_numeric(df["anleggId"], errors="coerce").astype("Int64")
    df["diagnoseDato"] = pd.to_datetime(df["diagnoseDato"], errors="coerce", utc=True)
    print(df.shape)
    reload_bigquery(client, df)
    print("Done!")
