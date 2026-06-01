"""
fetch_mattilsynet_disease.py
----------------------------
Fetches disease reports from Mattilsynet public API.
No authentication required — Client-Id header only.
Deletes all rows and reinserts fresh daily.
"""

import requests
import os
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID    = "salmofin"
DATASET_ID    = "salmofin"
DISEASE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.mattilsynet_disease"
BASE_URL      = "https://akvakultur-offentlig-api.fisk.mattilsynet.io/api/sykdomstilfeller/v1/rapporteringer"
HEADERS       = {"Client-Id": "salmofin", "Accept": "application/json"}

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def fetch_all_reports():
    print("Fetching disease reports from Mattilsynet...")
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
        print(f"  Fetched {len(all_rows):,} reports...")
        if len(batch) < limit:
            break
        offset += limit
    print(f"  Total reports: {len(all_rows):,}")
    return all_rows

def flatten_to_rows(reports):
    rows = []
    for r in reports:
        arter = r.get("arter") or []
        rows.append({
            "id":                          r.get("id"),
            "lokalitetsnummer":            r.get("lokalitetsnummer"),
            "lokalitetsnavn":              r.get("lokalitetsnavn"),
            "sykdomstype":                 r.get("sykdomstype"),
            "sykdomssubtype":              r.get("sykdomssubtype"),
            "artskode":                    arter[0].get("artskode") if arter else None,
            "varslingsdato":               r.get("varslingsdato"),
            "oppdrettersMistankedato":     r.get("oppdrettersMistankedato"),
            "kvalitetssikretMistankedato": r.get("kvalitetssikretMistankedato"),
            "diagnosedato":                r.get("diagnosedato"),
            "avslutningsdato":             r.get("avslutningsdato"),
            "avslutningsarsak":            r.get("avslutningsårsak"),
            "ugyldiggjøringsdato":         r.get("ugyldiggjøringsdato"),
            "opprettet":                   r.get("opprettet"),
            "oppdatert":                   r.get("oppdatert"),
            "sekvensnummer":               r.get("sekvensnummer"),
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
    client  = get_bq_client()
    print("BQ client ok")
    reports = fetch_all_reports()
    rows    = flatten_to_rows(reports)
    df      = pd.DataFrame(rows)

    for col in ["varslingsdato", "oppdrettersMistankedato", "kvalitetssikretMistankedato",
                "diagnosedato", "avslutningsdato", "ugyldiggjøringsdato", "opprettet", "oppdatert"]:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).astype("datetime64[us, UTC]")

    df["lokalitetsnummer"] = pd.to_numeric(df["lokalitetsnummer"], errors="coerce").astype("Int64")
    df["sekvensnummer"]    = pd.to_numeric(df["sekvensnummer"],    errors="coerce").astype("Int64")

    print(df.shape)
    reload_bigquery(client, df)
    print("Done!")
