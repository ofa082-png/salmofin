"""
fetch_locality_capacity.py
--------------------------
Monthly script — fetches MTB capacity history per locality from Barentswatch,
truncates and reloads BigQuery table.
Loops all active localities from the localities table.
"""

import json
import os
import time
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

TOKEN_URL  = "https://id.barentswatch.no/connect/token"
API_URL    = "https://www.barentswatch.no/bwapi/v1/geodata/locality/{}/capacity"

BW_CLIENT_ID     = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]

PROJECT_ID       = "salmofin"
DATASET_ID       = "salmofin"
CAPACITY_TABLE   = f"{PROJECT_ID}.{DATASET_ID}.locality_capacity"


def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


def get_token() -> str:
    resp = requests.post(TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     BW_CLIENT_ID,
        "client_secret": BW_CLIENT_SECRET,
        "scope":         "api"
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def get_locality_numbers(client) -> list:
    print("Fetching locality numbers from BigQuery...")
    query = "SELECT DISTINCT siteNr FROM `salmofin.salmofin.localities` WHERE siteNr IS NOT NULL ORDER BY siteNr"
    result = [row.siteNr for row in client.query(query).result()]
    print(f"  Found {len(result):,} localities")
    return result


def fetch_all_capacity(token: str, site_nrs: list) -> pd.DataFrame:
    print("Fetching capacity history from Barentswatch...")
    all_rows = []
    errors   = 0

    for i, site_nr in enumerate(site_nrs):
        try:
            resp = requests.get(
                API_URL.format(site_nr),
                headers={"Authorization": f"Bearer {token}"},
                timeout=10
            )
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            records = resp.json()
            for r in records:
                all_rows.append({
                    "localityNo": site_nr,
                    "year":       r.get("year"),
                    "type":       r.get("type"),
                    "capacity":   r.get("capacity"),
                })
        except Exception as e:
            errors += 1
            if errors <= 10:
                print(f"  ERROR for {site_nr}: {e}")
            continue

        # Progress update every 100
        if (i + 1) % 100 == 0:
            print(f"  Processed {i + 1:,}/{len(site_nrs):,} localities, {len(all_rows):,} records so far...")

        # Small delay to be polite to the API
        time.sleep(0.1)

    print(f"  Done — {len(all_rows):,} records, {errors} errors")
    df = pd.DataFrame(all_rows)
    df["year"]     = pd.to_numeric(df["year"],     errors="coerce").astype("Int64")
    df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce")
    return df


def reload_bigquery(client, df: pd.DataFrame) -> None:
    if len(df) == 0:
        raise Exception("0 rows — aborting!")
    print(f"Truncating {CAPACITY_TABLE}...")
    client.query(f"DELETE FROM `{CAPACITY_TABLE}` WHERE true").result()
    print(f"Inserting {len(df):,} rows...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, CAPACITY_TABLE, job_config=job_config).result()
    print("  Done.")


if __name__ == "__main__":
    client   = get_bq_client()
    token    = get_token()
    site_nrs = get_locality_numbers(client)
    df       = fetch_all_capacity(token, site_nrs)
    reload_bigquery(client, df)
    print("All done.")
