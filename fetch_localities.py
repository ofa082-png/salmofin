"""
fetch_localities.py
-------------------
Monthly script — fetches all aquaculture localities from Fiskeridir,
truncates and reloads BigQuery localities table.
No auth required.
"""

import json
import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

BASE_URL         = "https://api.fiskeridir.no/pub-aqua/api/v1/sites"
PROJECT_ID       = "salmofin"
DATASET_ID       = "salmofin"
LOCALITIES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.localities"
BATCH_SIZE       = 100


def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


def fetch_all_sites() -> list:
    print("Fetching all sites from Fiskeridir...")
    all_sites = []
    start = 0

    while True:
        end = start + BATCH_SIZE - 1
        resp = requests.get(BASE_URL, params={"range": f"{start}-{end}"})
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_sites.extend(batch)
        print(f"  Fetched {len(all_sites):,} sites...")
        if len(batch) < BATCH_SIZE:
            break
        start += BATCH_SIZE

    print(f"  Total: {len(all_sites):,} sites")
    return all_sites


def flatten_localities(sites: list) -> pd.DataFrame:
    rows = []
    for s in sites:
        placement = s.get("placement") or {}
        rows.append({
            "siteId":                s.get("siteId"),
            "siteNr":                s.get("siteNr"),
            "name":                  s.get("name"),
            "placementType":         s.get("placementType"),
            "waterType":             s.get("waterType"),
            "firstClearanceTime":    s.get("firstClearanceTime"),
            "firstClearanceType":    s.get("firstClearanceType"),
            "latitude":              s.get("latitude"),
            "longitude":             s.get("longitude"),
            "capacity":              s.get("capacity"),
            "tempCapacity":          s.get("tempCapacity"),
            "capacityUnitType":      s.get("capacityUnitType"),
            "municipalityCode":      placement.get("municipalityCode"),
            "municipalityName":      placement.get("municipalityName"),
            "countyCode":            placement.get("countyCode"),
            "countyName":            placement.get("countyName"),
            "prodAreaCode":          placement.get("prodAreaCode"),
            "prodAreaName":          placement.get("prodAreaName"),
            "prodAreaStatus":        placement.get("prodAreaStatus"),
            "isSlaughtery":          s.get("isSlaughtery"),
            "hasCommercialActivity": s.get("hasCommercialActivity"),
            "hasColocation":         s.get("hasColocation"),
            "hasJointOperation":     s.get("hasJointOperation"),
            "speciesTypes":          ",".join(s.get("speciesTypes") or []),
        })
    df = pd.DataFrame(rows)
    df["firstClearanceTime"] = pd.to_datetime(df["firstClearanceTime"], utc=True, errors="coerce")
    print(f"  Shape: {df.shape}")
    return df


def reload_table(client, df: pd.DataFrame) -> None:
    if len(df) == 0:
        raise Exception("0 rows — aborting!")
    print(f"Truncating {LOCALITIES_TABLE}...")
    client.query(f"DELETE FROM `{LOCALITIES_TABLE}` WHERE true").result()
    print(f"Inserting {len(df):,} rows...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, LOCALITIES_TABLE, job_config=job_config).result()
    print("  Done.")


if __name__ == "__main__":
    client = get_bq_client()
    sites  = fetch_all_sites()
    df     = flatten_localities(sites)
    reload_table(client, df)
    print("All done.")
