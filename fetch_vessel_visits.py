"""
fetch_vessel_visits.py
----------------------
Fetches vessel visits per salmon locality from Barentswatch for the
current year. Deletes current year rows and reinserts fresh data.
Safe to re-run.
"""

import asyncio
import aiohttp
import requests
import os
import json
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# ── Config ────────────────────────────────────────────────────────────────
FISKERIDIR_URL   = "https://api.fiskeridir.no/pub-aqua/api/v1/sites"
BW_TOKEN_URL     = "https://id.barentswatch.no/connect/token"
BW_API_URL       = "https://www.barentswatch.no/bwapi"
BW_CLIENT_ID     = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]
CURRENT_YEAR     = datetime.now().year
MAX_CONCURRENT   = 20

PROJECT_ID       = "salmofin"
DATASET_ID       = "salmofin"
VISITS_TABLE     = f"{PROJECT_ID}.{DATASET_ID}.vessel_visits"

# ── BigQuery client ───────────────────────────────────────────────────────
def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

# ── Barentswatch token ────────────────────────────────────────────────────
def get_bw_token():
    resp = requests.post(BW_TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     BW_CLIENT_ID,
        "client_secret": BW_CLIENT_SECRET,
        "scope":         "api"
    }, headers={"Content-Type": "application/x-www-form-urlencoded"})
    print("Token response:", resp.status_code)
    return resp.json()["access_token"]

# ── Get all salmon localities ─────────────────────────────────────────────
def get_localities():
    localities = []
    start = 0
    batch = 100
    while True:
        resp = requests.get(FISKERIDIR_URL, params={
            "species-type": "Salmon",
            "range": f"{start}-{start + batch - 1}"
        }, headers={"accept": "application/json; charset=UTF-8"})
        data = resp.json()
        if not data:
            break
        localities.extend(data)
        if len(data) < batch:
            break
        start += batch
    print(f"Found {len(localities)} salmon localities")
    return localities

# ── Fetch vessel visits for one locality ──────────────────────────────────
async def fetch_locality(session, locality_no, token, semaphore):
    async with semaphore:
        url = f"{BW_API_URL}/v1/geodata/fishhealth/locality/{locality_no}/Vessel/{CURRENT_YEAR}"
        try:
            async with session.get(url, headers={"Authorization": f"Bearer {token}"}) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                rows = []
                for week in data:
                    for visit in (week.get("vesselVisits") or []):
                        rows.append({
                            "localityNo":                   week.get("localityNo"),
                            "year":                         week.get("year"),
                            "week":                         week.get("week"),
                            "weekIsAnalyzed":               week.get("weekIsAnalyzed"),
                            "anlysisBasedOnSurfaceArea":    week.get("anlysisBasedOnSurfaceArea"),
                            "mmsi":                         visit.get("mmsi"),
                            "vesselName":                   visit.get("vesselName"),
                            "startTime":                    visit.get("startTime"),
                            "stopTime":                     visit.get("stopTime"),
                            "shipType":                     visit.get("shipType"),
                            "isWellboat":                   visit.get("isWellboat"),
                            "shipRegisterVesselType":       visit.get("shipRegisterVesselType"),
                            "shipRegisterVesselTypeNameNo": visit.get("shipRegisterVesselTypeNameNo"),
                            "shipRegisterVesselTypeNameEn": visit.get("shipRegisterVesselTypeNameEn"),
                        })
                return rows
        except Exception as e:
            print(f"Error fetching locality {locality_no}: {e}")
            return []

# ── Delete current year ───────────────────────────────────────────────────
def delete_current_year(client):
    print(f"Deleting {CURRENT_YEAR} rows from BigQuery...")
    client.query(f"""
        DELETE FROM `{VISITS_TABLE}`
        WHERE year = {CURRENT_YEAR}
    """).result()
    print("  Deleted.")

# ── Insert to BigQuery ────────────────────────────────────────────────────
def insert_to_bigquery(client, df):
    print(f"Inserting {len(df):,} rows to BigQuery...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, VISITS_TABLE, job_config=job_config).result()
    print(f"  Inserted {len(df):,} rows.")

# ── Main ──────────────────────────────────────────────────────────────────
async def main():
    print(f"Starting vessel visit fetch for {CURRENT_YEAR}...")
    client = get_bq_client()
    token  = get_bw_token()

    localities  = get_localities()
    loc_numbers = [loc["siteNr"] for loc in localities if loc.get("siteNr")]
    print(f"Fetching vessel visits for {len(loc_numbers)} localities...")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_locality(session, nr, token, semaphore) for nr in loc_numbers]
        results = await asyncio.gather(*tasks)

    all_rows = [row for result in results for row in result]
    print(f"Total vessel visit rows fetched: {len(all_rows)}")

    if not all_rows:
        print("No rows fetched — exiting.")
        return

    df = pd.DataFrame(all_rows)
    df["startTime"] = pd.to_datetime(df["startTime"], errors="coerce", utc=True)
    df["stopTime"]  = pd.to_datetime(df["stopTime"],  errors="coerce", utc=True)
    for col in ["localityNo", "year", "week", "mmsi", "shipType", "shipRegisterVesselType"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    delete_current_year(client)
    insert_to_bigquery(client, df)
    print("Done!")

if __name__ == "__main__":
    asyncio.run(main())
