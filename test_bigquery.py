"""
test_bigquery.py
----------------
Quick test — fetches current week lice data from Barentswatch
and inserts a small sample into BigQuery lice table.
"""

import os
import io
import json
import requests
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

TOKEN_URL = "https://id.barentswatch.no/connect/token"
API_URL = "https://www.barentswatch.no/bwapi/v1/geodata/download/fishhealth"
BW_CLIENT_ID = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]
CURRENT_YEAR = datetime.now().year
PROJECT_ID = "salmofin"
DATASET_ID = "salmofin"
LICE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.lice"

RENAME_MAP = {
    "År": "Ar",
    "Uke": "Uke",
    "Lokalitetsnummer": "Lokalitetsnummer",
    "Voksne hunnlus": "Voksne_hunnlus",
    "Lus i bevegelige stadier": "Lus_i_bevegelige_stadier",
    "Fastsittende lus": "Fastsittende_lus",
    "Trolig uten fisk": "Trolig_uten_fisk",
    "Har telt lakselus": "Har_telt_lakselus",
    "Lusegrense uke": "Lusegrense_uke",
    "Over lusegrense uke": "Over_lusegrense_uke",
    "Sjøtemperatur": "Sjotemperatur",
    "ProduksjonsområdeId": "ProduksjonsomraadeId",
}

KEEP_COLS = [
    "Uke", "Ar", "Lokalitetsnummer",
    "Voksne_hunnlus", "Lus_i_bevegelige_stadier", "Fastsittende_lus",
    "Trolig_uten_fisk", "Har_telt_lakselus",
    "Lusegrense_uke", "Over_lusegrense_uke", "Sjotemperatur",
    "ProduksjonsomraadeId"
]

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def get_token() -> str:
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": BW_CLIENT_ID,
        "client_secret": BW_CLIENT_SECRET,
        "scope": "api"
    })
    resp.raise_for_status()
    return resp.json()["access_token"]

def fetch_lice(token: str) -> pd.DataFrame:
    print(f"Fetching {CURRENT_YEAR} lice data from Barentswatch...")
    resp = requests.get(API_URL, params={
        "reporttype": "lice",
        "filetype": "csv",
        "fromyear": str(CURRENT_YEAR),
        "fromweek": "1",
        "toyear": str(CURRENT_YEAR),
        "toweek": "53"
    }, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.content.decode("utf-8-sig")), low_memory=False)
    print(f"  Fetched {len(df):,} rows")
    return df

if __name__ == "__main__":
    client = get_bq_client()
    print("BigQuery client OK")

    token = get_token()
    print("Barentswatch token OK")

    df = fetch_lice(token)
    df.columns = df.columns.str.strip()
    df = df.rename(columns=RENAME_MAP)
    cols = [c for c in KEEP_COLS if c in df.columns]
    df = df[cols].head(10)  # just 10 rows for testing
    print(f"\nSample data:\n{df.head()}")

    print("\nInserting 10 rows to BigQuery...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, LICE_TABLE, job_config=job_config).result()
    print("Insert OK")

    print("\nQuerying back...")
    query = f"SELECT * FROM `{LICE_TABLE}` LIMIT 10"
    result = client.query(query).to_dataframe()
    print(result)
