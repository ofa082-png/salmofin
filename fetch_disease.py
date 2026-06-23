"""
fetch_disease_bw.py
-------------------
Weekly script — fetches full disease history from Barentswatch,
replaces the entire table on each run (WRITE_TRUNCATE).
"""

import os
import io
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

TOKEN_URL = "https://id.barentswatch.no/connect/token"
API_URL   = "https://www.barentswatch.no/bwapi/v1/geodata/download/fishhealth"

BW_CLIENT_ID     = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]

PROJECT_ID    = "salmofin"
DATASET_ID    = "salmofin"
DISEASE_TABLE = f"{PROJECT_ID}.{DATASET_ID}.disease"

RENAME_MAP = {
    "År":              "Ar",
    "Uke":             "Uke",
    "Lokalitetsnummer": "Lokalitetsnummer",
    "Lokalitetsnavn":  "Lokalitetsnavn",
    "Sykdom":          "Sykdom",
    "Status":          "Status",
    "Fra dato":        "Fra_dato",
    "Til dato":        "Til_dato",
    "Kommunenummer":   "Kommunenummer",
    "Kommune":         "Kommune",
    "Fylkesnummer":    "Fylkesnummer",
    "Fylke":           "Fylke",
    "Lat":             "Lat",
    "Lon":             "Lon",
    "Produksjonsområde": "Produksjonsomraade",
    "UtbruddsId":      "UtbruddsId",
    "Subtype":         "Subtype",
    "Mistanke-dato":   "Mistanke_dato",
    "Påvist-dato":     "Paavist_dato",
}

KEEP_COLS = [
    "Ar", "Uke", "Lokalitetsnummer", "Lokalitetsnavn",
    "Sykdom", "Status", "Fra_dato", "Til_dato",
    "Kommunenummer", "Kommune", "Fylkesnummer", "Fylke",
    "Lat", "Lon", "Produksjonsomraade", "UtbruddsId",
    "Subtype", "Mistanke_dato", "Paavist_dato",
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
        "grant_type":    "client_credentials",
        "client_id":     BW_CLIENT_ID,
        "client_secret": BW_CLIENT_SECRET,
        "scope":         "api"
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_disease(token: str) -> pd.DataFrame:
    print("Fetching full disease history from Barentswatch...")
    resp = requests.get(API_URL, params={
        "reporttype": "disease",
        "filetype":   "csv",
    }, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.content.decode("utf-8-sig")), low_memory=False)
    print(f"  Fetched {len(df):,} rows")
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df = df.rename(columns=RENAME_MAP)

    missing = [c for c in KEEP_COLS if c not in df.columns]
    if missing:
        print(f"  WARNING — columns not found: {missing}")

    df = df[[c for c in KEEP_COLS if c in df.columns]]

    # Parse dates
    for col in ["Fra_dato", "Til_dato", "Mistanke_dato", "Paavist_dato"]:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col], errors="coerce"
            ).dt.date

    # Numeric
    for col in ["Lat", "Lon"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False).str.strip(),
                errors="coerce"
            )

    print(f"  Final shape: {df.shape}")
    return df


def reload_bigquery(client, df: pd.DataFrame) -> None:
    print(f"Replacing {DISEASE_TABLE}...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    client.load_table_from_dataframe(df, DISEASE_TABLE, job_config=job_config).result()
    print(f"  Loaded {len(df):,} rows. Done.")


if __name__ == "__main__":
    client = get_bq_client()
    token  = get_token()
    df     = fetch_disease(token)
    df     = clean(df)
    reload_bigquery(client, df)
    print("All done.")
