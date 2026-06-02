"""
fetch_treatments.py
-------------------
Weekly script — fetches all treatments data from Barentswatch,
truncates and reloads BigQuery table.
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

PROJECT_ID        = "salmofin"
DATASET_ID        = "salmofin"
TREATMENTS_TABLE  = f"{PROJECT_ID}.{DATASET_ID}.treatments"

RENAME_MAP = {
    "År":                   "Ar",
    "Uke":                  "Uke",
    "Lokalitetsnummer":     "Lokalitetsnummer",
    "Tiltak":               "Tiltak",
    "Type behandling":      "Type_behandling",
    "Virkestoff":           "Virkestoff",
    "ArtsId":               "ArtsId",
    "Rensefisk":            "Rensefisk",
    "Antall":               "Antall",
    "Omfang":               "Omfang",
    "Antall merder":        "Antall_merder",
    "ProduksjonsområdeId":  "ProduksjonsomraadeId",
}

KEEP_COLS = [
    "Lokalitetsnummer", "Ar", "Uke", "AarUke",
    "Tiltak", "Type_behandling", "Virkestoff",
    "ArtsId", "Rensefisk", "Antall", "Omfang", "Antall_merder",
    "ProduksjonsomraadeId",
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


def fetch_treatments(token: str) -> pd.DataFrame:
    print("Fetching treatments from Barentswatch...")
    resp = requests.get(API_URL, params={
        "reporttype": "treatments",
        "filetype":   "csv",
    }, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.content.decode("utf-8-sig")), low_memory=False)
    print(f"  Fetched {len(df):,} rows")
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df = df.rename(columns=RENAME_MAP)

    # Filter out rensefisk
    before = len(df)
    df = df[df["Tiltak"] != "rensefisk"].copy()
    print(f"  Filtered rensefisk: {before - len(df):,} rows removed, {len(df):,} remaining")

    # Add AarUke
    df["AarUke"] = df["Ar"].astype(str) + "-" + df["Uke"].astype(str)

    missing = [c for c in KEEP_COLS if c not in df.columns]
    if missing:
        print(f"  WARNING — columns not found: {missing}")

    df = df[[c for c in KEEP_COLS if c in df.columns]]

    # Fix integer columns
    for col in ["Lokalitetsnummer", "Ar", "Uke", "ArtsId", "Antall",
                "ProduksjonsomraadeId", "Antall_merder"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    print(f"  Final shape: {df.shape}")
    return df


def reload_bigquery(client, df: pd.DataFrame) -> None:
    if len(df) == 0:
        raise Exception("Fetch returned 0 rows — aborting, not deleting existing data!")
    print(f"Truncating {TREATMENTS_TABLE}...")
    client.query(f"DELETE FROM `{TREATMENTS_TABLE}` WHERE true").result()
    print(f"Inserting {len(df):,} rows...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, TREATMENTS_TABLE, job_config=job_config).result()
    print("  Done.")


if __name__ == "__main__":
    client = get_bq_client()
    token  = get_token()
    df     = fetch_treatments(token)
    df     = clean(df)
    reload_bigquery(client, df)
    print("All done.")
