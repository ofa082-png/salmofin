"""
fetch_lice.py
-------------
Weekly script — fetches current year lice data from Barentswatch,
deletes current year rows and reinserts fresh.
No joins — flat raw data only.
"""

import os
import io
import json
import datetime
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

TOKEN_URL = "https://id.barentswatch.no/connect/token"
API_URL   = "https://www.barentswatch.no/bwapi/v1/geodata/download/fishhealth"

BW_CLIENT_ID     = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]

PROJECT_ID   = "salmofin"
DATASET_ID   = "salmofin"
LICE_TABLE   = f"{PROJECT_ID}.{DATASET_ID}.lice_bw"
CURRENT_YEAR = datetime.datetime.now().year

RENAME_MAP = {
    "År":                       "Ar",
    "Uke":                      "Uke",
    "Lokalitetsnummer":         "Lokalitetsnummer",
    "Lokalitetsnavn":           "Lokalitetsnavn",        # added
    "Voksne hunnlus":           "Voksne_hunnlus",
    "Lus i bevegelige stadier": "Lus_i_bevegelige_stadier",
    "Fastsittende lus":         "Fastsittende_lus",
    "Trolig uten fisk":         "Trolig_uten_fisk",
    "Har telt lakselus":        "Har_telt_lakselus",
    "Lusegrense uke":           "Lusegrense_uke",
    "Over lusegrense uke":      "Over_lusegrense_uke",
    "Sjøtemperatur":            "Sjotemperatur",
    "ProduksjonsområdeId":      "ProduksjonsomraadeId",
}

KEEP_COLS = [
    "Uke", "Ar", "Lokalitetsnummer", "Lokalitetsnavn",   # added
    "Voksne_hunnlus", "Lus_i_bevegelige_stadier", "Fastsittende_lus",
    "Trolig_uten_fisk", "Har_telt_lakselus",
    "Lusegrense_uke", "Over_lusegrense_uke", "Sjotemperatur",
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


def max_week(year: int) -> int:
    """Returns 53 if the year has a week 53, otherwise 52."""
    last_day = datetime.date(year, 12, 28)  # Dec 28 is always in the last ISO week
    return last_day.isocalendar()[1]


def fetch_lice(token: str) -> pd.DataFrame:
    print(f"Fetching {CURRENT_YEAR} lice data from Barentswatch...")
    resp = requests.get(API_URL, params={
        "reporttype": "lice",
        "filetype":   "csv",
        "fromyear":   str(CURRENT_YEAR),
        "fromweek":   "1",
        "toyear":     str(CURRENT_YEAR),
        "toweek":     str(max_week(CURRENT_YEAR)),
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

    # Fix decimal comma → dot
    for col in ["Voksne_hunnlus", "Lus_i_bevegelige_stadier", "Fastsittende_lus",
                "Lusegrense_uke", "Sjotemperatur"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False).str.strip(),
                errors="coerce"
            )

    # Fix Norwegian booleans
    for col in ["Trolig_uten_fisk", "Har_telt_lakselus", "Over_lusegrense_uke"]:
        if col in df.columns:
            df[col] = df[col].map({"Ja": True, "Nei": False, True: True, False: False})

    print(f"  Final shape: {df.shape}")
    return df


def reload_bigquery(client, df: pd.DataFrame) -> None:
    print(f"Deleting {CURRENT_YEAR} rows from {LICE_TABLE}...")
    client.query(
        f"DELETE FROM `{LICE_TABLE}` WHERE Ar = {CURRENT_YEAR}"
    ).result()
    print(f"Inserting {len(df):,} rows...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, LICE_TABLE, job_config=job_config).result()
    print("  Done.")


if __name__ == "__main__":
    client = get_bq_client()
    token  = get_token()
    df     = fetch_lice(token)
    df     = clean(df)
    reload_bigquery(client, df)
    print("All done.")
