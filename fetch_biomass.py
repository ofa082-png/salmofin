"""
fetch_biomass.py
----------------
Monthly script — fetches biomass statistics from Fiskeridirektoratet,
truncates and reloads BigQuery table.
No auth required.
"""

import os
import json
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

API_URL = "https://register.fiskeridir.no/biomassestatistikk/BIOSTAT-LAKS-OMR/biostat-total-omr.json"

PROJECT_ID     = "salmofin"
DATASET_ID     = "salmofin"
BIOMASS_TABLE  = f"{PROJECT_ID}.{DATASET_ID}.biomass"

RENAME_MAP = {
    "ÅR":                              "Ar",
    "MÅNED_KODE":                      "Maaned_kode",
    "MÅNED":                           "Maaned",
    "PO_KODE":                         "PO_kode",
    "PO_NAVN":                         "PO_navn",
    "ARTSID":                          "Artsid",
    "UTSETTSÅR":                       "Utsettsar",
    "BEHFISK_STK":                     "Behfisk_stk",
    "BIOMASSE_KG":                     "Biomasse_kg",
    "UTSETT_SMOLT_STK":                "Utsett_smolt_stk",
    "UTSETT_SMOLT_STK_MINDRE_ENN_500G":"Utsett_smolt_stk_under500g",
    "FORFORBRUK_KG":                   "Forforbruk_kg",
    "UTTAK_STK":                       "Uttak_stk",
    "UTTAK_KG":                        "Uttak_kg",
    "UTTAK_SLØYD_KG":                  "Uttak_sloyd_kg",
    "UTTAK_HODEKAPPET_KG":             "Uttak_hodekappet_kg",
    "UTTAK_RUNDVEKT_KG":               "Uttak_rundvekt_kg",
    "DØDFISK_STK":                     "Dodfisk_stk",
    "UTKAST_STK":                      "Utkast_stk",
    "RØMMING_STK":                     "Romming_stk",
    "ANDRE_STK":                       "Andre_stk",
}

KEEP_COLS = [
    "Ar", "Maaned_kode", "Maaned", "PO_kode", "PO_navn", "Artsid",
    "Utsettsar", "Behfisk_stk", "Biomasse_kg", "Utsett_smolt_stk",
    "Utsett_smolt_stk_under500g", "Forforbruk_kg", "Uttak_stk", "Uttak_kg",
    "Uttak_sloyd_kg", "Uttak_hodekappet_kg", "Uttak_rundvekt_kg",
    "Dodfisk_stk", "Utkast_stk", "Romming_stk", "Andre_stk", "AarMnd", "running_month",
]


def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


def fetch_biomass() -> pd.DataFrame:
    print("Fetching biomass data from Fiskeridirektoratet...")
    resp = requests.get(API_URL, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    df = pd.DataFrame(data["Data"])
    print(f"  Fetched {len(df):,} rows")
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=RENAME_MAP)

    # Add AarMnd computed column
    df["AarMnd"] = df["Ar"].astype(str) + "-" + df["Maaned_kode"].astype(str).str.zfill(2)
    df["running_month"] = (df["Ar"] - df["Utsettsar"]).astype(int) * 12 + df["Maaned_kode"].astype(int)

    missing = [c for c in KEEP_COLS if c not in df.columns]
    if missing:
        print(f"  WARNING — columns not found: {missing}")

    df = df[[c for c in KEEP_COLS if c in df.columns]]

    # Fix integer columns
    for col in ["Ar", "Maaned_kode", "Utsettsar", "Behfisk_stk", "Utsett_smolt_stk",
                "Utsett_smolt_stk_under500g", "Forforbruk_kg", "Uttak_stk",
                "Dodfisk_stk", "Utkast_stk", "Romming_stk", "Andre_stk"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Fix float columns
    for col in ["Biomasse_kg", "Uttak_kg", "Uttak_sloyd_kg",
                "Uttak_hodekappet_kg", "Uttak_rundvekt_kg"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
   
    # Force string columns to str
    for col in ["PO_kode", "PO_navn", "Maaned", "Artsid", "AarMnd"]:
        if col in df.columns:
            df[col] = df[col].astype(str)

    print(f"  Final shape: {df.shape}")
    return df


def reload_bigquery(client, df: pd.DataFrame) -> None:
    if len(df) == 0:
        raise Exception("Fetch returned 0 rows — aborting, not deleting existing data!")
    print(f"Truncating {BIOMASS_TABLE}...")
    client.query(f"DELETE FROM `{BIOMASS_TABLE}` WHERE true").result()
    print(f"Inserting {len(df):,} rows...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, BIOMASS_TABLE, job_config=job_config).result()
    print("  Done.")


if __name__ == "__main__":
    client = get_bq_client()
    df     = fetch_biomass()
    df     = clean(df)
    reload_bigquery(client, df)
    print("All done.")
