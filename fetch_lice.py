"""
fetch_lice.py
-------------
Fetches 2026 salmon lice data from Barentswatch bulk CSV endpoint,
adds sequential index per Lokalitetsnummer (continuing from historical),
then truncates and reloads Supabase table.

Environment variables required:
    BW_CLIENT_ID      - Barentswatch client ID
    BW_CLIENT_SECRET  - Barentswatch client secret
    SUPABASE_URL      - Supabase project URL
    SUPABASE_KEY      - Supabase service role key
"""

import os
import io
import math
import requests
import pandas as pd

# --- Config ---
TOKEN_URL = "https://id.barentswatch.no/connect/token"
API_URL = "https://www.barentswatch.no/bwapi/v1/geodata/download/fishhealth"
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BW_CLIENT_ID = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]
TABLE = "lice"


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
    print("Fetching lice CSV from Barentswatch...")
    resp = requests.get(API_URL, params={
        "reporttype": "lice",
        "filetype": "csv",
        "fromyear": "2026",
        "fromweek": "1",
        "toyear": "2026",
        "toweek": "53"
    }, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    content = resp.content.decode("utf-8-sig")
    df = pd.read_csv(io.StringIO(content), low_memory=False)
    print(f"  Fetched {len(df):,} rows")
    print(f"  Raw columns: {list(df.columns)}")
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    # Explicit rename — fix special chars for Supabase compatibility
    rename_map = {
        "År": "År",
        "Uke": "Uke",
        "Lokalitetsnummer": "Lokalitetsnummer",
        "Lokalitetsnavn": "Lokalitetsnavn",
        "Voksne hunnlus": "Voksne_hunnlus",
        "Lus i bevegelige stadier": "Lus_i_bevegelige_stadier",
        "Fastsittende lus": "Fastsittende_lus",
        "Trolig uten fisk": "Trolig_uten_fisk",
        "Har telt lakselus": "Har_telt_lakselus",
        "Kommunenummer": "Kommunenummer",
        "Kommune": "Kommune",
        "Fylkesnummer": "Fylkesnummer",
        "Fylke": "Fylke",
        "Lat": "Lat",
        "Lon": "Lon",
        "Lusegrense uke": "Lusegrense_uke",
        "Over lusegrense uke": "Over_lusegrense_uke",
        "Sjøtemperatur": "Sjotemperatur",
        "ProduksjonsområdeId": "ProduksjonsomraadeId",
        "Produksjonsområde": "Produksjonsomraade",
    }
    df = df.rename(columns=rename_map)
    print(f"  Renamed columns: {list(df.columns)}")

    # Add AarUke
    df["AarUke"] = df["År"].astype(str) + "-" + df["Uke"].astype(str).str.zfill(2)

    # Sort by Lokalitetsnummer then AarUke for correct indexing
    df = df.sort_values(["Lokalitetsnummer", "År", "Uke"]).reset_index(drop=True)

    # Add sequential index per locality (1-based)
    df["Index"] = df.groupby("Lokalitetsnummer").cumcount() + 1

    # Fix numeric columns — handle comma decimals
    numeric_cols = [
        "Voksne_hunnlus", "Lus_i_bevegelige_stadier", "Fastsittende_lus",
        "Lusegrense_uke", "Sjotemperatur", "Lat", "Lon"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False).str.strip(),
                errors="coerce"
            )

    # Fix integer columns
    for col in ["Lokalitetsnummer", "År", "Uke", "Kommunenummer", "Fylkesnummer", "ProduksjonsomraadeId"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    print(f"  Final shape: {df.shape}")
    return df


def truncate_table(headers: dict) -> None:
    print(f"Truncating table '{TABLE}'...")
    resp = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{TABLE}",
        headers={**headers, "Prefer": "return=minimal"},
        params={"Lokalitetsnummer": "gte.0"}
    )
    if resp.status_code not in (200, 204):
        raise Exception(f"Truncate failed: {resp.status_code} {resp.text}")
    print("  Table truncated.")


def insert_to_supabase(df: pd.DataFrame) -> None:
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    truncate_table(headers)

    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    batch_size = 1000
    total = len(df)
    inserted = 0

    df = df.replace([float("inf"), float("-inf")], None)
    df = df.where(pd.notnull(df), None)
    records = df.to_dict(orient="records")
    records = [
        {k: (None if isinstance(v, float) and (math.isnan(v) or math.isinf(v)) else v)
         for k, v in row.items()}
        for row in records
    ]

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        resp = requests.post(url, json=batch, headers=headers)
        if resp.status_code not in (200, 201):
            print(f"  ERROR batch {i}-{i+len(batch)}: {resp.status_code} {resp.text[:200]}")
        else:
            inserted += len(batch)
            print(f"  Inserted {inserted:,}/{total:,}")

    print(f"Done. {inserted:,} rows inserted to Supabase table '{TABLE}'")


if __name__ == "__main__":
    token = get_token()
    df = fetch_lice(token)
    df = clean(df)
    insert_to_supabase(df)
