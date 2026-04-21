"""
fetch_treatments.py
-------------------
Fetches salmon treatment data from Barentswatch bulk CSV endpoint,
cleans and filters it, then truncates and reloads Supabase table.

- No indexing (treatments are sparse, index not meaningful)
- Drops dim columns (Kommune, Fylke, Lat, Lon etc.) to save space
- Filters out Tiltak = "rensefisk"
- Runs daily via GitHub Actions

Environment variables required:
    BW_CLIENT_ID      - Barentswatch client ID
    BW_CLIENT_SECRET  - Barentswatch client secret
    SUPABASE_URL      - Supabase project URL
    SUPABASE_KEY      - Supabase service role key
"""

import os
import io
import requests
import pandas as pd

# --- Config ---
TOKEN_URL = "https://id.barentswatch.no/connect/token"
API_URL = "https://www.barentswatch.no/bwapi/v1/geodata/download/fishhealth?reporttype=treatments&filetype=csv"
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BW_CLIENT_ID = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]
TABLE = "treatments"

# Columns to keep (drop dim columns to save Supabase space)
KEEP_COLS = [
    "Lokalitetsnummer", "År", "Uke", "AarUke",
    "Tiltak", "Type behandling", "Virkestoff",
    "ArtsId", "Rensefisk", "Antall", "Omfang", "Antall merder",
    "ProduksjonsområdeId"
]


def get_token() -> str:
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": BW_CLIENT_ID,
        "client_secret": BW_CLIENT_SECRET,
        "scope": "api"
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def fetch_treatments(token: str) -> pd.DataFrame:
    print("Fetching treatments CSV from Barentswatch...")
    resp = requests.get(API_URL, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text), encoding="utf-8")
    print(f"  Fetched {len(df):,} rows")
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    # Add AarUke
    df["AarUke"] = df["År"].astype(str) + "-" + df["Uke"].astype(str)

    # Filter out rensefisk
    before = len(df)
    df = df[df["Tiltak"] != "rensefisk"].copy()
    print(f"  Filtered rensefisk: {before - len(df):,} rows removed, {len(df):,} remaining")

    # Keep only needed columns (ignore missing ones gracefully)
    cols = [c for c in KEEP_COLS if c in df.columns]
    df = df[cols]

    # Clean up types
    for col in ["Lokalitetsnummer", "År", "Uke", "ArtsId", "Antall", "ProduksjonsområdeId"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    if "Antall merder" in df.columns:
        df["Antall merder"] = pd.to_numeric(df["Antall merder"], errors="coerce").astype("Int64")

    # Rename to snake_case for Supabase
    df = df.rename(columns={
        "Lokalitetsnummer": "lokalitetsnummer",
        "År": "aar",
        "Uke": "uke",
        "AarUke": "aaruke",
        "Tiltak": "tiltak",
        "Type behandling": "type_behandling",
        "Virkestoff": "virkestoff",
        "ArtsId": "arts_id",
        "Rensefisk": "rensefisk",
        "Antall": "antall",
        "Omfang": "omfang",
        "Antall merder": "antall_merder",
        "ProduksjonsområdeId": "produksjonsomraade_id"
    })

    print(f"  Columns: {list(df.columns)}")
    return df


def truncate_table(headers: dict) -> None:
    print(f"Truncating table '{TABLE}'...")
    resp = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{TABLE}",
        headers={**headers, "Prefer": "return=minimal"},
        params={"id": "gte.0"}  # delete all rows
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

    records = df.where(pd.notnull(df), None).to_dict(orient="records")

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
    df = fetch_treatments(token)
    df = clean(df)
    insert_to_supabase(df)
