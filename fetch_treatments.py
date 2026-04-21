"""
fetch_treatments.py
-------------------
Fetches salmon treatment data from Barentswatch bulk CSV endpoint,
cleans and filters it, then truncates and reloads Supabase table.
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

KEEP_COLS = [
    "lokalitetsnummer", "aar", "uke", "aaruke",
    "tiltak", "type_behandling", "virkestoff",
    "arts_id", "rensefisk", "antall", "omfang", "antall_merder",
    "produksjonsomraade_id"
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
    df = pd.read_csv(io.StringIO(resp.text), encoding="utf-8", low_memory=False)
    print(f"  Fetched {len(df):,} rows")
    print(f"  Raw columns: {list(df.columns)}")
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize column names: lowercase, strip spaces, replace special chars
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace("å", "aa", regex=False)
        .str.replace("ø", "o", regex=False)
        .str.replace("æ", "ae", regex=False)
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    print(f"  Normalized columns: {list(df.columns)}")

    # Rename specific columns to match our target schema
    rename_map = {
        "aar": "aar",           # år → aar (already normalized above)
        "type_behandling": "type_behandling",
        "produksjonsomraadeid": "produksjonsomraade_id",
        "antall_merder": "antall_merder",
        "artsid": "arts_id",
    }
    df = df.rename(columns=rename_map)

    # Add AarUke
    df["aaruke"] = df["aar"].astype(str) + "-" + df["uke"].astype(str)

    # Filter out rensefisk
    before = len(df)
    df = df[df["tiltak"] != "rensefisk"].copy()
    print(f"  Filtered rensefisk: {before - len(df):,} rows removed, {len(df):,} remaining")

    # Keep only needed columns
    cols = [c for c in KEEP_COLS if c in df.columns]
    missing = [c for c in KEEP_COLS if c not in df.columns]
    if missing:
        print(f"  WARNING - columns not found: {missing}")
    df = df[cols]

    # Clean up types
    for col in ["lokalitetsnummer", "aar", "uke", "arts_id", "antall", "produksjonsomraade_id", "antall_merder"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


def truncate_table(headers: dict) -> None:
    print(f"Truncating table '{TABLE}'...")
    resp = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{TABLE}",
        headers={**headers, "Prefer": "return=minimal"},
        params={"lokalitetsnummer": "gte.0"}
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
