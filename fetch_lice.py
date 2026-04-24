"""
fetch_lice.py
-------------
Daily script — fetches current year lice data from Barentswatch,
continues Index from historical data already in Supabase,
deletes current year rows and reinserts fresh data.

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
from datetime import datetime

TOKEN_URL = "https://id.barentswatch.no/connect/token"
API_URL = "https://www.barentswatch.no/bwapi/v1/geodata/download/fishhealth"
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BW_CLIENT_ID = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]
TABLE = "lice"
CURRENT_YEAR = datetime.now().year

KEEP_COLS = [
    "Uke", "År", "Lokalitetsnummer",
    "Voksne_hunnlus", "Lus_i_bevegelige_stadier", "Fastsittende_lus",
    "Trolig_uten_fisk", "Har_telt_lakselus",
    "Lusegrense_uke", "Over_lusegrense_uke", "Sjotemperatur",
    "ProduksjonsomraadeId", "Index"
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
    content = resp.content.decode("utf-8-sig")
    df = pd.read_csv(io.StringIO(content), low_memory=False)
    print(f"  Fetched {len(df):,} rows")
    return df


def get_max_index_per_locality(headers: dict) -> dict:
    """Get max Index per locality from historical data (År < current year)."""
    print("Fetching max historical index per locality from Supabase...")
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    
    all_rows = []
    offset = 0
    batch_size = 1000
    while True:
        resp = requests.get(url, headers=headers, params={
            "select": "Lokalitetsnummer,Index",
            "År": f"lt.{CURRENT_YEAR}",
            "limit": batch_size,
            "offset": offset
        })
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_rows.extend(batch)
        offset += batch_size
        if len(batch) < batch_size:
            break

    if not all_rows:
        print("  No historical data found — index will start at 1 for all localities.")
        return {}

    df = pd.DataFrame(all_rows)
    max_index = df.groupby("Lokalitetsnummer")["Index"].max().to_dict()
    print(f"  Found max index for {len(max_index):,} localities.")
    return max_index


def clean_and_index(df: pd.DataFrame, max_index: dict) -> pd.DataFrame:
    # Rename columns
    rename_map = {
        "År": "År",
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
    df = df.rename(columns=rename_map)

    # Sort for correct indexing
    df = df.sort_values(["Lokalitetsnummer", "År", "Uke"]).reset_index(drop=True)

    # Assign index continuing from historical max
    def assign_index(group):
        lok = group["Lokalitetsnummer"].iloc[0]
        start = max_index.get(lok, 0) + 1
        group["Index"] = range(start, start + len(group))
        return group

    df = df.groupby("Lokalitetsnummer", group_keys=False).apply(assign_index)

    # Fix numeric columns
    for col in ["Voksne_hunnlus", "Lus_i_bevegelige_stadier", "Fastsittende_lus",
                "Lusegrense_uke", "Sjotemperatur"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False).str.strip(),
                errors="coerce"
            )

    # Fix integer columns
    for col in ["Lokalitetsnummer", "År", "Uke", "ProduksjonsomraadeId"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    # Keep only needed columns
    cols = [c for c in KEEP_COLS if c in df.columns]
    df = df[cols]

    print(f"  Final shape: {df.shape}")
    print(f"  Index range: {df['Index'].min()} - {df['Index'].max()}")
    return df


def delete_current_year(headers: dict) -> None:
    print(f"Deleting {CURRENT_YEAR} rows from Supabase...")
    resp = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{TABLE}",
        headers={**headers, "Prefer": "return=minimal"},
        params={"År": f"eq.{CURRENT_YEAR}"}
    )
    if resp.status_code not in (200, 204):
        raise Exception(f"Delete failed: {resp.status_code} {resp.text}")
    print(f"  Deleted.")


def insert_to_supabase(df: pd.DataFrame, headers: dict) -> None:
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

    print(f"Inserting {total:,} rows...")
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        resp = requests.post(url, json=batch, headers=headers)
        if resp.status_code not in (200, 201):
            print(f"  ERROR batch {i}-{i+len(batch)}: {resp.status_code} {resp.text[:200]}")
        else:
            inserted += len(batch)
            print(f"  Inserted {inserted:,}/{total:,}")

    print(f"Done. {inserted:,} rows inserted.")


if __name__ == "__main__":
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    token = get_token()
    df = fetch_lice(token)
    max_index = get_max_index_per_locality(headers)
    df = clean_and_index(df, max_index)
    delete_current_year(headers)
    insert_to_supabase(df, headers)
