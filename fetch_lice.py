"""
fetch_lice.py
-------------
Daily script — fetches current year lice data from Barentswatch,
continues Index from historical data already in Supabase,
deletes current year rows and reinserts fresh data.
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

# Column name mapping — source → target
RENAME_MAP = {
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
    print(f"  Raw columns: {list(df.columns)}")
    return df


def get_max_index_per_locality(headers: dict) -> dict:
    """Get max Index per locality from historical data (År < current year)."""
    print("Fetching max historical index per locality from Supabase...")
    # Use URL encoding for Norwegian character in column name
    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"

    # Use Supabase aggregation to get max Index per locality directly
    # This avoids paginating through all 783k rows
    query_url = f"{url}?select=Lokalitetsnummer,Index.max()&%C3%85r=lt.{CURRENT_YEAR}&limit=10000"
    resp = requests.get(query_url, headers=headers)
    
    if resp.status_code != 200:
        print(f"  Aggregation query failed ({resp.status_code}), falling back to pagination...")
        # Fallback: paginate through all rows
        all_rows = []
        offset = 0
        batch_size = 1000
        while True:
            q = f"{url}?select=Lokalitetsnummer,Index&%C3%85r=lt.{CURRENT_YEAR}&limit={batch_size}&offset={offset}"
            r = requests.get(q, headers=headers)
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            all_rows.extend(batch)
            offset += batch_size
            if len(batch) < batch_size:
                break
        if not all_rows:
            print("  No historical data found — index will start at 1.")
            return {}
        df = pd.DataFrame(all_rows)
        max_index = df.groupby("Lokalitetsnummer")["Index"].max().to_dict()
    else:
        rows = resp.json()
        if not rows:
            print("  No historical data found — index will start at 1.")
            return {}
        df = pd.DataFrame(rows)
        print(f"  Aggregation response columns: {list(df.columns)}")
        # Column might be named 'max' or 'Index'
        index_col = [c for c in df.columns if c != "Lokalitetsnummer"][0]
        max_index = df.set_index("Lokalitetsnummer")[index_col].to_dict()

    print(f"  Found max index for {len(max_index):,} localities.")
    return max_index


def clean_and_index(df: pd.DataFrame, max_index: dict) -> pd.DataFrame:
    # Normalize column names first to handle any encoding issues
    df.columns = df.columns.str.strip()
    print(f"  Columns after strip: {list(df.columns)}")

    df = df.rename(columns=RENAME_MAP)
    print(f"  Columns after rename: {list(df.columns)}")

    # Sort for correct indexing
    df = df.sort_values(["Lokalitetsnummer", "År", "Uke"]).reset_index(drop=True)

    # Assign index continuing from historical max using cumcount
    df["Index"] = df.groupby("Lokalitetsnummer").cumcount() + 1
    # Add offset from historical max per locality
    df["Index"] = df.apply(
        lambda r: r["Index"] + int(max_index.get(r["Lokalitetsnummer"], 0)),
        axis=1
    )

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
    missing = [c for c in KEEP_COLS if c not in df.columns]
    if missing:
        print(f"  WARNING - columns not found: {missing}")
    df = df[cols]

    print(f"  Final shape: {df.shape}")
    print(f"  Index range: {df['Index'].min()} - {df['Index'].max()}")
    return df


def delete_current_year(headers: dict) -> None:
    print(f"Deleting {CURRENT_YEAR} rows from Supabase...")
    delete_url = f"{SUPABASE_URL}/rest/v1/{TABLE}?%C3%85r=eq.{CURRENT_YEAR}"
    resp = requests.delete(
        delete_url,
        headers={**headers, "Prefer": "return=minimal"}
    )
    if resp.status_code not in (200, 204):
        raise Exception(f"Delete failed: {resp.status_code} {resp.text}")
    print("  Deleted.")


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
