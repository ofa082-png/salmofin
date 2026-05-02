"""
fetch_lice.py
-------------
Daily script — fetches current year lice data from Barentswatch,
continues Index from historical data already in Supabase,
joins vessel visit counts per locality/week,
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

VESSEL_CATEGORIES_URL = "https://raw.githubusercontent.com/ofa082-png/salmofin/main/vessel_categories.csv"

KEEP_COLS = [
    "Uke", "År", "Lokalitetsnummer",
    "Voksne_hunnlus", "Lus_i_bevegelige_stadier", "Fastsittende_lus",
    "Trolig_uten_fisk", "Har_telt_lakselus",
    "Lusegrense_uke", "Over_lusegrense_uke", "Sjotemperatur",
    "ProduksjonsomraadeId", "Index",
    "feedCarrier", "wellboat", "silage", "delicing", "processing"
]

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
    return df


def fetch_vessels_from_supabase(headers: dict) -> pd.DataFrame:
    print("Fetching vessel visits from Supabase...")
    url = f"{SUPABASE_URL}/rest/v1/vessel_visits"
    all_rows = []
    offset = 0
    batch_size = 10000
    while True:
        query_url = f"{url}?select=mmsi,localityNo,week,year,startTime,stopTime&order=id.asc&limit={batch_size}&offset={offset}"
        resp = requests.get(query_url, headers=headers)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_rows.extend(batch)
        offset += batch_size
        if len(batch) < batch_size:
            break
    df = pd.DataFrame(all_rows)
    # Calculate visit duration in hours
    df["visitDuration"] = (
        pd.to_datetime(df["stopTime"], errors="coerce") - 
        pd.to_datetime(df["startTime"], errors="coerce")
    ).dt.total_seconds() / 3600
    df["visitDuration"] = df["visitDuration"].clip(lower=0)
    print(f"  Fetched {len(df):,} vessel rows")
    return df


def fetch_categories() -> pd.DataFrame:
    print("Fetching vessel categories...")
    resp = requests.get(VESSEL_CATEGORIES_URL)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.content.decode("utf-8-sig")), sep=None, engine="python")
    print(f"  Raw columns: {list(df.columns)}")
    # Keep only Type and MMSI
    df = df[["Type", "MMSI"]].dropna(subset=["MMSI"])
    df["MMSI"] = pd.to_numeric(df["MMSI"], errors="coerce").astype("Int64").astype(str).str.strip()
    df["Type"] = df["Type"].str.strip()

    print(f"  Loaded {len(df):,} vessel categories, types: {df['Type'].unique().tolist()}")
    return df


def build_vessel_aggregates(vessels: pd.DataFrame, categories: pd.DataFrame) -> pd.DataFrame:
    print("Joining vessel categories and aggregating...")
    vessels["mmsi"] = pd.to_numeric(vessels["mmsi"], errors="coerce").astype("Int64").astype(str).str.strip()

    merged = vessels.merge(categories, left_on="mmsi", right_on="MMSI", how="left")

    agg = merged.groupby(["localityNo", "year", "week"]).agg(
        feedCarrier=("Type", lambda x: (x == "Fish feed carrier").sum()),
        wellboat=("Type", lambda x: (x == "Wellboat").sum()),
        silage=("Type", lambda x: (x == "Silage").sum()),
        delicing=("Type", lambda x: (x == "Delicing vessel").sum()),
        processing=("Type", lambda x: (x == "Processing vessel").sum()),
    ).reset_index()

    print(f"  Aggregated to {len(agg):,} locality/week combinations")

    return agg


def get_max_index_per_locality(headers: dict) -> dict:
    print("Fetching max historical index per locality from Supabase...")
    rpc_url = f"{SUPABASE_URL}/rest/v1/rpc/get_max_lice_index?limit=10000"
    resp = requests.post(rpc_url, headers=headers, json={})
    if resp.status_code != 200:
        raise Exception(f"RPC call failed: {resp.status_code} {resp.text}")
    rows = resp.json()
    if not rows:
        print("  No historical data found — index will start at 1.")
        return {}
    df = pd.DataFrame(rows)
    max_index = df.set_index("Lokalitetsnummer")["max_index"].to_dict()
    print(f"  Found max index for {len(max_index):,} localities.")
    return max_index


def clean_and_index(df: pd.DataFrame, max_index: dict, vessel_agg: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df = df.rename(columns=RENAME_MAP)

    # Sort for correct indexing
    df = df.sort_values(["Lokalitetsnummer", "År", "Uke"]).reset_index(drop=True)

    # Assign index continuing from historical max
    df["Index"] = df.groupby("Lokalitetsnummer").cumcount() + 1
    df["Index"] = df.apply(
        lambda r: r["Index"] + int(max_index.get(r["Lokalitetsnummer"], 0)),
        axis=1
    )

    # Join vessel aggregates
    df = df.merge(
        vessel_agg,
        left_on=["Lokalitetsnummer", "År", "Uke"],
        right_on=["localityNo", "year", "week"],
        how="left"
    )
    # Drop duplicate join columns
    df = df.drop(columns=["localityNo", "year", "week"], errors="ignore")

    # Fix numeric columns
    for col in ["Voksne_hunnlus", "Lus_i_bevegelige_stadier", "Fastsittende_lus",
                "Lusegrense_uke", "Sjotemperatur"]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False).str.strip(),
                errors="coerce"
            )

    # Fix integer columns
    for col in ["Lokalitetsnummer", "År", "Uke", "ProduksjonsomraadeId",
                "feedCarrier", "wellboat", "silage", "delicing", "processing"]:
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
    resp = requests.delete(delete_url, headers={**headers, "Prefer": "return=minimal"})
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
    vessels = fetch_vessels_from_supabase(headers)
    categories = fetch_categories()
    vessel_agg = build_vessel_aggregates(vessels, categories)
    max_index = get_max_index_per_locality(headers)
    df = clean_and_index(df, max_index, vessel_agg)
    delete_current_year(headers)
    insert_to_supabase(df, headers)
