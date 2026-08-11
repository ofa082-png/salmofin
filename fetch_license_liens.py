"""
fetch_license_liens.py
-----------------------
Monthly script — fetches current liens (pant/heftelser) per license from
Fiskeridir, truncates and reloads BigQuery license_liens table.
No auth required. Only licenses with at least one registered lien produce rows.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.cloud import bigquery
from google.oauth2 import service_account

LICENSES_URL = "https://api.fiskeridir.no/pub-aqua/api/v1/licenses"
LIENS_URL    = "https://api.fiskeridir.no/pub-aqua/api/v1/licenses/{license_nr}/liens"

PROJECT_ID  = "salmofin"
DATASET_ID  = "salmofin"
LIENS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.license_liens"

BATCH_SIZE       = 100
WORKERS          = 4
MAX_ERROR_RATE   = 0.01  # abort the reload if more than 1% of licenses failed


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=6,
        backoff_factor=2,           # 2s, 4s, 8s, 16s, 32s, 64s
        status_forcelist=[429, 500, 502, 503, 504],
        respect_retry_after_header=True,
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


def fetch_all_license_nrs() -> list:
    print("Fetching license list from Fiskeridir...")
    nrs = []
    start = 0
    while True:
        end = start + BATCH_SIZE - 1
        resp = requests.get(LICENSES_URL, params={"range": f"{start}-{end}"})
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        nrs.extend(l["licenseNr"] for l in batch if l.get("licenseNr"))
        if len(batch) < BATCH_SIZE:
            break
        start += BATCH_SIZE
    print(f"  Total: {len(nrs):,} licenses")
    return nrs


def fetch_liens_for_license(session: requests.Session, license_nr: str) -> list:
    resp = session.get(LIENS_URL.format(license_nr=license_nr), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    ajour_date = data.get("ajourDate")
    rows = []
    for lien in data.get("liens") or []:
        owner = lien.get("registeredOwner") or {}
        holder = lien.get("lienholder") or {}
        rows.append({
            "license_nr":          license_nr,
            "ajour_date":          ajour_date,
            "journal_nr":          lien.get("journalNr"),
            "journal_date":        lien.get("journalDate"),
            "amount":              lien.get("amount"),
            "currency":            lien.get("currency"),
            "owner_org_nr":        owner.get("orgNr"),
            "owner_name":          owner.get("name"),
            "lienholder_org_nr":   holder.get("orgNr"),
            "lienholder_name":     holder.get("name"),
            "lienholder_zip":      holder.get("zipCode"),
            "lienholder_city":     holder.get("city"),
            "lienholder_country":  holder.get("country"),
        })
    return rows


def fetch_all_liens(license_nrs: list) -> pd.DataFrame:
    print(f"Fetching liens for {len(license_nrs):,} licenses ({WORKERS} workers)...")
    session = make_session()
    all_rows = []
    done = 0
    errors = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = {pool.submit(fetch_liens_for_license, session, nr): nr for nr in license_nrs}
        for future in as_completed(futures):
            nr = futures[future]
            try:
                all_rows.extend(future.result())
            except Exception as e:
                errors += 1
                print(f"  Error fetching {nr}: {e}")
            done += 1
            if done % 500 == 0:
                print(f"  ...{done:,}/{len(license_nrs):,}")

    error_rate = errors / len(license_nrs) if license_nrs else 0
    print(f"  Done. {len(all_rows):,} lien rows, {errors} errors ({error_rate:.1%} of licenses)")
    if error_rate > MAX_ERROR_RATE:
        raise Exception(
            f"Error rate {error_rate:.1%} exceeds {MAX_ERROR_RATE:.0%} threshold "
            f"({errors}/{len(license_nrs)} licenses failed) — aborting before touching BigQuery."
        )

    df = pd.DataFrame(all_rows)
    if len(df) > 0:
        df["ajour_date"]   = pd.to_datetime(df["ajour_date"], utc=True, errors="coerce")
        df["journal_date"] = pd.to_datetime(df["journal_date"], utc=True, errors="coerce")
        df["amount"]       = pd.to_numeric(df["amount"], errors="coerce")
        df["fetched_at"]   = pd.Timestamp.now(tz="UTC")
    return df


def reload_table(client, df: pd.DataFrame) -> None:
    if len(df) == 0:
        raise Exception("0 rows — aborting, refusing to wipe the table!")
    print(f"Truncating {LIENS_TABLE}...")
    client.query(f"DELETE FROM `{LIENS_TABLE}` WHERE true").result()
    print(f"Inserting {len(df):,} rows...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, LIENS_TABLE, job_config=job_config).result()
    print("  Done.")


if __name__ == "__main__":
    client       = get_bq_client()
    license_nrs  = fetch_all_license_nrs()
    df           = fetch_all_liens(license_nrs)
    reload_table(client, df)
    print("All done.")
