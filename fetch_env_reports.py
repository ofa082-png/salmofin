"""
fetch_env_reports.py
--------------------
Monthly script — fetches environmental survey reports from Fiskeridir,
truncates and reloads BigQuery table.
No auth required.
"""

import json
import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

BASE_URL   = "https://api.fiskeridir.no/envreportreg-public/api/v1/report/search"
PAGE_SIZE  = 100
START_DATE = "2000-01-01T00:00:00Z"

PROJECT_ID  = "salmofin"
DATASET_ID  = "salmofin"
ENV_TABLE   = f"{PROJECT_ID}.{DATASET_ID}.env_reports"


def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


def fetch_all_reports() -> list:
    print("Fetching environmental reports from Fiskeridir...")
    all_records = []
    page = 0

    while True:
        resp = requests.get(BASE_URL, params={
            "updatedAfter": START_DATE,
            "page":         page,
            "size":         PAGE_SIZE,
            "sort":         "id,ASC"
        })
        resp.raise_for_status()
        data = resp.json()

        content = data.get("content", [])
        if not content:
            break

        all_records.extend(content)
        total_pages = data.get("totalPages", 1)
        print(f"  Fetched page {page + 1}/{total_pages} — {len(all_records):,} records so far...")

        if data.get("last", True):
            break
        page += 1

    print(f"  Total: {len(all_records):,} reports")
    return all_records


def flatten(records: list) -> pd.DataFrame:
    rows = []
    for r in records:
        rows.append({
            "reportId":             r.get("reportId"),
            "organisationNumber":   r.get("organisationNumber"),
            "siteNumber":           r.get("siteNumber"),
            "siteName":             r.get("siteName"),
            "competentBodyNumber":  r.get("competentBodyNumber"),
            "reportCreated":        r.get("reportCreated"),
            "reportVersionUpdated": r.get("reportVersionUpdated"),
            "reportStatusUpdated":  r.get("reportStatusUpdated"),
            "siteCondition":        r.get("siteCondition"),
            "envExaminationType":   r.get("envExaminationType"),
            "summary":              r.get("summary"),
        })

    df = pd.DataFrame(rows)

    for col in ["reportCreated", "reportVersionUpdated", "reportStatusUpdated"]:
        df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    for col in ["reportId", "siteCondition"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    df["siteNumber"] = pd.to_numeric(df["siteNumber"], errors="coerce").astype("Int64")

    print(f"  Final shape: {df.shape}")
    return df


def reload_bigquery(client, df: pd.DataFrame) -> None:
    if len(df) == 0:
        raise Exception("0 rows — aborting!")
    print(f"Truncating {ENV_TABLE}...")
    client.query(f"DELETE FROM `{ENV_TABLE}` WHERE true").result()
    print(f"Inserting {len(df):,} rows...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, ENV_TABLE, job_config=job_config).result()
    print("  Done.")


if __name__ == "__main__":
    client  = get_bq_client()
    records = fetch_all_reports()
    df      = flatten(records)
    reload_bigquery(client, df)
    print("All done.")
