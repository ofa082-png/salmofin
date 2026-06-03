"""
fetch_licenses.py
-----------------
Monthly script — fetches all aquaculture licenses from Fiskeridir,
truncates and reloads BigQuery licenses table.
No auth required.
"""

import json
import os
import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

BASE_URL       = "https://api.fiskeridir.no/pub-aqua/api/v1/licenses"
PROJECT_ID     = "salmofin"
DATASET_ID     = "salmofin"
LICENSES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.licenses"
BATCH_SIZE     = 100


def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


def fetch_all_licenses() -> list:
    print("Fetching all licenses from Fiskeridir...")
    all_licenses = []
    start = 0

    while True:
        end = start + BATCH_SIZE - 1
        resp = requests.get(BASE_URL, params={"range": f"{start}-{end}"})
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        all_licenses.extend(batch)
        print(f"  Fetched {len(all_licenses):,} licenses...")
        if len(batch) < BATCH_SIZE:
            break
        start += BATCH_SIZE

    print(f"  Total: {len(all_licenses):,} licenses")
    return all_licenses


def flatten_licenses(licenses: list) -> pd.DataFrame:
    rows = []
    for l in licenses:
        capacity    = l.get("capacity") or {}
        type_       = l.get("type") or {}
        placement   = l.get("placement") or {}
        grant       = l.get("grantInformation") or {}
        species     = l.get("species") or {}
        connections = l.get("connections") or []

        rows.append({
            "licenseId":            l.get("licenseId"),
            "licenseNr":            l.get("licenseNr"),
            "legacyLicenseNr":      l.get("legacyLicenseNr"),
            "openLegalEntityNr":    l.get("openLegalEntityNr"),
            "legalEntityName":      l.get("legalEntityName"),
            "capacityAccumulated":  capacity.get("accumulated"),
            "capacityCurrent":      capacity.get("current"),
            "capacityUnit":         capacity.get("unit"),
            "capacityType":         capacity.get("type"),
            "intention":            type_.get("intention"),
            "intentionValue":       type_.get("intentionValue"),
            "productionStage":      type_.get("productionStage"),
            "productionStageValue": type_.get("productionStageValue"),
            "tag":                  type_.get("tag"),
            "municipalityCode":     placement.get("municipalityCode"),
            "municipalityName":     placement.get("municipalityName"),
            "countyCode":           placement.get("countyCode"),
            "countyName":           placement.get("countyName"),
            "prodAreaCode":         placement.get("prodAreaCode"),
            "prodAreaName":         placement.get("prodAreaName"),
            "grantedTime":          grant.get("grantedTime"),
            "grantCapacity":        grant.get("capacity"),
            "grantLegalEntityNr":   grant.get("openLegalEntityNr"),
            "grantLegalEntityName": grant.get("legalEntityName"),
            "speciesCodes":         ",".join([f.get("code", "") for f in species.get("fishCodes") or []]),
            "speciesNames":         ",".join([f.get("nbNoName", "") for f in species.get("fishCodes") or []]),
            "connectedSiteNrs":     ",".join([str(c.get("siteNr", "")) for c in connections]),
        })

    df = pd.DataFrame(rows)
    df["grantedTime"] = pd.to_datetime(df["grantedTime"], utc=True, errors="coerce")
    for col in ["capacityAccumulated", "capacityCurrent", "grantCapacity"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    print(f"  Shape: {df.shape}")
    return df


def reload_table(client, df: pd.DataFrame) -> None:
    if len(df) == 0:
        raise Exception("0 rows — aborting!")
    print(f"Truncating {LICENSES_TABLE}...")
    client.query(f"DELETE FROM `{LICENSES_TABLE}` WHERE true").result()
    print(f"Inserting {len(df):,} rows...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, LICENSES_TABLE, job_config=job_config).result()
    print("  Done.")


if __name__ == "__main__":
    client   = get_bq_client()
    licenses = fetch_all_licenses()
    df       = flatten_licenses(licenses)
    reload_table(client, df)
    print("All done.")
