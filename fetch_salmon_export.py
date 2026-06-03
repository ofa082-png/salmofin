"""
fetch_salmon_export.py
----------------------
Weekly script — fetches salmon export data from SSB table 03024,
transforms to flat table and reloads BigQuery.
No auth required.
"""

import json
import os
import urllib.request
import pandas as pd
from io import StringIO
from google.cloud import bigquery
from google.oauth2 import service_account

SSB_URL = (
    "https://data.ssb.no/api/pxwebapi/v2/tables/03024/data?lang=en"
    "&valueCodes[VareGrupper2]=*"
    "&valueCodes[Tid]=from(2000U01)"
    "&valueCodes[ContentsCode]=*"
    "&stub=VareGrupper2,Tid,ContentsCode"
    "&outputformat=csv&outputformatparams=separatorsemicolon,usetexts"
)

PROJECT_ID   = "salmofin"
DATASET_ID   = "salmofin"
EXPORT_TABLE = f"{PROJECT_ID}.{DATASET_ID}.salmon_export_weekly"


def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


def fetch_export() -> pd.DataFrame:
    print("Fetching salmon export data from SSB...")
    with urllib.request.urlopen(SSB_URL) as response:
        content = response.read().decode("latin-1")
    df = pd.read_csv(StringIO(content), sep=";")
    print(f"  Fetched {len(df):,} rows, columns: {list(df.columns)}")
    return df


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    last_col = df.columns[-1]
    df = df.rename(columns={last_col: "value"})

    col_map = {
        df.columns[0]: "product_category",
        df.columns[1]: "uke",
        df.columns[2]: "contents",
    }
    df = df.rename(columns=col_map)

    # Derive year and week
    df["year"] = pd.to_numeric(df["uke"].str[:4], errors="coerce").astype("Int64")
    df["week"] = pd.to_numeric(df["uke"].str[-2:], errors="coerce").astype("Int64")

    # Clean value
    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # Shorten product category names
    df["product_category"] = df["product_category"].str.replace(
        "Fish-farm bred salmon, fresh or chilled", "Fresh", regex=False
    ).str.replace(
        "Fish-farm bred salmon, frozen", "Frozen", regex=False
    )

    # Pivot contents to columns
    df = df.pivot_table(
        index=["product_category", "uke", "year", "week"],
        columns="contents",
        values="value"
    ).reset_index()
    df.columns.name = None

    # Rename value columns
    rename = {}
    for col in df.columns:
        if "Weight" in col or "tonne" in col.lower():
            rename[col] = "vekt_tonn"
        elif "Price" in col or "kilo" in col.lower():
            rename[col] = "kilopris_nok"
    df = df.rename(columns=rename)

    print(f"  Final shape: {df.shape}")
    return df


def reload_bigquery(client, df: pd.DataFrame) -> None:
    if len(df) == 0:
        raise Exception("0 rows — aborting!")
    print(f"Truncating {EXPORT_TABLE}...")
    client.query(f"DELETE FROM `{EXPORT_TABLE}` WHERE true").result()
    print(f"Inserting {len(df):,} rows...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    client.load_table_from_dataframe(df, EXPORT_TABLE, job_config=job_config).result()
    print("  Done.")


if __name__ == "__main__":
    client = get_bq_client()
    df     = fetch_export()
    df     = clean(df)
    reload_bigquery(client, df)
    print("All done.")
