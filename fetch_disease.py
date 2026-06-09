import requests
import pandas as pd
from google.cloud import bigquery
from io import StringIO
import os

# Auth
token_url = "https://id.barentswatch.no/connect/token"
client_id = os.environ["BW_CLIENT_ID"]
client_secret = os.environ["BW_CLIENT_SECRET"]

token_resp = requests.post(token_url, data={
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": "api"
})
token = token_resp.json()["access_token"]

# Fetch CSV
url = "https://www.barentswatch.no/bwapi/v1/geodata/download/fishhealth?reporttype=disease&filetype=csv"
resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
resp.raise_for_status()

# Parse
df = pd.read_csv(StringIO(resp.text))

# Rename columns to BigQuery-friendly names
df.columns = [
    "Uke", "Ar", "Lokalitetsnummer", "Lokalitetsnavn",
    "Sykdom", "Status", "Fra_dato", "Til_dato",
    "Kommunenummer", "Kommune", "Fylkesnummer", "Fylke",
    "Lat", "Lon", "Produksjonsomraade", "UtbruddsId",
    "Subtype", "Mistanke_dato", "Paavist_dato"
]

# Add AarUke
df["AarUke"] = df["Ar"].astype(str) + "-" + df["Uke"].astype(str)

# Parse dates
df["Fra_dato"] = pd.to_datetime(df["Fra_dato"], errors="coerce").dt.date
df["Til_dato"] = pd.to_datetime(df["Til_dato"], errors="coerce").dt.date
df["Mistanke_dato"] = pd.to_datetime(df["Mistanke_dato"], errors="coerce").dt.date
df["Paavist_dato"] = pd.to_datetime(df["Paavist_dato"], errors="coerce").dt.date

# Write to BigQuery
bq = bigquery.Client()
table_id = "salmofin.salmofin.disease_bw"

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    autodetect=True
)

job = bq.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

print(f"Loaded {len(df)} rows to {table_id}")
