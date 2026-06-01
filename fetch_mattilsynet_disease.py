"""
fetch_mattilsynet_disease.py
----------------------------
Fetches disease cases per facility from Mattilsynet public API.
No authentication required — Client-Id header only.
Deletes all rows and reinserts fresh (full dataset, not year-based).
"""

import requests
import os
import json
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID     = "salmofin"
DATASET_ID     = "salmofin"
DISEASE_TABLE  = f"{PROJECT_ID}.{DATASET_ID}.mattilsynet_disease"
BASE_URL       = "https://akvakultur-offentlig-api.fisk.mattilsynet.io/api/sykdom/v1/anlegg"
CLIENT_ID      = "salmofin"
HEADERS        = {"Client-Id": CLIENT_ID, "Accept": "application/json"}

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def fetch_all_facilities():
    print("Fetching facilities from Mattilsynet...")
    all_rows = []
    offset = 0
    limit = 100

    while True:
      resp = requests.get(BASE_URL, headers=HEADERS, params={"limit": limit, "offset": offset})
