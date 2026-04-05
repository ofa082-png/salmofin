import requests
import pandas as pd
import os
from datetime import datetime

# ── Auth ──────────────────────────────────────────────────────────────────────
CLIENT_ID     = os.environ.get("BW_CLIENT_ID")
CLIENT_SECRET = os.environ.get("BW_CLIENT_SECRET")
TOKEN_URL     = "https://id.barentswatch.no/connect/token"
BASE_URL      = "https://www.barentswatch.no/bwapi"

def get_token():
    resp = requests.post(TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "api"
    })
    resp.raise_for_status()
    return resp.json()["access_token"]

token   = get_token()
headers = {"Authorization": f"Bearer {token}"}

# ── Step 1: Get all active localities from Barentswatch ───────────────────────
# Using the localitieswithpd/ila download gives us some, but for all localities
# we use the Fiskeridirektoratet ArcGIS service — no auth needed
print("Fetching all locality numbers...")

arcgis_url = (
    "https://gis.fiskeridir.no/server/rest/services/FiskeridirWFS_akva/"
    "MapServer/0/query"
)
params = {
    "where":         "ARTSGRUPPE = 'Laks og regnbueørret'",
    "outFields":     "loknr,navn,org_nr,org_navn",
    "returnGeometry": "false",
    "f":             "json",
    "resultRecordCount": 5000,
}

resp = requests.get(arcgis_url, params=params)
data = resp.json()
features = data.get("features", [])

localities = []
for f in features:
    attrs = f.get("attributes", {})
    loc_no = attrs.get("loknr")
    if loc_no:
        localities.append({
            "LocalityNumber": int(loc_no),
            "LocalityName":   attrs.get("navn"),
            "OrgNumber":      attrs.get("org_nr"),
            "OrgName":        attrs.get("org_navn"),
        })

df_locs = pd.DataFrame(localities).drop_duplicates(subset=["LocalityNumber"])
print(f"Found {len(df_locs):,} localities")

# ── Step 2: Fetch capacity history for each locality ─────────────────────────
print("\nFetching capacity history...")

all_rows = []
errors   = 0

for i, row in df_locs.iterrows():
    loc_no = row["LocalityNumber"]
    
    resp = requests.get(
        f"{BASE_URL}/v1/geodata/locality/{loc_no}/capacity",
        headers=headers
    )
    
    if resp.status_code == 200:
        for entry in resp.json():
            all_rows.append({
                "LocalityNumber": loc_no,
                "LocalityName":   row["LocalityName"],
                "OrgNumber":      row["OrgNumber"],
                "OrgName":        row["OrgName"],
                "Year":           entry.get("year"),
                "Type":           entry.get("type"),
                "Capacity_kg":    entry.get("capacity"),
            })
    else:
        errors += 1

    # Progress every 100 localities
    if (i + 1) % 100 == 0:
        print(f"  {i+1}/{len(df_locs)} localities processed...")

print(f"\nDone — {errors} errors")

# ── Step 3: Save ──────────────────────────────────────────────────────────────
df = pd.DataFrame(all_rows)

# Filter to salmonoids only and remove zero capacity rows
df = df[df["Type"] == "Salmonoids"]
df = df[df["Capacity_kg"] > 0]

output_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "capacity_history.csv"
)
df.to_csv(output_path, index=False)

print(f"Saved {len(df):,} rows to {output_path}")
print(f"Unique localities: {df['LocalityNumber'].nunique()}")
print(f"Year range: {df['Year'].min()} - {df['Year'].max()}")
print(f"\nTop companies by total capacity:")
print(df.groupby("OrgName")["Capacity_kg"].sum().sort_values(ascending=False).head(10))
