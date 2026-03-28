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

# ── Current year and week ─────────────────────────────────────────────────────
now         = datetime.utcnow()
current_year = now.year
current_week = now.isocalendar()[1]

print(f"Fetching disease zone data for week {current_week}/{current_year}")

token   = get_token()
headers = {"Authorization": f"Bearer {token}"}

# ── Helper: get all forsknr zone IDs from a download endpoint ─────────────────
def get_zone_ids(endpoint):
    url  = f"{BASE_URL}{endpoint}?format=json"
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        print(f"  Warning: {endpoint} returned {resp.status_code}")
        return []
    data     = resp.json()
    features = data.get("features", [])
    ids      = list(set(
        f["properties"]["forsknr"] 
        for f in features 
        if f.get("properties", {}).get("forsknr")
    ))
    print(f"  Found {len(ids)} zones in {endpoint}")
    return ids

# ── Helper: get localities within a specific zone ─────────────────────────────
def get_localities_in_zone(forsknr, year, week, disease, zone_type):
    zone_endpoint = {
        ("PD",  "Protection"):   "pdprotectionzone",
        ("PD",  "Surveillance"): "pdsurveillancezone",
        ("ISA", "Protection"):   "ilaprotectionzone",
        ("ISA", "Surveillance"): "ilasurveillancezone",
    }[(disease, zone_type)]

    url  = f"{BASE_URL}/v1/geodata/fishhealth/{zone_endpoint}/{forsknr}/{year}/{week}"
    resp = requests.get(url, headers=headers)

    if resp.status_code not in (200, 204):
        return []

    data       = resp.json()
    localities = data.get("localities", [])
    zone_name  = data.get("forsknavn", forsknr)
    from_date  = data.get("fromDate")
    to_date    = data.get("toDate")

    rows = []
    for loc in localities:
        rows.append({
            "LocalityNumber": loc.get("localityNo"),
            "LocalityName":   loc.get("name"),
            "Disease":        disease,
            "ZoneType":       zone_type,
            "ZoneName":       zone_name,
            "Forsknr":        forsknr,
            "Suspected":      loc.get("pdSuspected") or loc.get("ilaSuspected"),
            "Confirmed":      loc.get("pdConfirmed") or loc.get("ilaConfirmed"),
            "IsOutbreakSite": loc.get("isReportingLocality"),
            "ZoneFrom":       from_date,
            "ZoneTo":         to_date,
            "FetchedDate":    now.strftime("%Y-%m-%d"),
        })
    return rows

# ── Fetch all zone IDs ────────────────────────────────────────────────────────
pd_protection_ids    = get_zone_ids("/v1/geodata/download/pdprotectionzone")
pd_surveillance_ids  = get_zone_ids("/v1/geodata/download/pdsurveillancezone")
isa_protection_ids   = get_zone_ids("/v1/geodata/download/ilaprotectionzone")
isa_surveillance_ids = get_zone_ids("/v1/geodata/download/ilasurveillancezone")

# ── Fetch localities for each zone ────────────────────────────────────────────
all_rows = []

for forsknr in pd_protection_ids:
    all_rows += get_localities_in_zone(forsknr, current_year, current_week, "PD", "Protection")

for forsknr in pd_surveillance_ids:
    all_rows += get_localities_in_zone(forsknr, current_year, current_week, "PD", "Surveillance")

for forsknr in isa_protection_ids:
    all_rows += get_localities_in_zone(forsknr, current_year, current_week, "ISA", "Protection")

for forsknr in isa_surveillance_ids:
    all_rows += get_localities_in_zone(forsknr, current_year, current_week, "ISA", "Surveillance")

# ── Build DataFrame ───────────────────────────────────────────────────────────
df = pd.DataFrame(all_rows)

if df.empty:
    print("No disease zone data found!")
else:
    # Remove duplicates — keep Protection over Surveillance for same locality+disease
    df["ZonePriority"] = df["ZoneType"].map({"Protection": 0, "Surveillance": 1})
    df = df.sort_values(["LocalityNumber", "Disease", "ZonePriority"])
    df = df.drop(columns=["ZonePriority"])
    
    print(f"Total rows: {len(df):,}")
    print(f"Unique localities: {df['LocalityNumber'].nunique()}")
    print(f"PD localities: {len(df[df['Disease']=='PD']['LocalityNumber'].unique())}")
    print(f"ISA localities: {len(df[df['Disease']=='ISA']['LocalityNumber'].unique())}")

# ── Save CSV ──────────────────────────────────────────────────────────────────
output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "disease_zones.csv")
df.to_csv(output_path, index=False)
print(f"Saved to {output_path}")
