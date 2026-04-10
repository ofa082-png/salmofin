import asyncio
import aiohttp
import requests
import os

# ── Config ────────────────────────────────────────────────────────────────
FISKERIDIR_URL   = "https://api.fiskeridir.no/pub-aqua/api/v1/sites"
BW_TOKEN_URL     = "https://id.barentswatch.no/connect/token"
BW_API_URL       = "https://www.barentswatch.no/bwapi"
SUPABASE_URL     = os.environ["SUPABASE_URL"]
SUPABASE_KEY     = os.environ["SUPABASE_KEY"]
BW_CLIENT_ID     = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]
YEAR             = 2026
MAX_CONCURRENT   = 20

# ── Step 1: Get Barentswatch token ────────────────────────────────────────
def get_bw_token():
    resp = requests.post(BW_TOKEN_URL, data={
        "grant_type":    "client_credentials",
        "client_id":     BW_CLIENT_ID,
        "client_secret": BW_CLIENT_SECRET,
        "scope":         "api"
    }, headers={"Content-Type": "application/x-www-form-urlencoded"})
    print("Token response:", resp.status_code)
    return resp.json()["access_token"]

# ── Step 2: Get all salmon localities from Fiskeridir ─────────────────────
def get_localities():
    localities = []
    start = 0
    batch = 100
    while True:
        resp = requests.get(FISKERIDIR_URL, params={
            "species-type": "Salmon",
            "range": f"{start}-{start + batch - 1}"
        }, headers={"accept": "application/json; charset=UTF-8"})
        data = resp.json()
        if not data:
            break
        localities.extend(data)
        if len(data) < batch:
            break
        start += batch
    print(f"Found {len(localities)} salmon localities")
    return localities

# ── Step 3: Fetch vessel visits for one locality ──────────────────────────
async def fetch_locality(session, locality_no, token, semaphore):
    async with semaphore:
        url = f"{BW_API_URL}/v1/geodata/fishhealth/locality/{locality_no}/Vessel/{YEAR}"
        try:
            async with session.get(url, headers={"Authorization": f"Bearer {token}"}) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                rows = []
                for week in data:
                    for visit in (week.get("vesselVisits") or []):
                        rows.append({
                            "localityNo":                   week.get("localityNo"),
                            "year":                         week.get("year"),
                            "week":                         week.get("week"),
                            "weekIsAnalyzed":               week.get("weekIsAnalyzed"),
                            "anlysisBasedOnSurfaceArea":    week.get("anlysisBasedOnSurfaceArea"),
                            "mmsi":                         visit.get("mmsi"),
                            "vesselName":                   visit.get("vesselName"),
                            "startTime":                    visit.get("startTime"),
                            "stopTime":                     visit.get("stopTime"),
                            "shipType":                     visit.get("shipType"),
                            "isWellboat":                   visit.get("isWellboat"),
                            "shipRegisterVesselType":       visit.get("shipRegisterVesselType"),
                            "shipRegisterVesselTypeNameNo": visit.get("shipRegisterVesselTypeNameNo"),
                            "shipRegisterVesselTypeNameEn": visit.get("shipRegisterVesselTypeNameEn"),
                        })
                return rows
        except Exception as e:
            print(f"Error fetching locality {locality_no}: {e}")
            return []

# ── Step 4: Upsert rows to Supabase ──────────────────────────────────────
def upsert_to_supabase(rows):
    if not rows:
        print("No rows to upsert!")
        return
    for i in range(0, len(rows), 500):
        chunk = rows[i:i+500]
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/vessel_visits",
            headers={
                "apikey":        SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type":  "application/json",
                "Prefer":        "resolution=merge-duplicates,return=minimal"
            },
            json=chunk
        )
        if resp.status_code not in (200, 201):
            print(f"Supabase error: {resp.status_code} {resp.text}")
        else:
            print(f"Upserted rows {i} to {i+len(chunk)}")

# ── Main ──────────────────────────────────────────────────────────────────
async def main():
    print(f"Starting vessel visit fetch for {YEAR}...")
    token       = get_bw_token()
    localities  = get_localities()
    loc_numbers = [loc["siteNr"] for loc in localities if loc.get("siteNr")]
    print(f"Fetching vessel visits for {len(loc_numbers)} localities...")

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_locality(session, nr, token, semaphore) for nr in loc_numbers]
        results = await asyncio.gather(*tasks)

    all_rows = [row for result in results for row in result]
    print(f"Total vessel visit rows: {len(all_rows)}")
    upsert_to_supabase(all_rows)
    print("Done!")

if __name__ == "__main__":
    asyncio.run(main())
