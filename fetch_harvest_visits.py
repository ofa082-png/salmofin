"""
fetch_harvest_visits.py
-----------------------
Detects wellboat and processing vessel visits to all Norwegian
fish slaughterhouses using Barentswatch fishhealth API.

Pipeline:
1. Fetch all active slaughterhouses for the week (with coordinates)
2. Load vessels from vessel_categories.csv (Wellboat + Processing vessel types)
3. For each vessel, fetch week track from Barentswatch
4. Haversine check each ping against each plant
5. Reconstruct visits (entry/exit) and filter short ones
6. Write to data/harvest_plant_visits_{year}_W{week:02d}.csv

Run weekly via GitHub Actions for the previous completed week.
"""

import os
import csv
from math import radians, cos, sin, atan2, sqrt
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests

# --- Config ---
TOKEN_URL = "https://id.barentswatch.no/connect/token"
BASE_URL = "https://www.barentswatch.no/bwapi/v1/geodata"
BW_CLIENT_ID = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]

RADIUS_M = 300
MIN_VISIT_HOURS = 1.0
DATA_DIR = Path("data")
VESSEL_FILE = Path("vessel_categories.csv")
PLANT_LOCATIONS_FILE = DATA_DIR / "plant_locations.csv"
PLANT_LOCATION_COLUMNS = ["plant_id", "plant_name", "plant_company", "approval_number", "lat", "lon"]

VESSEL_TYPES_TO_TRACK = {"Wellboat", "Processing vessel"}

CSV_COLUMNS = [
    "year",
    "week",
    "mmsi",
    "vessel_name",
    "vessel_type",
    "capacity",
    "capacity_unit",
    "plant_id",
    "plant_name",
    "plant_company",
    "approval_number",
    "entry_time",
    "exit_time",
    "duration_hrs",
]


# --- Auth ---

def get_token() -> str:
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": BW_CLIENT_ID,
        "client_secret": BW_CLIENT_SECRET,
        "scope": "api"
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


# --- Helpers ---

def haversine(lat1, lon1, lat2, lon2) -> float:
    R = 6371000
    phi1, phi2 = radians(lat1), radians(lat2)
    dphi = radians(lat2 - lat1)
    dlambda = radians(lon2 - lon1)
    a = sin(dphi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(dlambda / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def get_previous_week() -> tuple:
    today = datetime.now(timezone.utc)
    last_week = today - timedelta(weeks=1)
    iso = last_week.isocalendar()
    return iso.year, iso.week


def get_current_week() -> tuple:
    iso = datetime.now(timezone.utc).isocalendar()
    return iso.year, iso.week


# --- Vessel list ---

def load_vessels() -> list:
    vessels = []
    with open(VESSEL_FILE, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vessel_type = row.get("Type", "").strip()
            if vessel_type not in VESSEL_TYPES_TO_TRACK:
                continue
            mmsi_raw = row.get("MMSI", "").strip()
            if not mmsi_raw:
                continue
            try:
                mmsi = int(mmsi_raw)
            except ValueError:
                continue
            vessels.append({
                "mmsi": mmsi,
                "name": row.get("Navn", "Unknown").strip(),
                "vessel_type": vessel_type,
                "capacity": row.get("LAST-KAP", "").strip(),
                "capacity_unit": row.get("ENHET", "").strip(),
            })

    print(f"Loaded {len(vessels)} vessels from {VESSEL_FILE} "
          f"({sum(1 for v in vessels if v['vessel_type'] == 'Wellboat')} wellboats, "
          f"{sum(1 for v in vessels if v['vessel_type'] == 'Processing vessel')} processing vessels)")
    return vessels


# --- Barentswatch API ---

def get_slaughterhouses(token: str, year: int, week: int) -> list:
    resp = requests.get(
        f"{BASE_URL}/fishslaughterhouses/{year}/{week}",
        headers={"Authorization": f"Bearer {token}"}
    )
    resp.raise_for_status()
    plants = resp.json()

    result = []
    for p in plants:
        geometry = p.get("geometry")
        if not geometry:
            continue
        coords = geometry.get("coordinates", [])
        if len(coords) < 2:
            continue
        result.append({
            "id": p["id"],
            "name": p.get("establishment", "Unknown"),
            "company": p.get("company", "Unknown"),
            "approval_number": p.get("approvalNumber", ""),
            "lon": coords[0],
            "lat": coords[1],
        })

    print(f"Found {len(result)} active slaughterhouses for {year}/W{week:02d}")
    return result


def update_plant_locations(plants: list) -> None:
    """Upsert plant coordinates into data/plant_locations.csv, keyed by
    plant_id. Runs as a side effect of every get_slaughterhouses() call
    since that already fetches this data — it just wasn't kept anywhere
    before. Used by the big-vessel route tracker for connect-the-dots
    maps (harvest_plant_visits.csv only stores plant_name/company, not
    coordinates)."""
    existing = {}
    if PLANT_LOCATIONS_FILE.exists():
        with open(PLANT_LOCATIONS_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                existing[row["plant_id"]] = row

    for p in plants:
        existing[p["id"]] = {
            "plant_id": p["id"],
            "plant_name": p["name"],
            "plant_company": p["company"],
            "approval_number": p["approval_number"],
            "lat": p["lat"],
            "lon": p["lon"],
        }

    DATA_DIR.mkdir(exist_ok=True)
    with open(PLANT_LOCATIONS_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=PLANT_LOCATION_COLUMNS)
        writer.writeheader()
        for row in sorted(existing.values(), key=lambda r: r["plant_name"]):
            writer.writerow(row)
    print(f"Updated {PLANT_LOCATIONS_FILE} ({len(existing)} plants)")


def get_vessel_track(token: str, mmsi: int, year: int, week: int):
    resp = requests.get(
        f"{BASE_URL}/fishhealth/vesseltrack/{mmsi}/{year}/{week}",
        headers={"Authorization": f"Bearer {token}"}
    )
    if resp.status_code == 204:
        return None
    resp.raise_for_status()
    return resp.json()


# --- Geofence logic ---

def check_plant_visits(track: dict, plants: list) -> list:
    visits = []

    for segment in track.get("vesselTracks", []):
        if segment.get("isNoSignal"):
            continue

        active_visits = {}

        for point in segment.get("points", []):
            lat = point.get("lat")
            lon = point.get("lon")
            t = point.get("msgt")

            if lat is None or lon is None or t is None:
                continue

            for plant in plants:
                dist = haversine(lat, lon, plant["lat"], plant["lon"])
                plant_id = plant["id"]

                if dist <= RADIUS_M:
                    if plant_id not in active_visits:
                        active_visits[plant_id] = {
                            "plant": plant,
                            "entry_time": t,
                            "last_seen": t
                        }
                    else:
                        active_visits[plant_id]["last_seen"] = t
                else:
                    if plant_id in active_visits:
                        v = active_visits.pop(plant_id)
                        visit = _close_visit(v)
                        if visit:
                            visits.append(visit)

        for v in active_visits.values():
            visit = _close_visit(v)
            if visit:
                visits.append(visit)

    return visits


def _close_visit(v: dict):
    entry = datetime.fromisoformat(v["entry_time"].replace("Z", "+00:00"))
    exit_ = datetime.fromisoformat(v["last_seen"].replace("Z", "+00:00"))
    duration_hrs = (exit_ - entry).total_seconds() / 3600

    if duration_hrs < MIN_VISIT_HOURS:
        return None

    plant = v["plant"]
    return {
        "plant_id": plant["id"],
        "plant_name": plant["name"],
        "plant_company": plant["company"],
        "approval_number": plant["approval_number"],
        "entry_time": v["entry_time"],
        "exit_time": v["last_seen"],
        "duration_hrs": round(duration_hrs, 2),
    }


# --- CSV output ---

def write_csv(visits: list, year: int, week: int) -> Path:
    DATA_DIR.mkdir(exist_ok=True)
    path = DATA_DIR / f"harvest_plant_visits_{year}_W{week:02d}.csv"

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(visits)

    print(f"Written {len(visits)} visits to {path}")
    return path


# --- Per-week run ---

def run_for_week(token: str, year: int, week: int, vessels: list) -> Path | None:
    print(f"\n=== Running for {year}/W{week:02d} ===")

    plants = get_slaughterhouses(token, year, week)
    update_plant_locations(plants)

    all_visits = []
    processed = 0

    for vessel in vessels:
        mmsi = vessel["mmsi"]
        name = vessel["name"]

        try:
            track = get_vessel_track(token, mmsi, year, week)
        except requests.HTTPError as e:
            print(f"  WARNING: track fetch failed for {mmsi} ({name}): {e}")
            processed += 1
            continue

        if not track:
            processed += 1
            continue

        visits = check_plant_visits(track, plants)

        for v in visits:
            v["mmsi"] = mmsi
            v["vessel_name"] = name
            v["vessel_type"] = vessel["vessel_type"]
            v["capacity"] = vessel["capacity"]
            v["capacity_unit"] = vessel["capacity_unit"]
            v["year"] = year
            v["week"] = week

        if visits:
            print(f"  {name} ({mmsi}) [{vessel['vessel_type']}]: {len(visits)} visit(s)")
            all_visits.extend(visits)

        processed += 1
        if processed % 25 == 0:
            print(f"  ...{processed}/{len(vessels)} vessels processed")

    print(f"Total visits detected for {year}/W{week:02d}: {len(all_visits)}")

    if all_visits:
        return write_csv(all_visits, year, week)
    print(f"No visits found for {year}/W{week:02d} — no CSV written.")
    return None


# --- Main ---

if __name__ == "__main__":
    token = get_token()
    print("Token OK")

    vessels = load_vessels()

    # Previous (fully completed) week: final and immutable once written, so
    # skip re-fetching it if we already have it — the file only needs to be
    # produced once, right after the week ends.
    prev_year, prev_week = get_previous_week()
    prev_path = DATA_DIR / f"harvest_plant_visits_{prev_year}_W{prev_week:02d}.csv"
    if prev_path.exists():
        print(f"\n{prev_path} already exists — previous week is final, skipping refetch.")
    else:
        run_for_week(token, prev_year, prev_week, vessels)

    # Current (in-progress) week: partial and grows daily, so always
    # re-fetch and overwrite — this is what lets the traffic report show a
    # "this week so far" line instead of only ever-completed weeks.
    cur_year, cur_week = get_current_week()
    run_for_week(token, cur_year, cur_week, vessels)
