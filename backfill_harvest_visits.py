"""
backfill_harvest_visits.py
--------------------------
Backfills harvest plant vessel visit data for a full year.
Uses vessel_categories.csv as source of truth for vessel selection.
Writes one CSV per week to data/, skipping weeks that already exist.

Usage:
    python backfill_harvest_visits.py --year 2025
    python backfill_harvest_visits.py --year 2024

Requires env vars:
    BW_CLIENT_ID
    BW_CLIENT_SECRET
"""

import os
import csv
import time
import argparse
from math import radians, cos, sin, atan2, sqrt
from datetime import datetime, timezone
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
REQUEST_DELAY = 0.15

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


def weeks_in_year(year: int) -> int:
    return datetime(year, 12, 28).isocalendar().week


def all_weeks(year: int) -> list:
    return list(range(1, weeks_in_year(year) + 1))


def csv_path(year: int, week: int) -> Path:
    return DATA_DIR / f"harvest_plant_visits_{year}_W{week:02d}.csv"


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

    print(f"Loaded {len(vessels)} vessels "
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
    return result


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
    path = csv_path(year, week)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(visits)

    return path


# --- Per-week processing ---

def process_week(token: str, vessels: list, year: int, week: int) -> int:
    plants = get_slaughterhouses(token, year, week)
    if not plants:
        print(f"  W{week:02d}: no plants found, skipping")
        return 0

    all_visits = []

    for vessel in vessels:
        mmsi = vessel["mmsi"]
        name = vessel["name"]

        try:
            track = get_vessel_track(token, mmsi, year, week)
        except requests.HTTPError as e:
            print(f"    WARNING: track fetch failed for {mmsi} ({name}): {e}")
            time.sleep(REQUEST_DELAY)
            continue

        time.sleep(REQUEST_DELAY)

        if not track:
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

        all_visits.extend(visits)

    write_csv(all_visits, year, week)
    print(f"  W{week:02d}: {len(all_visits)} visits → {csv_path(year, week).name}")
    return len(all_visits)


# --- Main ---

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill harvest plant visits for a full year")
    parser.add_argument("--year", type=int, required=True, help="Year to backfill (e.g. 2025)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing CSV files")
    args = parser.parse_args()

    year = args.year
    weeks = all_weeks(year)

    # Don't go beyond last completed week if backfilling current year
    current_iso = datetime.now(timezone.utc).isocalendar()
    if year == current_iso.year:
        weeks = [w for w in weeks if w < current_iso.week]

    # Skip weeks that already have a CSV unless --overwrite
    if args.overwrite:
        weeks_to_run = weeks
    else:
        weeks_to_run = [w for w in weeks if not csv_path(year, w).exists()]

    skipped = len(weeks) - len(weeks_to_run)
    print(f"\nBackfilling {year}: {len(weeks_to_run)} weeks to fetch ({skipped} already exist)\n")

    if not weeks_to_run:
        print("Nothing to do. Use --overwrite to re-run existing weeks.")
        exit(0)

    vessels = load_vessels()
    print()

    total_visits = 0
    for i, week in enumerate(weeks_to_run, 1):
        print(f"[{i}/{len(weeks_to_run)}] {year}/W{week:02d}")
        token = get_token()
        total_visits += process_week(token, vessels, year, week)

    print(f"\nDone. {total_visits} total visits written across {len(weeks_to_run)} weeks.")
