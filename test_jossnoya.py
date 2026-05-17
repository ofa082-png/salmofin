"""
fetch_harvest_visits.py
-----------------------
Detects wellboat and slaughter boat visits to all Norwegian
fish slaughterhouses using Barentswatch fishhealth API.

Pipeline:
1. Fetch all active slaughterhouses for the week (with coordinates)
2. Fetch all wellboats + slaughter boats
3. For each vessel, fetch week track
4. Haversine check each ping against each plant
5. Reconstruct visits (entry/exit) and filter short ones
6. Upsert to Supabase

Run weekly via GitHub Actions for the previous completed week.
"""

import os
import math
import requests
from math import cos, radians, sin, atan2, sqrt
from datetime import datetime, timezone, timedelta

# --- Config ---
TOKEN_URL = "https://id.barentswatch.no/connect/token"
BASE_URL = "https://www.barentswatch.no/bwapi/v1/geodata"
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
BW_CLIENT_ID = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]

RADIUS_M = 300          # meters around plant coordinate
MIN_VISIT_HOURS = 1.0   # filter out short passbys
TABLE = "harvest_plant_visits"


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
    """Returns distance in meters between two coordinates."""
    R = 6371000
    φ1, φ2 = radians(lat1), radians(lat2)
    dφ = radians(lat2 - lat1)
    dλ = radians(lon2 - lon1)
    a = sin(dφ/2)**2 + cos(φ1)*cos(φ2)*sin(dλ/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))


def get_previous_week() -> tuple[int, int]:
    """Returns (year, week) for the most recently completed ISO week."""
    today = datetime.now(timezone.utc)
    last_week = today - timedelta(weeks=1)
    iso = last_week.isocalendar()
    return iso.year, iso.week


# --- Barentswatch API ---

def get_slaughterhouses(token: str, year: int, week: int) -> list:
    """Fetch all active fish slaughterhouses for a given week."""
    resp = requests.get(
        f"{BASE_URL}/fishslaughterhouses/{year}/{week}",
        headers={"Authorization": f"Bearer {token}"}
    )
    resp.raise_for_status()
    plants = resp.json()

    # Parse coordinates from GeoJSON geometry
    result = []
    for p in plants:
        coords = p.get("geometry", {}).get("coordinates", [])
        if len(coords) < 2:
            continue
        result.append({
            "id": p["id"],
            "name": p.get("establishment", "Unknown"),
            "company": p.get("company", "Unknown"),
            "approval_number": p.get("approvalNumber", ""),
            "lon": coords[0],  # GeoJSON is [lon, lat]
            "lat": coords[1],
        })

    print(f"Found {len(result)} active slaughterhouses for {year}/W{week}")
    return result


def get_vessels(token: str) -> list:
    """Fetch all wellboats and slaughter boats."""
    resp = requests.get(
        f"{BASE_URL}/fishhealth/vessels",
        headers={"Authorization": f"Bearer {token}"}
    )
    resp.raise_for_status()
    vessels = resp.json()
    relevant = [v for v in vessels if v.get("isWellboat") or v.get("isSlaughterBoat")]
    print(f"Found {len(vessels)} total vessels, {len(relevant)} wellboats/slaughter boats")
    return relevant


def get_vessel_track(token: str, mmsi: int, year: int, week: int) -> dict | None:
    """Fetch vessel track for a given week. Returns None if no data."""
    resp = requests.get(
        f"{BASE_URL}/fishhealth/vesseltrack/{mmsi}/{year}/{week}",
        headers={"Authorization": f"Bearer {token}"}
    )
    if resp.status_code == 204:
        return None
    resp.raise_for_status()
    return resp.json()


# --- Geofence logic ---

def check_plant_visits(track: dict, plants: list, radius_m: int, min_hours: float) -> list:
    """
    Check track points against all plant geofences.
    Returns completed visits filtered by minimum duration.
    """
    visits = []

    for segment in track.get("vesselTracks", []):
        if segment.get("isNoSignal"):
            continue

        # Track active visit per plant
        active_visits = {}  # plant_id -> {entry_time, last_seen, plant}

        for point in segment.get("points", []):
            lat = point.get("lat")
            lon = point.get("lon")
            t = point.get("msgt")

            if lat is None or lon is None or t is None:
                continue

            for plant in plants:
                dist = haversine(lat, lon, plant["lat"], plant["lon"])
                plant_id = plant["id"]

                if dist <= radius_m:
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
                        visit = _close_visit(v, min_hours)
                        if visit:
                            visits.append(visit)

        # Close any still-open visits at end of segment
        for plant_id, v in active_visits.items():
            visit = _close_visit(v, min_hours)
            if visit:
                visits.append(visit)

    return visits


def _close_visit(v: dict, min_hours: float) -> dict | None:
    """Close a visit and return it if it meets minimum duration."""
    entry = datetime.fromisoformat(v["entry_time"].replace("Z", "+00:00"))
    exit_ = datetime.fromisoformat(v["last_seen"].replace("Z", "+00:00"))
    duration_hrs = (exit_ - entry).total_seconds() / 3600

    if duration_hrs < min_hours:
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


# --- Supabase ---

def upsert_visits(visits: list, year: int, week: int) -> None:
    """Upsert visit records to Supabase."""
    if not visits:
        print("No visits to insert.")
        return

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    batch_size = 500
    total = len(visits)
    inserted = 0

    for i in range(0, total, batch_size):
        batch = visits[i:i + batch_size]
        resp = requests.post(url, json=batch, headers=headers)
        if resp.status_code not in (200, 201):
            print(f"  ERROR batch {i}: {resp.status_code} {resp.text[:200]}")
        else:
            inserted += len(batch)
            print(f"  Upserted {inserted}/{total} visits")

    print(f"Done. {inserted} visits upserted to '{TABLE}'")


# --- Main ---

if __name__ == "__main__":
    year, week = get_previous_week()
    print(f"Running for {year}/W{week}\n")

    token = get_token()
    print("Token OK\n")

    plants = get_slaughterhouses(token, year, week)
    vessels = get_vessels(token)

    all_visits = []
    processed = 0

    for vessel in vessels:
        mmsi = vessel["mmsi"]
        name = vessel.get("vesselName", "Unknown")
        is_wellboat = vessel.get("isWellboat", False)
        is_slaughter = vessel.get("isSlaughterBoat", False)

        track = get_vessel_track(token, mmsi, year, week)
        if not track:
            continue

        visits = check_plant_visits(track, plants, RADIUS_M, MIN_VISIT_HOURS)

        for v in visits:
            v["mmsi"] = mmsi
            v["vessel_name"] = name
            v["is_wellboat"] = is_wellboat
            v["is_slaughter_boat"] = is_slaughter
            v["year"] = year
            v["week"] = week

        if visits:
            print(f"  {name} ({mmsi}): {len(visits)} visit(s) detected")
            all_visits.extend(visits)

        processed += 1
        if processed % 25 == 0:
            print(f"  ...{processed}/{len(vessels)} vessels processed")

    print(f"\nTotal visits detected: {len(all_visits)}")
    upsert_visits(all_visits, year, week)
