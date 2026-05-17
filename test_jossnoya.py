"""
test_jossnoya_v2.py
-------------------
Uses the Barentswatch fishhealth vesseltrack API to detect
wellboat and slaughter boat visits to Mowi Jøsnøya harvest plant.

Approach:
1. Fetch all wellboats + slaughter boats from /vessels
2. For each vessel, fetch vesseltrack for given year/week
3. Run geofence check on track points against plant coordinates
4. Log visits (entry/exit time, vessel name, type)
"""

import os
import requests
from math import cos, radians, sin, atan2, sqrt
from datetime import datetime, timezone

TOKEN_URL = "https://id.barentswatch.no/connect/token"
BASE_URL = "https://www.barentswatch.no/bwapi/v1/geodata/fishhealth"
BW_CLIENT_ID = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]

# Mowi Jøsnøya harvest plant
PLANTS = [
    {"id": "mowi_jossnoya", "name": "Mowi Jøsnøya", "lat": 63.5085, "lon": 9.0719}
]
RADIUS_M = 1000

# Query week — change these to test different weeks
YEAR = 2026
WEEK = 19  # week 19 = early May 2026


def get_token() -> str:
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": BW_CLIENT_ID,
        "client_secret": BW_CLIENT_SECRET,
        "scope": "api"
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def haversine(lat1, lon1, lat2, lon2) -> float:
    """Returns distance in meters between two coordinates."""
    R = 6371000
    φ1, φ2 = radians(lat1), radians(lat2)
    dφ = radians(lat2 - lat1)
    dλ = radians(lon2 - lon1)
    a = sin(dφ/2)**2 + cos(φ1)*cos(φ2)*sin(dλ/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1-a))


def get_vessels(token: str) -> list:
    """Fetch all wellboats and slaughter boats."""
    resp = requests.get(
        f"{BASE_URL}/vessels",
        headers={"Authorization": f"Bearer {token}"}
    )
    resp.raise_for_status()
    vessels = resp.json()
    relevant = [v for v in vessels if v.get("isWellboat") or v.get("isSlaughterBoat")]
    print(f"Found {len(vessels)} total vessels, {len(relevant)} wellboats/slaughter boats")
    return relevant


def get_vessel_track(token: str, mmsi: int, year: int, week: int) -> dict | None:
    """Fetch vessel track for a given week."""
    resp = requests.get(
        f"{BASE_URL}/vesseltrack/{mmsi}/{year}/{week}",
        headers={"Authorization": f"Bearer {token}"}
    )
    if resp.status_code == 204:
        return None  # no data for this vessel/week
    resp.raise_for_status()
    return resp.json()


def check_plant_visits(track: dict, plants: list, radius_m: int) -> list:
    """
    Check if any track points fall within radius of plant coordinates.
    Returns list of visit events with entry/exit times.
    """
    visits = []

    for segment in track.get("vesselTracks", []):
        if segment.get("isNoSignal"):
            continue

        active_visit = None

        for point in segment.get("points", []):
            lat = point.get("lat")
            lon = point.get("lon")
            t = point.get("msgt")

            if lat is None or lon is None:
                continue

            for plant in plants:
                dist = haversine(lat, lon, plant["lat"], plant["lon"])

                if dist <= radius_m:
                    if active_visit is None or active_visit["plant_id"] != plant["id"]:
                        # Entry event
                        active_visit = {
                            "plant_id": plant["id"],
                            "plant_name": plant["name"],
                            "entry_time": t,
                            "last_seen": t
                        }
                    else:
                        active_visit["last_seen"] = t
                else:
                    if active_visit and active_visit["plant_id"] == plant["id"]:
                        # Exit event — close the visit
                        visits.append({**active_visit, "exit_time": active_visit["last_seen"]})
                        active_visit = None

        # Close any open visit at end of segment
        if active_visit:
            visits.append({**active_visit, "exit_time": active_visit["last_seen"]})

    return visits


if __name__ == "__main__":
    token = get_token()
    print(f"Token OK\n")

    vessels = get_vessels(token)

    all_visits = []

    for vessel in vessels:
        mmsi = vessel["mmsi"]
        name = vessel.get("vesselName", "Unknown")
        is_wellboat = vessel.get("isWellboat", False)
        is_slaughter = vessel.get("isSlaughterBoat", False)

        track = get_vessel_track(token, mmsi, YEAR, WEEK)
        if not track:
            continue

        visits = check_plant_visits(track, PLANTS, RADIUS_M)

        if visits:
            print(f"\n{'='*50}")
            print(f"VESSEL: {name} (MMSI: {mmsi})")
            print(f"  Wellboat: {is_wellboat} | SlaughterBoat: {is_slaughter}")
            for v in visits:
                print(f"  VISIT at {v['plant_name']}")
                print(f"    Entry: {v['entry_time']}")
                print(f"    Exit:  {v['exit_time']}")
            all_visits.extend(visits)

    print(f"\n{'='*50}")
    print(f"Total visits detected at harvest plants: {len(all_visits)}")
    if not all_visits:
        print("No visits detected — try a different week or check plant coordinates")
