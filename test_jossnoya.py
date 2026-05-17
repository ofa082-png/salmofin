"""
test_jøsnøya_geofence.py
------------------------
Fetches all vessels seen within ~150m of Mowi Jøsnøya
over the last 7 days, then pulls their tracks to inspect
what's actually visiting.
"""

import os
import requests
from math import cos, radians
from datetime import datetime, timedelta, timezone

TOKEN_URL = "https://id.barentswatch.no/connect/token"
BW_CLIENT_ID = os.environ["BW_CLIENT_ID"]
BW_CLIENT_SECRET = os.environ["BW_CLIENT_SECRET"]

# Mowi Jøsnøya coordinates
PLANT_LAT = 63.5085
PLANT_LON = 9.0719
RADIUS_M = 150

def get_token() -> str:
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "client_credentials",
        "client_id": BW_CLIENT_ID,
        "client_secret": BW_CLIENT_SECRET,
        "scope": "api"  # note: ais scope, not api
    })
    resp.raise_for_status()
    return resp.json()["access_token"]

def bbox_polygon(lat, lon, meters=150):
    dlat = meters / 111320
    dlon = meters / (111320 * cos(radians(lat)))
    return {
        "type": "Polygon",
        "coordinates": [[
            [lon - dlon, lat - dlat],
            [lon + dlon, lat - dlat],
            [lon + dlon, lat + dlat],
            [lon - dlon, lat + dlat],
            [lon - dlon, lat - dlat]
        ]]
    }

def get_mmsi_in_area(token, polygon, date_from, date_to):
    resp = requests.post(
        "https://historic.ais.barentswatch.no/v1/historic/mmsiinarea",
        headers={"Authorization": f"Bearer {token}"},
        json={
            "msgtimefrom": date_from,
            "msgtimeto": date_to,
            "polygon": polygon
        }
    )
    resp.raise_for_status()
    return resp.json()

def get_track(token, mmsi, date_from, date_to):
    resp = requests.get(
        f"https://historic.ais.barentswatch.no/v1/historic/tracks/{mmsi}/{date_from}/{date_to}",
        headers={"Authorization": f"Bearer {token}"}
    )
    resp.raise_for_status()
    return resp.json()

if __name__ == "__main__":
    token = get_token()

    now = datetime.now(timezone.utc)
    date_to = now.strftime("%Y-%m-%dT%H:%M:%S+00:00")
    date_from = (now - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S+00:00")

    polygon = bbox_polygon(PLANT_LAT, PLANT_LON, RADIUS_M)
    print(f"Querying {date_from} → {date_to}")
    print(f"Polygon: {polygon}")

    mmsi_list = get_mmsi_in_area(token, polygon, date_from, date_to)
    print(f"\nFound {len(mmsi_list)} vessels in area: {mmsi_list}")

    for mmsi in mmsi_list:
        track = get_track(token, mmsi, date_from, date_to)
        if track:
            # Just grab first ping for vessel info
            first = track[0]
            print(f"\nMMSI: {mmsi}")
            print(f"  Name:     {first.get('name')}")
            print(f"  ShipType: {first.get('shipType')}")
            print(f"  Pings:    {len(track)}")
            print(f"  First:    {first.get('msgtime')}")
            print(f"  Last:     {track[-1].get('msgtime')}")
