"""
generate_big_vessel_tracker.py
--------------------------------
Standalone route tracker for "big vessels" (>=400t capacity, from
vessel_categories.csv). For each vessel, over the last LOOKBACK_WEEKS
weeks: a chronological itinerary of locality stops (vessel_visits +
localities, both BigQuery) and harvest-plant stops (harvest_plant_visits
CSVs + data/plant_locations.csv), rendered as a connect-the-dots map
(straight lines between consecutive real-coordinate stops — not the
vessel's actual continuous AIS track, which isn't persisted anywhere)
plus a table.

Writes docs/big_vessels.html.
"""

import os
import csv
import json
import glob
import datetime
from collections import defaultdict
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "salmofin"
BASE_DIR = os.path.dirname(__file__)
OUT_PATH = os.path.join(BASE_DIR, "docs", "big_vessels.html")
FLEET_CSV = os.path.join(BASE_DIR, "vessel_categories.csv")
PLANT_LOCATIONS_CSV = os.path.join(BASE_DIR, "data", "plant_locations.csv")

BIG_VESSEL_MIN_TONNES = 400
LOOKBACK_WEEKS = 8

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def to_tonnes(capacity, unit):
    try:
        cap = float(capacity)
    except (TypeError, ValueError):
        return 0.0
    return cap * 0.1 if unit == "m3" else cap

def load_big_vessels():
    vessels = []
    with open(FLEET_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mmsi = (row.get("MMSI") or "").strip()
            if not mmsi.isdigit():
                continue
            cap_t = to_tonnes(row.get("LAST-KAP"), (row.get("ENHET") or "").strip())
            if cap_t < BIG_VESSEL_MIN_TONNES:
                continue
            vessels.append({
                "mmsi": int(mmsi),
                "name": (row.get("Navn") or "Ukjent").strip(),
                "type": (row.get("Type") or "").strip(),
                "capacity_t": round(cap_t),
            })
    vessels.sort(key=lambda v: -v["capacity_t"])
    return vessels

def load_plant_locations():
    locs = {}
    if not os.path.exists(PLANT_LOCATIONS_CSV):
        return locs
    with open(PLANT_LOCATIONS_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                locs[row["plant_id"]] = {
                    "name": row["plant_name"],
                    "company": row["plant_company"],
                    "lat": float(row["lat"]),
                    "lon": float(row["lon"]),
                }
            except (KeyError, ValueError):
                continue
    return locs

def fetch_locality_stops(client, mmsi_list, days_back):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("mmsi_list", "INT64", mmsi_list)]
    )
    rows = list(client.query(f"""
        SELECT v.mmsi, v.localityNo, v.startTime, v.stopTime,
               l.name AS locality_name, l.latitude, l.longitude
        FROM `salmofin.salmofin.vessel_visits` v
        LEFT JOIN `salmofin.salmofin.localities` l ON v.localityNo = l.siteNr
        WHERE v.mmsi IN UNNEST(@mmsi_list)
          AND DATE(v.startTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
        ORDER BY v.mmsi, v.startTime
    """, job_config=job_config).result())
    return rows

def all_plant_csvs():
    return sorted(glob.glob(os.path.join(BASE_DIR, "data", "harvest_plant_visits_*.csv")))

def fetch_plant_stops(mmsi_set, n_weeks):
    files = all_plant_csvs()[-n_weeks:]
    rows = []
    for path in files:
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if not row.get("mmsi"):
                    continue
                if int(row["mmsi"]) not in mmsi_set:
                    continue
                rows.append(row)
    return rows

def build_itineraries(vessels, locality_rows, plant_rows, plant_locations):
    by_mmsi = defaultdict(list)

    for r in locality_rows:
        if r.latitude is None or r.longitude is None:
            continue
        by_mmsi[r.mmsi].append({
            "type": "locality",
            "name": r.locality_name or f"Lokalitet {r.localityNo}",
            "lat": r.latitude,
            "lon": r.longitude,
            "start": r.startTime.isoformat(),
            "end": r.stopTime.isoformat() if r.stopTime else None,
        })

    for r in plant_rows:
        loc = plant_locations.get(r["plant_id"])
        if not loc:
            continue
        by_mmsi[int(r["mmsi"])].append({
            "type": "plant",
            "name": r["plant_name"].title(),
            "company": r["plant_company"].title(),
            "lat": loc["lat"],
            "lon": loc["lon"],
            "start": r["entry_time"],
            "end": r["exit_time"],
        })

    itineraries = {}
    for v in vessels:
        stops = sorted(by_mmsi.get(v["mmsi"], []), key=lambda s: s["start"])
        itineraries[str(v["mmsi"])] = stops
    return itineraries

def build_vessel_options(vessels, itineraries):
    return "".join(
        f'<option value="{v["mmsi"]}">{v["name"]} — {v["type"]} ({v["capacity_t"]}t)'
        f'{"" if itineraries.get(str(v["mmsi"])) else " · ingen data"}</option>'
        for v in vessels
    )

TEMPLATE = """<!doctype html>
<html lang="no">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Store fartøy — rutetracker</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
<style>
  :root {{ --surface-1:#f5f4f0; --surface-2:#ffffff; --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#898781; --border:#e1e0d9; --accent:#2a78d6; --accent2:#d68a2a; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --surface-1:#242422; --surface-2:#1a1a19; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#898781; --border:#2c2c2a; }}
  }}
  body {{ background:var(--surface-1); color:var(--text-primary); font-family:-apple-system,Segoe UI,Roboto,sans-serif; margin:0; padding:2rem 1rem; }}
  .wrap {{ max-width:760px; margin:0 auto; }}
  table {{ border-collapse:collapse; width:100%; }}
  a {{ color:var(--text-secondary); }}
  select {{ font-size:14px; padding:8px 10px; border-radius:8px; border:0.5px solid var(--border); background:var(--surface-2); color:var(--text-primary); width:100%; margin-bottom:12px; }}
  #map {{ width:100%; height:360px; border-radius:8px; border:0.5px solid var(--border); margin-bottom:1rem; }}
  .legend {{ display:flex; gap:16px; font-size:12px; color:var(--text-secondary); margin-bottom:1rem; }}
  .dot {{ display:inline-block; width:9px; height:9px; border-radius:50%; margin-right:5px; }}
</style>
</head>
<body>
<div class="wrap">
  <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:1.25rem;">
    <div>
      <div style="font-size:18px;font-weight:500;">Store fartøy — rutetracker</div>
      <div style="font-size:13px;color:var(--text-muted)">Fartøy ≥{min_tonnes}t · siste {lookback_weeks} uker · oppdatert {updated}</div>
    </div>
    <a href="traffic.html" style="font-size:11px;border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">trafikk →</a>
  </div>

  <select id="vesselSelect">{vessel_options}</select>

  <div class="legend">
    <span><span class="dot" style="background:var(--accent);"></span>Lokalitet</span>
    <span><span class="dot" style="background:var(--accent2);"></span>Slakteri</span>
  </div>

  <div id="map"></div>

  <div style="font-size:12px;color:var(--text-muted);margin-bottom:8px;">Rekkefølge av anløp, eldste øverst. Rette linjer mellom faktiske stopp — ikke fartøyets fulle AIS-spor.</div>
  <div style="border:0.5px solid var(--border);border-radius:8px;overflow:hidden;overflow-x:auto;">
    <table style="font-size:13px;table-layout:fixed;">
      <thead>
      <tr style="background:var(--surface-2);">
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Type</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Sted</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Fra</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Til</td>
      </tr>
      </thead>
      <tbody id="itineraryRows"></tbody>
    </table>
  </div>

  <div style="font-size:11px;color:var(--text-muted);border-top:0.5px solid var(--border);padding-top:12px;margin-top:1.5rem;">
    Lokalitetsanløp: BarentsWatch AIS (vessel_visits). Slakterianløp: BarentsWatch fiskehelse (harvest_plant_visits). Kart: OpenStreetMap. Via salmofin BigQuery-pipeline.
  </div>
</div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script>
const ITINERARIES = {itineraries_json};

const map = L.map('map').setView([65.0, 12.0], 4);
L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
  attribution: '&copy; OpenStreetMap',
  maxZoom: 18,
}}).addTo(map);

let layerGroup = L.layerGroup().addTo(map);

function fmt(iso) {{
  if (!iso) return '';
  const d = new Date(iso);
  return d.toLocaleDateString('no-NO', {{ day: '2-digit', month: '2-digit' }}) + ' ' +
         d.toLocaleTimeString('no-NO', {{ hour: '2-digit', minute: '2-digit' }});
}}

function showVessel(mmsi) {{
  layerGroup.clearLayers();
  const stops = ITINERARIES[mmsi] || [];
  const rowsEl = document.getElementById('itineraryRows');

  if (!stops.length) {{
    rowsEl.innerHTML = '<tr><td colspan="4" style="padding:14px 10px;color:var(--text-muted);text-align:center;">Ingen anløp registrert i perioden.</td></tr>';
    return;
  }}

  rowsEl.innerHTML = stops.map(s => `
    <tr style="border-top:0.5px solid var(--border);">
      <td style="padding:8px 10px;">${{s.type === 'plant' ? 'Slakteri' : 'Lokalitet'}}</td>
      <td style="padding:8px 10px;">${{s.name}}</td>
      <td style="padding:8px 10px;">${{fmt(s.start)}}</td>
      <td style="padding:8px 10px;">${{fmt(s.end)}}</td>
    </tr>`).join('');

  const latlngs = stops.map(s => [s.lat, s.lon]);
  L.polyline(latlngs, {{ color: '#7a4fc9', weight: 2, opacity: 0.6, dashArray: '4,4' }}).addTo(layerGroup);

  stops.forEach((s, i) => {{
    const color = s.type === 'plant' ? '#d68a2a' : '#2a78d6';
    L.circleMarker([s.lat, s.lon], {{ radius: 6, color, fillColor: color, fillOpacity: 0.85, weight: 1 }})
      .bindPopup(`<b>${{s.name}}</b><br>${{fmt(s.start)}} → ${{fmt(s.end)}}`)
      .addTo(layerGroup);
  }});

  const bounds = L.latLngBounds(latlngs);
  map.fitBounds(bounds.pad(0.2));
}}

document.getElementById('vesselSelect').addEventListener('change', (e) => showVessel(e.target.value));
// Object.keys() on integer-like mmsi keys reorders them numerically, not by
// insertion order, so pick the default by walking the <select> options
// instead — that preserves the server-side capacity-descending sort.
const options = [...document.getElementById('vesselSelect').options];
const defaultOption = options.find(o => (ITINERARIES[o.value] || []).length > 0) || options[0];
if (defaultOption) {{
  document.getElementById('vesselSelect').value = defaultOption.value;
  showVessel(defaultOption.value);
}}
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("Loading big vessels (>=%dt)..." % BIG_VESSEL_MIN_TONNES)
    vessels = load_big_vessels()
    print(f"  {len(vessels)} big vessels")

    plant_locations = load_plant_locations()
    print(f"  {len(plant_locations)} plant locations known")

    days_back = LOOKBACK_WEEKS * 7
    client = get_bq_client()
    mmsi_list = [v["mmsi"] for v in vessels]
    locality_rows = fetch_locality_stops(client, mmsi_list, days_back)
    print(f"  {len(locality_rows)} locality stop rows fetched")

    mmsi_set = set(mmsi_list)
    plant_rows = fetch_plant_stops(mmsi_set, LOOKBACK_WEEKS + 1)  # +1 to include current partial week file
    print(f"  {len(plant_rows)} plant stop rows fetched")

    itineraries = build_itineraries(vessels, locality_rows, plant_rows, plant_locations)
    vessel_options = build_vessel_options(vessels, itineraries)

    now = datetime.datetime.now(datetime.timezone.utc)
    html = TEMPLATE.format(
        min_tonnes=BIG_VESSEL_MIN_TONNES,
        lookback_weeks=LOOKBACK_WEEKS,
        updated=now.strftime("%d.%m.%Y %H:%M UTC"),
        vessel_options=vessel_options,
        itineraries_json=json.dumps(itineraries),
    )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
    for v in vessels:
        n = len(itineraries.get(str(v["mmsi"]), []))
        print(f"  {v['name']} ({v['type']}, {v['capacity_t']}t): {n} stops")
