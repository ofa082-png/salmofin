"""
generate_report.py
-------------------
Renders a static HTML fish-health report from BigQuery data and writes
it to docs/fiskehelse.html, for GitHub Pages to serve. Nightly script.

Moved off docs/index.html (2026-08-16) — that path is now the hub
frontpage (see generate_hub.py), which links here instead of this
page being the site root.

Fiskehelseindikator section (silage/feed vessel visit ratio, a
mortality proxy) moved in from generate_foring.py on 2026-08-17 — it's
a fish-health signal, not feed-logistics, so it belongs here instead.
"""

import os
import csv
import json
import datetime
from collections import defaultdict
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "salmofin"
OUT_PATH   = os.path.join(os.path.dirname(__file__), "docs", "fiskehelse.html")
FLEET_CSV  = os.path.join(os.path.dirname(__file__), "vessel_categories.csv")
INDICATOR_WEEKS_HISTORY = 10

STATUS_LABEL = {
    "PANKREASSYKDOM": "PD",
    "INFEKSIOES_LAKSEANEMI": "ILA",
    "BAKTERIELL_NYRESYKE": "BKD",
    "FRANCISELLOSE": "Francisellose",
}
DISEASE_COLOR = {
    "PANKREASSYKDOM": "#2a78d6",
    "INFEKSIOES_LAKSEANEMI": "#1baf7a",
    "BAKTERIELL_NYRESYKE": "#eda100",
    "FRANCISELLOSE": "#008300",
}

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def load_feed_silage_fleet():
    """MMSI -> vessel type, restricted to Fish feed carrier / Silage —
    only used for the Fiskehelseindikator chart below."""
    mmsi_to_type = {}
    with open(FLEET_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mmsi = (row.get("MMSI") or "").strip()
            vtype = (row.get("Type") or "").strip()
            if mmsi.isdigit() and vtype in ("Fish feed carrier", "Silage"):
                mmsi_to_type[int(mmsi)] = vtype
    return mmsi_to_type

def fetch_feed_silage_visits(client, mmsi_list, days_back):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("mmsi_list", "INT64", mmsi_list)]
    )
    return list(client.query(f"""
        SELECT DATE(startTime) AS visit_date, mmsi
        FROM salmofin.salmofin.vessel_visits
        WHERE DATE(startTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
          AND DATE(startTime) < CURRENT_DATE()
          AND mmsi IN UNNEST(@mmsi_list)
    """, job_config=job_config).result())

def monday_of(d):
    return d - datetime.timedelta(days=d.weekday())

def week_total(daily, monday, end_date):
    total = 0
    d = monday
    while d <= end_date:
        total += daily.get(d, 0)
        d += datetime.timedelta(days=1)
    return total

def build_weekly_series(daily, current_monday, weeks_history, yesterday):
    series = []
    for i in range(weeks_history - 1, -1, -1):
        m = current_monday - datetime.timedelta(weeks=i)
        end = yesterday if m == current_monday else m + datetime.timedelta(days=6)
        total = week_total(daily, m, end) if end >= m else 0
        series.append((f"U{m.isocalendar()[1]}", total))
    return series

def fetch_fiskehelseindikator(client):
    """Silage/feed vessel visit ratio — a mortality proxy (silage
    vessels collect dead fish/offal, so more silage activity relative
    to normal feed-carrier activity is a fairly direct operational
    readout of mortality). Moved here from generate_foring.py
    (2026-08-17) — belongs with the rest of the fish-health content,
    not bundled under the feed-logistics report."""
    mmsi_to_type = load_feed_silage_fleet()
    days_back = INDICATOR_WEEKS_HISTORY * 7 + 7
    rows = fetch_feed_silage_visits(client, list(mmsi_to_type.keys()), days_back)

    daily_by_type = defaultdict(lambda: defaultdict(int))
    for r in rows:
        vtype = mmsi_to_type.get(r.mmsi)
        if vtype:
            daily_by_type[vtype][r.visit_date] += 1

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    current_monday = monday_of(yesterday)

    feed_weekly = build_weekly_series(daily_by_type.get("Fish feed carrier", {}), current_monday, INDICATOR_WEEKS_HISTORY, yesterday)
    silage_weekly = build_weekly_series(daily_by_type.get("Silage", {}), current_monday, INDICATOR_WEEKS_HISTORY, yesterday)

    labels = [w[0] for w in feed_weekly]
    ratio_values = [
        round(s[1] / f[1], 3) if f[1] else None
        for f, s in zip(feed_weekly, silage_weekly)
    ]
    return labels, ratio_values

def fetch_data(client):
    lice_trend = list(client.query("""
        SELECT Uke, ROUND(AVG(Voksne_hunnlus),4) AS avg_lice
        FROM salmofin.salmofin.lice_bw
        WHERE Ar = EXTRACT(YEAR FROM CURRENT_DATE())
          AND Uke BETWEEN EXTRACT(ISOWEEK FROM CURRENT_DATE()) - 11
                       AND EXTRACT(ISOWEEK FROM CURRENT_DATE())
          AND Voksne_hunnlus IS NOT NULL
        GROUP BY Uke ORDER BY Uke
    """).result())

    kpis = list(client.query("""
        SELECT
          (SELECT COUNT(*) FROM salmofin.salmofin.mattilsynet_helsestatus) AS active_cases,
          (SELECT COUNT(DISTINCT lokalitetsnummer) FROM salmofin.salmofin.mattilsynet_helsestatus) AS active_localities,
          (SELECT COUNT(DISTINCT id) FROM salmofin.salmofin.mattilsynet_disease
             WHERE opprettet >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)) AS new_cases_14d,
          (SELECT COUNT(*) FROM salmofin.salmofin.treatments
             WHERE Ar = EXTRACT(YEAR FROM CURRENT_DATE())
               AND Uke IN (EXTRACT(ISOWEEK FROM CURRENT_DATE()) - 1, EXTRACT(ISOWEEK FROM CURRENT_DATE()))) AS treatments_14d
    """).result())[0]

    recent = list(client.query("""
        SELECT lokalitetsnummer, lokalitetsnavn, sykdomstype,
          CASE WHEN avslutningsdato IS NOT NULL THEN 'Avsluttet'
               WHEN diagnosedato IS NOT NULL THEN 'Bekreftet'
               ELSE 'Mistanke' END AS status,
          COALESCE(avslutningsdato, diagnosedato, kvalitetssikretMistankedato, varslingsdato, opprettet) AS status_date
        FROM salmofin.salmofin.mattilsynet_disease
        WHERE COALESCE(avslutningsdato, diagnosedato, kvalitetssikretMistankedato, varslingsdato, opprettet)
              >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
        ORDER BY status_date DESC
        LIMIT 15
    """).result())

    map_rows = list(client.query("""
        SELECT h.lokalitetsnummer, h.lokalitetsnavn, h.sykdomstype, l.latitude, l.longitude
        FROM salmofin.salmofin.mattilsynet_helsestatus h
        LEFT JOIN salmofin.salmofin.localities l ON h.lokalitetsnummer = l.siteNr
        WHERE l.latitude IS NOT NULL
    """).result())

    return lice_trend, kpis, recent, map_rows

def build_sites_json(map_rows):
    by_site = {}
    for r in map_rows:
        s = by_site.setdefault(r.lokalitetsnummer, {
            "id": str(r.lokalitetsnummer), "name": r.lokalitetsnavn.title(),
            "lat": r.latitude, "lon": r.longitude, "diseases": []
        })
        s["diseases"].append(r.sykdomstype)
    return list(by_site.values())

def build_table_rows(recent):
    status_bg = {"Mistanke": "#fac775", "Bekreftet": "#f7c1c1", "Avsluttet": "#c0dd97"}
    status_fg = {"Mistanke": "#633806", "Bekreftet": "#791f1f", "Avsluttet": "#27500a"}
    rows = []
    for r in recent:
        label = STATUS_LABEL.get(r.sykdomstype, r.sykdomstype)
        date_str = r.status_date.strftime("%d.%m") if r.status_date else ""
        rows.append(f"""
      <tr style="border-top:0.5px solid var(--border);">
        <td style="padding:8px 10px;">{r.lokalitetsnavn.title()}</td>
        <td style="padding:8px 10px;">{label}</td>
        <td style="padding:8px 10px;"><span style="background:{status_bg[r.status]};color:{status_fg[r.status]};font-size:11px;padding:2px 8px;border-radius:4px;">{r.status}</span></td>
        <td style="padding:8px 10px;text-align:right;color:var(--text-secondary);">{date_str}</td>
      </tr>""")
    return "".join(rows)

TEMPLATE = """<!doctype html>
<html lang="no">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fiskehelse — ukesrapport</title>
<style>
  :root {{ --surface-1:#f5f4f0; --surface-2:#ffffff; --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#898781; --border:#e1e0d9; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --surface-1:#242422; --surface-2:#1a1a19; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#898781; --border:#2c2c2a; }}
  }}
  body {{ background:var(--surface-1); color:var(--text-primary); font-family:-apple-system,Segoe UI,Roboto,sans-serif; margin:0; padding:2rem 1rem; }}
  .wrap {{ max-width:680px; margin:0 auto; }}
  table {{ border-collapse:collapse; }}
  a {{ color:var(--text-secondary); }}
</style>
</head>
<body>
<div class="wrap">
  <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:1.25rem;">
    <div>
      <div style="font-size:18px;font-weight:500;">Fiskehelse — ukesrapport</div>
      <div style="font-size:13px;color:var(--text-muted)">Uke {week}, {year} · oppdatert {updated}</div>
    </div>
    <div style="display:flex;gap:8px;align-items:baseline;">
      <a href="index.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">hjem →</a>
      <a href="traffic.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">trafikk →</a>
      <a href="foring.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">fôring →</a>
      <div style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;">kilde: mattilsynet.io</div>
    </div>
  </div>

  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:1.5rem;">
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Aktive sykdomstilfeller</div>
      <div style="font-size:24px;font-weight:500;">{active_cases}</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Nye siste 14 dager</div>
      <div style="font-size:24px;font-weight:500;">{new_cases_14d}</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Voksne hunnlus, snitt</div>
      <div style="font-size:24px;font-weight:500;">{avg_lice_latest}</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Behandlinger siste 14 dager</div>
      <div style="font-size:24px;font-weight:500;">{treatments_14d}</div>
    </div>
  </div>

  <div style="font-size:16px;font-weight:500;margin-bottom:8px;">Lusenivå, siste 12 uker</div>
  <div style="position:relative;width:100%;height:140px;margin-bottom:1.75rem;">
    <canvas id="liceChart" width="640" height="140"></canvas>
  </div>

  <div style="font-size:16px;font-weight:500;margin-bottom:2px;">Fiskehelseindikator</div>
  <div style="font-size:12px;color:var(--text-muted);margin-bottom:10px;">Egenutviklet indikator basert på vessel-trafikkmønstre (ensilasje/fôr-anløpsforhold). Stigende verdier kan tyde på økt dødelighet eller helseutfordringer på anleggene.</div>
  <div style="position:relative;width:100%;height:140px;margin-bottom:4px;">
    <canvas id="fishHealthChart" width="640" height="140"></canvas>
  </div>
  <div style="font-size:11px;color:var(--text-muted);margin-bottom:1.75rem;">Siste punkt er inneværende uke (delvis).</div>

  <div style="font-size:16px;font-weight:500;margin-bottom:8px;">Nye og pågående sykdomstilfeller, siste 14 dager</div>
  <div style="border:0.5px solid var(--border);border-radius:8px;overflow:hidden;margin-bottom:1.75rem;">
    <table style="width:100%;font-size:13px;table-layout:fixed;">
      <tr style="background:var(--surface-2);">
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Lokalitet</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Sykdom</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Status</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Dato</td>
      </tr>
      {table_rows}
    </table>
  </div>

  <div style="font-size:16px;font-weight:500;margin-bottom:8px;">Kart over aktive tilfeller</div>
  <div style="display:flex;flex-wrap:wrap;gap:16px;margin-bottom:8px;font-size:12px;color:var(--text-secondary);">
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:50%;background:#2a78d6;"></span>PD</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:50%;background:#1baf7a;"></span>ILA</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:50%;background:#eda100;"></span>BKD</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:50%;background:#008300;"></span>Francisellose</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:10px;height:10px;border-radius:50%;background:#4a3aa7;"></span>Flere</span>
  </div>
  <div id="map" style="width:100%;margin-bottom:1.5rem;"></div>

  <div style="font-size:11px;color:var(--text-muted);border-top:0.5px solid var(--border);padding-top:12px;">
    Data: Mattilsynet offentlig API og BarentsWatch, via salmofin BigQuery-pipeline. Generert automatisk hver natt.
  </div>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/topojson/3.0.2/topojson.min.js"></script>
<script>
const liceLabels = {lice_labels_json};
const liceValues = {lice_values_json};
new Chart(document.getElementById('liceChart'), {{
  type: 'line',
  data: {{ labels: liceLabels, datasets: [{{ data: liceValues, borderColor: '#2a78d6', backgroundColor: 'rgba(42,120,214,0.1)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2 }}] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }}, x: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ display: false }} }} }} }}
}});

new Chart(document.getElementById('fishHealthChart'), {{
  type: 'line',
  data: {{ labels: {fh_labels_json}, datasets: [
    {{ label: 'Indikator', data: {fh_values_json}, borderColor: '#7a4fc9', backgroundColor: '#7a4fc9', tension: 0.25, pointRadius: 3, spanGaps: true }}
  ] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }}, x: {{ ticks: {{ color: '#898781', font: {{ size: 10 }} }}, grid: {{ display: false }} }} }} }}
}});

const sites = {sites_json};
const diseaseColor = {{ 'PANKREASSYKDOM': '#2a78d6', 'INFEKSIOES_LAKSEANEMI': '#1baf7a', 'BAKTERIELL_NYRESYKE': '#eda100', 'FRANCISELLOSE': '#008300', 'multi': '#4a3aa7' }};
function colorFor(d) {{ return d.diseases.length > 1 ? diseaseColor.multi : diseaseColor[d.diseases[0]]; }}
const mw = 640, mh = 620;
const msvg = d3.select('#map').append('svg').attr('viewBox', `0 0 ${{mw}} ${{mh}}`).attr('width', '100%');
const isDark = matchMedia('(prefers-color-scheme: dark)').matches;
d3.json('https://cdn.jsdelivr.net/npm/datamaps@0.5.10/src/js/data/nor.topo.json').then(topo => {{
  const geoms = topo.objects.nor.geometries.filter(g => g.id && g.id.startsWith('NO.'));
  const features = topojson.feature(topo, {{ type: 'GeometryCollection', geometries: geoms }});
  const projection = d3.geoMercator().fitSize([mw, mh], features);
  const path = d3.geoPath(projection);
  msvg.append('g').selectAll('path').data(features.features).join('path')
    .attr('d', path).attr('fill', isDark ? '#2c2c2a' : '#e1e0d9')
    .attr('stroke', isDark ? '#1a1a19' : '#fcfcfb').attr('stroke-width', 0.75);
  msvg.append('g').selectAll('circle').data(sites).join('circle')
    .attr('cx', d => projection([d.lon, d.lat])[0]).attr('cy', d => projection([d.lon, d.lat])[1])
    .attr('r', 5).attr('fill', colorFor).attr('fill-opacity', 0.85)
    .attr('stroke', isDark ? '#1a1a19' : '#fcfcfb').attr('stroke-width', 1.2);
}});
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("Fetching data from BigQuery...")
    client = get_bq_client()
    lice_trend, kpis, recent, map_rows = fetch_data(client)
    fh_labels, fh_values = fetch_fiskehelseindikator(client)

    sites = build_sites_json(map_rows)
    table_rows = build_table_rows(recent)
    now = datetime.datetime.now(datetime.timezone.utc)

    html = TEMPLATE.format(
        week=now.isocalendar()[1],
        year=now.year,
        updated=now.strftime("%d.%m.%Y"),
        active_cases=kpis.active_cases,
        new_cases_14d=kpis.new_cases_14d,
        avg_lice_latest=lice_trend[-1].avg_lice if lice_trend else "–",
        treatments_14d=kpis.treatments_14d,
        table_rows=table_rows,
        lice_labels_json=json.dumps([f"u{r.Uke}" for r in lice_trend]),
        lice_values_json=json.dumps([r.avg_lice for r in lice_trend]),
        fh_labels_json=json.dumps(fh_labels),
        fh_values_json=json.dumps(fh_values),
        sites_json=json.dumps(sites, ensure_ascii=False),
    )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
