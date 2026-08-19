"""
generate_lakselus.py
---------------------
Renders the lice-pressure/treatment report and writes it to
docs/lakselus.html, for GitHub Pages to serve. Nightly script.

Split out of generate_report.py on 2026-08-19 — Lusenivå (lice level)
and Avlusningsfartøy (delousing vessel traffic) used to live on
fiskehelse.html alongside mortality/disease content, but that page was
getting bloated bundling two conceptually separate signals together.
This page owns lice pressure + treatment activity; fiskehelse.html
owns mortality + disease.

Self-contained by design, matching every other generate_*.py script in
this repo — duplicates the small bits of shared logic (BQ client,
vessel-fleet loading, weekly-series helpers) rather than importing from
generate_report.py.
"""

import os
import csv
import json
import datetime
from collections import defaultdict
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "salmofin"
OUT_PATH   = os.path.join(os.path.dirname(__file__), "docs", "lakselus.html")
FLEET_CSV  = os.path.join(os.path.dirname(__file__), "vessel_categories.csv")
WEEKS_HISTORY = 12  # matches the lice chart's "siste 12 uker" lookback, shared by the Avlusningsfartøy chart too

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def load_vessel_fleet(types):
    """MMSI -> vessel type, restricted to the given vessel_categories.csv
    Type values — used for the Avlusningsfartøy chart below."""
    mmsi_to_type = {}
    with open(FLEET_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mmsi = (row.get("MMSI") or "").strip()
            vtype = (row.get("Type") or "").strip()
            if mmsi.isdigit() and vtype in types:
                mmsi_to_type[int(mmsi)] = vtype
    return mmsi_to_type

def fetch_fleet_visits(client, mmsi_list, days_back):
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

def fetch_delousing_chart(client):
    """Avlusningsfartøy (delousing vessel visits, descriptive only —
    tested against actual delousing registrations and currently catches
    ~22-26% of them, not yet a reliable indicator on its own)."""
    mmsi_to_type = load_vessel_fleet(("Delicing vessel",))
    days_back = WEEKS_HISTORY * 7 + 7
    rows = fetch_fleet_visits(client, list(mmsi_to_type.keys()), days_back)

    daily = defaultdict(int)
    for r in rows:
        if r.mmsi in mmsi_to_type:
            daily[r.visit_date] += 1

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    current_monday = monday_of(yesterday)
    weekly = build_weekly_series(daily, current_monday, WEEKS_HISTORY, yesterday)
    return [w[0] for w in weekly], [w[1] for w in weekly]

def fetch_lice_data(client):
    lice_trend = list(client.query("""
        SELECT Uke, ROUND(AVG(Voksne_hunnlus),4) AS avg_lice
        FROM salmofin.salmofin.lice_bw
        WHERE Ar = EXTRACT(YEAR FROM CURRENT_DATE())
          AND Uke BETWEEN EXTRACT(ISOWEEK FROM CURRENT_DATE()) - 11
                       AND EXTRACT(ISOWEEK FROM CURRENT_DATE())
          AND Voksne_hunnlus IS NOT NULL
        GROUP BY Uke ORDER BY Uke
    """).result())

    # last *complete* ISO week — same "don't trust the still-filling-in
    # current period" pattern used throughout this project (e.g. the
    # traffic report anchoring on "yesterday", never "today")
    kpis = list(client.query("""
        SELECT
          (SELECT COUNT(*) FROM salmofin.salmofin.treatments
             WHERE Ar = EXTRACT(YEAR FROM CURRENT_DATE())
               AND Uke IN (EXTRACT(ISOWEEK FROM CURRENT_DATE()) - 1, EXTRACT(ISOWEEK FROM CURRENT_DATE()))) AS treatments_14d,
          (SELECT ROUND(COUNTIF(Over_lusegrense_uke) / NULLIF(COUNTIF(Har_telt_lakselus), 0) * 100, 1)
             FROM salmofin.salmofin.lice_bw
             WHERE Ar = EXTRACT(YEAR FROM CURRENT_DATE())
               AND Uke = EXTRACT(ISOWEEK FROM CURRENT_DATE()) - 1
               AND Trolig_uten_fisk = false) AS over_limit_pct,
          (SELECT COUNT(DISTINCT Lokalitetsnummer)
             FROM salmofin.salmofin.lice_bw
             WHERE Ar = EXTRACT(YEAR FROM CURRENT_DATE())
               AND Uke = EXTRACT(ISOWEEK FROM CURRENT_DATE()) - 1
               AND Har_telt_lakselus = true) AS sites_counted
    """).result())[0]

    return lice_trend, kpis

TEMPLATE = """<!doctype html>
<html lang="no">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Lakselus — ukesrapport</title>
<style>
  :root {{ --surface-1:#f5f4f0; --surface-2:#ffffff; --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#898781; --border:#e1e0d9; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --surface-1:#242422; --surface-2:#1a1a19; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#898781; --border:#2c2c2a; }}
  }}
  body {{ background:var(--surface-1); color:var(--text-primary); font-family:-apple-system,Segoe UI,Roboto,sans-serif; margin:0; padding:2rem 1rem; }}
  .wrap {{ max-width:680px; margin:0 auto; }}
  a {{ color:var(--text-secondary); }}
</style>
</head>
<body>
<div class="wrap">
  <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:1.25rem;">
    <div>
      <div style="font-size:18px;font-weight:500;">Lakselus — ukesrapport</div>
      <div style="font-size:13px;color:var(--text-muted)">Uke {week}, {year} · oppdatert {updated}</div>
    </div>
    <div style="display:flex;gap:8px;align-items:baseline;">
      <a href="index.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">hjem →</a>
      <a href="fiskehelse.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">fiskehelse →</a>
      <div style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;">kilde: BarentsWatch</div>
    </div>
  </div>

  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:1.5rem;">
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Voksne hunnlus, snitt</div>
      <div style="font-size:24px;font-weight:500;">{avg_lice_latest}</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Over lusegrense, siste uke</div>
      <div style="font-size:24px;font-weight:500;">{over_limit_pct}%</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Lokaliteter talt, siste uke</div>
      <div style="font-size:24px;font-weight:500;">{sites_counted}</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Behandlinger siste 14 dager</div>
      <div style="font-size:24px;font-weight:500;">{treatments_14d}</div>
    </div>
  </div>

  <div style="font-size:16px;font-weight:500;margin-bottom:8px;">Lusenivå, siste 12 uker</div>
  <div style="font-size:12px;color:var(--text-muted);margin-bottom:10px;">Voksne hunnlus, snitt per lokalitet, nasjonalt.</div>
  <div style="position:relative;width:100%;height:140px;margin-bottom:1.75rem;">
    <canvas id="liceChart" width="640" height="140"></canvas>
  </div>

  <div style="font-size:16px;font-weight:500;margin-bottom:2px;">Avlusningsfartøy</div>
  <div style="font-size:12px;color:var(--text-muted);margin-bottom:10px;">Kun beskrivende — testet mot faktiske avlusningsregistreringer og fanger foreløpig opp ca. 22–26% av dem, så dette er ikke en pålitelig indikator ennå, kun et rått anløpsbilde.</div>
  <div style="position:relative;width:100%;height:140px;margin-bottom:4px;">
    <canvas id="delousingWeeklyChart" width="640" height="140"></canvas>
  </div>
  <div style="font-size:11px;color:var(--text-muted);margin-bottom:1.75rem;">Siste søyle er inneværende uke (delvis).</div>

  <div style="font-size:11px;color:var(--text-muted);border-top:0.5px solid var(--border);padding-top:12px;">
    Data: BarentsWatch (lakselus, behandlinger), via salmofin BigQuery-pipeline. Vessel-indikator: BarentsWatch AIS, kun fartøy i vår flåteliste (vessel_categories.csv). "Over lusegrense"/"lokaliteter talt" gjelder siste avsluttede uke (inneværende uke er ikke ferdig rapportert ennå). Generert automatisk hver natt.
  </div>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
new Chart(document.getElementById('liceChart'), {{
  type: 'line',
  data: {{ labels: {lice_labels_json}, datasets: [{{ data: {lice_values_json}, borderColor: '#2a78d6', backgroundColor: 'rgba(42,120,214,0.1)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2 }}] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }}, x: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ display: false }} }} }} }}
}});

function barColors(labels, partialIdx, base) {{
  return labels.map((_, i) => i === partialIdx ? base + '80' : base);
}}
new Chart(document.getElementById('delousingWeeklyChart'), {{
  type: 'bar',
  data: {{ labels: {delousing_labels_json}, datasets: [{{ data: {delousing_values_json}, backgroundColor: barColors({delousing_labels_json}, {delousing_partial_idx}, '#52514e'), borderRadius: 4 }}] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }}, x: {{ ticks: {{ color: '#898781', font: {{ size: 10 }} }}, grid: {{ display: false }} }} }} }}
}});
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("Fetching data from BigQuery...")
    client = get_bq_client()
    lice_trend, kpis = fetch_lice_data(client)
    delousing_labels, delousing_values = fetch_delousing_chart(client)
    now = datetime.datetime.now(datetime.timezone.utc)

    html = TEMPLATE.format(
        week=now.isocalendar()[1],
        year=now.year,
        updated=now.strftime("%d.%m.%Y"),
        avg_lice_latest=lice_trend[-1].avg_lice if lice_trend else "–",
        over_limit_pct=kpis.over_limit_pct if kpis.over_limit_pct is not None else "–",
        sites_counted=kpis.sites_counted,
        treatments_14d=kpis.treatments_14d,
        lice_labels_json=json.dumps([f"U{r.Uke}" for r in lice_trend]),
        lice_values_json=json.dumps([r.avg_lice for r in lice_trend]),
        delousing_labels_json=json.dumps(delousing_labels),
        delousing_values_json=json.dumps(delousing_values),
        delousing_partial_idx=len(delousing_labels) - 1,
    )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
