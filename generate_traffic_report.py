"""
generate_traffic_report.py
---------------------------
Renders a daily vessel traffic report (locality visits + harvest plant
deliveries) from BigQuery + the harvest_plant_visits CSVs, aimed at
traders/exporters. Writes docs/traffic.html.
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
BASE_DIR   = os.path.dirname(__file__)
OUT_PATH   = os.path.join(BASE_DIR, "docs", "traffic.html")

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def fetch_daily_visits(client):
    rows = list(client.query("""
        SELECT DATE(startTime) AS visit_date, COUNT(*) AS visits,
               COUNT(DISTINCT localityNo) AS localities, COUNT(DISTINCT mmsi) AS vessels,
               COUNTIF(isWellboat) AS wellboat_visits
        FROM salmofin.salmofin.vessel_visits
        WHERE DATE(startTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 16 DAY)
          AND DATE(startTime) < CURRENT_DATE()
        GROUP BY visit_date ORDER BY visit_date
    """).result())
    return {r.visit_date: r for r in rows}

def latest_plant_csv():
    files = sorted(glob.glob(os.path.join(BASE_DIR, "data", "harvest_plant_visits_*.csv")))
    return files[-1] if files else None

def fetch_plant_status():
    path = latest_plant_csv()
    if not path:
        return [], None
    week_label = os.path.basename(path).replace("harvest_plant_visits_", "").replace(".csv", "")
    plants = defaultdict(lambda: {"visits": 0, "capacity": 0.0, "company": None, "last_exit": None})
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            p = plants[row["plant_name"]]
            p["visits"] += 1
            p["capacity"] += float(row["capacity"])
            p["company"] = row["plant_company"]
            if not p["last_exit"] or row["exit_time"] > p["last_exit"]:
                p["last_exit"] = row["exit_time"]
    ranked = sorted(plants.items(), key=lambda kv: -kv[1]["capacity"])[:12]
    return ranked, week_label

def build_plant_rows(ranked):
    rows = []
    for name, p in ranked:
        last_date = p["last_exit"][:10] if p["last_exit"] else ""
        rows.append(f"""
      <tr style="border-top:0.5px solid var(--border);">
        <td style="padding:8px 10px;">{name.title()}</td>
        <td style="padding:8px 10px;">{p['company'].title()}</td>
        <td style="padding:8px 10px;text-align:right;">{p['visits']}</td>
        <td style="padding:8px 10px;text-align:right;">{p['capacity']:,.0f} t</td>
        <td style="padding:8px 10px;text-align:right;color:var(--text-secondary);">{last_date}</td>
      </tr>""")
    return "".join(rows)

TEMPLATE = """<!doctype html>
<html lang="no">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Trafikkrapport — vessel- og anleggstrafikk</title>
<style>
  :root {{ --surface-1:#f5f4f0; --surface-2:#ffffff; --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#898781; --border:#e1e0d9; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --surface-1:#242422; --surface-2:#1a1a19; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#898781; --border:#2c2c2a; }}
  }}
  body {{ background:var(--surface-1); color:var(--text-primary); font-family:-apple-system,Segoe UI,Roboto,sans-serif; margin:0; padding:2rem 1rem; }}
  .wrap {{ max-width:680px; margin:0 auto; }}
  table {{ border-collapse:collapse; width:100%; }}
  a {{ color:var(--text-secondary); }}
</style>
</head>
<body>
<div class="wrap">
  <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:1.25rem;">
    <div>
      <div style="font-size:18px;font-weight:500;">Trafikkrapport</div>
      <div style="font-size:13px;color:var(--text-muted)">Data for {yesterday_label} · oppdatert {updated}</div>
    </div>
    <a href="index.html" style="font-size:11px;border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">fiskehelse →</a>
  </div>

  <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin-bottom:1.5rem;">
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Anløp i går ({yesterday_weekday})</div>
      <div style="font-size:24px;font-weight:500;">{yesterday_visits}</div>
      <div style="font-size:12px;color:{y_diff_color};">{y_diff_label} vs. forrige {yesterday_weekday}</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Hittil denne uken</div>
      <div style="font-size:24px;font-weight:500;">{wtd_visits}</div>
      <div style="font-size:12px;color:{wtd_diff_color};">{wtd_diff_label} vs. samme periode forrige uke</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Brønnbåtanløp i går</div>
      <div style="font-size:24px;font-weight:500;">{yesterday_wellboat}</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Lokaliteter besøkt i går</div>
      <div style="font-size:24px;font-weight:500;">{yesterday_localities}</div>
    </div>
  </div>

  <div style="font-size:16px;font-weight:500;margin-bottom:8px;">Anløp per dag, siste 14 dager</div>
  <div style="position:relative;width:100%;height:140px;margin-bottom:1.75rem;">
    <canvas id="visitsChart" width="640" height="140"></canvas>
  </div>

  <div style="font-size:16px;font-weight:500;margin-bottom:4px;">Status per slakteri, uke {plant_week}</div>
  <div style="font-size:12px;color:var(--text-muted);margin-bottom:8px;">Kapasitet = summert fartøykapasitet ved anløp, ikke bekreftet levert volum.</div>
  <div style="border:0.5px solid var(--border);border-radius:8px;overflow:hidden;margin-bottom:1.75rem;overflow-x:auto;">
    <table style="font-size:13px;table-layout:fixed;">
      <tr style="background:var(--surface-2);">
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Anlegg</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Selskap</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Anløp</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Kapasitet</td>
        <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Siste</td>
      </tr>
      {plant_rows}
    </table>
  </div>

  <div style="font-size:11px;color:var(--text-muted);border-top:0.5px solid var(--border);padding-top:12px;">
    Anløpsdata: BarentsWatch AIS, oppdatert daglig. Slakteridata: BarentsWatch fiskehelse, oppdatert ukentlig for forrige fullførte uke. Via salmofin BigQuery-pipeline.
  </div>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
new Chart(document.getElementById('visitsChart'), {{
  type: 'bar',
  data: {{ labels: {chart_labels_json}, datasets: [{{ data: {chart_values_json}, backgroundColor: '#2a78d6', borderRadius: 4 }}] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }}, x: {{ ticks: {{ color: '#898781', font: {{ size: 10 }} }}, grid: {{ display: false }} }} }} }}
}});
</script>
</body>
</html>
"""

NO_WEEKDAY = ["mandag", "tirsdag", "onsdag", "torsdag", "fredag", "lørdag", "søndag"]

if __name__ == "__main__":
    print("Fetching vessel visit data from BigQuery...")
    client = get_bq_client()
    daily = fetch_daily_visits(client)

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    last_week_same_day = yesterday - datetime.timedelta(days=7)

    y_row = daily.get(yesterday)
    lw_row = daily.get(last_week_same_day)
    y_visits = y_row.visits if y_row else 0
    lw_visits = lw_row.visits if lw_row else 0
    y_diff_pct = ((y_visits - lw_visits) / lw_visits * 100) if lw_visits else 0

    monday_this_week = yesterday - datetime.timedelta(days=yesterday.weekday())
    wtd_dates = [monday_this_week + datetime.timedelta(days=i) for i in range((yesterday - monday_this_week).days + 1)]
    wtd_visits = sum(daily[d].visits for d in wtd_dates if d in daily)
    lw_wtd_dates = [d - datetime.timedelta(days=7) for d in wtd_dates]
    lw_wtd_visits = sum(daily[d].visits for d in lw_wtd_dates if d in daily)
    wtd_diff_pct = ((wtd_visits - lw_wtd_visits) / lw_wtd_visits * 100) if lw_wtd_visits else 0

    chart_dates = sorted(daily.keys())
    chart_labels = [d.strftime("%d.%m") for d in chart_dates]
    chart_values = [daily[d].visits for d in chart_dates]

    plant_ranked, plant_week = fetch_plant_status()
    plant_rows = build_plant_rows(plant_ranked)

    def diff_label(pct):
        sign = "+" if pct >= 0 else ""
        return f"{sign}{pct:.0f}%"
    def diff_color(pct):
        return "#008300" if pct >= 0 else "#a32d2d"

    now = datetime.datetime.now(datetime.timezone.utc)
    html = TEMPLATE.format(
        yesterday_label=yesterday.strftime("%d.%m.%Y"),
        yesterday_weekday=NO_WEEKDAY[yesterday.weekday()],
        updated=now.strftime("%d.%m.%Y %H:%M UTC"),
        yesterday_visits=y_visits,
        y_diff_label=diff_label(y_diff_pct),
        y_diff_color=diff_color(y_diff_pct),
        wtd_visits=wtd_visits,
        wtd_diff_label=diff_label(wtd_diff_pct),
        wtd_diff_color=diff_color(wtd_diff_pct),
        yesterday_wellboat=y_row.wellboat_visits if y_row else 0,
        yesterday_localities=y_row.localities if y_row else 0,
        plant_week=plant_week or "-",
        plant_rows=plant_rows,
        chart_labels_json=json.dumps(chart_labels),
        chart_values_json=json.dumps(chart_values),
    )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
    print(f"Yesterday ({yesterday}): {y_visits} visits vs {lw_visits} same weekday last week ({y_diff_pct:+.1f}%)")
    print(f"WTD: {wtd_visits} vs {lw_wtd_visits} last week ({wtd_diff_pct:+.1f}%)")
