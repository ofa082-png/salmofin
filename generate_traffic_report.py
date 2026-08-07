"""
generate_traffic_report.py
---------------------------
Renders a weekly-focused vessel traffic report for traders/exporters,
scoped to the fleet in vessel_categories.csv. Sections:

  A/B. Harvest signal — Wellboat + Processing vessel locality visits
       (vessel_visits) and harvest-plant deliveries (harvest_plant_visits
       CSVs), with a weekday-pacing forecast for the current week.
  C.   Fôring — feed carrier locality visits, same weekly/forecast treatment.
  D.   Fôr vs. ensilasje — feed vs. silage visits overlaid, to spot
       silage ramping up without a matching rise in feed (event- rather
       than production-driven activity).

Writes docs/traffic.html.
"""

import os
import csv
import json
import glob
import datetime
from collections import defaultdict
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID   = "salmofin"
BASE_DIR     = os.path.dirname(__file__)
OUT_PATH     = os.path.join(BASE_DIR, "docs", "traffic.html")
FLEET_CSV    = os.path.join(BASE_DIR, "vessel_categories.csv")

WEEKS_HISTORY = 10   # weeks shown in bar charts, including the current (partial) week
PACING_WEEKS  = 8     # completed weeks used to build the weekday-pacing curve for forecasts
PLANT_WEEKS_HISTORY = 14

HARVEST_LABELS = {
    "Alle":              "Alle",
    "Wellboat":          "Brønnbåt",
    "Processing vessel": "Prosesseringsfartøy",
}
HARVEST_ORDER = ["Alle", "Wellboat", "Processing vessel"]

NO_WEEKDAY = ["mandag", "tirsdag", "onsdag", "torsdag", "fredag", "lørdag", "søndag"]

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def load_fleet():
    """MMSI -> vessel type, restricted to our own vessel list."""
    mmsi_to_type = {}
    with open(FLEET_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mmsi = (row.get("MMSI") or "").strip()
            vtype = (row.get("Type") or "").strip()
            if mmsi.isdigit():
                mmsi_to_type[int(mmsi)] = vtype
    return mmsi_to_type

def fetch_visit_rows(client, mmsi_list, days_back):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("mmsi_list", "INT64", mmsi_list)]
    )
    rows = list(client.query(f"""
        SELECT DATE(startTime) AS visit_date, mmsi, localityNo
        FROM salmofin.salmofin.vessel_visits
        WHERE DATE(startTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_back} DAY)
          AND DATE(startTime) < CURRENT_DATE()
          AND mmsi IN UNNEST(@mmsi_list)
    """, job_config=job_config).result())
    return rows

def build_daily_stats(rows, mmsi_to_type):
    """{vessel_type: {date: {"visits": int, "localities": set, "vessels": set}}}"""
    stats = defaultdict(lambda: defaultdict(lambda: {"visits": 0, "localities": set(), "vessels": set()}))
    for row in rows:
        vtype = mmsi_to_type.get(row.mmsi)
        if not vtype:
            continue
        rec = stats[vtype][row.visit_date]
        rec["visits"] += 1
        rec["localities"].add(row.localityNo)
        rec["vessels"].add(row.mmsi)
    return stats

def combine_daily(*daily_dicts):
    combined = defaultdict(lambda: {"visits": 0, "localities": set(), "vessels": set()})
    for dd in daily_dicts:
        for date, rec in dd.items():
            c = combined[date]
            c["visits"] += rec["visits"]
            c["localities"] |= rec["localities"]
            c["vessels"] |= rec["vessels"]
    return combined

def monday_of(d):
    return d - datetime.timedelta(days=d.weekday())

def week_total(daily, monday, end_date):
    total = 0
    d = monday
    while d <= end_date:
        total += daily.get(d, {}).get("visits", 0)
        d += datetime.timedelta(days=1)
    return total

def build_pacing_curve(daily, current_monday, pacing_weeks):
    """avg fraction of a full week's visits accumulated through each weekday
    (0=Mon..6=Sun), based on the `pacing_weeks` completed weeks immediately
    before `current_monday`. Falls back to a flat/linear curve where there's
    no history, so forecasts degrade gracefully instead of erroring."""
    fractions = [[] for _ in range(7)]
    for w in range(1, pacing_weeks + 1):
        m = current_monday - datetime.timedelta(weeks=w)
        days = [m + datetime.timedelta(days=i) for i in range(7)]
        vals = [daily.get(d, {}).get("visits", 0) for d in days]
        wk_total = sum(vals)
        if wk_total == 0:
            continue
        cum = 0
        for i in range(7):
            cum += vals[i]
            fractions[i].append(cum / wk_total)
    return [sum(f) / len(f) if f else (i + 1) / 7 for i, f in enumerate(fractions)]

def build_weekly_series(daily, current_monday, weeks_history, yesterday):
    """[(label, total, is_partial), ...] oldest -> newest, newest may be partial."""
    series = []
    for i in range(weeks_history - 1, -1, -1):
        m = current_monday - datetime.timedelta(weeks=i)
        is_partial = (m == current_monday)
        end = yesterday if is_partial else m + datetime.timedelta(days=6)
        total = week_total(daily, m, end) if end >= m else 0
        label = f"U{m.isocalendar()[1]}"
        series.append((label, total, is_partial))
    return series

def diff_label(pct):
    sign = "+" if pct >= 0 else ""
    return f"{sign}{pct:.0f}%"

def diff_color(pct):
    return "#008300" if pct >= 0 else "#a32d2d"

def build_harvest_group_data(daily, current_monday, yesterday, two_days_ago, plant_weekly=None):
    pacing = build_pacing_curve(daily, current_monday, PACING_WEEKS)
    wtd = week_total(daily, current_monday, yesterday)
    lw_end = yesterday - datetime.timedelta(days=7)
    lw_monday = current_monday - datetime.timedelta(days=7)
    lw_wtd = week_total(daily, lw_monday, lw_end)
    wtd_diff_pct = ((wtd - lw_wtd) / lw_wtd * 100) if lw_wtd else 0

    frac = pacing[yesterday.weekday()]
    if frac <= 0:
        frac = (yesterday.weekday() + 1) / 7
    forecast = round(wtd / frac)

    weekly = build_weekly_series(daily, current_monday, WEEKS_HISTORY, yesterday)

    y_visits = daily.get(yesterday, {}).get("visits", 0)
    tda_visits = daily.get(two_days_ago, {}).get("visits", 0)
    y_diff_pct = ((y_visits - tda_visits) / tda_visits * 100) if tda_visits else 0
    y_vessels = len(daily.get(yesterday, {}).get("vessels", set()))
    y_localities = len(daily.get(yesterday, {}).get("localities", set()))

    result = {
        "wtd_visits": wtd,
        "wtd_diff_label": diff_label(wtd_diff_pct),
        "wtd_diff_color": diff_color(wtd_diff_pct),
        "forecast": forecast,
        "pace_pct": round(frac * 100),
        "weekly_labels": [w[0] for w in weekly],
        "weekly_values": [w[1] for w in weekly],
        "weekly_partial_idx": len(weekly) - 1,
        "yesterday_visits": y_visits,
        "y_diff_label": diff_label(y_diff_pct),
        "y_diff_color": diff_color(y_diff_pct),
        "yesterday_vessels": y_vessels,
        "yesterday_localities": y_localities,
    }

    if plant_weekly is not None:
        p_labels = [w[0] for w in plant_weekly]
        p_values = [w[1] for w in plant_weekly]
        p_last = p_values[-1] if p_values else 0
        p_prev = p_values[-2] if len(p_values) > 1 else 0
        p_diff_pct = ((p_last - p_prev) / p_prev * 100) if p_prev else 0
        result.update({
            "plant_weekly_labels": p_labels,
            "plant_weekly_values": p_values,
            "plant_last_week": p_last,
            "plant_diff_label": diff_label(p_diff_pct),
            "plant_diff_color": diff_color(p_diff_pct),
        })

    return result

def latest_plant_csv():
    files = sorted(glob.glob(os.path.join(BASE_DIR, "data", "harvest_plant_visits_*.csv")))
    return files[-1] if files else None

def all_plant_csvs():
    return sorted(glob.glob(os.path.join(BASE_DIR, "data", "harvest_plant_visits_*.csv")))

def fetch_plant_status():
    """Latest-week plant ranking per vessel type. The CSV is already
    restricted to Wellboat + Processing vessel (fetch_harvest_visits.py
    only tracks those two types against harvest plants)."""
    path = latest_plant_csv()
    if not path:
        return {}, None
    week_label = os.path.basename(path).replace("harvest_plant_visits_", "").replace(".csv", "")
    plants_by_type = defaultdict(lambda: defaultdict(lambda: {"visits": 0, "capacity": 0.0, "company": None, "last_exit": None}))
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            vtype = row["vessel_type"].strip()
            for key in ("Alle", vtype):
                p = plants_by_type[key][row["plant_name"]]
                p["visits"] += 1
                p["capacity"] += float(row["capacity"])
                p["company"] = row["plant_company"]
                if not p["last_exit"] or row["exit_time"] > p["last_exit"]:
                    p["last_exit"] = row["exit_time"]
    ranked_by_type = {
        key: sorted(plants.items(), key=lambda kv: -kv[1]["capacity"])[:12]
        for key, plants in plants_by_type.items()
    }
    return ranked_by_type, week_label

NO_PLANT_DATA_ROW = ('<tr><td colspan="5" style="padding:14px 10px;color:var(--text-muted);'
                      'text-align:center;">Ingen slakterianløp registrert for denne fartøytypen.</td></tr>')

def build_plant_rows(ranked):
    if not ranked:
        return NO_PLANT_DATA_ROW
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

def fetch_plant_weekly_series(n_weeks):
    """Weekly plant-visit totals per vessel type, from the last n_weeks
    harvest_plant_visits CSVs (each file = one already-completed week)."""
    files = all_plant_csvs()[-n_weeks:]
    series = {"Alle": [], "Wellboat": [], "Processing vessel": []}
    for path in files:
        label = os.path.basename(path).replace("harvest_plant_visits_", "").replace(".csv", "").split("_")[-1]
        counts = {"Alle": 0, "Wellboat": 0, "Processing vessel": 0}
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                counts["Alle"] += 1
                vtype = row["vessel_type"].strip()
                if vtype in counts:
                    counts[vtype] += 1
        for k in series:
            series[k].append((label, counts[k]))
    return series

TEMPLATE = """<!doctype html>
<html lang="no">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Trafikkrapport — vessel- og anleggstrafikk</title>
<style>
  :root {{ --surface-1:#f5f4f0; --surface-2:#ffffff; --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#898781; --border:#e1e0d9; --accent:#2a78d6; --accent2:#d68a2a; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --surface-1:#242422; --surface-2:#1a1a19; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#898781; --border:#2c2c2a; }}
  }}
  body {{ background:var(--surface-1); color:var(--text-primary); font-family:-apple-system,Segoe UI,Roboto,sans-serif; margin:0; padding:2rem 1rem; }}
  .wrap {{ max-width:680px; margin:0 auto; }}
  table {{ border-collapse:collapse; width:100%; }}
  a {{ color:var(--text-secondary); }}
  .pill {{ font-size:12px; border:0.5px solid var(--border); border-radius:999px; padding:5px 12px; cursor:pointer; background:var(--surface-2); color:var(--text-secondary); white-space:nowrap; }}
  .pill.active {{ background:var(--accent); border-color:var(--accent); color:#fff; }}
  .card {{ background:var(--surface-2); border-radius:8px; padding:1rem; }}
  .section-title {{ font-size:16px; font-weight:500; margin-bottom:2px; }}
  .section-sub {{ font-size:12px; color:var(--text-muted); margin-bottom:10px; }}
  section {{ margin-bottom:2.25rem; }}
</style>
</head>
<body>
<div class="wrap">
  <div style="display:flex;justify-content:space-between;align-items:baseline;margin-bottom:1.5rem;">
    <div>
      <div style="font-size:18px;font-weight:500;">Trafikkrapport</div>
      <div style="font-size:13px;color:var(--text-muted)">Data t.o.m. {yesterday_label} · oppdatert {updated}</div>
    </div>
    <a href="index.html" style="font-size:11px;border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">fiskehelse →</a>
  </div>

  <section>
    <div class="section-title">Slakteaktivitet — lokalitetsanløp</div>
    <div class="section-sub">Brønnbåt og prosesseringsfartøy ved oppdrettslokaliteter (ikke slakteri). BarentsWatch AIS.</div>
    <div id="harvestPills" style="display:flex;gap:6px;margin-bottom:12px;">{harvest_pills}</div>

    <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin-bottom:14px;">
      <div class="card">
        <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Hittil denne uken</div>
        <div id="h-wtd" style="font-size:24px;font-weight:500;"></div>
        <div id="h-wtddiff" style="font-size:12px;"></div>
      </div>
      <div class="card">
        <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Anslag hele uken</div>
        <div id="h-forecast" style="font-size:24px;font-weight:500;"></div>
        <div id="h-pace" style="font-size:12px;color:var(--text-muted);"></div>
      </div>
    </div>
    <div style="font-size:12px;color:var(--text-muted);margin-bottom:8px;">
      I går: <span id="h-yesterday"></span> anløp (<span id="h-ydiff"></span> vs. i forgårs) · <span id="h-vessels"></span> fartøy · <span id="h-localities"></span> lokaliteter
    </div>

    <div style="position:relative;width:100%;height:150px;margin-bottom:4px;">
      <canvas id="harvestChart" width="640" height="150"></canvas>
    </div>
    <div style="font-size:11px;color:var(--text-muted);">Siste søyle er inneværende uke (delvis).</div>
  </section>

  <section>
    <div class="section-title">Slakterianløp</div>
    <div class="section-sub">Fysiske anløp ved slakteri, forrige fullførte uke ({plant_week}) mot uken før — samme fartøyfilter som over.</div>

    <div class="card" style="margin-bottom:14px;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Forrige uke ({plant_week})</div>
      <div id="p-lastweek" style="font-size:24px;font-weight:500;"></div>
      <div id="p-diff" style="font-size:12px;"></div>
    </div>

    <div style="position:relative;width:100%;height:150px;margin-bottom:14px;">
      <canvas id="plantWeeklyChart" width="640" height="150"></canvas>
    </div>

    <div style="font-size:12px;color:var(--text-muted);margin-bottom:8px;">Status per slakteri, uke {plant_week}. Kapasitet = summert fartøykapasitet ved anløp, ikke bekreftet levert volum.</div>
    <div style="border:0.5px solid var(--border);border-radius:8px;overflow:hidden;overflow-x:auto;">
      <table style="font-size:13px;table-layout:fixed;">
        <thead>
        <tr style="background:var(--surface-2);">
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Anlegg</td>
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Selskap</td>
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Anløp</td>
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Kapasitet</td>
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Siste</td>
        </tr>
        </thead>
        <tbody id="plantRows"></tbody>
      </table>
    </div>
  </section>

  <section>
    <div class="section-title">Fôring</div>
    <div class="section-sub">Fôrbåtanløp ved lokaliteter, ukentlig. BarentsWatch AIS.</div>

    <div style="display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin-bottom:14px;">
      <div class="card">
        <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Hittil denne uken</div>
        <div style="font-size:24px;font-weight:500;">{feed_wtd_visits}</div>
        <div style="font-size:12px;color:{feed_wtd_diff_color};">{feed_wtd_diff_label} vs. samme periode forrige uke</div>
      </div>
      <div class="card">
        <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Anslag hele uken</div>
        <div style="font-size:24px;font-weight:500;">{feed_forecast}</div>
        <div style="font-size:12px;color:var(--text-muted);">basert på {feed_pace_pct}% typisk fremdrift til {yesterday_weekday}</div>
      </div>
    </div>
    <div style="position:relative;width:100%;height:150px;margin-bottom:4px;">
      <canvas id="feedChart" width="640" height="150"></canvas>
    </div>
    <div style="font-size:11px;color:var(--text-muted);">Siste søyle er inneværende uke (delvis).</div>
  </section>

  <section>
    <div class="section-title">Fôr vs. ensilasje</div>
    <div class="section-sub">Fôr og ensilasje følger normalt hverandre (produksjonsdrevet). Ensilasje som stiger uten tilsvarende fôrøkning kan indikere en hendelse (f.eks. dødelighet) snarere enn normal drift.</div>
    <div style="position:relative;width:100%;height:170px;margin-bottom:6px;">
      <canvas id="feedSilageChart" width="640" height="170"></canvas>
    </div>
    <div style="font-size:11px;color:var(--text-muted);">Siste søyle er inneværende uke (delvis).</div>
  </section>

  <div style="font-size:11px;color:var(--text-muted);border-top:0.5px solid var(--border);padding-top:12px;">
    Lokalitetsanløp: BarentsWatch AIS, kun fartøy i vår flåteliste (vessel_categories.csv). Slakterianløp: BarentsWatch fiskehelse, oppdatert ukentlig for forrige fullførte uke. Anslag hele uken bruker gjennomsnittlig ukentlig fremdriftsmønster fra de siste {pacing_weeks} fullførte ukene. Via salmofin BigQuery-pipeline.
  </div>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
const HARVEST_DATA = {harvest_data_json};
const PLANT_ROWS_BY_TYPE = {plant_rows_json};
const YESTERDAY_WEEKDAY = {yesterday_weekday_json};

function barColors(labels, partialIdx, base) {{
  return labels.map((_, i) => i === partialIdx ? base + '80' : base);
}}

const harvestChart = new Chart(document.getElementById('harvestChart'), {{
  type: 'bar',
  data: {{ labels: [], datasets: [{{ data: [], backgroundColor: '#2a78d6', borderRadius: 4 }}] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }}, x: {{ ticks: {{ color: '#898781', font: {{ size: 10 }} }}, grid: {{ display: false }} }} }} }}
}});

const plantWeeklyChart = new Chart(document.getElementById('plantWeeklyChart'), {{
  type: 'bar',
  data: {{ labels: [], datasets: [{{ data: [], backgroundColor: '#2a78d6', borderRadius: 4 }}] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }}, x: {{ ticks: {{ color: '#898781', font: {{ size: 10 }} }}, grid: {{ display: false }} }} }} }}
}});

function showHarvest(key) {{
  const d = HARVEST_DATA[key];
  if (!d) return;
  document.getElementById('h-wtd').textContent = d.wtd_visits;
  const wd = document.getElementById('h-wtddiff'); wd.textContent = d.wtd_diff_label + ' vs. samme periode forrige uke'; wd.style.color = d.wtd_diff_color;
  document.getElementById('h-forecast').textContent = d.forecast;
  document.getElementById('h-pace').textContent = 'basert på ' + d.pace_pct + '% typisk fremdrift til ' + YESTERDAY_WEEKDAY;
  document.getElementById('h-yesterday').textContent = d.yesterday_visits;
  const yd = document.getElementById('h-ydiff'); yd.textContent = d.y_diff_label; yd.style.color = d.y_diff_color;
  document.getElementById('h-vessels').textContent = d.yesterday_vessels;
  document.getElementById('h-localities').textContent = d.yesterday_localities;
  harvestChart.data.labels = d.weekly_labels;
  harvestChart.data.datasets[0].data = d.weekly_values;
  harvestChart.data.datasets[0].backgroundColor = barColors(d.weekly_labels, d.weekly_partial_idx, '#2a78d6');
  harvestChart.update();
  document.getElementById('plantRows').innerHTML = PLANT_ROWS_BY_TYPE[key] || '';
  document.getElementById('p-lastweek').textContent = d.plant_last_week;
  const pd = document.getElementById('p-diff'); pd.textContent = d.plant_diff_label + ' vs. uken før'; pd.style.color = d.plant_diff_color;
  plantWeeklyChart.data.labels = d.plant_weekly_labels;
  plantWeeklyChart.data.datasets[0].data = d.plant_weekly_values;
  plantWeeklyChart.update();
  document.querySelectorAll('#harvestPills .pill').forEach(el => el.classList.toggle('active', el.dataset.type === key));
}}

document.querySelectorAll('#harvestPills .pill').forEach(el => {{
  el.addEventListener('click', () => showHarvest(el.dataset.type));
}});
showHarvest('Alle');

new Chart(document.getElementById('feedChart'), {{
  type: 'bar',
  data: {{ labels: {feed_weekly_labels_json}, datasets: [{{ data: {feed_weekly_values_json}, backgroundColor: barColors({feed_weekly_labels_json}, {feed_partial_idx}, '#2a78d6'), borderRadius: 4 }}] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }}, x: {{ ticks: {{ color: '#898781', font: {{ size: 10 }} }}, grid: {{ display: false }} }} }} }}
}});

new Chart(document.getElementById('feedSilageChart'), {{
  data: {{ labels: {feed_weekly_labels_json}, datasets: [
    {{ type: 'bar', label: 'Ensilasje', data: {silage_weekly_values_json}, backgroundColor: barColors({feed_weekly_labels_json}, {feed_partial_idx}, '#d68a2a'), borderRadius: 3, yAxisID: 'y', order: 2 }},
    {{ type: 'line', label: 'Ensilasje/fôr-forhold', data: {ratio_weekly_values_json}, borderColor: '#7a4fc9', backgroundColor: '#7a4fc9', tension: 0.25, pointRadius: 3, yAxisID: 'y1', order: 1, spanGaps: true }}
  ] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: true, labels: {{ color: '#898781', font: {{ size: 11 }}, boxWidth: 10 }} }} }},
    scales: {{
      y: {{ position: 'left', ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }},
      y1: {{ position: 'right', ticks: {{ color: '#7a4fc9', font: {{ size: 11 }} }}, grid: {{ display: false }} }},
      x: {{ ticks: {{ color: '#898781', font: {{ size: 10 }} }}, grid: {{ display: false }} }}
    }} }}
}});
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("Loading fleet list...")
    mmsi_to_type = load_fleet()
    print(f"  {len(mmsi_to_type)} vessels in vessel_categories.csv")

    days_back = (WEEKS_HISTORY + PACING_WEEKS) * 7
    print(f"Fetching {days_back} days of vessel visit data from BigQuery...")
    client = get_bq_client()
    rows = fetch_visit_rows(client, list(mmsi_to_type.keys()), days_back)
    stats = build_daily_stats(rows, mmsi_to_type)

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    two_days_ago = yesterday - datetime.timedelta(days=1)
    current_monday = monday_of(yesterday)

    # --- Plant (slakteri) section — computed first so it can feed into harvest_data ---
    plant_ranked_by_type, plant_week = fetch_plant_status()
    plant_rows_by_type = {
        t: build_plant_rows(plant_ranked_by_type.get(t, [])) for t in HARVEST_ORDER
    }
    plant_weekly = fetch_plant_weekly_series(PLANT_WEEKS_HISTORY)

    # --- Harvest section (Wellboat + Processing vessel) ---
    harvest_daily = {
        "Alle": combine_daily(stats.get("Wellboat", {}), stats.get("Processing vessel", {})),
        "Wellboat": stats.get("Wellboat", {}),
        "Processing vessel": stats.get("Processing vessel", {}),
    }
    harvest_data = {
        key: build_harvest_group_data(daily, current_monday, yesterday, two_days_ago, plant_weekly=plant_weekly[key])
        for key, daily in harvest_daily.items()
    }
    harvest_pills = "".join(
        f'<button class="pill" data-type="{t}">{HARVEST_LABELS[t]}</button>' for t in HARVEST_ORDER
    )

    # --- Feed section ---
    feed_daily = stats.get("Fish feed carrier", {})
    feed_data = build_harvest_group_data(feed_daily, current_monday, yesterday, two_days_ago)

    # --- Silage (for feed vs. silage overlay) ---
    silage_daily = stats.get("Silage", {})
    silage_weekly = build_weekly_series(silage_daily, current_monday, WEEKS_HISTORY, yesterday)
    silage_weekly_values = [w[1] for w in silage_weekly]
    ratio_weekly_values = [
        round(s / f, 3) if f else None
        for s, f in zip(silage_weekly_values, feed_data["weekly_values"])
    ]

    now = datetime.datetime.now(datetime.timezone.utc)
    html = TEMPLATE.format(
        yesterday_label=yesterday.strftime("%d.%m.%Y"),
        yesterday_weekday=NO_WEEKDAY[yesterday.weekday()],
        yesterday_weekday_json=json.dumps(NO_WEEKDAY[yesterday.weekday()]),
        updated=now.strftime("%d.%m.%Y %H:%M UTC"),
        harvest_pills=harvest_pills,
        harvest_data_json=json.dumps(harvest_data),
        plant_rows_json=json.dumps(plant_rows_by_type),
        plant_week=plant_week or "-",
        feed_wtd_visits=feed_data["wtd_visits"],
        feed_wtd_diff_label=feed_data["wtd_diff_label"],
        feed_wtd_diff_color=feed_data["wtd_diff_color"],
        feed_forecast=feed_data["forecast"],
        feed_pace_pct=feed_data["pace_pct"],
        feed_weekly_labels_json=json.dumps(feed_data["weekly_labels"]),
        feed_weekly_values_json=json.dumps(feed_data["weekly_values"]),
        feed_partial_idx=feed_data["weekly_partial_idx"],
        silage_weekly_values_json=json.dumps(silage_weekly_values),
        ratio_weekly_values_json=json.dumps(ratio_weekly_values),
        pacing_weeks=PACING_WEEKS,
    )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
    print(f"Harvest (Alle): WTD={harvest_data['Alle']['wtd_visits']} forecast={harvest_data['Alle']['forecast']} ({harvest_data['Alle']['pace_pct']}% typical pace)")
    print(f"Plant last week ({plant_week}): {harvest_data['Alle']['plant_last_week']} ({harvest_data['Alle']['plant_diff_label']} vs prev week)")
    print(f"Feed: WTD={feed_data['wtd_visits']} forecast={feed_data['forecast']}")
