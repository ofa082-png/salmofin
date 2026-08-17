"""
generate_foring.py
-------------------
Renders the feed-vessel report — split out of generate_traffic_report.py
(2026-08-16) so feed/silage traffic isn't bundled under the harvest
("Trafikk") page it doesn't conceptually belong to. Feed carrier and
silage vessel visits are a production-intensity signal, not a
harvest-logistics one.

Sections:
  A. Fôring — feed carrier locality visits, weekly + forecast.
  B. Fiskehelseindikator — silage/feed visit ratio, a mortality proxy
     (silage vessels collect dead fish/offal, so more silage activity
     relative to normal feed-carrier activity is a fairly direct
     operational readout of mortality — see project notes).

Writes docs/foring.html.
"""

import os
import csv
import json
import datetime
from collections import defaultdict
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "salmofin"
BASE_DIR   = os.path.dirname(__file__)
OUT_PATH   = os.path.join(BASE_DIR, "docs", "foring.html")
FLEET_CSV  = os.path.join(BASE_DIR, "vessel_categories.csv")

WEEKS_HISTORY = 10
PACING_WEEKS  = 8

NO_WEEKDAY_SHORT = ["Man", "Tir", "Ons", "Tor", "Fre", "Lør", "Søn"]

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def load_fleet():
    """MMSI -> vessel type, restricted to Fish feed carrier / Silage."""
    mmsi_to_type = {}
    with open(FLEET_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mmsi = (row.get("MMSI") or "").strip()
            vtype = (row.get("Type") or "").strip()
            if mmsi.isdigit() and vtype in ("Fish feed carrier", "Silage"):
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

def build_group_data(daily, current_monday, yesterday, two_days_ago):
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

    return {
        "wtd_visits": wtd,
        "wtd_diff_label": diff_label(wtd_diff_pct),
        "wtd_diff_color": diff_color(wtd_diff_pct),
        "forecast": forecast,
        "pace_pct": round(frac * 100),
        "weekly_labels": [w[0] for w in weekly],
        "weekly_values": [w[1] for w in weekly],
        "weekly_partial_idx": len(weekly) - 1,
    }

TEMPLATE = """<!doctype html>
<html lang="no">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fôringsrapport — fôr- og ensilasjefartøy</title>
<style>
  :root {{ --surface-1:#f5f4f0; --surface-2:#ffffff; --text-primary:#0b0b0b; --text-secondary:#52514e; --text-muted:#898781; --border:#e1e0d9; --accent:#2a78d6; --accent2:#d68a2a; }}
  @media (prefers-color-scheme: dark) {{
    :root {{ --surface-1:#242422; --surface-2:#1a1a19; --text-primary:#ffffff; --text-secondary:#c3c2b7; --text-muted:#898781; --border:#2c2c2a; }}
  }}
  body {{ background:var(--surface-1); color:var(--text-primary); font-family:-apple-system,Segoe UI,Roboto,sans-serif; margin:0; padding:2rem 1rem; }}
  .wrap {{ max-width:680px; margin:0 auto; }}
  a {{ color:var(--text-secondary); }}
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
      <div style="font-size:18px;font-weight:500;">Fôringsrapport</div>
      <div style="font-size:13px;color:var(--text-muted)">Data t.o.m. {yesterday_label} · oppdatert {updated}</div>
    </div>
    <div style="display:flex;gap:6px;">
      <a href="index.html" style="font-size:11px;border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">hjem →</a>
      <a href="traffic.html" style="font-size:11px;border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">trafikk →</a>
      <a href="fiskehelse.html" style="font-size:11px;border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">fiskehelse →</a>
    </div>
  </div>

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
    <div class="section-title">Fiskehelseindikator</div>
    <div class="section-sub">Egenutviklet indikator basert på vessel-trafikkmønstre (ensilasje/fôr-anløpsforhold). Stigende verdier kan tyde på økt dødelighet eller helseutfordringer på anleggene.</div>
    <div style="position:relative;width:100%;height:170px;margin-bottom:4px;">
      <canvas id="fishHealthChart" width="640" height="170"></canvas>
    </div>
    <div style="font-size:11px;color:var(--text-muted);">Siste punkt er inneværende uke (delvis).</div>
  </section>

  <div style="font-size:11px;color:var(--text-muted);border-top:0.5px solid var(--border);padding-top:12px;">
    Lokalitetsanløp: BarentsWatch AIS, kun fartøy i vår flåteliste (vessel_categories.csv). Anslag hele uken bruker gjennomsnittlig ukentlig fremdriftsmønster fra de siste {pacing_weeks} fullførte ukene. Via salmofin BigQuery-pipeline.
  </div>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
function barColors(labels, partialIdx, base) {{
  return labels.map((_, i) => i === partialIdx ? base + '80' : base);
}}

new Chart(document.getElementById('feedChart'), {{
  type: 'bar',
  data: {{ labels: {feed_weekly_labels_json}, datasets: [{{ data: {feed_weekly_values_json}, backgroundColor: barColors({feed_weekly_labels_json}, {feed_partial_idx}, '#2a78d6'), borderRadius: 4 }}] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }}, x: {{ ticks: {{ color: '#898781', font: {{ size: 10 }} }}, grid: {{ display: false }} }} }} }}
}});

new Chart(document.getElementById('fishHealthChart'), {{
  type: 'line',
  data: {{ labels: {feed_weekly_labels_json}, datasets: [
    {{ label: 'Indikator', data: {ratio_weekly_values_json}, borderColor: '#7a4fc9', backgroundColor: '#7a4fc9', tension: 0.25, pointRadius: 3, spanGaps: true }}
  ] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{
      y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }},
      x: {{ ticks: {{ color: '#898781', font: {{ size: 10 }} }}, grid: {{ display: false }} }}
    }} }}
}});
</script>
</body>
</html>
"""

if __name__ == "__main__":
    mmsi_to_type = load_fleet()
    print(f"  {len(mmsi_to_type)} feed/silage vessels in vessel_categories.csv")

    days_back = (WEEKS_HISTORY + PACING_WEEKS) * 7
    client = get_bq_client()
    rows = fetch_visit_rows(client, list(mmsi_to_type.keys()), days_back)
    stats = build_daily_stats(rows, mmsi_to_type)

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    two_days_ago = yesterday - datetime.timedelta(days=1)
    current_monday = monday_of(yesterday)

    feed_daily = stats.get("Fish feed carrier", {})
    feed_data = build_group_data(feed_daily, current_monday, yesterday, two_days_ago)

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
        yesterday_weekday=["mandag", "tirsdag", "onsdag", "torsdag", "fredag", "lørdag", "søndag"][yesterday.weekday()],
        updated=now.strftime("%d.%m.%Y %H:%M UTC"),
        feed_wtd_visits=feed_data["wtd_visits"],
        feed_wtd_diff_label=feed_data["wtd_diff_label"],
        feed_wtd_diff_color=feed_data["wtd_diff_color"],
        feed_forecast=feed_data["forecast"],
        feed_pace_pct=feed_data["pace_pct"],
        feed_weekly_labels_json=json.dumps(feed_data["weekly_labels"]),
        feed_weekly_values_json=json.dumps(feed_data["weekly_values"]),
        feed_partial_idx=feed_data["weekly_partial_idx"],
        ratio_weekly_values_json=json.dumps(ratio_weekly_values),
        pacing_weeks=PACING_WEEKS,
    )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
