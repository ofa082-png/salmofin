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

The "Fiskehelseindikator" section (silage/feed visit ratio, a mortality
proxy) that used to live here moved to generate_report.py/fiskehelse.html
on 2026-08-17 — it's a fish-health signal, not a feed-logistics one, so
it belongs with the rest of the health content instead of this page.

Writes docs/foring.html.
"""

import os
import csv
import json
import datetime
import math
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
    if not os.environ.get("GOOGLE_CREDENTIALS"):   # local run: application default credentials
        return bigquery.Client(project=PROJECT_ID)
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def load_fleet():
    """MMSI -> vessel type, restricted to Fish feed carrier. (Silage was
    only needed for the Fiskehelseindikator section, which moved to
    generate_report.py 2026-08-17 — no longer fetched here.)"""
    mmsi_to_type = {}
    with open(FLEET_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            mmsi = (row.get("MMSI") or "").strip()
            vtype = (row.get("Type") or "").strip()
            if mmsi.isdigit() and vtype == "Fish feed carrier":
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

# --------------------------------------------------------------------------
# Fôrforbruk: Fiskeridirektoratet (monthly, ~7 weeks lag) vs feed-vessel visits
# --------------------------------------------------------------------------
# Feed per visit is strongly seasonal (~82 t in winter, ~120 t in late summer)
# and rising over time (bigger loads), so a visits-only model flattens the
# peaks and lows. Model, fitted on full months with both series:
#   feed/day = a + b*visits/day + c*sin(2πm/12) + d*cos(2πm/12) + e*years
# Walk-forward error ~2% (Jan 2024–Aug 2026). The same per-day formula turns
# daily visits into weekly feed, so the weeks keep running past FD's last month.

FEED_HISTORY_START = datetime.date(2024, 1, 1)
FEED_WEEKS = 26
NO_MONTHS = ["jan", "feb", "mar", "apr", "mai", "jun", "jul", "aug", "sep", "okt", "nov", "des"]


def fetch_daily_feed_visits(client, mmsi_list, yesterday):
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("mmsi_list", "INT64", mmsi_list),
        bigquery.ScalarQueryParameter("start", "DATE", FEED_HISTORY_START),
        bigquery.ScalarQueryParameter("end", "DATE", yesterday)])
    rows = client.query("""
        SELECT DATE(startTime) AS d, COUNT(*) AS v
        FROM salmofin.salmofin.vessel_visits
        WHERE mmsi IN UNNEST(@mmsi_list) AND DATE(startTime) BETWEEN @start AND @end
        GROUP BY 1""", job_config=job_config).result()
    daily = {r.d: r.v for r in rows}
    d = FEED_HISTORY_START
    while d <= yesterday:               # days without visits count as zero
        daily.setdefault(d, 0)
        d += datetime.timedelta(days=1)
    return daily


def fetch_fd_feed(client):
    rows = client.query("""
        SELECT Ar AS y, Maaned_kode AS m, SUM(Forforbruk_kg)/1000 AS feed
        FROM salmofin.salmofin.biomass WHERE Ar >= 2023 GROUP BY 1,2""").result()
    return {(r.y, r.m): float(r.feed or 0) for r in rows}


def _solve(A, b):
    n = len(b)
    M = [row[:] + [b[i]] for i, row in enumerate(A)]
    for i in range(n):
        p = max(range(i, n), key=lambda r: abs(M[r][i]))
        M[i], M[p] = M[p], M[i]
        for j in range(n):
            if j != i and M[i][i]:
                f = M[j][i] / M[i][i]
                for c in range(i, n + 1):
                    M[j][c] -= f * M[i][c]
    return [M[i][n] / M[i][i] for i in range(n)]


def _ols(X, y):
    k = len(X[0])
    A = [[sum(x[i] * x[j] for x in X) for j in range(k)] for i in range(k)]
    b = [sum(x[i] * yy for x, yy in zip(X, y)) for i in range(k)]
    return _solve(A, b)


def _features(month_index, visits_per_day):
    m = month_index % 12
    years = (month_index - (FEED_HISTORY_START.year * 12)) / 12
    return [1.0, visits_per_day, math.sin(2 * math.pi * m / 12), math.cos(2 * math.pi * m / 12), years]


def _days_in_month(y, m):
    nxt = datetime.date(y + (m == 12), m % 12 + 1, 1)
    return (nxt - datetime.date(y, m, 1)).days


def build_fd_section(daily, fd, yesterday):
    # monthly visits for full months
    mv = defaultdict(int)
    for d, v in daily.items():
        mv[(d.year, d.month)] += v
    months = sorted(k for k in mv if datetime.date(k[0], k[1], _days_in_month(*k)) <= yesterday)
    fit_rows = [(k, mv[k], fd[k]) for k in months if k in fd and fd[k] > 0]
    X = [_features(k[0] * 12 + k[1] - 1, v / _days_in_month(*k)) for k, v, _ in fit_rows]
    Y = [f / _days_in_month(*k) for k, _, f in fit_rows]
    coef = _ols(X, Y)
    day_feed = lambda d, v: sum(c * x for c, x in zip(coef, _features(d.year * 12 + d.month - 1, v)))

    # walk-forward accuracy (train on everything before each month)
    errs = []
    for i in range(18, len(fit_rows)):
        cf = _ols(X[:i], Y[:i])
        pred = sum(c * x for c, x in zip(cf, X[i]))
        errs.append(abs(pred / Y[i] - 1))
    mape = sum(errs) / len(errs) if errs else 0.03

    # weekday pacing from the last 8 complete weeks (for the partial month)
    last_monday = yesterday - datetime.timedelta(days=yesterday.weekday() + 7)
    wd = defaultdict(list)
    for i in range(56):
        d = last_monday + datetime.timedelta(days=6) - datetime.timedelta(days=i)
        wd[d.weekday()].append(daily.get(d, 0))
    wd_avg = {k: sum(v) / len(v) for k, v in wd.items()}

    last_fd = max(fd)
    estimates = []
    y, m = last_fd
    while True:
        m += 1
        if m == 13:
            y, m = y + 1, 1
        start = datetime.date(y, m, 1)
        if start > yesterday:
            break
        n_days = _days_in_month(y, m)
        end = datetime.date(y, m, n_days)
        seen = min((yesterday - start).days + 1, n_days)
        if seen < 7:
            continue
        total, d = 0.0, start
        while d <= end:
            v = daily.get(d, 0) if d <= yesterday else wd_avg.get(d.weekday(), 0)
            total += day_feed(d, v)
            d += datetime.timedelta(days=1)
        band = mape * 1.5 + (0.05 * (1 - seen / n_days))
        estimates.append({"label": f"{NO_MONTHS[m-1]} {y}", "value": round(total), "band": round(band * 100, 1),
                          "partial": seen < n_days, "days": seen, "ndays": n_days})

    # monthly chart: last 24 FD months + estimates, model fit line
    fd_months = sorted(fd)[-24:]
    m_labels = [f"{NO_MONTHS[k[1]-1]} {str(k[0])[2:]}" for k in fd_months] + [e["label"].replace(" 20", " ") for e in estimates]
    m_actual = [round(fd[k]) for k in fd_months] + [None] * len(estimates)
    m_est = [None] * len(fd_months) + [e["value"] for e in estimates]
    m_fit = []
    for k in fd_months:
        if k in mv and k in months:
            m_fit.append(round(sum(day_feed(datetime.date(k[0], k[1], dd), 0) for dd in range(1, _days_in_month(*k) + 1))
                               + coef[1] * mv[k]))
        else:
            m_fit.append(None)
    m_fit += [e["value"] for e in estimates]

    # weekly implied feed (t/day) vs FD monthly (t/day)
    cur_monday = yesterday - datetime.timedelta(days=yesterday.weekday())
    w_labels, w_model, w_fd = [], [], []
    for i in range(FEED_WEEKS - 1, -1, -1):
        mon = cur_monday - datetime.timedelta(weeks=i)
        days = [mon + datetime.timedelta(days=j) for j in range(7) if mon + datetime.timedelta(days=j) <= yesterday]
        if not days:
            continue
        w_labels.append(f"U{mon.isocalendar()[1]}")
        w_model.append(round(sum(day_feed(d, daily.get(d, 0)) for d in days) / len(days)))
        mid = mon + datetime.timedelta(days=3)
        k = (mid.year, mid.month)
        w_fd.append(round(fd[k] / _days_in_month(*k)) if k in fd else None)

    # feed per visit by calendar month and year
    ratio = {}
    for k in months:
        if k in fd and mv[k]:
            ratio.setdefault(k[0], [None] * 12)[k[1] - 1] = round(fd[k] / mv[k], 1)

    lk = last_fd
    ly = (lk[0] - 1, lk[1])
    return {
        "last_label": f"{NO_MONTHS[lk[1]-1]} {lk[0]}",
        "last_value": round(fd[lk]),
        "last_yoy": round((fd[lk] / fd[ly] - 1) * 100, 1) if ly in fd else None,
        "estimates": estimates,
        "mape": round(mape * 100, 1),
        "n_fit": len(fit_rows),
        "m_labels": m_labels, "m_actual": m_actual, "m_est": m_est, "m_fit": m_fit,
        "w_labels": w_labels, "w_model": w_model, "w_fd": w_fd,
        "ratio": {str(yr): v for yr, v in sorted(ratio.items())},
    }


def render_fd_section(s):
    html = _render_fd(s)
    a, b = html.split('<!--SPLIT-->')
    return a, b


def _render_fd(s):
    def card(title, value, sub, sub_color="var(--text-muted)"):
        return (f'<div class="card"><div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">{title}</div>'
                f'<div style="font-size:24px;font-weight:500;">{value}</div>'
                f'<div style="font-size:12px;color:{sub_color};">{sub}</div></div>')
    yoy = s["last_yoy"]
    cards = [card(f"Fiskeridirektoratet, {s['last_label']}", f"{s['last_value']/1000:,.1f}k t".replace(",", " "),
                  f"{'+' if yoy >= 0 else ''}{yoy:.1f}% mot året før" if yoy is not None else "",
                  "#008300" if (yoy or 0) >= 0 else "#a32d2d")]
    for e in s["estimates"]:
        sub = f"±{e['band']:.0f}% · " + (f"anløp t.o.m. dag {e['days']} av {e['ndays']}" if e["partial"] else "fullført måned, anløp")
        cards.append(card(f"Anslag {e['label']}", f"{e['value']/1000:,.1f}k t".replace(",", " "), sub))
    data = json.dumps({k: s[k] for k in ("m_labels", "m_actual", "m_est", "m_fit", "w_labels", "w_model", "w_fd", "ratio")})
    return f"""
  <section>
    <div class="section-title">Fôrforbruk — Fiskeridirektoratet mot fôrbåtanløp</div>
    <div class="section-sub">Faktisk fôrforbruk (laks og ørret) fra Fiskeridirektoratet, månedlig med ca. 7 ukers forsinkelse.
      Anløpene oversettes til tonn med en modell som korrigerer for sesong og større laster over tid. Anslagene dekker månedene etter siste FD-tall.</div>
    <div style="display:grid;grid-template-columns:repeat({min(len(cards),3)},1fr);gap:12px;margin-bottom:14px;">{''.join(cards)}</div>

    <div class="chart-title">Fôrforbruk per måned, tonn</div>
    <div style="position:relative;width:100%;height:190px;margin-bottom:4px;"><canvas id="fdMonthChart" width="640" height="190"></canvas></div>
    <div class="legend-row"><span><i style="background:#2a78d6"></i>Fiskeridirektoratet</span><span><i style="background:#2a78d655;border:1px dashed #2a78d6"></i>Anslag fra anløp</span><span><i class="ln" style="background:#d68a2a"></i>Modell</span></div>

    <div class="chart-title" style="margin-top:18px;">Tonn fôr per dag, ukentlig</div>
    <div style="position:relative;width:100%;height:170px;margin-bottom:4px;"><canvas id="fdWeekChart" width="640" height="170"></canvas></div>
    <div class="legend-row"><span><i class="ln" style="background:#2a78d6"></i>Anslag fra anløp (uke)</span><span><i class="ln" style="background:#0b0b0b"></i>Fiskeridirektoratet (månedssnitt)</span></div>
    <div style="font-size:11px;color:var(--text-muted);margin-bottom:12px;">Ukene fortsetter etter siste FD-måned; siste uke er delvis.</div>

    <div class="chart-title" style="margin-top:18px;">Tonn fôr per anløp</div>
    <div style="position:relative;width:100%;height:150px;margin-bottom:4px;"><canvas id="fdRatioChart" width="640" height="150"></canvas></div>
    <div style="font-size:11px;color:var(--text-muted);">Lastene er større om sommeren og høsten og har økt fra år til år; derfor undervurderer rene anløpstall både topper og bunner.
      Modellens treffsikkerhet utenfor utvalget: ±{s['mape']:.1f}% per måned ({s['n_fit']} måneder).</div>
  </section>
<!--SPLIT--><script>
(function(){{
const D={data};const grid='#e1e0d9',tick={{color:'#898781',font:{{size:10}}}};
new Chart(document.getElementById('fdMonthChart'),{{data:{{labels:D.m_labels,datasets:[
  {{type:'bar',data:D.m_actual,backgroundColor:'#2a78d6',borderRadius:3,order:2}},
  {{type:'bar',data:D.m_est,backgroundColor:'#2a78d655',borderColor:'#2a78d6',borderWidth:1,borderDash:[4,3],borderRadius:3,order:2}},
  {{type:'line',data:D.m_fit,borderColor:'#d68a2a',borderWidth:2,pointRadius:0,spanGaps:true,order:1}}]}},
  options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{x:{{stacked:true,ticks:tick,grid:{{display:false}}}},y:{{ticks:tick,grid:{{color:grid}}}}}}}}}});
new Chart(document.getElementById('fdWeekChart'),{{type:'line',data:{{labels:D.w_labels,datasets:[
  {{data:D.w_model,borderColor:'#2a78d6',borderWidth:2,pointRadius:2}},
  {{data:D.w_fd,borderColor:'#0b0b0b',borderWidth:1.5,pointRadius:0,stepped:true,spanGaps:false}}]}},
  options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:false}}}},scales:{{x:{{ticks:tick,grid:{{display:false}}}},y:{{ticks:tick,grid:{{color:grid}}}}}}}}}});
const cols={{'2024':'#b4b2a9','2025':'#898781','2026':'#2a78d6'}};
new Chart(document.getElementById('fdRatioChart'),{{type:'line',data:{{labels:{json.dumps(NO_MONTHS)},datasets:Object.entries(D.ratio).map(([y,v])=>({{label:y,data:v,borderColor:cols[y]||'#52514e',borderWidth:2,pointRadius:2}}))}},
  options:{{responsive:true,maintainAspectRatio:false,plugins:{{legend:{{display:true,labels:{{boxWidth:10,font:{{size:11}}}}}}}},scales:{{x:{{ticks:tick,grid:{{display:false}}}},y:{{ticks:tick,grid:{{color:grid}}}}}}}}}});
}})();
</script>"""


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
  .chart-title {{ font-size:13px; color:var(--text-secondary); margin-bottom:4px; }}
  .legend-row {{ display:flex; gap:14px; flex-wrap:wrap; font-size:11px; color:var(--text-muted); margin-bottom:6px; }}
  .legend-row i {{ display:inline-block; width:10px; height:10px; border-radius:2px; margin-right:5px; vertical-align:-1px; }}
  .legend-row i.ln {{ height:2px; width:14px; vertical-align:3px; }}
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

{fd_section}
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
</script>
{fd_script}
</body>
</html>
"""

if __name__ == "__main__":
    mmsi_to_type = load_fleet()
    print(f"  {len(mmsi_to_type)} feed vessels in vessel_categories.csv")

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

    fd_daily = fetch_daily_feed_visits(client, list(mmsi_to_type.keys()), yesterday)
    fd_feed = fetch_fd_feed(client)
    fd_stats = build_fd_section(fd_daily, fd_feed, yesterday)
    fd_section, fd_script = render_fd_section(fd_stats)
    print(f"  FD feed: last {fd_stats['last_label']} {fd_stats['last_value']} t; estimates {fd_stats['estimates']}; walk-forward MAPE {fd_stats['mape']}%")

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
        pacing_weeks=PACING_WEEKS,
        fd_section=fd_section,
        fd_script=fd_script,
    )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
