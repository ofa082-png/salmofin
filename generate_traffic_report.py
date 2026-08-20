"""
generate_traffic_report.py
---------------------------
Renders a weekly-focused vessel traffic report for traders/exporters,
scoped to Wellboat + Processing vessel — the harvest-signal fleet —
in vessel_categories.csv. Sections:

  A/B. Harvest signal — Wellboat + Processing vessel locality visits
       (vessel_visits) and harvest-plant deliveries (harvest_plant_visits
       CSVs), with a weekday-pacing forecast for the current week.

Feed carrier + silage traffic moved to generate_foring.py (2026-08-16)
— that's a production-intensity signal, not a harvest-logistics one,
so it didn't belong bundled in here just because it shares the same
AIS data source. Avlusningsfartøy (delousing vessel visits) moved to
generate_report.py/fiskehelse.html (2026-08-17) for the same reason —
it's a fish-health-adjacent signal, not harvest-logistics.

Export volume forecast (fetch_export_regression et al.) switched its
underlying predictor from locality visits (vessel_visits — vessels
visiting farm sites generally) to harvest-*plant* visits
(harvest_plant_visits CSVs — vessels physically arriving at
slaughterhouses) on 2026-08-19, plus added a same-week vessel-capacity
term. Confirmed via live backtest to close much of a systematic
under-prediction the locality-based model had developed over several
recent weeks (e.g. week 31: -9% -> -4%). The locality-visit section
("Slakteaktivitet — lokalitetsanløp") stays locality-based — it's
useful for its own purpose (broad, same-day activity tracking), just
not as an export predictor.

Writes docs/traffic.html.
"""

import os
import csv
import json
import glob
import math
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
PLANT_WEEKS_HISTORY = WEEKS_HISTORY  # kept equal to the locality-visit lookback for a consistent trend window

HARVEST_LABELS = {
    "Alle":              "Alle",
    "Wellboat":          "Brønnbåt",
    "Processing vessel": "Prosesseringsfartøy",
}
HARVEST_ORDER = ["Alle", "Wellboat", "Processing vessel"]

NO_WEEKDAY = ["mandag", "tirsdag", "onsdag", "torsdag", "fredag", "lørdag", "søndag"]
NO_WEEKDAY_SHORT = ["Man", "Tir", "Ons", "Tor", "Fre", "Lør", "Søn"]

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

def _solve_linear(A, B):
    """Gaussian elimination with partial pivoting for a general NxN
    system — avoids adding numpy as a pipeline dependency for one small
    linear solve. Replaces the old Cramer's-rule 3x3 solver now that the
    export regression has grown past 3 unknowns (seasonal + trend
    terms, see fetch_export_regression)."""
    n = len(B)
    M = [row[:] + [B[i]] for i, row in enumerate(A)]
    for col in range(n):
        pivot = max(range(col, n), key=lambda r: abs(M[r][col]))
        if abs(M[pivot][col]) < 1e-9:
            return None
        M[col], M[pivot] = M[pivot], M[col]
        pv = M[col][col]
        M[col] = [x / pv for x in M[col]]
        for r in range(n):
            if r != col:
                factor = M[r][col]
                M[r] = [x - factor * y for x, y in zip(M[r], M[col])]
    return [M[i][n] for i in range(n)]

def parse_plant_capacity(capacity_str, unit_str):
    """Vessel capacity in tonnes from a harvest_plant_visits CSV row —
    m3 is converted to tonnes at 0.1 t/m3, the standard wellboat
    conversion used throughout this project."""
    try:
        val = float(capacity_str)
    except (TypeError, ValueError):
        return 0.0
    return val * 0.1 if (unit_str or "").strip().lower() == "m3" else val

def fetch_plant_export_series():
    """{(year, week): {"visit_count": int, "capacity_t": float}} from
    every local harvest_plant_visits CSV — completed weeks and the
    current in-progress week both included (the caller decides which
    weeks to actually use). This is the export regression's input as of
    2026-08-19 (see fetch_export_regression's docstring for why), built
    from the same CSVs the plant-status/plant-visit-matrix sections of
    this page already read. Named distinctly from the pre-existing
    fetch_plant_weekly_series(n_weeks) — that one returns per-vessel-
    type visit *counts only* for the activity chart; this one is
    export-regression-specific (visit count + capacity, keyed by
    (year, week) for direct lookup, not a fixed n_weeks window)."""
    weekly = defaultdict(lambda: {"visit_count": 0, "capacity_t": 0.0})
    for path in all_plant_csvs():
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                wk = weekly[(int(row["year"]), int(row["week"]))]
                wk["visit_count"] += 1
                wk["capacity_t"] += parse_plant_capacity(row["capacity"], row["capacity_unit"])
    return weekly

def fetch_plant_current_week_daily():
    """{date: {"visits": int, "capacity_t": float}} for just the current
    in-progress week's plant CSV, keyed by entry_time's date — used to
    pace-project this week's plant visits/capacity for the live forecast
    card. Returns {} if no partial file exists yet (e.g. very early
    Monday before the day's fetch has run)."""
    path = current_week_plant_path()
    if not path:
        return {}
    daily = defaultdict(lambda: {"visits": 0, "capacity_t": 0.0})
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            d = datetime.date.fromisoformat(row["entry_time"][:10])
            rec = daily[d]
            rec["visits"] += 1
            rec["capacity_t"] += parse_plant_capacity(row["capacity"], row["capacity_unit"])
    return daily

def fetch_export_regression(client, plant_weekly):
    """Fit exports_tonn[i] ~ a*visits[i] + b*visits[i-1] + g*capacity_t[i]
    + d*sin(2*pi*wk/52) + e*cos(2*pi*wk/52) + f*trend_weeks + c using
    every matched (year, week) of harvest-*plant* visits (vessels
    physically arriving at slaughterhouses) vs. BigQuery export data.
    Refit live on every run — rather than hardcoding coefficients — so
    the relationship self-corrects as the fleet or export mix drifts,
    instead of silently going stale.

    Switched from BigQuery *locality* visits (vessel_visits — vessels
    visiting farm sites generally, one step removed from the actual
    slaughter event) to harvest-*plant* visits (harvest_plant_visits
    CSVs) on 2026-08-19. A capacity-only patch on top of the old
    locality-based model was tried first and genuinely improved overall
    fit (R^2 0.82->0.88) but did NOT close a real, live systematic
    under-prediction spanning several recent weeks (confirmed against
    an independently-built plant-visits+capacity model that tracked
    those same weeks 3-6x tighter, e.g. week 31: locality-based -6.6%
    vs. plant-based +1.6%) — the plant signal itself, not capacity, is
    what was missing. Same two-visit-term / seasonal-harmonic / trend
    structure as the old model, just fed the better predictor, plus the
    capacity term (kept, since it's a real improvement on its own).

    Two visit terms, not one: a single-variable same-week-only fit gets
    real independent signal from last week's *already-known* (not
    forecast) visit count too — a day-level lag scan on the old
    locality-based model peaked at a 2-3 day shift, consistent with the
    real harvest-to-export processing lag — adding it as a second term
    costs zero extra forecast uncertainty since it's always already known.

    Seasonal harmonic + linear trend, on top of that: exports have a
    real annual pattern (calmer H1, a Jul-Sep ramp) that a pure
    visits-only fit can't see, and a slow multi-year drift toward more
    tonnes per vessel trip (bigger/fuller loads over time) that the
    same-week visit count alone doesn't fully capture either."""
    export_rows = list(client.query("""
        SELECT year AS yr, week AS wk, SUM(vekt_tonn) AS export_tonn
        FROM `salmofin.salmofin.salmon_export_weekly`
        GROUP BY yr, wk
    """).result())
    export_lookup = {(int(r.yr), int(r.wk)): r.export_tonn for r in export_rows}

    keys = sorted(k for k in plant_weekly if k in export_lookup)

    # 7 unknowns now (was 6) — keep a wider safety margin than the old
    # "12" floor so the fit isn't running on a handful more rows than
    # parameters. Local plant-CSV history is 130+ weeks (2024+), far past this.
    if len(keys) < 26:
        return None

    visits = [plant_weekly[k]["visit_count"] for k in keys]
    capacity = [plant_weekly[k]["capacity_t"] for k in keys]
    exports = [export_lookup[k] for k in keys]
    # ISO (year, week) -> Monday date, so trend can be a plain weeks-since-
    # start count rather than dealing with 52/53-week-year ISO-week math.
    mondays = [datetime.date.fromisocalendar(k[0], k[1], 1) for k in keys]
    ref_monday = mondays[0]

    xs = visits[1:]        # this-week visits
    xs_prev = visits[:-1]  # last-week visits (known, not forecast)
    caps = capacity[1:]    # this-week capacity
    ys = exports[1:]
    wks = [k[1] for k in keys[1:]]
    trend = [(mondays[i] - ref_monday).days // 7 for i in range(1, len(keys))]
    m = len(ys)

    # design matrix columns: [visits, visits_prev, capacity, sin, cos, trend, 1]
    design = [
        [xs[i], xs_prev[i], caps[i], math.sin(2 * math.pi * wks[i] / 52), math.cos(2 * math.pi * wks[i] / 52), trend[i], 1.0]
        for i in range(m)
    ]
    p = len(design[0])
    A = [[0.0] * p for _ in range(p)]
    B = [0.0] * p
    for row, y in zip(design, ys):
        for i in range(p):
            B[i] += row[i] * y
            for j in range(p):
                A[i][j] += row[i] * row[j]

    coef = _solve_linear(A, B)
    if coef is None:
        return None
    a, b, g, d, e, f, c = coef

    preds = [sum(coef_i * x_i for coef_i, x_i in zip(coef, row)) for row in design]
    my = sum(ys) / m
    ss_res = sum((y - p_) ** 2 for y, p_ in zip(ys, preds))
    ss_tot = sum((y - my) ** 2 for y in ys)
    if ss_tot == 0:
        return None
    r2 = 1 - ss_res / ss_tot
    rmse = (ss_res / m) ** 0.5

    return {
        "a": a,
        "b": b,
        "g": g,
        "d": d,
        "e": e,
        "f": f,
        "c": c,
        "ref_monday": ref_monday,
        "r2": r2,
        "rmse_pct": round(rmse / my * 100) if my else 0,
        "n_weeks": m,
    }

def _predict_export(regression, visits, prev_visits, capacity, monday):
    """Apply a fitted export_regression to one (visits, prev_visits,
    capacity, week) point. `monday` gives both the ISO week (for the
    seasonal term) and the trend position (weeks since the regression's
    ref_monday) — shared by the live forecast card and the backtest
    table so both use exactly the same model, not a re-derived copy."""
    wk = monday.isocalendar()[1]
    trend = (monday - regression["ref_monday"]).days // 7
    return (
        regression["a"] * visits
        + regression["b"] * prev_visits
        + regression["g"] * capacity
        + regression["d"] * math.sin(2 * math.pi * wk / 52)
        + regression["e"] * math.cos(2 * math.pi * wk / 52)
        + regression["f"] * trend
        + regression["c"]
    )

def fetch_export_lookup(client, min_year):
    """{(year, week): export_tonn} for actual published exports — used to
    check the regression's predictions against reality, and to detect
    "not published yet" (a missing key) for the most recent 1-2 weeks,
    since official export stats lag ~3-4 days behind the week itself."""
    rows = list(client.query(f"""
        SELECT year AS yr, week AS wk, SUM(vekt_tonn) AS export_tonn
        FROM `salmofin.salmofin.salmon_export_weekly`
        WHERE year >= {min_year}
        GROUP BY yr, wk
    """).result())
    return {(r.yr, r.wk): r.export_tonn for r in rows}

def build_export_backtest_rows(regression, weekly_mondays, plant_weekly, export_lookup, n_weeks):
    """Predicted-vs-actual for the last n_weeks *completed* weeks —
    `weekly_mondays` must already exclude the current partial week (its
    own prediction is the live forecast card, not this table). Each
    prediction uses fully-known plant visit counts and capacity — no
    forecast uncertainty — so any gap between predicted and actual here
    is purely model error, not projection error. `plant_weekly` is
    keyed by (year, week) directly (see fetch_plant_export_series), so
    each backtested week — and its "last week" term — is looked up
    directly rather than needing a parallel index-aligned array."""
    rows = []
    for monday in weekly_mondays[-n_weeks:]:
        iso = monday.isocalendar()
        prev_iso = (monday - datetime.timedelta(weeks=1)).isocalendar()
        wk = plant_weekly.get((iso[0], iso[1]), {"visit_count": 0, "capacity_t": 0.0})
        prev_wk = plant_weekly.get((prev_iso[0], prev_iso[1]), {"visit_count": 0, "capacity_t": 0.0})
        predicted = _predict_export(regression, wk["visit_count"], prev_wk["visit_count"], wk["capacity_t"], monday)
        actual = export_lookup.get((iso[0], iso[1]))
        diff_pct = ((predicted - actual) / actual * 100) if actual else None
        rows.append({
            "label": f"U{iso[1]}",
            "predicted": round(predicted),
            "actual": round(actual) if actual is not None else None,
            "diff_pct": diff_pct,
        })
    return rows

def build_export_backtest_section(rows):
    """Small predicted-vs-actual table for the last few completed weeks —
    the most recent row(s) typically show "ikke publisert ennå" since
    official export stats lag ~3-4 days behind the week, and the rest
    let you see how the model has actually been tracking."""
    if not rows:
        return ""
    trs = []
    for r in rows:
        actual_cell = f"{r['actual']:,.0f} t" if r["actual"] is not None else '<span style="color:var(--text-muted);">ikke publisert ennå</span>'
        if r["diff_pct"] is not None:
            color = "#008300" if abs(r["diff_pct"]) <= 10 else "#a32d2d"
            diff_cell = f'<span style="color:{color};">{diff_label(r["diff_pct"])}</span>'
        else:
            diff_cell = ""
        trs.append(f"""
      <tr style="border-top:0.5px solid var(--border);">
        <td style="padding:8px 10px;">{r['label']}</td>
        <td style="padding:8px 10px;text-align:right;">{r['predicted']:,.0f} t</td>
        <td style="padding:8px 10px;text-align:right;">{actual_cell}</td>
        <td style="padding:8px 10px;text-align:right;">{diff_cell}</td>
      </tr>""")
    return f"""
    <div style="font-size:12px;color:var(--text-muted);margin:8px 0;">Prognose vs. faktisk eksportvolum, siste uker (begge tall bruker kun kjente anløp — ingen prognoseusikkerhet):</div>
    <div style="border:0.5px solid var(--border);border-radius:8px;overflow:hidden;overflow-x:auto;margin-bottom:14px;">
      <table style="font-size:13px;table-layout:fixed;">
        <thead>
        <tr style="background:var(--surface-2);">
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Uke</td>
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Anslått</td>
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Faktisk</td>
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Avvik</td>
        </tr>
        </thead>
        <tbody>{"".join(trs)}</tbody>
      </table>
    </div>"""

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

def build_weekday_avg_counts(daily, current_monday, pacing_weeks):
    """avg visit count per weekday (0=Mon..6=Sun), based on the
    `pacing_weeks` completed weeks immediately before `current_monday` —
    the baseline a "this week" / "last week" line gets compared against."""
    sums = [0] * 7
    for w in range(1, pacing_weeks + 1):
        m = current_monday - datetime.timedelta(weeks=w)
        for i in range(7):
            d = m + datetime.timedelta(days=i)
            sums[i] += daily.get(d, {}).get("visits", 0)
    return [round(s / pacing_weeks, 1) for s in sums]

def build_weekday_actuals(daily, monday, upto=None):
    """actual visit count per weekday for the week starting `monday`.
    Days after `upto` (if given) are None — not yet occurred, so a line
    chart just stops there instead of drawing a false zero."""
    result = []
    for i in range(7):
        d = monday + datetime.timedelta(days=i)
        if upto is not None and d > upto:
            result.append(None)
        else:
            result.append(daily.get(d, {}).get("visits", 0))
    return result

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

def build_harvest_group_data(daily, current_monday, yesterday, two_days_ago, plant_weekly=None, plant_current=None, plant_weekday=None):
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
    weekday_avg = build_weekday_avg_counts(daily, current_monday, PACING_WEEKS)
    weekday_this_week = build_weekday_actuals(daily, current_monday, upto=yesterday)
    weekday_last_week = build_weekday_actuals(daily, lw_monday, upto=None)

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
        "weekday_avg": weekday_avg,
        "weekday_this_week": weekday_this_week,
        "weekday_last_week": weekday_last_week,
        "yesterday_visits": y_visits,
        "y_diff_label": diff_label(y_diff_pct),
        "y_diff_color": diff_color(y_diff_pct),
        "yesterday_vessels": y_vessels,
        "yesterday_localities": y_localities,
    }

    if plant_weekly is not None:
        # p_last/p_prev (the "forrige uke" tile) always come from the
        # completed-weeks-only series — a partial current week must never
        # feed into that comparison. It's appended to the chart series
        # afterwards, purely as an extra (lighter-shaded) trend bar.
        p_labels = [w[0] for w in plant_weekly]
        p_values = [w[1] for w in plant_weekly]
        p_last = p_values[-1] if p_values else 0
        p_prev = p_values[-2] if len(p_values) > 1 else 0
        p_diff_pct = ((p_last - p_prev) / p_prev * 100) if p_prev else 0

        chart_labels = list(p_labels)
        chart_values = list(p_values)
        chart_partial_idx = None
        if plant_current is not None:
            cur_label, cur_value = plant_current
            chart_labels.append(cur_label)
            chart_values.append(cur_value)
            chart_partial_idx = len(chart_values) - 1

        result.update({
            "plant_weekly_labels": chart_labels,
            "plant_weekly_values": chart_values,
            "plant_weekly_partial_idx": chart_partial_idx,
            "plant_last_week": p_last,
            "plant_diff_label": diff_label(p_diff_pct),
            "plant_diff_color": diff_color(p_diff_pct),
        })

    if plant_weekday is not None:
        result.update({
            "plant_weekday_avg": plant_weekday["avg"],
            "plant_weekday_last_week": plant_weekday["last_week"],
            "plant_weekday_this_week": plant_weekday.get("this_week", [None] * 7),
        })

    return result

def all_plant_csvs():
    return sorted(glob.glob(os.path.join(BASE_DIR, "data", "harvest_plant_visits_*.csv")))

def current_week_plant_path():
    """Path to the in-progress week's plant CSV, refreshed daily by
    fetch_harvest_visits.py — may not exist yet (e.g. very early Monday
    before the first vessel track has any pings)."""
    iso = datetime.date.today().isocalendar()
    path = os.path.join(BASE_DIR, "data", f"harvest_plant_visits_{iso[0]}_W{iso[1]:02d}.csv")
    return path if os.path.exists(path) else None

def completed_plant_csvs():
    """All plant CSVs excluding the current in-progress week's file, so
    weekly/weekday averages and the "last completed week" figures never
    get contaminated by a still-growing partial week."""
    current = current_week_plant_path()
    return [f for f in all_plant_csvs() if f != current]

def latest_plant_csv():
    files = completed_plant_csvs()
    return files[-1] if files else None

def fetch_plant_current_week_counts():
    """Per vessel type: (label, count) for the current in-progress week,
    or None if no partial file exists yet."""
    path = current_week_plant_path()
    if not path:
        return None
    label = os.path.basename(path).replace("harvest_plant_visits_", "").replace(".csv", "").split("_")[-1]
    counts = {"Alle": 0, "Wellboat": 0, "Processing vessel": 0}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            counts["Alle"] += 1
            vtype = row["vessel_type"].strip()
            if vtype in counts:
                counts[vtype] += 1
    return {k: (label, v) for k, v in counts.items()}

def fetch_plant_weekday_this_week():
    """Per vessel type: actual plant-visit count per weekday (of
    entry_time) for the current in-progress week, with None for weekdays
    not yet reached — mirrors build_weekday_actuals for the locality
    (vessel_visits) side."""
    path = current_week_plant_path()
    result = {"Alle": [0] * 7, "Wellboat": [0] * 7, "Processing vessel": [0] * 7}
    if not path:
        return {k: [None] * 7 for k in result}
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            vtype = row["vessel_type"].strip()
            weekday = datetime.date.fromisoformat(row["entry_time"][:10]).weekday()
            for key in ("Alle", vtype):
                result[key][weekday] += 1
    today_weekday = datetime.date.today().weekday()
    return {k: [v if i <= today_weekday else None for i, v in enumerate(vals)] for k, vals in result.items()}

def fetch_plant_status():
    """Latest-week plant ranking per vessel type — every plant with
    activity, not just the top N. The CSV is already restricted to
    Wellboat + Processing vessel (fetch_harvest_visits.py only tracks
    those two types against harvest plants)."""
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
        key: sorted(plants.items(), key=lambda kv: -kv[1]["capacity"])
        for key, plants in plants_by_type.items()
    }
    return ranked_by_type, week_label

NO_PLANT_DATA_ROW = ('<tr><td colspan="5" style="padding:14px 10px;color:var(--text-muted);'
                      'text-align:center;">Ingen slakterianløp registrert for denne fartøytypen.</td></tr>')

PLANT_MATRIX_WEEKS = 8   # weeks of history shown in the per-plant sparkline column
SPARK_BAR_H = 24         # px

def fetch_plant_visit_matrix(ranked_by_type, n_weeks):
    """Per type: weekly visit counts for each of that type's ranked plants,
    over the last n_weeks completed weeks plus the current partial week
    (if a file for it exists) — the data behind the "Anløp" sparkline
    column. Kept separate from fetch_plant_status (which only reads the
    single latest completed week) since this needs several files."""
    completed = completed_plant_csvs()[-n_weeks:]
    current = current_week_plant_path()
    files = completed + ([current] if current else [])
    labels = [os.path.basename(p).replace("harvest_plant_visits_", "").replace(".csv", "").split("_")[-1] for p in files]
    partial_idx = len(files) - 1 if current else None

    counts = defaultdict(lambda: defaultdict(lambda: [0] * len(files)))
    for idx, path in enumerate(files):
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                vtype = row["vessel_type"].strip()
                name = row["plant_name"]
                for key in ("Alle", vtype):
                    counts[name][key][idx] += 1

    matrix_by_type = {}
    for type_key, ranked in ranked_by_type.items():
        rows = {name: counts[name][type_key] for name, _ in ranked}
        max_val = max((max(vals) for vals in rows.values()), default=0)
        matrix_by_type[type_key] = {"labels": labels, "partial_idx": partial_idx, "rows": rows, "max_val": max_val}
    return matrix_by_type

def render_sparkline(values, labels, partial_idx, max_val):
    bars = []
    for i, v in enumerate(values):
        h = max(2, round((v / max_val) * SPARK_BAR_H)) if v and max_val else 1
        opacity = "0.5" if i == partial_idx else "1"
        bars.append(
            f'<div title="{labels[i]}: {v}" style="width:5px;height:{h}px;'
            f'background:var(--accent);opacity:{opacity};border-radius:1px;"></div>'
        )
    return (f'<div style="display:flex;align-items:flex-end;gap:2px;height:{SPARK_BAR_H}px;">'
            f'{"".join(bars)}</div>')

def build_plant_rows(ranked, matrix=None):
    if not ranked:
        return NO_PLANT_DATA_ROW
    rows = []
    for name, p in ranked:
        last_date = p["last_exit"][:10] if p["last_exit"] else ""
        if matrix and name in matrix["rows"]:
            anlop_cell = render_sparkline(matrix["rows"][name], matrix["labels"], matrix["partial_idx"], matrix["max_val"])
        else:
            anlop_cell = str(p["visits"])
        rows.append(f"""
      <tr style="border-top:0.5px solid var(--border);">
        <td style="padding:8px 10px;">{name.title()}</td>
        <td style="padding:8px 10px;">{p['company'].title()}</td>
        <td style="padding:8px 10px;">{anlop_cell}</td>
        <td style="padding:8px 10px;text-align:right;">{p['capacity']:,.0f} t</td>
        <td style="padding:8px 10px;text-align:right;color:var(--text-secondary);">{last_date}</td>
      </tr>""")
    return "".join(rows)

def fetch_plant_weekly_series(n_weeks):
    """Weekly plant-visit totals per vessel type, from the last n_weeks
    completed harvest_plant_visits CSVs (the in-progress week, if any, is
    added separately by the caller so it can be marked partial)."""
    files = completed_plant_csvs()[-n_weeks:]
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

def fetch_plant_weekday_series(n_weeks):
    """Per vessel type: avg plant-visit count per weekday (of entry_time)
    across the last n_weeks completed weeks, plus the actual per-weekday
    count for just the newest completed week — the "last week" line to
    compare against that average. The current in-progress week (if any)
    is handled separately by fetch_plant_weekday_this_week."""
    files = completed_plant_csvs()[-n_weeks:]
    avg_sums = {"Alle": [0] * 7, "Wellboat": [0] * 7, "Processing vessel": [0] * 7}
    last_week_counts = {"Alle": [0] * 7, "Wellboat": [0] * 7, "Processing vessel": [0] * 7}
    for idx, path in enumerate(files):
        is_last = (idx == len(files) - 1)
        with open(path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                vtype = row["vessel_type"].strip()
                weekday = datetime.date.fromisoformat(row["entry_time"][:10]).weekday()
                for key in ("Alle", vtype):
                    if key not in avg_sums:
                        continue
                    avg_sums[key][weekday] += 1
                    if is_last:
                        last_week_counts[key][weekday] += 1
    result = {}
    for key in avg_sums:
        result[key] = {
            "avg": [round(s / len(files), 1) for s in avg_sums[key]] if files else [0] * 7,
            "last_week": last_week_counts[key],
        }
    return result

def build_export_forecast_card(regression, this_week_visit_forecast, prev_week_visits, this_week_capacity_forecast, current_monday):
    """Card showing this week's projected total export tonnage, derived
    from this week's harvest-*plant*-visit forecast (paced from the
    current in-progress week's plant CSV, refreshed daily — see
    fetch_plant_current_week_daily) plus this week's paced capacity
    forecast, last week's already-known actual plant visit count, and a
    regression fit live against BigQuery export data accounting for
    seasonal position and long-run trend too.

    This week's visit/capacity pacing reuses the same weekday-elapsed
    fraction already computed for the locality-visit forecast elsewhere
    on this page (build_harvest_group_data's `frac`), rather than
    building a second, separate plant-specific pacing curve — plant and
    locality visits come from the same wellboat/processing fleet, so
    the fraction of a typical week elapsed by today is a reasonable
    shared proxy for both, not something that needs re-deriving per
    data source.

    Independent of the Alle/Brønnbåt/Prosesseringsfartøy pill — exports
    aren't a per-vessel-type quantity, so this always uses the combined
    numbers regardless of which pill is currently selected."""
    if regression is None or prev_week_visits is None:
        return ""
    predicted = round(_predict_export(regression, this_week_visit_forecast, prev_week_visits, this_week_capacity_forecast, current_monday))
    return f"""
    <div class="card" style="margin-bottom:14px;border:1px solid var(--accent);">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Anslått eksportvolum denne uken</div>
      <div style="font-size:24px;font-weight:500;">{predicted:,.0f} t</div>
      <div style="font-size:12px;color:var(--text-muted);">Utledet fra slakterianløp-prognose denne uken (anløp + kapasitet) + faktiske anløp forrige uke, justert for sesongvariasjon og trend. Modell tilpasset live mot eksportdata: R²={regression['r2']:.2f}, avvik ~±{regression['rmse_pct']}% (siste {regression['n_weeks']} uker). Ikke offisielle tall.</div>
    </div>"""

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
    <div style="display:flex;gap:6px;">
      <a href="index.html" style="font-size:11px;border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">hjem →</a>
      <a href="foring.html" style="font-size:11px;border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">fôring →</a>
      <a href="big_vessels.html" style="font-size:11px;border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">store fartøy →</a>
      <a href="fiskehelse.html" style="font-size:11px;border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">fiskehelse →</a>
    </div>
  </div>

  <section>
    <div class="section-title">Slakteaktivitet — lokalitetsanløp</div>
    <div class="section-sub">Brønnbåt og prosesseringsfartøy ved oppdrettslokaliteter (ikke slakteri). BarentsWatch AIS.</div>
    <div id="harvestPills" style="display:flex;gap:6px;margin-bottom:12px;">{harvest_pills}</div>

    {export_forecast_card}
    {export_backtest_section}

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
    <div style="font-size:11px;color:var(--text-muted);margin-bottom:16px;">Siste søyle er inneværende uke (delvis).</div>

    <div style="font-size:13px;color:var(--text-secondary);margin-bottom:2px;">Ukedagsmønster: denne uken og forrige uke mot snitt (siste {pacing_weeks} fullførte uker)</div>
    <div style="position:relative;width:100%;height:150px;">
      <canvas id="harvestWeekdayChart" width="640" height="150"></canvas>
    </div>
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

    <div style="font-size:13px;color:var(--text-secondary);margin-bottom:2px;">Ukedagsmønster: denne uken (delvis) og forrige uke ({plant_week}) mot snitt (siste {plant_weeks_history} uker)</div>
    <div style="position:relative;width:100%;height:150px;margin-bottom:14px;">
      <canvas id="plantWeekdayChart" width="640" height="150"></canvas>
    </div>

    <div style="font-size:12px;color:var(--text-muted);margin-bottom:8px;">Status per slakteri, uke {plant_week}. Kapasitet = summert fartøykapasitet ved anløp, ikke bekreftet levert volum. Siste (lysere) søyle i Anløp-kolonnen er inneværende uke (delvis).</div>
    <div style="border:0.5px solid var(--border);border-radius:8px;overflow:hidden;overflow-x:auto;">
      <table style="font-size:13px;table-layout:fixed;">
        <thead>
        <tr style="background:var(--surface-2);">
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Anlegg</td>
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Selskap</td>
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;">Anløp, siste {plant_matrix_weeks} uker</td>
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Kapasitet</td>
          <td style="padding:8px 10px;color:var(--text-secondary);font-weight:500;text-align:right;">Siste</td>
        </tr>
        </thead>
        <tbody id="plantRows"></tbody>
      </table>
    </div>
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

const harvestWeekdayChart = new Chart(document.getElementById('harvestWeekdayChart'), {{
  data: {{ labels: {no_weekday_short_json}, datasets: [
    {{ type: 'bar', label: 'Snitt', data: [], backgroundColor: '#e1e0d9', borderRadius: 3, order: 3 }},
    {{ type: 'line', label: 'Forrige uke', data: [], borderColor: '#898781', backgroundColor: '#898781', tension: 0.25, pointRadius: 3, order: 2 }},
    {{ type: 'line', label: 'Denne uken', data: [], borderColor: '#2a78d6', backgroundColor: '#2a78d6', tension: 0.25, pointRadius: 3, order: 1, spanGaps: false }}
  ] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: true, labels: {{ color: '#898781', font: {{ size: 11 }}, boxWidth: 10 }} }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }}, x: {{ ticks: {{ color: '#898781', font: {{ size: 10 }} }}, grid: {{ display: false }} }} }} }}
}});

const plantWeekdayChart = new Chart(document.getElementById('plantWeekdayChart'), {{
  data: {{ labels: {no_weekday_short_json}, datasets: [
    {{ type: 'bar', label: 'Snitt', data: [], backgroundColor: '#e1e0d9', borderRadius: 3, order: 3 }},
    {{ type: 'line', label: 'Forrige uke', data: [], borderColor: '#898781', backgroundColor: '#898781', tension: 0.25, pointRadius: 3, order: 2 }},
    {{ type: 'line', label: 'Denne uken', data: [], borderColor: '#2a78d6', backgroundColor: '#2a78d6', tension: 0.25, pointRadius: 3, order: 1, spanGaps: false }}
  ] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: true, labels: {{ color: '#898781', font: {{ size: 11 }}, boxWidth: 10 }} }} }},
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
  plantWeeklyChart.data.datasets[0].backgroundColor = d.plant_weekly_partial_idx === null
    ? '#2a78d6' : barColors(d.plant_weekly_labels, d.plant_weekly_partial_idx, '#2a78d6');
  plantWeeklyChart.update();
  harvestWeekdayChart.data.datasets[0].data = d.weekday_avg;
  harvestWeekdayChart.data.datasets[1].data = d.weekday_last_week;
  harvestWeekdayChart.data.datasets[2].data = d.weekday_this_week;
  harvestWeekdayChart.update();
  plantWeekdayChart.data.datasets[0].data = d.plant_weekday_avg;
  plantWeekdayChart.data.datasets[1].data = d.plant_weekday_last_week;
  plantWeekdayChart.data.datasets[2].data = d.plant_weekday_this_week;
  plantWeekdayChart.update();
  document.querySelectorAll('#harvestPills .pill').forEach(el => el.classList.toggle('active', el.dataset.type === key));
}}

document.querySelectorAll('#harvestPills .pill').forEach(el => {{
  el.addEventListener('click', () => showHarvest(el.dataset.type));
}});
showHarvest('Alle');
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
    plant_matrix = fetch_plant_visit_matrix(plant_ranked_by_type, PLANT_MATRIX_WEEKS)
    plant_rows_by_type = {
        t: build_plant_rows(plant_ranked_by_type.get(t, []), matrix=plant_matrix.get(t))
        for t in HARVEST_ORDER
    }
    plant_current = fetch_plant_current_week_counts()
    # The current partial week gets appended to this series separately (see
    # build_harvest_group_data), so fetch one fewer completed week when it
    # exists — otherwise the plant chart would show one more bar than the
    # harvest chart even though both use the same *_WEEKS_HISTORY value.
    plant_weekly = fetch_plant_weekly_series(PLANT_WEEKS_HISTORY - 1 if plant_current else PLANT_WEEKS_HISTORY)
    plant_weekday = fetch_plant_weekday_series(PLANT_WEEKS_HISTORY)
    plant_weekday_this_week = fetch_plant_weekday_this_week()

    # --- Harvest section (Wellboat + Processing vessel) ---
    harvest_daily = {
        "Alle": combine_daily(stats.get("Wellboat", {}), stats.get("Processing vessel", {})),
        "Wellboat": stats.get("Wellboat", {}),
        "Processing vessel": stats.get("Processing vessel", {}),
    }
    harvest_data = {
        key: build_harvest_group_data(
            daily, current_monday, yesterday, two_days_ago,
            plant_weekly=plant_weekly[key],
            plant_current=plant_current[key] if plant_current else None,
            plant_weekday={**plant_weekday[key], "this_week": plant_weekday_this_week[key]},
        )
        for key, daily in harvest_daily.items()
    }
    harvest_pills = "".join(
        f'<button class="pill" data-type="{t}">{HARVEST_LABELS[t]}</button>' for t in HARVEST_ORDER
    )

    # --- Export volume forecast (regression fit live against local plant CSVs + BigQuery) ---
    plant_weekly = fetch_plant_export_series()
    export_regression = fetch_export_regression(client, plant_weekly)

    # This week's plant-visit/capacity forecast — paced from the current
    # in-progress week's plant CSV using the same weekday-elapsed fraction
    # already computed for the locality-visit forecast (harvest_data's
    # `pace_pct`), see build_export_forecast_card's docstring for why.
    plant_daily_current = fetch_plant_current_week_daily()
    wtd_plant_visits = sum(v["visits"] for d, v in plant_daily_current.items() if d <= yesterday)
    wtd_plant_capacity = sum(v["capacity_t"] for d, v in plant_daily_current.items() if d <= yesterday)
    frac = harvest_data["Alle"]["pace_pct"] / 100 or (yesterday.weekday() + 1) / 7
    plant_visit_forecast = wtd_plant_visits / frac
    plant_capacity_forecast = wtd_plant_capacity / frac

    prev_week_monday = current_monday - datetime.timedelta(weeks=1)
    prev_week_iso = prev_week_monday.isocalendar()
    prev_week_plant = plant_weekly.get((prev_week_iso[0], prev_week_iso[1]))
    prev_week_plant_visits = prev_week_plant["visit_count"] if prev_week_plant else None

    export_forecast_card = build_export_forecast_card(
        export_regression, plant_visit_forecast, prev_week_plant_visits, plant_capacity_forecast, current_monday
    )

    # Predicted-vs-actual backtest table: last few *completed* weeks only
    # (drop the current partial week — its own prediction is the card above).
    weekly_mondays = [current_monday - datetime.timedelta(weeks=i) for i in range(WEEKS_HISTORY, 0, -1)]
    export_backtest_section = ""
    if export_regression:
        export_lookup = fetch_export_lookup(client, current_monday.year - 1)
        backtest_rows = build_export_backtest_rows(
            export_regression, weekly_mondays, plant_weekly, export_lookup, n_weeks=6
        )
        export_backtest_section = build_export_backtest_section(backtest_rows)

    now = datetime.datetime.now(datetime.timezone.utc)
    html = TEMPLATE.format(
        yesterday_label=yesterday.strftime("%d.%m.%Y"),
        yesterday_weekday=NO_WEEKDAY[yesterday.weekday()],
        yesterday_weekday_json=json.dumps(NO_WEEKDAY[yesterday.weekday()]),
        updated=now.strftime("%d.%m.%Y %H:%M UTC"),
        harvest_pills=harvest_pills,
        export_forecast_card=export_forecast_card,
        export_backtest_section=export_backtest_section,
        harvest_data_json=json.dumps(harvest_data),
        plant_rows_json=json.dumps(plant_rows_by_type),
        plant_week=plant_week or "-",
        plant_weeks_history=PLANT_WEEKS_HISTORY,
        plant_matrix_weeks=PLANT_MATRIX_WEEKS,
        no_weekday_short_json=json.dumps(NO_WEEKDAY_SHORT),
        pacing_weeks=PACING_WEEKS,
    )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
    print(f"Harvest (Alle): WTD={harvest_data['Alle']['wtd_visits']} forecast={harvest_data['Alle']['forecast']} ({harvest_data['Alle']['pace_pct']}% typical pace)")
    print(f"Plant last week ({plant_week}): {harvest_data['Alle']['plant_last_week']} ({harvest_data['Alle']['plant_diff_label']} vs prev week)")
