"""
generate_traffic_report.py
---------------------------
Renders a weekly-focused vessel traffic report for traders/exporters,
scoped to Wellboat + Processing vessel — the harvest-signal fleet —
in vessel_categories.csv. Rendered into templates/traffic_template.html
(English, control-report style; rebuilt 2026-10-02 with a 52-week export
model-vs-actual chart). Sections:

  A/B. Harvest signal — Wellboat + Processing vessel locality visits
       (vessel_visits) and harvest-plant deliveries (harvest_plant_visits
       CSVs), with a weekday-pacing forecast for the current week.

Feed carrier + silage traffic moved to generate_foring.py (2026-08-16)
— that's a production-intensity signal, not a harvest-logistics one,
so it didn't belong bundled in here just because it shares the same
AIS data source. Avlusningsfartøy (delousing vessel visits) moved to
generate_report.py/fiskehelse.html (2026-08-17) for the same reason —
it's a fish-health-adjacent signal, not harvest-logistics.

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
TEMPLATE     = os.path.join(BASE_DIR, "templates", "traffic_template.html")
FLEET_CSV    = os.path.join(BASE_DIR, "vessel_categories.csv")

WEEKS_HISTORY = 10   # weeks shown in bar charts, including the current (partial) week
PACING_WEEKS  = 8     # completed weeks used to build the weekday-pacing curve for forecasts
PLANT_WEEKS_HISTORY = WEEKS_HISTORY  # kept equal to the locality-visit lookback for a consistent trend window

HARVEST_ORDER = ["Alle", "Wellboat", "Processing vessel"]


def get_bq_client():
    if not os.environ.get("GOOGLE_CREDENTIALS"):
        return bigquery.Client(project=PROJECT_ID)
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

def fetch_export_regression(client, harvest_mmsi_list):
    """Fit exports_tonn[i] ~ a*visits[i] + b*visits[i-1]
    + d*sin(2*pi*wk/52) + e*cos(2*pi*wk/52) + f*trend_weeks + c using
    every matched (year, week) of harvest-fleet locality visits vs.
    BigQuery export data. Refit live on every run — rather than
    hardcoding coefficients — so the relationship self-corrects as the
    fleet or export mix drifts, instead of silently going stale.

    Two visit terms, not one: a single-variable same-week-only fit gets
    r=0.91, but last week's *already-known* (not forecast) visit count
    carries real independent signal — a day-level lag scan peaks at a
    2-3 day shift, consistent with the real harvest-to-export processing
    lag — and adding it as a second term lifts R^2 from 0.82 to 0.86
    while adding zero extra forecast uncertainty for that term.

    Seasonal harmonic + linear trend, on top of that: exports have a
    real annual pattern (calmer H1, a Jul-Sep ramp) that a pure
    visits-only fit can't see, and a slow multi-year drift toward more
    tonnes per vessel trip (bigger/fuller loads over time) that the
    same-week visit count alone doesn't capture either. Adding a
    52-week sin/cos pair plus a linear weeks-since-start trend term
    lifts weekly R^2 further (~0.86 -> ~0.87 measured live against the
    current dataset) — a real, if modest, gain, not just added
    noise-fitting: seasonality and trend are genuine structural
    features of the export series, not artifacts of this particular
    fleet's visit pattern."""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("mmsi_list", "INT64", harvest_mmsi_list)]
    )
    rows = list(client.query("""
        WITH visits AS (
          SELECT EXTRACT(ISOYEAR FROM startTime) AS yr, EXTRACT(ISOWEEK FROM startTime) AS wk, COUNT(*) AS visit_count
          FROM `salmofin.salmofin.vessel_visits`
          WHERE mmsi IN UNNEST(@mmsi_list)
          GROUP BY yr, wk
        ),
        exports AS (
          SELECT year AS yr, week AS wk, SUM(vekt_tonn) AS export_tonn
          FROM `salmofin.salmofin.salmon_export_weekly`
          GROUP BY yr, wk
        )
        SELECT v.yr, v.wk, v.visit_count, e.export_tonn
        FROM visits v JOIN exports e ON v.yr = e.yr AND v.wk = e.wk
        ORDER BY v.yr, v.wk
    """, job_config=job_config).result())

    # 6 unknowns now (was 3) — keep a wider safety margin than the old
    # "12" floor so the fit isn't running on a handful more rows than
    # parameters. Live history is ~135 weeks (2024+), far past this.
    if len(rows) < 26:
        return None

    visits = [r.visit_count for r in rows]
    exports = [r.export_tonn for r in rows]
    # ISO (year, week) -> Monday date, so trend can be a plain weeks-since-
    # start count rather than dealing with 52/53-week-year ISO-week math.
    mondays = [datetime.date.fromisocalendar(int(r.yr), int(r.wk), 1) for r in rows]
    ref_monday = mondays[0]

    xs = visits[1:]        # this-week visits
    xs_prev = visits[:-1]  # last-week visits (known, not forecast)
    ys = exports[1:]
    wks = [r.wk for r in rows[1:]]
    trend = [(mondays[i] - ref_monday).days // 7 for i in range(1, len(rows))]
    m = len(ys)

    # design matrix columns: [visits, visits_prev, sin, cos, trend, 1]
    design = [
        [xs[i], xs_prev[i], math.sin(2 * math.pi * wks[i] / 52), math.cos(2 * math.pi * wks[i] / 52), trend[i], 1.0]
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
    a, b, d, e, f, c = coef

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
        "d": d,
        "e": e,
        "f": f,
        "c": c,
        "ref_monday": ref_monday,
        "r2": r2,
        "rmse_pct": round(rmse / my * 100) if my else 0,
        "n_weeks": m,
        "series": [{"monday": mondays[i + 1].isoformat(), "wk": wks[i], "visits": xs[i],
                    "actual": round(ys[i]), "fit": round(preds[i])} for i in range(m)],
    }

def _predict_export(regression, visits, prev_visits, monday):
    """Apply a fitted export_regression to one (visits, prev_visits,
    week) point. `monday` gives both the ISO week (for the seasonal
    term) and the trend position (weeks since the regression's
    ref_monday) — shared by the live forecast card and the backtest
    table so both use exactly the same model, not a re-derived copy."""
    wk = monday.isocalendar()[1]
    trend = (monday - regression["ref_monday"]).days // 7
    return (
        regression["a"] * visits
        + regression["b"] * prev_visits
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

def build_export_backtest_rows(regression, weekly_mondays, weekly_visits, export_lookup, n_weeks):
    """Predicted-vs-actual for the last n_weeks *completed* weeks (the
    current partial week is excluded by the caller). Each prediction
    uses two fully-known visit counts — no forecast uncertainty — so
    any gap between predicted and actual here is purely model error,
    not projection error."""
    rows = []
    # weekly_mondays/weekly_visits are oldest -> newest; need index i-1 for
    # the "last week" term, so start from 1.
    start = max(1, len(weekly_mondays) - n_weeks)
    for i in range(start, len(weekly_mondays)):
        monday = weekly_mondays[i]
        visits = weekly_visits[i]
        prev_visits = weekly_visits[i - 1]
        predicted = _predict_export(regression, visits, prev_visits, monday)
        iso = monday.isocalendar()
        actual = export_lookup.get((iso[0], iso[1]))
        diff_pct = ((predicted - actual) / actual * 100) if actual else None
        rows.append({
            "label": f"U{iso[1]}",
            "predicted": round(predicted),
            "actual": round(actual) if actual is not None else None,
            "diff_pct": diff_pct,
        })
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

PLANT_MATRIX_WEEKS = 8   # weeks of history shown in the per-plant sparkline column

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

def plant_table(ranked, matrix):
    out = []
    for name, p in ranked or []:
        out.append({"name": name.title(), "company": (p["company"] or "").title(), "cap": round(p["capacity"]),
                    "visits": p["visits"], "last": (p["last_exit"] or "")[:10],
                    "spark": matrix["rows"].get(name, []) if matrix else []})
    return out


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

    # --- Plant (slakteri) section ---
    plant_ranked_by_type, plant_week = fetch_plant_status()
    plant_matrix = fetch_plant_visit_matrix(plant_ranked_by_type, PLANT_MATRIX_WEEKS)
    plant_current = fetch_plant_current_week_counts()
    # the current partial week is appended separately, so fetch one fewer completed week when it exists
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
    for key in HARVEST_ORDER:
        d = harvest_data[key]
        for k in ("wtd_diff_color", "y_diff_color", "plant_diff_color"):
            d.pop(k, None)
        d["plants"] = plant_table(plant_ranked_by_type.get(key, []), plant_matrix.get(key))
    spark_labels = next(iter(plant_matrix.values()))["labels"] if plant_matrix else []
    spark_partial = next(iter(plant_matrix.values()))["partial_idx"] if plant_matrix else None

    # --- Export volume (regression fit live against BigQuery) ---
    harvest_mmsi_list = [mmsi for mmsi, t in mmsi_to_type.items() if t in ("Wellboat", "Processing vessel")]
    reg = fetch_export_regression(client, harvest_mmsi_list)
    alle_weekly_values = harvest_data["Alle"]["weekly_values"]
    last_week_actual = alle_weekly_values[-2] if len(alle_weekly_values) >= 2 else None
    export = None
    if reg:
        weekly_mondays = [current_monday - datetime.timedelta(weeks=i) for i in range(WEEKS_HISTORY - 1, -1, -1)]
        export_lookup = fetch_export_lookup(client, current_monday.year - 1)
        backtest = build_export_backtest_rows(reg, weekly_mondays[:-1], alle_weekly_values[:-1], export_lookup, n_weeks=8)
        forecast = round(_predict_export(reg, harvest_data["Alle"]["forecast"], last_week_actual, current_monday)) if last_week_actual is not None else None
        export = {"forecast": forecast, "week": current_monday.isocalendar()[1], "monday": current_monday.isoformat(),
                  "r2": round(reg["r2"], 2), "rmse_pct": reg["rmse_pct"], "n_weeks": reg["n_weeks"],
                  "series": reg["series"][-78:], "backtest": backtest}

    data = {
        "updated": datetime.datetime.now(datetime.timezone.utc).strftime("%d %b %Y %H:%M UTC"),
        "yesterday": yesterday.isoformat(), "monday": current_monday.isoformat(),
        "pacingWeeks": PACING_WEEKS, "plantWeek": plant_week or "-",
        "sparkLabels": spark_labels, "sparkPartial": spark_partial,
        "types": harvest_data, "export": export,
    }
    with open(TEMPLATE, encoding="utf-8") as f:
        html = f.read().replace("__DATA__", json.dumps(data, separators=(",", ":"), ensure_ascii=False))
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
    print(f"Harvest (Alle): WTD={harvest_data['Alle']['wtd_visits']} forecast={harvest_data['Alle']['forecast']} ({harvest_data['Alle']['pace_pct']}% typical pace)")
    if export:
        print(f"Export forecast week {export['week']}: {export['forecast']} t (R2 {export['r2']}, ±{export['rmse_pct']}%)")
