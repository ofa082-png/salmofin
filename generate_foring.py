"""
generate_foring.py
-------------------
Renders the feed report (English, same visual system as kontroll.html via templates/foring_template.html) — split out of generate_traffic_report.py
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
MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


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
        estimates.append({"label": f"{MONTHS[m-1]} {y}", "value": round(total), "band": round(band * 100, 1),
                          "partial": seen < n_days, "days": seen, "ndays": n_days})

    # monthly chart: last 24 FD months + estimates, model fit line
    fd_months = sorted(fd)[-24:]
    m_labels = [f"{MONTHS[k[1]-1]} {str(k[0])[2:]}" for k in fd_months] + [e["label"].replace(" 20", " ") for e in estimates]
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
        "last_label": f"{MONTHS[lk[1]-1]} {lk[0]}",
        "last_value": round(fd[lk]),
        "last_yoy": round((fd[lk] / fd[ly] - 1) * 100, 1) if ly in fd else None,
        "estimates": estimates,
        "mape": round(mape * 100, 1),
        "n_fit": len(fit_rows),
        "m_labels": m_labels, "m_actual": m_actual, "m_est": m_est, "m_fit": m_fit,
        "w_labels": w_labels, "w_model": w_model, "w_fd": w_fd,
        "ratio": {str(yr): v for yr, v in sorted(ratio.items())},
    }


# --------------------------------------------------------------------------
# Regions, FD feed history, FCR and feeding rate
# --------------------------------------------------------------------------
# Biomass, count-balance and class-weight helpers are shared with the control
# report (generate_kontroll.py) so FCR here matches kontroll.html.
import generate_kontroll as gk

TEMPLATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates", "foring_template.html")
HIST_START = 2018 * 12          # Jan 2018


def fetch_daily_visits_by_area(client, mmsi_list, yesterday):
    job_config = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("mmsi_list", "INT64", mmsi_list),
        bigquery.ScalarQueryParameter("start", "DATE", FEED_HISTORY_START),
        bigquery.ScalarQueryParameter("end", "DATE", yesterday)])
    rows = client.query("""
        SELECT DATE(v.startTime) AS d, l.prodAreaCode AS po, COUNT(*) AS n
        FROM salmofin.salmofin.vessel_visits v
        LEFT JOIN salmofin.salmofin.localities l ON l.siteNr = v.localityNo
        WHERE v.mmsi IN UNNEST(@mmsi_list) AND DATE(v.startTime) BETWEEN @start AND @end
        GROUP BY 1, 2""", job_config=job_config).result()
    daily = {a: defaultdict(int) for a in gk.AREAS}
    for r in rows:
        for a in gk.areas_of(r.po):
            daily[a][r.d] += r.n
    for a in gk.AREAS:
        d = FEED_HISTORY_START
        while d <= yesterday:
            daily[a].setdefault(d, 0)
            d += datetime.timedelta(days=1)
        daily[a] = dict(daily[a])
    return daily


def monthly_history(salmon, trout, temp_po):
    """Per area and month since 2018: feed (salmon+trout), and salmon inputs for FCR."""
    T1 = max(r["t"] for r in salmon)
    periods = [(2018, 2021), (2022, 2023), (2024, T1 // 12)]
    weights = {p: gk.class_weights(salmon, *p) for p in periods}

    def wfor(a, t):
        return next(weights[p][a] for p in periods if p[0] <= t // 12 <= p[1])

    M = {a: defaultdict(lambda: defaultdict(float)) for a in gk.AREAS}
    for r in salmon + trout:
        if r["t"] < HIST_START - 1:
            continue
        for a in gk.areas_of(r["po"]):
            M[a][r["t"]]["feed_all"] += r["feed"]
    tw = {a: defaultdict(lambda: [0.0, 0.0]) for a in gk.AREAS}
    for r in salmon:
        t = r["t"]
        if t < HIST_START - 1:
            continue
        mw = r["bio"] / r["n"] if r["n"] else 0
        T = temp_po.get((r["po"], t))
        for a in gk.areas_of(r["po"]):
            c = M[a][t]
            c["feed"] += r["feed"]; c["bio"] += r["bio"]; c["ht"] += r["harv_t"]; c["db"] += r["dead"] * mw
            if T is not None and r["po"] != "(null)":
                tw[a][t][0] += T * r["bio"]; tw[a][t][1] += r["bio"]
    # stocking by class (count balance, stocking-year generations)
    G = {}
    for r in salmon:
        if r["y"] != r["g"]:
            continue
        k = (r["po"], r["g"], r["t"])
        c = G.setdefault(k, defaultdict(float))
        c["m"] = r["m"]; c["n"] += r["n"]; c["s"] += r["inp_s"]; c["u"] += r["inp"]; c["tf"] += r["tf"]
        c["out"] += r["harv"] + r["dead"] + r["utk"] + r["rom"] + r["andre"] + r["andre_ny"]
    big = {a: defaultdict(float) for a in gk.AREAS}
    for (po, g, t), c in G.items():
        prev = G.get((po, g, t - 1))
        a_n = 0.0 if c["m"] == 1 else (prev["n"] if prev else 0.0)
        res = a_n + c["u"] - c["out"] + c["tf"] - c["n"]
        for a in gk.areas_of(po):
            M[a][t]["lt"] += c["s"]; M[a][t]["mid"] += c["u"] - c["s"]; big[a][t] += -res
    out = {}
    for a in gk.AREAS:
        rows = []
        for t in range(HIST_START, T1 + 1):
            c, p = M[a][t], M[a][t - 1]
            w = wfor(a, t)
            inb = (c["lt"] * w["lt"] + c["mid"] * w["mid"] + max(0.0, big[a][t]) * w["big"]) / 1000
            dbio = c["bio"] - p["bio"]
            grow = dbio + c["ht"] + c["db"] - inb
            avg_bio = (c["bio"] + p["bio"]) / 2
            tt = tw[a][t]
            rows.append({"t": t, "feed": round(c["feed_all"]), "feedS": round(c["feed"]), "grow": round(grow),
                         "g2": round(dbio + c["ht"]),
                         "sfr": round(c["feed"] / avg_bio / gk.days_in_month(t) * 100, 3) if avg_bio else None,
                         "temp": round(tt[0] / tt[1], 2) if tt[1] else None, "masked": t == gk.DEC_2022})
        out[a] = rows
    return out


def area_payload(daily, fd_all_area, hist_rows, yesterday, current_monday, two_days_ago):
    wrapped = {d: {"visits": v} for d, v in daily.items()}
    vis = build_group_data(wrapped, current_monday, yesterday, two_days_ago)
    fd = build_fd_section(daily, fd_all_area, yesterday)
    years = defaultdict(lambda: [None] * 12)
    for r in hist_rows:
        years[r["t"] // 12][r["t"] % 12] = r["feed"]
    est = {MONTHS.index(e["label"].split()[0]): e["value"] for e in fd["estimates"]}
    roll, b12, e12 = [], [], []
    for i in range(11, len(hist_rows)):
        w = hist_rows[i - 11:i + 1]
        roll.append(round(sum(r["feed"] for r in w)))
        w = [r for r in w if not r["masked"]]       # Dec 2022 left out of the sums, not the whole window
        b12.append(round(sum(r["feedS"] for r in w) / sum(r["grow"] for r in w), 3))
        e12.append(round(sum(r["feedS"] for r in w) / sum(r["g2"] for r in w), 3))
    last25 = hist_rows[-25:]
    sfr, temp = defaultdict(lambda: [None] * 12), defaultdict(lambda: [None] * 12)
    for r in hist_rows:
        sfr[r["t"] // 12][r["t"] % 12] = r["sfr"]
        temp[r["t"] // 12][r["t"] % 12] = r["temp"]
    visits = {k: vis[k] for k in ("wtd_visits", "forecast", "pace_pct", "weekly_labels", "weekly_values", "weekly_partial_idx")}
    visits["wtdDiff"] = vis["wtd_diff_label"]
    return {
        "visits": visits,
        "fd": fd,
        "yoy": {"years": {str(y): v for y, v in sorted(years.items())}, "est": est},
        "roll": {"t": [r["t"] for r in hist_rows[11:]], "feed": roll, "bio": b12, "efcr": e12},
        "fcr": {"t": [r["t"] for r in last25],
                "bio": [None if r["masked"] or r["grow"] <= 0 else round(r["feedS"] / r["grow"], 3) for r in last25],
                "efcr": [None if r["g2"] <= 0 else round(r["feedS"] / r["g2"], 3) for r in last25]},
        "sfr": {str(y): v for y, v in sorted(sfr.items())},
        "temp": {str(y): v for y, v in sorted(temp.items())},
    }


if __name__ == "__main__":
    mmsi_to_type = load_fleet()
    print(f"  {len(mmsi_to_type)} feed vessels in vessel_categories.csv")
    client = get_bq_client()

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    two_days_ago = yesterday - datetime.timedelta(days=1)
    current_monday = monday_of(yesterday)

    daily = fetch_daily_visits_by_area(client, list(mmsi_to_type.keys()), yesterday)
    salmon, trout = gk.fetch_biomass(client)
    temp_po = gk.fetch_temperature(client)
    hist = monthly_history(salmon, trout, temp_po)

    areas = {}
    for a in gk.AREAS:
        fd_all = {(r["t"] // 12, r["t"] % 12 + 1): r["feed"] for r in hist[a]}
        areas[a] = area_payload(daily[a], fd_all, hist[a], yesterday, current_monday, two_days_ago)
        p = areas[a]
        print(f"  {a}: last FD {p['fd']['last_label']} {p['fd']['last_value']} t; "
              f"estimates {[(x['label'], x['value']) for x in p['fd']['estimates']]}; MAPE {p['fd']['mape']}%; "
              f"12m bio FCR {p['roll']['bio'][-1]}, eFCR {p['roll']['efcr'][-1]}")

    now = datetime.datetime.now(datetime.timezone.utc)
    data = {"through": yesterday.strftime("%d %b %Y"), "weekday": yesterday.strftime("%A"),
            "updated": now.strftime("%Y-%m-%d %H:%M UTC"), "pacingWeeks": PACING_WEEKS,
            "curYear": yesterday.year, "areas": areas}
    with open(TEMPLATE, encoding="utf-8") as f:
        html = f.read().replace("__DATA__", json.dumps(data, separators=(",", ":")))
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
