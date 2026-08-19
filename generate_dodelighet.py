"""
generate_dodelighet.py
-----------------------
Renders the mortality report and writes it to docs/dodelighet.html, for
GitHub Pages to serve. Nightly script.

Split out of generate_report.py on 2026-08-19 — fiskehelse.html was
bundling mortality and disease together (already split lice out
earlier the same day), and the mortality section alone had grown to
five charts (dødelighet, utkast, cumulative-by-year, cumulative-by-
cohort-age, fiskehelseindikator) — its own conceptual area, distinct
from disease-case tracking, and big enough to earn its own page rather
than keep stretching fiskehelse.html further.

Self-contained by design, matching every other generate_*.py script in
this repo — duplicates the small bits of shared logic (BQ client,
vessel-fleet loading, weekly-series helpers) rather than importing from
generate_report.py.
"""

import os
import csv
import json
import math
import datetime
from collections import defaultdict
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "salmofin"
OUT_PATH   = os.path.join(os.path.dirname(__file__), "docs", "dodelighet.html")
FLEET_CSV  = os.path.join(os.path.dirname(__file__), "vessel_categories.csv")
WEEKS_HISTORY = 12  # lookback for the Fiskehelseindikator chart, "siste 12 uker"
MORTALITY_MONTHS_HISTORY = 12
COHORT_FIRST_YEAR = 2018  # earlier Utsettsar cohorts are too thin/sparse to trust nationally

NO_MONTH_SHORT = ["Jan","Feb","Mar","Apr","Mai","Jun","Jul","Aug","Sep","Okt","Nov","Des"]

def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)

def load_vessel_fleet(types):
    """MMSI -> vessel type, restricted to the given vessel_categories.csv
    Type values — used for the Fiskehelseindikator chart below."""
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

def fetch_fiskehelseindikator(client):
    """Silage/feed vessel visit ratio, a mortality proxy — silage
    vessels collect dead fish/offal, so more silage activity relative
    to normal feed-carrier activity is a fairly direct operational
    readout of mortality. Moved here from generate_report.py on
    2026-08-19 along with the rest of the mortality content."""
    mmsi_to_type = load_vessel_fleet(("Fish feed carrier", "Silage"))
    days_back = WEEKS_HISTORY * 7 + 7
    rows = fetch_fleet_visits(client, list(mmsi_to_type.keys()), days_back)

    daily_by_type = defaultdict(lambda: defaultdict(int))
    for r in rows:
        vtype = mmsi_to_type.get(r.mmsi)
        if vtype:
            daily_by_type[vtype][r.visit_date] += 1

    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    current_monday = monday_of(yesterday)

    feed_weekly = build_weekly_series(daily_by_type.get("Fish feed carrier", {}), current_monday, WEEKS_HISTORY, yesterday)
    silage_weekly = build_weekly_series(daily_by_type.get("Silage", {}), current_monday, WEEKS_HISTORY, yesterday)

    fh_labels = [w[0] for w in feed_weekly]
    fh_values = [
        round(s[1] / f[1], 3) if f[1] else None
        for f, s in zip(feed_weekly, silage_weekly)
    ]
    return fh_labels, fh_values

def fetch_mortality_and_losses(client):
    """National monthly mortality risk PLUS the other three Fiskeridirektoratet
    tap (loss) categories, all from one query against the biomass table
    (Artsid='LAKS') — dødfisk (mortality), utkast (slaughter rejects),
    rømming (escapes), annet (other/predators/theft).

    Mortality uses Veterinærinstituttets own published formula: dM_t =
    dead_t / N_bar_t, N_bar_t = (stock[t-1] + stock[t]) / 2 (stock is an
    end-of-month snapshot — confirmed by reconciling it against the flow
    fields), R_t = 1 - e^(-dM_t). Internal proxy, not the official
    Veterinærinstituttet figure — confirmed (2026-08-17/18) to sit ~3-5pp
    below their published annual number every year, because they average
    per-locality rates and per-locality raw data isn't public, while this
    pools nationally from Fiskeridirektoratet's PO-level public data.
    Chose this over ingesting Veterinærinstituttet's own public API
    (apps.vetinst.no/salmonid-mortality-public-api) because that only
    updates quarterly and would be staler than every other metric on
    this page — see project notes for the full tradeoff (2026-08-18).

    Utkast (discard) rate = Utkast_stk / Uttak_stk per month, same
    formula already used in the standalone `utkast_trend` artifact
    (2026-08-18) — kept consistent rather than switching to the more
    technically-precise Utkast/(Uttak+Utkast) denominator used later in
    that same session's harvest-volume estimate, since the two are
    within ~1% of each other at these magnitudes and matching the
    already-published chart matters more than the small precision gain.

    Rømming (escapes) and Annet (other) are both consistently near-zero
    nationally (~0.0-0.05% of harvest) — not worth full trend charts,
    so returned as a simple 12-month total instead."""
    rows = list(client.query("""
        SELECT Ar, Maaned_kode,
          SUM(Behfisk_stk) AS behfisk,
          SUM(Dodfisk_stk) AS dodfisk,
          SUM(Uttak_stk) AS uttak,
          SUM(Utkast_stk) AS utkast,
          SUM(Romming_stk) AS romming,
          SUM(Andre_stk) AS andre
        FROM salmofin.salmofin.biomass
        WHERE Artsid = 'LAKS'
        GROUP BY Ar, Maaned_kode
        ORDER BY Ar, Maaned_kode
    """).result())
    data = [(r.Ar, r.Maaned_kode, r.behfisk, r.dodfisk, r.uttak, r.utkast, r.romming, r.andre) for r in rows]

    monthly = []
    for i in range(1, len(data)):
        ar, mnd, n_t, m_t, uttak, utkast, romming, andre = data[i]
        n_bar = (data[i - 1][2] + n_t) / 2
        dM = m_t / n_bar if n_bar else 0
        R = (1 - math.exp(-dM)) * 100
        discard_pct = (utkast / uttak * 100) if uttak else 0
        monthly.append((ar, mnd, R, discard_pct, romming, andre))

    last = monthly[-MORTALITY_MONTHS_HISTORY:]
    if len(last) < 2:
        return None, None

    mort_current, mort_prior = last[-1][2], last[-2][2]
    mortality = {
        "labels": [NO_MONTH_SHORT[mnd - 1] for _, mnd, _, _, _, _ in last],
        "values": [round(r, 3) for _, _, r, _, _, _ in last],
        "current": round(mort_current, 2),
        "delta_pp": round(mort_current - mort_prior, 2),
    }

    discard_current, discard_prior = last[-1][3], last[-2][3]
    romming_12mo = sum(r[4] for r in last)
    andre_12mo = sum(r[5] for r in last)
    losses = {
        "labels": mortality["labels"],
        "values": [round(r, 3) for _, _, _, r, _, _ in last],
        "current": round(discard_current, 2),
        "delta_pp": round(discard_current - discard_prior, 2),
        "romming_12mo": romming_12mo,
        "andre_12mo": andre_12mo,
    }
    return mortality, losses

def fetch_mortality_history(client):
    """Two longer-horizon dødelighet views, both built on the same
    Veterinærinstituttet risk formula as fetch_mortality_and_losses but
    over the biomass table's full history (2017-) instead of a rolling
    12 months:

    1. year_cumulative — within-calendar-year cumulative risk (resets
       each January), one line per year, so this year's trajectory can
       be read against past years at the same point in the season.

    2. cohort_cumulative — cumulative risk by age-in-months-since-stocking
       (age 0 = December of Utsettsar-1, since national stocking ramps up
       from about there), one line per year-class (Utsettsar), so a
       generation's whole-lifecycle mortality can be compared across
       generations regardless of calendar year.

    Cohort curves are truncated once a generation's national stock falls
    below 10% of its own peak (checked only *after* the peak, so the
    stocking ramp-up isn't mistaken for the harvest-out tail). Without
    this, the curve for an almost-fully-harvested generation keeps
    dividing by the tiny remaining broodstock/leftover population, and
    normal-looking absolute death counts against that shrunk denominator
    blow the cumulative risk up towards 100% — an artifact of the
    formula, not a real mortality signal (found 2026-08-19: raw curves
    for older cohorts spiked to 90%+ by month 48, far above anything in
    the official annual figures, before this fix)."""
    year_rows = list(client.query("""
        SELECT Ar, Maaned_kode, SUM(Behfisk_stk) AS beh, SUM(Dodfisk_stk) AS dod
        FROM salmofin.salmofin.biomass
        WHERE Artsid = 'LAKS'
        GROUP BY Ar, Maaned_kode ORDER BY Ar, Maaned_kode
    """).result())
    year_rows = [(r.Ar, r.Maaned_kode, r.beh, r.dod) for r in year_rows]

    by_year = defaultdict(dict)
    for i in range(1, len(year_rows)):
        ar, mnd, beh, dod = year_rows[i]
        if ar < COHORT_FIRST_YEAR:
            continue
        n_bar = (year_rows[i - 1][2] + beh) / 2
        dM = dod / n_bar if n_bar else 0
        by_year[ar][mnd] = dM

    current_year = max(by_year)
    year_series = []
    for ar in sorted(by_year):
        cum, values = 0, []
        for mnd in range(1, 13):
            if mnd not in by_year[ar]:
                values.append(None)
                continue
            cum += by_year[ar][mnd]
            values.append(round((1 - math.exp(-cum)) * 100, 2))
        year_series.append({"year": ar, "values": values})

    complete_years = {s["year"]: s["values"][11] for s in year_series if s["year"] != current_year and s["values"][11] is not None}
    worst_year = max(complete_years, key=complete_years.get) if complete_years else None
    recent_complete_year = max(complete_years) if complete_years else None
    if worst_year == recent_complete_year and len(complete_years) > 1:
        # they'd otherwise collide on one highlight slot and the other
        # goes unlabeled — fall back to the runner-up worst year instead
        others = {y: v for y, v in complete_years.items() if y != recent_complete_year}
        worst_year = max(others, key=others.get)
    for s in year_series:
        s["highlight"] = ("current" if s["year"] == current_year else
                           "worst" if s["year"] == worst_year else
                           "recent" if s["year"] == recent_complete_year else None)

    cohort_rows = list(client.query(f"""
        SELECT Utsettsar, Ar, Maaned_kode, SUM(Behfisk_stk) AS beh, SUM(Dodfisk_stk) AS dod
        FROM salmofin.salmofin.biomass
        WHERE Artsid = 'LAKS' AND Utsettsar >= {COHORT_FIRST_YEAR}
        GROUP BY Utsettsar, Ar, Maaned_kode ORDER BY Utsettsar, Ar, Maaned_kode
    """).result())
    by_cohort = defaultdict(list)
    for r in cohort_rows:
        by_cohort[r.Utsettsar].append((r.Ar, r.Maaned_kode, r.beh, r.dod))

    cohort_series, cohort_final = [], {}
    for utsettsar, rows in sorted(by_cohort.items()):
        if len(rows) < 6:
            continue  # too new / too little history to plot a meaningful curve
        peak_idx = max(range(len(rows)), key=lambda i: rows[i][2])
        peak = rows[peak_idx][2]
        thresh = 0.1 * peak
        base_ym = rows[0][0] * 12 + rows[0][1]
        cum, points, completed = 0, [], False
        for i in range(1, len(rows)):
            ar, mnd, beh, dod = rows[i]
            if i > peak_idx and beh < thresh:
                completed = True
                break
            n_bar = (rows[i - 1][2] + beh) / 2
            dM = dod / n_bar if n_bar else 0
            cum += dM
            age = ar * 12 + mnd - base_ym
            points.append({"age": age, "r": round((1 - math.exp(-cum)) * 100, 2)})
        if not points:
            continue
        cohort_series.append({"cohort": utsettsar, "points": points, "completed": completed})
        if completed:
            cohort_final[utsettsar] = points[-1]["r"]

    worst_cohort = max(cohort_final, key=cohort_final.get) if cohort_final else None
    recent_complete_cohort = max(cohort_final) if cohort_final else None
    if worst_cohort == recent_complete_cohort and len(cohort_final) > 1:
        others = {c: v for c, v in cohort_final.items() if c != recent_complete_cohort}
        worst_cohort = max(others, key=others.get)
    # "current" needs a real trajectory (>=12mo) to be worth highlighting — a
    # brand-new cohort with only a couple of months would otherwise outrank
    # a more informative one just for being more recent
    current_cohort = max((s["cohort"] for s in cohort_series if not s["completed"] and len(s["points"]) >= 12), default=None)
    for s in cohort_series:
        s["highlight"] = ("worst" if s["cohort"] == worst_cohort else
                           "recent" if s["cohort"] == recent_complete_cohort else
                           "current" if s["cohort"] == current_cohort else None)

    return (
        {"labels": NO_MONTH_SHORT, "series": year_series},
        {"series": cohort_series},
    )

TEMPLATE = """<!doctype html>
<html lang="no">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Dødelighet — ukesrapport</title>
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
      <div style="font-size:18px;font-weight:500;">Dødelighet — ukesrapport</div>
      <div style="font-size:13px;color:var(--text-muted)">Uke {week}, {year} · oppdatert {updated}</div>
    </div>
    <div style="display:flex;gap:8px;align-items:baseline;">
      <a href="index.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">hjem →</a>
      <a href="fiskehelse.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">sykdom →</a>
      <a href="lakselus.html" style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;text-decoration:none;">lakselus →</a>
      <div style="font-size:11px;color:var(--text-muted);border:0.5px solid var(--border);border-radius:8px;padding:4px 8px;">kilde: Fiskeridirektoratet</div>
    </div>
  </div>

  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:1.5rem;">
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Dødelighet, siste måned</div>
      <div style="font-size:24px;font-weight:500;">{mortality_current}%</div>
      <div style="font-size:12px;color:{mortality_delta_color};margin-top:2px;">{mortality_delta_label} vs. forrige mnd</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Dødelighet, hittil i år</div>
      <div style="font-size:24px;font-weight:500;">{mortality_ytd}%</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Utkast, siste måned</div>
      <div style="font-size:24px;font-weight:500;">{discard_current}%</div>
    </div>
    <div style="background:var(--surface-2);border-radius:8px;padding:1rem;">
      <div style="font-size:13px;color:var(--text-secondary);margin-bottom:4px;">Rømming, siste 12 mnd</div>
      <div style="font-size:24px;font-weight:500;">{romming_12mo} fisk</div>
    </div>
  </div>

  <div style="font-size:16px;font-weight:500;margin-bottom:2px;">Dødelighet</div>
  <div style="font-size:12px;color:var(--text-muted);margin-bottom:10px;">Beregnet dødelighetsrisiko, nasjonalt, Veterinærinstituttets formel anvendt på Fiskeridirektoratets biomassedata. Siste måned: <b style="color:var(--text-primary)">{mortality_current}%</b> ({mortality_delta_label} vs. forrige måned). Intern proxy — ikke identisk med Veterinærinstituttets offisielle (kvartalsvis oppdaterte) tall, se fotnote.</div>
  <div style="position:relative;width:100%;height:140px;margin-bottom:4px;">
    <canvas id="mortalityChart" width="640" height="140"></canvas>
  </div>
  <div style="font-size:11px;color:var(--text-muted);margin-bottom:1.75rem;">Siste måned kan justeres når Fiskeridirektoratet publiserer korrigerte tall.</div>

  <div style="font-size:16px;font-weight:500;margin-bottom:2px;">Utkast ved slakt</div>
  <div style="font-size:12px;color:var(--text-muted);margin-bottom:10px;">Andel av slaktet fisk som kasseres (kjønnsmodning, defekter, sår). Siste måned: <b style="color:var(--text-primary)">{discard_current}%</b> ({discard_delta_label} vs. forrige måned). Har falt betydelig siden 2018 (se fotnote) — henger ikke tett sammen med dødelighet måned for måned, men begge har bedret seg over tid.</div>
  <div style="position:relative;width:100%;height:140px;margin-bottom:4px;">
    <canvas id="discardChart" width="640" height="140"></canvas>
  </div>
  <div style="font-size:11px;color:var(--text-muted);margin-bottom:10px;">Øvrige tapskategorier, siste 12 mnd: rømming {romming_12mo} fisk, annet (predatorer, tyveri m.m.) {andre_12mo} fisk — begge marginale nasjonalt (typisk under 0,05 % av produksjonen).</div>
  <div style="font-size:11px;color:var(--text-muted);margin-bottom:1.75rem;">Siste måned kan justeres når Fiskeridirektoratet publiserer korrigerte tall.</div>

  <div style="font-size:16px;font-weight:500;margin-bottom:2px;">Dødelighet, kumulativt gjennom året</div>
  <div style="font-size:12px;color:var(--text-muted);margin-bottom:8px;">Samme formel og datagrunnlag som over, men nullstilt 1. januar hvert år — viser om {mort_year_current} ligger bedre eller dårligere an enn tidligere år på samme tidspunkt i sesongen.</div>
  <div style="display:flex;flex-wrap:wrap;gap:14px;margin-bottom:8px;font-size:11px;color:var(--text-secondary);">
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:2px;background:#c1392b;"></span>{mort_year_current} (så langt)</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:2px;background:#2a78d6;"></span>{mort_year_recent} (siste fulle år)</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:2px;background:#eda100;"></span>{mort_year_worst} (verst på rekord)</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:2px;background:#89878180;"></span>Øvrige år</span>
  </div>
  <div style="position:relative;width:100%;height:180px;margin-bottom:1.75rem;">
    <canvas id="mortalityYearChart" width="640" height="180"></canvas>
  </div>

  <div style="font-size:16px;font-weight:500;margin-bottom:2px;">Dødelighet per årsklasse, kumulativt etter alder</div>
  <div style="font-size:12px;color:var(--text-muted);margin-bottom:8px;">Samme formel, gruppert på utsettsår (årsklasse) i stedet for kalenderår — alder 0 måneder ≈ desember året før utsett. Kurvene stopper når årsklassens nasjonale bestand har falt under 10 % av toppen (dvs. i praksis nedslaktet) — uten dette blåser kumulativ risiko kunstig opp mot slutten, se fotnote.</div>
  <div style="display:flex;flex-wrap:wrap;gap:14px;margin-bottom:8px;font-size:11px;color:var(--text-secondary);">
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:2px;background:#c1392b;"></span>{cohort_current} (pågår)</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:2px;background:#2a78d6;"></span>{cohort_recent} (siste nedslaktede)</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:2px;background:#eda100;"></span>{cohort_worst} (verst på rekord)</span>
    <span style="display:flex;align-items:center;gap:4px;"><span style="width:16px;height:2px;background:#89878180;"></span>Øvrige årsklasser</span>
  </div>
  <div style="position:relative;width:100%;height:180px;margin-bottom:4px;">
    <canvas id="mortalityCohortChart" width="640" height="180"></canvas>
  </div>
  <div style="font-size:11px;color:var(--text-muted);margin-bottom:1.75rem;">10 %-avkuttingen er en egen metodikkvalg for denne siden, ikke en del av Veterinærinstituttets formel — uten den divideres døde fisk mot en krympende resterende bestand (stamfisk/restpartier) mot slutten av syklusen, som kunstig driver risikoen mot 100 %.</div>

  <div style="font-size:16px;font-weight:500;margin-bottom:2px;">Fiskehelseindikator</div>
  <div style="font-size:12px;color:var(--text-muted);margin-bottom:10px;">Egenutviklet indikator basert på vessel-trafikkmønstre (ensilasje/fôr-anløpsforhold). Stigende verdier kan tyde på økt dødelighet eller helseutfordringer på anleggene.</div>
  <div style="position:relative;width:100%;height:140px;margin-bottom:4px;">
    <canvas id="fishHealthChart" width="640" height="140"></canvas>
  </div>
  <div style="font-size:11px;color:var(--text-muted);margin-bottom:1.75rem;">Siste punkt er inneværende uke (delvis).</div>

  <div style="font-size:11px;color:var(--text-muted);border-top:0.5px solid var(--border);padding-top:12px;">
    Data: Fiskeridirektoratets biomassestatistikk og BarentsWatch AIS (kun fartøy i vår flåteliste, vessel_categories.csv), via salmofin BigQuery-pipeline. Dødelighet: beregnet med Veterinærinstituttets formel (dødelighetsrisiko = 1−e^(−dødfisk/gjennomsnittlig bestand)) — egen beregning, oppdatert månedlig; avviker noe fra Veterinærinstituttets egne (kvartalsvise) offisielle tall siden de midler per lokalitet og vi ikke har tilgang til lokalitetsdata. Utkast: andel av Uttak_stk (samme kilde og periode). Generert automatisk hver natt.
  </div>
</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
<script>
new Chart(document.getElementById('mortalityChart'), {{
  type: 'line',
  data: {{ labels: {mortality_labels_json}, datasets: [
    {{ data: {mortality_values_json}, borderColor: '#c1392b', backgroundColor: 'rgba(193,57,43,0.1)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2 }}
  ] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }}, x: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ display: false }} }} }} }}
}});

new Chart(document.getElementById('discardChart'), {{
  type: 'line',
  data: {{ labels: {discard_labels_json}, datasets: [
    {{ data: {discard_values_json}, borderColor: '#eb6834', backgroundColor: 'rgba(235,104,52,0.1)', fill: true, tension: 0.3, pointRadius: 0, borderWidth: 2 }}
  ] }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ color: '#e1e0d9' }} }}, x: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ display: false }} }} }} }}
}});

function highlightColor(h) {{ return h === 'current' ? '#c1392b' : h === 'recent' ? '#2a78d6' : h === 'worst' ? '#eda100' : '#89878180'; }}

const yearSeries = {mort_year_series_json};
new Chart(document.getElementById('mortalityYearChart'), {{
  type: 'line',
  data: {{ labels: {mort_year_labels_json}, datasets: yearSeries.map(s => ({{
    label: String(s.year), data: s.values, spanGaps: false,
    borderColor: highlightColor(s.highlight), backgroundColor: highlightColor(s.highlight),
    borderWidth: s.highlight ? 2 : 1, borderDash: s.highlight === 'current' ? [4,3] : [],
    tension: 0.25, pointRadius: 0, order: s.highlight ? 1 : 2
  }})) }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }}, callback: v => v + '%' }}, grid: {{ color: '#e1e0d9' }} }},
               x: {{ ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ display: false }} }} }} }}
}});

const cohortSeries = {mort_cohort_series_json};
new Chart(document.getElementById('mortalityCohortChart'), {{
  type: 'line',
  data: {{ datasets: cohortSeries.map(s => ({{
    label: String(s.cohort), data: s.points.map(p => ({{ x: p.age, y: p.r }})),
    borderColor: highlightColor(s.highlight), backgroundColor: highlightColor(s.highlight),
    borderWidth: s.highlight ? 2 : 1, borderDash: s.highlight === 'current' ? [4,3] : [],
    tension: 0.25, pointRadius: 0, order: s.highlight ? 1 : 2
  }})) }},
  options: {{ responsive: true, maintainAspectRatio: false, plugins: {{ legend: {{ display: false }} }},
    scales: {{ y: {{ ticks: {{ color: '#898781', font: {{ size: 11 }}, callback: v => v + '%' }}, grid: {{ color: '#e1e0d9' }} }},
               x: {{ type: 'linear', title: {{ display: true, text: 'Måneder siden smoltutsett', color: '#898781', font: {{ size: 11 }} }},
                    ticks: {{ color: '#898781', font: {{ size: 11 }} }}, grid: {{ display: false }} }} }} }}
}});

new Chart(document.getElementById('fishHealthChart'), {{
  type: 'line',
  data: {{ labels: {fh_labels_json}, datasets: [
    {{ label: 'Indikator', data: {fh_values_json}, borderColor: '#7a4fc9', backgroundColor: '#7a4fc9', tension: 0.25, pointRadius: 3, spanGaps: true }}
  ] }},
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
    fh_labels, fh_values = fetch_fiskehelseindikator(client)
    mortality, losses = fetch_mortality_and_losses(client)
    mort_year, mort_cohort = fetch_mortality_history(client)
    now = datetime.datetime.now(datetime.timezone.utc)

    def find_highlight(series, key, label):
        return next((s[key] for s in series if s["highlight"] == label), "–")

    current_year_series = next((s for s in mort_year["series"] if s["highlight"] == "current"), None)
    mortality_ytd = next((v for v in reversed(current_year_series["values"]) if v is not None), "–") if current_year_series else "–"

    html = TEMPLATE.format(
        week=now.isocalendar()[1],
        year=now.year,
        updated=now.strftime("%d.%m.%Y"),
        mortality_current=mortality["current"] if mortality else "–",
        mortality_delta_label=(f"{'+' if mortality['delta_pp'] >= 0 else ''}{mortality['delta_pp']}pp" if mortality else "–"),
        mortality_delta_color=("#c1392b" if mortality and mortality["delta_pp"] > 0 else "#1baf7a" if mortality and mortality["delta_pp"] < 0 else "var(--text-muted)"),
        mortality_ytd=mortality_ytd,
        mortality_labels_json=json.dumps(mortality["labels"] if mortality else []),
        mortality_values_json=json.dumps(mortality["values"] if mortality else []),
        discard_current=losses["current"] if losses else "–",
        discard_delta_label=(f"{'+' if losses['delta_pp'] >= 0 else ''}{losses['delta_pp']}pp" if losses else "–"),
        discard_labels_json=json.dumps(losses["labels"] if losses else []),
        discard_values_json=json.dumps(losses["values"] if losses else []),
        romming_12mo=f"{losses['romming_12mo']:,}" if losses else "–",
        andre_12mo=f"{losses['andre_12mo']:,}" if losses else "–",
        fh_labels_json=json.dumps(fh_labels),
        fh_values_json=json.dumps(fh_values),
        mort_year_labels_json=json.dumps(mort_year["labels"]),
        mort_year_series_json=json.dumps(mort_year["series"]),
        mort_year_current=find_highlight(mort_year["series"], "year", "current"),
        mort_year_recent=find_highlight(mort_year["series"], "year", "recent"),
        mort_year_worst=find_highlight(mort_year["series"], "year", "worst"),
        mort_cohort_series_json=json.dumps(mort_cohort["series"]),
        cohort_current=find_highlight(mort_cohort["series"], "cohort", "current"),
        cohort_recent=find_highlight(mort_cohort["series"], "cohort", "recent"),
        cohort_worst=find_highlight(mort_cohort["series"], "cohort", "worst"),
    )

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")
