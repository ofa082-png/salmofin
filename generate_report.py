"""
generate_report.py
-------------------
Renders the fish health report and writes it to docs/fiskehelse.html, for
GitHub Pages to serve. Nightly script.

Rebuilt 2026-10-02 in the control-report visual system (English, region tabs,
templates/fiskehelse_template.html). Four parts:
  1. Mortality: Fiskeridirektoratet dead fish per month vs an estimate from
     silage-vessel visits (silage boats collect dead fish), fitted per region
     as dead tonnes = a + b x silage visits per day. Months past FD's latest
     month are estimated from visits once >= 7 days of visits exist.
  2. The vessel fish-health indicator (silage / feed-carrier visits, the same
     ratio as dodelighet.html) against the mortality rate.
  3. ISA and PD: sites with suspected or confirmed disease per month
     (BarentsWatch disease table).
  4. Current Mattilsynet cases: snapshot counts, last-14-day table and map.

Earlier history: moved off docs/index.html 2026-08-16; lice and mortality
content split out to lakselus.html / dodelighet.html 2026-08-19.
"""

import os
import csv
import json
import math
import datetime
from collections import defaultdict
from google.cloud import bigquery

import generate_kontroll as gk

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
OUT_PATH  = os.path.join(BASE_DIR, "docs", "fiskehelse.html")
TEMPLATE  = os.path.join(BASE_DIR, "templates", "fiskehelse_template.html")
FLEET_CSV = os.path.join(BASE_DIR, "vessel_categories.csv")
VESSEL_START = (2024, 1)      # vessel_visits coverage starts here
DISEASE_START = 2018
MIN_DAYS_PARTIAL = 7

DISEASE_LABEL = {"PANKREASSYKDOM": "PD", "INFEKSIOES_LAKSEANEMI": "ISA", "INFEKSIØS_LAKSEANEMI": "ISA",
                 "BAKTERIELL_NYRESYKE": "BKD", "FRANCISELLOSE": "Francisellosis",
                 "SYSTEMISK_INFEKSJON_MED_FLAVOBACTERIUM_PSYCHROPHILUM": "Flavobacteriosis"}


def load_fleet():
    out = {}
    with open(FLEET_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            m, t = (row.get("MMSI") or "").strip(), (row.get("Type") or "").strip()
            if m.isdigit() and t in ("Silage", "Fish feed carrier"):
                out[int(m)] = t
    return out


def fetch_visits(client, fleet):
    """Visits per area x month x vessel type, plus days covered per month."""
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ArrayQueryParameter("m", "INT64", list(fleet))])
    rows = client.query("""
        SELECT DATE(v.startTime) d, v.mmsi, l.prodAreaCode po, COUNT(*) n
        FROM salmofin.salmofin.vessel_visits v
        LEFT JOIN salmofin.salmofin.localities l ON l.siteNr = v.localityNo
        WHERE v.mmsi IN UNNEST(@m) AND DATE(v.startTime) < CURRENT_DATE()
        GROUP BY 1, 2, 3""", job_config=cfg).result()
    V = defaultdict(float)
    last = None
    for r in rows:
        t = gk.ym(r.d.year, r.d.month)
        for a in gk.areas_of(r.po):
            V[(a, t, fleet[r.mmsi])] += r.n
        last = r.d if last is None or r.d > last else last
    return V, last


def fetch_disease_history(client):
    rows = client.query(f"""
        WITH po AS (SELECT Lokalitetsnummer loc, ANY_VALUE(ProduksjonsomraadeId) po
                    FROM salmofin.salmofin.lice_bw WHERE ProduksjonsomraadeId IS NOT NULL GROUP BY 1),
        d AS (SELECT DISTINCT Ar, Uke, Lokalitetsnummer loc, Sykdom
              FROM salmofin.salmofin.disease
              WHERE Ar >= {DISEASE_START} AND Sykdom IN ('ILA', 'PD') AND Status IN ('Påvist', 'Mistanke')),
        dm AS (SELECT loc, Sykdom,
                 DATE_ADD(DATE_TRUNC(DATE(Ar, 1, 4), ISOWEEK), INTERVAL (Uke - 1) * 7 + 3 DAY) dt FROM d)
        SELECT EXTRACT(YEAR FROM dt) y, EXTRACT(MONTH FROM dt) m, po.po, Sykdom s, COUNT(DISTINCT dm.loc) n
        FROM dm LEFT JOIN po USING (loc) GROUP BY 1, 2, 3, 4""").result()
    H = defaultdict(lambda: {"pd": 0, "ila": 0})
    for r in rows:
        for a in gk.areas_of(r.po):
            H[(a, gk.ym(r.y, r.m))]["pd" if r.s == "PD" else "ila"] += r.n
    return H


def fetch_cases(client):
    snap = list(client.query("""
        SELECT h.lokalitetsnummer loc, h.lokalitetsnavn name, h.sykdomstype s, l.prodAreaCode po,
               l.latitude lat, l.longitude lon
        FROM salmofin.salmofin.mattilsynet_helsestatus h
        LEFT JOIN salmofin.salmofin.localities l ON h.lokalitetsnummer = l.siteNr""").result())
    recent = list(client.query("""
        SELECT d.lokalitetsnummer loc, d.lokalitetsnavn name, d.sykdomstype s, l.prodAreaCode po,
          CASE WHEN avslutningsdato IS NOT NULL THEN 'Closed'
               WHEN diagnosedato IS NOT NULL THEN 'Confirmed' ELSE 'Suspected' END status,
          COALESCE(avslutningsdato, diagnosedato, kvalitetssikretMistankedato, varslingsdato, opprettet) dt,
          opprettet >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY) is_new
        FROM salmofin.salmofin.mattilsynet_disease d
        LEFT JOIN salmofin.salmofin.localities l ON d.lokalitetsnummer = l.siteNr
        WHERE COALESCE(avslutningsdato, diagnosedato, kvalitetssikretMistankedato, varslingsdato, opprettet)
              >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY)
        ORDER BY dt DESC""").result())
    sites = {}
    for r in snap:
        s = sites.setdefault(r.loc, {"name": (r.name or "").title(), "po": r.po, "lat": r.lat, "lon": r.lon, "d": []})
        s["d"].append(DISEASE_LABEL.get(r.s, r.s))
    return {
        "sites": [dict(v, id=k) for k, v in sites.items()],
        "recent": [{"name": (r.name or "").title(), "d": DISEASE_LABEL.get(r.s, r.s), "po": r.po, "status": r.status,
                    "dt": r.dt.strftime("%d %b") if r.dt else "", "new": bool(r.is_new)} for r in recent],
    }


def ols1(xs, ys):
    n = len(xs); mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs); sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    b = sxy / sxx
    return my - b * mx, b


def corr(xs, ys):
    p = [(x, y) for x, y in zip(xs, ys) if x is not None and y is not None]
    if len(p) < 4:
        return None
    mx = sum(x for x, _ in p) / len(p); my = sum(y for _, y in p) / len(p)
    sxy = sum((x - mx) * (y - my) for x, y in p)
    return round(sxy / math.sqrt(sum((x - mx) ** 2 for x, _ in p) * sum((y - my) ** 2 for _, y in p)), 2)


def build(salmon, V, last_visit, H):
    T1 = max(r["t"] for r in salmon)
    M = defaultdict(lambda: defaultdict(float))
    for r in salmon:
        for a in gk.areas_of(r["po"]):
            x = M[(a, r["t"])]
            x["dead"] += r["dead"]; x["n"] += r["n"]
            if r["n"]:
                x["deadt"] += r["dead"] * r["bio"] / r["n"]       # dead count x mean weight (t)
    t0 = gk.ym(*VESSEL_START)
    tlast = gk.ym(last_visit.year, last_visit.month)
    out = {"T1": T1, "areas": {}}
    for a in gk.AREAS:
        rows = []
        for t in range(t0, tlast + 1):
            days = gk.days_in_month(t) if t < tlast else last_visit.day
            sil, feed = V.get((a, t, "Silage"), 0.0), V.get((a, t, "Fish feed carrier"), 0.0)
            m = M.get((a, t)) if t <= T1 else None
            prev_n = M[(a, t - 1)]["n"] if (a, t - 1) in M else None
            rate = None
            if m and m["n"]:
                nbar = (prev_n + m["n"]) / 2 if prev_n else m["n"]
                rate = round((1 - math.exp(-m["dead"] / nbar)) * 100, 3)
            rows.append({"t": t, "days": days, "sil": round(sil), "feed": round(feed), "silpd": round(sil / days, 2),
                         "ratio": round(sil / feed, 3) if feed else None,
                         "deadt": round(m["deadt"]) if m else None, "dead": round(m["dead"] / 1e6, 3) if m else None,
                         "rate": rate})
        fit = [r for r in rows if r["deadt"] is not None and r["days"] == gk.days_in_month(r["t"])]
        a0, b0 = ols1([r["silpd"] for r in fit], [r["deadt"] for r in fit])
        # leave-one-out error
        errs = []
        for i, r in enumerate(fit):
            rest = fit[:i] + fit[i + 1:]
            ai, bi = ols1([q["silpd"] for q in rest], [q["deadt"] for q in rest])
            errs.append(abs(ai + bi * r["silpd"] - r["deadt"]) / r["deadt"])
        for r in rows:
            r["est"] = round(a0 + b0 * r["silpd"]) if r["days"] >= MIN_DAYS_PARTIAL else None
        d = {r["t"]: r for r in fit}
        yoy = [(math.log(d[t]["sil"] / d[t - 12]["sil"]), math.log(d[t]["deadt"] / d[t - 12]["deadt"]))
               for t in d if t - 12 in d and d[t]["sil"] and d[t - 12]["sil"] and d[t]["deadt"] and d[t - 12]["deadt"]]
        dis = [{"t": t, **H[(a, t)]} for t in range(DISEASE_START * 12, tlast + 1) if (a, t) in H]
        out["areas"][a] = {
            "rows": rows, "dis": dis,
            "model": {"a": round(a0, 1), "b": round(b0, 1), "mape": round(sum(errs) / len(errs) * 100, 1), "n": len(fit),
                      "r_t": corr([r["silpd"] for r in fit], [r["deadt"] for r in fit]),
                      "r_rate": corr([r["ratio"] for r in fit], [r["rate"] for r in fit]),
                      "r_yoy": corr([x for x, _ in yoy], [y for _, y in yoy]), "n_yoy": len(yoy)},
        }
    out["lastVisit"] = last_visit.isoformat()
    return out


def main():
    client = gk.get_bq_client()
    print("Fetching biomass...")
    salmon, _ = gk.fetch_biomass(client)
    print("Fetching vessel visits...")
    V, last_visit = fetch_visits(client, load_fleet())
    print("Fetching disease history and cases...")
    H = fetch_disease_history(client)
    data = build(salmon, V, last_visit, H)
    data["cases"] = fetch_cases(client)
    data["updated"] = datetime.datetime.now(datetime.timezone.utc).strftime("%d %b %Y")
    with open(TEMPLATE, encoding="utf-8") as f:
        html = f.read().replace("__DATA__", json.dumps(data, separators=(",", ":"), ensure_ascii=False))
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    for a in gk.AREAS:
        print(a, data["areas"][a]["model"])
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")


if __name__ == "__main__":
    main()
