"""
generate_lakselus.py
---------------------
Renders the lice and treatment report and writes it to docs/lakselus.html,
for GitHub Pages to serve. Nightly script.

Rebuilt 2026-10-02 in the control-report visual system (English, region tabs,
templates/lakselus_template.html). Complete weeks only: a week is shown once at
least 90% of the usual number of sites have reported lice counts, and treatments
and delousing-vessel visits stop at the same week.

Sections:
  * Season curve: adult female lice per fish by ISO week, this year against the
    2017+ normal (geometric mean per week), the range, and the last two years,
    with a forecast to year end (up to 12 weeks ahead). The forecast starts from
    the log gap between the latest week and its normal: one method keeps the
    gap, the other lets it fade at the rate seen since 2017 (AR(1) on the gap,
    plus the sea-temperature anomaly, which also fades). Leave-one-year-out
    backtest from the same week gives the typical error, added to the band.
  * Life stages (attached, mobile, adult female), last 12 weeks.
  * Lice treatments per week by type (BarentsWatch; type is only recorded from
    2024 week 10, earlier non-medicinal treatments are "unspecified").
  * Late-summer sea temperature (weeks 28-36) against autumn lice (36-44).
  * Delousing-vessel visits per week (AIS) - descriptive only, catches about a
    quarter of registered treatments.

Earlier history: split out of generate_report.py 2026-08-19.
"""

import os
import csv
import json
import math
import datetime
import statistics
from collections import defaultdict
from google.cloud import bigquery

import generate_kontroll as gk

BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
OUT_PATH  = os.path.join(BASE_DIR, "docs", "lakselus.html")
TEMPLATE  = os.path.join(BASE_DIR, "templates", "lakselus_template.html")
FLEET_CSV = os.path.join(BASE_DIR, "vessel_categories.csv")
FIRST_YEAR = 2017          # traffic-light era; normal and model fit from here
STAGE_WEEKS = 12
TREAT_WEEKS = 52
VESSEL_WEEKS = 26
MAX_HORIZON = 12
COMPLETE_SHARE = 0.9

TREAT_TYPES = [("mekanisk behandling", "Mechanical"), ("termisk behandling", "Thermal"),
               ("ferskvannsbehandling", "Freshwater"), ("annen behandling", "Other non-medicinal"),
               (None, "Non-medicinal, unspecified"), ("badebehandling", "Bath"), ("fôrbehandling", "In-feed")]


def wkey(y, w):
    return y * 53 + w


def fetch_lice(client):
    rows = client.query(f"""
        SELECT Ar y, LEAST(Uke, 52) w, ProduksjonsomraadeId po, COUNT(*) ns,
          SUM(Voksne_hunnlus) af, SUM(Lus_i_bevegelige_stadier) mob, SUM(Fastsittende_lus) att,
          COUNTIF(Voksne_hunnlus IS NOT NULL) n, COUNTIF(Over_lusegrense_uke) ov,
          SUM(Sjotemperatur) ts, COUNTIF(Sjotemperatur IS NOT NULL) tn
        FROM salmofin.salmofin.lice_bw
        WHERE Ar >= {FIRST_YEAR} AND Har_telt_lakselus AND NOT IFNULL(Trolig_uten_fisk, false)
        GROUP BY 1, 2, 3""").result()
    A = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for r in rows:
        for a in gk.areas_of(r.po):
            x = A[a][(r.y, r.w)]
            for k in ("ns", "af", "mob", "att", "n", "ov", "ts", "tn"):
                x[k] += getattr(r, k) or 0
    return A


def fetch_treatments(client):
    rows = client.query(f"""
        WITH po AS (SELECT Lokalitetsnummer loc, ANY_VALUE(ProduksjonsomraadeId) po
                    FROM salmofin.salmofin.lice_bw WHERE ProduksjonsomraadeId IS NOT NULL GROUP BY 1)
        SELECT t.Ar y, LEAST(t.Uke, 52) w, COALESCE(t.ProduksjonsomraadeId, po.po) po, t.Tiltak tiltak,
          t.Type_behandling typ, COUNT(DISTINCT t.Lokalitetsnummer) n
        FROM salmofin.salmofin.treatments t LEFT JOIN po ON po.loc = t.Lokalitetsnummer
        WHERE t.Ar >= {FIRST_YEAR - 1}
        GROUP BY 1, 2, 3, 4, 5""").result()
    T = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    for r in rows:
        label = next((l for k, l in TREAT_TYPES if k == r.typ), None)
        if label is None:
            label = "Non-medicinal, unspecified" if r.tiltak != "medikamentell" else "Bath"
        for a in gk.areas_of(r.po):
            T[a][(r.y, r.w)][label] += r.n
    return T


def fetch_vessels(client):
    mm = []
    with open(FLEET_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            m = (row.get("MMSI") or "").strip()
            if m.isdigit() and (row.get("Type") or "").strip() == "Delicing vessel":
                mm.append(int(m))
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("m", "INT64", mm)])
    rows = client.query(f"""
        SELECT EXTRACT(ISOYEAR FROM v.startTime) y, LEAST(EXTRACT(ISOWEEK FROM v.startTime), 52) w, l.prodAreaCode po, COUNT(*) n
        FROM salmofin.salmofin.vessel_visits v
        LEFT JOIN salmofin.salmofin.localities l ON l.siteNr = v.localityNo
        WHERE v.mmsi IN UNNEST(@m) AND DATE(v.startTime) >= DATE_SUB(CURRENT_DATE(), INTERVAL {VESSEL_WEEKS * 7 + 21} DAY)
        GROUP BY 1, 2, 3""", job_config=cfg).result()
    V = defaultdict(lambda: defaultdict(float))
    for r in rows:
        for a in gk.areas_of(r.po):
            V[a][(r.y, r.w)] += r.n
    return V


# ---------- forecast model ----------

def prev(y, w):
    return (y, w - 1) if w > 1 else (y - 1, 52)


def nxt(y, w):
    return (y, w + 1) if w < 52 else (y + 1, 1)


def clim(S, years):
    al, th = defaultdict(list), defaultdict(list)
    for (y, w), (l, T) in S.items():
        if y in years:
            al[w].append(math.log(l)); th[w].append(T)
    return {w: sum(v) / len(v) for w, v in al.items()}, {w: sum(v) / len(v) for w, v in th.items()}


def anom(S, th, y, w, span=4):
    v = []
    for _ in range(span):
        if (y, w) in S and w in th:
            v.append(S[(y, w)][1] - th[w])
        y, w = prev(y, w)
    return sum(v) / len(v) if v else 0.0


def fit(S, years):
    al, th = clim(S, years)
    X, Y = [], []
    for (y, w), (l, T) in S.items():
        p = prev(y, w)
        if y in years and p in S and p[1] in al and w in al:
            X.append((math.log(S[p][0]) - al[p[1]], anom(S, th, y, w))); Y.append(math.log(l) - al[w])
    sxx = sum(a * a for a, _ in X); syy = sum(b * b for _, b in X); sxy = sum(a * b for a, b in X)
    sxz = sum(a * t for (a, _), t in zip(X, Y)); syz = sum(b * t for (_, b), t in zip(X, Y))
    det = sxx * syy - sxy * sxy
    rho, beta = (sxz * syy - syz * sxy) / det, (syz * sxx - sxz * sxy) / det
    num = den = 0.0
    for (y, w), (l, T) in S.items():
        p = prev(y, w)
        if y in years and p in S and w in th and p[1] in th:
            a0, a1 = S[p][1] - th[p[1]], T - th[w]; num += a0 * a1; den += a0 * a0
    return rho, beta, num / den if den else 0.0, al, th


def forecast(S, rho, beta, phi, al, th, y0, w0, H):
    Sx = dict(S); g = math.log(S[(y0, w0)][0]) - al[w0]; d = g; a0 = S[(y0, w0)][1] - th[w0]
    out, y, w = [], y0, w0
    for h in range(1, H + 1):
        y, w = nxt(y, w)
        if w not in al:
            break
        Sx[(y, w)] = (None, th[w] + a0 * phi ** h)
        d = rho * d + beta * anom(Sx, th, y, w)
        out.append({"w": w, "keep": math.exp(al[w] + g), "fade": math.exp(al[w] + d)})
    return out


def area_payload(lice, treat, vessels, last):
    y0, w0 = last
    fit_years = list(range(FIRST_YEAR, y0))
    S = {k: (x["af"] / x["n"], x["ts"] / x["tn"]) for k, x in lice.items()
         if x["n"] and x["tn"] and x["af"] > 0 and wkey(*k) <= wkey(y0, w0)}
    rho, beta, phi, al, th = fit(S, fit_years)
    H = min(MAX_HORIZON, 52 - w0)
    # leave-one-year-out backtest from the same week
    errs, bt = [], []
    for yt in fit_years:
        if (yt, w0) not in S or H < 1:
            continue
        yrs = [y for y in fit_years if y != yt]
        r_, b_, p_, al_, th_ = fit(S, yrs)
        fc = forecast(S, r_, b_, p_, al_, th_, yt, w0, H)
        act = [S[(yt, f["w"])][0] for f in fc if (yt, f["w"]) in S]
        if not act:
            continue
        mid = sum((f["keep"] + f["fade"]) / 2 for f in fc) / len(fc)
        bt.append({"y": yt, "fc": round(mid, 3), "act": round(sum(act) / len(act), 3)})
        errs.append(abs(mid - sum(act) / len(act)))
    mae = sum(errs) / len(errs) if errs else 0.03
    fc = []
    if H >= 1 and (y0, w0) in S:
        for f in forecast(S, rho, beta, phi, al, th, y0, w0, H):
            mid = (f["keep"] + f["fade"]) / 2
            fc.append({"w": f["w"], "mid": round(mid, 3), "lo": round(max(0, min(f["keep"], f["fade"], mid - mae)), 3),
                       "hi": round(max(f["keep"], f["fade"], mid + mae), 3)})
    weeks = defaultdict(dict)
    for (y, w), x in lice.items():
        if wkey(y, w) <= wkey(y0, w0) and x["n"]:
            weeks[y][w] = [round(x["af"] / x["n"], 3), round(x["ts"] / x["tn"], 2) if x["tn"] else None]
    band = {w: [round(min(S[(y, w)][0] for y in fit_years if (y, w) in S), 3),
                round(max(S[(y, w)][0] for y in fit_years if (y, w) in S), 3)]
            for w in al if any((y, w) in S for y in fit_years)}
    # life stages, last STAGE_WEEKS complete weeks
    def stage_norm(st, w):
        v = [math.log(lice[(y, w)][st] / lice[(y, w)]["n"]) for y in fit_years
             if (y, w) in lice and lice[(y, w)]["n"] and lice[(y, w)][st] > 0]
        return math.exp(sum(v) / len(v)) if v else None
    stages, k = [], (y0, w0)
    for _ in range(STAGE_WEEKS):
        x = lice.get(k)
        if x and x["n"]:
            e = {"y": k[0], "w": k[1], "ov": round(x["ov"] / x["n"] * 100, 1), "ns": int(x["n"])}
            for st in ("af", "mob", "att"):
                v, nm = x[st] / x["n"], stage_norm(st, k[1])
                e[st] = round(v, 3); e[st + "N"] = round(nm, 3) if nm else None
                e[st + "P"] = round((v / nm - 1) * 100, 1) if nm else None
            stages.append(e)
        k = prev(*k)
    stages.reverse()
    # treatments, last TREAT_WEEKS weeks, plus same week a year earlier
    tr, k = [], (y0, w0)
    for _ in range(TREAT_WEEKS):
        x = lice.get(k)
        tr.append({"y": k[0], "w": k[1], "by": {l: int(v) for l, v in treat.get(k, {}).items()},
                   "sw": int(x["ns"]) if x else None})
        k = prev(*k)
    tr.reverse()
    ly = treat.get((y0 - 1, w0), {})
    # late summer temp vs autumn lice
    seas = []
    for y in range(FIRST_YEAR, y0 + 1):
        T = [S[(y, w)][1] for w in range(28, 37) if (y, w) in S]
        L = [S[(y, w)][0] for w in range(36, 45) if (y, w) in S]
        if len(T) >= 6 and L:
            seas.append({"y": y, "T": round(sum(T) / len(T), 2), "L": round(sum(L) / len(L), 3), "nL": len(L)})
    # delousing vessels
    ves, k = [], (y0, w0)
    for _ in range(VESSEL_WEEKS):
        ves.append({"y": k[0], "w": k[1], "n": int(vessels.get(k, 0))})
        k = prev(*k)
    ves.reverse()
    cur = lice[(y0, w0)]
    lyx = lice.get((y0 - 1, w0))
    return {
        "weeks": weeks, "norm": {w: round(math.exp(v), 3) for w, v in al.items()},
        "tnorm": {w: round(v, 2) for w, v in th.items()}, "band": band, "fc": fc,
        "model": {"mae": round(mae, 3), "n": len(bt), "rho": round(rho, 2)}, "bt": bt,
        "stages": stages, "treat": tr, "treatLY": int(sum(ly.values())),
        "swLY": int(lyx["ns"]) if lyx else None, "seas": seas, "vessels": ves,
        "now": {"af": round(cur["af"] / cur["n"], 3), "ov": round(cur["ov"] / cur["n"] * 100, 1), "sites": int(cur["n"]),
                "temp": round(cur["ts"] / cur["tn"], 1) if cur["tn"] else None,
                "afLY": round(lyx["af"] / lyx["n"], 3) if lyx and lyx["n"] else None},
    }


def last_complete_week(lice_norge, treat_norge):
    keys = sorted(lice_norge, key=lambda k: wkey(*k))
    last = None
    for i, k in enumerate(keys):
        prior = [lice_norge[q]["n"] for q in keys[max(0, i - 4):i]]
        if prior and lice_norge[k]["n"] >= COMPLETE_SHARE * statistics.median(prior):
            last = k
    tmax = max(treat_norge, key=lambda k: wkey(*k))
    return last if wkey(*last) <= wkey(*tmax) else tmax


def main():
    client = gk.get_bq_client()
    print("Fetching lice counts...")
    L = fetch_lice(client)
    print("Fetching treatments...")
    T = fetch_treatments(client)
    print("Fetching delousing-vessel visits...")
    V = fetch_vessels(client)
    last = last_complete_week(L["Norge"], T["Norge"])
    print(f"Last complete week: {last[0]} week {last[1]}")
    data = {"last": list(last), "types": [l for _, l in TREAT_TYPES],
            "updated": datetime.datetime.now(datetime.timezone.utc).strftime("%d %b %Y"),
            "areas": {a: area_payload(L[a], T[a], V[a], last) for a in gk.AREAS}}
    with open(TEMPLATE, encoding="utf-8") as f:
        html = f.read().replace("__DATA__", json.dumps(data, separators=(",", ":"), ensure_ascii=False))
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    for a in gk.AREAS:
        d = data["areas"][a]
        f0 = d["fc"][4] if len(d["fc"]) > 4 else None
        print(a, d["now"], "MAE", d["model"]["mae"], "fc+5", f0)
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")


if __name__ == "__main__":
    main()
