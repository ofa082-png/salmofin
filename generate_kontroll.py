"""
generate_kontroll.py
--------------------
Renders the monthly Biological Control Report (docs/kontroll.html).

Everything is derived from tables already in BigQuery:
  - biomass        Fiskeridirektoratet biomass statistics, PO x generation x month
                   (salmon for all figures; rainbow trout only for MTB use, since
                   salmon and trout share one MTB pool)
  - lice_bw        sea temperature from the weekly lice counts
  - licenses /     sea-site licence MTB per region
    localities

Method notes (see the report footer for the reader-facing version):
  * Fiskeridirektoratet has NO stocking field for fish over 500 g. Stocking is
    split into <250 g (UTSETT_SMOLT_STK), 250-500 g (UTSETT_SMOLT_STK_MINDRE_ENN_500G
    minus the former) and >500 g, which is recovered from the count balance of
    each generation in its stocking year (fish that appear in Behfisk_stk with no
    reported stocking). ANDRE_NY_STK is a LOSS, not an inflow. Dec 2022 is a
    one-off reporting error (57 M ghost stocking) and is masked.
  * Class weights are estimated per region by OLS on PO-months of stocking-year
    generations:  dBiomass + harvest + losses*mean_wt
                    = a*feed + w1*N(<250) + w2*N(250-500) + w3*N(>500)
  * Growth at sea = dBiomass + harvest + mortality biomass - stocked biomass.
    Biological FCR = feed / growth. eFCR = feed / (dBiomass + harvest).
  * TGC = 1000*(W1^(1/3) - W0^(1/3)) / (degC*days), W in grams, per PO x
    generation x month, biomass-weighted. Shown as a range: uncorrected (harvest
    removes the biggest fish, so too low) to harvest-corrected start weight
    (fragile in heavy-harvest months, so possibly too high).

Runs monthly after fetch_biomass.yml (FD publishes on the 20th).
"""

import os
import json
import math
import datetime
from collections import defaultdict

from google.cloud import bigquery

PROJECT_ID = "salmofin"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE = os.path.join(BASE_DIR, "templates", "kontroll_template.html")
OUT_PATH = os.path.join(BASE_DIR, "docs", "kontroll.html")

AREAS = ["Norge", "Vest", "Midt", "Nord"]
DEC_2022 = 2022 * 12 + 11
NORM_YEARS = range(2019, 2024)


def get_bq_client():
    """Service account in CI (GOOGLE_CREDENTIALS), application default locally."""
    if os.environ.get("GOOGLE_CREDENTIALS"):
        from google.oauth2 import service_account
        info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
        creds = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/cloud-platform"])
        return bigquery.Client(credentials=creds, project=PROJECT_ID)
    return bigquery.Client(project=PROJECT_ID)


def region(po):
    try:
        p = int(po)
    except (TypeError, ValueError):
        return None
    return "Vest" if p <= 4 else "Midt" if p <= 7 else "Nord"


def areas_of(po):
    r = region(po)
    return ["Norge", r] if r else ["Norge"]


def ym(y, m):
    return int(y) * 12 + int(m) - 1


def days_in_month(t):
    y, m = divmod(t, 12)
    nxt = datetime.date(y + (m == 11), (m + 1) % 12 + 1, 1)
    return (nxt - datetime.date(y, m + 1, 1)).days


# ---------------------------------------------------------------- data pulls

def fetch_biomass(client):
    q = """
    SELECT Ar y, Maaned_kode m, PO_kode po, Utsettsar g, Artsid sp,
      SUM(Behfisk_stk) n, SUM(Biomasse_kg)/1000 bio,
      SUM(IFNULL(Utsett_smolt_stk_under500g,0)) inp, SUM(IFNULL(Utsett_smolt_stk,0)) inp_s,
      SUM(IFNULL(Andre_ny_stk,0)) andre_ny, SUM(IFNULL(Uttak_stk,0)) harv,
      SUM(IFNULL(Dodfisk_stk,0)) dead, SUM(IFNULL(Utkast_stk,0)) utk,
      SUM(IFNULL(Romming_stk,0)) rom, SUM(IFNULL(Andre_stk,0)) andre,
      SUM(IFNULL(Tellefeil_stk,0)) tf, SUM(IFNULL(Uttak_kg,0))/1000 harv_t,
      SUM(IFNULL(Forforbruk_kg,0))/1000 feed
    FROM `salmofin.salmofin.biomass`
    WHERE Utsettsar IS NOT NULL
    GROUP BY 1,2,3,4,5"""
    rows = []
    for r in client.query(q).result():
        d = dict(r)
        for k in ("n", "bio", "inp", "inp_s", "andre_ny", "harv", "dead", "utk", "rom",
                  "andre", "tf", "harv_t", "feed"):
            d[k] = float(d[k] or 0)
        d["y"], d["m"], d["g"] = int(d["y"]), int(d["m"]), int(d["g"])
        d["po"] = str(d["po"])
        d["t"] = ym(d["y"], d["m"])
        rows.append(d)
    salmon = [r for r in rows if r["sp"] == "LAKS"]
    trout = [r for r in rows if r["sp"] != "LAKS"]
    return salmon, trout


def fetch_temperature(client):
    q = """
    SELECT CAST(ProduksjonsomraadeId AS INT64) po,
      EXTRACT(YEAR FROM DATE_ADD(DATE_TRUNC(DATE(Ar,1,4), ISOWEEK), INTERVAL (Uke-1)*7+3 DAY)) y,
      EXTRACT(MONTH FROM DATE_ADD(DATE_TRUNC(DATE(Ar,1,4), ISOWEEK), INTERVAL (Uke-1)*7+3 DAY)) m,
      AVG(Sjotemperatur) t
    FROM `salmofin.salmofin.lice_bw`
    WHERE Ar >= 2017 AND Sjotemperatur BETWEEN -2 AND 25 AND Trolig_uten_fisk = false
      AND ProduksjonsomraadeId IS NOT NULL
    GROUP BY 1,2,3"""
    return {(str(r["po"]), ym(r["y"], r["m"])): float(r["t"]) for r in client.query(q).result()}


def fetch_mtb(client):
    sites = {}
    for r in client.query("SELECT CAST(siteNr AS STRING) site, placementType pt, prodAreaCode po "
                          "FROM `salmofin.salmofin.localities`").result():
        sites[r["site"]] = (r["pt"], r["po"])
    mtb = defaultdict(float)
    unknown = 0.0
    q = ("SELECT prodAreaCode po, intention, capacityCurrent cap, connectedSiteNrs sites "
         "FROM `salmofin.salmofin.licenses` WHERE productionStage IN ('Tablefish','Broodfish')")
    for r in client.query(q).result():
        cap = float(r["cap"] or 0)
        sea = [s.strip() for s in (r["sites"] or "").split(",")
               if s.strip() in sites and sites[s.strip()][0] == "Offshore"]
        if not sea or r["intention"] == "SLAUGHTER PEN":
            continue
        po = r["po"] if r["po"] not in (None, "", "(null)") else None
        if not po:
            counts = defaultdict(int)
            for s in sea:
                if sites[s][1]:
                    counts[sites[s][1]] += 1
            po = max(counts, key=counts.get) if counts else None
        reg = region(po)
        if reg:
            mtb[reg] += cap
        else:
            unknown += cap
    mtb["Norge"] = sum(mtb[a] for a in ("Vest", "Midt", "Nord"))
    return {a: round(mtb[a]) for a in AREAS}, round(unknown)


# ---------------------------------------------------------------- helpers

def solve(A, b):
    """Gaussian elimination for the small normal-equation systems."""
    n = len(b)
    M = [row[:] + [b[i]] for i, row in enumerate(A)]
    for i in range(n):
        p = max(range(i, n), key=lambda r: abs(M[r][i]))
        M[i], M[p] = M[p], M[i]
        if abs(M[i][i]) < 1e-12:
            return None
        for j in range(n):
            if j != i:
                f = M[j][i] / M[i][i]
                for c in range(i, n + 1):
                    M[j][c] -= f * M[i][c]
    return [M[i][n] / M[i][i] for i in range(n)]


def young_po_months(salmon):
    """Stocking-year generation aggregated per (po, t)."""
    K = {}
    for r in salmon:
        if r["y"] != r["g"]:
            continue
        k = (r["po"], r["t"])
        c = K.setdefault(k, defaultdict(float))
        c["m"] = r["m"]
        for f in ("n", "bio", "inp", "inp_s", "harv", "harv_t", "feed"):
            c[f] += r[f]
        c["loss"] += r["dead"] + r["utk"] + r["rom"] + r["andre"] + r["andre_ny"]
        c["out"] += r["harv"] + r["dead"] + r["utk"] + r["rom"] + r["andre"] + r["andre_ny"]
        c["tf"] += r["tf"]
    return K


def class_weights(salmon, y0, y1):
    """OLS class weights (kg/fish) per area on stocking-year PO-months."""
    K = young_po_months(salmon)
    obs = defaultdict(list)
    for (po, t), c in K.items():
        y = t // 12
        if y < y0 or y > y1 or t == DEC_2022:
            continue
        prev = K.get((po, t - 1)) if c["m"] > 1 else None
        pn, pb = (prev["n"], prev["bio"]) if prev else (0.0, 0.0)
        unb = -(pn + c["inp"] - c["out"] + c["tf"] - c["n"])
        mw = c["bio"] / c["n"] if c["n"] else 0
        X = c["bio"] - pb + c["harv_t"] + c["loss"] * mw
        row = [c["feed"], c["inp_s"] / 1000, (c["inp"] - c["inp_s"]) / 1000, unb / 1000]
        for a in areas_of(po):
            obs[a].append((row, X))
    out = {}
    for a in AREAS:
        A = [[0.0] * 4 for _ in range(4)]
        b = [0.0] * 4
        for row, X in obs[a]:
            for i in range(4):
                b[i] += row[i] * X
                for j in range(4):
                    A[i][j] += row[i] * row[j]
        coef = solve(A, b) or [0, 0.15, 0.30, 0.80]
        out[a] = {"lt": round(coef[1], 3), "mid": round(coef[2], 3), "big": round(coef[3], 3)}
    return out


# ---------------------------------------------------------------- build

def build(salmon, trout, temp_po, mtb, mtb_unknown):
    T1 = max(r["t"] for r in salmon)
    T0 = T1 - 24
    H0 = T1 - 50
    yr = T1 // 12

    # monthly aggregates per area
    M = {a: defaultdict(lambda: defaultdict(float)) for a in AREAS}
    for r in salmon:
        t = r["t"]
        if t < T0 - 1:
            continue
        mw = r["bio"] / r["n"] if r["n"] else 0
        for a in areas_of(r["po"]):
            c = M[a][t]
            c["bio"] += r["bio"]; c["n"] += r["n"]; c["ht"] += r["harv_t"]; c["hn"] += r["harv"]
            c["dn"] += r["dead"]; c["db"] += r["dead"] * mw; c["feed"] += r["feed"]
            c["utk"] += r["utk"]; c["rom"] += r["rom"]; c["andre"] += r["andre_ny"]; c["tf"] += r["tf"]
    for r in trout:
        if r["t"] >= T0 - 1:
            for a in areas_of(r["po"]):
                M[a][r["t"]]["trout"] += r["bio"]

    # count balance per PO x generation x month
    G = {}
    for r in salmon:
        k = (r["po"], r["g"], r["t"])
        c = G.setdefault(k, defaultdict(float))
        c["young"] = r["y"] == r["g"]; c["m"] = r["m"]
        c["n"] += r["n"]; c["s"] += r["inp_s"]; c["u"] += r["inp"]; c["tf"] += r["tf"]
        c["out"] += r["harv"] + r["dead"] + r["utk"] + r["rom"] + r["andre"] + r["andre_ny"]
    for (po, g, t), c in G.items():
        if t < T0 or t == DEC_2022:
            continue
        prev = G.get((po, g, t - 1))
        a_n = 0.0 if (c["young"] and c["m"] == 1) else (prev["n"] if prev else 0.0)
        res = a_n + c["u"] - c["out"] + c["tf"] - c["n"]
        for a in areas_of(po):
            m = M[a][t]
            if c["young"]:
                m["lt"] += c["s"]; m["mid"] += c["u"] - c["s"]; m["bigNet"] += -res
            else:
                m["resOld"] += res

    data = {"T0": T0, "T1": T1, "mtb": mtb, "mtbUnknown": mtb_unknown,
            "w": class_weights(salmon, yr - 2, yr), "m": {}, "nullpo": {}, "hist": {},
            "temp": {}, "tgc": {}, "tgcRaw": {}, "norm": {}, "sea": {}}
    for a in AREAS:
        lst = []
        for t in range(T0 - 1, T1 + 1):
            c = M[a][t]
            big = max(0.0, c["bigNet"])
            lst.append({"t": t, "bio": round(c["bio"]), "n": round(c["n"] / 1e6, 2), "ht": round(c["ht"]),
                        "hn": round(c["hn"] / 1e6, 3), "dn": round(c["dn"] / 1e6, 3), "db": round(c["db"]),
                        "feed": round(c["feed"]), "lt": round(c["lt"] / 1e6, 3), "mid": round(c["mid"] / 1e6, 3),
                        "big": round(big / 1e6, 3), "bigNet": round(c["bigNet"] / 1e6, 3),
                        "utk": round(c["utk"] / 1e6, 3), "rom": round(c["rom"] / 1e6, 4),
                        "andre": round(c["andre"] / 1e6, 3), "tf": round(c["tf"] / 1e6, 3),
                        "resOld": round(c["resOld"] / 1e6, 3), "trout": round(c["trout"])})
        data["m"][a] = lst

    for r in salmon:
        if r["po"] == "(null)" and r["t"] >= T0:
            o = data["nullpo"].setdefault(r["t"], {"n": 0.0, "b": 0.0})
            o["n"] += r["n"]; o["b"] += r["bio"]
    data["nullpo"] = {t: {"n": round(o["n"] / 1e6, 1), "b": round(o["b"])} for t, o in data["nullpo"].items()}

    # history for comparison lines, temperature (biomass-weighted)
    hist = {a: defaultdict(lambda: [0.0, 0.0]) for a in AREAS}
    tw = {a: defaultdict(lambda: [0.0, 0.0]) for a in AREAS}
    for r in salmon:
        t = r["t"]
        for a in areas_of(r["po"]):
            if t >= H0:
                hist[a][t][0] += r["bio"]; hist[a][t][1] += r["n"] / 1e6
        T = temp_po.get((r["po"], t))
        if T is not None and r["po"] != "(null)":
            for a in areas_of(r["po"]):
                tw[a][t][0] += T * r["bio"]; tw[a][t][1] += r["bio"]
    for a in AREAS:
        data["hist"][a] = {t: {"bio": round(v[0]), "n": round(v[1], 2)} for t, v in hist[a].items()}
        data["temp"][a] = {t: round(v[0] / v[1], 2) for t, v in tw[a].items() if t >= H0 and v[1]}

    # TGC, corrected and uncorrected
    by = {(r["po"], r["g"], r["t"]): r for r in salmon if r["po"] != "(null)"}
    acc = defaultdict(lambda: [0.0, 0.0, 0.0])
    for (po, g, t), r in by.items():
        p = by.get((po, g, t - 1))
        if not p or not p["n"] or not r["n"] or r["inp"] > 0:
            continue
        if r["y"] == r["g"] and r["m"] <= 7:
            continue
        T = temp_po.get((po, t))
        if T is None or T < 1:
            continue
        w0 = p["bio"] * 1e6 / p["n"]
        hw = r["harv_t"] * 1e6 / r["harv"] if r["harv"] else 0
        loss = r["dead"] + r["utk"] + r["rom"] + r["andre_ny"]
        nrem = p["n"] - r["harv"] - loss
        if nrem < p["n"] * 0.5:
            continue
        w0r = (p["n"] * w0 - r["harv"] * hw - loss * w0) / nrem
        w1 = r["bio"] * 1e6 / r["n"]
        if w0r <= 50:
            continue
        dd = T * days_in_month(t)
        corr = 1000 * (w1 ** (1 / 3) - w0r ** (1 / 3)) / dd
        raw = 1000 * (w1 ** (1 / 3) - w0 ** (1 / 3)) / dd
        if not (-2 < corr < 8):
            continue
        for a in areas_of(po):
            o = acc[(a, t)]
            o[0] += corr * r["bio"]; o[1] += raw * r["bio"]; o[2] += r["bio"]
    for a in AREAS:
        data["tgc"][a] = {t: round(o[0] / o[2], 2) for (aa, t), o in acc.items() if aa == a and t >= H0}
        data["tgcRaw"][a] = {t: round(o[1] / o[2], 2) for (aa, t), o in acc.items() if aa == a and t >= H0}
        norm = {}
        for mo in range(12):
            tv = [tw[a][y * 12 + mo][0] / tw[a][y * 12 + mo][1] for y in NORM_YEARS if tw[a][y * 12 + mo][1]]
            gv = [acc[(a, y * 12 + mo)][0] / acc[(a, y * 12 + mo)][2] for y in NORM_YEARS if acc[(a, y * 12 + mo)][2]]
            norm[mo] = {"temp": round(sum(tv) / len(tv), 2) if tv else None,
                        "tgc": round(sum(gv) / len(gv), 2) if gv else None}
        data["norm"][a] = norm

    # months at sea per generation (stocking incl. >500 g, harvest-weighted exit)
    gen = defaultdict(lambda: defaultdict(float))
    for r in salmon:
        age = (r["y"] - r["g"]) * 12 + r["m"]
        for a in areas_of(r["po"]):
            o = gen[(a, r["g"])]
            o["hn"] += r["harv"]; o["hm"] += r["harv"] * age
    for (po, g, t), c in G.items():
        if not c["young"] or t == DEC_2022:
            continue
        prev = G.get((po, g, t - 1))
        a_n = 0.0 if c["m"] == 1 else (prev["n"] if prev else 0.0)
        inp = c["u"] + max(0.0, -(a_n + c["u"] - c["out"] + c["tf"] - c["n"]))
        for a in areas_of(po):
            o = gen[(a, g)]
            o["in"] += inp; o["im"] += inp * c["m"]
    g_first, g_last = 2018, yr - 2
    for a in AREAS:
        data["sea"][a] = {}
        for g in (g_first, g_last):
            o = gen[(a, g)]
            if o["hn"] and o["in"]:
                data["sea"][a][g] = round(o["hm"] / o["hn"] - o["im"] / o["in"], 1)
    data["seaGens"] = [g_first, g_last]
    return data


def main():
    client = get_bq_client()
    salmon, trout = fetch_biomass(client)
    temp_po = fetch_temperature(client)
    mtb, unknown = fetch_mtb(client)
    data = build(salmon, trout, temp_po, mtb, unknown)
    data["updated"] = datetime.date.today().isoformat()
    with open(TEMPLATE, encoding="utf-8") as f:
        html = f.read()
    html = html.replace("__DATA__", json.dumps(data, separators=(",", ":")))
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    last = data["m"]["Norge"][-1]
    print(f"Wrote {OUT_PATH}: report month t={data['T1']}, Norway biomass {last['bio']:,} t, "
          f"weights {data['w']}")


if __name__ == "__main__":
    main()
