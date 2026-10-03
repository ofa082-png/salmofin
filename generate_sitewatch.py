"""
generate_sitewatch.py
----------------------
Renders the site watch page (docs/sitewatch.html): a watchlist of farm sites
flagged by simple rules as of the latest complete week, plus a week-by-week
detail view of any site. Prototyped as a claude.ai artifact (2026-10-03).

Flags: over / near the lice limit, lice rising, new ISA or PD, many treatments,
repeated mechanical or thermal treatment, silage-vessel spike, harvest started,
just stocked, just emptied (definitions in the template footer).
Site detail: production cycles since 2015 (runs of counted fish weeks, gaps up
to 6 weeks), weekly detail from 2024 (when vessel visits start), and the
licences connected to the site (today's register).

Output is deterministic and carries no run timestamp: it only changes when a
new complete week lands or the registers change, so the nightly job commits
roughly weekly even though the embedded data is ~4 MB.
"""

import os
import csv
import json
import re
import statistics
from collections import defaultdict
from google.cloud import bigquery

import generate_kontroll as gk

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FLEET    = os.path.join(BASE_DIR, "vessel_categories.csv")
TEMPLATE = os.path.join(BASE_DIR, "templates", "sitewatch_template.html")
OUT_PATH = os.path.join(BASE_DIR, "docs", "sitewatch.html")
FIRST_YEAR = 2015
VESSEL_START = (2024, 1)
TREAT_CODE = {"mekanisk behandling": "M", "termisk behandling": "T", "ferskvannsbehandling": "F",
              "badebehandling": "B", "fôrbehandling": "I", "annen behandling": "O"}
VESSEL_TYPES = {"Wellboat": "wb", "Processing vessel": "pv", "Silage": "sil", "Fish feed carrier": "feed", "Delicing vessel": "del"}


def name(co):
    return re.sub(r"\bAsa\b", "ASA", re.sub(r"\bAs\b", "AS", (co or "").title()))


def wk(y, w):
    """Week key with 52 weeks per year (ISO week 53 is folded into 52)."""
    return y * 52 + min(w, 52)


def unwk(k):
    return (k - 1) // 52, (k - 1) % 52 + 1


def fetch(c):
    loc = {}
    for r in c.query("SELECT siteNr, name, prodAreaCode, capacity, capacityUnitType FROM salmofin.salmofin.localities").result():
        loc[r.siteNr] = {"n": (r.name or "").title(), "po": int(r.prodAreaCode) if r.prodAreaCode else 0,
                         "cap": round(r.capacity or 0) if r.capacityUnitType == "TN" else 0}
    capsite, lics = defaultdict(float), defaultdict(list)
    for r in c.query("""SELECT legalEntityName co, capacityCurrent cap, CAST(connectedSiteNrs AS STRING) s, licenseNr nr,
                          productionStage st, intention FROM salmofin.salmofin.licenses
                        WHERE productionStage IN ('Tablefish','Broodfish')""").result():
        sites = re.findall(r"\d+", r.s or "")
        for s in sites:
            capsite[(int(s), r.co)] += r.cap or 0
            lics[int(s)].append([r.nr, name(r.co), round(r.cap or 0), r.st, r.intention or "", len(sites)])
    op = {}
    for (s, co), cap in sorted(capsite.items()):
        if s not in op or cap > capsite[(s, op[s])]:
            op[s] = co

    W = defaultdict(dict)
    for r in c.query(f"""SELECT Lokalitetsnummer s, Ar y, Uke w, Voksne_hunnlus af, Over_lusegrense_uke ov, Lusegrense_uke lim,
                          Sjotemperatur t, Har_telt_lakselus cnt, IFNULL(Trolig_uten_fisk,false) nofish, ProduksjonsomraadeId po
                        FROM salmofin.salmofin.lice_bw WHERE Ar >= {FIRST_YEAR}""").result():
        W[r.s][wk(r.y, r.w)] = (r.af, bool(r.ov), r.lim, r.t, bool(r.cnt) and not r.nofish, r.po)
    trt = defaultdict(lambda: defaultdict(set))
    for r in c.query(f"SELECT Lokalitetsnummer s, Ar y, Uke w, Tiltak t, Type_behandling typ FROM salmofin.salmofin.treatments WHERE Ar >= {FIRST_YEAR}").result():
        trt[r.s][wk(r.y, r.w)].add(TREAT_CODE.get(r.typ, "N" if r.t != "medikamentell" else "B"))
    dis = defaultdict(dict)
    for r in c.query(f"""SELECT Lokalitetsnummer s, Ar y, Uke w, Sykdom d, Status st FROM salmofin.salmofin.disease
                        WHERE Ar >= {FIRST_YEAR} AND Sykdom IN ('ILA','PD') AND Status IN ('Påvist','Mistanke')""").result():
        k = wk(r.y, r.w); lab = ("ISA" if r.d == "ILA" else "PD") + ("?" if r.st == "Mistanke" else "")
        prev = dis[r.s].get(k)
        dis[r.s][k] = lab if not prev or prev.endswith("?") else prev
    mm = {}
    with open(FLEET, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            m, t = (r.get("MMSI") or "").strip(), (r.get("Type") or "").strip()
            if m.isdigit() and t in VESSEL_TYPES:
                mm[int(m)] = VESSEL_TYPES[t]
    cfg = bigquery.QueryJobConfig(query_parameters=[bigquery.ArrayQueryParameter("m", "INT64", sorted(mm))])
    vis = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for r in c.query("""SELECT localityNo s, mmsi, EXTRACT(ISOYEAR FROM startTime) y, EXTRACT(ISOWEEK FROM startTime) w, COUNT(*) n
                        FROM salmofin.salmofin.vessel_visits WHERE mmsi IN UNNEST(@m) AND DATE(startTime) < CURRENT_DATE()
                        GROUP BY 1,2,3,4""", job_config=cfg).result():
        vis[r.s][wk(r.y, r.w)][mm[r.mmsi]] += r.n
    return loc, lics, op, W, trt, dis, vis


def last_complete_week(W, trt):
    cnt = defaultdict(int)
    for d in W.values():
        for k, v in d.items():
            if v[4]:
                cnt[k] += 1
    ks, last = sorted(cnt), None
    for i, k in enumerate(ks):
        prior = [cnt[q] for q in ks[max(0, i - 4):i]]
        if prior and cnt[k] >= 0.9 * statistics.median(prior):
            last = k
    return min(last, max(k for d in trt.values() for k in d))


def cycles(Ws, LAST):
    """Runs of weeks with counted fish, gaps up to 6 weeks; at least 5 weeks long."""
    out, cur = [], None
    for k in sorted(k for k, v in Ws.items() if v[4] and k <= LAST):
        if cur and k - cur[1] <= 7:
            cur[1] = k
        else:
            if cur:
                out.append(cur)
            cur = [k, k]
    if cur:
        out.append(cur)
    return [cy for cy in out if cy[1] - cy[0] >= 4]


def build(loc, lics, op, W, trt, dis, vis):
    LAST = last_complete_week(W, trt)
    D24 = wk(*VESSEL_START)
    SITES, CYC, WEEKLY, FLAGS = {}, {}, {}, []
    for s in sorted(W):
        Ws, cys = W[s], cycles(W[s], LAST)
        if not cys:
            continue
        L = loc.get(s, {"n": str(s), "po": 0, "cap": 0})
        po = L["po"] or next((v[5] for v in Ws.values() if v[5]), 0)
        SITES[s] = [L["n"], po, name(op.get(s, "")), L["cap"]]
        V = vis.get(s, {})
        rows = []
        for a, b in cys:
            ya, wa = unwk(a)
            ks = range(a, b + 1)
            af = [Ws[k][0] for k in ks if k in Ws and Ws[k][0] is not None and Ws[k][4]]
            tt = [Ws[k][3] for k in ks if k in Ws and Ws[k][3] is not None and Ws[k][4]]
            ds = sorted({dis[s][k].rstrip("?") for k in ks if k in dis[s]})
            v = defaultdict(int)
            for k in ks:
                for t_, n in V.get(k, {}).items():
                    v[t_] += n
            rows.append([a, b, f"{ya} {'Spring' if wa <= 26 else 'Fall'}", b - a + 1,
                         round(sum(af) / len(af), 2) if af else None, sum(1 for k in ks if k in Ws and Ws[k][1]),
                         round(sum(tt) / len(tt), 1) if tt else None, min(tt) if tt else None, max(tt) if tt else None,
                         "/".join(ds), sum(1 for k in ks if trt[s].get(k)),
                         v["wb"], v["pv"], v["sil"], v["feed"], v["del"], 1 if b >= LAST - 2 else 0])
        CYC[s] = rows
        inc = set(k for k in V if D24 <= k <= LAST)
        for a, b in cys:
            inc.update(range(max(a, D24), b + 1))
        wl = []
        for k in sorted(inc):
            ci = next((i for i, (a, b) in enumerate(cys) if a <= k <= b), -1)
            x, vv = Ws.get(k), V.get(k, {})
            wl.append([k, ci, k - cys[ci][0] + 1 if ci >= 0 else None, x[0] if x and x[4] else None, 1 if x and x[1] else 0,
                       dis[s].get(k, ""), "".join(sorted(trt[s].get(k, ()))),
                       vv.get("wb", 0), vv.get("pv", 0), vv.get("sil", 0), vv.get("feed", 0), vv.get("del", 0)])
        WEEKLY[s] = wl
        FLAGS += flags(s, Ws, cys, trt[s], dis[s], V, LAST, D24)
    y0, w0 = unwk(LAST)
    return {"last": [y0, w0], "lastKey": LAST, "sites": SITES, "cycles": CYC, "weekly": WEEKLY, "flags": FLAGS,
            "lics": {s: sorted(lics[s], key=lambda l: (-l[2], l[0])) for s in SITES if lics.get(s)}}


def flags(s, Ws, cys, T, Dz, V, LAST, D24):
    last = cys[-1]
    active = last[1] >= LAST - 1
    win = lambda n: range(LAST - n + 1, LAST + 1)
    f = []
    if active:
        x = Ws.get(LAST)
        af_now = x[0] if x and x[4] else None
        lim = x[2] if x and x[2] else 0.5
        if any(Ws.get(k, (None, False))[1] for k in win(4)):
            f.append("over")
        elif af_now is not None and af_now >= 0.7 * lim:
            f.append("near")
        prev4 = [Ws[k][0] for k in range(LAST - 7, LAST - 3) if k in Ws and Ws[k][0] is not None]
        if af_now is not None and prev4 and af_now >= 0.25 and af_now >= 2 * max(statistics.mean(prev4), 0.05):
            f.append("rising")
        if any(k in Dz for k in win(6)) and not any(k in Dz for k in range(LAST - 30, LAST - 5)):
            f.append("disease")
        if sum(1 for k in win(4) if T.get(k)) >= 3:
            f.append("treat")
        if sum(1 for k in win(6) if T.get(k, set()) & {"M", "T"}) >= 3:
            f.append("mechtherm")
        pv_now = sum(V.get(k, {}).get("pv", 0) for k in win(2))
        pv_before = sum(V.get(k, {}).get("pv", 0) for k in range(LAST - 9, LAST - 1))
        if pv_now >= 1 and pv_before == 0 and last[1] - last[0] > 26:
            f.append("harvest")
        s4 = sum(V.get(k, {}).get("sil", 0) for k in win(4))
        hist = [sum(V.get(k + j, {}).get("sil", 0) for j in range(4)) for k in range(max(last[0], D24), LAST - 7, 4)]
        if s4 >= 3 and hist and s4 >= 2.5 * max(statistics.mean(hist), 0.5):
            f.append("silage")
        if last[0] >= LAST - 7:
            f.append("stocked")
    elif LAST - 8 <= last[1] < LAST - 1:
        f.append("emptied")
    if not f:
        return []
    x = Ws.get(LAST)
    return [[s, f, x[0] if x and x[4] else None, (LAST - last[0] + 1) if active else None]]


def main():
    data = build(*fetch(gk.get_bq_client()))
    with open(TEMPLATE, encoding="utf-8") as fh:
        html = fh.read().replace("__DATA__", json.dumps(data, separators=(",", ":"), ensure_ascii=False))
    with open(OUT_PATH, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(html)
    cnt = defaultdict(int)
    for _, f, *_ in data["flags"]:
        for x in f:
            cnt[x] += 1
    print(f"Week {data['last'][1]} {data['last'][0]}: {len(data['sites'])} sites, {len(data['flags'])} flagged {dict(cnt)}")
    print(f"Wrote {OUT_PATH} ({len(html):,} chars)")


if __name__ == "__main__":
    main()
