"""
generate_biokontroll.py
-----------------------
Renders the monthly biological control report - biomass balance, growth, feed
factor and mortality against the MTB ceiling, nationally and per region.

It answers the question a biological controller actually signs off each month:
does the biomass reconcile, and is the harvest covered by growth?

    opening biomass + growth - harvest - mortality = closing biomass

Growth is a RESIDUAL, not a reported figure. Fiskeridirektoratet reports
standing biomass, harvest and mortality counts; growth is what must have
happened in between. Anyone reconciling this against a company's own production
figures will find small differences, and that is why.

Mortality tonnage is likewise derived: reported mortality is a COUNT, so it is
multiplied by mean weight in the pen. Dead fish are on average lighter than the
standing stock, so this slightly overstates the tonnage lost - which is also why
the biomass loss rate (~5-6%) looks so different from the count-based mortality
rate (~10-17%). Both are correct; they measure different things.

Feed factor is feed divided by that residual growth, i.e. biological FCR over
the whole standing population, not per cohort and not economic FCR.

OUTPUT
    <out>/biokontroll.html        the report
    <out>/biokontroll_data.json   the same figures as data, for reuse

WHERE IT PUBLISHES, AND HOW TO MOVE IT LATER
    The scheduled workflow runs with --out docs, which is the PUBLIC GitHub
    Pages site. Both the page and biokontroll_data.json beside it are then
    world-readable. That is a deliberate choice for now.

    To move this behind a paywall later, nothing in this script needs to change.
    Two things do:
      1. change --out in .github/workflows/generate_biokontroll.yml so the files
         land somewhere the paying site can read and the public cannot, and drop
         the git-auto-commit step that writes them into the repo;
      2. delete docs/biokontroll.html and docs/biokontroll_data.json - they stay
         readable in git history, so treat anything already published as public.

    biokontroll_data.json exists precisely to make that move cheap: a subscriber
    site can render its own page from it and never expose the figures directly.

    The local default stays ./build so a manual run never writes to the public
    site by accident.

SCHEDULE
    Fiskeridirektoratet publishes biomass statistics on the 20th of each month.
    fetch_biomass.yml lands them on the 21st at 06:00 UTC, so this runs on the
    21st at 09:00. Running on the 20th would render the previous month.

USAGE
    python generate_biokontroll.py                      # -> ./build
    python generate_biokontroll.py --out docs           # public Pages site
    python generate_biokontroll.py --months 25 --out build
"""

import argparse
import datetime
import json
import os
from collections import defaultdict

from google.cloud import bigquery

PROJECT_ID = "salmofin"
DATASET_ID = "salmofin"
TEMPLATE   = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "biokontroll_template.html")

# 25 months so the page can compare a full rolling 12 against the previous 12.
# Below 25 the rolling comparison is not computable and the table would be wrong.
MONTHS = 25

SCOPES = ("Norway", "West", "Mid", "North")


def region_of(po: int) -> str:
    if po <= 5:
        return "West"
    if po <= 7:
        return "Mid"
    return "North"


def get_bq_client():
    if os.environ.get("GOOGLE_CREDENTIALS"):
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_info(
            json.loads(os.environ["GOOGLE_CREDENTIALS"]),
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        return bigquery.Client(credentials=creds, project=PROJECT_ID)
    import google.auth
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    print("  (application-default credentials)")
    return bigquery.Client(credentials=creds, project=PROJECT_ID)


BIOMASS_SQL = f"""
SELECT Ar AS y, Maaned_kode AS m, SAFE_CAST(PO_kode AS INT64) AS po,
       SUM(Biomasse_kg)/1000   AS bio,
       SUM(Uttak_kg)/1000      AS harv,
       SUM(Uttak_stk)          AS harv_n,
       SUM(Dodfisk_stk)        AS dead_n,
       SUM(Behfisk_stk)        AS n,
       SUM(Forforbruk_kg)/1000 AS feed
FROM `{PROJECT_ID}.{DATASET_ID}.biomass`
WHERE SAFE_CAST(PO_kode AS INT64) IS NOT NULL
GROUP BY 1,2,3
"""

# Sea-cage MTB from the newest licence snapshot, all purposes. Production area
# comes from the licence's connected sites, not licenses.prodAreaCode, which is
# only populated for commercial licences - see build_licence_dim.py.
CEILING_SQL = f"""
WITH lic AS (
  SELECT REPLACE(legacyLicenseNr,' ','') AS k, SPLIT(connectedSiteNrs,',') AS sites
  FROM `{PROJECT_ID}.{DATASET_ID}.licenses`
  WHERE connectedSiteNrs IS NOT NULL AND connectedSiteNrs != ''),
po AS (
  SELECT k, MIN(SAFE_CAST(loc.prodAreaCode AS INT64)) AS po
  FROM lic l, UNNEST(l.sites) s
  JOIN `{PROJECT_ID}.{DATASET_ID}.localities` loc ON loc.siteNr = SAFE_CAST(s AS INT64)
  WHERE loc.prodAreaCode IS NOT NULL AND loc.prodAreaCode != ''
  GROUP BY k),
latest AS (
  SELECT snapshot_year, snapshot_week
  FROM `{PROJECT_ID}.{DATASET_ID}.licence_capacity_history`
  ORDER BY snapshot_date DESC LIMIT 1)
SELECT po.po AS po, ROUND(SUM(h.capacity)) AS cap
FROM `{PROJECT_ID}.{DATASET_ID}.licence_capacity_history` h
JOIN po ON po.k = h.licenseNo
JOIN latest ON latest.snapshot_year = h.snapshot_year
           AND latest.snapshot_week = h.snapshot_week
WHERE h.productionType LIKE '%atfisk%' AND h.speciesCategory LIKE '%aks%'
  AND UPPER(h.unit) IN ('TN','TONN')
GROUP BY 1
"""


def fetch(client):
    raw = defaultdict(lambda: defaultdict(float))
    for r in client.query(BIOMASS_SQL).result():
        for scope in ("Norway", region_of(r.po)):
            c = raw[(scope, r.y, r.m)]
            for f in ("bio", "harv", "harv_n", "dead_n", "n", "feed"):
                c[f] += float(getattr(r, f) or 0)

    ceiling = {s: 0.0 for s in SCOPES}
    for r in client.query(CEILING_SQL).result():
        ceiling["Norway"] += float(r.cap or 0)
        ceiling[region_of(r.po)] += float(r.cap or 0)
    return raw, {k: round(v) for k, v in ceiling.items()}


def build_series(raw, months_wanted):
    """Monthly rows per scope, oldest first. The first month is consumed as the
    opening balance for the second, so months_wanted+1 months are read."""
    all_months = sorted({(y, m) for (_, y, m) in raw})
    keep = all_months[-(months_wanted + 1):]
    out = {}
    for scope in SCOPES:
        rows = []
        for i, (y, m) in enumerate(keep):
            if i == 0:
                continue
            cur, prev = raw[(scope, y, m)], raw[(scope, *keep[i - 1])]
            if cur["n"] <= 0 or prev["n"] <= 0:
                continue
            mean_w = cur["bio"] * 1000 / cur["n"]           # kg per fish in the pen
            dead_t = cur["dead_n"] * mean_w / 1000
            growth = cur["bio"] - prev["bio"] + cur["harv"] + dead_t
            rows.append({
                "y": y, "m": m,
                "bio":    round(cur["bio"]),
                "prod":   round(growth),
                "harv":   round(cur["harv"]),
                "dead_t": round(dead_t),
                "feed":   round(cur["feed"]),
                "fcr":    round(cur["feed"] / growth, 2) if growth > 200 else None,
                "wt":     round(cur["harv"] * 1000 / cur["harv_n"], 2) if cur["harv_n"] else None,
                "mw":     round(mean_w, 2),
                "n":      round(cur["n"] / 1e6, 1),
                "harv_n": round(cur["harv_n"] / 1e6, 2),
                "dead_n": round(cur["dead_n"] / 1e6, 2),
            })
        out[scope] = rows
    return out


def rolling12(series):
    """Last 12 months against the 12 before them. Needs 24 rows; None if short."""
    out = {}
    for scope, rows in series.items():
        if len(rows) < 24:
            print(f"  WARNING - {scope} has {len(rows)} months, need 24 for a "
                  f"rolling comparison; table will be omitted")
            return None
        cur, prv = rows[-12:], rows[-24:-12]
        tot = lambda L, k: sum(x[k] for x in L)
        h, p, d = tot(cur, "harv"), tot(cur, "prod"), tot(cur, "dead_t")
        out[scope] = {
            "harv": round(h), "prod": round(p), "dead": round(d),
            "hv": round((h / tot(prv, "harv") - 1) * 100, 1) if tot(prv, "harv") else 0,
            "pv": round((p / tot(prv, "prod") - 1) * 100, 1) if tot(prv, "prod") else 0,
        }
    return out


def render(series, ceiling, r12, out_dir, page_months):
    with open(TEMPLATE, encoding="utf-8") as f:
        html = f.read()
    page = {s: series[s][-page_months:] for s in series}
    html = html.replace("/*__MONTHLY__*/null", json.dumps(page, separators=(",", ":")))
    html = html.replace("/*__CEILING__*/null", json.dumps(ceiling, separators=(",", ":")))
    html = html.replace("/*__ROLLING12__*/null",
                        json.dumps(r12, separators=(",", ":")) if r12 else "null")

    os.makedirs(out_dir, exist_ok=True)
    html_path = os.path.join(out_dir, "biokontroll.html")
    with open(html_path, "w", encoding="utf-8") as f:
        f.write(html)

    data_path = os.path.join(out_dir, "biokontroll_data.json")
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump({"generated_at": datetime.datetime.now(datetime.timezone.utc)
                                     .strftime("%Y-%m-%dT%H:%M:%SZ"),
                   "ceiling_t": ceiling, "monthly": series, "rolling12": r12},
                  f, ensure_ascii=False, separators=(",", ":"))
    return html_path, data_path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="build",
                    help="output directory. docs/ is the public Pages site; see module docstring")
    ap.add_argument("--months", type=int, default=MONTHS,
                    help="months of history to compute (>=25 for rolling 12v12)")
    ap.add_argument("--page-months", type=int, default=13,
                    help="months embedded in the page's chart")
    args = ap.parse_args()

    client = get_bq_client()
    print("Querying biomass and licence ceiling...")
    raw, ceiling = fetch(client)

    series = build_series(raw, args.months)
    last = series["Norway"][-1]
    print(f"  {len(series['Norway'])} months, latest {last['y']}-{last['m']:02d}")
    print(f"  sea-cage MTB: " +
          "  ".join(f"{s} {ceiling[s]:,}" for s in SCOPES))

    r12 = rolling12(series)
    if r12:
        print("  rolling 12m harvest: " +
              "  ".join(f"{s} {r12[s]['harv']:,} ({r12[s]['hv']:+.1f}%)" for s in SCOPES))

    # Reconciliation check: the balance must close on the latest month.
    prev = series["Norway"][-2]
    closes = prev["bio"] + last["prod"] - last["harv"] - last["dead_t"]
    drift = abs(closes - last["bio"])
    if drift > max(50, last["bio"] * 0.0005):
        raise SystemExit(f"balance does not close: {closes:,} vs {last['bio']:,} "
                         f"(drift {drift:,}) - refusing to write")
    print(f"  balance closes to within {drift:,.0f} t")

    h, d = render(series, ceiling, r12, args.out, args.page_months)
    print(f"Wrote {h}\nWrote {d}")


if __name__ == "__main__":
    main()
