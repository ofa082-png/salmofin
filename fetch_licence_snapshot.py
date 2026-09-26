"""
fetch_licence_snapshot.py
-------------------------
Quarterly script - captures the aquaculture register AS IT STOOD in a given
week and appends it to BigQuery as one snapshot.

WHY THIS EXISTS
    Fiskeridirektoratet's pub-aqua API (see fetch_licenses.py) serves the
    CURRENT state of every licence only. It cannot answer "what was licensed
    capacity in 2019?" - surrendered and expired licences simply vanish.
    BarentsWatch's per-locality endpoint carries the register as attached to
    that locality in that week, so walking localities reconstructs history.

    Endpoint: /v1/geodata/fishhealth/locality/{localityNo}/{year}/{week}
              -> .aquaCultureRegister.licenses[]  (LicenseDto)

DEDUPING IS ESSENTIAL
    A licence is attached to EVERY locality in its group. Summing per locality
    multiply-counts it. We dedupe on the whitespace-stripped licence number,
    which is also the only stable cross-year key - the register formats the
    same licence as "H AV0011" in 2018 and "H AV 0011" in 2026.

DATA TRAPS HANDLED HERE (all found the hard way - do not remove)
    * expirationDate "9999-12-30" is a NULL placeholder, not a date. It leaked
      into the feed for 2020 w21/w34 on 423 commercial licences (~349,000 t),
      all of which are still in the register today. Left as-is it makes a third
      of national capacity look like it is expiring.
    * speciesCategory contains a comma ("Laks, regnbueoerret og oerret"). The
      first version of this series wrote unquoted CSV and shifted every column
      right of it. We go straight to BigQuery, but keep the field quoted if you
      ever add a CSV path.
    * purpose matters. "Kommersiell" is not the whole register - Forskning,
      Utvikling, Slaktemerd, Undervisning and Visning hold real fish that DO
      appear in the biomass statistics. Store every purpose and let the query
      decide; a commercial-only ceiling against an all-purpose biomass
      numerator reads ~112% utilisation and is simply wrong.

RUN
    python fetch_licence_snapshot.py            # any target snapshots missing from BQ
    python fetch_licence_snapshot.py --probe    # 5 localities, no writes, check shape
    python fetch_licence_snapshot.py --year 2026 --week 47   # one specific snapshot

~1,800 localities at ~190 ms each is roughly 6 minutes per snapshot.
"""

import argparse
import base64
import datetime
import json
import os
import time

import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

TOKEN_URL = "https://id.barentswatch.no/connect/token"
BASE      = "https://www.barentswatch.no/bwapi/v1/geodata/fishhealth"

PROJECT_ID = "salmofin"
DATASET_ID = "salmofin"
TABLE      = f"{PROJECT_ID}.{DATASET_ID}.licence_capacity_history"

# One snapshot per calendar month, on a fixed ISO-week grid.
#
# The grid deliberately CONTAINS the old quarterly weeks 8, 21, 34 and 47
# (February, May, August, November). The 2018-2026 history was captured at
# those four weeks a year, so anchoring here means every existing snapshot
# stays on-grid and only the eight remaining months per year need fetching.
# Picking, say, "the ISO week containing the 15th" instead lands on 7/20/33/46
# - one week off - which would strand the whole backfill and litter the table
# with near-duplicate snapshots seven days apart.
#
# History of the cadence: the first series sampled week 26 only, so capacity
# granted in H2 stayed invisible until the following June. That sampling lag is
# what made 2020 utilisation read 114% - a measurement artefact, not a breach.
# Widened to quarterly, now monthly. Spacing before the monthly switch is
# irregular, so never assume a fixed step between snapshots.
MONTH_WEEKS = (4, 8, 12, 17, 21, 25, 30, 34, 38, 43, 47, 51)
FIRST_YEAR  = 2018


def target_week(month: int) -> int:
    """The ISO week this calendar month is sampled at. 1-indexed month."""
    return MONTH_WEEKS[month - 1]

SLEEP   = 0.06        # BarentsWatch asks for single-threaded use
TIMEOUT = 30


def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


def get_token() -> str:
    """
    The BW client is registered for client_secret_basic. Sending the secret in
    the form body is rejected with invalid_client on some client registrations
    even though the discovery document advertises both, so try Basic first and
    fall back to the body form used by fetch_lice.py.
    """
    cid, secret = os.environ["BW_CLIENT_ID"], os.environ["BW_CLIENT_SECRET"]
    basic = base64.b64encode(f"{cid}:{secret}".encode()).decode()
    resp = requests.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials", "scope": "api"},
        headers={"Content-Type": "application/x-www-form-urlencoded",
                 "Authorization": "Basic " + basic},
        timeout=20,
    )
    if resp.status_code == 200:
        return resp.json()["access_token"]
    resp = requests.post(TOKEN_URL, data={
        "grant_type": "client_credentials", "client_id": cid,
        "client_secret": secret, "scope": "api"}, timeout=20)
    resp.raise_for_status()
    return resp.json()["access_token"]


def get(url: str, token: str, tries: int = 3):
    for _ in range(tries):
        try:
            r = requests.get(url, headers={"Authorization": f"Bearer {token}"},
                             timeout=TIMEOUT)
            if r.status_code == 404:
                return None                     # locality absent that week
            if r.status_code == 401:
                return "__EXPIRED__"
            if r.status_code == 429:
                time.sleep(4)
                continue
            if not r.ok:
                time.sleep(0.8)
                continue
            return r.json()
        except requests.RequestException:
            time.sleep(0.8)
    return None


def clean_date(v):
    """9999-* is the register's NULL placeholder. See module docstring."""
    if not v or str(v).startswith("9999"):
        return None
    return v


def localities_from_bq(client) -> list:
    rows = client.query(
        f"SELECT DISTINCT siteNr FROM `{PROJECT_ID}.{DATASET_ID}.localities` "
        "WHERE siteNr IS NOT NULL ORDER BY siteNr"
    ).result()
    return [r.siteNr for r in rows]


def existing_snapshots(client) -> set:
    try:
        rows = client.query(
            f"SELECT DISTINCT snapshot_year, snapshot_week FROM `{TABLE}`"
        ).result()
        return {(r.snapshot_year, r.snapshot_week) for r in rows}
    except Exception:
        return set()          # table not created yet - first run


def fetch_snapshot(token: str, localities: list, year: int, week: int) -> pd.DataFrame:
    seen, hits = {}, 0
    for i, loc in enumerate(localities):
        payload = get(f"{BASE}/locality/{loc}/{year}/{week}", token)
        if payload == "__EXPIRED__":
            token = get_token()
            payload = get(f"{BASE}/locality/{loc}/{year}/{week}", token)
        time.sleep(SLEEP)
        if not payload:
            continue
        licences = (payload.get("aquaCultureRegister") or {}).get("licenses") or []
        if licences:
            hits += 1
        for L in licences:
            raw = L.get("licenseNo")
            if not raw:
                continue
            key = "".join(raw.split())        # the stable cross-year join key
            if key in seen:
                continue                       # same licence, another locality
            seen[key] = {
                "snapshot_year":   year,
                "snapshot_week":   week,
                "licenseNo":       key,
                "licenseNoRaw":    raw,
                "licensee":        L.get("licensee"),
                "capacity":        L.get("capacity"),
                "unit":            L.get("unit"),
                "purpose":         L.get("purpose"),
                "productionType":  L.get("productionType"),
                "speciesCategory": L.get("speciesCategory"),
                "isGreen":         L.get("isGreen"),
                "grantDate":       clean_date(L.get("grantDate")),
                "expirationDate":  clean_date(L.get("expirationDate")),
                "nLocalities":     len(L.get("localities") or []),
            }
        if i % 250 == 0:
            print(f"    {year}w{week:02d}  {i}/{len(localities)}  licences {len(seen)}",
                  flush=True)

    df = pd.DataFrame(list(seen.values()))
    if df.empty:
        return df

    df["snapshot_date"] = pd.to_datetime(datetime.date.fromisocalendar(year, week, 1))
    df["capacity"]      = pd.to_numeric(df["capacity"], errors="coerce")
    df["nLocalities"]   = pd.to_numeric(df["nLocalities"], errors="coerce").astype("Int64")
    df["isGreen"]       = df["isGreen"].astype("boolean")
    for c in ("grantDate", "expirationDate"):
        df[c] = pd.to_datetime(df[c], errors="coerce")

    salmon = df[
        df["productionType"].str.contains("atfisk", na=False)
        & df["speciesCategory"].str.contains("aks", na=False)
        & df["unit"].str.upper().isin(["TN", "TONN"])
    ]
    commercial = salmon[salmon["purpose"] == "Kommersiell"]["capacity"].sum()
    print(f"  {year}w{week:02d}: {hits} localities with licences, "
          f"{len(df)} distinct licences, salmonid matfisk {len(salmon)} "
          f"({salmon['capacity'].sum():,.0f} t; commercial {commercial:,.0f} t)")
    return df


def append(client, df: pd.DataFrame) -> None:
    if df.empty:
        raise SystemExit("0 rows - refusing to write")
    year, week = int(df["snapshot_year"].iloc[0]), int(df["snapshot_week"].iloc[0])
    # Idempotent: a re-run replaces its own snapshot rather than duplicating it.
    try:
        client.query(f"DELETE FROM `{TABLE}` WHERE snapshot_year={year} "
                     f"AND snapshot_week={week}").result()
    except Exception:
        pass                                   # table does not exist yet
    client.load_table_from_dataframe(
        df, TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    ).result()
    print(f"  wrote {len(df):,} rows to {TABLE}")


def probe(token: str) -> None:
    for year, week in ((FIRST_YEAR, 26), (datetime.date.today().year, 8)):
        print(f"--- {year} w{week} ---")
        for loc in (10029, 12108, 13235, 11966, 30777):
            j = get(f"{BASE}/locality/{loc}/{year}/{week}", token)
            time.sleep(SLEEP)
            if not j:
                print(f"  {loc}: no response / 404")
                continue
            lic = (j.get("aquaCultureRegister") or {}).get("licenses") or []
            print(f"  {loc}  licences={len(lic)}")
            for L in lic[:3]:
                print(f"      {L.get('licenseNo')}  cap={L.get('capacity')}"
                      f"{L.get('unit') or ''}  {L.get('purpose')} / "
                      f"{L.get('productionType')}  exp={L.get('expirationDate') or '-'}")


def due_snapshots(client) -> list:
    """
    Every monthly snapshot from FIRST_YEAR to now that BigQuery lacks.

    Weeks already present from the earlier quarterly series are skipped, so
    switching cadence does not re-fetch anything. Backfilling the full monthly
    history is ~100 snapshots at roughly 6 minutes each - run it locally with a
    raised --max-snapshots rather than through Actions.
    """
    have = existing_snapshots(client)
    this_year, this_week, _ = datetime.date.today().isocalendar()
    out = []
    for year in range(FIRST_YEAR, this_year + 1):
        for month in range(1, 13):
            week = target_week(month)
            if year == this_year and week > this_week:
                continue                       # not reachable yet
            if (year, week) not in have:
                out.append((year, week))
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--probe", action="store_true")
    ap.add_argument("--year", type=int)
    ap.add_argument("--week", type=int)
    ap.add_argument("--max-snapshots", type=int, default=2,
                    help="cap per run so a scheduled job cannot sit for hours")
    args = ap.parse_args()

    token = get_token()
    if args.probe:
        probe(token)
        raise SystemExit

    client = get_bq_client()
    localities = localities_from_bq(client)
    print(f"localities to query: {len(localities):,}")

    if args.year and args.week:
        todo = [(args.year, args.week)]
    else:
        todo = due_snapshots(client)
        print(f"snapshots missing from BigQuery: {len(todo)} -> {todo}")
        todo = todo[-args.max_snapshots:]

    for year, week in todo:
        token = get_token()                    # short-lived; refresh per snapshot
        df = fetch_snapshot(token, localities, year, week)
        append(client, df)

    print("All done.")
