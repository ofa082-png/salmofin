"""
load_licence_history_backfill.py
--------------------------------
One-off loader - fills licence_capacity_history from the licence_snap_*.csv
files already captured on disk, instead of re-fetching them from BarentsWatch.

WHY
    fetch_licence_snapshot.py can rebuild the whole history from the API, but
    that is ~6 minutes per snapshot and the quarterly 2018-2026 series was
    already captured. Loading the CSVs takes about a minute and produces rows
    identical in shape to what the fetcher writes, so the monthly job simply
    carries on from where these leave off.

    The monthly grid in fetch_licence_snapshot.py deliberately contains the
    quarterly weeks 8/21/34/47 these files sit on, so nothing here is orphaned
    or duplicated.

WHAT THESE FILES DO AND DO NOT HAVE
    They carry year, week, licenceNo, licensee, capacity, unit, purpose,
    productionType, speciesCategory, grantDate, expirationDate, nLocalities.
    They do NOT carry isGreen, so it is written as NULL for backfilled rows -
    a fetched snapshot will have it. Do not read isGreen as "false" for
    anything before the monthly series starts.

    Do NOT load licence_history.csv. That file is the older week-26-only series
    and its speciesCategory ("Laks, regnbueoerret og oerret") was written
    unquoted, so every column right of it is shifted by one. The licence_snap_*
    files quote correctly. This loader only reads licence_snap_*.csv.

USAGE
    python load_licence_history_backfill.py --dir "C:/path/with/the/csvs"
    python load_licence_history_backfill.py --dir . --dry-run

Auth: GOOGLE_CREDENTIALS if set (same as the scheduled jobs), otherwise
application-default credentials, since this is normally run by hand.
"""

import argparse
import datetime
import glob
import json
import os
import re

import pandas as pd
from google.cloud import bigquery

PROJECT_ID = "salmofin"
DATASET_ID = "salmofin"
TABLE      = f"{PROJECT_ID}.{DATASET_ID}.licence_capacity_history"

COLUMNS = [
    "snapshot_year", "snapshot_week", "snapshot_date",
    "licenseNo", "licenseNoRaw", "licensee", "capacity", "unit", "purpose",
    "productionType", "speciesCategory", "isGreen",
    "grantDate", "expirationDate", "nLocalities",
]


def get_bq_client():
    if os.environ.get("GOOGLE_CREDENTIALS"):
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_info(
            json.loads(os.environ["GOOGLE_CREDENTIALS"]),
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(credentials=creds, project=PROJECT_ID)
    import google.auth
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"])
    print("  (using application-default credentials)")
    return bigquery.Client(credentials=creds, project=PROJECT_ID)


def clean_ts(series: pd.Series) -> pd.Series:
    """9999-* is the register's NULL placeholder for 'no expiry'."""
    s = series.astype("string")
    s = s.mask(s.str.startswith("9999", na=False))
    return pd.to_datetime(s, errors="coerce")


def read_snapshot(path: str) -> pd.DataFrame:
    year, week = map(int, re.search(r"(\d{4})w(\d{2})", os.path.basename(path)).groups())
    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])

    out = pd.DataFrame({
        "snapshot_year":   year,
        "snapshot_week":   week,
        "snapshot_date":   pd.to_datetime(datetime.date.fromisocalendar(year, week, 1)),
        # Strip whitespace again rather than trusting the file: it is the dedup
        # and cross-year join key, and the register formats it inconsistently.
        "licenseNo":       df["licenseNo"].astype("string").str.replace(r"\s+", "", regex=True),
        "licenseNoRaw":    df["licenseNoRaw"].astype("string"),
        "licensee":        df["licensee"].astype("string"),
        "capacity":        pd.to_numeric(df["capacity"], errors="coerce"),
        "unit":            df["unit"].astype("string"),
        "purpose":         df["purpose"].astype("string"),
        "productionType":  df["productionType"].astype("string"),
        "speciesCategory": df["speciesCategory"].astype("string"),
        "isGreen":         pd.Series([pd.NA] * len(df), dtype="boolean"),
        "grantDate":       clean_ts(df["grantDate"]),
        "expirationDate":  clean_ts(df["expirationDate"]),
        "nLocalities":     pd.to_numeric(df["nLocalities"], errors="coerce").astype("Int64"),
    })

    before = len(out)
    out = out.drop_duplicates(subset=["licenseNo"])
    if len(out) != before:
        print(f"    dropped {before - len(out)} duplicate licence rows")
    return out[COLUMNS]


def existing_snapshots(client) -> set:
    try:
        rows = client.query(
            f"SELECT DISTINCT snapshot_year, snapshot_week FROM `{TABLE}`").result()
        return {(r.snapshot_year, r.snapshot_week) for r in rows}
    except Exception:
        return set()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dir", default=".", help="directory holding licence_snap_*.csv")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    paths = sorted(glob.glob(os.path.join(args.dir, "licence_snap_*.csv")))
    if not paths:
        raise SystemExit(f"no licence_snap_*.csv found in {args.dir}")
    print(f"found {len(paths)} snapshot files")

    client = get_bq_client()
    have = existing_snapshots(client)
    if have:
        print(f"table already holds {len(have)} snapshots; those are skipped")

    frames = []
    for p in paths:
        year, week = map(int, re.search(r"(\d{4})w(\d{2})", os.path.basename(p)).groups())
        if (year, week) in have:
            print(f"  {year}w{week:02d}  already loaded, skipping")
            continue
        df = read_snapshot(p)
        frames.append(df)
        salmon = df[
            df["productionType"].str.contains("atfisk", na=False)
            & df["speciesCategory"].str.contains("aks", na=False)
            & df["unit"].str.upper().isin(["TN", "TONN"])
        ]
        print(f"  {year}w{week:02d}  {len(df):>5} licences   salmonid matfisk "
              f"{len(salmon):>4} ({salmon['capacity'].sum():>10,.0f} t)")

    if not frames:
        print("nothing to load.")
        return

    all_rows = pd.concat(frames, ignore_index=True)
    print(f"\ntotal rows to load: {len(all_rows):,} "
          f"across {all_rows.groupby(['snapshot_year','snapshot_week']).ngroups} snapshots")

    if args.dry_run:
        print("dry run - nothing written.")
        return

    client.load_table_from_dataframe(
        all_rows, TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    ).result()
    print(f"wrote {len(all_rows):,} rows to {TABLE}")


if __name__ == "__main__":
    main()
