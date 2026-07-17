"""
fetch_aqua_applications.py
--------------------------
Fetches aquaculture license applications from the Fiskeridir Aqua Portal API
and upserts them into the existing BigQuery table salmofin.salmofin.aqua_applications.

Open API — no authentication required for Fiskeridir itself.
BigQuery auth is via a service account JSON in the GOOGLE_CREDENTIALS env var
(same pattern as the other SalmoFin pipelines).

Endpoint chain per application:
  1. GET /api/v1/applications                                      -> paginated list
  2. GET /api/v1/application/{applicationNo}                        -> full detail (stored raw; fields unconfirmed)
  3. GET /api/v1/application/{applicationNo}/submissions            -> paginated submission objects
  4. GET /api/v1/application/{applicationNo}/submission/{id}/data   -> latest submission data (confirmed schema)
  5. GET /api/v1/evaluation/{applicationNo}                         -> evaluation / decision (confirmed schema)

Usage:
  python fetch_aqua_applications.py              # incremental — only applications created since last run
  python fetch_aqua_applications.py --backfill   # full history from START_DATE
"""

import os
import sys
import json
import time
import requests
import pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery
from google.oauth2 import service_account

BASE_URL   = "https://api.fiskeridir.no/aqua-portal-api-public"
PROJECT_ID = "salmofin"
DATASET_ID = "salmofin"
TABLE_ID   = f"{PROJECT_ID}.{DATASET_ID}.aqua_applications"

START_DATE  = "2025-01-01T00:00:00.000Z"  # used for --backfill if the table is empty
PAGE_SIZE   = 100
SLEEP_LIST  = 0.2   # seconds between list pages
SLEEP_APP   = 0.4   # seconds between per-application endpoint calls

session = requests.Session()
session.headers.update({"Accept": "application/json"})


# --------------------------------------------------------------------------- #
# BigQuery client
# --------------------------------------------------------------------------- #

def get_bq_client():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(os.environ["GOOGLE_CREDENTIALS"]),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    return bigquery.Client(credentials=creds, project=PROJECT_ID)


def get_last_created_at(client) -> str | None:
    """MAX(created_at) from the existing table, used as createdAfter for incremental runs."""
    try:
        for row in client.query(
            f"SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E3SZ', MAX(created_at)) AS last "
            f"FROM `{TABLE_ID}`"
        ).result():
            return row.last
    except Exception:
        return None


# --------------------------------------------------------------------------- #
# Generic GET helper
# --------------------------------------------------------------------------- #

def get(path: str, params: dict = None):
    try:
        r = session.get(f"{BASE_URL}{path}", params=params, timeout=30)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"    GET {path} failed: {e}")
        return None


# --------------------------------------------------------------------------- #
# Endpoint 1 — application list (paginated)
# --------------------------------------------------------------------------- #

def fetch_list(created_after: str = None) -> list[dict]:
    all_apps, page = [], 0
    while True:
        params = {"page": page, "size": PAGE_SIZE, "sort": "created,desc"}
        if created_after:
            params["createdAfter"] = created_after

        data = get("/api/v1/applications", params)
        if not data:
            break
        content = data.get("content", [])
        if not content:
            break

        all_apps.extend(content)
        total_pages = data.get("totalPages", 1)
        print(f"  Page {page+1}/{total_pages} — {len(all_apps):,} applications so far")

        if page + 1 >= total_pages:
            break
        page += 1
        time.sleep(SLEEP_LIST)

    return all_apps


# --------------------------------------------------------------------------- #
# Endpoint 2 — application detail (raw backup — field names unconfirmed)
# --------------------------------------------------------------------------- #

def fetch_detail(app_no: str) -> dict | None:
    return get(f"/api/v1/application/{app_no}")


# --------------------------------------------------------------------------- #
# Endpoint 3 — submissions list -> take the most recent
# --------------------------------------------------------------------------- #

def fetch_latest_submission(app_no: str) -> dict | None:
    """
    GET /api/v1/application/{app_no}/submissions
    Confirmed shape: {"content": [{"submissionId": int, "applicationNo": str,
    "submittedAt": str, "siteName": str, "siteLatitudeDecimalDegree": float,
    "siteLongitudeDecimalDegree": float, "countyMunicipalityName": str|None,
    "municipalityName": str|None, "status": str, "statusSetAt": str}], ...}
    API sorts submitted,desc so content[0] is the most recent submission.
    """
    data = get(f"/api/v1/application/{app_no}/submissions",
               params={"page": 0, "size": 1, "sort": "submitted,desc"})
    if not data:
        return None
    content = data.get("content", [])
    if not content:
        return None
    return content[0]


# --------------------------------------------------------------------------- #
# Endpoint 4 — submission data (deep production/area fields)
# --------------------------------------------------------------------------- #

def fetch_submission_data(app_no: str, submission_id: int) -> dict | None:
    return get(f"/api/v1/application/{app_no}/submission/{submission_id}/data")


# --------------------------------------------------------------------------- #
# Endpoint 5 — evaluation
# --------------------------------------------------------------------------- #

def fetch_evaluation(app_no: str) -> dict | None:
    return get(f"/api/v1/evaluation/{app_no}")


# --------------------------------------------------------------------------- #
# Small helpers for {value, unit} pairs, with Fiskeridir's -1.0 null sentinel
# --------------------------------------------------------------------------- #

def _v(parent: dict, key: str):
    obj = parent.get(key)
    val = obj.get("value") if isinstance(obj, dict) else None
    return val if val not in (None, -1.0, -1) else None

def _u(parent: dict, key: str):
    obj = parent.get(key)
    return obj.get("unit") if isinstance(obj, dict) else None


# --------------------------------------------------------------------------- #
# Flatten all 5 sources into one row matching the BigQuery schema exactly
# --------------------------------------------------------------------------- #

def flatten_row(item: dict, detail: dict, sub: dict, sub_data: dict, evaluation: dict) -> dict:
    d  = detail     or {}
    su = sub        or {}   # submission list object (confirmed schema)
    s  = sub_data   or {}   # submission /data object (confirmed schema)
    e  = evaluation or {}   # evaluation (confirmed schema)

    area_data    = s.get("areaData")              or {}
    center       = area_data.get("centerPoint")    or {}
    fish_data    = s.get("fishProductionData")     or {}
    nonfish      = s.get("nonFishProductionData")  or {}
    species_data = s.get("speciesData")            or {}
    net_data     = s.get("netData")                or {}
    site_data    = s.get("siteData")               or {}
    license_data = s.get("licenseData")             or {}

    species_list  = species_data.get("species") or []
    first_species = species_list[0] if species_list else {}
    all_species_json = json.dumps(species_list, ensure_ascii=False) if species_list else None

    area_polygon  = json.dumps(area_data.get("polygon"), ensure_ascii=False) if area_data.get("polygon") else None
    anchor_points = json.dumps(area_data.get("anchorPoints"), ensure_ascii=False) if area_data.get("anchorPoints") else None
    new_licenses  = json.dumps(license_data.get("newLicenses"), ensure_ascii=False) if license_data.get("newLicenses") else None

    # Lat/lon: prefer submission /data centerPoint, fall back to submission list fields.
    # Fiskeridir uses -1.0 as a "no coordinates yet" sentinel, not null.
    sub_lat = center.get("latitudeDecimalDegree")  or su.get("siteLatitudeDecimalDegree")
    sub_lon = center.get("longitudeDecimalDegree") or su.get("siteLongitudeDecimalDegree")
    latitude  = sub_lat if sub_lat not in (None, -1.0) else None
    longitude = sub_lon if sub_lon not in (None, -1.0) else None

    eval_parts       = e.get("evaluationParts") or []
    responsible_part = next((p for p in eval_parts if p.get("responsiblePart")), {})
    all_decisions    = [dec for p in eval_parts for dec in (p.get("decisions") or [])]
    all_statements   = [st  for p in eval_parts for st  in (p.get("statements") or [])]

    row = {
        # ── 1. List endpoint (confirmed) ───────────────────────────────────
        "application_no":              item.get("applicationNo"),
        "created_at":                  item.get("createdAt"),
        "status":                      item.get("status"),
        "applicant_org_number":        item.get("applicantOrganisationNumber"),
        "applicant_org_name":          item.get("applicantOrganisationName"),
        "type":                        item.get("type"),
        "title":                       item.get("title"),
        # The list endpoint often doesn't carry these — the detail endpoint (d)
        # reliably has them (confirmed via raw_detail_json), so prefer that.
        "withdrawn_at":                d.get("withdrawnAt") or item.get("withdrawnAt"),
        "submitted_at":                d.get("submittedAt") or item.get("submittedAt"),

        # ── 2. Application detail — raw backup, field names not yet confirmed ──
        "raw_detail_json":             json.dumps(d, ensure_ascii=False) if d else None,

        # ── 3. Latest submission list object (confirmed) ──────────────────
        "submission_id":               su.get("submissionId"),
        "submission_submitted_at":     su.get("submittedAt"),
        "submission_status":           su.get("status"),
        "submission_status_set_at":    su.get("statusSetAt"),
        "site_name":                   su.get("siteName") or site_data.get("siteName"),
        "municipality_name":           su.get("municipalityName"),
        "county_municipality_name":    su.get("countyMunicipalityName"),

        # ── 4. Submission /data — confirmed schema ─────────────────────────
        "latitude":                    latitude,
        "longitude":                   longitude,
        "area_polygon_json":           area_polygon,
        "anchor_points_json":          anchor_points,
        "is_area_changed":             area_data.get("isAreaChanged"),
        "site_no":                     site_data.get("siteNr"),
        "production_area_name":        site_data.get("prodAreaName"),
        "species_scientific":          first_species.get("scientificName"),
        "species_popular":             first_species.get("popularName"),
        "all_species_json":            all_species_json,
        "desired_biomass_value":       _v(fish_data, "desiredBiomassSize"),
        "desired_biomass_unit":        _u(fish_data, "desiredBiomassSize"),
        "planned_production_value":    _v(fish_data, "plannedProductionSizeForEachProductionCycle"),
        "planned_production_unit":     _u(fish_data, "plannedProductionSizeForEachProductionCycle"),
        "planned_feed_value":          _v(fish_data, "maximumFeedoutSizeForEachMonth"),
        "planned_feed_unit":           _u(fish_data, "maximumFeedoutSizeForEachMonth"),
        "production_cycle_duration":   _v(fish_data, "productionCycleDuration"),
        "production_cycle_unit":       _u(fish_data, "productionCycleDuration"),
        "max_feedout_monthly_value":   _v(fish_data, "maximumFeedoutSizeForEachMonth"),
        "max_feedout_monthly_unit":    _u(fish_data, "maximumFeedoutSizeForEachMonth"),
        "nonfish_production_value":    _v(nonfish, "productionSize"),
        "nonfish_production_unit":     _u(nonfish, "productionSize"),
        "net_type":                    net_data.get("netType"),
        "net_depth_value":             _v(net_data, "netDepth"),
        "net_depth_unit":              _u(net_data, "netDepth"),
        "net_treatment":               net_data.get("treatment"),
        "new_licenses_json":           new_licenses,
        "num_tilsagn":                 license_data.get("numTilsagn"),
        "raw_submission_json":         json.dumps(s, ensure_ascii=False) if s else None,

        # ── 5. Evaluation — confirmed schema ───────────────────────────────
        "eval_result":                 e.get("result"),
        "eval_finished_at":            e.get("evaluationFinishedAt"),
        "eval_responsible_org":        responsible_part.get("organisationName"),
        "eval_responsible_org_no":     responsible_part.get("organisationNumber"),
        "eval_decisions_json":         json.dumps(all_decisions, ensure_ascii=False)  if all_decisions  else None,
        "eval_statements_json":        json.dumps(all_statements, ensure_ascii=False) if all_statements else None,
        "raw_evaluation_json":         json.dumps(e, ensure_ascii=False) if e else None,

        "fetched_at":                  datetime.now(timezone.utc).isoformat(),
    }

    # Guard against stray non-string values landing in a STRING column
    # (e.g. eval_result occasionally coming back as something other than a plain string)
    string_cols = [
        "application_no", "status", "applicant_org_number", "applicant_org_name",
        "type", "title", "raw_detail_json", "submission_status", "site_name",
        "municipality_name", "county_municipality_name", "area_polygon_json",
        "anchor_points_json", "production_area_name", "species_scientific",
        "species_popular", "all_species_json", "desired_biomass_unit",
        "planned_production_unit", "planned_feed_unit", "production_cycle_unit",
        "max_feedout_monthly_unit", "nonfish_production_unit", "net_type",
        "net_depth_unit", "net_treatment", "new_licenses_json", "raw_submission_json",
        "eval_result", "eval_responsible_org", "eval_responsible_org_no",
        "eval_decisions_json", "eval_statements_json", "raw_evaluation_json",
    ]
    for col in string_cols:
        if row.get(col) is not None and not isinstance(row[col], str):
            row[col] = str(row[col])

    return {k: (v if v != "" else None) for k, v in row.items()}


# --------------------------------------------------------------------------- #
# BigQuery upsert — load to temp table, then MERGE with explicit casts
# --------------------------------------------------------------------------- #

def upsert_to_bigquery(client, rows: list[dict]):
    if not rows:
        print("No rows to upsert.")
        return

    df = pd.DataFrame(rows)

    string_cols = ["application_no", "status", "applicant_org_number", "applicant_org_name",
                   "type", "title", "raw_detail_json", "submission_status", "site_name",
                   "municipality_name", "county_municipality_name", "area_polygon_json",
                   "anchor_points_json", "production_area_name", "species_scientific",
                   "species_popular", "all_species_json", "desired_biomass_unit",
                   "planned_production_unit", "planned_feed_unit", "production_cycle_unit",
                   "max_feedout_monthly_unit", "nonfish_production_unit", "net_type",
                   "net_depth_unit", "net_treatment", "new_licenses_json", "raw_submission_json",
                   "eval_result", "eval_responsible_org", "eval_responsible_org_no",
                   "eval_decisions_json", "eval_statements_json", "raw_evaluation_json"]
    ts_cols     = ["created_at", "withdrawn_at", "submitted_at", "submission_submitted_at",
                   "submission_status_set_at", "eval_finished_at", "fetched_at"]
    float_cols  = ["latitude", "longitude", "desired_biomass_value", "planned_production_value",
                   "planned_feed_value", "production_cycle_duration", "max_feedout_monthly_value",
                   "nonfish_production_value", "net_depth_value"]
    int_cols    = ["submission_id", "site_no", "num_tilsagn"]
    bool_cols   = ["is_area_changed"]

    all_cols = list(df.columns)

    def bq_type(col):
        if col in ts_cols:    return "TIMESTAMP"
        if col in float_cols: return "FLOAT64"
        if col in int_cols:   return "INT64"
        if col in bool_cols:  return "BOOL"
        return "STRING"

    # Explicit schema — do NOT rely on autodetect. When a batch happens to have
    # an entirely-null column (e.g. no withdrawals this run), autodetect can't
    # infer a type and silently defaults to something like INT64, which then
    # breaks the CAST(... AS TIMESTAMP) below with "Invalid cast from INT64 to
    # TIMESTAMP". An explicit schema makes every column's type independent of
    # what happens to be present in a given batch.
    schema = [bigquery.SchemaField(col, bq_type(col)) for col in all_cols]

    temp = f"{TABLE_ID}_temp"
    print(f"Loading {len(df):,} rows to temp table...")
    client.load_table_from_dataframe(
        df, temp,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=schema,
        )
    ).result()

    def cast(col):
        if col in string_cols: return f"CAST(S.{col} AS STRING) AS {col}"
        if col in ts_cols:     return f"CAST(S.{col} AS TIMESTAMP) AS {col}"
        if col in float_cols:  return f"CAST(S.{col} AS FLOAT64) AS {col}"
        if col in int_cols:    return f"CAST(S.{col} AS INT64) AS {col}"
        if col in bool_cols:   return f"CAST(S.{col} AS BOOL) AS {col}"
        return f"S.{col}"

    cast_select = ",\n                ".join(cast(c) for c in all_cols)
    update_cols = [c for c in all_cols if c not in ("application_no", "created_at")]
    update_set  = ",\n            ".join(f"T.{c} = src.{c}" for c in update_cols)
    insert_cols = ", ".join(all_cols)
    insert_vals = ", ".join(f"src.{c}" for c in all_cols)

    client.query(f"""
        MERGE `{TABLE_ID}` T
        USING (
            SELECT
                {cast_select}
            FROM `{temp}` S
        ) src
        ON T.application_no = src.application_no
        WHEN MATCHED THEN UPDATE SET
            {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
        VALUES ({insert_vals})
    """).result()

    print(f"Upserted {len(df):,} rows to {TABLE_ID}.")
    client.delete_table(temp, not_found_ok=True)


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #

def main(backfill: bool = False):
    client = get_bq_client()

    if backfill:
        created_after = START_DATE
        print(f"BACKFILL mode — fetching all applications since {START_DATE}")
    else:
        created_after = get_last_created_at(client)
        print(f"INCREMENTAL mode — created after: {created_after or 'none (full fetch)'}")

    print("\nFetching application list...")
    app_list = fetch_list(created_after=created_after)
    print(f"Found {len(app_list):,} applications.\n")

    if not app_list:
        print("Nothing to do.")
        return

    rows = []
    for i, item in enumerate(app_list, 1):
        app_no = item.get("applicationNo")
        if i % 50 == 0 or i <= 3:
            print(f"  [{i}/{len(app_list)}] {app_no}")

        detail   = fetch_detail(app_no)
        sub      = fetch_latest_submission(app_no)
        sub_data = fetch_submission_data(app_no, sub["submissionId"]) if sub and sub.get("submissionId") else None
        evalu    = fetch_evaluation(app_no)

        rows.append(flatten_row(item, detail, sub, sub_data, evalu))
        time.sleep(SLEEP_APP)

    print(f"\nFlattened {len(rows):,} rows.")
    upsert_to_bigquery(client, rows)
    print("\nAll done.")


if __name__ == "__main__":
    main(backfill="--backfill" in sys.argv)
