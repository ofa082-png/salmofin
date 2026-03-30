import requests
import pandas as pd
import os
from datetime import datetime, timezone

# ── Settings ──────────────────────────────────────────────────────────────────

BASE_URL   = "https://api.fiskeridir.no/aqua-portal-api-public"
START_DATE = "2025-01-01T00:00:00.000Z"

# ── No auth needed — public API ───────────────────────────────────────────────

headers = {"Accept": "application/json"}

# ── Helper: fetch all pages from a paginated endpoint ────────────────────────

def fetch_all_pages(url, params={}):
    all_items = []
    page = 0
    while True:
        p = {**params, "page": page, "size": 100}
        resp = requests.get(url, headers=headers, params=p)
        if resp.status_code != 200:
            print(f"  Warning: {url} returned {resp.status_code}")
            break
        data = resp.json()
        content = data.get("content", [])
        all_items.extend(content)
        total_pages = data.get("totalPages", 1)
        print(f"  Page {page+1}/{total_pages} — {len(content)} items")
        if page >= total_pages - 1:
            break
        page += 1
    return all_items

# ── Step 1: Fetch all applications ───────────────────────────────────────────

print("Fetching applications...")
applications = fetch_all_pages(
    f"{BASE_URL}/api/v1/applications",
    params={"createdAfter": START_DATE, "sort": "created,asc"}
)
print(f"Total applications: {len(applications):,}")

# ── Step 2: Build applications table ─────────────────────────────────────────

app_rows = []
for app in applications:
    app_rows.append({
        "ApplicationNo":          app.get("applicationNo"),
        "AquaApplicationNo":      app.get("aquaApplicationNo"),
        "CreatedAt":              app.get("createdAt"),
        "Status":                 app.get("status"),
        "Type":                   app.get("type"),
        "Title":                  app.get("title"),
        "ApplicantOrgNumber":     app.get("applicantOrganisationNumber"),
        "ApplicantOrgName":       app.get("applicantOrganisationName"),
        "WithdrawnAt":            app.get("withdrawnAt"),
        "SubmittedAt":            app.get("submittedAt"),
    })

df_apps = pd.DataFrame(app_rows)

# ── Step 3: Fetch evaluations (decisions) ────────────────────────────────────

print("\nFetching evaluations...")
eval_rows = []

for app_no in df_apps["ApplicationNo"].dropna().unique():
    resp = requests.get(
        f"{BASE_URL}/api/v1/evaluation/{app_no}",
        headers=headers
    )
    if resp.status_code == 200:
        ev = resp.json()
        result      = ev.get("result")
        finished_at = ev.get("evaluationFinishedAt")
        for part in ev.get("evaluationParts", []):
            org_name = part.get("organisationName")
            is_responsible = part.get("responsiblePart", False)
            # Get statements
            statements = part.get("statements", [])
            statement_count = len(statements)
            latest_statement = statements[-1].get("statementTime") if statements else None
            for decision in part.get("decisions", []):
                eval_rows.append({
                    "ApplicationNo":        app_no,
                    "OverallResult":        result,
                    "EvaluationFinishedAt": finished_at,
                    "EvalOrgName":          org_name,
                    "IsResponsiblePart":    is_responsible,
                    "DecisionResult":       decision.get("result"),
                    "DecisionTime":         decision.get("decisionTime"),
                    "StatementCount":       statement_count,
                    "LatestStatementTime":  latest_statement,
                })
    else:
        eval_rows.append({
            "ApplicationNo":        app_no,
            "OverallResult":        "PENDING",
            "EvaluationFinishedAt": None,
            "EvalOrgName":          None,
            "IsResponsiblePart":    None,
            "DecisionResult":       None,
            "DecisionTime":         None,
            "StatementCount":       0,
            "LatestStatementTime":  None,
        })

df_evals = pd.DataFrame(eval_rows)
print(f"Evaluations fetched: {len(df_evals):,}")

# ── Step 4: Fetch submission data (biomass, cycle details) ────────────────────

print("\nFetching submission data...")
sub_rows = []

for app_no in df_apps["ApplicationNo"].dropna().unique():
    # First get submission list
    resp = requests.get(
        f"{BASE_URL}/api/v1/application/{app_no}/submissions",
        headers=headers,
        params={"page": 0, "size": 1, "sort": "submitted,desc"}
    )
    if resp.status_code != 200:
        continue
    submissions = resp.json().get("content", [])
    if not submissions:
        continue

    # Get the latest submission ID
    sub_id = submissions[0] if isinstance(submissions[0], int) else submissions[0].get("id")
    if not sub_id:
        continue

    # Get submission data
    resp2 = requests.get(
        f"{BASE_URL}/api/v1/application/{app_no}/submission/{sub_id}/data",
        headers=headers
    )
    if resp2.status_code != 200:
        continue

    data = resp2.json()

    # Extract fish production data
    fpd = data.get("fishProductionData", {})
    ld  = data.get("licenseData", {})

    sub_rows.append({
        "ApplicationNo":              app_no,
        "NumLicences":                ld.get("numTilsagn"),
        "DesiredBiomass_value":       fpd.get("desiredBiomassSize", {}).get("value"),
        "DesiredBiomass_unit":        fpd.get("desiredBiomassSize", {}).get("unit"),
        "PlannedProductionSize_value": fpd.get("plannedProductionSizeForEachProductionCycle", {}).get("value"),
        "PlannedProductionSize_unit":  fpd.get("plannedProductionSizeForEachProductionCycle", {}).get("unit"),
        "ProductionCycleDuration_value": fpd.get("productionCycleDuration", {}).get("value"),
        "ProductionCycleDuration_unit":  fpd.get("productionCycleDuration", {}).get("unit"),
        "MaxFeedPerMonth_value":      fpd.get("maximumFeedoutSizeForEachMonth", {}).get("value"),
        "MaxFeedPerMonth_unit":       fpd.get("maximumFeedoutSizeForEachMonth", {}).get("unit"),
    })

df_subs = pd.DataFrame(sub_rows)
print(f"Submission data fetched: {len(df_subs):,}")

# ── Step 5: Join everything together ─────────────────────────────────────────

# Get one row per application from evaluations
df_evals_dedup = df_evals.drop_duplicates(subset=["ApplicationNo"])

df = df_apps.merge(
    df_evals_dedup[[
        "ApplicationNo", "OverallResult", "EvaluationFinishedAt",
        "DecisionResult", "DecisionTime", "StatementCount", "LatestStatementTime"
    ]],
    on="ApplicationNo", how="left"
)

df = df.merge(df_subs, on="ApplicationNo", how="left")

# Fill pending
df["OverallResult"] = df["OverallResult"].fillna("PENDING")

# Processing time
df["SubmittedAt"]          = pd.to_datetime(df["SubmittedAt"], utc=True, errors="coerce")
df["EvaluationFinishedAt"] = pd.to_datetime(df["EvaluationFinishedAt"], utc=True, errors="coerce")
df["ProcessingDays"]       = (df["EvaluationFinishedAt"] - df["SubmittedAt"]).dt.days

# ── Step 6: Save ──────────────────────────────────────────────────────────────

output_path = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "applications.csv"
)
df.to_csv(output_path, index=False)

print(f"\nSaved {len(df):,} rows to {output_path}")
print(f"\nStatus breakdown:")
print(df["OverallResult"].value_counts())
print(f"\nType breakdown:")
print(df["Type"].value_counts().head(10))
print(f"\nTop applicants:")
print(df["ApplicantOrgName"].value_counts().head(10))
print(f"\nSubmission data coverage: {df['DesiredBiomass_value'].notna().sum()} of {len(df)} applications")
