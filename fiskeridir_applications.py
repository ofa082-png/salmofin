import requests
import pandas as pd
import os
from datetime import datetime, timezone

# ── Settings ──────────────────────────────────────────────────────────────────
BASE_URL = "https://api.fiskeridir.no/aqua-portal-api-public"
START_DATE = "2020-01-01T00:00:00.000Z"  # pull all applications since 2020

# ── No auth needed — public API ───────────────────────────────────────────────

headers = {"Accept": "application/json"}

# ── Helper: fetch all pages from a paginated endpoint ────────────────────────

def fetch_all_pages(url, params={}):
    all_items = []
    page = 0
    while True:
        params["page"] = page
        params["size"] = 100
        resp = requests.get(url, headers=headers, params=params)
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

# ── Step 1: Fetch all applications since 2020 ─────────────────────────────────

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

# ── Step 3: Fetch evaluations (decisions) for each application ────────────────

print("\nFetching evaluations...")
eval_rows = []

for app_no in df_apps["ApplicationNo"].dropna().unique():
    resp = requests.get(
        f"{BASE_URL}/v1/evaluation/{app_no}",
        headers=headers
    )
    if resp.status_code == 200:
        ev = resp.json()
        # Get overall result
        result       = ev.get("result")
        finished_at  = ev.get("evaluationFinishedAt")
        # Get per-organisation decisions
        for part in ev.get("evaluationParts", []):
            org_name = part.get("organisationName")
            for decision in part.get("decisions", []):
                eval_rows.append({
                    "ApplicationNo":     app_no,
                    "OverallResult":     result,
                    "EvaluationFinishedAt": finished_at,
                    "OrgName":           org_name,
                    "DecisionResult":    decision.get("result"),
                    "DecisionTime":      decision.get("decisionTime"),
                    "RegisteredAt":      decision.get("registeredAt"),
                })
    elif resp.status_code == 404:
        # No evaluation yet — still pending
        eval_rows.append({
            "ApplicationNo":     app_no,
            "OverallResult":     "PENDING",
            "EvaluationFinishedAt": None,
            "OrgName":           None,
            "DecisionResult":    None,
            "DecisionTime":      None,
            "RegisteredAt":      None,
        })

df_evals = pd.DataFrame(eval_rows)
print(f"Evaluations fetched: {len(df_evals):,}")

# ── Step 4: Join applications with evaluations ────────────────────────────────

df = df_apps.merge(
    df_evals[[
        "ApplicationNo", "OverallResult", 
        "EvaluationFinishedAt", "DecisionResult", "DecisionTime"
    ]].drop_duplicates(subset=["ApplicationNo"]),
    on="ApplicationNo",
    how="left"
)

# Fill pending where no evaluation exists
df["OverallResult"] = df["OverallResult"].fillna("PENDING")

# ── Step 5: Add processing time (days from submitted to decision) ─────────────

df["SubmittedAt"]          = pd.to_datetime(df["SubmittedAt"], utc=True, errors="coerce")
df["EvaluationFinishedAt"] = pd.to_datetime(df["EvaluationFinishedAt"], utc=True, errors="coerce")
df["ProcessingDays"]       = (
    df["EvaluationFinishedAt"] - df["SubmittedAt"]
).dt.days

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
