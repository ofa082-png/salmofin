"""
build_licence_dim.py
--------------------
Monthly script - rebuilds salmofin.licence_dim, the lookup that makes
licence_capacity_history joinable to production area and owner.

The history table stores what the register said at a point in time. It does NOT
carry a production area, because the snapshot endpoint does not return one.
This script supplies it, plus ownership, from the current register.

WHY PRODUCTION AREA IS DERIVED FROM SITES, NOT FROM licenses.prodAreaCode
    licenses.prodAreaCode is only populated for KOMMERSIELL licences. Every
    other purpose - Slaktemerd, Forskning, Visning, Undervisning - comes back
    blank even though most of those are sea sites. Treating blank as
    "land-based" drops ~87,000 t of real sea capacity, which is what made
    regional utilisation read 112% in Mid. Going through connectedSiteNrs ->
    localities.prodAreaCode covers every purpose.
    Verified: for commercial licences the two agree on all 1,128 rows.

SEA VS LAND
    is_sea_cage = the licence has at least one connected site that has a
    production area. Land-based sites carry no prodAreaCode. In 2026 this
    separates ~1,020,000 t of sea capacity from ~460,000 t of land-based, and
    the register's 1.47 M t headline is the sum of the two.

    This is why `region` is materialised here rather than left to downstream
    queries. In BigQuery, IF(NULL <= 5, 'West', IF(NULL <= 7, 'Mid', 'North'))
    returns 'North', because a NULL condition takes the ELSE branch. Written
    that way, every land-based licence silently lands in North and doubles it
    (469,729 t becomes 930,060 t). Join to licence_dim.region and the case
    cannot arise; if you must write the CASE by hand, test prod_area_code IS
    NULL first.

OWNERSHIP - READ THIS BEFORE TRUSTING owner_group
    The register records the LICENCE HOLDER, not the ultimate owner. A change
    of control that happens by share purchase is invisible here unless the
    holding entity is also renamed. Mowi's majority stake in Nova Sea, for
    example, does not appear at all - those licences still read NOVA SEA
    HAVBRUK AS. owner_group therefore tracks the legal holder's parent as far
    as the register reveals it, and understates real concentration.
    For true ownership you need Broennoeysund's shareholder register joined on
    openLegalEntityNr. That is a separate source and is not wired up.

RUN
    python build_licence_dim.py
"""

import json
import os

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "salmofin"
DATASET_ID = "salmofin"
DIM_TABLE  = f"{PROJECT_ID}.{DATASET_ID}.licence_dim"
GROUPS_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                          "licence_owner_groups.csv")

SQL = f"""
WITH lic AS (
  SELECT
    REPLACE(legacyLicenseNr, ' ', '')       AS licenseNo,
    legacyLicenseNr                          AS licenseNoRaw,
    licenseNr,
    openLegalEntityNr,
    legalEntityName,
    capacityCurrent,
    capacityUnit,
    intentionValue,
    productionStageValue,
    prodAreaCode                             AS licence_prod_area,
    grantedTime,
    SPLIT(connectedSiteNrs, ',')             AS sites
  FROM `{PROJECT_ID}.{DATASET_ID}.licenses`
  WHERE legacyLicenseNr IS NOT NULL
),
site_area AS (
  SELECT
    l.licenseNo,
    MIN(SAFE_CAST(loc.prodAreaCode AS INT64)) AS prod_area_code,
    COUNT(DISTINCT loc.prodAreaCode)          AS n_prod_areas,
    COUNT(DISTINCT loc.siteNr)                AS n_sites_with_area
  FROM lic l, UNNEST(l.sites) s
  JOIN `{PROJECT_ID}.{DATASET_ID}.localities` loc
    ON loc.siteNr = SAFE_CAST(s AS INT64)
  WHERE loc.prodAreaCode IS NOT NULL AND loc.prodAreaCode != ''
  GROUP BY 1
)
SELECT
  l.licenseNo,
  ANY_VALUE(l.licenseNoRaw)         AS licenseNoRaw,
  ANY_VALUE(l.licenseNr)            AS licenseNr,
  ANY_VALUE(l.openLegalEntityNr)    AS openLegalEntityNr,
  ANY_VALUE(l.legalEntityName)      AS legalEntityName,
  ANY_VALUE(l.intentionValue)       AS intentionValue,
  ANY_VALUE(l.productionStageValue) AS productionStageValue,
  ANY_VALUE(l.capacityCurrent)      AS capacityCurrent,
  ANY_VALUE(l.capacityUnit)         AS capacityUnit,
  ANY_VALUE(l.grantedTime)          AS grantedTime,
  ANY_VALUE(sa.prod_area_code)      AS prod_area_code,
  ANY_VALUE(sa.n_prod_areas)        AS n_prod_areas,
  ANY_VALUE(sa.n_sites_with_area)   AS n_sites_with_area,
  ANY_VALUE(sa.prod_area_code) IS NOT NULL AS is_sea_cage
FROM lic l
LEFT JOIN site_area sa USING (licenseNo)
GROUP BY l.licenseNo
"""

# PO -> region. The three-way split used across the reporting: West is the
# whole south and west coast up to Stadt-Hustadvika, Mid is Troendelag, North
# is everything from Helgeland up.
def region_of(po):
    if pd.isna(po):
        return None
    po = int(po)
    if po <= 5:
        return "West"
    if po <= 7:
        return "Mid"
    return "North"


PROD_AREA_NAMES = {
    1: "Svenskegrensen til Jaeren", 2: "Ryfylke", 3: "Karmoey til Sotra",
    4: "Nordhordland til Stadt", 5: "Stadt til Hustadvika",
    6: "Nordmoere og Soer-Troendelag", 7: "Nord-Troendelag med Bindal",
    8: "Helgeland til Bodoe", 9: "Vestfjorden og Vesteraalen",
    10: "Andoeya til Senja", 11: "Kvaloey til Loppa", 12: "Vest-Finnmark",
    13: "Oest-Finnmark",
}


def get_bq_client():
    credentials_info = json.loads(os.environ["GOOGLE_CREDENTIALS"])
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    return bigquery.Client(credentials=credentials, project=PROJECT_ID)


def load_owner_groups() -> pd.DataFrame:
    """
    licence_owner_groups.csv maps org number -> parent group, and is
    hand-maintained. Org number is the key because company names churn
    constantly (MARINE HARVEST NORWAY AS -> MOWI NORWAY AS -> MOWI SEAWATER
    NORWAY AS is one entity across three names).
    """
    if not os.path.exists(GROUPS_CSV):
        print(f"  WARNING - {GROUPS_CSV} not found, owner_group will be NULL")
        return pd.DataFrame(columns=["openLegalEntityNr", "owner_group"])
    g = pd.read_csv(GROUPS_CSV, dtype={"openLegalEntityNr": str})
    g["openLegalEntityNr"] = g["openLegalEntityNr"].str.strip()
    print(f"  owner groups: {len(g)} entities -> {g['owner_group'].nunique()} groups")
    return g[["openLegalEntityNr", "owner_group"]]


def build(client) -> pd.DataFrame:
    print("Querying licences and their sites...")
    df = client.query(SQL).result().to_dataframe()
    print(f"  {len(df):,} licences")

    df["region"]         = df["prod_area_code"].map(region_of)
    df["prod_area_name"] = df["prod_area_code"].map(
        lambda p: PROD_AREA_NAMES.get(int(p)) if pd.notna(p) else None)
    df["openLegalEntityNr"] = df["openLegalEntityNr"].astype("string").str.strip()

    groups = load_owner_groups()
    df = df.merge(groups, on="openLegalEntityNr", how="left")
    df["owner_group"] = df["owner_group"].fillna(df["legalEntityName"])

    sea = df[df["is_sea_cage"]]
    print(f"  sea-cage licences {len(sea):,}  "
          f"({sea['capacityCurrent'].sum():,.0f} {sea['capacityUnit'].mode().iat[0]})")
    print(f"  land / no production area {len(df) - len(sea):,}")
    multi = int((df["n_prod_areas"] > 1).sum())
    if multi:
        print(f"  NOTE - {multi} licences have sites in more than one production "
              f"area; prod_area_code takes the lowest")
    unmapped = df[df["is_sea_cage"] & df["owner_group"].eq(df["legalEntityName"])]
    if len(unmapped):
        top = (unmapped.groupby("legalEntityName")["capacityCurrent"].sum()
               .sort_values(ascending=False).head(5))
        print(f"  {len(unmapped)} sea licences have no owner-group mapping; largest holders:")
        for name, cap in top.items():
            print(f"      {cap:>9,.0f}  {name}")
    return df


def reload_table(client, df: pd.DataFrame) -> None:
    if df.empty:
        raise SystemExit("0 rows - refusing to write")
    print(f"Replacing {DIM_TABLE}...")
    client.load_table_from_dataframe(
        df, DIM_TABLE,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    ).result()
    print(f"  wrote {len(df):,} rows")


if __name__ == "__main__":
    client = get_bq_client()
    reload_table(client, build(client))
    print("All done.")
