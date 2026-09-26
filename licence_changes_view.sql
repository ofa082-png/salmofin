-- licence_changes_view.sql
-- -------------------------------------------------------------------------
-- Creates salmofin.salmofin.licence_changes: what moved between consecutive
-- snapshots of licence_capacity_history.
--
-- The history table stores full state at each snapshot, so a change is a
-- difference between a row and the same licence's previous row. This view
-- does that diff once so nothing downstream has to rewrite the window logic.
--
-- change_type is one of:
--   APPEARED          first snapshot this licence is seen in
--   DISAPPEARED       present in the previous snapshot, absent in this one
--                     (expired, surrendered, or merged into another licence)
--   HOLDER_CHANGED    licensee string differs
--   CAPACITY_CHANGED  capacity differs
--   PURPOSE_CHANGED   purpose differs (e.g. Utvikling -> Kommersiell)
--
-- CAVEAT ON HOLDER_CHANGED. The licensee is a free-text name, so a rename
-- registers as a change even when nothing was sold - MARINE HARVEST NORWAY AS
-- to MOWI NORWAY AS to MOWI SEAWATER NORWAY AS is one company across three
-- names. Real transfers with dates live in license_transfers; use this column
-- to find candidates, not to count acquisitions.
--
-- CAVEAT ON SPACING. Snapshots were quarterly (weeks 8/21/34/47) up to the
-- monthly switch, so prev_date is 13 weeks earlier for older rows and ~4 weeks
-- for newer ones. Always read prev_date rather than assuming a fixed step.
--
-- A licence appearing in one snapshot and gone in the next is only visible as
-- DISAPPEARED if a later snapshot exists, so the newest snapshot never
-- produces DISAPPEARED rows.

CREATE OR REPLACE VIEW `salmofin.salmofin.licence_changes` AS
-- Each snapshot paired with the one that follows it. Computed with LEAD rather
-- than a correlated subquery, which BigQuery rejects in a join predicate
-- ("Unsupported subquery with table in join predicate").
WITH snaps AS (
  SELECT snapshot_date, snapshot_year, snapshot_week,
         LEAD(snapshot_date)  OVER (ORDER BY snapshot_date) AS next_date,
         LEAD(snapshot_year)  OVER (ORDER BY snapshot_date) AS next_year,
         LEAD(snapshot_week)  OVER (ORDER BY snapshot_date) AS next_week
  FROM (SELECT DISTINCT snapshot_year, snapshot_week, snapshot_date
        FROM `salmofin.salmofin.licence_capacity_history`)
),
h AS (
  SELECT
    licenseNo, licenseNoRaw, snapshot_year, snapshot_week, snapshot_date,
    licensee, capacity, purpose, unit, productionType, speciesCategory,
    expirationDate,
    LAG(licensee)       OVER w AS prev_licensee,
    LAG(capacity)       OVER w AS prev_capacity,
    LAG(purpose)        OVER w AS prev_purpose,
    LAG(expirationDate) OVER w AS prev_expirationDate,
    LAG(snapshot_date)  OVER w AS prev_row_date
  FROM `salmofin.salmofin.licence_capacity_history`
  WINDOW w AS (PARTITION BY licenseNo ORDER BY snapshot_date)
),
-- Present, then gone: attribute the disappearance to the snapshot that follows
-- the last one it was seen in.
gone AS (
  SELECT
    last_seen.licenseNo, last_seen.licenseNoRaw,
    nxt.next_date AS snapshot_date,
    nxt.next_year AS snapshot_year,
    nxt.next_week AS snapshot_week,
    last_seen.snapshot_date AS prev_row_date,
    last_seen.licensee AS prev_licensee, last_seen.capacity AS prev_capacity,
    last_seen.purpose AS prev_purpose, last_seen.unit, last_seen.productionType,
    last_seen.speciesCategory, last_seen.expirationDate AS prev_expirationDate
  FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY licenseNo ORDER BY snapshot_date DESC) rn
    FROM `salmofin.salmofin.licence_capacity_history`
  ) last_seen
  JOIN snaps nxt ON nxt.snapshot_date = last_seen.snapshot_date
  -- next_date IS NULL means the licence was still present in the newest
  -- snapshot, so it has not disappeared - it is simply current.
  WHERE last_seen.rn = 1 AND nxt.next_date IS NOT NULL
)
SELECT
  h.snapshot_year, h.snapshot_week, h.snapshot_date,
  h.prev_row_date AS prev_date,
  h.licenseNo, h.licenseNoRaw,
  CASE
    WHEN h.prev_row_date IS NULL                              THEN 'APPEARED'
    WHEN h.licensee  IS DISTINCT FROM h.prev_licensee          THEN 'HOLDER_CHANGED'
    WHEN h.capacity  IS DISTINCT FROM h.prev_capacity          THEN 'CAPACITY_CHANGED'
    WHEN h.purpose   IS DISTINCT FROM h.prev_purpose           THEN 'PURPOSE_CHANGED'
    WHEN h.expirationDate IS DISTINCT FROM h.prev_expirationDate THEN 'EXPIRY_CHANGED'
    ELSE 'UNCHANGED'
  END AS change_type,
  h.licensee, h.prev_licensee,
  h.capacity, h.prev_capacity, h.capacity - h.prev_capacity AS capacity_delta,
  h.purpose, h.prev_purpose,
  h.expirationDate, h.prev_expirationDate,
  h.unit, h.productionType, h.speciesCategory
FROM h

UNION ALL

SELECT
  g.snapshot_year, g.snapshot_week, g.snapshot_date,
  g.prev_row_date AS prev_date,
  g.licenseNo, g.licenseNoRaw,
  'DISAPPEARED' AS change_type,
  CAST(NULL AS STRING) AS licensee, g.prev_licensee,
  CAST(NULL AS FLOAT64) AS capacity, g.prev_capacity,
  -g.prev_capacity AS capacity_delta,
  CAST(NULL AS STRING) AS purpose, g.prev_purpose,
  -- DATETIME, not TIMESTAMP: the history table's date columns are written by
  -- pandas without a timezone, which BigQuery types as DATETIME.
  CAST(NULL AS DATETIME) AS expirationDate, g.prev_expirationDate,
  g.unit, g.productionType, g.speciesCategory
FROM gone g;
