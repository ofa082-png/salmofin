import copernicusmarine
import pandas as pd
import xarray as xr
import tempfile
import os
from datetime import datetime, timedelta, timezone

# ── Settings ──────────────────────────────────────────────────────────────────

DATASET_ID = "cmems_mod_nws_bgc-chl_anfc_7km-3D_P1D-m"

MIN_LON = 4.0
MAX_LON = 13.0
MIN_LAT = 57.5
MAX_LAT = 65.0
MIN_DEPTH = 0.0
MAX_DEPTH = 1.0
DAYS_BACK = 7

# ── Date range ────────────────────────────────────────────────────────────────

end_date   = datetime.now(timezone.utc)
start_date = end_date - timedelta(days=DAYS_BACK)

start_str = start_date.strftime("%Y-%m-%dT00:00:00")
end_str   = end_date.strftime("%Y-%m-%dT00:00:00")

# ── Fetch data ────────────────────────────────────────────────────────────────

tmp_dir = tempfile.mkdtemp()

copernicusmarine.subset(
    copernicusmarine.subset(
    dataset_id        = DATASET_ID,
    minimum_longitude = MIN_LON,
    maximum_longitude = MAX_LON,
    minimum_latitude  = MIN_LAT,
    maximum_latitude  = MAX_LAT,
    minimum_depth     = MIN_DEPTH,
    maximum_depth     = MAX_DEPTH,
    start_datetime    = start_str,
    end_datetime      = end_str,
    variables         = ["chl"],
    output_directory  = tmp_dir,
    username          = os.environ.get("CMEMS_USERNAME"),
    password          = os.environ.get("CMEMS_PASSWORD"),
)

# ── Load and convert ──────────────────────────────────────────────────────────

nc_files = [f for f in os.listdir(tmp_dir) if f.endswith(".nc")]
ds = xr.open_dataset(os.path.join(tmp_dir, nc_files[0]))

df = ds["chl"].squeeze().to_dataframe().reset_index()
df = df.dropna(subset=["chl"])

df = df.rename(columns={
    "latitude":  "Latitude",
    "longitude": "Longitude",
    "time":      "Date",
    "chl":       "Chlorophyll_mgm3",
})

df["Latitude"]  = df["Latitude"].round(4)
df["Longitude"] = df["Longitude"].round(4)
df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()

def label_region(lat, lon):
    if lat < 59.5 and lon < 9.0:
        return "Southern Norway / Skagerrak"
    elif lat < 62.0:
        return "Western Norway"
    elif lat < 65.0:
        return "Mid Norway"
    else:
        return "Northern Norway"

df["Region"] = df.apply(lambda r: label_region(r["Latitude"], r["Longitude"]), axis=1)

# ── Save to CSV ───────────────────────────────────────────────────────────────

output_path = os.path.join(os.path.dirname(__file__), "chlorophyll.csv")

df[["Date", "Latitude", "Longitude", "Chlorophyll_mgm3", "Region"]].to_csv(
    output_path, index=False
)

print(f"Saved {len(df):,} rows to {output_path}")
print(f"Date range: {df['Date'].min()} → {df['Date'].max()}")
