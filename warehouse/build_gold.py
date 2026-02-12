import os
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(BASE_DIR)

SILVER_DIR = os.path.join(PROJECT_ROOT, "lakehouse", "silver")
GOLD_DIR = os.path.join(PROJECT_ROOT, "lakehouse", "gold")
os.makedirs(GOLD_DIR, exist_ok=True)

# Read all silver parquet partitions
dfs = []
for root, _, files in os.walk(SILVER_DIR):
    for fn in files:
        if fn.endswith(".parquet"):
            dfs.append(pd.read_parquet(os.path.join(root, fn)))

if not dfs:
    raise SystemExit("No silver parquet files found. Run streaming first.")

df = pd.concat(dfs, ignore_index=True)

# Parse timestamp + numeric glucose
# Force everything to UTC-safe parsing and drop bad rows
df["event_ts"] = pd.to_datetime(df["event_ts"], errors="coerce", utc=True)

df["glucose_mg_dl"] = pd.to_numeric(df["glucose_mg_dl"], errors="coerce")

# Keep only rows with valid timestamps
df = df.dropna(subset=["event_ts"]).copy()

# Now .dt will work because event_ts is datetime64[ns, UTC]
df["event_date"] = df["event_ts"].dt.date.astype(str)

# Basic Gold metrics: by date + meal_type
gold = (
    df.groupby(["event_date", "meal_type"], dropna=False)
      .agg(
          readings=("glucose_mg_dl", "count"),
          avg_glucose=("glucose_mg_dl", "mean"),
          max_glucose=("glucose_mg_dl", "max"),
          fasting_readings=("is_fasting", "sum"),
      )
      .reset_index()
)

out_path = os.path.join(GOLD_DIR, "gold_meal_daily.parquet")
gold.to_parquet(out_path, index=False)

print("Wrote:", out_path)
print(gold.head(10))

bad_ts = df["event_ts"].isna().sum()
print("Bad timestamps:", bad_ts)

