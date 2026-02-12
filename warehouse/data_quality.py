import os
import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_DIR = os.path.join(PROJECT_ROOT, "lakehouse", "silver")

bad = []

for root, _, files in os.walk(SILVER_DIR):
    for fn in files:
        if fn.endswith(".parquet"):
            df = pd.read_parquet(os.path.join(root, fn))

            # rules
            if df["event_ts"].isna().mean() > 0.05:
                bad.append((fn, "too_many_null_event_ts"))
            if pd.to_numeric(df["glucose_mg_dl"], errors="coerce").isna().mean() > 0.2:
                bad.append((fn, "too_many_null_glucose"))

if bad:
    print("DQ FAILED:")
    for b in bad:
        print(b)
    raise SystemExit(1)

print("DQ PASSED ")
