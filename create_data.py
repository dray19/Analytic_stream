import pandas as pd
import numpy as np

# ----------------------------
# Parameters
# ----------------------------
start_time = "2026-01-02 23:00:00"
end_time = "2026-01-03 09:59:00"

pnodes = [
    "MEC.MECB",
    "CWLP.AZ",
    "ILLINOIS.HUB",
    "PSI_GEN.AGG",
    "MINN.HUB",
]

price_type = "ExPost"
output_path = "data/input.csv"

# ----------------------------
# Create time index (1-minute frequency)
# ----------------------------
timestamps = pd.date_range(start=start_time, end=end_time, freq="5min")

# ----------------------------
# Generate dataset
# ----------------------------
rows = []
rng = np.random.default_rng(seed=42)

for ts in timestamps:
    hour = ts.hour
    day = ts.strftime("%Y-%m-%d")

    for pnode in pnodes:
        # LMP behavior by node (slightly different centers)
        base_lmp = {
            "MEC.MECB": 41,
            "CWLP.AZ": 36,
            "ILLINOIS.HUB": 36,
            "PSI_GEN.AGG": 36,
            "MINN.HUB": 35,
        }[pnode]

        lmp = round(rng.normal(loc=base_lmp, scale=2.5), 2)
        mcc = round(rng.normal(loc=-1.0, scale=2.0), 2)
        mlc = round(rng.normal(loc=-1.5, scale=2.5), 2)

        rows.append([
            ts.strftime("%Y-%m-%d %H:%M:%S"),
            day,
            price_type,
            pnode,
            hour,
            max(lmp, 0.0),
            mcc,
            mlc
        ])

# ----------------------------
# Save CSV
# ----------------------------
df = pd.DataFrame(
    rows,
    columns=[
        "timestamp",
        "day",
        "priceType",
        "pnodeName",
        "hour",
        "lmp",
        "mcc",
        "mlc",
    ]
)

df.to_csv(output_path, index=False)

print(f"Saved {len(df)} rows to {output_path}")