"""Run preprocessing without notebook and export processed CSV for analysis."""

import json
import os
from datetime import datetime

import numpy as np
import pandas as pd


with open("config.json", encoding="utf-8") as f:
    cfg = json.load(f)

project_root = os.path.abspath(".")
input_csv = os.path.join(project_root, cfg["output"]["csv_export"])
output_csv = input_csv.replace(".csv", "_processed.csv")
report_json = os.path.join(project_root, "data", "preprocessing_report.json")

suspect_rating_threshold = 4.5
suspect_review_min = 1
suspect_review_max = 10
extreme_discount_threshold = 50
dead_column_threshold = 0.95

print("=" * 60)
print("PREPROCESSING PIPELINE")
print("=" * 60)

df = pd.read_csv(input_csv, encoding="utf-8-sig")
original_row_count = len(df)
print(f"[S1] Loaded: {len(df):,} rows, {len(df.columns)} cols")

# Remove dead numeric columns if any
numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
dead_columns = []
for col in numeric_cols:
    zero_pct = (df[col] == 0).mean()
    if zero_pct > dead_column_threshold:
        dead_columns.append(col)
if dead_columns:
    df.drop(columns=dead_columns, inplace=True)
    print(f"[S2] Dropped dead columns: {dead_columns}")
else:
    print("[S2] No dead columns")

# purchase_status
df["purchase_status"] = np.where(
    (df["quantity_sold"] > 0) | (df["review_count"] > 0),
    "has_sales",
    "new_listing",
)

# rating suspect
df["is_rating_suspect"] = (
    (df["rating_average"] > suspect_rating_threshold)
    & (df["review_count"] >= suspect_review_min)
    & (df["review_count"] < suspect_review_max)
)


def assign_discount_flag(row):
    dr = row["discount_rate"]
    price = row["price"]
    original_price = row["original_price"]
    if dr == 0:
        return "no_discount"
    if price >= original_price and dr > 0:
        return "fake_discount"
    if dr >= extreme_discount_threshold and price < original_price:
        return "extreme_discount"
    return "normal_discount"


df["discount_flag"] = df.apply(assign_discount_flag, axis=1)
df["is_fake_discount"] = df["discount_flag"].eq("fake_discount")

# Assertions
assert len(df) == original_row_count, "Row count changed unexpectedly"
assert set(df["purchase_status"].unique()).issubset({"has_sales", "new_listing"})
assert set(df["discount_flag"].unique()).issubset(
    {"no_discount", "fake_discount", "extreme_discount", "normal_discount"}
)
assert not ((df["is_fake_discount"]) & (df["discount_rate"] == 0)).any()
assert df["is_rating_suspect"].dtype == bool

# Export processed CSV
df.to_csv(output_csv, index=False, encoding="utf-8-sig")
print(f"[S3] Exported processed CSV -> {output_csv}")

# Export compact report JSON
summary = {
    "timestamp": datetime.now().isoformat(),
    "input_csv": input_csv,
    "output_csv": output_csv,
    "row_count": int(len(df)),
    "column_count": int(len(df.columns)),
    "purchase_status": df["purchase_status"].value_counts().to_dict(),
    "discount_flag": df["discount_flag"].value_counts().to_dict(),
    "is_rating_suspect_count": int(df["is_rating_suspect"].sum()),
    "dead_columns_removed": dead_columns,
}
with open(report_json, "w", encoding="utf-8") as f:
    json.dump(summary, f, ensure_ascii=False, indent=2)
print(f"[S4] Exported preprocessing report -> {report_json}")

print("[DONE] Preprocessing completed successfully.")



