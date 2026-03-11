"""
Verify script - chạy toàn bộ logic notebook bằng Python thuần để test không cần Jupyter.
"""
import json, os, sys, numpy as np, pandas as pd
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────
with open("config.json", encoding="utf-8") as f:
    _cfg = json.load(f)

PROJECT_ROOT               = os.path.abspath(".")
INPUT_CSV                  = os.path.join(PROJECT_ROOT, _cfg["output"]["csv_export"])
OUTPUT_CSV                 = INPUT_CSV.replace(".csv", "_processed.csv")
OUTPUT_DB                  = OUTPUT_CSV.replace(".csv", ".db")
REPORT_JSON                = os.path.join(PROJECT_ROOT, "data", "preprocessing_report.json")
SUSPECT_RATING_THRESHOLD   = 4.5
SUSPECT_REVIEW_MIN         = 1
SUSPECT_REVIEW_MAX         = 10
EXTREME_DISCOUNT_THRESHOLD = 50
DEAD_COLUMN_THRESHOLD      = 0.95

print("=" * 60)
print("VERIFY PREPROCESSING PIPELINE")
print("=" * 60)

# Section 1 – Load
df = pd.read_csv(INPUT_CSV, encoding="utf-8-sig")
ORIGINAL_ROW_COUNT = len(df)
print(f"[S1] Loaded: {len(df):,} rows, {len(df.columns)} cols")
assert "is_official" not in df.columns, "is_official van con trong CSV!"
print(f"[S1] OK: is_official NOT in CSV (da duoc remove dung)")

# Section 2 – Dead columns
numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
dead_columns = []
for col in numeric_cols:
    zero_pct = (df[col] == 0).mean()
    if zero_pct > DEAD_COLUMN_THRESHOLD:
        dead_columns.append(col)
        print(f"[S2] DROP dead column: '{col}' ({zero_pct:.1%} zeros)")
if not dead_columns:
    print(f"[S2] OK: Khong co dead column nao")
else:
    df.drop(columns=dead_columns, inplace=True)

# Section 3 – purchase_status
df["purchase_status"] = df.apply(
    lambda r: "has_sales" if (r["quantity_sold"] > 0 or r["review_count"] > 0) else "new_listing",
    axis=1
)
vc = df["purchase_status"].value_counts()
for s, c in vc.items():
    print(f"[S3] {s}: {c:,} ({c/len(df)*100:.1f}%)")

# Section 4 – Rating anomaly
df["is_rating_suspect"] = (
    (df["rating_average"] > SUSPECT_RATING_THRESHOLD) &
    (df["review_count"] >= SUSPECT_REVIEW_MIN) &
    (df["review_count"] < SUSPECT_REVIEW_MAX)
)
print("[S4] Suspect rate by brand_type (review>=1):")
df_wr = df[df["review_count"] >= SUSPECT_REVIEW_MIN]
suspect_summary = []
for bt in sorted(df["brand_type"].unique()):
    el  = df_wr[df_wr["brand_type"] == bt]
    sus = el[el["is_rating_suspect"]]
    rate = len(sus)/len(el)*100 if len(el) > 0 else 0
    print(f"  {bt:<20s}: {len(sus):>4}/{len(el):>5} = {rate:.1f}%")
    suspect_summary.append({"brand_type": bt, "eligible_count": len(el),
                            "suspect_count": len(sus), "suspect_rate": round(rate, 2)})

# Section 5 – Discount flag
def assign_discount_flag(row):
    dr, price, orig = row["discount_rate"], row["price"], row["original_price"]
    if dr == 0:     return "no_discount"
    if price >= orig and dr > 0: return "fake_discount"
    if dr >= EXTREME_DISCOUNT_THRESHOLD and price < orig: return "extreme_discount"
    return "normal_discount"

df["discount_flag"]    = df.apply(assign_discount_flag, axis=1)
df["is_fake_discount"] = (df["discount_flag"] == "fake_discount")
discount_summary = []
print("[S5] Discount flag distribution:")
for flag, cnt in df["discount_flag"].value_counts().items():
    print(f"  {flag:<20s}: {cnt:>5,} ({cnt/len(df)*100:.1f}%)")
for bt in sorted(df["brand_type"].unique()):
    sub = df[df["brand_type"] == bt]
    fake = (sub["discount_flag"] == "fake_discount").sum()
    extr = (sub["discount_flag"] == "extreme_discount").sum()
    discount_summary.append({"brand_type": bt, "total": len(sub),
        "fake_discount_count": int(fake), "fake_discount_rate": round(fake/len(sub)*100, 2),
        "extreme_discount_count": int(extr), "extreme_discount_rate": round(extr/len(sub)*100, 2)})

# Section 6 – Assertions
print("[S6] Assertions...")
assert len(df) == ORIGINAL_ROW_COUNT, f"Row count changed!"
assert set(df["purchase_status"].unique()).issubset({"has_sales", "new_listing"})
assert set(df["discount_flag"].unique()).issubset({"no_discount","fake_discount","extreme_discount","normal_discount"})
assert not ((df["is_fake_discount"]) & (df["discount_rate"] == 0)).any()
assert df["is_rating_suspect"].dtype == bool
assert "is_official" not in df.columns
new_cols = ["purchase_status", "is_rating_suspect", "discount_flag", "is_fake_discount"]
assert all(c in df.columns for c in new_cols)
print("[S6] ALL ASSERTIONS PASSED!")

# Section 7 – Export
import sqlite3

df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
print(f"[S7] Processed CSV: {OUTPUT_CSV}")
print(f"     {len(df):,} rows | {len(df.columns)} columns")

# Export to SQLite
with sqlite3.connect(OUTPUT_DB) as conn:
    df.to_sql("products", conn, if_exists="replace", index=False)
print(f"[S7] Processed DB: {OUTPUT_DB}")

report = {
    "metadata": {"processed_at": datetime.now().isoformat(),
                 "input_csv": INPUT_CSV, "output_csv": OUTPUT_CSV,
                 "output_db": OUTPUT_DB,
                 "total_rows": len(df), "total_columns": len(df.columns)},
    "dead_columns_dropped": dead_columns,
    "purchase_status": {s: int(c) for s, c in df["purchase_status"].value_counts().items()},
    "purchase_status_pct": {s: round(float(c/len(df)*100), 2) for s, c in df["purchase_status"].value_counts().items()},
    "rating_anomaly": {
        "thresholds": {"rating_above": SUSPECT_RATING_THRESHOLD, "review_min": SUSPECT_REVIEW_MIN, "review_max": SUSPECT_REVIEW_MAX},
        "by_brand_type": {r["brand_type"]: {"eligible_count": r["eligible_count"], "suspect_count": r["suspect_count"], "suspect_rate_pct": r["suspect_rate"]} for r in suspect_summary}
    },
    "discount_analysis": {
        "thresholds": {"extreme_discount_pct": EXTREME_DISCOUNT_THRESHOLD},
        "distribution": {f: int(c) for f, c in df["discount_flag"].value_counts().items()},
        "by_brand_type": {r["brand_type"]: r for r in discount_summary}
    }
}
with open(REPORT_JSON, "w", encoding="utf-8") as f:
    json.dump(report, f, ensure_ascii=False, indent=2)
print(f"[S7] Report JSON: {REPORT_JSON}")
print()
print("PIPELINE VERIFICATION COMPLETE - TAT CA SECTIONS PASSED!")
print(f"Cot moi: {new_cols}")
print(f"Cot bi drop: {dead_columns if dead_columns else 'Khong co'}")
