# ============================================================
# BÁO CÁO PHÂN TÍCH DỮ LIỆU - LAB 01
# Đề tài: Phân hóa thị trường Global Brand vs Local/OEM Generic
#         trên sàn Tiki - Ngành Linh kiện Điện tử (2026)
# Dữ liệu: tiki_electronics_2026_processed.csv
# Công cụ: Python 3, pandas, matplotlib, seaborn, squarify
# ============================================================

import matplotlib
matplotlib.use('Agg')
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from pathlib import Path
import runpy
import warnings
warnings.filterwarnings('ignore')

# --- CẤU HÌNH CHUNG ---
plt.rcParams.update({
    'font.size': 11,
    'axes.titlesize': 14,
    'axes.titleweight': 'bold',
    'axes.labelsize': 12,
    'figure.dpi': 150,
    'savefig.dpi': 150,
    'savefig.bbox': 'tight',
    'figure.facecolor': 'white',
})
sns.set_style("whitegrid")

MERGED_BRAND = 'Local/OEM Generic'
BRAND_COLORS = {
  'Global_Brand': '#2196F3',
  MERGED_BRAND: '#FF9800',
}
BRAND_ORDER = ['Global_Brand', MERGED_BRAND]
CHART_DIR = Path(__file__).parent / 'charts'
CHART_DIR.mkdir(exist_ok=True)

DATA_PATH = Path(__file__).parent.parent / 'data' / 'tiki_electronics_2026_processed.csv'
df = pd.read_csv(DATA_PATH)

def save_fig(name):
    plt.savefig(CHART_DIR / f'{name}.png', bbox_inches='tight')
    plt.show()
    plt.close()

def fmt_num(n):
    if n >= 1e9: return f'{n/1e9:.1f}B'
    if n >= 1e6: return f'{n/1e6:.1f}M'
    if n >= 1e3: return f'{n/1e3:.1f}K'
    return f'{n:.0f}'

# ============================================================
#  PHẦN 1: TỔNG QUAN DỮ LIỆU
# ============================================================
print("=" * 70)
print("PHẦN 1: TỔNG QUAN DỮ LIỆU")
print("=" * 70)

print(f"\n1.1 KÍCH THƯỚC MẪU")
print(f"  - Số sản phẩm (dòng) : {df.shape[0]:,}")
print(f"  - Số trường (cột)    : {df.shape[1]}")
print(f"  - Thời gian crawl    : {df['first_crawled_at'].min()} → {df['last_crawled_at'].max()}")

print(f"\n1.2 CẤU TRÚC DỮ LIỆU")
print("-" * 55)
print(f"{'Cột':<25} {'Kiểu':<15} {'Null':>6}")
print("-" * 55)
for col in df.columns:
    print(f"  {col:<23} {str(df[col].dtype):<15} {df[col].isnull().sum():>6}")

# --- Biểu đồ 1.1: Phân bố sản phẩm theo danh mục ---
fig, ax = plt.subplots(figsize=(12, 6))
cat_counts = df['category_name'].value_counts()
bars = ax.barh(cat_counts.index[::-1], cat_counts.values[::-1], color=sns.color_palette("viridis", len(cat_counts)))
for bar, val in zip(bars, cat_counts.values[::-1]):
    ax.text(bar.get_width() + 30, bar.get_y() + bar.get_height()/2,
            f'{val:,} ({val/len(df)*100:.1f}%)', va='center', fontsize=9)
ax.set_xlabel('Số lượng sản phẩm')
ax.set_title('Hình 1.1: Phân bố sản phẩm theo danh mục')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
save_fig('01_category_distribution')
print("\n→ Nhận xét: Danh mục 'Linh Kiện Máy Tính' chiếm tỷ trọng lớn nhất (2,600 SP = 21.5%),")
print("  tiếp theo là 'Cáp Chuyển Đổi' (2,151 SP = 17.8%). Phân bố không đồng đều.")

# --- Biểu đồ 1.2: Phân bố brand_type ---
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
bt_counts = df['brand_type'].value_counts().reindex(BRAND_ORDER)
colors = [BRAND_COLORS[b] for b in BRAND_ORDER]

axes[0].pie(bt_counts.values, labels=BRAND_ORDER, autopct='%1.1f%%',
            colors=colors, startangle=90, textprops={'fontsize': 11})
axes[0].set_title('Tỷ lệ sản phẩm theo nhóm thương hiệu')

axes[1].bar(BRAND_ORDER, bt_counts.values, color=colors)
for i, v in enumerate(bt_counts.values):
    axes[1].text(i, v + 100, f'{v:,}', ha='center', fontweight='bold')
axes[1].set_ylabel('Số lượng sản phẩm')
axes[1].set_title('Số lượng sản phẩm theo nhóm thương hiệu')
axes[1].spines['top'].set_visible(False)
axes[1].spines['right'].set_visible(False)
fig.suptitle('Hình 1.2: Phân bố sản phẩm theo loại thương hiệu', fontsize=14, fontweight='bold')
plt.tight_layout()
save_fig('02_brand_type_distribution')
merged_listing_pct = bt_counts.loc[MERGED_BRAND] / len(df) * 100
global_listing_pct = bt_counts.loc['Global_Brand'] / len(df) * 100
print(f"\n→ Nhận xét: {MERGED_BRAND} chiếm {merged_listing_pct:.1f}%, Global_Brand chiếm {global_listing_pct:.1f}% listing.")
print("  Cấu trúc brand đã được tinh gọn còn 2 nhóm để phân tích rõ hơn.")

# --- Biểu đồ 1.3: Phân bố giá (histogram) ---
fig, axes = plt.subplots(1, 2, figsize=(14, 5))
df_under5m = df[df['price'] <= 5_000_000]
axes[0].hist(df_under5m['price'] / 1e6, bins=50, color='#5C6BC0', edgecolor='white', alpha=0.8)
axes[0].set_xlabel('Giá (triệu VNĐ)')
axes[0].set_ylabel('Số sản phẩm')
axes[0].set_title('Phân bố giá (≤ 5 triệu)')
axes[0].axvline(df['price'].median() / 1e6, color='red', linestyle='--', label=f'Median = {df["price"].median()/1e6:.2f}M')
axes[0].legend()

for bt in BRAND_ORDER:
    sub = df[df['brand_type'] == bt]['price'] / 1e6
    axes[1].hist(sub[sub <= 5], bins=40, alpha=0.5, label=bt, color=BRAND_COLORS[bt])
axes[1].set_xlabel('Giá (triệu VNĐ)')
axes[1].set_ylabel('Số sản phẩm')
axes[1].set_title('Phân bố giá theo nhóm thương hiệu (≤ 5 triệu)')
axes[1].legend()
fig.suptitle('Hình 1.3: Phân bố giá sản phẩm', fontsize=14, fontweight='bold')
plt.tight_layout()
save_fig('03_price_distribution')
print(f"\n→ Nhận xét: Phân bố giá lệch phải mạnh (right-skewed), median={df['price'].median()/1e6:.2f}M VNĐ.")
print("  Phần lớn sản phẩm có giá dưới 500K. Global Brand có giá trung bình cao hơn rõ rệt.")

# ============================================================
#  PHẦN 2: MỤC TIÊU 1 (TV1) - THỊ PHẦN DOANH SỐ
# ============================================================
print("\n" + "=" * 70)
print("MỤC TIÊU 1: Đo lường khoảng cách thị phần doanh số")
print("           giữa Global Brand và Local/OEM Generic")
print("=" * 70)
print("""
SMART:
  S: So sánh thị phần doanh số (volume) giữa 2 nhóm brand_type
  M: Tổng, trung bình, trung vị quantity_sold; tỷ lệ % đóng góp
  A: Dữ liệu quantity_sold sẵn có cho 100% sản phẩm
  R: Xác định nhóm thương hiệu thống lĩnh về doanh số
  T: Dữ liệu snapshot 03/2026

Trường dữ liệu: brand_type, quantity_sold, category_name
Biểu đồ: Pie Chart (tỷ lệ %) + Grouped Bar Chart (sum/mean/median)
""")

# Tính toán
sales_by_brand = df.groupby('brand_type')['quantity_sold'].agg(['sum', 'mean', 'median', 'count'])
sales_by_brand = sales_by_brand.reindex(BRAND_ORDER)
total_sold = df['quantity_sold'].sum()
sales_by_brand['pct'] = sales_by_brand['sum'] / total_sold * 100

print("Thống kê doanh số theo nhóm thương hiệu:")
print("-" * 65)
print(f"{'Nhóm':<18} {'Tổng':>10} {'TB':>10} {'Trung vị':>10} {'Tỷ lệ %':>10}")
print("-" * 65)
for idx, row in sales_by_brand.iterrows():
    print(f"  {idx:<16} {row['sum']:>10,.0f} {row['mean']:>10.1f} {row['median']:>10.1f} {row['pct']:>9.1f}%")

# --- Biểu đồ 2.1: Pie chart thị phần doanh số ---
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Pie chart
sizes = sales_by_brand['sum'].values
labels_pie = [f"{b}\n{fmt_num(s)} ({s/total_sold*100:.1f}%)" for b, s in zip(BRAND_ORDER, sizes)]
wedges, texts = axes[0].pie(sizes, labels=labels_pie, colors=colors, startangle=90,
                             textprops={'fontsize': 10})
axes[0].set_title('Tỷ lệ doanh số (volume) theo nhóm')

# Grouped bar chart
x = np.arange(len(BRAND_ORDER))
w = 0.25
bars1 = axes[1].bar(x - w, sales_by_brand['sum'].values, w, label='Tổng ÷ 100', color=colors, alpha=0.9)
# Scale for visibility
scale_mean = sales_by_brand['mean'].values
scale_median = sales_by_brand['median'].values
axes_r = axes[1].twinx()

axes_r.bar(x, scale_mean, w, label='Trung bình', color=colors, alpha=0.5, hatch='//')
axes_r.bar(x + w, scale_median, w, label='Trung vị', color=colors, alpha=0.5, hatch='\\\\')
axes[1].set_xticks(x)
axes[1].set_xticklabels(BRAND_ORDER, fontsize=9)
axes[1].set_ylabel('Tổng doanh số')
axes_r.set_ylabel('TB / Trung vị')
axes[1].legend(loc='upper left')
axes_r.legend(loc='upper right')
axes[1].set_title('So sánh Tổng / TB / Trung vị doanh số')

fig.suptitle('Hình 2: MT1 – Thị phần doanh số theo nhóm thương hiệu', fontsize=14, fontweight='bold')
plt.tight_layout()
save_fig('04_mt1_market_share')

print(f"""
KẾT LUẬN MT1:
  • Global Brand chiếm {sales_by_brand.loc['Global_Brand','pct']:.1f}% tổng doanh số dù chỉ có {global_listing_pct:.1f}% listing
    → Hiệu quả bán hàng trung bình cao hơn đáng kể.
  • {MERGED_BRAND} chiếm {sales_by_brand.loc[MERGED_BRAND,'pct']:.1f}% doanh số với {merged_listing_pct:.1f}% listing.
  • Trung vị doanh số Global ({sales_by_brand.loc['Global_Brand','median']:.0f}) > {MERGED_BRAND} ({sales_by_brand.loc[MERGED_BRAND,'median']:.0f})
    → Sự chênh lệch không chỉ ở sản phẩm đầu bảng mà còn ở toàn phân khúc.
""")

# ============================================================
#  PHẦN 3: MỤC TIÊU 10 (TV1) - DOANH THU THỰC TẾ
# ============================================================
print("\n" + "=" * 70)
print("MỤC TIÊU 10: Ước tính doanh thu thực tế (Volume × Price)")
print("=" * 70)
print("""
SMART:
  S: Tính doanh thu = quantity_sold × price cho từng sản phẩm
  M: Tổng revenue, mean revenue; xếp hạng theo nhóm
  A: Tất cả trường cần thiết đã có sẵn
  R: Kết luận về "Miếng bánh thị trường" (Value vs Volume)
  T: Dữ liệu snapshot 03/2026

Trường dữ liệu: price, quantity_sold, brand_type, category_name
Biểu đồ: Treemap + Horizontal Bar Chart
""")

df['revenue'] = df['price'] * df['quantity_sold']
rev_by_brand = df.groupby('brand_type')['revenue'].agg(['sum', 'mean']).reindex(BRAND_ORDER)
total_rev = df['revenue'].sum()
rev_by_brand['pct'] = rev_by_brand['sum'] / total_rev * 100

print("Doanh thu ước tính theo nhóm thương hiệu:")
print("-" * 60)
print(f"{'Nhóm':<18} {'Tổng DT':>18} {'TB/SP':>14} {'Tỷ lệ':>8}")
print("-" * 60)
for idx, row in rev_by_brand.iterrows():
    print(f"  {idx:<16} {fmt_num(row['sum']):>18} {fmt_num(row['mean']):>14} {row['pct']:>7.1f}%")

# --- Biểu đồ 3.1: So sánh % listing vs % doanh số vs % doanh thu ---
fig, ax = plt.subplots(figsize=(10, 6))
bt_listing_pct = df['brand_type'].value_counts(normalize=True).reindex(BRAND_ORDER) * 100
bt_sales_pct = sales_by_brand['pct']
bt_rev_pct = rev_by_brand['pct']

x = np.arange(len(BRAND_ORDER))
w = 0.25
ax.bar(x - w, bt_listing_pct.values, w, label='% Listing', color='#90CAF9')
ax.bar(x, bt_sales_pct.values, w, label='% Doanh số (volume)', color='#2196F3')
ax.bar(x + w, bt_rev_pct.values, w, label='% Doanh thu (value)', color='#0D47A1')

for i in range(len(BRAND_ORDER)):
    ax.text(i - w, bt_listing_pct.values[i] + 1, f'{bt_listing_pct.values[i]:.1f}%', ha='center', fontsize=8)
    ax.text(i, bt_sales_pct.values[i] + 1, f'{bt_sales_pct.values[i]:.1f}%', ha='center', fontsize=8)
    ax.text(i + w, bt_rev_pct.values[i] + 1, f'{bt_rev_pct.values[i]:.1f}%', ha='center', fontsize=8)

ax.set_xticks(x)
ax.set_xticklabels(BRAND_ORDER)
ax.set_ylabel('Tỷ lệ %')
ax.set_title('Hình 3.1: So sánh tỷ trọng Listing – Doanh số – Doanh thu')
ax.legend()
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
save_fig('05_mt10_listing_vs_sales_vs_revenue')

# --- Biểu đồ 3.2: Top 10 sản phẩm doanh thu cao nhất ---
fig, ax = plt.subplots(figsize=(12, 6))
top10 = df.nlargest(10, 'revenue')[['product_name', 'brand_type', 'price', 'quantity_sold', 'revenue']]
top10['short_name'] = top10['product_name'].str[:45] + '...'
bar_colors = [BRAND_COLORS[bt] for bt in top10['brand_type']]
bars = ax.barh(range(len(top10)-1, -1, -1), top10['revenue'].values / 1e9, color=bar_colors)
ax.set_yticks(range(len(top10)-1, -1, -1))
ax.set_yticklabels(top10['short_name'].values, fontsize=8)
for i, (bar, rev) in enumerate(zip(bars, top10['revenue'].values)):
    ax.text(bar.get_width() + 0.05, bar.get_y() + bar.get_height()/2,
            f'{rev/1e9:.2f}B', va='center', fontsize=8)
ax.set_xlabel('Doanh thu ước tính (tỷ VNĐ)')
ax.set_title('Hình 3.2: Top 10 sản phẩm có doanh thu ước tính cao nhất')
from matplotlib.patches import Patch
legend_elements = [Patch(facecolor=c, label=l) for l, c in BRAND_COLORS.items()]
ax.legend(handles=legend_elements, loc='lower right')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
save_fig('06_mt10_top10_revenue')

# --- Biểu đồ 3.3: Doanh thu theo danh mục ---
fig, ax = plt.subplots(figsize=(12, 6))
rev_by_cat = df.groupby(['category_name', 'brand_type'])['revenue'].sum().unstack(fill_value=0)
rev_by_cat = rev_by_cat.reindex(columns=BRAND_ORDER, fill_value=0)
rev_by_cat_total = rev_by_cat.sum(axis=1).sort_values(ascending=True)
rev_by_cat = rev_by_cat.loc[rev_by_cat_total.index]

rev_by_cat.plot(kind='barh', stacked=True, ax=ax, color=[BRAND_COLORS[b] for b in BRAND_ORDER])
ax.set_xlabel('Doanh thu ước tính (VNĐ)')
ax.set_title('Hình 3.3: Doanh thu theo danh mục & nhóm thương hiệu')
ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: fmt_num(x)))
ax.legend(title='Brand Type')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
save_fig('07_mt10_revenue_by_category')

gb_rev_pct = rev_by_brand.loc['Global_Brand', 'pct']
print(f"""
KẾT LUẬN MT10:
  • Global Brand chiếm đến {gb_rev_pct:.1f}% tổng doanh thu ước tính dù chỉ có {global_listing_pct:.1f}% listing.
  • Chênh lệch giữa % doanh thu và % listing của Global Brand rất lớn
    → Thương hiệu lớn bán ít sản phẩm hơn nhưng giá trị mỗi đơn hàng cao hơn rất nhiều.
  • Top 10 sản phẩm doanh thu cao nhất hầu hết thuộc Global Brand.
  • Danh mục "Thiết Bị Số - Phụ Kiện Số" đóng góp doanh thu lớn nhất.
""")

print("\n✅ Hoàn thành Phần 1-3 (Tổng quan + MT1 + MT10)")

if __name__ == "__main__":
  # Keep one-entry analysis flow: running this file will continue with the remaining targets.
  part2_path = Path(__file__).with_name('report_part2.py')
  if part2_path.exists():
    print("\n▶ Chạy tiếp phần phân tích còn lại (MT2,5,3,7,4,8) từ report_part2.py ...")
    runpy.run_path(str(part2_path), run_name="__main__")
