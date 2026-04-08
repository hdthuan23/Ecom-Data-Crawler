# ============================================================
# BÁO CÁO PHÂN TÍCH DỮ LIỆU - LAB 01 (PHẦN 2)
# Mục tiêu 2, 5, 3, 7, 4, 8
# ============================================================

import matplotlib
matplotlib.use('Agg')
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

plt.rcParams.update({
    'font.size': 11, 'axes.titlesize': 14, 'axes.titleweight': 'bold',
    'axes.labelsize': 12, 'figure.dpi': 150, 'savefig.dpi': 150,
    'savefig.bbox': 'tight', 'figure.facecolor': 'white',
})
sns.set_style("whitegrid")

MERGED_BRAND = 'Local/OEM Generic'
BRAND_COLORS = {'Global_Brand': '#2196F3', MERGED_BRAND: '#FF9800'}
BRAND_ORDER = ['Global_Brand', MERGED_BRAND]
CHART_DIR = Path(__file__).parent / 'charts'
DATA_PATH = Path(__file__).parent.parent / 'data' / 'tiki_electronics_2026_processed.csv'
df = pd.read_csv(DATA_PATH)
df['revenue'] = df['price'] * df['quantity_sold']

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
#  MỤC TIÊU 2 (TV2) - PRICE PREMIUM
# ============================================================
print("=" * 70)
print("MỤC TIÊU 2: Phân tích mức chênh lệch giá (Price Premium)")
print("           giữa các nhóm thương hiệu")
print("=" * 70)
print("""
SMART:
  S: So sánh giá bán trung bình (price) của Global Brand vs Local/OEM Generic
  M: Mean, median price; hệ số premium = giá Global / giá nhóm khác
  A: Đầy đủ thông tin 12,069 sản phẩm
  R: Xác định mức "phí thương hiệu" người tiêu dùng chi trả
  T: Snapshot giá 03/2026

Trường dữ liệu: price, brand_type, category_name
Biểu đồ: Box Plot + Grouped Bar Chart (giá TB theo danh mục)
""")

# Thống kê giá
price_stats = df.groupby('brand_type')['price'].agg(['mean', 'median', 'std', 'min', 'max'])
price_stats = price_stats.reindex(BRAND_ORDER)
print("Thống kê giá theo nhóm thương hiệu:")
print("-" * 75)
print(f"{'Nhóm':<18} {'TB giá':>12} {'Trung vị':>12} {'Std':>12} {'Min':>10} {'Max':>12}")
print("-" * 75)
for idx, row in price_stats.iterrows():
    print(f"  {idx:<16} {row['mean']:>12,.0f} {row['median']:>12,.0f} {row['std']:>12,.0f} {row['min']:>10,.0f} {row['max']:>12,.0f}")

gb_median = price_stats.loc['Global_Brand', 'median']
lg_median = price_stats.loc[MERGED_BRAND, 'median']
premium = gb_median / lg_median if lg_median > 0 else 0
print(f"\n  Hệ số Price Premium (median): Global / {MERGED_BRAND} = {premium:.2f}x")

# --- Biểu đồ 4.1: Boxplot giá theo brand_type ---
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Boxplot tổng thể (giới hạn ≤ 5 triệu)
df_plot = df[df['price'] <= 5_000_000]
bp = axes[0].boxplot([df_plot[df_plot['brand_type'] == bt]['price'] / 1e6 for bt in BRAND_ORDER],
                      labels=BRAND_ORDER, patch_artist=True, showfliers=False)
for patch, color in zip(bp['boxes'], [BRAND_COLORS[b] for b in BRAND_ORDER]):
    patch.set_facecolor(color)
    patch.set_alpha(0.7)
axes[0].set_ylabel('Giá (triệu VNĐ)')
axes[0].set_title('Phân bố giá tổng thể (≤ 5M)')

# Giá trung vị theo danh mục × brand_type
median_by_cat = df.groupby(['category_name', 'brand_type'])['price'].median().unstack(fill_value=0)
median_by_cat = median_by_cat.reindex(columns=BRAND_ORDER, fill_value=0)
top_cats = df['category_name'].value_counts().head(8).index
median_by_cat_top = median_by_cat.loc[median_by_cat.index.isin(top_cats)]
median_by_cat_top = median_by_cat_top.sort_values('Global_Brand', ascending=True)

x = np.arange(len(median_by_cat_top))
w = 0.25
for i, bt in enumerate(BRAND_ORDER):
    axes[1].barh(x + i*w, median_by_cat_top[bt].values / 1e6, w,
                  label=bt, color=BRAND_COLORS[bt], alpha=0.8)
axes[1].set_yticks(x + w)
axes[1].set_yticklabels([n[:30] for n in median_by_cat_top.index], fontsize=8)
axes[1].set_xlabel('Giá trung vị (triệu VNĐ)')
axes[1].set_title('Giá trung vị theo danh mục (top 8)')
axes[1].legend(fontsize=8)

fig.suptitle('Hình 4: MT2 – Phân tích Price Premium', fontsize=14, fontweight='bold')
plt.tight_layout()
save_fig('08_mt2_price_premium')

# --- Biểu đồ 4.2: Heatmap giá trung vị ---
fig, ax = plt.subplots(figsize=(10, 8))
heatmap_data = df.groupby(['category_name', 'brand_type'])['price'].median().unstack(fill_value=0) / 1e6
heatmap_data = heatmap_data.reindex(columns=BRAND_ORDER, fill_value=0)
sns.heatmap(heatmap_data, annot=True, fmt='.2f', cmap='YlOrRd', ax=ax,
            linewidths=0.5, cbar_kws={'label': 'Giá trung vị (triệu VNĐ)'})
ax.set_title('Hình 4.2: Heatmap giá trung vị theo Danh mục × Nhóm thương hiệu')
ax.set_ylabel('Danh mục')
ax.set_xlabel('Nhóm thương hiệu')
plt.tight_layout()
save_fig('09_mt2_price_heatmap')

print(f"""
KẾT LUẬN MT2:
  • Giá trung vị của Global Brand ({gb_median:,.0f} VNĐ) gấp {premium:.1f} lần {MERGED_BRAND} ({lg_median:,.0f} VNĐ).
  • Hệ số premium cao nhất ở nhóm: Tai Nghe, Thiết Bị Số, Thiết Bị Lưu Trữ.
  • Ở danh mục "Linh Kiện Máy Tính", khoảng cách giá nhỏ hơn → cạnh tranh giá gay gắt hơn.
  • Xu hướng: Người tiêu dùng sẵn sàng trả "phí thương hiệu" cao cho phụ kiện hàng ngày
    (tai nghe, sạc dự phòng) nhưng cân nhắc hơn với linh kiện kỹ thuật.
""")

# ============================================================
#  MỤC TIÊU 5 (TV2) - CHIẾN LƯỢC GIẢM GIÁ
# ============================================================
print("\n" + "=" * 70)
print("MỤC TIÊU 5: Chiến lược giảm giá quyết liệt (Aggressive Discounting)")
print("=" * 70)
print("""
SMART:
  S: So sánh tỷ lệ SP có giảm giá (discount_flag) và discount_rate TB giữa các nhóm
  M: % SP có discount; số SP extreme_discount; TB discount_rate
  A: Biến discount_flag, discount_rate sẵn có
  R: Hiểu brand lớn giảm giá để giữ thị phần hay brand nhỏ giảm giá để tồn tại
  T: 03/2026

Trường dữ liệu: discount_flag, discount_rate, brand_type, category_name
Biểu đồ: Stacked Bar Chart + Donut Chart
""")

# Thống kê
disc_cross = pd.crosstab(df['brand_type'], df['discount_flag'], normalize='index') * 100
disc_cross = disc_cross.reindex(BRAND_ORDER)
print("Tỷ lệ % sản phẩm theo loại giảm giá:")
print(disc_cross.round(1).to_string())

has_disc = df[df['discount_rate'] > 0]
disc_rate_by_brand = has_disc.groupby('brand_type')['discount_rate'].agg(['mean', 'median', 'count'])
disc_rate_by_brand = disc_rate_by_brand.reindex(BRAND_ORDER)
print("\nThống kê discount_rate cho SP có giảm giá:")
print(disc_rate_by_brand.round(1).to_string())

# --- Biểu đồ 5.1: Stacked bar chart ---
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

disc_flags = ['no_discount', 'normal_discount', 'extreme_discount']
disc_colors = ['#E0E0E0', '#FFA726', '#E53935']
bottom = np.zeros(len(BRAND_ORDER))
for flag, color in zip(disc_flags, disc_colors):
    if flag in disc_cross.columns:
        vals = disc_cross[flag].values
        axes[0].bar(BRAND_ORDER, vals, bottom=bottom, label=flag, color=color)
        for i, v in enumerate(vals):
            if v > 3:
                axes[0].text(i, bottom[i] + v/2, f'{v:.1f}%', ha='center', va='center', fontsize=8)
        bottom += vals
axes[0].set_ylabel('Tỷ lệ %')
axes[0].set_title('Phân bố loại giảm giá theo nhóm')
axes[0].legend(fontsize=8)

# Biểu đồ 5.2: Box plot discount_rate (chỉ SP có giảm giá)
bp = axes[1].boxplot([has_disc[has_disc['brand_type'] == bt]['discount_rate'] for bt in BRAND_ORDER],
                      labels=BRAND_ORDER, patch_artist=True, showfliers=True)
for patch, color in zip(bp['boxes'], [BRAND_COLORS[b] for b in BRAND_ORDER]):
    patch.set_facecolor(color)
    patch.set_alpha(0.7)
axes[1].set_ylabel('Discount Rate (%)')
axes[1].set_title('Mức giảm giá (chỉ SP có giảm)')

fig.suptitle('Hình 5: MT5 – Chiến lược giảm giá', fontsize=14, fontweight='bold')
plt.tight_layout()
save_fig('10_mt5_discount_strategy')

gb_disc_pct = 100 - disc_cross.loc['Global_Brand', 'no_discount'] if 'no_discount' in disc_cross.columns else 0
lg_disc_pct = 100 - disc_cross.loc[MERGED_BRAND, 'no_discount'] if 'no_discount' in disc_cross.columns else 0
print(f"""
KẾT LUẬN MT5:
  • Global Brand có tỷ lệ giảm giá ({gb_disc_pct:.1f}%) cao hơn {MERGED_BRAND} ({lg_disc_pct:.1f}%).
  • Điều này cho thấy các brand lớn chủ động sử dụng chiến lược giảm giá để thu hút khách hàng.
  • Extreme discount (≥50%) xuất hiện ở cả 2 nhóm nhưng số lượng rất ít (< 1%).
  • Mức giảm giá trung bình của Global Brand thường cao hơn → chiến lược giảm giá mạnh tay hơn.
  • Nhận định: Global Brand dùng discount như công cụ marketing; {MERGED_BRAND} ít giảm giá vì biên lợi nhuận thấp.
""")

# ============================================================
#  MỤC TIÊU 3 (TV3) - RATING NGHI VẤN
# ============================================================
print("\n" + "=" * 70)
print("MỤC TIÊU 3: Tỷ lệ rating nghi vấn (Rating Suspect)")
print("=" * 70)
print("""
SMART:
  S: Đo tỷ lệ SP bị gắn nhãn is_rating_suspect theo brand_type và category
  M: % is_rating_suspect = True; so sánh mean rating giữa nhóm suspect vs non-suspect
  A: Biến binary sẵn có cho toàn bộ SP
  R: Đánh giá nguy cơ "thổi phồng" đánh giá, ảnh hưởng niềm tin người mua
  T: 03/2026

Trường dữ liệu: is_rating_suspect, brand_type, category_name, rating_average
Biểu đồ: Stacked Bar Chart + Heatmap
""")

suspect_cross = pd.crosstab(df['brand_type'], df['is_rating_suspect'], normalize='index') * 100
suspect_cross = suspect_cross.reindex(BRAND_ORDER)
print("Tỷ lệ rating nghi vấn theo nhóm:")
print(suspect_cross.round(1).to_string())

# --- Biểu đồ 6.1: Stacked bar ---
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

suspect_colors = ['#81C784', '#EF5350']
for i, (col, color, label) in enumerate(zip([False, True], suspect_colors, ['Bình thường', 'Nghi vấn'])):
    if col in suspect_cross.columns:
        vals = suspect_cross[col].values
        if i == 0:
            axes[0].bar(BRAND_ORDER, vals, label=label, color=color)
            bottom_v = vals
        else:
            axes[0].bar(BRAND_ORDER, vals, bottom=bottom_v, label=label, color=color)
            for j, v in enumerate(vals):
                axes[0].text(j, bottom_v[j] + v/2, f'{v:.1f}%', ha='center', va='center', fontsize=9)
axes[0].set_ylabel('Tỷ lệ %')
axes[0].set_title('Tỷ lệ rating nghi vấn theo nhóm')
axes[0].legend()

# Heatmap suspect theo danh mục
suspect_cat = pd.crosstab(df['category_name'], df['is_rating_suspect'], normalize='index') * 100
suspect_cat.columns = suspect_cat.columns.map(str)  # Convert bool columns to string
suspect_heatmap = suspect_cat[['True']].rename(columns={'True': '% Nghi vấn'})
sns.heatmap(suspect_heatmap.sort_values('% Nghi vấn', ascending=False),
            annot=True, fmt='.1f', cmap='Reds', ax=axes[1],
            linewidths=0.5, cbar_kws={'label': '% Rating nghi vấn'})
axes[1].set_title('Tỷ lệ nghi vấn theo danh mục')
axes[1].set_ylabel('')

fig.suptitle('Hình 6: MT3 – Tỷ lệ Rating nghi vấn', fontsize=14, fontweight='bold')
plt.tight_layout()
save_fig('11_mt3_rating_suspect')

total_suspect = df['is_rating_suspect'].sum()
total_suspect_pct = total_suspect / len(df) * 100
print(f"""
KẾT LUẬN MT3:
  • Tổng cộng {total_suspect:,} SP ({total_suspect_pct:.1f}%) bị gắn nhãn rating nghi vấn.
  • Tỷ lệ nghi vấn phân bổ tương đối đồng đều giữa các nhóm brand_type
    → Hiện tượng "thổi phồng" rating không đặc thù cho nhóm nào.
  • Nguyên nhân phổ biến: SP có rating > 4.5 nhưng < 10 lượt review → thống kê chưa đủ tin cậy.
  • Đây là vấn đề chung của sàn TMĐT, không phải đặc trưng riêng của brand lớn hay nhỏ.
""")

# ============================================================
#  MỤC TIÊU 7 (TV3) - TƯƠNG QUAN RATING-REVIEW-DOANH SỐ
# ============================================================
print("\n" + "=" * 70)
print("MỤC TIÊU 7: Tương quan Rating - Review - Doanh số")
print("=" * 70)
print("""
SMART:
  S: Đo mức độ tương quan (Pearson) giữa rating, review, price với quantity_sold
  M: Correlation Matrix; Scatter plots với trend line
  A: Các chỉ số numeric sẵn có
  R: Trả lời "SP tốt hay SP nhiều người mua trước sẽ hút khách hơn?"
  T: 03/2026

Trường dữ liệu: rating_average, review_count, quantity_sold, price, brand_type
Biểu đồ: Correlation Matrix Heatmap + Scatter Plot
""")

# Chỉ lấy SP có doanh số và rating
df_active = df[(df['quantity_sold'] > 0) & (df['rating_average'] > 0)].copy()
corr_cols = ['price', 'rating_average', 'review_count', 'quantity_sold', 'discount_rate']
corr_matrix = df_active[corr_cols].corr()

print(f"Số SP có cả doanh số và rating: {len(df_active):,}")
print("\nMa trận tương quan Pearson:")
print(corr_matrix.round(3).to_string())

# --- Biểu đồ 7.1: Correlation heatmap ---
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
sns.heatmap(corr_matrix, mask=mask, annot=True, fmt='.2f', cmap='RdYlBu_r',
            center=0, ax=axes[0], linewidths=0.5,
            xticklabels=[c.replace('_', '\n') for c in corr_cols],
            yticklabels=[c.replace('_', '\n') for c in corr_cols])
axes[0].set_title('Ma trận tương quan Pearson')

# Scatter: review_count vs quantity_sold
scatter = axes[1].scatter(df_active['review_count'], df_active['quantity_sold'],
                           c=[BRAND_COLORS[bt] for bt in df_active['brand_type']],
                           alpha=0.4, s=15, edgecolors='none')
axes[1].set_xlabel('Số lượt đánh giá (review_count)')
axes[1].set_ylabel('Số lượng đã bán (quantity_sold)')
axes[1].set_title('Scatter: Review vs Doanh số')
from matplotlib.patches import Patch
legend_el = [Patch(facecolor=c, label=l) for l, c in BRAND_COLORS.items()]
axes[1].legend(handles=legend_el, fontsize=8)
# Log scale for better visibility
axes[1].set_xscale('symlog')
axes[1].set_yscale('symlog')

fig.suptitle('Hình 7: MT7 – Tương quan Rating-Review-Doanh số', fontsize=14, fontweight='bold')
plt.tight_layout()
save_fig('12_mt7_correlation')

# --- Biểu đồ 7.2: Scatter rating vs quantity ---
fig, ax = plt.subplots(figsize=(10, 6))
for bt in BRAND_ORDER:
    sub = df_active[df_active['brand_type'] == bt]
    ax.scatter(sub['rating_average'], sub['quantity_sold'],
               alpha=0.4, s=15, color=BRAND_COLORS[bt], label=bt, edgecolors='none')
ax.set_xlabel('Điểm đánh giá trung bình (rating_average)')
ax.set_ylabel('Số lượng đã bán (quantity_sold)')
ax.set_yscale('symlog')
ax.set_title('Hình 7.2: Scatter – Rating vs Doanh số theo nhóm thương hiệu')
ax.legend()
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
save_fig('13_mt7_rating_vs_sales')

rc = corr_matrix.loc['review_count', 'quantity_sold']
rr = corr_matrix.loc['rating_average', 'quantity_sold']
print(f"""
KẾT LUẬN MT7:
  • review_count có tương quan MẠNH với quantity_sold (r = {rc:.3f})
    → SP được nhiều người đánh giá thì bán được nhiều hơn (hoặc ngược lại).
  • rating_average có tương quan YẾU với quantity_sold (r = {rr:.3f})
    → Điểm rating cao không đảm bảo doanh số cao.
  • Kết luận: "Social proof" (số lượng review) có ảnh hưởng lớn hơn "chất lượng" (điểm rating).
  • price có tương quan ÂM nhẹ với quantity_sold → Sản phẩm rẻ hơn bán được nhiều hơn.
""")

# ============================================================
#  MỤC TIÊU 4 (TV4) - TIKI TRADING VS 3RD PARTY
# ============================================================
print("\n" + "=" * 70)
print("MỤC TIÊU 4: Hiệu quả kênh Tiki Trading vs Nhà bán thứ ba")
print("=" * 70)
print("""
SMART:
  S: So sánh quantity_sold TB, rating, review giữa Tiki Trading và 3rd party
  M: TB/trung vị các chỉ số; % SP tham gia Tiki Trading theo brand_type
  A: is_tiki_trading binary (748 SP Tiki vs 11,321 SP 3rd party)
  R: Xác định ưu thế gian hàng Tiki Trading đối với từng phân khúc
  T: 03/2026

Trường dữ liệu: is_tiki_trading, quantity_sold, rating_average, review_count, brand_type
Biểu đồ: Grouped Bar Chart + Violin Plot
""")

tiki_stats = df.groupby('is_tiki_trading').agg({
    'quantity_sold': ['mean', 'median', 'sum'],
    'rating_average': 'mean',
    'review_count': ['mean', 'median'],
    'price': 'mean'
})
print("So sánh Tiki Trading (1) vs 3rd Party (0):")
print(tiki_stats.round(1).to_string())

tiki_by_brand = pd.crosstab(df['brand_type'], df['is_tiki_trading'], normalize='index') * 100
print("\n% SP tham gia Tiki Trading theo brand_type:")
print(tiki_by_brand.round(1).to_string())

# --- Biểu đồ 8.1 ---
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

# Bar chart so sánh
metrics = ['quantity_sold', 'review_count', 'rating_average']
labels = ['Doanh số TB', 'Review TB', 'Rating TB']
tiki_vals = [df[df['is_tiki_trading']==1][m].mean() for m in metrics]
third_vals = [df[df['is_tiki_trading']==0][m].mean() for m in metrics]

x = np.arange(len(labels))
w = 0.35
bars1 = axes[0].bar(x - w/2, tiki_vals, w, label='Tiki Trading', color='#E53935')
bars2 = axes[0].bar(x + w/2, third_vals, w, label='3rd Party', color='#78909C')
axes[0].set_xticks(x)
axes[0].set_xticklabels(labels)
axes[0].set_title('So sánh chỉ số TB: Tiki Trading vs 3rd Party')
axes[0].legend()
for bar in bars1:
    axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                 f'{bar.get_height():.1f}', ha='center', va='bottom', fontsize=8)
for bar in bars2:
    axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                 f'{bar.get_height():.1f}', ha='center', va='bottom', fontsize=8)

# Violin plot doanh số
df_has_sales = df[df['quantity_sold'] > 0].copy()
df_has_sales['channel'] = df_has_sales['is_tiki_trading'].map({1: 'Tiki Trading', 0: '3rd Party'})
df_has_sales['log_sold'] = np.log1p(df_has_sales['quantity_sold'])
sns.violinplot(data=df_has_sales, x='channel', y='log_sold', hue='channel',
               palette={'Tiki Trading': '#E53935', '3rd Party': '#78909C'},
               ax=axes[1], legend=False)
axes[1].set_ylabel('log(1 + quantity_sold)')
axes[1].set_xlabel('')
axes[1].set_title('Phân bố doanh số (log scale)')

fig.suptitle('Hình 8: MT4 – Tiki Trading vs Nhà bán thứ ba', fontsize=14, fontweight='bold')
plt.tight_layout()
save_fig('14_mt4_tiki_trading')

tiki_mean = df[df['is_tiki_trading']==1]['quantity_sold'].mean()
third_mean = df[df['is_tiki_trading']==0]['quantity_sold'].mean()
print(f"""
KẾT LUẬN MT4:
  • SP bán qua Tiki Trading có doanh số TB ({tiki_mean:.1f}) cao hơn đáng kể so với 3rd Party ({third_mean:.1f}).
  • Rating và review count cũng cao hơn ở kênh Tiki Trading.
  • Tỷ lệ tham gia Tiki Trading: Global Brand > {MERGED_BRAND}
    → Brand lớn ưu tiên kênh chính hãng để tăng niềm tin.
  • Tiki Trading đóng vai trò như "seal of trust", giúp tăng chuyển đổi mua hàng.
""")

# ============================================================
#  MỤC TIÊU 8 (TV4) - TỶ LỆ CHUYỂN ĐỔI
# ============================================================
print("\n" + "=" * 70)
print("MỤC TIÊU 8: Tỷ lệ chuyển đổi Listing to Sale")
print("=" * 70)
print("""
SMART:
  S: So sánh tỷ lệ SP đã có doanh số (has_sales) giữa các nhóm
  M: % has_sales cho mỗi brand_type; so sánh theo category
  A: Cột purchase_status sẵn có
  R: Brand lớn có dễ bán hơn khi vừa niêm yết không?
  T: 03/2026

Trường dữ liệu: purchase_status, brand_type, category_name
Biểu đồ: Stacked Bar Chart + Grouped Bar Chart theo danh mục
""")

conv_cross = pd.crosstab(df['brand_type'], df['purchase_status'], normalize='index') * 100
conv_cross = conv_cross.reindex(BRAND_ORDER)
print("Tỷ lệ chuyển đổi (has_sales) theo nhóm:")
print(conv_cross.round(1).to_string())

# --- Biểu đồ 9.1: Stacked bar ---
fig, axes = plt.subplots(1, 2, figsize=(14, 6))

status_colors = {'has_sales': '#43A047', 'new_listing': '#BDBDBD'}
bottom_v = np.zeros(len(BRAND_ORDER))
for status, color in status_colors.items():
    if status in conv_cross.columns:
        vals = conv_cross[status].values
        axes[0].bar(BRAND_ORDER, vals, bottom=bottom_v, label=status, color=color)
        for i, v in enumerate(vals):
            if v > 5:
                axes[0].text(i, bottom_v[i] + v/2, f'{v:.1f}%', ha='center', va='center', fontsize=9, fontweight='bold')
        bottom_v += vals
axes[0].set_ylabel('Tỷ lệ %')
axes[0].set_title('Tỷ lệ has_sales vs new_listing')
axes[0].legend()

# Grouped bar theo category
conv_cat = pd.crosstab([df['category_name'], df['brand_type']], df['purchase_status'], normalize='index') * 100
if 'has_sales' in conv_cat.columns:
    conv_cat_pivot = conv_cat['has_sales'].unstack(level='brand_type').fillna(0)
    conv_cat_pivot = conv_cat_pivot.reindex(columns=BRAND_ORDER, fill_value=0)
    top_cats = df['category_name'].value_counts().head(8).index
    conv_cat_top = conv_cat_pivot.loc[conv_cat_pivot.index.isin(top_cats)]

    x = np.arange(len(conv_cat_top))
    w = 0.25
    for i, bt in enumerate(BRAND_ORDER):
        if bt in conv_cat_top.columns:
            axes[1].barh(x + i*w, conv_cat_top[bt].values, w,
                          label=bt, color=BRAND_COLORS[bt], alpha=0.8)
    axes[1].set_yticks(x + w)
    axes[1].set_yticklabels([n[:25] for n in conv_cat_top.index], fontsize=8)
    axes[1].set_xlabel('% has_sales')
    axes[1].set_title('Tỷ lệ chuyển đổi theo danh mục (top 8)')
    axes[1].legend(fontsize=8)

fig.suptitle('Hình 9: MT8 – Tỷ lệ chuyển đổi Listing to Sale', fontsize=14, fontweight='bold')
plt.tight_layout()
save_fig('15_mt8_conversion_rate')

gb_conv = conv_cross.loc['Global_Brand', 'has_sales'] if 'has_sales' in conv_cross.columns else 0
lg_conv = conv_cross.loc[MERGED_BRAND, 'has_sales'] if 'has_sales' in conv_cross.columns else 0
print(f"""
KẾT LUẬN MT8:
  • Global Brand có tỷ lệ has_sales = {gb_conv:.1f}%, cao hơn {MERGED_BRAND} ({lg_conv:.1f}%).
  • Brand lớn có lợi thế "brand recognition" – khách hàng tìm đến sản phẩm chủ động.
  • Tỷ lệ new_listing cao ở {MERGED_BRAND} cho thấy nhiều SP mới được đăng nhưng chưa bán được.
  • Kết hợp với MT4: SP bán qua Tiki Trading + là Global Brand → tỷ lệ chuyển đổi cao nhất.
""")

print("\n" + "=" * 70)
print("✅ HOÀN THÀNH PHÂN TÍCH 8 MỤC TIÊU")
print("=" * 70)
print(f"\nTổng số biểu đồ đã tạo: 15")
print(f"Biểu đồ lưu tại: {CHART_DIR.resolve()}")
