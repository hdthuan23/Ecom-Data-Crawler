# ĐẶC TẢ QUY TRÌNH TRIỂN KHAI MACHINE LEARNING (v2 — Train/Valid/Test)

## Bối cảnh dữ liệu

| Thông tin | Giá trị |
|-----------|---------|
| Dataset | `tiki_electronics_2026_processed.csv` |
| Số mẫu | 10,978 sản phẩm |
| Số trường | 20 cột |
| Câu hỏi nghiên cứu | Phân hóa thị trường Global Brand vs OEM/Local Generic |
| Brand types | Global_Brand (4,514), Local_Generic (6,307), OEM_Generic (157) |
| Mẫu có doanh số | ~5,482 sản phẩm (has_sales — dùng cho LR & KMeans) |

---

## CHIẾN LƯỢC CHIA DỮ LIỆU TỔNG THỂ

### Nguyên tắc chung

Tất cả 3 mô hình đều tuân thủ **chiến lược 3-tập tách biệt hoàn toàn**:

```
Toàn bộ dữ liệu (N mẫu)
│
├── TRAIN SET  (60%) — Học tham số mô hình
├── VALID SET  (20%) — Chỉnh siêu tham số, phát hiện overfitting sớm
└── TEST SET   (20%) — Đánh giá khách quan cuối cùng, KHÔNG đụng vào cho đến khi mô hình được chốt
```

**Quy tắc bất khả xâm phạm:**
- Test set chỉ được dùng **đúng 1 lần** cuối cùng
- Mọi quyết định về model (threshold, hyperparameter) đều dựa trên **valid set**
- Random state = 42 cố định cho mọi phép chia — đảm bảo tái lặp

### Tỷ lệ chia theo từng mô hình

| Mô hình | N mẫu đầu vào | Train | Valid | Test | Lý do |
|---------|--------------|-------|-------|------|-------|
| RF Classifier | 10,977* | 6,586 | 2,196 | 2,195 | Đủ mẫu, stratified |
| Linear Regression | ~5,482† | 3,289 | 1,097 | 1,096 | Chỉ SP has_sales |
| K-Means | ~5,482† | **N/A** | **N/A** | toàn bộ | Unsupervised — không cần split |

> *Bỏ 1 dòng thiếu brand_name  
> †Lọc `purchase_status == 'has_sales'` trước khi chia

### Chiến lược Stratified

| Mô hình | Stratify theo | Lý do |
|---------|--------------|-------|
| RF | `brand_type` | OEM_Generic chỉ 1.4% — cần giữ tỷ lệ đều |
| LR (Global) | không cần | Mỗi subset là 1 nhóm thuần nhất |
| LR (Local) | không cần | Tương tự |
| LR (All) | `brand_type` | Giữ đại diện 3 nhóm trong mỗi tập |

---

## CHIẾN LƯỢC TRAIN & ĐÁNH GIÁ TỪNG MÔ HÌNH

---

## Mô hình 1: Random Forest Classifier — Phân loại Brand Type

### Mục tiêu phân tích
Dự đoán một sản phẩm thuộc nhóm thương hiệu nào (`Global_Brand` / `Local_Generic` / `OEM_Generic`) dựa trên các đặc trưng thị trường (giá, rating, lượng bán, kênh phân phối...).

> **Câu hỏi trả lời:** *"Những yếu tố nào phân biệt rõ nhất sản phẩm Global Brand với Local Generic?"*  
> **Liên kết:** MT1 (thị phần), MT2 (giá), MT6 (danh mục)

### Lý do chọn Random Forest
- Xử lý tốt dữ liệu mixed (số + category) mà không cần chuẩn hóa
- Cung cấp **Feature Importance** — yếu tố giải thích cốt lõi của đặc tả
- Robust với outlier, ít nhạy cảm với imbalanced class (kết hợp `class_weight`)

---

### Chiến lược chia dữ liệu — ML1

```
N = 10,977 (drop 1 missing brand_name)
│
├── TRAIN  60% = 6,586 mẫu   →  Fit RandomForest
├── VALID  20% = 2,196 mẫu   →  Kiểm tra overfitting, chọn n_estimators
└── TEST   20% = 2,195 mẫu   →  Báo cáo kết quả cuối

Stratify = brand_type (đảm bảo OEM_Generic xuất hiện đủ ở cả 3 tập)

Code tương ứng:
  X_tv, X_test, y_tv, y_test = train_test_split(X, y, test_size=0.20, stratify=y, random_state=42)
  X_train, X_val, y_train, y_val = train_test_split(X_tv, y_tv, test_size=0.25, stratify=y_tv, random_state=42)
  # 0.25 × 0.80 = 0.20 → valid set chiếm đúng 20% tổng
```

### Quy trình train

```
Bước 1: Feature Engineering
├── Features số:    price, original_price, discount_rate, rating_average,
│                   review_count, quantity_sold
├── Features binary: is_tiki_trading, is_rating_suspect, is_fake_discount
│                    purchase_status (map: has_sales→1, new_listing→0)
└── Features encoded: category_name → LabelEncoder (fit trên TRAIN only)
                      discount_flag → OrdinalEncoder [no_discount, normal, extreme]

Bước 2: Fit trên TRAIN
└── RandomForestClassifier(n_estimators=200, class_weight='balanced', random_state=42)

Bước 3: Theo dõi trên VALID
├── So sánh Accuracy(train) vs Accuracy(valid) → phát hiện overfit
├── Nếu gap > 10%: tăng min_samples_leaf hoặc giảm max_depth
└── Chốt tham số khi gap < 5%

Bước 4: Đánh giá cuối trên TEST (1 lần duy nhất)
├── Classification Report: Precision, Recall, F1 mỗi class
├── Accuracy tổng thể
└── Confusion Matrix
```

### Trực quan hóa kết quả

| # | Biểu đồ | Loại | Liên kết phân tích |
|---|---------|------|-------------------|
| 1 | **Feature Importance** (top 10) | Horizontal bar chart | MT2: price quan trọng nhất? |
| 2 | **Confusion Matrix** (valid + test) | Heatmap (2 subplot) | OEM_Generic bị nhầm với ai? |
| 3 | **Train vs Valid Accuracy** curve | Line chart (n_estimators) | Mô hình ổn định từ n=? |
| 4 | **Phân phối top-3 features** theo brand_type | Violin plot | Xác nhận MT2 price gap |

### Kết quả kỳ vọng & phân tích kỳ vọng

| Kỳ vọng | Cơ sở | Hỗ trợ mục tiêu |
|---------|-------|----------------|
| `price` + `category_name` = top 2 features | Global Brand giá cao 2.5x (MT2) | MT2, MT6 |
| F1(OEM_Generic) thấp nhất | Chỉ 157 mẫu, đặc trưng giống Local | MT1 |
| Accuracy(test) > 80% | 3 class phân hóa rõ | MT6 |
| Valid accuracy ≈ Test accuracy ± 2% | Mô hình không overfit | Độ tin cậy |

---

## Mô hình 2: Linear Regression — Dự đoán Doanh số (quantity_sold)

### Mục tiêu phân tích
Xây dựng mô hình hồi quy dự đoán `quantity_sold`, **tách riêng theo brand_type**, để so sánh yếu tố ảnh hưởng đến doanh số của từng nhóm thương hiệu.

> **Câu hỏi trả lời:** *"Yếu tố nào thúc đẩy doanh số cho Global Brand khác gì so với Local Generic?"*  
> **Liên kết:** MT4 (Tiki Trading), MT7 (tương quan), MT10 (doanh thu)

### Lý do chọn Linear Regression
- Hệ số β dễ giải thích: *"review_count tăng 1 đơn vị → quantity_sold tăng β đơn vị"*
- Cho phép so sánh trực tiếp ảnh hưởng của từng feature giữa 2 nhóm thương hiệu
- Liên kết chặt với phân tích tương quan Spearman ở MT7 (r=0.772)

---

### Chiến lược chia dữ liệu — ML2

```
Dữ liệu đầu vào: df[df['purchase_status'] == 'has_sales']  →  ~5,482 mẫu

Tách thành 3 dataset riêng (mỗi cái chia Train/Valid/Test độc lập):

┌─ Dataset A: df_global  (Global_Brand,  ~2,700 mẫu)
│   ├── Train  60% = ~1,620
│   ├── Valid  20% = ~540
│   └── Test   20% = ~540
│
├─ Dataset B: df_local   (Local_Generic, ~2,700 mẫu)
│   ├── Train  60% = ~1,620
│   ├── Valid  20% = ~540
│   └── Test   20% = ~540
│
└─ Dataset C: df_all     (toàn bộ 5,482, thêm brand_type encoded)
    ├── Train  60% = ~3,289  (stratify=brand_type)
    ├── Valid  20% = ~1,097
    └── Test   20% = ~1,096

Code tương ứng:
  X_tv, X_test, y_tv, y_test = train_test_split(X, y, test_size=0.20, random_state=42)
  X_train, X_val, y_train, y_val = train_test_split(X_tv, y_tv, test_size=0.25, random_state=42)
```

### Quy trình train

```
Bước 1: Feature Engineering
├── Target: log(quantity_sold + 1)  —  bắt buộc vì phân phối lệch phải rất mạnh
├── Features số: price (log), discount_rate, rating_average, review_count (log)
├── Features binary: is_tiki_trading, is_rating_suspect
└── Features category: category_name → One-Hot Encoding
    (fit OneHotEncoder trên TRAIN only, transform Valid & Test)

Bước 2: Chuẩn hóa
└── StandardScaler fit trên TRAIN → transform Valid + Test (tránh data leakage)

Bước 3: Fit Model A (Global_Brand) trên TRAIN_A
├── LinearRegression().fit(X_train_A, y_train_A)
└── Ghi nhận: R²(train), R²(valid), coefficients

Bước 4: Fit Model B (Local_Generic) trên TRAIN_B
├── LinearRegression().fit(X_train_B, y_train_B)
└── Ghi nhận: R²(train), R²(valid), coefficients

Bước 5: Fit Model C (All) trên TRAIN_C
└── LinearRegression().fit(X_train_C, y_train_C)  (thêm feature brand_type dummy)

Bước 6: Kiểm tra overfitting qua Valid
├── Nếu R²(train) - R²(valid) > 0.15 → overfit → xem xét giảm features
└── Chốt mô hình khi gap ≤ 0.10

Bước 7: Báo cáo cuối trên TEST
└── R²(test), RMSE(test), MAE(test) cho cả Model A, B, C
```

### Trực quan hóa kết quả

| # | Biểu đồ | Loại | Liên kết phân tích |
|---|---------|------|-------------------|
| 1 | **So sánh Coefficients** Global vs Local (features chính) | Grouped horizontal bar | MT7: review vs price |
| 2 | **Actual vs Predicted** (scatter, 2 subplot A & B) | Scatter + identity line | Độ chính xác mô hình |
| 3 | **R² Train / Valid / Test** 3 model | Grouped bar chart | Overfitting check |
| 4 | **Residual distribution** (histogram, 2 subplot) | Histogram | Kiểm tra giả định LR |

### Kết quả kỳ vọng & phân tích kỳ vọng

| Kỳ vọng | Cơ sở | Hỗ trợ mục tiêu |
|---------|-------|----------------|
| `review_count` có β lớn nhất ở cả 2 nhóm | Spearman r=0.772 (MT7) | MT7 |
| `is_tiki_trading` β > 0, lớn với Global Brand | Tiki Trading bán 34x (MT4) | MT4 |
| `discount_rate` β khác chiều giữa 2 nhóm | Local Generic dùng giảm giá khác hướng | MT5 |
| R²(test) ≈ 0.35–0.55 | LR đơn giản, quantity_sold có nhiều nhiễu | Thực tế |
| R²(valid) ≈ R²(train) ± 0.05 | Mô hình tổng quát tốt | Tin cậy |

---

## Mô hình 3: K-Means Clustering — Phân cụm sản phẩm

### Mục tiêu phân tích
Phân cụm sản phẩm dựa trên đặc trưng thị trường để khám phá **các phân khúc tự nhiên**, sau đó đối chiếu với `brand_type` để đo mức độ phân hóa.

> **Câu hỏi trả lời:** *"Thị trường điện tử Tiki có mấy phân khúc tự nhiên? Các nhóm thương hiệu có cô lập trong phân khúc riêng không?"*  
> **Liên kết:** MT1 (thị phần), MT6 (danh mục), MT9 (tập trung người bán)

### Lý do chọn K-Means
- Unsupervised — khám phá pattern không cần label định sẵn
- Cho phép đặt câu hỏi mở: thị trường tự nhiên phân hóa như thế nào?
- Kết hợp PCA → trực quan 2D dễ trình bày

---

### Chiến lược chia dữ liệu — ML3

```
K-Means là unsupervised → không cần train/valid/test theo nghĩa supervised.
Tuy nhiên vẫn áp dụng chiến lược phân tách để đánh giá tính ổn định:

Dữ liệu: df[df['purchase_status'] == 'has_sales']  →  ~5,482 mẫu

├── FIT SET   80% = ~4,385  →  Fit K-Means, chọn K tối ưu
└── HOLDOUT   20% = ~1,097  →  Kiểm tra Silhouette trên mẫu chưa thấy
                                → Nếu Silhouette(holdout) ≈ Silhouette(fit): cluster ổn định
                                → Nếu lệch nhiều: cluster không tổng quát được

Code:
  fit_df, holdout_df = train_test_split(df_sales, test_size=0.20, random_state=42)
  kmeans.fit(X_fit_scaled)
  labels_holdout = kmeans.predict(X_holdout_scaled)
  # So sánh silhouette score giữa fit và holdout
```

### Quy trình train

```
Bước 1: Feature Engineering
├── Features: price, discount_rate, rating_average, review_count, quantity_sold
├── Biến đổi log: log1p(price), log1p(review_count), log1p(quantity_sold)
│                 (bắt buộc — phân phối lệch rất mạnh ảnh hưởng K-Means)
└── Chuẩn hóa: StandardScaler fit trên FIT SET, transform Holdout

Bước 2: Chọn số cụm K tối ưu (trên FIT SET)
├── Elbow Method: Inertia vs K (K=2..10)
├── Silhouette Score vs K
└── Chọn K* tại điểm "khuỷu tay" + Silhouette cao nhất

Bước 3: Fit K-Means tối ưu
├── KMeans(n_clusters=K*, random_state=42, n_init=10)
├── Gán cluster label cho FIT SET
└── Predict cluster label cho HOLDOUT SET

Bước 4: Kiểm tra tính ổn định
└── Silhouette(FIT) vs Silhouette(HOLDOUT) — gap < 0.05 là ổn định

Bước 5: Phân tích cluster
├── Mean/Median profile mỗi cluster (bảng tổng hợp)
├── Cross-tab: cluster × brand_type (FIT + HOLDOUT kết hợp)
└── Đặt tên cluster dựa trên đặc trưng:
    VD: "Star Products" / "Budget Volume" / "Dormant Listings"
```

### Trực quan hóa kết quả

| # | Biểu đồ | Loại | Liên kết phân tích |
|---|---------|------|-------------------|
| 1 | **Elbow + Silhouette** (chọn K) | 2 line charts cạnh nhau | Căn cứ chọn K |
| 2 | **Scatter PCA 2D** — màu = cluster, marker = brand_type | Scatter plot | Trực quan phân cụm |
| 3 | **Radar chart** profile các cluster | Spider/Radar chart | So sánh đặc trưng cụm |
| 4 | **Stacked bar** — % brand_type trong mỗi cluster | Stacked bar | MT1, MT6 |
| 5 | **Silhouette FIT vs HOLDOUT** | Bar chart | Tính ổn định cluster |

### Kết quả kỳ vọng & phân tích kỳ vọng

| Kỳ vọng | Cơ sở | Hỗ trợ mục tiêu |
|---------|-------|----------------|
| K* = 3 hoặc 4 | 3 brand_type + phân tầng giá rõ | MT6 |
| Cluster "Premium" chứa >70% Global Brand | MT2: Global cao giá 2.5x | MT2, MT10 |
| Cluster "Budget" chứa >70% Local Generic | MT1: Local nhiều nhưng ít doanh số | MT1, MT9 |
| Silhouette(hold) ≈ Silhouette(fit) ± 0.03 | Cluster ổn định | Tin cậy |

---

## Tổng hợp chiến lược Train/Valid/Test

### Sơ đồ toàn bộ quy trình

```
RAW DATA (10,978 rows)
│
├─► [Preprocessing] drop missing, encode categories, log-transform skewed vars
│
├─► ML1 — RF Classifier (10,977 rows, all purchase_status)
│    ├── TRAIN  60%  → Fit model
│    ├── VALID  20%  → Chỉnh n_estimators, phát hiện overfit
│    └── TEST   20%  → Báo cáo Accuracy / F1 / Confusion Matrix
│
├─► ML2 — Linear Regression (5,482 rows, has_sales only)
│    ├── Dataset A (Global_Brand):  TRAIN 60% / VALID 20% / TEST 20%
│    ├── Dataset B (Local_Generic): TRAIN 60% / VALID 20% / TEST 20%
│    └── Dataset C (All brands):    TRAIN 60% / VALID 20% / TEST 20%
│         → Báo cáo R² / RMSE / Coefficients
│
└─► ML3 — K-Means Clustering (5,482 rows, has_sales only)
     ├── FIT     80%  → Chọn K, fit model, gán cluster
     └── HOLDOUT 20%  → Kiểm tra Silhouette ổn định
          → Báo cáo Cluster Profile / Brand Distribution
```

### Nguyên tắc chống Data Leakage

| Nguyên tắc | Áp dụng |
|-----------|---------|
| Fit encoder/scaler chỉ trên TRAIN | LabelEncoder, StandardScaler, OneHotEncoder |
| Không dùng TEST để chọn siêu tham số | Mọi quyết định qua VALID |
| Log-transform tính trên toàn bộ trước khi chia | log1p không gây leakage |
| Random state = 42 cho mọi phép chia | Tái lặp hoàn toàn |

### Đóng góp của ML vào nhận xét chung

| Mô hình | Loại | Câu hỏi | Mục tiêu |
|---------|------|---------|---------|
| **Random Forest** | Classification | Yếu tố nào phân biệt brand_type? | MT1, MT2, MT6 |
| **Linear Regression** | Regression | Yếu tố nào thúc đẩy doanh số mỗi nhóm? | MT4, MT7, MT10 |
| **K-Means** | Clustering | Thị trường tự nhiên phân hóa như thế nào? | MT1, MT6, MT9 |

### Thư viện cần dùng

```python
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LinearRegression
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler, LabelEncoder, OneHotEncoder
from sklearn.metrics import (classification_report, confusion_matrix,
                             accuracy_score, r2_score, mean_squared_error,
                             silhouette_score)
from sklearn.decomposition import PCA
import numpy as np, pandas as pd, matplotlib.pyplot as plt, seaborn as sns
```

### Đa dạng biểu đồ (tổng hợp)

| Loại biểu đồ | Dùng ở | Mục đích |
|-------------|--------|---------|
| Horizontal bar | ML1 Feature Importance | Yếu tố quan trọng nhất |
| Heatmap | ML1 Confusion Matrix | Phân loại đúng/sai |
| Line chart | ML1 Accuracy curve, ML3 Elbow/Silhouette | Chọn tham số |
| Violin plot | ML1 Feature distribution | So sánh brand_type |
| Grouped bar | ML2 Coefficients, R² comparison | So sánh model |
| Scatter + line | ML2 Actual vs Predicted | Độ chính xác |
| Histogram | ML2 Residuals | Kiểm tra giả định |
| Scatter PCA | ML3 Clusters 2D | Trực quan phân cụm |
| Radar/Spider | ML3 Cluster profile | Đặc trưng từng cụm |
| Stacked bar | ML3 Brand × Cluster | Phân bố thương hiệu |

---

## Output dự kiến
- **Notebook**: `analysis/03_machine_learning.ipynb`
- **Cấu trúc**:
  1. Cell 1: Import + Load data + Preprocessing chung
  2. Cell 2–6: ML1 Random Forest (split → fit → valid check → test → charts)
  3. Cell 7–11: ML2 Linear Regression (3 models → coefficients → charts)
  4. Cell 12–16: ML3 K-Means (elbow → fit → holdout → profile → charts)
  5. Cell 17: Tổng kết ML → Kết luận nghiên cứu
