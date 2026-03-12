# Ecom-Data-Crawler

Hệ thống thu thập, tiền xử lý và phân tích dữ liệu **linh kiện điện tử trên sàn Tiki** phục vụ nghiên cứu phân hóa thị trường **Global Brands vs OEM/Generic** năm 2026.

---

## Mục tiêu nghiên cứu

Phân tích sự phân hóa thị trường giữa **Thương hiệu chính hãng (Global Brand)** và **Hàng gia công / nội địa (OEM/Local Generic)** trong ngành linh kiện điện tử trên Tiki, nhằm xác định các yếu tố ảnh hưởng đến giá, doanh số, và niềm tin của người dùng.

---

## Trạng thái dự án

| Giai đoạn | Trạng thái | Kết quả |
|-----------|-----------|---------|
| Thu thập dữ liệu (Crawl) | ✅ Hoàn thành | 11,668 sản phẩm từ 10 nhóm danh mục |
| Tiền xử lý (Preprocessing) | ✅ Hoàn thành | 20 cột, đã gắn nhãn anomaly & discount |
| Phân tích thị trường (EDA) | ✅ Hoàn thành | `analysis/02_market_analysis.ipynb` |
| Machine Learning | ✅ Hoàn thành | `analysis/03_machine_learning.ipynb` |

---

## Cấu trúc dự án

```
Ecom-Data-Crawler/
├── config.json                    # Cấu hình crawl (categories, brands, tốc độ)
├── main.py                        # Entry point: điều phối 3 Phase crawl
├── migrate_db.py                  # Migrate schema SQLite
├── get_tiki_categories.py         # Khám phá category ID trên Tiki
│
├── crawler/
│   ├── category_mapper.py         # Phase 1: Đệ quy tìm leaf categories
│   ├── brand_classifier.py        # Phase 2: Phân loại brand real-time
│   ├── scraper.py                 # Phase 2: Gọi Tiki API + pagination
│   └── storage.py                 # Phase 3: UPSERT vào SQLite, export CSV
│
├── preprocessing/
│   └── 01_data_quality_check.ipynb  # Kiểm tra chất lượng, gắn nhãn, export
│
├── analysis/
│   ├── 02_market_analysis.ipynb   # EDA: phân hóa thị trường
│   ├── 03_machine_learning.ipynb  # RF Classifier, Linear Regression, K-Means
│   └── ML.MD                      # Đặc tả chiến lược ML (train/valid/test)
│
└── data/
    ├── tiki_electronics_2026.csv           # Raw data (output crawl)
    └── tiki_electronics_2026_processed.csv # Processed data (output preprocessing)
```

---

## Dữ liệu thu thập được

- **11,668 sản phẩm** từ 10 nhóm danh mục: Linh kiện máy tính, Thiết bị mạng, Thiết bị lưu trữ, Phụ kiện game, Tai nghe Bluetooth, Pin dự phòng, Adapter, Cáp chuyển đổi, ...
- **3 nhóm thương hiệu:**

| Brand Type | Số sản phẩm | Tỷ lệ |
|------------|------------|-------|
| Global_Brand | 4,605 | 39.5% |
| Local_Generic | 6,906 | 59.2% |
| OEM_Generic | 157 | 1.3% |

- **Trạng thái bán hàng:** 48.2% có doanh số (`has_sales`), 51.8% chưa bán (`new_listing`)

---

## Schema dữ liệu (20 cột sau preprocessing)

| Cột | Kiểu | Mô tả |
|-----|------|-------|
| `product_id` | INT (PK) | Định danh sản phẩm |
| `product_name` | TEXT | Tên sản phẩm |
| `category_id` / `category_name` | INT / TEXT | Danh mục |
| `brand_name` | TEXT | Tên thương hiệu thô |
| `brand_type` | TEXT | **Global_Brand / Local_Generic / OEM_Generic** |
| `price` | INT | Giá bán thực tế |
| `original_price` | INT | Giá gốc |
| `discount_rate` | REAL | % giảm giá |
| `rating_average` | REAL | Điểm đánh giá (1–5) |
| `review_count` | INT | Số lượt đánh giá |
| `quantity_sold` | INT | Số sản phẩm đã bán |
| `seller_name` | TEXT | Tên nhà bán hàng |
| `is_tiki_trading` | INT | 1 = do Tiki Trading phân phối |
| `first_crawled_at` / `last_crawled_at` | TIMESTAMP | Metadata crawl |
| `purchase_status` | TEXT | `has_sales` / `new_listing` |
| `is_rating_suspect` | INT | 1 = rating > 4.5 với < 10 review (gắn nhãn preprocessing) |
| `discount_flag` | TEXT | `no_discount` / `normal_discount` / `extreme_discount` |
| `is_fake_discount` | INT | 1 = discount_rate > 0 nhưng price ≥ original_price |

> `is_official` đã bị loại bỏ — Tiki API list response không trả về badge data, field này luôn = 0.

---

## Kết quả Preprocessing (`01_data_quality_check.ipynb`)

- **Không có dead columns** cần drop (kiểm tra tự động: cột numeric với >95% giá trị = 0)
- **Rating anomaly:** ~54% sản phẩm đủ điều kiện có dấu hiệu review không đáng tin (rating > 4.5, review < 10) — phân bổ đều ở cả 3 nhóm brand
- **Discount:**
  - 91.6% sản phẩm không giảm giá
  - 7.6% giảm giá bình thường
  - 0.8% giảm giá cực đoan (≥50%)
  - 0% fake discount (giá bán ≥ giá gốc dù hiển thị % giảm)

---

## Phân tích Machine Learning (`03_machine_learning.ipynb`)

Ba mô hình được triển khai theo chiến lược **Train / Validation / Test (60/20/20)**:

| Mô hình | Mục tiêu | Dữ liệu đầu vào |
|---------|---------|----------------|
| **Random Forest Classifier** | Phân loại brand_type từ đặc trưng thị trường | 10,977 sản phẩm (stratified) |
| **Linear Regression** | Dự đoán quantity_sold, tách riêng theo brand_type | ~5,482 sản phẩm has_sales |
| **K-Means Clustering** | Phân cụm thị trường không giám sát | ~5,482 sản phẩm has_sales |

- Random state = 42 cố định ở tất cả phép chia — đảm bảo tái lặp
- Test set chỉ được dùng **1 lần duy nhất** sau khi chốt mô hình trên validation set

---

## Cài đặt & Sử dụng

```bash
git clone https://github.com/hdthuan23/Ecom-Data-Crawler.git
cd Ecom-Data-Crawler
pip install -r requirements.txt
```

### Chạy crawler

```bash
python main.py                      # Dùng config.json mặc định
python main.py my_config.json       # Dùng config tùy chỉnh
python main.py --export-only        # Chỉ export CSV, không crawl
python main.py --stats              # Xem thống kê database hiện tại
```

### Cấu hình crawl (`config.json`)

- **`target_parents`**: Danh sách parent category ID cần crawl. Crawler tự động tìm toàn bộ sub-category bên trong.
- **`global_brands_dict`**: Danh sách brand quốc tế (lowercase). Entry bắt đầu bằng `__` là comment, bị bỏ qua.
- **`scraping_settings`**: Điều chỉnh tốc độ — `delay_between_requests_sec`, `max_retries`, `request_timeout_sec`.

---

## Chống trùng lặp dữ liệu

- **Trong 1 lần crawl:** Category ID được de-duplicate bằng `set()` trước khi crawl
- **Giữa nhiều lần crawl:** `product_id` là PRIMARY KEY trong SQLite — sản phẩm mới INSERT, cũ UPDATE
- `first_crawled_at` giữ nguyên; `last_crawled_at` tự cập nhật mỗi lần crawl
- Mỗi phiên crawl có `run_id` riêng, lưu trong bảng `crawl_runs`