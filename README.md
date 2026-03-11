# Ecom-Data-Crawler

> **Hệ thống thu thập dữ liệu linh kiện điện tử trên sàn Tiki**
> Phục vụ phân tích phân hóa thị trường **Global Brands vs OEM/Generic** năm 2026

---

## Mục tiêu nghiên cứu

Phân tích sự phân hóa thị trường giữa nhóm **Thương hiệu chính hãng (Global Brands)** và **Hàng gia công nội địa (OEM/Generic)** trong ngành linh kiện điện tử trên sàn Tiki, nhằm xác định các yếu tố trọng yếu ảnh hưởng đến niềm tin và quyết định chi tiêu của người dùng.

---

## Kiến trúc hệ thống

```
Ecom-Data-Crawler/
├── config.json              # Bộ não điều khiển (Dynamic Input)
├── main.py                  # Điều phối 3 Phase crawl
├── crawler/
│   ├── __init__.py
│   ├── category_mapper.py   # Tầng 2: Deep Category Discovery
│   ├── brand_classifier.py  # Tầng 3: Real-time Brand Classification
│   ├── scraper.py           # Tầng 4: Core Crawl Engine
│   └── storage.py           # Tầng 5: SQLite + CSV Storage
├── data/                    # Output (auto-created)
│   ├── tiki_electronics_2026.db      # SQLite database
│   └── tiki_electronics_2026_clean.csv
├── logs/                    # Log files (auto-created)
└── requirements.txt
```

---

## Cài đặt

```bash
# Clone project
git clone <repo-url>
cd Ecom-Data-Crawler

# Cài dependencies
pip install -r requirements.txt
```

---

## Cách sử dụng

### 1. Chạy crawl (mặc định dùng config.json)

```bash
python main.py
```

### 2. Chạy với config tùy chỉnh

```bash
python main.py my_custom_config.json
```

### 3. Chỉ export CSV (không crawl)

```bash
python main.py --export-only
```

### 4. Xem thống kê database hiện tại

```bash
python main.py --stats
```

---

## Hướng dẫn sử dụng Config.json

### Thay đổi Category để crawl

File `config.json` cho phép bạn thay đổi **bất kỳ ngành hàng nào** trên Tiki mà không cần sửa code.

```jsonc
"target_parents": [
  {
    "id": 8322,          // ID danh mục trên Tiki
    "name": "Linh kiện máy tính",
    "description": "RAM, CPU, Mainboard, VGA..."
  },
  {
    "id": 1815,
    "name": "Thiết bị mạng",
    "description": "Router, Switch, Access Point..."
  }
]
```

**Cách tìm Category ID:**
1. Vào [tiki.vn](https://tiki.vn), chọn danh mục cần crawl
2. Gọi API: `https://tiki.vn/api/v2/categories/{ID}` để xác minh
3. Hoặc dùng URL category trên Tiki, đoạn cuối thường chứa ID

**Quan trọng:**
- Chỉ cần đặt **parent category** (cha). Crawler sẽ **TỰ ĐỘNG** tìm tất cả sub-categories con/cháu bên trong.
- Nếu muốn crawl ngành hàng khác (ví dụ: Phụ kiện điện thoại), chỉ cần thêm 1 object mới vào mảng `target_parents`.

### Thay đổi danh sách Global Brands

```jsonc
"global_brands_dict": [
  // Các entry bắt đầu bằng "__" là comment/separator, sẽ bị bỏ qua
  "__CPU_GPU__",
  "intel", "amd", "nvidia",
  "__MEMORY__",
  "samsung", "kingston", "corsair",
  // Thêm brand mới tại đây...
]
```

### Điều chỉnh tốc độ crawl

```jsonc
"scraping_settings": {
  "max_pages_per_category": 50,     // Tối đa 50 trang/category
  "products_per_page": 40,          // 40 SP/trang (giới hạn Tiki API)
  "delay_between_requests_sec": 2.5, // Chờ 2.5s giữa mỗi request
  "max_retries": 3,                 // Retry tối đa 3 lần khi lỗi
  "retry_backoff_sec": 5,           // Chờ 5s trước khi retry
  "request_timeout_sec": 15         // Timeout mỗi request 15s
}
```

---

## Chiến lược Phân loại Brand (3 tầng)

Mỗi sản phẩm được phân loại **ngay lúc crawl** (real-time) thành 3 nhóm:

| Nhóm | Điều kiện | Ví dụ |
|------|-----------|-------|
| **Global_Brand** | `brand_name` ∈ `global_brands_dict` | Intel, Samsung, Corsair |
| **OEM_Generic** | `brand_name` ∈ OEM indicators ("OEM", "No Brand", rỗng) HOẶC hàng cross-border | OEM, No Brand, Generic |
| **Local_Generic** | Không thuộc 2 nhóm trên (brand nhỏ lẻ/nội địa) | Các brand chưa biết |

**Lưu ý:** Trường `is_tiki_trading` cho biết sản phẩm có do Tiki Trading phân phối không (detect qua `seller_name`). Xem `brand_classifier.py` để biết chi tiết decision tree.

---

## Chiến lược Chống trùng lặp

Hệ thống đảm bảo **crawl bao nhiêu lần cũng không trùng dữ liệu**:

1. **Trong 1 lần crawl:** Category IDs được de-duplicate bằng `set()` trước khi crawl
2. **Giữa nhiều lần crawl:** `product_id` là PRIMARY KEY trong SQLite
   - Sản phẩm mới → INSERT
   - Sản phẩm đã có → UPDATE dữ liệu mới nhất
   - `first_crawled_at`: Giữ nguyên thời điểm phát hiện lần đầu
   - `last_crawled_at`: Tự động cập nhật mỗi lần crawl
3. **Theo dõi phiên:** Mỗi lần crawl có `run_id` riêng, lưu trong bảng `crawl_runs`

---

## Schema dữ liệu (14 trường + metadata)

| Trường | Kiểu | Ý nghĩa phân tích |
|--------|------|-------------------|
| `product_id` | INT (PK) | Định danh, chống trùng lặp |
| `product_name` | TEXT | Text mining, xác minh brand |
| `category_id` | INT | Phân khúc theo ngách SP |
| `category_name` | TEXT | Tên danh mục (human-readable) |
| `brand_name` | TEXT | Tên thương hiệu thô |
| `brand_type` | TEXT | **Cốt lõi**: Global/OEM/Local |
| `price` | INT | Giá bán thực tế |
| `original_price` | INT | Giá gốc (tính discount thực) |
| `discount_rate` | REAL | % giảm giá (phát hiện ảo) |
| `rating_average` | REAL | Điểm đánh giá 1-5 sao |
| `review_count` | INT | Lượng đánh giá (độ tin cậy) |
| `quantity_sold` | INT | Đã bán (thị phần) |
| `seller_name` | TEXT | Nhà bán hàng |
| `is_tiki_trading` | INT | 1=Tiki Trading, 0=Không |
| `first_crawled_at` | TIMESTAMP | Lần đầu phát hiện |
| `last_crawled_at` | TIMESTAMP | Lần crawl gần nhất |

> **Lưu ý:** `is_official` đã bị loại bỏ. API danh sách Tiki không trả về badge data trong list response — field này luôn = 0 và không có giá trị phân tích. Xem `brand_classifier.py::_detect_official_badge()` nếu cần nghiên cứu thêm qua detail API.

---

## Output

- **SQLite Database:** `data/tiki_electronics_2026.db` - Dùng cho truy vấn SQL, kết nối PowerBI
- **CSV File:** `data/tiki_electronics_2026.csv` - Import vào Excel/Tableau/PowerBI
- **Log Files:** `logs/crawl_YYYYMMDD_HHMMSS.log` - Debug và audit trail

---

## Ví dụ truy vấn phân tích (SQL)

```sql
-- So sánh giá trung bình Global vs OEM
SELECT brand_type, 
       COUNT(*) as total,
       ROUND(AVG(price), 0) as avg_price,
       ROUND(AVG(rating_average), 2) as avg_rating,
       SUM(quantity_sold) as total_sold
FROM products
GROUP BY brand_type;

-- Top 10 brand có nhiều sản phẩm nhất
SELECT brand_name, brand_type, COUNT(*) as product_count
FROM products
GROUP BY brand_name
ORDER BY product_count DESC
LIMIT 10;

-- Sản phẩm OEM có rating cao bất thường (>4.5 nhưng ít review)
SELECT product_name, brand_name, price, rating_average, review_count
FROM products
WHERE brand_type = 'OEM_Generic'
  AND rating_average > 4.5
  AND review_count < 10;
```