# -*- coding: utf-8 -*-
"""
📝 INPUT FILE - Cấu hình crawl Tiki
Chỉnh sửa file này để thiết lập những gì bạn muốn crawl
Sau đó chạy: python run_crawler.py
"""

# ============================================================
# CHỌN CHẾ ĐỘ CRAWL (chỉ chọn 1 trong các mode)
# ============================================================
# Các mode: 'category', 'url', 'search', 'product'
MODE = 'category'


# ============================================================
# CẤU HÌNH CHO MODE 'category' - Crawl theo danh mục
# ============================================================
# Danh sách categories có sẵn:
#   'dien_thoai'         - Điện thoại Smartphone
#   'laptop'             - Laptop
#   'may_tinh_bang'      - Máy tính bảng
#   'am_thanh'           - Thiết bị âm thanh
#   'phu_kien_dien_thoai'- Phụ kiện điện thoại
#   'thiet_bi_deo'       - Thiết bị đeo thông minh
#   'camera'             - Camera, máy quay
#   'may_choi_game'      - Máy chơi game
#   'linh_kien_may_tinh' - Linh kiện máy tính
#   'thiet_bi_van_phong' - Thiết bị văn phòng

# Để trống [] = crawl TẤT CẢ categories
# Hoặc chỉ định: ['laptop', 'dien_thoai']
CATEGORIES = ['laptop', 'dien_thoai']


# ============================================================
# CẤU HÌNH CHO MODE 'url' - Crawl từ URL
# ============================================================
# Nhập URL trang Tiki (danh mục, bộ lọc, v.v.)
URL = "https://tiki.vn/dien-thoai/c1789"


# ============================================================
# CẤU HÌNH CHO MODE 'search' - Tìm kiếm sản phẩm
# ============================================================
# Từ khóa tìm kiếm (có thể là 1 từ khóa hoặc list nhiều từ khóa)
SEARCH_KEYWORDS = "iphone 15"
# Hoặc nhiều từ khóa: SEARCH_KEYWORDS = ["iphone 15", "samsung galaxy", "xiaomi"]

# Sắp xếp: 'default', 'top_seller', 'newest', 'price,asc', 'price,desc'
SORT_BY = 'top_seller'


# ============================================================
# CẤU HÌNH CHO MODE 'product' - Lấy chi tiết sản phẩm
# ============================================================
# URL sản phẩm hoặc Product ID
PRODUCT_IDS = [
    # "https://tiki.vn/ten-san-pham-p12345678.html",
    # 12345678,
]


# ============================================================
# CẤU HÌNH CHUNG
# ============================================================
# Số trang tối đa mỗi category/tìm kiếm (mỗi trang ~40 sản phẩm)
MAX_PAGES = 3

# Có lưu file không (True/False)
SAVE_DATA = True

# Có phân tích dữ liệu không (True/False)
ANALYZE_DATA = True

# Thư mục lưu file (để None = lưu ở thư mục hiện tại)
OUTPUT_DIR = None
