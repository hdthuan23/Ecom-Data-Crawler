"""
SQLite Storage - Lưu trữ & Quản lý Dữ liệu với SQLite

Chịu trách nhiệm:
  1. Tạo database + tables với schema tối ưu
  2. UPSERT sản phẩm (chống trùng lặp giữa nhiều lần crawl)
  3. Theo dõi lịch sử các lần crawl (crawl_runs)
  4. Lưu metadata danh mục (categories)
  5. Export ra CSV cho phân tích với PowerBI/Tableau
  6. Thống kê nhanh (stats)

Chiến lược chống trùng lặp:
  - product_id là PRIMARY KEY
  - INSERT ... ON CONFLICT DO UPDATE (UPSERT)
  - first_crawled_at: giữ nguyên giá trị lần đầu phát hiện
  - last_crawled_at: cập nhật mỗi lần crawl
  - Kết quả: crawl bao nhiêu lần cũng không trùng lặp
"""

import os
import csv
import sqlite3
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class SQLiteStorage:
    """
    Quản lý SQLite database cho dữ liệu crawl.

    Tables:
    - categories: Metadata danh mục đã phát hiện
    - products: Dữ liệu sản phẩm chính (15 trường + metadata)
    - crawl_runs: Lịch sử các phiên crawl

    Indexes: Tối ưu cho truy vấn phân tích theo brand_type,
    category_id, brand_name.
    """

    def __init__(self, db_path):
        """
        Args:
            db_path (str): Đường dẫn file database SQLite.
                Directory sẽ tự động tạo nếu chưa tồn tại.
        """
        self.db_path = db_path

        # Tạo thư mục nếu chưa có
        db_dir = os.path.dirname(db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        self.conn = sqlite3.connect(db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._create_tables()

        logger.info(f"SQLite database initialized: {db_path}")

    def _create_tables(self):
        """Tạo toàn bộ tables và indexes cần thiết."""
        cursor = self.conn.cursor()

        # ========== BẢNG DANH MỤC ==========
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                category_id   INTEGER PRIMARY KEY,
                category_name TEXT    NOT NULL,
                parent_id     INTEGER,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # ========== BẢNG SẢN PHẨM (CORE) ==========
        # 15 trường dữ liệu + 3 trường metadata (first/last_crawled, run_id)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id       INTEGER PRIMARY KEY,
                product_name     TEXT,
                category_id      INTEGER,
                category_name    TEXT,
                brand_name       TEXT,
                brand_type       TEXT,
                price            INTEGER,
                original_price   INTEGER,
                discount_rate    REAL,
                rating_average   REAL,
                review_count     INTEGER,
                quantity_sold    INTEGER,
                seller_name      TEXT,
                is_official      INTEGER DEFAULT 0,
                is_tiki_trading  INTEGER DEFAULT 0,
                first_crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_crawled_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                crawl_run_id     TEXT,
                FOREIGN KEY (category_id) REFERENCES categories(category_id)
            )
        """)

        # ========== BẢNG LỊCH SỬ CRAWL ==========
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS crawl_runs (
                run_id             TEXT PRIMARY KEY,
                start_time         TIMESTAMP,
                end_time           TIMESTAMP,
                categories_crawled INTEGER DEFAULT 0,
                products_found     INTEGER DEFAULT 0,
                products_new       INTEGER DEFAULT 0,
                products_updated   INTEGER DEFAULT 0,
                status             TEXT DEFAULT 'running'
            )
        """)

        # ========== INDEXES cho phân tích nhanh ==========
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_products_brand_type "
            "ON products(brand_type)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_products_category "
            "ON products(category_id)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_products_brand_name "
            "ON products(brand_name)"
        )
        cursor.execute(
            "CREATE INDEX IF NOT EXISTS idx_products_price "
            "ON products(price)"
        )

        self.conn.commit()
        logger.debug("Database tables and indexes created/verified.")

    # ==============================================================
    # CATEGORY OPERATIONS
    # ==============================================================

    def save_categories(self, categories, parent_id):
        """
        Lưu danh sách categories vào database.
        Dùng INSERT OR IGNORE để không ghi đè nếu đã tồn tại.

        Args:
            categories (list[dict]): Danh sách {"id", "name", "parent_id"}
            parent_id (int): ID của parent category
        """
        cursor = self.conn.cursor()
        for cat in categories:
            cursor.execute(
                """
                INSERT OR IGNORE INTO categories
                    (category_id, category_name, parent_id)
                VALUES (?, ?, ?)
                """,
                (cat["id"], cat["name"], parent_id),
            )
        self.conn.commit()
        logger.debug(f"Saved {len(categories)} categories (parent: {parent_id})")

    # ==============================================================
    # PRODUCT OPERATIONS (UPSERT - CHỐNG TRÙNG LẶP)
    # ==============================================================

    def upsert_products(self, products, run_id):
        """
        Insert hoặc Update sản phẩm (UPSERT).

        Chiến lược chống trùng lặp khi crawl nhiều lần:
        - Nếu product_id CHƯA tồn tại -> INSERT mới
        - Nếu product_id ĐÃ tồn tại -> UPDATE dữ liệu mới nhất
        - first_crawled_at: GIỮ NGUYÊN giá trị lần đầu
        - last_crawled_at: TỰ ĐỘNG cập nhật thành thời điểm crawl mới

        Args:
            products (list[dict]): Danh sách sản phẩm từ scraper
            run_id (str): ID phiên crawl hiện tại

        Returns:
            tuple: (new_count, updated_count)
        """
        cursor = self.conn.cursor()
        new_count = 0
        updated_count = 0

        for p in products:
            # Kiểm tra sản phẩm đã tồn tại chưa
            cursor.execute(
                "SELECT 1 FROM products WHERE product_id = ?",
                (p["product_id"],),
            )
            exists = cursor.fetchone() is not None

            if exists:
                updated_count += 1
            else:
                new_count += 1

            # UPSERT: Insert or Update on conflict
            cursor.execute(
                """
                INSERT INTO products (
                    product_id, product_name, category_id, category_name,
                    brand_name, brand_type, price, original_price,
                    discount_rate, rating_average, review_count,
                    quantity_sold, seller_name, is_official, is_tiki_trading,
                    first_crawled_at, last_crawled_at, crawl_run_id
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    COALESCE(
                        (SELECT first_crawled_at FROM products
                         WHERE product_id = ?),
                        CURRENT_TIMESTAMP
                    ),
                    CURRENT_TIMESTAMP,
                    ?
                )
                ON CONFLICT(product_id) DO UPDATE SET
                    product_name     = excluded.product_name,
                    category_id      = excluded.category_id,
                    category_name    = excluded.category_name,
                    brand_name       = excluded.brand_name,
                    brand_type       = excluded.brand_type,
                    price            = excluded.price,
                    original_price   = excluded.original_price,
                    discount_rate    = excluded.discount_rate,
                    rating_average   = excluded.rating_average,
                    review_count     = excluded.review_count,
                    quantity_sold    = excluded.quantity_sold,
                    seller_name      = excluded.seller_name,
                    is_official      = excluded.is_official,
                    is_tiki_trading  = excluded.is_tiki_trading,
                    last_crawled_at  = CURRENT_TIMESTAMP,
                    crawl_run_id     = excluded.crawl_run_id
                """,
                (
                    p["product_id"], p["product_name"],
                    p["category_id"], p["category_name"],
                    p["brand_name"], p["brand_type"],
                    p["price"], p["original_price"],
                    p["discount_rate"], p["rating_average"],
                    p["review_count"], p["quantity_sold"],
                    p["seller_name"], p["is_official"], p["is_tiki_trading"],
                    p["product_id"],  # Cho subquery COALESCE
                    run_id,
                ),
            )

        self.conn.commit()
        logger.info(
            f"Upserted {len(products)} products "
            f"(New: {new_count}, Updated: {updated_count})"
        )
        return new_count, updated_count

    # ==============================================================
    # CRAWL RUN TRACKING
    # ==============================================================

    def start_run(self, run_id):
        """Ghi nhận bắt đầu 1 phiên crawl mới."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO crawl_runs (run_id, start_time, status)
            VALUES (?, CURRENT_TIMESTAMP, 'running')
            """,
            (run_id,),
        )
        self.conn.commit()
        logger.info(f"Crawl run started: {run_id}")

    def finish_run(self, run_id, categories_crawled, products_found,
                   products_new, products_updated):
        """Ghi nhận kết thúc phiên crawl với thống kê."""
        cursor = self.conn.cursor()
        cursor.execute(
            """
            UPDATE crawl_runs SET
                end_time           = CURRENT_TIMESTAMP,
                categories_crawled = ?,
                products_found     = ?,
                products_new       = ?,
                products_updated   = ?,
                status             = 'completed'
            WHERE run_id = ?
            """,
            (categories_crawled, products_found,
             products_new, products_updated, run_id),
        )
        self.conn.commit()
        logger.info(f"Crawl run completed: {run_id}")

    # ==============================================================
    # EXPORT & STATISTICS
    # ==============================================================

    def export_to_csv(self, csv_path):
        """
        Export toàn bộ bảng products ra file CSV.
        Encoding utf-8-sig để mở đúng tiếng Việt trên Excel.

        Args:
            csv_path (str): Đường dẫn file CSV output

        Returns:
            int: Số lượng records đã export
        """
        csv_dir = os.path.dirname(csv_path)
        if csv_dir:
            os.makedirs(csv_dir, exist_ok=True)

        cursor = self.conn.cursor()
        cursor.execute(
            """
            SELECT
                product_id, product_name, category_id, category_name,
                brand_name, brand_type, price, original_price,
                discount_rate, rating_average, review_count,
                quantity_sold, seller_name, is_official, is_tiki_trading,
                first_crawled_at, last_crawled_at
            FROM products
            ORDER BY brand_type, category_id, price DESC
            """
        )

        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()

        with open(csv_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(columns)
            writer.writerows(rows)

        logger.info(f"Exported {len(rows)} products -> {csv_path}")
        return len(rows)

    def get_stats(self):
        """
        Thống kê nhanh dữ liệu trong database.

        Returns:
            dict: {
                "total_products": int,
                "by_brand_type": {"Global_Brand": n, "OEM_Generic": n, ...},
                "unique_categories": int,
                "unique_brands": int,
                "total_runs": int,
                "avg_price_by_type": {"Global_Brand": avg, ...},
                "avg_rating_by_type": {"Global_Brand": avg, ...}
            }
        """
        cursor = self.conn.cursor()
        stats = {}

        cursor.execute("SELECT COUNT(*) FROM products")
        stats["total_products"] = cursor.fetchone()[0]

        cursor.execute(
            "SELECT brand_type, COUNT(*) FROM products GROUP BY brand_type"
        )
        stats["by_brand_type"] = dict(cursor.fetchall())

        cursor.execute("SELECT COUNT(DISTINCT category_id) FROM products")
        stats["unique_categories"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(DISTINCT brand_name) FROM products")
        stats["unique_brands"] = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM crawl_runs")
        stats["total_runs"] = cursor.fetchone()[0]

        cursor.execute(
            """
            SELECT brand_type, ROUND(AVG(price), 0)
            FROM products GROUP BY brand_type
            """
        )
        stats["avg_price_by_type"] = dict(cursor.fetchall())

        cursor.execute(
            """
            SELECT brand_type, ROUND(AVG(rating_average), 2)
            FROM products
            WHERE rating_average > 0
            GROUP BY brand_type
            """
        )
        stats["avg_rating_by_type"] = dict(cursor.fetchall())

        return stats

    def get_run_history(self):
        """Lấy lịch sử tất cả các phiên crawl."""
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT * FROM crawl_runs ORDER BY start_time DESC"
        )
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def close(self):
        """Đóng kết nối database."""
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed.")
