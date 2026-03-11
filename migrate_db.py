"""
migrate_db.py – Xóa cột is_official khỏi SQLite database hiện tại.

Dùng khi:
- Đã crawl dữ liệu với schema cũ (có is_official)
- Muốn đồng nhất với schema mới (14 trường, không có is_official)

Chiến lược (SQLite không hỗ trợ DROP COLUMN trực tiếp trước v3.35):
  1. Tạo bảng products_new với schema mới (không có is_official)
  2. Copy toàn bộ dữ liệu từ products → products_new
  3. Drop bảng products cũ
  4. Rename products_new → products
  5. Recreate indexes

An toàn: Backup DB trước khi chạy!
"""

import sqlite3
import shutil
import os
import sys
from datetime import datetime


def migrate(db_path: str, dry_run: bool = False):
    """
    Xóa cột is_official khỏi bảng products.

    Args:
        db_path: Đường dẫn file .db
        dry_run: Nếu True, chỉ kiểm tra (không thực sự thay đổi)
    """
    if not os.path.exists(db_path):
        print(f"ERROR: Không tìm thấy DB: {db_path}")
        sys.exit(1)

    # ── Backup trước khi migrate ───────────────────────────────────────────────
    backup_path = db_path.replace(".db", f"_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db")
    shutil.copy2(db_path, backup_path)
    print(f"[1/5] Backup OK: {backup_path}")

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    # ── Kiểm tra schema hiện tại ───────────────────────────────────────────────
    cur.execute("PRAGMA table_info(products)")
    existing_cols = [row[1] for row in cur.fetchall()]
    print(f"[2/5] Schema hien tai: {existing_cols}")

    if "is_official" not in existing_cols:
        print("      -> is_official khong ton tai. Khong can migrate.")
        conn.close()
        return

    if dry_run:
        print("      [DRY RUN] Se xoa cot 'is_official'. Dung -run de thuc thi.")
        conn.close()
        return

    # ── Tạo bảng mới không có is_official ─────────────────────────────────────
    print("[3/5] Tao bang products_new (khong co is_official)...")
    conn.execute("""
        CREATE TABLE products_new (
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
            is_tiki_trading  INTEGER DEFAULT 0,
            first_crawled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_crawled_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            crawl_run_id     TEXT,
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        )
    """)

    # ── Copy dữ liệu (bỏ cột is_official) ────────────────────────────────────
    print("[4/5] Copy du lieu sang bang moi...")
    conn.execute("""
        INSERT INTO products_new (
            product_id, product_name, category_id, category_name,
            brand_name, brand_type, price, original_price,
            discount_rate, rating_average, review_count,
            quantity_sold, seller_name, is_tiki_trading,
            first_crawled_at, last_crawled_at, crawl_run_id
        )
        SELECT
            product_id, product_name, category_id, category_name,
            brand_name, brand_type, price, original_price,
            discount_rate, rating_average, review_count,
            quantity_sold, seller_name, is_tiki_trading,
            first_crawled_at, last_crawled_at, crawl_run_id
        FROM products
    """)
    conn.commit()

    # Xác nhận số dòng khớp
    cur.execute("SELECT COUNT(*) FROM products")
    old_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM products_new")
    new_count = cur.fetchone()[0]
    assert old_count == new_count, f"Du lieu bi mat! old={old_count} new={new_count}"
    print(f"      -> {new_count:,} dong da copy thanh cong.")

    # ── Drop bảng cũ, rename bảng mới ────────────────────────────────────────
    conn.execute("DROP TABLE products")
    conn.execute("ALTER TABLE products_new RENAME TO products")

    # ── Recreate indexes ──────────────────────────────────────────────────────
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_brand_type ON products(brand_type)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_category ON products(category_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_brand_name ON products(brand_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_products_price ON products(price)")
    conn.commit()
    print("[5/5] Indexes da duoc tao lai.")

    # ── Verify schema mới ─────────────────────────────────────────────────────
    cur.execute("PRAGMA table_info(products)")
    new_cols = [row[1] for row in cur.fetchall()]
    assert "is_official" not in new_cols, "is_official van con ton tai!"
    print(f"\nSchema moi: {new_cols}")
    print(f"\nMigrate HOAN THANH! DB: {db_path}")
    print(f"Backup luu tai: {backup_path}")

    conn.close()


if __name__ == "__main__":
    import json

    # Đọc path từ config.json
    with open("config.json", encoding="utf-8") as f:
        cfg = json.load(f)

    db_path = cfg["output"]["sqlite_db"]
    dry_run = "--dry-run" in sys.argv

    migrate(db_path, dry_run=dry_run)
